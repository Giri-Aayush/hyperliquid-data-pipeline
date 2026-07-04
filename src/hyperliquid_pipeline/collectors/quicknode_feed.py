"""QuickNode Hypercore L4 book feed: StreamL4Book gRPC -> per-coin L4Books.

The provider streams, per subscribed coin, one full L4 snapshot on subscribe
followed by incremental per-block diffs whose ``data`` field is the verbatim
node JSON (``{order_statuses, book_diffs}``) — which means the entire book
pipeline is reused unchanged: snapshots go through ``parse_l4_snapshot`` and
diffs through ``parse_obj``/``apply_block``, with the same strict-mode drift
reporting and skip counting as the file feeds.

Mirrors :class:`NodeDiffFeed`'s consumer surface: ``books`` (BookView per
coin), collector-identical callbacks, one ``MarketDataPoint`` per (block,
coin with diffs), ``get_stats()``. Extras here: ``bytes_received`` (provider
billing is per-MB) and reconnect accounting with the collector's full-jitter
backoff. A reconnect is self-healing by construction — the server re-sends a
snapshot on resubscribe, which clears any ``stale`` flag from the gap.

Offline-testable by design: :meth:`_consume_stream` accepts any (a)iterable
of ``L4BookUpdate`` messages, so tests drive it with lists of pb objects and
no network. The live path (:meth:`stream`) only adds channel/stub plumbing.
"""

import asyncio
import inspect
import json
import random
import time
from datetime import datetime, timezone
from typing import Any, AsyncIterable, Callable, Dict, Iterable, List, Optional, Union

from loguru import logger

from ..book.diff_parser import UnrecognizedDiffFormat, parse_l4_snapshot, parse_obj
from ..book.l4_book import L4Book
from ..book.schemas import BlockDiffBatch
from ..config import settings
from ._qn_pb import orderbook_pb2
from .realtime_collector import MarketDataPoint


class QuickNodeFeed:
    """Durable order-level market state from a QuickNode Hypercore stream."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        token: Optional[str] = None,
        coins: Optional[List[str]] = None,
        strict: bool = False,
    ):
        """Initialize the feed.

        Args:
            endpoint: gRPC host:port (defaults to settings.quicknode_grpc_endpoint).
            token: Provider auth token (defaults to settings.quicknode_grpc_token).
            coins: Coins to subscribe (defaults to settings.symbols_list).
            strict: Passed to the book parser — raise on any format drift
                instead of skipping (run the first live session with this on).
        """
        self.endpoint = endpoint or settings.quicknode_grpc_endpoint or None
        self.token = token if token is not None else settings.quicknode_grpc_token
        self.coins = list(coins) if coins else list(settings.symbols_list)
        self.strict = strict
        self.logger = logger.bind(component="quicknode_feed")

        # One L4Book per coin, read-only for consumers (BookView protocol).
        self.books: Dict[str, L4Book] = {}
        self.data_callbacks: List[Callable[[MarketDataPoint], None]] = []

        self.blocks = 0
        self.diffs = 0
        self.parse_skips = 0
        self.snapshots_loaded = 0
        self.bytes_received = 0
        self.reconnects = 0
        self._running = False

    # --- callbacks (same contract as the other collectors) ------------------

    def add_data_callback(self, callback: Callable[[MarketDataPoint], None]):
        """Add a callback for emitted points; sync or async, run serially.

        Identical semantics to the WebSocket collector and NodeDiffFeed: an
        awaitable return is awaited, a failing callback is logged and never
        stops the feed.
        """
        self.data_callbacks.append(callback)

    async def _dispatch(self, data_point: MarketDataPoint) -> None:
        for callback in self.data_callbacks:
            try:
                result = callback(data_point)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                self.logger.error(f"Error in data callback: {e}")

    # --- stream consumption (offline-testable core) ---------------------------

    async def _consume_stream(
        self,
        coin: str,
        updates: Union[Iterable[Any], AsyncIterable[Any]],
    ) -> None:
        """Apply a stream of L4BookUpdate messages for one coin.

        ``updates`` may be a plain iterable (tests: a list of pb objects) or
        an async iterable (live: the gRPC server-streaming call).
        """
        if hasattr(updates, "__aiter__"):
            async for update in updates:
                await self._process_update(coin, update)
        else:
            for update in updates:
                await self._process_update(coin, update)

    async def _process_update(self, coin: str, update: Any) -> None:
        recv_ts_ms = time.time() * 1000
        recv_mono_ns = time.monotonic_ns()
        self.bytes_received += update.ByteSize()
        which = update.WhichOneof("update")
        if which == "snapshot":
            self._load_snapshot(coin, update.snapshot)
        elif which == "diff":
            await self._apply_diff(coin, update.diff, recv_ts_ms, recv_mono_ns)
        else:  # empty oneof: unknown server behavior, count it, keep going
            self._count_skip(f"empty L4BookUpdate for {coin}")

    def _load_snapshot(self, coin: str, snapshot_pb: Any) -> None:
        """Full book reset from a proto snapshot (sent on every subscribe)."""
        obj = {
            "coin": snapshot_pb.coin or coin,
            "time": snapshot_pb.time,
            "height": snapshot_pb.height,
            "bids": [self._order_dict(o) for o in snapshot_pb.bids],
            "asks": [self._order_dict(o) for o in snapshot_pb.asks],
        }
        self._book(obj["coin"]).load_snapshot(parse_l4_snapshot(obj))
        self.snapshots_loaded += 1

    @staticmethod
    def _order_dict(order_pb: Any) -> dict:
        # Proto field names match the verified snapshot schema (snake_case),
        # so the dict flows through parse_l4_snapshot untouched.
        return {
            "user": order_pb.user,
            "coin": order_pb.coin,
            "side": order_pb.side,
            "limit_px": order_pb.limit_px,
            "sz": order_pb.sz,
            "oid": order_pb.oid,
            "timestamp": order_pb.timestamp,
            "trigger_condition": order_pb.trigger_condition,
            "is_trigger": order_pb.is_trigger,
            "trigger_px": order_pb.trigger_px or None,
            "is_position_tpsl": order_pb.is_position_tpsl,
            "reduce_only": order_pb.reduce_only,
            "order_type": order_pb.order_type,
            "tif": order_pb.tif if order_pb.HasField("tif") else None,
        }

    async def _apply_diff(
        self, coin: str, diff_pb: Any, recv_ts_ms: float, recv_mono_ns: int
    ) -> None:
        """One per-block diff: rebuild the node envelope and reuse the parser."""
        try:
            data = json.loads(diff_pb.data)
        except json.JSONDecodeError as exc:
            if self.strict:
                raise UnrecognizedDiffFormat(
                    f"L4BookDiff.data is not JSON ({exc.msg})"
                ) from exc
            self._count_skip(diff_pb.data)
            return
        envelope = {"time": diff_pb.time, "height": diff_pb.height, "data": data}
        parsed = parse_obj(envelope, strict=self.strict, on_skip=self._count_skip)
        if not isinstance(parsed, BlockDiffBatch):
            self._count_skip(json.dumps(envelope))
            return

        book = self._book(coin)
        book.apply_block(parsed)  # foreign-coin diffs surface as anomalies
        self.blocks += 1
        self.diffs += len(parsed.diffs)
        if not parsed.diffs:
            return  # quiet block: height/clock advanced, nothing to report

        await self._dispatch(
            MarketDataPoint(
                timestamp=datetime.fromtimestamp(
                    parsed.time_ms / 1000, tz=timezone.utc
                ),
                symbol=coin,
                data_type="book_diff",
                data={
                    "height": parsed.height,
                    "time_ms": parsed.time_ms,
                    "n_diffs": len(parsed.diffs),
                    "best_bid": book.best_bid(),
                    "best_ask": book.best_ask(),
                    "mid": book.mid(),
                    "crossed": book.is_crossed(),
                    "stale": book.stale,
                },
                recv_ts_ms=recv_ts_ms,
                recv_mono_ns=recv_mono_ns,
            )
        )

    # --- live plumbing ---------------------------------------------------------

    async def stream(self) -> None:
        """Subscribe StreamL4Book for every coin until stop(); reconnects with
        the collector's full-jitter backoff. Requires endpoint (and usually
        token) to be configured."""
        if not self.endpoint:
            raise ValueError(
                "no QuickNode endpoint configured (settings.quicknode_grpc_endpoint)"
            )
        self._running = True
        tasks = [
            asyncio.create_task(self._stream_coin(coin)) for coin in self.coins
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            self._running = False

    async def _stream_coin(self, coin: str) -> None:
        import grpc  # deferred: the offline core never needs the runtime

        from ._qn_pb import orderbook_pb2_grpc

        consecutive_failures = 0
        while self._running:
            try:
                async with grpc.aio.secure_channel(
                    self.endpoint, grpc.ssl_channel_credentials()
                ) as channel:
                    stub = orderbook_pb2_grpc.OrderBookStreamingStub(channel)
                    call = stub.StreamL4Book(
                        orderbook_pb2.L4BookRequest(coin=coin),
                        metadata=self._metadata(),
                    )
                    consecutive_failures = 0  # connected; streak resets
                    await self._consume_stream(coin, call)
                self.logger.warning(f"{coin}: L4 stream ended; resubscribing")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger.warning(f"{coin}: L4 stream error: {e}")
            if not self._running:
                break
            consecutive_failures += 1
            self.reconnects += 1
            await asyncio.sleep(self._next_reconnect_delay(consecutive_failures))

    def stop(self) -> None:
        """Ask the per-coin stream loops to exit after their current cycle."""
        self._running = False

    def _metadata(self) -> tuple:
        # VERIFY-ON-REAL-DATA: x-token is the QuickNode gRPC convention;
        # confirm against the header l4_validate.py used once the plan is live.
        return (("x-token", self.token),) if self.token else ()

    def _next_reconnect_delay(self, consecutive_failures: int) -> float:
        """Full-jitter exponential backoff, same convention as the collector:
        uniform(0, min(cap, base * 2^(n-1)))."""
        base = float(settings.websocket_reconnect_delay)
        cap = float(settings.websocket_reconnect_max_delay)
        bound = min(cap, base * (2 ** max(0, consecutive_failures - 1)))
        return random.uniform(0, bound)

    # --- introspection -----------------------------------------------------------

    def _book(self, coin: str) -> L4Book:
        book = self.books.get(coin)
        if book is None:
            book = self.books[coin] = L4Book(coin)
        return book

    def _count_skip(self, detail: str) -> None:
        self.parse_skips += 1
        if self.parse_skips % 1000 == 1:
            self.logger.warning(
                f"unparseable stream content skipped: {self.parse_skips} total "
                f"(consider strict=True); last: {detail[:120]}"
            )

    def get_stats(self) -> Dict[str, Any]:
        """Feed counters for monitoring parity with the other collectors."""
        return {
            "blocks": self.blocks,
            "diffs": self.diffs,
            "parse_skips": self.parse_skips,
            "snapshots_loaded": self.snapshots_loaded,
            "anomalies_total": sum(b.anomaly_count for b in self.books.values()),
            "stale_coins": sorted(c for c, b in self.books.items() if b.stale),
            "bytes_received": self.bytes_received,
            "reconnects": self.reconnects,
            "coins": sorted(self.books),
        }
