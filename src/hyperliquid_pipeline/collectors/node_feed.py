"""Node raw-book-diff feed: drives per-coin L4 books from node output files.

The node (``--write-raw-book-diffs``) appends one JSON document per line to
hourly files; this feed turns those into live :class:`L4Book` state plus
:class:`MarketDataPoint` callbacks, so downstream consumers get order-level
books through the same callback pattern the WebSocket collector uses.

Two drive modes:

* offline — :meth:`NodeDiffFeed.replay_files` consumes recorded files in
  order (plain or ``.lz4``);
* live — ``await`` :meth:`NodeDiffFeed.tail` follows the newest hourly file
  under ``data_dir``, surviving a missing directory (the node may not have
  started yet) and rolling to each new hour's file as it appears.

Height continuity: block heights are chain-global, so EVERY tracked book
receives every block via ``apply_block`` — with an empty diff list when the
coin is absent from that block. A coin that goes quiet for a few blocks
therefore never gets false-flagged stale; ``stale`` only fires on real gaps
in the file stream. MarketDataPoints are still emitted only for coins that
actually had diffs in the block.
"""

import asyncio
import inspect
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from loguru import logger

from ..book.diff_parser import iter_diff_file, parse_line
from ..book.l4_book import L4Book
from ..book.schemas import BlockDiffBatch, BookDiff
from ..config import settings
from .realtime_collector import MarketDataPoint


class NodeDiffFeed:
    """Order-level market state from a node's raw-book-diff output."""

    def __init__(
        self,
        data_dir: Optional[str] = None,
        coins: Optional[List[str]] = None,
        strict: bool = False,
        poll_interval: float = 0.25,
    ):
        """Initialize the feed.

        Args:
            data_dir: Root of the node's book-diff output (defaults to
                settings.node_data_dir). Only needed for tail().
            coins: Coins to track; None tracks every coin seen.
            strict: Passed through to the parser — strict mode raises
                UnrecognizedDiffFormat on any format drift instead of
                skipping (use on the first hour of real node data).
            poll_interval: Seconds between checks for appended data in tail().
        """
        raw_dir = data_dir if data_dir is not None else (settings.node_data_dir or None)
        self.data_dir: Optional[Path] = (
            Path(raw_dir).expanduser() if raw_dir else None
        )
        self.coins = list(coins) if coins else None
        self.strict = strict
        self.poll_interval = poll_interval
        self.logger = logger.bind(component="node_feed")

        # One L4Book per coin, read-only for consumers (BookView protocol).
        self.books: Dict[str, L4Book] = {}
        self.data_callbacks: List[Callable[[MarketDataPoint], None]] = []

        self.blocks = 0
        self.diffs = 0
        self.parse_skips = 0  # non-blank lines the tolerant parser dropped
        self.current_file: Optional[str] = None
        self._running = False

    # --- callbacks (same contract as HyperliquidWebSocketCollector) --------

    def add_data_callback(self, callback: Callable[[MarketDataPoint], None]):
        """Add a callback for emitted points; sync or async, run serially.

        Identical semantics to the WebSocket collector: an awaitable return
        is awaited, a failing callback is logged and never stops the feed.
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

    # --- offline ------------------------------------------------------------

    def replay_files(self, paths: Iterable[str | Path]) -> None:
        """Consume recorded diff files in order, driving books and callbacks.

        Synchronous entry point — call it from non-async code (it runs its
        own event loop so async callbacks get the same serial-await treatment
        as in tail()).
        """
        asyncio.run(self._replay_files(paths))

    async def _replay_files(self, paths: Iterable[str | Path]) -> None:
        for path in paths:
            self.current_file = str(path)
            for item in iter_diff_file(
                path, strict=self.strict, on_skip=self._count_skip
            ):
                # Stamped when the line was read — offline that is now.
                await self._process_item(
                    item, recv_ts_ms=time.time() * 1000,
                    recv_mono_ns=time.monotonic_ns(),
                )

    # --- live ---------------------------------------------------------------

    async def tail(self) -> None:
        """Follow the newest hourly file under data_dir until stop().

        The node appends one document per line and starts a new file each
        hour; "newest" is picked by mtime so the file currently being written
        wins. A missing/empty directory is logged once and retried forever —
        the node may simply not be up yet.  # VERIFY-ON-REAL-DATA (layout:
        {data_dir}/hourly/{date}/{hour}, plain text while live)
        """
        self._running = True
        handle = None
        buffer = ""
        warned = False
        try:
            while self._running:
                latest = self._latest_file()
                if latest is None:
                    if not warned:
                        self.logger.warning(
                            f"node data dir missing or empty: {self.data_dir}; retrying"
                        )
                        warned = True
                    await asyncio.sleep(self.poll_interval)
                    continue
                warned = False

                if handle is not None and self.current_file != str(latest):
                    # Hour rolled: drain what's left of the old file first.
                    buffer = await self._consume_chunk(handle.read(), buffer)
                    if buffer.strip():
                        await self._consume_chunk("\n", buffer)
                    handle.close()
                    handle = None
                    buffer = ""
                if handle is None:
                    handle = open(latest, "r")
                    self.current_file = str(latest)

                chunk = handle.read()  # whatever got appended since last read
                if not chunk:
                    await asyncio.sleep(self.poll_interval)
                    continue
                buffer = await self._consume_chunk(chunk, buffer)
        finally:
            if handle is not None:
                handle.close()
            self._running = False

    def stop(self) -> None:
        """Ask tail() to exit after its current cycle."""
        self._running = False

    async def _consume_chunk(self, chunk: str, buffer: str) -> str:
        """Process complete lines from buffer+chunk; return the partial tail.

        The node may be mid-append, so only newline-terminated lines are
        parsed; the trailing fragment waits for its newline.
        """
        recv_ts_ms = time.time() * 1000
        recv_mono_ns = time.monotonic_ns()
        buffer += chunk
        *lines, buffer = buffer.split("\n")
        for line in lines:
            if not line.strip():
                continue
            item = parse_line(line, strict=self.strict, on_skip=self._count_skip)
            if item is None:
                self._count_skip(line)
                continue
            await self._process_item(item, recv_ts_ms, recv_mono_ns)
        return buffer

    def _latest_file(self) -> Optional[Path]:
        """Newest file under data_dir (hourly/ preferred), by mtime."""
        if self.data_dir is None:
            return None
        root = self.data_dir / "hourly"
        base = root if root.is_dir() else self.data_dir
        if not base.is_dir():
            return None
        newest: Optional[Path] = None
        newest_mtime = -1.0
        for path in base.rglob("*"):
            try:
                if not path.is_file() or path.name.startswith("."):
                    continue
                mtime = path.stat().st_mtime
            except OSError:  # rotated/removed mid-scan
                continue
            if mtime > newest_mtime:
                newest, newest_mtime = path, mtime
        return newest

    # --- core ---------------------------------------------------------------

    def _count_skip(self, line: str) -> None:
        self.parse_skips += 1
        if self.parse_skips % 1000 == 1:
            self.logger.warning(
                f"parser skipped unrecognized line(s): {self.parse_skips} total "
                f"(a skipped remove leaves a phantom order — consider strict=True)"
            )

    def _allowed(self, coin: str) -> bool:
        return self.coins is None or coin in self.coins

    def _book(self, coin: str) -> L4Book:
        book = self.books.get(coin)
        if book is None:
            book = self.books[coin] = L4Book(coin)
        return book

    async def _process_item(
        self,
        item: BookDiff | BlockDiffBatch,
        recv_ts_ms: float,
        recv_mono_ns: int,
    ) -> None:
        if isinstance(item, BookDiff):
            # Bare event lines carry no exchange timestamp (see L4Book.apply);
            # book state advances, no point is emitted (points are per block).
            if self._allowed(item.coin):
                self._book(item.coin).apply(item)
                self.diffs += 1
            return

        self.blocks += 1
        per_coin: Dict[str, List[BookDiff]] = {}
        for diff in item.diffs:
            if self._allowed(diff.coin):
                per_coin.setdefault(diff.coin, []).append(diff)

        # Every tracked book sees every block (empty batch when absent) so
        # chain-global heights stay consecutive per book — see module note.
        for coin in set(self.books) | set(per_coin):
            self._book(coin).apply_block(
                BlockDiffBatch(
                    time_ms=item.time_ms,
                    height=item.height,
                    diffs=per_coin.get(coin, []),
                )
            )
        self.diffs += sum(len(diffs) for diffs in per_coin.values())

        for coin, coin_diffs in per_coin.items():
            book = self.books[coin]
            await self._dispatch(
                MarketDataPoint(
                    timestamp=datetime.fromtimestamp(
                        item.time_ms / 1000, tz=timezone.utc
                    ),
                    symbol=coin,
                    data_type="book_diff",
                    data={
                        "height": item.height,
                        "time_ms": item.time_ms,
                        "n_diffs": len(coin_diffs),
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

    # --- introspection --------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Feed counters for monitoring/bench parity with the collector."""
        return {
            "blocks": self.blocks,
            "diffs": self.diffs,
            "parse_skips": self.parse_skips,
            "anomalies_total": sum(b.anomaly_count for b in self.books.values()),
            "stale_coins": sorted(c for c, b in self.books.items() if b.stale),
            "current_file": self.current_file,
        }

    # --- future: order_book_server websocket mode -----------------------------

    def connect_l4_ws(self) -> None:
        """L4 order-book websocket mode — activates once a local
        order_book_server exists next to the node.

        The subscribe payload for that server is:
            {"method": "subscribe", "subscription": {"type": "l4Book", "coin": <coin>}}
        and each message carries L4 snapshots/updates that would feed
        load_snapshot/apply on the same books.  # VERIFY-ON-REAL-DATA
        """
        raise NotImplementedError(
            "l4Book websocket mode needs a local order_book_server; "
            'subscribe payload: {"method": "subscribe", '
            '"subscription": {"type": "l4Book", "coin": ...}}'
        )
