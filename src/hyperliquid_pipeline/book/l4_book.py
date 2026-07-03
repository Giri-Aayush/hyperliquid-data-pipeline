"""Order-level (L4/MBO) book reconstruction with FIFO queue-position queries.

One :class:`L4Book` instance per coin. Seed it with a parsed snapshot
(:meth:`L4Book.load_snapshot`) and/or drive it with a stream of
:class:`BookDiff` / :class:`BlockDiffBatch` from ``diff_parser``; read it
through the frozen BookView protocol (``best_bid``/``best_ask``/``mid``/
``depth``/``is_crossed``/``last_update_ms``) plus the order-level extra
:meth:`queue_position`.

Robustness contract: malformed or unknown inputs never raise mid-stream —
they are recorded in ``anomalies`` and the book keeps going. A block-height
gap sets ``stale=True`` so consumers know a resync (fresh snapshot) is
needed; ``stale`` clears only on :meth:`load_snapshot`.

Structures: ``dict[oid -> ref]`` for O(1) order lookup; per side a
``SortedDict`` keyed by price (bids negated, so index 0 is always the best
level on both sides) mapping to a price level whose insertion-ordered
``dict[oid -> Decimal]`` IS the FIFO time-priority queue.
"""

import hashlib
from decimal import Decimal, InvalidOperation
from itertools import islice
from typing import Any, NamedTuple

from sortedcontainers import SortedDict

from hyperliquid_pipeline.book.schemas import (
    BID,
    BlockDiffBatch,
    BookDiff,
    L4Snapshot,
)


class _PriceLevel:
    """One price level: FIFO order queue plus a cached total size."""

    __slots__ = ("px", "orders", "total")

    def __init__(self, px: str):
        self.px = px
        self.orders: dict[int, Decimal] = {}  # oid -> size; dict order == FIFO
        self.total = Decimal(0)


class _OrderRef(NamedTuple):
    side: str
    key: Decimal  # the level key in that side's SortedDict


class L4Book:
    """Full order-by-order book for a single coin."""

    def __init__(self, coin: str | None = None):
        self.coin = coin
        self.last_update_ms: int = 0
        self.height: int | None = None
        self.stale: bool = False
        self.anomalies: list[dict] = []
        self._orders: dict[int, _OrderRef] = {}
        # Both sides iterate best-first: bid keys are negated prices.
        self._bids: SortedDict = SortedDict()
        self._asks: SortedDict = SortedDict()

    # --- feeding ----------------------------------------------------------

    def load_snapshot(self, snapshot: L4Snapshot) -> None:
        """Reset to a snapshot's state (the resync path — clears ``stale``).

        Insertion order within each price level follows the snapshot's list
        order, assumed to be price-time priority as emitted by the node.
        # VERIFY-ON-REAL-DATA
        """
        coin_mismatch = self.coin is not None and snapshot.coin != self.coin
        self.coin = snapshot.coin
        self._orders.clear()
        self._bids.clear()
        self._asks.clear()
        self.anomalies = []
        self.stale = False
        if coin_mismatch:
            self._anomaly("snapshot_coin_mismatch", detail=snapshot.coin)
        for order in snapshot.orders():
            self._insert(order.oid, order.side, order.limit_px, order.sz)
        self.height = snapshot.height
        self.last_update_ms = snapshot.time_ms

    def apply(self, diff: BookDiff, time_ms: int | None = None) -> None:
        """Apply one diff. Never raises: problems land in ``anomalies``.

        Bare event lines carry no timestamp, so ``last_update_ms`` only
        advances when ``time_ms`` is passed (``apply_block`` does).
        """
        if self.coin is None:
            self.coin = diff.coin
        elif diff.coin != self.coin:
            self._anomaly("coin_mismatch", oid=diff.oid, detail=diff.coin)
            return
        if diff.kind == "new":
            self._apply_new(diff)
        elif diff.kind == "update":
            self._apply_update(diff)
        elif diff.kind == "remove":
            self._apply_remove(diff)
        else:  # parser never produces this; guard the never-raise contract
            self._anomaly("unknown_kind", oid=diff.oid, detail=diff.kind)
        if time_ms is not None:
            # Coerced so a feed passing float wall-clock ms can't drift the
            # BookView protocol type (last_update_ms: int).
            self.last_update_ms = int(time_ms)

    def apply_block(self, batch: BlockDiffBatch) -> None:
        """Apply one block of diffs, tracking block-height continuity.

        Any non-consecutive height (gap, repeat, or rewind) marks the book
        ``stale`` — order-level state can silently diverge across a gap, so
        consumers should resync from a snapshot.
        """
        if self.height is not None and batch.height != self.height + 1:
            self.stale = True
            self._anomaly(
                "height_gap",
                detail=f"expected {self.height + 1}, got {batch.height}",
            )
        for diff in batch.diffs:
            self.apply(diff)
        self.height = batch.height
        self.last_update_ms = int(batch.time_ms)

    # --- frozen BookView read protocol (Track 1 consumes these) ------------

    def best_bid(self) -> tuple[str, float] | None:
        if not self._bids:
            return None
        level = self._bids.peekitem(0)[1]
        return (level.px, float(level.total))

    def best_ask(self) -> tuple[str, float] | None:
        if not self._asks:
            return None
        level = self._asks.peekitem(0)[1]
        return (level.px, float(level.total))

    def mid(self) -> float | None:
        if not self._bids or not self._asks:
            return None
        best_bid_px = -self._bids.peekitem(0)[0]
        best_ask_px = self._asks.peekitem(0)[0]
        return float((best_bid_px + best_ask_px) / 2)

    def depth(self, n: int) -> dict:
        """Top-``n`` levels per side, best first.

        Shape: ``{"bids": [{"px": str, "sz": float, "n": int}, ...],
        "asks": [...]}`` — ``n`` is the resting order count at the level,
        mirroring the exchange's L2 level shape.
        """
        count = max(n, 0)
        return {
            "bids": [self._level_entry(lvl) for lvl in islice(self._bids.values(), count)],
            "asks": [self._level_entry(lvl) for lvl in islice(self._asks.values(), count)],
        }

    def is_crossed(self) -> bool:
        """True when best bid >= best ask (locked or crossed)."""
        if not self._bids or not self._asks:
            return False
        return -self._bids.peekitem(0)[0] >= self._asks.peekitem(0)[0]

    # --- order-level extras -------------------------------------------------

    def queue_position(self, oid: int) -> tuple[int, float] | None:
        """FIFO position of a resting order within its price level.

        Returns ``(orders_ahead, size_ahead)`` — both improve as orders ahead
        fill/cancel — or ``None`` for an unknown oid.
        """
        ref = self._orders.get(oid)
        if ref is None:
            return None
        level = self._side(ref.side)[ref.key]
        ahead = 0
        size_ahead = Decimal(0)
        for other_oid, size in level.orders.items():
            if other_oid == oid:
                break
            ahead += 1
            size_ahead += size
        return (ahead, float(size_ahead))

    def checksum(self) -> str:
        """Deterministic digest of full book state (levels, FIFO order, sizes).

        Same input stream => same checksum; used by replay to pin determinism.
        """
        digest = hashlib.sha256()
        digest.update(f"coin={self.coin};height={self.height};".encode())
        for tag, side in (("B", self._bids), ("A", self._asks)):
            for level in side.values():
                orders = ",".join(
                    f"{oid}:{format(size, 'f')}" for oid, size in level.orders.items()
                )
                digest.update(f"{tag}|{level.px}|{orders};".encode())
        return digest.hexdigest()

    def __len__(self) -> int:
        """Number of resting orders across both sides."""
        return len(self._orders)

    # --- internals ----------------------------------------------------------

    def _side(self, side: str) -> SortedDict:
        return self._bids if side == BID else self._asks

    def _level_key(self, side: str, px: str) -> Decimal:
        price = Decimal(px)
        return -price if side == BID else price

    @staticmethod
    def _level_entry(level: _PriceLevel) -> dict:
        return {"px": level.px, "sz": float(level.total), "n": len(level.orders)}

    def _anomaly(self, kind: str, **fields: Any) -> None:
        self.anomalies.append({"type": kind, **fields})

    def _insert(self, oid: int, side: str, px: str, sz: str) -> None:
        try:
            size = Decimal(sz)
            key = self._level_key(side, px)
        except (InvalidOperation, TypeError):
            self._anomaly("bad_decimal", oid=oid, detail=f"px={px!r} sz={sz!r}")
            return
        if oid in self._orders:
            # VERIFY-ON-REAL-DATA: a re-announced oid is treated as
            # cancel+replace, which puts it at the back of the queue.
            self._anomaly("duplicate_new", oid=oid)
            self._delete(oid)
        levels = self._side(side)
        level = levels.get(key)
        if level is None:
            level = _PriceLevel(px)
            levels[key] = level
        level.orders[oid] = size
        level.total += size
        self._orders[oid] = _OrderRef(side=side, key=key)

    def _delete(self, oid: int) -> None:
        ref = self._orders.pop(oid)
        levels = self._side(ref.side)
        level = levels[ref.key]
        level.total -= level.orders.pop(oid)
        if not level.orders:
            del levels[ref.key]

    def _apply_new(self, diff: BookDiff) -> None:
        if diff.sz is None:
            self._anomaly("new_without_sz", oid=diff.oid)
            return
        self._insert(diff.oid, diff.side, diff.px, diff.sz)

    def _apply_update(self, diff: BookDiff) -> None:
        """Resize in place, preserving queue position.

        ASSUMPTION (documented per brief): size updates keep time priority —
        true for partial fills and size-downs; size-UPS may lose priority on
        the real exchange. Revisit when real node data lands.
        # VERIFY-ON-REAL-DATA
        """
        ref = self._orders.get(diff.oid)
        if ref is None:
            self._anomaly("unknown_oid_update", oid=diff.oid)
            return
        try:
            new_size = Decimal(diff.new_sz) if diff.new_sz is not None else None
        except InvalidOperation:
            new_size = None
        if new_size is None:
            self._anomaly("bad_decimal", oid=diff.oid, detail=f"newSz={diff.new_sz!r}")
            return
        level = self._side(ref.side)[ref.key]
        current = level.orders[diff.oid]
        if diff.orig_sz is not None:
            try:
                if Decimal(diff.orig_sz) != current:
                    # Feed and book disagree about the pre-update size: apply
                    # newSz anyway (feed wins) but flag the divergence.
                    self._anomaly(
                        "orig_sz_mismatch",
                        oid=diff.oid,
                        detail=f"book={format(current, 'f')} origSz={diff.orig_sz}",
                    )
            except InvalidOperation:
                self._anomaly("bad_decimal", oid=diff.oid, detail=f"origSz={diff.orig_sz!r}")
        if new_size <= 0:
            # VERIFY-ON-REAL-DATA: zero-size update treated as removal; the
            # verified feed uses an explicit "remove" variant instead.
            self._anomaly("update_to_zero", oid=diff.oid)
            self._delete(diff.oid)
            return
        level.orders[diff.oid] = new_size  # in-place: dict keeps FIFO slot
        level.total += new_size - current

    def _apply_remove(self, diff: BookDiff) -> None:
        if diff.oid not in self._orders:
            self._anomaly("unknown_oid_remove", oid=diff.oid)
            return
        self._delete(diff.oid)
