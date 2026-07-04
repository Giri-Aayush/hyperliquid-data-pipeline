"""QueueSim: virtual maker orders overlaid on a replayed book, per coin.

One instance simulates ONE coin under ONE queue bound (see
:class:`~hyperliquid_pipeline.sim.fills.QueueBound`); the engine runs the
bounds x latency grid as independent passes, because fills diverge across
bounds and therefore so do inventory and policy behavior.

Internal model: each price level we quote at is a merged FIFO of segments —
real volume (anonymous in L2 mode, per-oid in EXACT mode) interleaved with
our own orders in placement order. Trades consume the sequence from the
front; the three L2 cancel assumptions are just different removal orders on
the same structure (behind-first = pessimistic, front-first = optimistic,
proportional = pro-rata), and the reality clamp (you can never have more
ahead of you than the level holds) falls out for free.

Contract (frozen, v1.1 design):
* the engine delivers on_trade for all of block N's trades BEFORE
  on_book(N); trades between on_book(N-1) and on_book(N) belong to block N;
* place()/cancel() are only called between blocks, after book delivery —
  latency lives entirely in the engine, QueueSim executes immediately;
* EXACT mode maintains queue-ahead from block diffs only; trades compute OUR
  fills against the pre-block queue (no double count by construction);
* on_book returns fills for contract stability but yields [] in v1 — a book
  crossing our price without a print is counted (crossed_unfilled), never
  filled.
"""

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from hyperliquid_pipeline.book.schemas import ASK, BID, BlockDiffBatch, BookView, normalize_side
from hyperliquid_pipeline.sim.fills import (
    resting_side_hit,
    trade_reaches,
    trade_through,
)
from hyperliquid_pipeline.sim.types import Fill, QueueBound

_EPS = 1e-12
_DEPTH_ALL = 10_000  # depth(n) big enough to always mean "every level"


class _Segment:
    """One slice of a level's FIFO: real volume or one of our orders."""

    __slots__ = ("kind", "sz", "oid")

    def __init__(self, kind: str, sz: float, oid: Optional[int] = None):
        self.kind = kind  # 'real' | 'own'
        self.sz = sz
        self.oid = oid  # real order id (EXACT) or our order_id ('own')


class _Level:
    """Tracked state for one (side, px) we are quoting at."""

    __slots__ = ("px", "segments", "consumed_in_block")

    def __init__(self, px: str):
        self.px = px
        self.segments: List[_Segment] = []  # FIFO: front of queue first
        # EXACT mode: real volume consumed by this block's trades so far —
        # the block's diffs embody that consumption, so it resets at on_book.
        self.consumed_in_block = 0.0

    def real_size(self) -> float:
        return sum(s.sz for s in self.segments if s.kind == "real")

    def own_segments(self) -> List[_Segment]:
        return [s for s in self.segments if s.kind == "own"]


class QueueSim:
    """Virtual order queue simulation for a single coin under one bound."""

    def __init__(self, coin: str, bound: QueueBound):
        self.coin = coin
        self.bound = QueueBound(bound)
        self._exact = self.bound is QueueBound.EXACT
        self._next_order_id = 1
        # order_id -> (side, px key); the segment holds the remaining size.
        self._registry: Dict[int, Tuple[str, Decimal]] = {}
        self._levels: Dict[Tuple[str, Decimal], _Level] = {}
        self._view: Optional[BookView] = None
        self._last_height: Optional[int] = None

        self.fills_count = 0
        self.filled_volume = 0.0
        self.stale_evictions = 0
        self.crossed_unfilled = 0
        self.untracked_diffs = 0
        self.placed_blind = 0

    # --- actions (engine-delayed; executed immediately here) ----------------

    def place(self, side: str, px: str, sz: float, t_ms: int) -> int:
        """Rest a virtual order; queue-ahead snapshots the as-delivered book.

        Joining an untracked level seeds its real FIFO from the current view:
        per-oid in EXACT mode (L4Book.level_orders), one anonymous segment of
        the visible size in L2 mode. Joining a level we already quote at just
        appends behind our earlier orders — interleaved real joins are
        already tracked.
        """
        side = normalize_side(side)
        key = (side, Decimal(px))
        level = self._levels.get(key)
        if level is None:
            level = _Level(px)
            self._seed_real(level, side, px)
            self._levels[key] = level
        order_id = self._next_order_id
        self._next_order_id += 1
        level.segments.append(_Segment("own", float(sz), order_id))
        self._registry[order_id] = key
        return order_id

    def cancel(self, order_id: int, t_ms: int, reason: str = "policy") -> bool:
        """Pull a virtual order; False when it is already gone (filled).

        ``reason='stale'`` is the engine's height-gap eviction path and is
        counted in ``stale_evictions``.
        """
        key = self._registry.pop(order_id, None)
        if key is None:
            return False
        level = self._levels[key]
        level.segments = [
            s for s in level.segments if not (s.kind == "own" and s.oid == order_id)
        ]
        self._drop_level_if_unquoted(key)
        if reason == "stale":
            self.stale_evictions += 1
        return True

    # --- event flow -----------------------------------------------------------

    def on_trade(self, trade: Any) -> List[Fill]:
        """A trade print: consume queues front-first, fill us where reached."""
        if trade.coin != self.coin:
            return []
        resting = resting_side_hit(normalize_side(trade.side))
        trade_px = Decimal(trade.px)
        fills: List[Fill] = []
        # Best-first so through-fills (which executed earlier in reality)
        # are emitted before the at-price partial.
        for side, level_px in self._levels_best_first(resting):
            if not trade_reaches(resting, level_px, trade_px):
                continue
            level = self._levels[(side, level_px)]
            if trade_through(resting, level_px, trade_px):
                fills.extend(self._fill_through(level, side, trade))
            else:
                fills.extend(self._fill_at_price(level, side, trade))
            self._drop_level_if_unquoted((side, level_px))
        return fills

    def on_book(
        self,
        view: BookView,
        batch: Optional[BlockDiffBatch],
        t_ms: int,
    ) -> List[Fill]:
        """A block boundary: refresh real-queue state under this sim's bound.

        Returns [] in v1 by design — see the module docstring.
        """
        self._view = view
        if batch is not None:
            self._last_height = batch.height
        if self._exact and batch is not None:
            self._apply_exact_diffs(batch)
        elif not self._exact:
            self._reconcile_l2_levels(view)
        self._count_crossed(view)
        return []

    # --- reads ------------------------------------------------------------------

    def queue_ahead(self, order_id: int) -> Optional[float]:
        """Real size still ahead of one of our orders; None if unknown."""
        key = self._registry.get(order_id)
        if key is None:
            return None
        level = self._levels[key]
        ahead = 0.0
        budget = level.consumed_in_block  # EXACT: this block's trades already
        for segment in level.segments:  # walked past this much real volume
            if segment.kind == "own" and segment.oid == order_id:
                return max(0.0, ahead)
            if segment.kind == "real":
                take = min(segment.sz, budget)
                budget -= take
                ahead += segment.sz - take
        return None  # registry said it exists but no segment: defensive

    def open_orders(self) -> List[Dict[str, Any]]:
        out = []
        for order_id, (side, px_key) in sorted(self._registry.items()):
            level = self._levels[(side, px_key)]
            remaining = sum(
                s.sz for s in level.segments if s.kind == "own" and s.oid == order_id
            )
            out.append(
                {
                    "order_id": order_id,
                    "side": side,
                    "px": level.px,
                    "sz": remaining,
                    "queue_ahead": self.queue_ahead(order_id),
                }
            )
        return out

    def get_stats(self) -> Dict[str, Any]:
        return {
            "coin": self.coin,
            "bound": self.bound.value,
            "open_orders": len(self._registry),
            "fills": self.fills_count,
            "filled_volume": self.filled_volume,
            "stale_evictions": self.stale_evictions,
            "crossed_unfilled": self.crossed_unfilled,
            "untracked_diffs": self.untracked_diffs,
            "placed_blind": self.placed_blind,
        }

    # --- internals: seeding ------------------------------------------------------

    def _seed_real(self, level: _Level, side: str, px: str) -> None:
        if self._view is None:
            self.placed_blind += 1  # engine placed before any book: empty level
            return
        if self._exact:
            level_orders = getattr(self._view, "level_orders", None)
            if level_orders is None:  # EXACT bound demands an L4 view
                self.placed_blind += 1
                return
            for oid, sz in level_orders(side, px):
                level.segments.append(_Segment("real", sz, oid))
        else:
            size = self._depth_map(side).get(Decimal(px), 0.0)
            if size > 0:
                level.segments.append(_Segment("real", size))

    def _depth_map(self, side: str) -> Dict[Decimal, float]:
        depth = self._view.depth(_DEPTH_ALL)
        rows = depth["bids"] if side == BID else depth["asks"]
        return {Decimal(row["px"]): float(row["sz"]) for row in rows}

    # --- internals: fills -----------------------------------------------------------

    def _levels_best_first(self, side: str) -> List[Tuple[str, Decimal]]:
        keys = [key for key in self._levels if key[0] == side]
        return sorted(keys, key=lambda k: -k[1] if side == BID else k[1])

    def _fill_through(self, level: _Level, side: str, trade: Any) -> List[Fill]:
        """Price printed through our level: everything of ours there fills."""
        fills = []
        ahead_at_fill = self._real_ahead_of_first_own(level)
        for segment in level.own_segments():
            if segment.sz > _EPS:
                fills.append(self._emit(segment.oid, side, level, segment.sz, trade, ahead_at_fill))
                segment.sz = 0.0
        self._sweep_filled(level)
        return fills

    def _fill_at_price(self, level: _Level, side: str, trade: Any) -> List[Fill]:
        """Trade at exactly our price: walk the merged FIFO from the front."""
        budget = float(trade.sz)
        skip = level.consumed_in_block if self._exact else 0.0
        fills: List[Fill] = []
        real_ahead_walked = 0.0
        for segment in level.segments:
            if budget <= _EPS:
                break
            if segment.kind == "real":
                available = segment.sz
                if self._exact:
                    already = min(available, skip)
                    skip -= already
                    available -= already
                take = min(budget, available)
                if take > 0:
                    if self._exact:
                        level.consumed_in_block += take  # diffs settle it later
                    else:
                        segment.sz -= take
                    budget -= take
                real_ahead_walked += available - take
            else:
                take = min(budget, segment.sz)
                if take > _EPS:
                    fills.append(
                        self._emit(segment.oid, side, level, take, trade, real_ahead_walked)
                    )
                    segment.sz -= take
                    budget -= take
        self._sweep_filled(level)
        return fills

    def _real_ahead_of_first_own(self, level: _Level) -> float:
        ahead = 0.0
        budget = level.consumed_in_block if self._exact else 0.0
        for segment in level.segments:
            if segment.kind == "own":
                return max(0.0, ahead)
            take = min(segment.sz, budget)
            budget -= take
            ahead += segment.sz - take
        return max(0.0, ahead)

    def _emit(
        self,
        order_id: int,
        side: str,
        level: _Level,
        sz: float,
        trade: Any,
        queue_ahead_at_fill: float,
    ) -> Fill:
        self.fills_count += 1
        self.filled_volume += sz
        return Fill(
            order_id=order_id,
            coin=self.coin,
            side=side,
            px=level.px,
            sz=sz,
            t_ms=trade.t_ms,
            # Trades between on_book(N-1) and on_book(N) belong to block N.
            height=(self._last_height + 1) if self._last_height is not None else None,
            queue_bound=self.bound.value,
            queue_ahead_at_fill=queue_ahead_at_fill,
            mid_at_fill=self._view.mid() if self._view is not None else None,
        )

    def _sweep_filled(self, level: _Level) -> None:
        emptied = [
            s.oid for s in level.segments if s.kind == "own" and s.sz <= _EPS
        ]
        for order_id in emptied:
            self._registry.pop(order_id, None)
        level.segments = [
            s
            for s in level.segments
            if s.sz > _EPS or (s.kind == "real" and self._exact)
        ]
        # EXACT keeps zero-size real segments? No: they only reach zero via
        # diff removal, which deletes them outright — the filter above keeps
        # real segments solely because trades never mutate them in EXACT.

    def _drop_level_if_unquoted(self, key: Tuple[str, Decimal]) -> None:
        level = self._levels.get(key)
        if level is not None and not level.own_segments():
            del self._levels[key]  # stop tracking real state we don't need

    # --- internals: book deltas --------------------------------------------------------

    def _apply_exact_diffs(self, batch: BlockDiffBatch) -> None:
        for level in self._levels.values():
            level.consumed_in_block = 0.0  # the diffs below settle the trades
        for diff in batch.diffs:
            try:
                key = (diff.side, Decimal(diff.px))
            except InvalidOperation:
                continue
            level = self._levels.get(key)
            if level is None:
                continue
            if diff.kind == "new":
                level.segments.append(
                    _Segment("real", float(diff.sz or 0.0), diff.oid)
                )
                continue
            target = next(
                (
                    s
                    for s in level.segments
                    if s.kind == "real" and s.oid == diff.oid
                ),
                None,
            )
            if target is None:
                self.untracked_diffs += 1
                continue
            if diff.kind == "remove":
                level.segments.remove(target)
            elif diff.kind == "update":
                # In-place resize keeps the slot, same assumption as L4Book.
                target.sz = float(diff.new_sz or 0.0)
                if target.sz <= _EPS:
                    level.segments.remove(target)

    def _reconcile_l2_levels(self, view: BookView) -> None:
        depth = {BID: self._depth_map(BID), ASK: self._depth_map(ASK)}
        for (side, px_key), level in self._levels.items():
            real_now = depth[side].get(px_key, 0.0)
            real_tracked = level.real_size()
            delta = real_now - real_tracked
            if delta > _EPS:
                # Net growth joined after everyone present: back of the queue.
                level.segments.append(_Segment("real", delta))
            elif delta < -_EPS:
                self._remove_real(level, -delta)

    def _remove_real(self, level: _Level, volume: float) -> None:
        """Remove canceled real volume per this sim's bound.

        Behind-first (pessimistic) / front-first (optimistic) / proportional
        (pro-rata). Behind-first naturally overflows into volume ahead once
        everything behind is gone — the reality clamp: our queue-ahead can
        never exceed what the level visibly holds.
        """
        real_segments = [s for s in level.segments if s.kind == "real"]
        if not real_segments:
            return
        if self.bound is QueueBound.PRORATA:
            total = sum(s.sz for s in real_segments)
            if total <= _EPS:
                return
            scale = max(0.0, 1.0 - volume / total)
            for segment in real_segments:
                segment.sz *= scale
        else:
            ordered = (
                list(reversed(real_segments))
                if self.bound is QueueBound.PESSIMISTIC
                else real_segments
            )
            remaining = volume
            for segment in ordered:
                take = min(segment.sz, remaining)
                segment.sz -= take
                remaining -= take
                if remaining <= _EPS:
                    break
        level.segments = [
            s for s in level.segments if s.kind == "own" or s.sz > _EPS
        ]

    def _count_crossed(self, view: BookView) -> None:
        best_bid = view.best_bid()
        best_ask = view.best_ask()
        for side, px_key in self._registry.values():
            crossed = (
                side == BID
                and best_ask is not None
                and px_key >= Decimal(best_ask[0])
            ) or (
                side == ASK
                and best_bid is not None
                and px_key <= Decimal(best_bid[0])
            )
            if crossed:
                self.crossed_unfilled += 1  # no print, no fill — but visible
