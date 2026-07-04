"""Quoting policies: the strategy surface of the maker simulator.

A policy sees the post-block world (BookView, its own inventory and open
orders, this block's fills) and returns QuoteActions; the engine applies
them after the configured latency. Policies must be causal — nothing here
may look at future events.

ReferenceOfiPolicy is the null hypothesis every fancier policy must beat:
join the touch on both sides, pull the side that strong order-flow imbalance
says is about to be run over (the OFI decile tables in the strategy memo are
exactly this adverse-selection map), and stop quoting a side once inventory
is past its band.
"""

from collections import deque
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol

from .types import Fill


@dataclass
class QuoteAction:
    """One instruction to the engine: rest a quote or pull one.

    kind 'place' uses side/px/sz; kind 'cancel' uses order_id. px is the
    exact price string (book px strings are exact; never round-trip through
    float formatting).
    """

    kind: str  # 'place' | 'cancel'
    side: str = ""  # our resting side: 'B' | 'A'
    px: Optional[str] = None
    sz: float = 0.0
    order_id: Optional[int] = None


class MakerPolicy(Protocol):
    """What the engine calls once per block."""

    def on_block(
        self,
        view: Any,
        inventory: float,
        open_orders: List[Dict[str, Any]],
        t_ms: int,
        fills: List[Fill],
    ) -> List[QuoteAction]: ...


class _RollingOfi:
    """Causal best-level OFI (Cont–Kukanov–Stoikov) summed over a window.

    Same event formula as research/ofi.py, computed incrementally from the
    BookView's best levels block by block, normalized by a slow EMA of its
    own magnitude so the skew threshold is scale-free across coins.
    """

    def __init__(self, window_ms: int = 1000, norm_halflife: int = 300, warmup: int = 30):
        self.window_ms = window_ms
        self.warmup = warmup  # no signal until the normalizer has seen this many blocks
        self._events: deque = deque()  # (t_ms, e_n)
        self._prev_bid = None  # (px_float, sz)
        self._prev_ask = None
        self._norm = 0.0
        self._updates = 0
        self._alpha = 1.0 - 0.5 ** (1.0 / norm_halflife)

    def update(self, view: Any, t_ms: int) -> float:
        """Feed one block's best levels; return the normalized windowed OFI."""
        bid = view.best_bid()
        ask = view.best_ask()
        if bid is None or ask is None:
            return 0.0
        cur_bid = (float(bid[0]), bid[1])
        cur_ask = (float(ask[0]), ask[1])

        if self._prev_bid is not None:
            pb, qb = self._prev_bid
            pa, qa = self._prev_ask
            e_n = 0.0
            if cur_bid[0] >= pb:
                e_n += cur_bid[1]
            if cur_bid[0] <= pb:
                e_n -= qb
            if cur_ask[0] <= pa:
                e_n -= cur_ask[1]
            if cur_ask[0] >= pa:
                e_n += qa
            self._events.append((t_ms, e_n))
        self._prev_bid, self._prev_ask = cur_bid, cur_ask

        while self._events and self._events[0][0] < t_ms - self.window_ms:
            self._events.popleft()
        ofi = sum(e for _, e in self._events)
        self._norm += self._alpha * (abs(ofi) - self._norm)
        self._updates += 1
        # A cold normalizer makes any first flicker read as maximum signal;
        # emit nothing until it has seen enough blocks to mean something.
        if self._updates < self.warmup or self._norm <= 0:
            return 0.0
        signal = ofi / self._norm
        return max(-3.0, min(3.0, signal))


class ReferenceOfiPolicy:
    """Join the touch; pull the side that imbalance is about to run over."""

    def __init__(
        self,
        quote_size: float,
        skew_cut: float = 1.5,
        inventory_limit: float = None,
        ofi_window_ms: int = 1000,
        ofi_warmup: int = 30,
    ):
        """Args:
        quote_size: size per side per quote (coin units).
        skew_cut: normalized-OFI magnitude beyond which the threatened side
            is pulled (buy pressure runs over resting asks and vice versa).
        inventory_limit: stop bidding above +limit, stop offering below
            -limit. Defaults to 5x quote_size.
        ofi_window_ms: OFI aggregation window (the memo's signal horizon).
        ofi_warmup: blocks before the OFI signal is trusted (normalizer warmup).
        """
        self.quote_size = quote_size
        self.skew_cut = skew_cut
        self.inventory_limit = (
            inventory_limit if inventory_limit is not None else 5 * quote_size
        )
        self._ofi = _RollingOfi(window_ms=ofi_window_ms, warmup=ofi_warmup)

    def on_block(self, view, inventory, open_orders, t_ms, fills) -> List[QuoteAction]:
        signal = self._ofi.update(view, t_ms)
        best_bid = view.best_bid()
        best_ask = view.best_ask()

        desired: Dict[str, Optional[str]] = {"B": None, "A": None}
        if best_bid is not None and best_ask is not None and not view.is_crossed():
            desired["B"] = best_bid[0]
            desired["A"] = best_ask[0]
            # Strong buy pressure runs over resting asks; sell pressure over bids.
            if signal >= self.skew_cut:
                desired["A"] = None
            elif signal <= -self.skew_cut:
                desired["B"] = None
            # Inventory bands trump the signal.
            if inventory >= self.inventory_limit:
                desired["B"] = None
            if inventory <= -self.inventory_limit:
                desired["A"] = None

        actions: List[QuoteAction] = []
        open_by_side: Dict[str, List[Dict[str, Any]]] = {"B": [], "A": []}
        for order in open_orders:
            open_by_side.setdefault(order["side"], []).append(order)

        for side in ("B", "A"):
            want_px = desired[side]
            keep_one = False
            for order in open_by_side[side]:
                if want_px is not None and order["px"] == want_px and not keep_one:
                    keep_one = True  # already resting where we want: keep priority
                else:
                    actions.append(QuoteAction(kind="cancel", order_id=order["order_id"]))
            if want_px is not None and not keep_one:
                actions.append(
                    QuoteAction(kind="place", side=side, px=want_px, sz=self.quote_size)
                )
        return actions
