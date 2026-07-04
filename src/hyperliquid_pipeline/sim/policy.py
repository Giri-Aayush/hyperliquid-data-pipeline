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
from decimal import Decimal
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


def _infer_tick(px: str) -> Decimal:
    """The smallest price increment implied by a price string's decimals.

    Hyperliquid prices are exact decimal strings; the venue's tick at this
    price level is one unit of the last decimal place ("115323.0" -> 0.1,
    "83.585" -> 0.001). Inferring per block keeps the policy coin-agnostic.
    """
    d = Decimal(px)
    return Decimal(1).scaleb(d.as_tuple().exponent)


class WidthPolicy:
    """v2: quote DEEPER than the touch so capture can clear fee + adverse.

    The v1 run proved touch-joining is structurally unprofitable here: max
    capture (half a ~0.16bps spread) is ~10x below the 1.5bps maker fee, and
    stale quotes get picked off. Quoting k ticks behind the touch fills less
    often but at prices k ticks better — the sweep (sim/sweep.py) searches
    for the width region, if any, where that trade-off clears the
    PESSIMISTIC gate net of flat 1.5bps fees.

    Skew is asymmetric, not pull-only: positive OFI (buy pressure) tightens
    the bid and backs the ask off, and vice versa. Positive funding (longs
    pay) adds a constant tilt toward short inventory.
    """

    def __init__(
        self,
        quote_size: float,
        width_ticks: int = 2,
        skew_gain: float = 1.0,
        inventory_limit: float = None,
        funding_tilt_ticks: float = 0.0,
        ofi_window_ms: int = 1000,
        ofi_warmup: int = 30,
    ):
        """Args:
        quote_size: size per side (coin units).
        width_ticks: base distance behind the touch, in venue ticks (0 = join).
        skew_gain: ticks of asymmetric shift per unit of normalized OFI
            (signal is clipped to [-3, 3] upstream).
        inventory_limit: hard band; stop bidding above +limit / offering
            below -limit. Defaults to 5x quote_size.
        funding_tilt_ticks: constant tilt toward short inventory when
            positive (bid backs off, ask tightens) — set from the measured
            funding regime.
        """
        self.quote_size = quote_size
        self.width_ticks = width_ticks
        self.skew_gain = skew_gain
        self.inventory_limit = (
            inventory_limit if inventory_limit is not None else 5 * quote_size
        )
        self.funding_tilt_ticks = funding_tilt_ticks
        self._ofi = _RollingOfi(window_ms=ofi_window_ms, warmup=ofi_warmup)

    def _desired(self, view, signal: float, inventory: float) -> Dict[str, Optional[str]]:
        desired: Dict[str, Optional[str]] = {"B": None, "A": None}
        best_bid = view.best_bid()
        best_ask = view.best_ask()
        if best_bid is None or best_ask is None or view.is_crossed():
            return desired

        # Positive signal: price likely up -> tighten bid, back off ask.
        shift = self.skew_gain * signal
        bid_off = self.width_ticks - shift + self.funding_tilt_ticks
        ask_off = self.width_ticks + shift - self.funding_tilt_ticks
        bid_off = max(0.0, bid_off)  # 0 = join; never improve into the spread
        ask_off = max(0.0, ask_off)

        bid_px = Decimal(best_bid[0])
        ask_px = Decimal(best_ask[0])
        tick = min(_infer_tick(best_bid[0]), _infer_tick(best_ask[0]))
        desired["B"] = format(bid_px - round(bid_off) * tick, "f")
        desired["A"] = format(ask_px + round(ask_off) * tick, "f")

        if inventory >= self.inventory_limit:
            desired["B"] = None
        if inventory <= -self.inventory_limit:
            desired["A"] = None
        return desired

    def on_block(self, view, inventory, open_orders, t_ms, fills) -> List[QuoteAction]:
        signal = self._ofi.update(view, t_ms)
        desired = self._desired(view, signal, inventory)

        actions: List[QuoteAction] = []
        open_by_side: Dict[str, List[Dict[str, Any]]] = {"B": [], "A": []}
        for order in open_orders:
            open_by_side.setdefault(order["side"], []).append(order)

        for side in ("B", "A"):
            want_px = desired[side]
            keep_one = False
            for order in open_by_side[side]:
                if want_px is not None and order["px"] == want_px and not keep_one:
                    keep_one = True  # resting where we want: keep queue priority
                else:
                    actions.append(QuoteAction(kind="cancel", order_id=order["order_id"]))
            if want_px is not None and not keep_one:
                actions.append(
                    QuoteAction(kind="place", side=side, px=want_px, sz=self.quote_size)
                )
        return actions


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
