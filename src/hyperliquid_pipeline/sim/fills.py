"""Fill mechanics for the maker simulator: the frozen Fill record and the
pure allocation math QueueSim drives.

Everything here is stateless and exhaustively unit-testable: given a trade,
the size resting ahead of us, and our own orders at the level, who gets how
much. The FIFO discipline is: a trade at our price consumes the real queue
ahead first, then our virtual orders in our own placement order; a trade
*through* our price fills our resting orders entirely at our price.

Aggressor-side convention (frozen, from the Hyperliquid ws trade feed):
``TradeEvent.side`` is the TAKER's side — ``'B'`` (taker buys) consumes ASK
queues at or below the trade price, ``'A'`` (taker sells) consumes BID
queues at or above it. Inverting this silently flips every fill; the tests
pin both directions.
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Sequence, Tuple

from hyperliquid_pipeline.book.schemas import ASK, BID


class QueueBound(str, Enum):
    """Cancel-location assumption for L2-estimated queue positions.

    L2 data shows level sizes, not order identity, so cancels ahead of us are
    unobservable. PESSIMISTIC assumes cancels land behind us (floor on our
    progress), OPTIMISTIC assumes they all land ahead (ceiling — what the
    draft called "front-loaded"), PRORATA draws them uniformly from the level
    (central estimate, NOT a bound). EXACT is L4 ground truth: no assumption.
    The decision rule for any policy: it must clear PESSIMISTIC.
    """

    PESSIMISTIC = "pessimistic"
    PRORATA = "prorata"
    OPTIMISTIC = "optimistic"
    EXACT = "exact"


@dataclass(frozen=True, slots=True)
class Fill:
    """One (partial) execution of a virtual maker order. FROZEN contract.

    ``height`` follows the block-attribution rule: trades delivered between
    on_book(N-1) and on_book(N) belong to block N, so it is last seen height
    + 1 (None before any block is seen or in height-less L2 replays).
    ``queue_ahead_at_fill`` is the real size still ahead when this trade
    started consuming the level — the fill-quality number queue modeling
    exists to produce.
    """

    order_id: int
    coin: str
    side: str  # OUR resting side: 'B' bid / 'A' ask
    px: str
    sz: float
    t_ms: int
    height: Optional[int]
    queue_bound: str
    queue_ahead_at_fill: float
    mid_at_fill: Optional[float]
    maker: bool = True  # v1 is post-only: every fill is a maker fill


def resting_side_hit(aggressor_side: str) -> str:
    """Which side of the book an aggressor consumes: taker buy hits asks."""
    return ASK if aggressor_side == BID else BID


def trade_reaches(
    resting_side: str, resting_px: Decimal, trade_px: Decimal
) -> bool:
    """Does a trade at ``trade_px`` touch a resting order at ``resting_px``?

    A taker sell prints downward: it consumes bids priced at or above the
    print. A taker buy consumes asks priced at or below it.
    """
    if resting_side == BID:
        return resting_px >= trade_px
    return resting_px <= trade_px


def trade_through(
    resting_side: str, resting_px: Decimal, trade_px: Decimal
) -> bool:
    """Did the trade print at a strictly worse price than our level?

    Then the venue's price-time priority guarantees every order at our level
    executed before that print — our order fills fully regardless of queue.
    """
    if resting_side == BID:
        return trade_px < resting_px
    return trade_px > resting_px


def allocate_at_level(
    trade_sz: float,
    queue_ahead: float,
    own_orders: Sequence[Tuple[int, float]],
) -> Tuple[float, List[Tuple[int, float]]]:
    """Split one trade's size at our price level, FIFO.

    ``own_orders`` are our (order_id, remaining_sz) in placement order.
    Returns ``(ahead_consumed, [(order_id, fill_sz), ...])``. Whatever the
    trade doesn't consume from the queue ahead flows into our orders in
    order; size beyond our orders hits the real queue behind us (not our
    concern).
    """
    if trade_sz <= 0:
        return 0.0, []
    ahead_consumed = min(trade_sz, max(queue_ahead, 0.0))
    remaining = trade_sz - ahead_consumed
    fills: List[Tuple[int, float]] = []
    for order_id, order_sz in own_orders:
        if remaining <= 0:
            break
        take = min(remaining, order_sz)
        if take > 0:
            fills.append((order_id, take))
            remaining -= take
    return ahead_consumed, fills
