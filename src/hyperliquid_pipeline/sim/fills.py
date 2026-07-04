"""Pure fill mechanics for the maker simulator.

Stateless, exhaustively unit-testable math QueueSim drives: given a trade,
the size resting ahead of us, and our own orders at the level — who gets how
much. The FIFO discipline is: a trade at our price consumes the real queue
ahead first, then our virtual orders in our own placement order; a trade
*through* our price fills our resting orders entirely at our price.

The contract types (Fill, QueueBound, TradeEvent, BookEvent) are LOCKED and
live in :mod:`hyperliquid_pipeline.sim.types` — the aggressor-side
convention is documented there: taker 'B' consumes ASK queues, taker 'A'
consumes BID queues.
"""

from decimal import Decimal
from typing import List, Sequence, Tuple

from hyperliquid_pipeline.book.schemas import ASK, BID


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
