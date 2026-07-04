"""End-to-end: both halves of the simulator together on a synthetic tape.

Real QueueSim + real Engine + a scripted quote: our bid joins the touch, an
aggressive sell prints through the queue, and we get filled with correct
ledger math — the whole contract exercised once, no fakes.
"""

import asyncio

from hyperliquid_pipeline.book import L2Book
from hyperliquid_pipeline.sim.engine import Engine, EngineConfig
from hyperliquid_pipeline.sim.policy import QuoteAction
from hyperliquid_pipeline.sim.queue import QueueSim
from hyperliquid_pipeline.sim.report import decompose
from hyperliquid_pipeline.sim.types import BookEvent, QueueBound, TradeEvent

T0 = 1_700_000_000_000


class _OneShotBid:
    def __init__(self, px, sz):
        self._actions = [QuoteAction(kind="place", side="B", px=px, sz=sz)]

    def on_block(self, view, inventory, open_orders, t_ms, fills):
        actions, self._actions = self._actions, []
        return actions


def _snapshot_event(book, t_ms, bid_sz):
    book.update_from_snapshot(
        [{"px": "100", "sz": str(bid_sz), "n": 2}],
        [{"px": "101", "sz": "1.0", "n": 1}],
        t_ms,
    )
    return BookEvent(coin="BTC", t_ms=t_ms, height=None, view=book, batch=None)


def _run(bound):
    book = L2Book(coin="BTC")
    sim = QueueSim("BTC", bound)
    engine = Engine(sim, _OneShotBid("100", 1.0), EngineConfig(
        submit_delay_ms=400, maker_fee_bps=1.5,
    ))

    def events():
        # Lazily, like the real sources: the shared view mutates just before
        # each yield (the contract: consume in order, don't retain).
        yield _snapshot_event(book, T0, bid_sz=2.0)          # policy submits bid
        yield _snapshot_event(book, T0 + 1000, bid_sz=2.0)   # bid rests (400ms)
        # sell aggressor prints 2.5 at our price: 2.0 real ahead, then us
        yield TradeEvent(coin="BTC", t_ms=T0 + 1500, px="100", sz=2.5, side="A")
        yield _snapshot_event(book, T0 + 2000, bid_sz=0.5)
        yield _snapshot_event(book, T0 + 3000, bid_sz=0.5)

    return engine.run(events())


def test_full_stack_fill_and_ledger():
    result = _run(QueueBound.PESSIMISTIC)
    assert len(result.fills) == 1
    fill = result.fills[0]
    assert fill.side == "B" and fill.px == "100"
    assert abs(fill.sz - 0.5) < 1e-9              # 2.5 print - 2.0 real ahead
    assert abs(fill.queue_ahead_at_fill - 2.0) < 1e-9  # arrival semantics
    assert abs(result.inventory - 0.5) < 1e-9
    fee = 100 * 0.5 * 1.5 / 10_000
    assert abs(result.cash - (-(100 * 0.5) - fee)) < 1e-9

    decomp = decompose(result)
    assert decomp.fill_count == 1
    assert abs(decomp.spread_capture - (100.5 - 100.0) * 0.5) < 1e-9


def test_bounds_bracket_on_the_same_tape():
    fills_by_bound = {
        bound: sum(f.sz for f in _run(bound).fills)
        for bound in (QueueBound.PESSIMISTIC, QueueBound.PRORATA, QueueBound.OPTIMISTIC)
    }
    assert (
        fills_by_bound[QueueBound.OPTIMISTIC]
        >= fills_by_bound[QueueBound.PRORATA]
        >= fills_by_bound[QueueBound.PESSIMISTIC]
    )
