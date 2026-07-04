"""Engine contracts: ordering, latency, stale eviction, and the ledger.

Uses a contract-shaped fake QueueSim that records its call sequence — the
engine's guarantees (trades before book, actions rest only after latency,
stale evicts everything) are exactly what the real queue core assumes.
"""

from dataclasses import dataclass
from typing import Optional

from hyperliquid_pipeline.sim.engine import Engine, EngineConfig, RunResult
from hyperliquid_pipeline.sim.policy import QuoteAction
from hyperliquid_pipeline.sim.types import BookEvent, Fill, QueueBound, TradeEvent

T0 = 1_700_000_000_000


@dataclass
class _FakeView:
    bid_px: str = "100"
    bid_sz: float = 2.0
    ask_px: str = "101"
    ask_sz: float = 1.0
    stale: bool = False
    last_update_ms: int = 0

    def best_bid(self):
        return (self.bid_px, self.bid_sz)

    def best_ask(self):
        return (self.ask_px, self.ask_sz)

    def mid(self):
        return (float(self.bid_px) + float(self.ask_px)) / 2

    def depth(self, n):
        return {"bids": [], "asks": []}

    def is_crossed(self):
        return False


class _FakeSim:
    """Records call order; emits scripted fills on demand."""

    def __init__(self, fills_on_trade=None):
        self.coin = "BTC"
        self.bound = QueueBound.PESSIMISTIC
        self.calls = []
        self._next_id = 0
        self._open = {}
        self._fills_on_trade = list(fills_on_trade or [])

    def place(self, side, px, sz, t_ms):
        self._next_id += 1
        self.calls.append(("place", side, px, t_ms))
        self._open[self._next_id] = {"order_id": self._next_id, "side": side,
                                     "px": px, "sz": sz}
        return self._next_id

    def cancel(self, order_id, t_ms, reason="policy"):
        self.calls.append(("cancel", order_id, reason))
        return self._open.pop(order_id, None) is not None

    def on_trade(self, trade):
        self.calls.append(("on_trade", trade.t_ms))
        return [self._fills_on_trade.pop(0)] if self._fills_on_trade else []

    def on_book(self, view, batch, t_ms):
        self.calls.append(("on_book", t_ms))
        return []

    def queue_ahead(self, order_id):
        return 0.0

    def open_orders(self):
        return list(self._open.values())

    def get_stats(self):
        return {"fake": True}


class _ScriptedPolicy:
    """Returns a fixed action list on the first block, then nothing."""

    def __init__(self, first_actions):
        self._first = list(first_actions)

    def on_block(self, view, inventory, open_orders, t_ms, fills):
        actions, self._first = self._first, []
        return actions


def _book(t_ms, view=None):
    return BookEvent(coin="BTC", t_ms=t_ms, height=None,
                     view=view or _FakeView(), batch=None)


def _trade(t_ms, px="100", side="A"):
    return TradeEvent(coin="BTC", t_ms=t_ms, px=px, sz=1.0, side=side)


def test_trades_delivered_before_book_and_actions_before_trades():
    sim = _FakeSim()
    policy = _ScriptedPolicy([QuoteAction(kind="place", side="B", px="100", sz=1.0)])
    engine = Engine(sim, policy, EngineConfig(submit_delay_ms=400))
    events = [
        _book(T0),                # block 1: policy submits the place (rests T0+400)
        _trade(T0 + 900),         # belongs to block 2
        _book(T0 + 1000),         # block 2: place applies FIRST, then trade, then book
    ]
    engine.run(events)
    assert sim.calls == [
        ("on_book", T0),
        ("place", "B", "100", T0 + 1000),
        ("on_trade", T0 + 900),
        ("on_book", T0 + 1000),
    ]


def test_latency_defers_actions_past_near_blocks():
    sim = _FakeSim()
    policy = _ScriptedPolicy([QuoteAction(kind="place", side="B", px="100", sz=1.0)])
    engine = Engine(sim, policy, EngineConfig(submit_delay_ms=400))
    engine.run([_book(T0), _book(T0 + 300), _book(T0 + 500)])
    # submitted at T0, rests from T0+400: NOT applied at T0+300, applied at T0+500
    assert ("place", "B", "100", T0 + 500) in sim.calls
    assert ("place", "B", "100", T0 + 300) not in sim.calls


def test_stale_book_evicts_all_open_orders():
    sim = _FakeSim()
    policy = _ScriptedPolicy([
        QuoteAction(kind="place", side="B", px="100", sz=1.0),
        QuoteAction(kind="place", side="A", px="101", sz=1.0),
    ])
    engine = Engine(sim, policy, EngineConfig(submit_delay_ms=0))
    stale_view = _FakeView(stale=True)
    result = engine.run([_book(T0), _book(T0 + 1000), _book(T0 + 2000, view=stale_view)])
    cancel_reasons = [c[2] for c in sim.calls if c[0] == "cancel"]
    assert cancel_reasons == ["stale", "stale"]
    assert result.stale_evictions == 2


def test_ledger_math_on_a_scripted_fill():
    fill = Fill(order_id=1, coin="BTC", side="B", px="100", sz=2.0, t_ms=T0 + 900,
                height=None, queue_bound="pessimistic", queue_ahead_at_fill=0.0,
                mid_at_fill=100.5)
    sim = _FakeSim(fills_on_trade=[fill])
    engine = Engine(sim, _ScriptedPolicy([]), EngineConfig(maker_fee_bps=1.5))
    result = engine.run([_book(T0), _trade(T0 + 900), _book(T0 + 1000)])
    assert result.inventory == 2.0
    fee = 100 * 2.0 * 1.5 / 10_000
    assert result.cash == -(100 * 2.0) - fee
    assert result.fees_paid == fee
    # mid is 100.5 -> mark-to-mid PnL: -200.03 + 2*100.5 = +0.97
    assert abs(result.total_pnl() - (-(200 + fee) + 2 * 100.5)) < 1e-9


def test_funding_accrues_on_inventory_longs_pay():
    fill = Fill(order_id=1, coin="BTC", side="B", px="100", sz=1.0, t_ms=T0 + 100,
                height=None, queue_bound="pessimistic", queue_ahead_at_fill=0.0,
                mid_at_fill=100.5)
    sim = _FakeSim(fills_on_trade=[fill])
    config = EngineConfig(maker_fee_bps=0.0, funding_rate_hourly=0.0001)
    engine = Engine(sim, _ScriptedPolicy([]), config)
    # fill lands in the block at T0+1000; then one hour to the next block
    result = engine.run([
        _book(T0), _trade(T0 + 100), _book(T0 + 1000),
        _book(T0 + 1000 + 3_600_000),
    ])
    expected_funding = 1.0 * 100.5 * 0.0001  # inventory * mid * hourly rate * 1h
    assert abs(result.funding_paid - expected_funding) < 1e-9
    assert result.funding_paid > 0  # positive funding: our long paid
