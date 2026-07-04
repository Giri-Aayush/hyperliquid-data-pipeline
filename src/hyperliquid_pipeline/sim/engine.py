"""The replay loop: event ordering, action latency, and the cash ledger.

The engine owns everything QueueSim is contractually allowed to assume
(sim/types.py docstring): all of a block's trades are delivered before its
BookEvent, actions only land between blocks, latency is applied here (an
action submitted at t rests from t + submit_delay_ms), and a stale book
evicts every virtual order.

One Engine instance runs ONE (bound, latency) grid cell — independent passes
by design: fills diverge across bounds, so inventory and hence policy
behavior diverge; sharing a pass would contaminate the counterfactuals.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

from loguru import logger

from .policy import MakerPolicy, QuoteAction
from .types import BookEvent, Fill, TradeEvent


@dataclass
class EngineConfig:
    """One grid cell's knobs.

    submit_delay_ms defaults to our measured public-gateway reality (~400ms);
    the Tokyo scenario runs the same tape at ~200ms. maker_fee_bps is charged
    on every fill's notional (negative = rebate). funding_rate_hourly accrues
    on marked inventory per Hyperliquid's hourly convention (positive rate:
    longs pay).
    """

    submit_delay_ms: float = 400.0
    maker_fee_bps: float = 1.5
    funding_rate_hourly: float = 0.0


@dataclass
class RunResult:
    """Everything the report needs to decompose one pass."""

    coin: str
    bound: str
    config: EngineConfig
    fills: List[Fill] = field(default_factory=list)
    mid_series: List[Tuple[int, float]] = field(default_factory=list)
    inventory_series: List[Tuple[int, float]] = field(default_factory=list)
    cash: float = 0.0
    inventory: float = 0.0
    fees_paid: float = 0.0
    funding_paid: float = 0.0
    final_mid: Optional[float] = None
    blocks: int = 0
    trades_seen: int = 0
    actions_submitted: int = 0
    stale_evictions: int = 0
    sim_stats: Dict[str, Any] = field(default_factory=dict)

    def total_pnl(self) -> float:
        """Cash plus inventory marked at the final mid (fees/funding already in cash)."""
        mark = self.final_mid if self.final_mid is not None else 0.0
        return self.cash + self.inventory * mark


class Engine:
    """Drive one policy over one event stream against one QueueSim."""

    def __init__(self, queue_sim, policy: MakerPolicy, config: EngineConfig):
        self.sim = queue_sim
        self.policy = policy
        self.config = config
        self.logger = logger.bind(component="sim_engine")

    def run(self, events: Iterable) -> RunResult:
        sim, cfg = self.sim, self.config
        result = RunResult(coin=sim.coin, bound=str(sim.bound.value), config=cfg)

        pending: List[Tuple[float, QuoteAction]] = []  # (rests_from_ms, action)
        trades_buf: List[TradeEvent] = []
        mid: Optional[float] = None
        last_t: Optional[int] = None

        for event in events:
            if isinstance(event, TradeEvent):
                trades_buf.append(event)
                continue
            if not isinstance(event, BookEvent):
                continue

            t = event.t_ms
            result.blocks += 1

            # Funding accrues on marked inventory over the inter-block gap.
            if (
                last_t is not None and mid is not None
                and cfg.funding_rate_hourly and result.inventory
            ):
                funding = (
                    result.inventory * mid * cfg.funding_rate_hourly
                    * (t - last_t) / 3_600_000.0
                )
                result.cash -= funding
                result.funding_paid += funding

            # 1) Actions whose latency has elapsed rest BEFORE this block
            #    executes (they arrived during block formation).
            due = [(eff, a) for eff, a in pending if eff <= t]
            pending = [(eff, a) for eff, a in pending if eff > t]
            for _, action in due:
                self._apply_action(action, t)

            # 2) The block's trades, in arrival order, before its book state
            #    (the book diffs already embed these trades' effects).
            new_fills: List[Fill] = []
            for trade in trades_buf:
                new_fills.extend(sim.on_trade(trade))
            result.trades_seen += len(trades_buf)
            trades_buf.clear()

            # 3) The block's book state.
            new_fills.extend(sim.on_book(event.view, event.batch, t))

            # 4) A stale book means queue estimates are garbage: evict all.
            if getattr(event.view, "stale", False):
                for order in sim.open_orders():
                    sim.cancel(order["order_id"], t, reason="stale")
                    result.stale_evictions += 1

            # Ledger.
            for fill in new_fills:
                self._apply_fill(fill, result)
            result.fills.extend(new_fills)

            mid = event.view.mid() or mid
            if mid is not None:
                result.mid_series.append((t, mid))
            result.inventory_series.append((t, result.inventory))

            # 5) Policy sees the post-block world; its actions incur latency.
            actions = self.policy.on_block(
                view=event.view,
                inventory=result.inventory,
                open_orders=sim.open_orders(),
                t_ms=t,
                fills=new_fills,
            )
            for action in actions:
                pending.append((t + cfg.submit_delay_ms, action))
            result.actions_submitted += len(actions)
            last_t = t

        result.final_mid = mid
        result.sim_stats = sim.get_stats()
        return result

    def _apply_action(self, action: QuoteAction, t: int) -> None:
        if action.kind == "place":
            self.sim.place(action.side, action.px, action.sz, t)
        elif action.kind == "cancel":
            self.sim.cancel(action.order_id, t)
        else:  # never raise mid-replay; count nothing, log once per kind
            self.logger.warning(f"unknown QuoteAction kind: {action.kind!r}")

    @staticmethod
    def _apply_fill(fill: Fill, result: RunResult) -> None:
        px = float(fill.px)
        notional = px * fill.sz
        if fill.side == "B":  # our bid bought
            result.inventory += fill.sz
            result.cash -= notional
        else:  # our ask sold
            result.inventory -= fill.sz
            result.cash += notional
        fee = notional * result.config.maker_fee_bps / 10_000.0
        result.cash -= fee
        result.fees_paid += fee
