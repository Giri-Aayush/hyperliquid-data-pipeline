"""PnL decomposition: where a maker's money actually came from.

A single PnL number hides the question that matters. Every pass decomposes
into: spread capture (edge vs mid at the moment of fill), post-fill drift
(the adverse-selection term — how the mid moved against the position Δt
after each fill), fees/rebates, funding, and a mark residual. The Δt-later
mid is computed HERE from the run's mid series, keeping Fill records causal.
"""

from bisect import bisect_right
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .engine import RunResult


@dataclass
class Decomposition:
    coin: str
    bound: str
    submit_delay_ms: float
    total_pnl: float
    spread_capture: float
    post_fill_drift: float  # negative = adverse selection cost
    fees: float
    funding: float
    mark_residual: float
    fill_count: int
    filled_notional: float
    drift_skipped_fills: int  # fills too close to the end for the Δt lookup
    adverse_dt_ms: int
    blocks: int
    stale_evictions: int
    sim_stats: Dict = field(default_factory=dict)
    caveats: Tuple[str, ...] = (
        "queue bound is a modeling assumption in L2 mode; trust only what "
        "clears PESSIMISTIC",
        "no self-impact: our fills do not perturb the replayed tape",
        "single venue, single session per tape — a directional read, not a "
        "strategy certification",
    )


def _mid_at(mid_series: List[Tuple[int, float]], t_ms: float) -> Optional[float]:
    """Step-function lookup: the last mid at or before t_ms."""
    index = bisect_right(mid_series, (t_ms, float("inf"))) - 1
    if index < 0:
        return None
    return mid_series[index][1]


def decompose(result: RunResult, adverse_dt_ms: int = 5000) -> Decomposition:
    """Break one pass's PnL into its economic parts.

    adverse_dt_ms defaults to 5s — inside the horizon where the OFI study
    showed information still lives; drift beyond ~30s is noise, not fill
    quality.
    """
    spread_capture = 0.0
    drift = 0.0
    filled_notional = 0.0
    skipped = 0
    end_t = result.mid_series[-1][0] if result.mid_series else 0

    for fill in result.fills:
        px = float(fill.px)
        filled_notional += px * fill.sz
        if fill.mid_at_fill is not None:
            edge = (fill.mid_at_fill - px) if fill.side == "B" else (px - fill.mid_at_fill)
            spread_capture += edge * fill.sz
        lookup_t = fill.t_ms + adverse_dt_ms
        if lookup_t > end_t or fill.mid_at_fill is None:
            skipped += 1
            continue
        mid_later = _mid_at(result.mid_series, lookup_t)
        if mid_later is None:
            skipped += 1
            continue
        direction = 1.0 if fill.side == "B" else -1.0
        drift += (mid_later - fill.mid_at_fill) * fill.sz * direction

    total = result.total_pnl()
    residual = total - (
        spread_capture + drift - result.fees_paid - result.funding_paid
    )
    return Decomposition(
        coin=result.coin,
        bound=result.bound,
        submit_delay_ms=result.config.submit_delay_ms,
        total_pnl=total,
        spread_capture=spread_capture,
        post_fill_drift=drift,
        fees=-result.fees_paid,
        funding=-result.funding_paid,
        mark_residual=residual,
        fill_count=len(result.fills),
        filled_notional=filled_notional,
        drift_skipped_fills=skipped,
        adverse_dt_ms=adverse_dt_ms,
        blocks=result.blocks,
        stale_evictions=result.stale_evictions,
        sim_stats=dict(result.sim_stats),
    )


def render(decompositions: List[Decomposition]) -> str:
    """Human table over grid cells (bounds x latencies), one coin per call."""
    if not decompositions:
        return "no passes to report"
    lines = [
        f"Maker sim — {decompositions[0].coin} "
        f"(adverse Δt={decompositions[0].adverse_dt_ms}ms)",
        f"{'bound':<12}{'δms':>6}{'fills':>7}{'PnL':>12}{'spread':>10}"
        f"{'drift':>10}{'fees':>9}{'funding':>9}{'resid':>9}",
        "-" * 84,
    ]
    for d in decompositions:
        lines.append(
            f"{d.bound:<12}{d.submit_delay_ms:>6.0f}{d.fill_count:>7}"
            f"{d.total_pnl:>12.4f}{d.spread_capture:>10.4f}"
            f"{d.post_fill_drift:>10.4f}{d.fees:>9.4f}{d.funding:>9.4f}"
            f"{d.mark_residual:>9.4f}"
        )
    lines.append("")
    for caveat in decompositions[0].caveats:
        lines.append(f"caveat: {caveat}")
    return "\n".join(lines)
