"""Parameter sweep + the live gate: the one question that guards $150.

Runs a policy grid (width, skew, size) x coins x days x all three queue
bounds through the simulator and answers: is there a parameter region that
clears the PESSIMISTIC bound net of flat 1.5bps maker fees on EVERY captured
day and in EVERY volatility regime — not on average. Average-of-days hides
the day that kills a small account.

Gate semantics (deliberate, documented):
* day gate: strictly positive pessimistic net PnL on every day — a maker
  that doesn't trade clears nothing;
* regime gate: no regime bucket may be NEGATIVE (>= 0) — being flat in calm
  regimes is acceptable, losing in any regime is not.
* regimes are calm/mid/volatile by realized-vol terciles of 5-minute
  buckets, pooled per coin across all its days (log-return vol, so buckets
  are comparable across days; pooling across coins would mix vol scales).

Latency: one submit delay per sweep (default the measured ~400ms) — the L2
tape cadence cannot resolve the latency axis, so this harness makes no
latency claims by construction. Funding accrual is off in the sweep
(engine default); the policy's funding tilt is a quoting input, not a PnL
term here.

CLI:
    python -m hyperliquid_pipeline.sim.sweep data/daily_captures \
        --coins BTC,ETH,SOL --widths 1,2,4 --skews 0,1,2 --notional 1000 \
        [--output gate_report.json]
"""

import argparse
import json
import math
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .engine import Engine, EngineConfig
from .events import iter_capture_events
from .policy import WidthPolicy
from .queue import QueueSim
from .report import Decomposition, decompose
from .types import QueueBound

GATE_BOUND = QueueBound.PESSIMISTIC
DEFAULT_BOUNDS: Tuple[QueueBound, ...] = (
    QueueBound.PESSIMISTIC,
    QueueBound.PRORATA,
    QueueBound.OPTIMISTIC,
)
DEFAULT_BUCKET_MS = 300_000  # 5-minute regime buckets
REGIMES = ("calm", "mid", "volatile")
# Below this many captured days, no verdict is a live decision — the gate
# still computes but every result prints ADVISORY. A green table on two days
# of tape is temptation, not evidence, and this guards a $150 account.
MIN_DAYS_FOR_VERDICT = 5
# Referral discount (docs/research/fee-schedule.md §4): a sensitivity row
# only. Fees are linear in filled notional, so it re-fees analytically with
# no rerun; it is NEVER the gate.
GATE_FEE_BPS = 1.5
REFERRAL_FEE_BPS = 1.44


@dataclass(frozen=True)
class ParamPoint:
    """One point of the policy grid."""

    width_ticks: int
    skew_gain: float
    quote_size: float  # coin units (the CLI converts --notional per coin)

    def label(self) -> str:
        return f"w{self.width_ticks}/g{self.skew_gain:g}/s{self.quote_size:g}"


@dataclass
class Bucket:
    """One time bucket of one run: realized vol + equity change within it."""

    t_start_ms: int
    realized_vol: float
    pnl: float
    fills: int
    regime: Optional[str] = None  # labeled after pooling terciles


@dataclass
class CellResult:
    """One (param, coin, day, bound) engine pass."""

    param: ParamPoint
    coin: str
    day: str
    bound: str
    decomposition: Decomposition
    buckets: List[Bucket] = field(default_factory=list)


@dataclass
class GateVerdict:
    """The gate answer for one (coin, param): pessimistic, never averaged."""

    coin: str
    param: ParamPoint
    day_pnls: Dict[str, float]
    regime_pnls: Dict[str, float]
    min_day: float
    min_regime: float
    passes_days: bool
    passes_regimes: bool
    passes: bool
    bound_totals: Dict[str, float]  # all-days total per bound (the bracket)
    fill_count: int
    filled_notional: float          # pessimistic, all days — for fee sensitivity
    advisory: bool                  # True below MIN_DAYS_FOR_VERDICT

    def net_referral(self) -> float:
        """Pessimistic all-days net re-feed at the referral rate (linear)."""
        pess = self.bound_totals.get(GATE_BOUND.value, 0.0)
        return pess + self.filled_notional * (GATE_FEE_BPS - REFERRAL_FEE_BPS) / 10_000.0

    def verdict_label(self) -> str:
        base = "PASS" if self.passes else "fail"
        return f"ADVISORY-{base}" if self.advisory else base


@dataclass
class SweepReport:
    coins: List[str]
    days: List[str]
    params: List[ParamPoint]
    maker_fee_bps: float
    submit_delay_ms: float
    bucket_ms: int
    cells: List[CellResult]
    verdicts: List[GateVerdict]
    caveats: Tuple[str, ...] = (
        "gate = strictly positive PESSIMISTIC net PnL on EVERY day and "
        "non-negative in EVERY vol regime; averages are not consulted",
        "flat maker fee; rebate tiers are a growth story, not an assumption",
        "single submit delay; the L2 tape cadence cannot resolve latency",
        "no self-impact; single venue; captured sessions only",
    )


# --- one engine pass ------------------------------------------------------------


def _default_policy_factory(param: ParamPoint) -> WidthPolicy:
    return WidthPolicy(
        quote_size=param.quote_size,
        width_ticks=param.width_ticks,
        skew_gain=param.skew_gain,
    )


def _default_events_factory(coin: str, capture_dir: Path) -> Iterable:
    return iter_capture_events(capture_dir, coin)


def _run_cell(
    param: ParamPoint,
    coin: str,
    day_label: str,
    capture_dir: Path,
    bound: QueueBound,
    policy_factory: Callable[[ParamPoint], Any],
    events_factory: Callable[[str, Path], Iterable],
    maker_fee_bps: float,
    submit_delay_ms: float,
    bucket_ms: int,
) -> CellResult:
    engine = Engine(
        queue_sim=QueueSim(coin, bound),
        policy=policy_factory(param),
        config=EngineConfig(
            submit_delay_ms=submit_delay_ms, maker_fee_bps=maker_fee_bps
        ),
    )
    # A fresh lazy stream per pass: BookEvent views are live shared state,
    # and every pass diverges — tapes are never shared or pre-built.
    result = engine.run(events_factory(coin, capture_dir))
    return CellResult(
        param=param,
        coin=coin,
        day=day_label,
        bound=bound.value,
        decomposition=decompose(result),
        buckets=_bucketize(result, bucket_ms, maker_fee_bps),
    )


def _bucketize(result, bucket_ms: int, maker_fee_bps: float) -> List[Bucket]:
    """Equity change + realized vol per time bucket.

    Equity is reconstructed from the run's own fills and mid series with the
    engine's exact fee arithmetic, so bucket PnLs telescope to the pass's
    total_pnl to the float digit (pinned in tests). Realized vol is
    sqrt(sum of squared log mid returns) within the bucket — used only for
    ranking into terciles, so no annualization.
    """
    mids = result.mid_series
    if not mids:
        return []
    start = mids[0][0]
    end = mids[-1][0]
    edges = list(range(start, end, bucket_ms)) + [end]

    fills = sorted(result.fills, key=lambda f: f.t_ms)
    fill_index = 0
    cash = 0.0
    inventory = 0.0
    prev_equity = 0.0
    mid_index = 0
    last_mid = mids[0][1]
    buckets: List[Bucket] = []

    for edge_start, edge_end in zip(edges, edges[1:]):
        # fills up to (and including) the bucket's right edge
        bucket_fills = 0
        while fill_index < len(fills) and fills[fill_index].t_ms <= edge_end:
            fill = fills[fill_index]
            notional = float(fill.px) * fill.sz
            if fill.side == "B":
                inventory += fill.sz
                cash -= notional
            else:
                inventory -= fill.sz
                cash += notional
            cash -= notional * maker_fee_bps / 10_000.0
            bucket_fills += 1
            fill_index += 1

        squared_returns = 0.0
        while mid_index < len(mids) and mids[mid_index][0] <= edge_end:
            mid = mids[mid_index][1]
            if last_mid > 0 and mid > 0 and mid_index > 0:
                squared_returns += math.log(mid / last_mid) ** 2
            last_mid = mid
            mid_index += 1

        equity = cash + inventory * last_mid
        buckets.append(
            Bucket(
                t_start_ms=edge_start,
                realized_vol=math.sqrt(squared_returns),
                pnl=equity - prev_equity,
                fills=bucket_fills,
            )
        )
        prev_equity = equity
    return buckets


# --- regimes + the gate -------------------------------------------------------------


def _label_regimes(cells: List[CellResult]) -> None:
    """Tercile-label every bucket, pooled per coin across its days.

    Pooling uses each coin's PESSIMISTIC cells only (one vol series per tape;
    other bounds share the same tape so their buckets get the same labels by
    (day, t_start)).
    """
    for coin in {cell.coin for cell in cells}:
        base = [
            c for c in cells if c.coin == coin and c.bound == GATE_BOUND.value
        ]
        vols = sorted(
            bucket.realized_vol for cell in base for bucket in cell.buckets
        )
        if not vols:
            continue
        lo = vols[len(vols) // 3]
        hi = vols[(2 * len(vols)) // 3]
        labels: Dict[Tuple[str, int], str] = {}
        for cell in base:
            for bucket in cell.buckets:
                if bucket.realized_vol <= lo:
                    bucket.regime = "calm"
                elif bucket.realized_vol <= hi:
                    bucket.regime = "mid"
                else:
                    bucket.regime = "volatile"
                labels[(cell.day, bucket.t_start_ms)] = bucket.regime
        for cell in cells:
            if cell.coin != coin or cell.bound == GATE_BOUND.value:
                continue
            for bucket in cell.buckets:
                bucket.regime = labels.get((cell.day, bucket.t_start_ms))


def _verdicts(cells: List[CellResult]) -> List[GateVerdict]:
    verdicts = []
    combos = sorted(
        {(cell.coin, cell.param) for cell in cells},
        key=lambda pair: (pair[0], pair[1].label()),
    )
    for coin, param in combos:
        mine = [c for c in cells if c.coin == coin and c.param == param]
        pess = [c for c in mine if c.bound == GATE_BOUND.value]

        day_pnls = {c.day: c.decomposition.total_pnl for c in pess}
        regime_pnls = {name: 0.0 for name in REGIMES}
        for cell in pess:
            for bucket in cell.buckets:
                if bucket.regime is not None:
                    regime_pnls[bucket.regime] += bucket.pnl

        bound_totals: Dict[str, float] = {}
        for cell in mine:
            bound_totals[cell.bound] = (
                bound_totals.get(cell.bound, 0.0) + cell.decomposition.total_pnl
            )

        min_day = min(day_pnls.values()) if day_pnls else float("-inf")
        min_regime = min(regime_pnls.values()) if regime_pnls else float("-inf")
        passes_days = bool(day_pnls) and min_day > 0
        passes_regimes = min_regime >= 0 if regime_pnls else False
        verdicts.append(
            GateVerdict(
                coin=coin,
                param=param,
                day_pnls=day_pnls,
                regime_pnls=regime_pnls,
                min_day=min_day,
                min_regime=min_regime,
                passes_days=passes_days,
                passes_regimes=passes_regimes,
                passes=passes_days and passes_regimes,
                bound_totals=bound_totals,
                fill_count=sum(c.decomposition.fill_count for c in pess),
                filled_notional=sum(c.decomposition.filled_notional for c in pess),
                advisory=len(day_pnls) < MIN_DAYS_FOR_VERDICT,
            )
        )
    return verdicts


def run_sweep(
    days: Sequence[Tuple[str, Path]],
    coins: Sequence[str],
    params: Sequence[ParamPoint],
    policy_factory: Callable[[ParamPoint], Any] = _default_policy_factory,
    events_factory: Callable[[str, Path], Iterable] = _default_events_factory,
    bounds: Sequence[QueueBound] = DEFAULT_BOUNDS,
    maker_fee_bps: float = 1.5,
    submit_delay_ms: float = 400.0,
    bucket_ms: int = DEFAULT_BUCKET_MS,
) -> SweepReport:
    """Run the whole grid and produce verdicts. Every pass is independent."""
    cells: List[CellResult] = []
    for param in params:
        for coin in coins:
            for day_label, capture_dir in days:
                for bound in bounds:
                    cells.append(
                        _run_cell(
                            param,
                            coin,
                            day_label,
                            Path(capture_dir),
                            bound,
                            policy_factory,
                            events_factory,
                            maker_fee_bps,
                            submit_delay_ms,
                            bucket_ms,
                        )
                    )
    _label_regimes(cells)
    return SweepReport(
        coins=list(coins),
        days=[label for label, _ in days],
        params=list(params),
        maker_fee_bps=maker_fee_bps,
        submit_delay_ms=submit_delay_ms,
        bucket_ms=bucket_ms,
        cells=cells,
        verdicts=_verdicts(cells),
    )


# --- day discovery + CLI ----------------------------------------------------------


def discover_days(roots: Sequence[str]) -> List[Tuple[str, Path]]:
    """Map capture roots to (label, dir) days.

    A root whose subdirectories contain capture JSONL is a multi-day root
    (each subdir one day, labeled by its name); a root holding capture JSONL
    directly is itself a single day.
    """
    days: List[Tuple[str, Path]] = []
    for root_str in roots:
        root = Path(root_str)
        if not root.is_dir():
            continue
        if any(root.glob("*_orderbook_*.jsonl")):
            days.append((root.name, root))
            continue
        for sub in sorted(p for p in root.iterdir() if p.is_dir()):
            if any(sub.glob("*_orderbook_*.jsonl")):
                days.append((sub.name, sub))
    return days


def _first_mid(capture_dir: Path, coin: str) -> Optional[float]:
    """Cheap peek at a day's first two-sided book for --notional conversion."""
    for path in sorted(capture_dir.glob(f"{coin}_orderbook_*.jsonl")):
        with open(path) as fh:
            for line in fh:
                try:
                    data = json.loads(line)["data"]
                    bid = data["bids"][0]["px"]
                    ask = data["asks"][0]["px"]
                    return (float(bid) + float(ask)) / 2
                except (KeyError, IndexError, ValueError, json.JSONDecodeError):
                    continue
    return None


def render_gate(report: SweepReport) -> str:
    advisory = len(report.days) < MIN_DAYS_FOR_VERDICT
    header = (
        f"maker gate — days: {', '.join(report.days)} ({len(report.days)}) · "
        f"fee {report.maker_fee_bps}bps · delay {report.submit_delay_ms:g}ms · "
        f"gate bound: {GATE_BOUND.value}"
    )
    lines = [header]
    if advisory:
        lines.append(
            f"*** ADVISORY: {len(report.days)} day(s) < {MIN_DAYS_FOR_VERDICT} "
            "required — NOT a live decision, no PASS is actionable ***"
        )
    lines.append(
        f"{'coin':4} {'param':>16} {'min day':>10} {'min regime':>10} "
        f"{'fills':>6} {'pess':>9} {'prorata':>9} {'optim':>9} {'@1.44bp':>9}  verdict"
    )
    for verdict in report.verdicts:
        totals = verdict.bound_totals
        lines.append(
            f"{verdict.coin:4} {verdict.param.label():>16} "
            f"{verdict.min_day:10.2f} {verdict.min_regime:10.2f} "
            f"{verdict.fill_count:6} "
            f"{totals.get('pessimistic', 0.0):9.2f} "
            f"{totals.get('prorata', 0.0):9.2f} "
            f"{totals.get('optimistic', 0.0):9.2f} "
            f"{verdict.net_referral():9.2f}  "
            f"{verdict.verdict_label()}"
        )
    passing = [v for v in report.verdicts if v.passes]
    actionable = "" if advisory else " (actionable)"
    lines.append(
        f"gate: {len(passing)}/{len(report.verdicts)} (coin, param) combos pass"
        f"{actionable}"
        + (
            " — " + "; ".join(f"{v.coin} {v.param.label()}" for v in passing)
            if passing
            else ""
        )
    )
    lines.append("caveats:")
    lines.extend(f"  - {caveat}" for caveat in report.caveats)
    if advisory:
        lines.append(
            f"  - fewer than {MIN_DAYS_FOR_VERDICT} days: every verdict is ADVISORY, "
            "not a basis for risking capital"
        )
    return "\n".join(lines)


def _report_json(report: SweepReport) -> Dict[str, Any]:
    return {
        "coins": report.coins,
        "days": report.days,
        "maker_fee_bps": report.maker_fee_bps,
        "submit_delay_ms": report.submit_delay_ms,
        "bucket_ms": report.bucket_ms,
        "gate_bound": GATE_BOUND.value,
        "verdicts": [
            {
                **asdict(verdict),
                "param": asdict(verdict.param),
                "param_label": verdict.param.label(),
            }
            for verdict in report.verdicts
        ],
        "cells": [
            {
                "param": asdict(cell.param),
                "coin": cell.coin,
                "day": cell.day,
                "bound": cell.bound,
                "decomposition": {
                    key: value
                    for key, value in asdict(cell.decomposition).items()
                    if key != "sim_stats"
                },
                "sim_stats": cell.decomposition.sim_stats,
                "buckets": [asdict(bucket) for bucket in cell.buckets],
            }
            for cell in report.cells
        ],
        "caveats": list(report.caveats),
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m hyperliquid_pipeline.sim.sweep",
        description=(
            "Sweep WidthPolicy parameters over captured days and print the "
            "pessimistic gate verdict (every day, every regime — no averages)."
        ),
    )
    parser.add_argument("roots", nargs="+", help="capture day dirs or their parents")
    parser.add_argument("--coins", default="BTC,ETH,SOL")
    parser.add_argument("--widths", default="1,2,4", help="width_ticks grid")
    parser.add_argument("--skews", default="0,1", help="skew_gain grid")
    parser.add_argument(
        "--notional",
        type=float,
        default=1000.0,
        help="quote size per side in USD, converted per coin at first mid",
    )
    parser.add_argument("--fee-bps", type=float, default=1.5)
    parser.add_argument("--delay-ms", type=float, default=400.0)
    parser.add_argument("--output", default=None, help="write full JSON here")
    args = parser.parse_args(argv)

    days = discover_days(args.roots)
    if not days:
        print("no capture days found under the given roots")
        return 1
    coins = [c.strip() for c in args.coins.split(",") if c.strip()]
    widths = [int(w) for w in args.widths.split(",") if w.strip()]
    skews = [float(s) for s in args.skews.split(",") if s.strip()]

    all_verdicts: List[GateVerdict] = []
    all_cells: List[CellResult] = []
    report: Optional[SweepReport] = None
    for coin in coins:
        mid = _first_mid(days[0][1], coin)
        if mid is None:
            print(f"{coin}: no usable book in {days[0][1]}, skipped")
            continue
        size = round(args.notional / mid, 6)
        params = [
            ParamPoint(width_ticks=w, skew_gain=g, quote_size=size)
            for w in widths
            for g in skews
        ]
        report = run_sweep(
            days,
            [coin],
            params,
            maker_fee_bps=args.fee_bps,
            submit_delay_ms=args.delay_ms,
        )
        all_verdicts.extend(report.verdicts)
        all_cells.extend(report.cells)

    if report is None:
        print("no coins produced a run")
        return 1
    report.coins = coins
    report.cells = all_cells
    report.verdicts = all_verdicts
    print(render_gate(report))
    if args.output:
        with open(args.output, "w") as fh:
            json.dump(_report_json(report), fh, indent=1)
        print(f"\nfull report written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
