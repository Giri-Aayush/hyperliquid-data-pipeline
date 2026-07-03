"""Order-flow imbalance (OFI) at top-of-book — the first signal-research read.

Implements the best-level OFI of Cont, Kukanov & Stoikov (2014), "The price
impact of order book events": per BBO event n,

    e_n =  1{Pb_n >= Pb_(n-1)} * qb_n  -  1{Pb_n <= Pb_(n-1)} * qb_(n-1)
         - 1{Pa_n <= Pa_(n-1)} * qa_n  +  1{Pa_n >= Pa_(n-1)} * qa_(n-1)

Both indicators of a pair fire when a price is unchanged — that is the
published form: an unchanged best bid contributes qb_n - qb_(n-1) (depth
added minus depth removed at the bid), and symmetrically for the ask.

The analysis sums e_n into non-overlapping windows and regresses forward mid
changes on each window's OFI, per (window, horizon). The output is a
directional research read over one captured session — short-sample, non-iid,
no costs/latency modeled — NOT a strategy and NOT a backtest.

Three input formats, auto-detected per line, all on the exchange clock:

* DataLogger 'bbo' records (event-level, preferred): ``data.bid/ask`` with
  ``data.timestamp_ms``;
* DataLogger 'orderbook' records (live L2 snapshots, top level used);
* hyperliquid-archive raw l2Book hours (``market_data/<date>/<hour>/l2Book/
  <coin>.lz4``): one ``{"time": ms, "coin", "levels": [bids, asks]}`` per
  line — the same shape historical_collector parses. Plain or ``.lz4``.

CLI (mix capture files and archive hours freely):
    python -m hyperliquid_pipeline.research.ofi <capture.jsonl|hour.lz4 ...> \
        [--windows 1,5] [--horizons 1,5,30] [--symbol BTC] [--output report.json]
"""

import argparse
import bisect
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Sequence, TextIO, Tuple

import lz4.frame


class BboEvent(NamedTuple):
    """One two-sided top-of-book observation, on the exchange clock."""

    t_ms: int
    bid_px: float
    bid_sz: float
    ask_px: float
    ask_sz: float

    @property
    def mid(self) -> float:
        return (self.bid_px + self.ask_px) / 2


DEFAULT_WINDOWS_S: Tuple[float, ...] = (1.0, 5.0)
DEFAULT_HORIZONS_S: Tuple[float, ...] = (1.0, 5.0, 30.0)

# Printed on every report on purpose: the numbers are easy to over-read.
CAVEATS: Tuple[str, ...] = (
    "short sample: a single capture session, one venue",
    "windowed observations are autocorrelated (non-iid); the t-stat assumes "
    "iid errors and therefore OVERSTATES significance",
    "no fees, latency, queueing, or adverse selection modeled",
    "a directional research read, not a strategy or a backtest",
)


# --- loading ----------------------------------------------------------------


def _open_text(path: Path) -> TextIO:
    """Open plain or .lz4 files as text (archive hours ship lz4-compressed)."""
    if path.suffix == ".lz4":
        return lz4.frame.open(path, mode="rt")  # type: ignore[return-value]
    return open(path, mode="rt")


def _top_of_book(record: Dict[str, Any]) -> Optional[Tuple[str, int, dict, dict]]:
    """Extract (symbol, t_ms, bid_level, ask_level) from any supported line
    shape, or None when the line carries no two-sided top of book."""
    data_type = record.get("data_type")
    if data_type is not None:  # DataLogger capture record
        sym = record.get("symbol")
        data = record.get("data") or {}
        t_ms = data.get("timestamp_ms")
        if data_type == "bbo":
            bid, ask = data.get("bid"), data.get("ask")
        elif data_type == "orderbook":
            bids, asks = data.get("bids") or [], data.get("asks") or []
            bid = bids[0] if bids else None
            ask = asks[0] if asks else None
        else:
            return None
    elif "levels" in record:  # hyperliquid-archive raw l2Book line
        sym = record.get("coin")
        t_ms = record.get("time")
        levels = record.get("levels") or []
        bids = levels[0] if len(levels) > 0 else []
        asks = levels[1] if len(levels) > 1 else []
        bid = bids[0] if bids else None
        ask = asks[0] if asks else None
    else:
        return None
    if sym is None or t_ms is None or bid is None or ask is None:
        return None
    return sym, t_ms, bid, ask


def load_bbo_events(
    path: str, symbol: Optional[str] = None
) -> Dict[str, List[BboEvent]]:
    """Parse one capture/archive file into per-symbol, time-ordered series.

    Accepts DataLogger 'bbo' records ({data: {bid, ask, timestamp_ms}}),
    DataLogger 'orderbook' records (top level used), and hyperliquid-archive
    raw l2Book lines ({time, coin, levels: [bids, asks]}), plain or .lz4 —
    auto-detected per line. One-sided events (null bid/ask, empty book side)
    carry no two-sided top and are skipped, as are malformed lines — the
    loader is capture-tolerant by design; correctness of the math is pinned
    by tests, not by the reader.
    """
    series: Dict[str, List[BboEvent]] = {}
    with _open_text(Path(path)) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            top = _top_of_book(record)
            if top is None:
                continue
            sym, t_ms, bid, ask = top
            if symbol is not None and sym != symbol:
                continue
            try:
                event = BboEvent(
                    t_ms=int(t_ms),
                    bid_px=float(bid["px"]),
                    bid_sz=float(bid["sz"]),
                    ask_px=float(ask["px"]),
                    ask_sz=float(ask["sz"]),
                )
            except (KeyError, TypeError, ValueError):
                continue
            series.setdefault(sym, []).append(event)
    for events in series.values():
        events.sort(key=lambda e: e.t_ms)
    return series


# --- the signal --------------------------------------------------------------


def ofi_series(events: Sequence[BboEvent]) -> List[Tuple[int, float]]:
    """Per-event OFI contributions e_n (CKS best-level form), stamped with
    the event's exchange time. len(result) == len(events) - 1."""
    out: List[Tuple[int, float]] = []
    for prev, cur in zip(events, events[1:]):
        e = 0.0
        if cur.bid_px >= prev.bid_px:
            e += cur.bid_sz
        if cur.bid_px <= prev.bid_px:
            e -= prev.bid_sz
        if cur.ask_px <= prev.ask_px:
            e -= cur.ask_sz
        if cur.ask_px >= prev.ask_px:
            e += prev.ask_sz
        out.append((cur.t_ms, e))
    return out


def aggregate_windows(
    ofi_events: Iterable[Tuple[int, float]], window_ms: int
) -> List[Tuple[int, float]]:
    """Sum e_n into non-overlapping windows; window k covers
    [k*window_ms, (k+1)*window_ms). Only windows containing at least one
    event are returned — a quiet window carries no flow to read."""
    sums: Dict[int, float] = {}
    for t_ms, e in ofi_events:
        k = t_ms // window_ms
        sums[k] = sums.get(k, 0.0) + e
    return [(k * window_ms, total) for k, total in sorted(sums.items())]


# --- the read ----------------------------------------------------------------


def forward_pairs(
    events: Sequence[BboEvent],
    window_sums: Sequence[Tuple[int, float]],
    window_ms: int,
    horizon_ms: int,
) -> List[Tuple[float, float]]:
    """(window OFI, forward mid change) pairs.

    The mid is read as a step function of exchange time (last observation at
    or before t). A window only produces a pair when the full horizon is
    observable inside the capture — no forward fill past the end.
    """
    if not events:
        return []
    times = [e.t_ms for e in events]
    last_t = times[-1]

    def mid_at(t_ms: int) -> Optional[float]:
        i = bisect.bisect_right(times, t_ms) - 1
        return events[i].mid if i >= 0 else None

    pairs: List[Tuple[float, float]] = []
    for start_ms, ofi in window_sums:
        end = start_ms + window_ms
        target = end + horizon_ms
        if target > last_t:
            continue
        mid_now = mid_at(end)
        mid_fwd = mid_at(target)
        if mid_now is None or mid_fwd is None:
            continue
        pairs.append((ofi, mid_fwd - mid_now))
    return pairs


def ols_stats(pairs: Sequence[Tuple[float, float]]) -> Dict[str, Any]:
    """Slope/r/t for y ~ a + b*x over the pairs; None where undefined.

    The t-stat is the textbook iid one — see CAVEATS for why it flatters.
    """
    n = len(pairs)
    if n < 3:
        return {"n": n, "slope": None, "r": None, "t_stat": None}
    xs = [x for x, _ in pairs]
    ys = [y for _, y in pairs]
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    syy = sum((y - mean_y) ** 2 for y in ys)
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
    if sxx == 0:  # constant OFI: no regressor variance, nothing to estimate
        return {"n": n, "slope": None, "r": None, "t_stat": None}
    slope = sxy / sxx
    if syy == 0:  # constant forward change: slope is 0 by construction
        return {"n": n, "slope": slope, "r": None, "t_stat": None}
    r = max(-1.0, min(1.0, sxy / math.sqrt(sxx * syy)))
    denom = 1.0 - r * r
    t_stat = float("inf") if denom <= 1e-15 else r * math.sqrt((n - 2) / denom)
    return {"n": n, "slope": slope, "r": r, "t_stat": t_stat}


def analyze(
    events: Sequence[BboEvent],
    symbol: str = "?",
    windows_s: Sequence[float] = DEFAULT_WINDOWS_S,
    horizons_s: Sequence[float] = DEFAULT_HORIZONS_S,
) -> Dict[str, Any]:
    """Full OFI read for one symbol: per (window, horizon [+ next-window])
    OLS slope / correlation / t-stat / N, with the caveats attached."""
    results: List[Dict[str, Any]] = []
    for window_s in windows_s:
        window_ms = int(round(window_s * 1000))
        window_sums = aggregate_windows(ofi_series(events), window_ms)
        horizon_specs = [(f"{h:g}s", int(round(h * 1000))) for h in horizons_s]
        # next-window read: forward change over exactly the following window.
        horizon_specs.append((f"next({window_s:g}s)", window_ms))
        for label, horizon_ms in horizon_specs:
            stats = ols_stats(
                forward_pairs(events, window_sums, window_ms, horizon_ms)
            )
            results.append({"window_s": window_s, "horizon": label, **stats})
    start_ms = events[0].t_ms if events else None
    end_ms = events[-1].t_ms if events else None
    return {
        "symbol": symbol,
        "events": len(events),
        "start_ms": start_ms,
        "end_ms": end_ms,
        "duration_s": ((end_ms - start_ms) / 1000.0) if events else 0.0,
        "windows_s": list(windows_s),
        "horizons_s": list(horizons_s),
        "results": results,
        "caveats": list(CAVEATS),
    }


# --- CLI ----------------------------------------------------------------------


def _format_report(report: Dict[str, Any]) -> str:
    def fmt(value: Any, spec: str) -> str:
        if value is None:
            return "-"
        if value == float("inf"):
            return "inf"
        return format(value, spec)

    lines = [
        f"OFI read — {report['symbol']}: {report['events']} events over "
        f"{report['duration_s']:.1f}s",
        f"{'window':>8}  {'horizon':>10}  {'N':>6}  {'slope':>12}  "
        f"{'r':>8}  {'t-stat':>8}",
    ]
    for row in report["results"]:
        lines.append(
            f"{row['window_s']:>7g}s  {row['horizon']:>10}  {row['n']:>6}  "
            f"{fmt(row['slope'], '.3e'):>12}  {fmt(row['r'], '.3f'):>8}  "
            f"{fmt(row['t_stat'], '.2f'):>8}"
        )
    lines.append("caveats:")
    lines.extend(f"  - {caveat}" for caveat in report["caveats"])
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m hyperliquid_pipeline.research.ofi",
        description=(
            "Order-flow imbalance read over live capture JSONL and/or "
            "hyperliquid-archive l2Book hours: does top-of-book flow lead "
            "the mid? Prints per-symbol tables."
        ),
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="DataLogger capture JSONL and/or archive l2Book hours (.lz4 ok)",
    )
    parser.add_argument(
        "--windows", default="1,5", help="window lengths in seconds (comma-separated)"
    )
    parser.add_argument(
        "--horizons", default="1,5,30", help="forward horizons in seconds"
    )
    parser.add_argument("--symbol", default=None, help="restrict to one symbol")
    parser.add_argument(
        "--output", default=None, help="also write all reports to this JSON file"
    )
    args = parser.parse_args(argv)

    windows_s = [float(w) for w in args.windows.split(",") if w.strip()]
    horizons_s = [float(h) for h in args.horizons.split(",") if h.strip()]

    merged: Dict[str, List[BboEvent]] = {}
    for path in args.files:
        for sym, events in load_bbo_events(path, symbol=args.symbol).items():
            merged.setdefault(sym, []).extend(events)
    for events in merged.values():
        events.sort(key=lambda e: e.t_ms)

    if not merged:
        print("no usable bbo/orderbook events found in the given files")
        return 1

    reports = [
        analyze(events, symbol=sym, windows_s=windows_s, horizons_s=horizons_s)
        for sym, events in sorted(merged.items())
    ]
    print("\n\n".join(_format_report(report) for report in reports))
    if args.output:
        with open(args.output, "w") as fh:
            json.dump(reports, fh, indent=2)
        print(f"\nreports written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
