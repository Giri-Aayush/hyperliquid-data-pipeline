"""Sweep gate contracts.

The gate exists to protect a small account, so its safety properties are
pinned hardest: the min-days ADVISORY rule (no green table on thin tape), the
every-day / every-regime discipline (one bad day fails the whole verdict),
and the referral fee as a re-fee that never moves the gate. Uses synthetic
CellResults so the gate logic is tested without running the engine.
"""

from pathlib import Path

from hyperliquid_pipeline.sim import sweep
from hyperliquid_pipeline.sim.sweep import (
    Bucket,
    CellResult,
    GateVerdict,
    ParamPoint,
    _label_regimes,
    _verdicts,
    discover_days,
    render_gate,
)


class _FakeDecomp:
    """Minimal stand-in for a Decomposition the gate reads from."""

    def __init__(self, total_pnl, fill_count=1, filled_notional=1000.0):
        self.total_pnl = total_pnl
        self.fill_count = fill_count
        self.filled_notional = filled_notional
        self.sim_stats = {}


def _cell(param, coin, day, bound, pnl, buckets=None, notional=1000.0):
    cell = CellResult(param=param, coin=coin, day=day, bound=bound,
                      decomposition=_FakeDecomp(pnl, filled_notional=notional),
                      buckets=buckets or [])
    return cell


P = ParamPoint(width_ticks=2, skew_gain=1.0, quote_size=0.01)


def _pess_cells_over_days(day_pnls):
    """One pessimistic cell per day with a single non-negative-labeled bucket."""
    cells = []
    for day, pnl in day_pnls.items():
        bucket = Bucket(t_start_ms=0, realized_vol=1.0, pnl=pnl, fills=1, regime="mid")
        cells.append(_cell(P, "BTC", day, "pessimistic", pnl, [bucket]))
    return cells


def test_advisory_below_min_days_even_when_passing():
    # 2 profitable days: gate math passes, but tape is too thin to act on
    cells = _pess_cells_over_days({"d1": 5.0, "d2": 3.0})
    verdicts = _verdicts(cells)
    v = verdicts[0]
    assert v.passes is True
    assert v.advisory is True
    assert v.verdict_label() == "ADVISORY-PASS"


def test_not_advisory_at_min_days():
    cells = _pess_cells_over_days({f"d{i}": 2.0 for i in range(sweep.MIN_DAYS_FOR_VERDICT)})
    v = _verdicts(cells)[0]
    assert v.advisory is False
    assert v.verdict_label() == "PASS"


def test_one_losing_day_fails_the_whole_verdict():
    cells = _pess_cells_over_days({f"d{i}": 3.0 for i in range(5)} | {"bad": -0.5})
    v = _verdicts(cells)[0]
    assert v.min_day == -0.5
    assert v.passes_days is False
    assert v.passes is False  # the average is +14.5 but the gate still fails


def test_one_losing_regime_fails_the_verdict():
    # profitable every day, but negative in the volatile bucket
    good = Bucket(t_start_ms=0, realized_vol=0.1, pnl=10.0, fills=2, regime="calm")
    bad = Bucket(t_start_ms=300_000, realized_vol=9.0, pnl=-2.0, fills=1, regime="volatile")
    cells = [_cell(P, "BTC", f"d{i}", "pessimistic", 8.0, [good, bad]) for i in range(6)]
    v = _verdicts(cells)[0]
    assert v.min_day == 8.0 and v.passes_days is True
    assert v.regime_pnls["volatile"] < 0
    assert v.passes_regimes is False
    assert v.passes is False


def test_referral_is_a_refeed_not_the_gate():
    cells = _pess_cells_over_days({f"d{i}": -0.10 for i in range(6)})
    v = _verdicts(cells)[0]
    # gate fails at 1.5bp; referral improves net but must NOT flip passes
    assert v.passes is False
    fee_saving = v.filled_notional * (sweep.GATE_FEE_BPS - sweep.REFERRAL_FEE_BPS) / 10_000
    assert abs(v.net_referral() - (v.bound_totals["pessimistic"] + fee_saving)) < 1e-9
    assert v.net_referral() > v.bound_totals["pessimistic"]  # cheaper fees help


def test_regime_terciles_pooled_per_coin():
    # six buckets with distinct vols across two days -> tercile boundaries
    cells = []
    vols = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6]
    for i, vol in enumerate(vols):
        b = Bucket(t_start_ms=i * 300_000, realized_vol=vol, pnl=1.0, fills=1)
        cells.append(_cell(P, "BTC", "d1", "pessimistic", 1.0, [b]))
    _label_regimes(cells)
    labels = [c.buckets[0].regime for c in cells]
    assert labels[0] == "calm" and labels[-1] == "volatile"
    assert "mid" in labels


def test_bounds_recorded_for_the_bracket():
    cells = [
        _cell(P, "BTC", "d1", "pessimistic", -5.0),
        _cell(P, "BTC", "d1", "prorata", -4.0),
        _cell(P, "BTC", "d1", "optimistic", -3.0),
    ]
    v = _verdicts(cells)[0]
    assert v.bound_totals == {"pessimistic": -5.0, "prorata": -4.0, "optimistic": -3.0}


def test_render_shows_advisory_banner_and_referral_column():
    cells = _pess_cells_over_days({"d1": 5.0, "d2": 3.0})
    report = sweep.SweepReport(
        coins=["BTC"], days=["d1", "d2"], params=[P], maker_fee_bps=1.5,
        submit_delay_ms=400.0, bucket_ms=300_000, cells=cells,
        verdicts=_verdicts(cells),
    )
    text = render_gate(report)
    assert "ADVISORY" in text
    assert "@1.44bp" in text
    assert "not a live decision" in text.lower() or "not a basis" in text.lower()


def test_discover_days_single_and_multi(tmp_path):
    # multi-day root: two subdirs each with capture JSONL
    for day in ("20260704", "20260705"):
        d = tmp_path / day
        d.mkdir()
        (d / "BTC_orderbook_x.jsonl").write_text("{}\n")
    days = discover_days([str(tmp_path)])
    assert sorted(name for name, _ in days) == ["20260704", "20260705"]

    # single-day root: capture JSONL directly inside
    flat = tmp_path / "flat"
    flat.mkdir()
    (flat / "BTC_orderbook_x.jsonl").write_text("{}\n")
    days = discover_days([str(flat)])
    assert [name for name, _ in days] == ["flat"]
