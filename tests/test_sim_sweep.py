"""Behavior contract for the sweep harness and the live gate.

All tapes are synthetic lazy generators over a real L2Book (BookEvent views
are live shared state — the tape lesson), policies are deterministic stubs,
and every gate clause is exercised independently: strict-positive day gate,
non-negative regime gate, pooled-tercile regime labels, and the bucket-PnL
telescoping tie-out against the engine's own decomposition.
"""

import json

import pytest

from hyperliquid_pipeline.book.l2_book import L2Book
from hyperliquid_pipeline.sim.policy import QuoteAction
from hyperliquid_pipeline.sim.sweep import (
    ParamPoint,
    discover_days,
    main,
    render_gate,
    run_sweep,
    _report_json,
)
from hyperliquid_pipeline.sim.types import BookEvent, TradeEvent

BUCKET_MS = 10_000  # small buckets so short synthetic tapes span many


class BuyOncePolicy:
    """Rest one 1.0 bid at the touch on the first block, then hold."""

    def __init__(self):
        self.acted = False

    def on_block(self, view, inventory, open_orders, t_ms, fills):
        if self.acted:
            return []
        best_bid = view.best_bid()
        if best_bid is None:
            return []
        self.acted = True
        return [QuoteAction(kind="place", side="B", px=best_bid[0], sz=1.0)]


class NullPolicy:
    def on_block(self, view, inventory, open_orders, t_ms, fills):
        return []


def _tape_factory(bid_path, trades_at=None, coin="BTC"):
    """Lazy tape: one block per (t_ms, bid_px) point, ask = bid + 0.2.

    trades_at: {t_ms: TradeEvent} delivered just before the block at t_ms.
    Each factory call builds a FRESH generator and book (passes diverge).
    """
    trades_at = trades_at or {}

    def factory(_coin, _capture_dir):
        def generate():
            book = L2Book(coin)
            for t_ms, bid_px in bid_path:
                if t_ms in trades_at:
                    yield trades_at[t_ms]
                book.update_from_snapshot(
                    [{"px": f"{bid_px:.1f}", "sz": "2", "n": 1}],
                    [{"px": f"{bid_px + 0.2:.1f}", "sz": "2", "n": 1}],
                    t_ms,
                )
                yield BookEvent(
                    coin=coin, t_ms=t_ms, height=None, view=book, batch=None
                )

        return generate()

    return factory


def _sell(t_ms, px, sz=3.0):
    return TradeEvent(coin="BTC", t_ms=t_ms, px=f"{px:.1f}", sz=sz, side="A")


def _blocks(phases, start_t=0, step_ms=1000, start_px=100.0):
    """[(n_blocks, px_step_per_block), ...] -> [(t_ms, bid_px), ...]"""
    path = []
    t, px = start_t, start_px
    for n_blocks, px_step in phases:
        for _ in range(n_blocks):
            path.append((t, px))
            t += step_ms
            px += px_step
    return path


# A day where the mid drifts UP after our early fill: every bucket >= 0.
UP_DAY = _blocks([(10, 0.0), (20, 0.02), (20, 0.1)])
# A mirror day drifting DOWN: the day that kills the account.
DOWN_DAY = _blocks([(10, 0.0), (20, -0.02), (20, -0.1)])
# Fill trigger: our bid rests from ~t=400; this sell reaches it at t=1500.
FILL_TRADES = {2000: _sell(1500, 100.0)}

PARAM = ParamPoint(width_ticks=0, skew_gain=0.0, quote_size=1.0)


def _sweep(days, params=(PARAM,), policy=BuyOncePolicy, **kwargs):
    return run_sweep(
        days=days,
        coins=["BTC"],
        params=list(params),
        policy_factory=lambda _param: policy(),
        bucket_ms=BUCKET_MS,
        **kwargs,
    )


def test_grid_shape_and_fresh_tape_per_pass():
    calls = []
    base = _tape_factory(UP_DAY, FILL_TRADES)

    def counting_factory(coin, capture_dir):
        calls.append((coin, str(capture_dir)))
        return base(coin, capture_dir)

    report = _sweep(
        days=[("day1", "d1"), ("day2", "d2")],
        params=[PARAM, ParamPoint(1, 1.0, 1.0)],
        events_factory=counting_factory,
    )
    # 2 params x 1 coin x 2 days x 3 bounds = 12 independent passes.
    assert len(report.cells) == 12
    assert len(calls) == 12  # a FRESH lazy tape per pass, never shared
    assert len(report.verdicts) == 2  # one per (coin, param)
    bounds_seen = {cell.bound for cell in report.cells}
    assert bounds_seen == {"pessimistic", "prorata", "optimistic"}


def test_gate_passes_only_when_every_day_is_positive():
    factory_up = _tape_factory(UP_DAY, FILL_TRADES)
    report = _sweep(days=[("up", "x")], events_factory=factory_up)
    (verdict,) = report.verdicts
    assert verdict.passes_days and verdict.passes_regimes and verdict.passes
    assert verdict.min_day > 0
    assert verdict.fill_count > 0

    def mixed_factory(coin, capture_dir):
        path = UP_DAY if str(capture_dir) == "up" else DOWN_DAY
        return _tape_factory(path, FILL_TRADES)(coin, capture_dir)

    report = _sweep(
        days=[("up", "up"), ("down", "down")], events_factory=mixed_factory
    )
    (verdict,) = report.verdicts
    assert verdict.passes is False
    assert verdict.day_pnls["up"] > 0 > verdict.day_pnls["down"]
    assert verdict.min_day == verdict.day_pnls["down"]  # min, never average


def test_bucket_pnls_telescope_to_the_decomposition_total():
    report = _sweep(
        days=[("up", "x")], events_factory=_tape_factory(UP_DAY, FILL_TRADES)
    )
    for cell in report.cells:
        assert sum(b.pnl for b in cell.buckets) == pytest.approx(
            cell.decomposition.total_pnl, abs=1e-9
        )


def test_regime_labels_come_from_pooled_terciles():
    report = _sweep(
        days=[("up", "x")], events_factory=_tape_factory(UP_DAY, FILL_TRADES)
    )
    pess = [c for c in report.cells if c.bound == "pessimistic"]
    buckets = [b for cell in pess for b in cell.buckets]
    labels = {b.regime for b in buckets}
    assert labels <= {"calm", "mid", "volatile"} and "volatile" in labels
    calm_vols = [b.realized_vol for b in buckets if b.regime == "calm"]
    hot_vols = [b.realized_vol for b in buckets if b.regime == "volatile"]
    assert max(calm_vols) <= min(hot_vols)  # labels are ordered by vol
    # every bound's buckets carry labels (shared per (day, t_start))
    assert all(
        b.regime is not None for cell in report.cells for b in cell.buckets
    )


def test_null_policy_never_clears_the_strict_day_gate():
    report = _sweep(
        days=[("up", "x")],
        events_factory=_tape_factory(UP_DAY, FILL_TRADES),
        policy=NullPolicy,
    )
    (verdict,) = report.verdicts
    assert verdict.day_pnls == {"up": 0.0}
    assert verdict.passes_days is False  # zero is not clearing the gate
    assert verdict.passes_regimes is True  # flat regimes are acceptable
    assert verdict.passes is False
    assert verdict.fill_count == 0


def test_regime_gate_fails_a_day_that_wins_overall_but_loses_volatile():
    # Up +0.04/block for 40 blocks (+1.6), then down -0.06/block for 20
    # (-1.2): the day nets positive, but the loss concentrates in the
    # highest-vol buckets — exactly what averaging would hide.
    win_then_lose = _blocks([(10, 0.0), (40, 0.04), (20, -0.06)])
    report = _sweep(
        days=[("mixed", "x")],
        events_factory=_tape_factory(win_then_lose, FILL_TRADES),
    )
    (verdict,) = report.verdicts
    assert verdict.passes_days is True  # the day as a whole made money
    assert verdict.min_regime < 0  # ... by losing in the volatile regime
    assert verdict.regime_pnls["volatile"] < 0
    assert verdict.passes is False


def test_single_day_run_is_advisory_not_a_verdict():
    """One day of tape cannot be a live decision — the gate computes but
    labels ADVISORY, guarding a $150 account from two-day temptation."""
    report = _sweep(
        days=[("up", "x")], events_factory=_tape_factory(UP_DAY, FILL_TRADES)
    )
    (verdict,) = report.verdicts
    assert verdict.advisory is True  # 1 day < MIN_DAYS_FOR_VERDICT
    assert verdict.verdict_label().startswith("ADVISORY-")
    assert "ADVISORY" in render_gate(report)


def test_referral_fee_is_a_linear_sensitivity_never_the_gate():
    report = _sweep(
        days=[("up", "x")], events_factory=_tape_factory(UP_DAY, FILL_TRADES)
    )
    from hyperliquid_pipeline.sim.sweep import GATE_FEE_BPS, REFERRAL_FEE_BPS

    (verdict,) = report.verdicts
    pess = verdict.bound_totals["pessimistic"]
    expected = pess + verdict.filled_notional * (
        GATE_FEE_BPS - REFERRAL_FEE_BPS
    ) / 10_000.0
    assert verdict.net_referral() == pytest.approx(expected)
    # Referral only ever HELPS (lower fee) and never flips the gate itself:
    assert verdict.net_referral() >= pess
    assert verdict.passes == (verdict.passes_days and verdict.passes_regimes)


def test_render_and_json_are_shippable():
    report = _sweep(
        days=[("up", "x")], events_factory=_tape_factory(UP_DAY, FILL_TRADES)
    )
    text = render_gate(report)
    assert "PASS" in text and "gate:" in text and "caveats:" in text
    payload = json.dumps(_report_json(report))
    parsed = json.loads(payload)
    assert parsed["gate_bound"] == "pessimistic"
    assert parsed["verdicts"][0]["param_label"] == PARAM.label()
    assert parsed["cells"][0]["buckets"]


def test_discover_days_handles_both_layouts(tmp_path):
    multi = tmp_path / "daily_captures"
    for date in ("20260704", "20260705"):
        day = multi / date
        day.mkdir(parents=True)
        (day / "BTC_orderbook_x.jsonl").write_text("")
    single = tmp_path / "research_capture"
    single.mkdir()
    (single / "BTC_orderbook_y.jsonl").write_text("")
    (tmp_path / "empty_dir").mkdir()

    days = discover_days([str(multi), str(single), str(tmp_path / "empty_dir")])
    assert [(label, path.name) for label, path in days] == [
        ("20260704", "20260704"),
        ("20260705", "20260705"),
        ("research_capture", "research_capture"),
    ]


def test_cli_end_to_end_on_capture_format_files(tmp_path, capsys):
    """Mini integration: real files, real loader, real WidthPolicy."""
    day = tmp_path / "20260704"
    day.mkdir()
    book_lines = []
    for k in range(30):
        px = 100.0 + 0.1 * (k // 10)
        book_lines.append(
            json.dumps(
                {
                    "timestamp": "t",
                    "symbol": "BTC",
                    "data_type": "orderbook",
                    "data": {
                        "bids": [{"px": f"{px:.1f}", "sz": "2", "n": 1}],
                        "asks": [{"px": f"{px + 0.2:.1f}", "sz": "2", "n": 1}],
                        "timestamp_ms": 1000 * k,
                    },
                }
            )
        )
    (day / "BTC_orderbook_20260704.jsonl").write_text("\n".join(book_lines) + "\n")
    (day / "BTC_trade_20260704.jsonl").write_text(
        json.dumps(
            {
                "timestamp": "t",
                "symbol": "BTC",
                "data_type": "trade",
                "data": {
                    "price": "100.0",
                    "size": "5.0",
                    "side": "A",
                    "timestamp_ms": 2500,
                },
            }
        )
        + "\n"
    )

    exit_code = main(
        [
            str(tmp_path),
            "--coins",
            "BTC",
            "--widths",
            "0,1",
            "--skews",
            "0",
            "--notional",
            "100",
            "--output",
            str(tmp_path / "gate.json"),
        ]
    )
    out = capsys.readouterr().out
    assert exit_code == 0
    assert "gate:" in out and "caveats:" in out
    saved = json.loads((tmp_path / "gate.json").read_text())
    assert saved["days"] == ["20260704"]
    assert len(saved["verdicts"]) == 2  # two widths
