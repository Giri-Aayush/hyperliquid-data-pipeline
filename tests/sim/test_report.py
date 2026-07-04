"""Report contracts: the decomposition adds up and each term is hand-checkable."""

from hyperliquid_pipeline.sim.engine import EngineConfig, RunResult
from hyperliquid_pipeline.sim.report import decompose, render
from hyperliquid_pipeline.sim.types import Fill

T0 = 1_700_000_000_000


def _fill(side, px, sz, t_ms, mid_at_fill):
    return Fill(order_id=1, coin="BTC", side=side, px=px, sz=sz, t_ms=t_ms,
                height=None, queue_bound="pessimistic", queue_ahead_at_fill=0.0,
                mid_at_fill=mid_at_fill)


def _result(fills, mid_series, cash, inventory, fees=0.0, funding=0.0):
    result = RunResult(coin="BTC", bound="pessimistic", config=EngineConfig())
    result.fills = fills
    result.mid_series = mid_series
    result.cash = cash
    result.inventory = inventory
    result.fees_paid = fees
    result.funding_paid = funding
    result.final_mid = mid_series[-1][1]
    return result


def test_decomposition_hand_computed():
    # Buy 2 @ 99 when mid=100 (capture +2). Mid 5s later = 98 (drift -4).
    fills = [_fill("B", "99", 2.0, T0, mid_at_fill=100.0)]
    mids = [(T0, 100.0), (T0 + 5000, 98.0), (T0 + 10_000, 98.0)]
    fees = 99 * 2.0 * 1.5 / 10_000
    cash = -(99 * 2.0) - fees
    result = _result(fills, mids, cash=cash, inventory=2.0, fees=fees)

    decomp = decompose(result, adverse_dt_ms=5000)
    assert abs(decomp.spread_capture - 2.0) < 1e-9        # (100-99)*2
    assert abs(decomp.post_fill_drift - (-4.0)) < 1e-9    # (98-100)*2, long
    assert abs(decomp.fees - (-fees)) < 1e-12
    # total = cash + inv*final_mid = -198.0297 + 196 = -2.0297
    assert abs(decomp.total_pnl - (cash + 2.0 * 98.0)) < 1e-9
    # residual closes the identity: total = capture + drift + fees + funding + residual
    reconstructed = (decomp.spread_capture + decomp.post_fill_drift
                     + decomp.fees + decomp.funding + decomp.mark_residual)
    assert abs(reconstructed - decomp.total_pnl) < 1e-9


def test_sell_side_signs():
    # Sell 1 @ 101 when mid=100.5 (capture +0.5); mid later 101.5 -> drift -1 (short hurt)
    fills = [_fill("A", "101", 1.0, T0, mid_at_fill=100.5)]
    mids = [(T0, 100.5), (T0 + 5000, 101.5)]
    result = _result(fills, mids, cash=101.0, inventory=-1.0)
    decomp = decompose(result, adverse_dt_ms=5000)
    assert abs(decomp.spread_capture - 0.5) < 1e-9
    assert abs(decomp.post_fill_drift - (-1.0)) < 1e-9


def test_fills_near_tape_end_skipped_from_drift():
    fills = [_fill("B", "99", 1.0, T0 + 9000, mid_at_fill=100.0)]
    mids = [(T0, 100.0), (T0 + 10_000, 100.0)]  # tape ends 1s after the fill
    result = _result(fills, mids, cash=-99.0, inventory=1.0)
    decomp = decompose(result, adverse_dt_ms=5000)
    assert decomp.drift_skipped_fills == 1
    assert decomp.post_fill_drift == 0.0


def test_render_contains_the_numbers_and_caveats():
    fills = [_fill("B", "99", 2.0, T0, mid_at_fill=100.0)]
    mids = [(T0, 100.0), (T0 + 5000, 98.0)]
    decomp = decompose(_result(fills, mids, cash=-198.0, inventory=2.0))
    table = render([decomp])
    assert "pessimistic" in table
    assert "caveat" in table
    assert "PESSIMISTIC" in table  # the trust rule is stated
