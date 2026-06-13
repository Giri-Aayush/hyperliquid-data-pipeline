"""Tests for backtest metrics — hand-computed values and edge cases."""

import numpy as np
import pandas as pd
import pytest

from hyperliquid_pipeline.backtest import metrics


def _equity(values, freq="1D"):
    idx = pd.date_range("2024-01-01", periods=len(values), freq=freq, tz="UTC")
    return pd.Series(values, index=idx, dtype=float)


def test_total_return():
    assert metrics.total_return(_equity([100, 110])) == pytest.approx(0.10)
    assert metrics.total_return(_equity([])) == 0.0


def test_max_drawdown():
    # peak 120, trough 90 -> 90/120 - 1 = -0.25
    assert metrics.max_drawdown(_equity([100, 120, 90, 100])) == pytest.approx(-0.25)
    # monotonic up -> no drawdown
    assert metrics.max_drawdown(_equity([100, 110, 120])) == pytest.approx(0.0)


def test_periods_per_year():
    hourly = pd.date_range("2024-01-01", periods=10, freq="1h", tz="UTC")
    assert metrics.periods_per_year(hourly) == pytest.approx(365.25 * 24)
    daily = pd.date_range("2024-01-01", periods=10, freq="1D", tz="UTC")
    assert metrics.periods_per_year(daily) == pytest.approx(365.25)


def test_sharpe_zero_variance_is_zero():
    r = pd.Series([0.01, 0.01, 0.01])
    assert metrics.sharpe(r, 252) == 0.0


def test_sharpe_sign_tracks_mean():
    assert metrics.sharpe(pd.Series([0.02, -0.01, 0.03, 0.01]), 252) > 0
    assert metrics.sharpe(pd.Series([-0.02, 0.01, -0.03, -0.01]), 252) < 0


def test_sortino_no_downside_is_inf_when_profitable():
    assert metrics.sortino(pd.Series([0.01, 0.02, 0.0]), 252) == float("inf")
    # all non-positive with no profit -> 0.0
    assert metrics.sortino(pd.Series([0.0, 0.0, 0.0]), 252) == 0.0


def test_win_rate():
    assert metrics.win_rate([0.1, -0.05, 0.2]) == pytest.approx(2 / 3)
    assert metrics.win_rate([]) == 0.0


def test_profit_factor():
    assert metrics.profit_factor([0.1, -0.05]) == pytest.approx(2.0)
    assert metrics.profit_factor([0.1, 0.2]) == float("inf")  # no losses
    assert metrics.profit_factor([]) == 0.0
    assert metrics.profit_factor([-0.1, -0.2]) == pytest.approx(0.0)  # no gains


def test_expectancy():
    assert metrics.expectancy([0.1, -0.05, 0.2]) == pytest.approx(0.25 / 3)
    assert metrics.expectancy([]) == 0.0


def test_cagr_doubling_over_one_year():
    idx = pd.to_datetime(["2024-01-01", "2025-01-01"], utc=True)
    equity = pd.Series([100.0, 200.0], index=idx)
    # ~100% over ~one year
    assert metrics.cagr(equity) == pytest.approx(1.0, abs=0.01)


def test_summarize_shape():
    eq = _equity([100, 105, 103, 110])
    returns = eq.pct_change().fillna(0.0)
    positions = pd.Series([0, 1, 1, 1], index=eq.index)
    report = metrics.summarize(eq, returns, positions, [0.05, -0.02, 0.07])
    for key in ("total_return", "cagr", "sharpe", "sortino", "max_drawdown",
                "win_rate", "profit_factor", "expectancy", "num_trades",
                "exposure", "final_equity"):
        assert key in report
    assert report["num_trades"] == 3
    assert report["exposure"] == pytest.approx(0.75)
