"""Tests for the backtest engine — no-lookahead, costs, shorts, trades, errors."""

import pandas as pd
import pytest

from hyperliquid_pipeline.backtest import BuyAndHold, run_backtest
from hyperliquid_pipeline.backtest.strategies import Strategy


def _df(closes, freq="1D"):
    idx = pd.date_range("2024-01-01", periods=len(closes), freq=freq, tz="UTC")
    return pd.DataFrame({"close": [float(c) for c in closes]}, index=idx)


class _Signals(Strategy):
    """Emit a fixed list of target positions (aligned to the frame)."""

    def __init__(self, values):
        self.values = values

    def generate_signals(self, df):
        return pd.Series(self.values, index=df.index, dtype=float)


def test_buy_and_hold_tracks_price_without_fees():
    df = _df([100, 110, 121])  # +10% per bar
    res = run_backtest(df, BuyAndHold(), fee_bps=0.0)
    # close-to-close, fully invested from bar 1 -> 121/100 - 1
    assert res.metrics["total_return"] == pytest.approx(0.21)


def test_fees_reduce_return():
    df = _df([100, 110, 121])
    free = run_backtest(df, BuyAndHold(), fee_bps=0.0).metrics["total_return"]
    charged = run_backtest(df, BuyAndHold(), fee_bps=100.0).metrics["total_return"]  # 1%/side
    assert charged < free
    # one entry of size 1 at 1% -> ~0.21 - 0.01 compounded
    assert charged == pytest.approx(0.199, abs=1e-3)


def test_no_lookahead_position_lags_one_bar():
    # signal goes long only at bar index 1; engine must hold it from bar 2.
    res = run_backtest(_df([100, 100, 110, 110]), _Signals([0, 1, 0, 0]), fee_bps=0.0)
    assert res.positions.iloc[1] == 0.0   # not acted on the signal bar
    assert res.positions.iloc[2] == 1.0   # acted on the next bar


def test_short_profits_when_price_falls():
    df = _df([100, 90, 81])  # -10% per bar
    res = run_backtest(df, _Signals([-1, -1, -1]), fee_bps=0.0)
    assert res.metrics["total_return"] == pytest.approx(0.21)  # short gains ~21%


def test_flat_strategy_is_flat():
    res = run_backtest(_df([100, 120, 90]), _Signals([0, 0, 0]), fee_bps=50.0)
    assert res.metrics["total_return"] == pytest.approx(0.0)
    assert res.metrics["num_trades"] == 0


def test_flip_long_to_short_charges_both_sides():
    # positions held: [0,1,1,-1] after shift of [1,1,-1,-1]
    df = _df([100, 100, 100, 100])  # flat price isolates the cost
    res = run_backtest(df, _Signals([1, 1, -1, -1]), fee_bps=100.0)  # 1%/side
    # bar 3 flips +1 -> -1: turnover 2 -> 2% cost; price flat so net == -0.02
    assert res.returns.iloc[3] == pytest.approx(-0.02, abs=1e-9)


def test_single_long_trade_extracted():
    df = _df([100, 110, 121, 121])
    res = run_backtest(df, _Signals([1, 1, 0, 0]), fee_bps=0.0)
    assert len(res.trades) == 1
    t = res.trades[0]
    assert t.side == 1
    assert t.return_pct > 0


def test_flip_makes_two_trades():
    res = run_backtest(_df([100, 100, 100, 100]), _Signals([1, 1, -1, -1]), fee_bps=0.0)
    assert len(res.trades) == 2
    assert [t.side for t in res.trades] == [1, -1]


def test_empty_df_raises():
    with pytest.raises(ValueError):
        run_backtest(pd.DataFrame({"close": []}, index=pd.DatetimeIndex([])), BuyAndHold())


def test_missing_close_raises():
    idx = pd.date_range("2024-01-01", periods=3, freq="1D", tz="UTC")
    with pytest.raises(ValueError):
        run_backtest(pd.DataFrame({"price": [1, 2, 3]}, index=idx), BuyAndHold())


def test_non_datetime_index_raises():
    with pytest.raises(ValueError):
        run_backtest(pd.DataFrame({"close": [1, 2, 3]}), BuyAndHold())


def test_signals_clipped_to_unit_range():
    # a strategy returning 5 is clamped to 1 (no leverage), so it matches B&H.
    df = _df([100, 110, 121])
    big = run_backtest(df, _Signals([5, 5, 5]), fee_bps=0.0).metrics["total_return"]
    assert big == pytest.approx(0.21)


def test_ruin_floors_equity_at_zero_no_phantom_recovery():
    # short into a +150% bar then a recovery bar: equity must hit 0 and stay 0,
    # never go negative or "recover".
    df = _df([100, 250, 100])
    res = run_backtest(df, _Signals([-1, -1, -1]), fee_bps=0.0)
    assert (res.equity >= 0).all()
    assert res.metrics["final_equity"] == pytest.approx(0.0)
    assert res.metrics["total_return"] == pytest.approx(-1.0)
    assert res.metrics["max_drawdown"] >= -1.0  # can't lose more than 100%


def test_open_trade_excluded_from_realized_stats():
    # still long at the end -> trade is open, not a realized win
    res = run_backtest(_df([100, 110, 121]), BuyAndHold(), fee_bps=0.0)
    assert len(res.trades) == 1 and res.trades[0].is_open is True
    assert res.metrics["num_trades"] == 0          # no closed trades
    assert res.metrics["win_rate"] == 0.0
    assert res.metrics["total_return"] == pytest.approx(0.21)  # equity still tracks


def test_closed_trade_entry_price_ties_to_return():
    # closed long over 100 -> 121; entry anchored at pre-run close
    res = run_backtest(_df([100, 110, 121, 121]), _Signals([1, 1, 0, 0]), fee_bps=0.0)
    t = res.trades[0]
    assert t.is_open is False
    assert t.entry_price == pytest.approx(100.0)
    assert t.exit_price == pytest.approx(121.0)
    # price-implied return matches the realized return (no fees)
    assert (t.exit_price / t.entry_price - 1) == pytest.approx(t.return_pct, abs=1e-9)


def test_short_intraday_window_does_not_crash_cagr():
    import math
    idx = pd.date_range("2024-01-01", periods=3, freq="1min", tz="UTC")
    df = pd.DataFrame({"close": [100.0, 101.0, 102.0]}, index=idx)
    res = run_backtest(df, BuyAndHold(), fee_bps=0.0)  # must not raise OverflowError
    assert math.isnan(res.metrics["cagr"])  # too short to annualize
    assert "nan%" not in res.report()       # rendered as n/a, not 'nan%'


def test_nan_close_raises():
    idx = pd.date_range("2024-01-01", periods=3, freq="1D", tz="UTC")
    df = pd.DataFrame({"close": [100.0, float("nan"), 102.0]}, index=idx)
    with pytest.raises(ValueError):
        run_backtest(df, BuyAndHold())


def test_unsorted_index_raises():
    idx = pd.to_datetime(["2024-01-03", "2024-01-01", "2024-01-02"], utc=True)
    df = pd.DataFrame({"close": [121.0, 100.0, 110.0]}, index=idx)
    with pytest.raises(ValueError):
        run_backtest(df, BuyAndHold())
