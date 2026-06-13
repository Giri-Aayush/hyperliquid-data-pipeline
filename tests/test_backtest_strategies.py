"""Tests for reference strategies and the RSI helper."""

import numpy as np
import pandas as pd
import pytest

from hyperliquid_pipeline.backtest import BuyAndHold, RSIStrategy, SMACrossover
from hyperliquid_pipeline.backtest.strategies import rsi


def _df(closes):
    idx = pd.date_range("2024-01-01", periods=len(closes), freq="1h", tz="UTC")
    return pd.DataFrame({"close": [float(c) for c in closes]}, index=idx)


def test_buy_and_hold_always_long():
    df = _df([1, 2, 3])
    assert (BuyAndHold().generate_signals(df) == 1.0).all()


def test_sma_crossover_long_on_uptrend_short_on_downtrend():
    up = list(range(1, 41))            # strictly rising
    down = list(range(40, 0, -1))      # strictly falling
    df = _df(up + down)
    sig = SMACrossover(fast=3, slow=10).generate_signals(df)
    # late in the uptrend, fast SMA > slow SMA -> long
    assert sig.iloc[35] == 1.0
    # late in the downtrend, fast SMA < slow SMA -> short
    assert sig.iloc[-1] == -1.0


def test_sma_crossover_no_short_when_disabled():
    df = _df(list(range(40, 0, -1)))
    sig = SMACrossover(fast=3, slow=10, allow_short=False).generate_signals(df)
    assert (sig >= 0).all()


def test_sma_requires_fast_lt_slow():
    with pytest.raises(ValueError):
        SMACrossover(fast=30, slow=10)


def test_rsi_all_gains_is_100():
    r = rsi(pd.Series([float(i) for i in range(1, 30)]), period=14)
    assert r.dropna().iloc[-1] == pytest.approx(100.0)


def test_rsi_bounded_0_100():
    rng = np.random.default_rng(1)
    series = pd.Series(50_000 + np.cumsum(rng.normal(0, 50, 200)))
    r = rsi(series, period=14).dropna()
    assert (r >= 0).all() and (r <= 100).all()


def test_rsi_strategy_long_oversold_short_overbought():
    # craft RSI extremes: a long fall (oversold) then a long rise (overbought)
    closes = list(range(200, 150, -1)) + list(range(150, 220))
    df = _df(closes)
    sig = RSIStrategy(period=14, oversold=30, overbought=70).generate_signals(df)
    assert sig.isin([-1.0, 0.0, 1.0]).all()
    assert 1.0 in sig.values   # went long somewhere during the fall
    assert -1.0 in sig.values  # went short somewhere during the rise


def test_rsi_strategy_holds_until_opposite_threshold():
    # once a signal fires it is held (ffill); flat appears only before the first.
    df = _df(list(range(200, 150, -1)) + list(range(150, 220)))
    sig = RSIStrategy(period=14, oversold=30, overbought=70).generate_signals(df)
    first_nonzero = sig[sig != 0].index[0]
    assert (sig.loc[first_nonzero:] != 0).all()
