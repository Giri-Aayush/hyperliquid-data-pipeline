"""Reference strategies. Each maps an OHLCV frame to a target-position series in
[-1, 1] (1 = long, -1 = short, 0 = flat). The engine handles the one-bar
execution lag, so strategies may freely use the current bar's close.
"""

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


class Strategy(ABC):
    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """Return a target position per bar, indexed like df, in [-1, 1]."""


class BuyAndHold(Strategy):
    """Always long. The baseline every strategy should be measured against."""

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        return pd.Series(1.0, index=df.index)


class SMACrossover(Strategy):
    """Long when the fast SMA is above the slow SMA, short (optional) when below."""

    def __init__(self, fast: int = 10, slow: int = 30, allow_short: bool = True):
        if fast >= slow:
            raise ValueError("fast window must be shorter than slow window")
        self.fast = fast
        self.slow = slow
        self.allow_short = allow_short

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        close = df["close"].astype(float)
        fast = close.rolling(self.fast).mean()
        slow = close.rolling(self.slow).mean()
        sig = pd.Series(0.0, index=df.index)
        sig[fast > slow] = 1.0
        sig[fast < slow] = -1.0 if self.allow_short else 0.0
        return sig


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Wilder-style RSI via simple rolling averages of gains/losses."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - 100 / (1 + rs)
    # No losses but real gains -> 100. A flat window (no gains and no losses) is
    # undefined; leave it NaN so it triggers no signal.
    out[(avg_loss == 0) & (avg_gain > 0)] = 100.0
    return out


class RSIStrategy(Strategy):
    """Mean reversion: go long below ``oversold``, short above ``overbought``,
    and hold the last position in between."""

    def __init__(self, period: int = 14, oversold: float = 30.0,
                 overbought: float = 70.0, allow_short: bool = True):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought
        self.allow_short = allow_short

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        r = rsi(df["close"].astype(float), self.period)
        sig = pd.Series(np.nan, index=df.index)
        sig[r < self.oversold] = 1.0
        sig[r > self.overbought] = -1.0 if self.allow_short else 0.0
        return sig.ffill().fillna(0.0)
