"""Performance metrics for a backtest run.

Pure functions over a per-bar net-return series, the equity curve, and a list of
per-trade returns. Annualization is inferred from the bar spacing, so the same
code works for 1m, 1h, or 1d data.
"""

from typing import List, Sequence

import numpy as np
import pandas as pd

SECONDS_PER_YEAR = 365.25 * 24 * 3600


def periods_per_year(index: pd.DatetimeIndex) -> float:
    """Annualization factor from the median spacing between bars."""
    if len(index) < 2:
        return float("nan")
    deltas = pd.Series(index).diff().dropna().dt.total_seconds()
    median = deltas.median()
    if not median or median <= 0:
        return float("nan")
    return SECONDS_PER_YEAR / median


def total_return(equity: pd.Series) -> float:
    if len(equity) == 0 or equity.iloc[0] == 0:
        return 0.0
    return float(equity.iloc[-1] / equity.iloc[0] - 1)


# Below ~one week, annualizing a return is statistical noise (and the exponent
# can overflow float64), so CAGR is reported as NaN ("not applicable").
MIN_CAGR_YEARS = 7 / 365.25


def cagr(equity: pd.Series) -> float:
    if len(equity) < 2:
        return 0.0
    years = (equity.index[-1] - equity.index[0]).total_seconds() / SECONDS_PER_YEAR
    if years < MIN_CAGR_YEARS:
        return float("nan")  # window too short (or non-positive span) to annualize
    growth = 1 + total_return(equity)
    if growth <= 0:
        return -1.0  # wiped out
    try:
        return float(growth ** (1 / years) - 1)
    except OverflowError:
        return float("inf")


def sharpe(returns: pd.Series, ppy: float) -> float:
    r = returns.dropna()
    if len(r) < 2:
        return 0.0
    std = r.std(ddof=0)
    if std == 0 or np.isnan(ppy):
        return 0.0
    return float(r.mean() / std * np.sqrt(ppy))


def sortino(returns: pd.Series, ppy: float) -> float:
    r = returns.dropna()
    if len(r) < 2 or np.isnan(ppy):
        return 0.0
    downside = r[r < 0]
    dd = downside.std(ddof=0) if len(downside) else 0.0
    if dd == 0:
        # No downside volatility: infinitely good if we made money, else flat.
        return float("inf") if r.mean() > 0 else 0.0
    return float(r.mean() / dd * np.sqrt(ppy))


def max_drawdown(equity: pd.Series) -> float:
    """Most negative peak-to-trough drop (<= 0)."""
    if len(equity) == 0:
        return 0.0
    peak = equity.cummax()
    return float((equity / peak - 1).min())


def win_rate(trade_returns: Sequence[float]) -> float:
    if not len(trade_returns):
        return 0.0
    wins = sum(1 for r in trade_returns if r > 0)
    return wins / len(trade_returns)


def profit_factor(trade_returns: Sequence[float]) -> float:
    """Gross gains / gross losses. inf when there are gains but no losses."""
    gains = sum(r for r in trade_returns if r > 0)
    losses = -sum(r for r in trade_returns if r < 0)
    if losses == 0:
        return float("inf") if gains > 0 else 0.0
    return gains / losses


def expectancy(trade_returns: Sequence[float]) -> float:
    """Average return per trade."""
    if not len(trade_returns):
        return 0.0
    return float(np.mean(trade_returns))


def summarize(
    equity: pd.Series,
    returns: pd.Series,
    positions: pd.Series,
    trade_returns: List[float],
) -> dict:
    """Roll the individual metrics into one report dict."""
    ppy = periods_per_year(equity.index)
    return {
        "total_return": total_return(equity),
        "cagr": cagr(equity),
        "sharpe": sharpe(returns, ppy),
        "sortino": sortino(returns, ppy),
        "max_drawdown": max_drawdown(equity),
        "win_rate": win_rate(trade_returns),
        "profit_factor": profit_factor(trade_returns),
        "expectancy": expectancy(trade_returns),
        "num_trades": len(trade_returns),
        "exposure": float((positions != 0).mean()) if len(positions) else 0.0,
        "final_equity": float(equity.iloc[-1]) if len(equity) else 0.0,
    }
