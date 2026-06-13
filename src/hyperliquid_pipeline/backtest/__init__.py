"""Backtesting: run a strategy over the OHLCV this pipeline collects and report
PnL, Sharpe, Sortino, drawdown, win rate, profit factor, and expectancy."""

from .engine import BacktestResult, Trade, run_backtest
from .strategies import BuyAndHold, RSIStrategy, SMACrossover, Strategy, rsi
from . import data, metrics

__all__ = [
    "run_backtest",
    "BacktestResult",
    "Trade",
    "Strategy",
    "BuyAndHold",
    "SMACrossover",
    "RSIStrategy",
    "rsi",
    "data",
    "metrics",
]
