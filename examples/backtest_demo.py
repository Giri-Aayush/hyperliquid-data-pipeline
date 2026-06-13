"""Backtest a strategy over OHLCV and print the metrics.

Deterministic (fixed seed), no network. Generates a synthetic price series,
runs an SMA crossover against buy-and-hold, and prints PnL / Sharpe / drawdown.
Point it at real data instead with backtest.data.from_csv / from_trades_parquet.

    python examples/backtest_demo.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hyperliquid_pipeline.backtest import (
    BuyAndHold,
    SMACrossover,
    data,
    run_backtest,
)


def synthetic_ohlcv(n: int = 1500, start: float = 50_000.0) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    # Trending random walk with mild drift so there's something to trade.
    steps = rng.normal(0.0005, 0.02, n)
    close = start * np.exp(np.cumsum(steps))
    index = pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC")
    df = pd.DataFrame({"close": close}, index=index)
    df["open"] = df["close"].shift(1).fillna(df["close"])
    df["high"] = df[["open", "close"]].max(axis=1) * 1.001
    df["low"] = df[["open", "close"]].min(axis=1) * 0.999
    df["volume"] = 1.0
    return df


def main() -> None:
    df = data.from_dataframe(synthetic_ohlcv())

    for name, strat in [
        ("Buy & Hold", BuyAndHold()),
        ("SMA 10/30", SMACrossover(fast=10, slow=30)),
    ]:
        result = run_backtest(df, strat, fee_bps=10.0, slippage_bps=2.0)
        print(f"\n=== {name} ===")
        print(result.report())


if __name__ == "__main__":
    main()
