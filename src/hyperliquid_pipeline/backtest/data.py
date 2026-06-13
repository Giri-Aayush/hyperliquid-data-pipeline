"""Load OHLCV for backtesting — from a DataFrame, a CSV, or the Parquet trade
files this pipeline writes (resampled into candles)."""

from pathlib import Path
from typing import Union

import pandas as pd

_OHLCV_COLS = ["open", "high", "low", "close"]


def from_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize an OHLCV frame (DatetimeIndex + a 'close' column)."""
    out = df.copy()
    if not isinstance(out.index, pd.DatetimeIndex):
        raise ValueError("OHLCV frame must be indexed by a DatetimeIndex")
    if "close" not in out.columns:
        raise ValueError("OHLCV frame must have a 'close' column")
    # Fail fast in the loader if a price/volume column isn't numeric, rather
    # than blowing up cryptically inside the engine later.
    for col in ("open", "high", "low", "close", "volume"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="raise")
    return out.sort_index()


def from_csv(path: Union[str, Path], time_col: str = "timestamp") -> pd.DataFrame:
    """Read an OHLCV CSV. ``time_col`` is parsed as the index."""
    df = pd.read_csv(path)
    if time_col not in df.columns:
        raise ValueError(f"CSV has no '{time_col}' column; columns: {list(df.columns)}")
    df[time_col] = pd.to_datetime(df[time_col])
    df = df.set_index(time_col)
    # Normalize to UTC so CSV history aligns with the pipeline's UTC data.
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return from_dataframe(df)


def trades_to_ohlcv(trades: pd.DataFrame, freq: str = "1min") -> pd.DataFrame:
    """Resample a trades frame (DatetimeIndex, 'price' [+ 'size']) into candles."""
    if not isinstance(trades.index, pd.DatetimeIndex):
        raise ValueError("trades frame must be indexed by a DatetimeIndex")
    if "price" not in trades.columns:
        raise ValueError("trades frame must have a 'price' column")
    if len(trades) == 0:
        return pd.DataFrame(columns=_OHLCV_COLS + ["volume"])

    ohlc = trades["price"].resample(freq).ohlc()
    if "size" in trades.columns:
        volume = trades["size"].resample(freq).sum().rename("volume")
    else:
        volume = trades["price"].resample(freq).count().rename("volume")
    out = ohlc.join(volume.astype(float))  # keep volume float regardless of path
    # Drop empty buckets (no trades in that interval).
    return out.dropna(subset=["open"])


def from_trades_parquet(path: Union[str, Path], freq: str = "1min") -> pd.DataFrame:
    """Load a pipeline trades Parquet file and resample it into OHLCV candles."""
    trades = pd.read_parquet(path)
    if not isinstance(trades.index, pd.DatetimeIndex):
        # The pipeline indexes trades by timestamp; recover it if it's a column.
        if "timestamp" in trades.columns:
            trades = trades.set_index(pd.to_datetime(trades["timestamp"]))
        else:
            raise ValueError("parquet has no DatetimeIndex or 'timestamp' column")
    return trades_to_ohlcv(trades, freq=freq)
