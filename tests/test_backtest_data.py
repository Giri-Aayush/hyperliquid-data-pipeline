"""Tests for backtest data loaders, including the pipeline Parquet path."""

import pandas as pd
import pytest

from hyperliquid_pipeline.backtest import data


def _trades(rows, freq_index=None):
    """rows: list of (iso_time, price, size)."""
    idx = pd.to_datetime([t for (t, _, _) in rows])
    return pd.DataFrame(
        {"price": [p for (_, p, _) in rows], "size": [s for (_, _, s) in rows]},
        index=idx,
    )


def test_from_dataframe_validates():
    idx = pd.date_range("2024-01-01", periods=3, freq="1D", tz="UTC")
    ok = pd.DataFrame({"close": [1.0, 2.0, 3.0]}, index=idx)
    assert "close" in data.from_dataframe(ok).columns

    with pytest.raises(ValueError):
        data.from_dataframe(pd.DataFrame({"close": [1, 2]}))  # no datetime index
    with pytest.raises(ValueError):
        data.from_dataframe(pd.DataFrame({"price": [1, 2, 3]}, index=idx))  # no close


def test_from_csv_roundtrip(tmp_path):
    csv = tmp_path / "ohlcv.csv"
    csv.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2024-01-01T00:00:00,100,101,99,100.5,5\n"
        "2024-01-01T01:00:00,100.5,102,100,101.5,7\n"
    )
    df = data.from_csv(csv)
    assert isinstance(df.index, pd.DatetimeIndex)
    assert list(df["close"]) == [100.5, 101.5]


def test_trades_to_ohlcv_resamples():
    trades = _trades([
        ("2024-01-01T00:00:10", 100.0, 1.0),
        ("2024-01-01T00:00:30", 102.0, 2.0),
        ("2024-01-01T00:00:50", 101.0, 1.0),
        ("2024-01-01T00:01:10", 105.0, 3.0),
    ])
    out = data.trades_to_ohlcv(trades, freq="1min")
    assert len(out) == 2
    first = out.iloc[0]
    assert first["open"] == 100.0
    assert first["high"] == 102.0
    assert first["low"] == 100.0
    assert first["close"] == 101.0
    assert first["volume"] == 4.0  # 1 + 2 + 1
    assert out.iloc[1]["open"] == 105.0


def test_trades_to_ohlcv_empty():
    out = data.trades_to_ohlcv(pd.DataFrame({"price": [], "size": []},
                                            index=pd.DatetimeIndex([])))
    assert len(out) == 0


def test_trades_to_ohlcv_requires_price():
    idx = pd.date_range("2024-01-01", periods=2, freq="1min", tz="UTC")
    with pytest.raises(ValueError):
        data.trades_to_ohlcv(pd.DataFrame({"size": [1, 2]}, index=idx))


def test_from_trades_parquet(tmp_path):
    # Mirror the pipeline's trades parquet: DatetimeIndex + price/size/side.
    trades = _trades([
        ("2024-01-01T00:00:10", 100.0, 1.0),
        ("2024-01-01T00:00:40", 103.0, 2.0),
        ("2024-01-01T00:01:05", 99.0, 1.0),
    ])
    trades["side"] = ["B", "A", "B"]
    path = tmp_path / "BTC_trades.parquet"
    trades.to_parquet(path)

    ohlcv = data.from_trades_parquet(path, freq="1min")
    assert list(ohlcv.columns) == ["open", "high", "low", "close", "volume"]
    assert ohlcv.iloc[0]["open"] == 100.0
    assert ohlcv.iloc[0]["close"] == 103.0
    assert ohlcv.iloc[1]["close"] == 99.0


def test_from_dataframe_rejects_non_numeric_close():
    idx = pd.date_range("2024-01-01", periods=2, freq="1D", tz="UTC")
    with pytest.raises((ValueError, TypeError)):
        data.from_dataframe(pd.DataFrame({"close": ["100.5", "oops"]}, index=idx))


def test_from_csv_is_utc(tmp_path):
    csv = tmp_path / "o.csv"
    csv.write_text("timestamp,close\n2024-01-01T00:00:00,100\n2024-01-01T01:00:00,101\n")
    df = data.from_csv(csv)
    assert df.index.tz is not None  # localized to UTC


def test_volume_is_float_without_size():
    idx = pd.to_datetime(["2024-01-01T00:00:10", "2024-01-01T00:00:40"])
    out = data.trades_to_ohlcv(pd.DataFrame({"price": [100.0, 101.0]}, index=idx))
    assert out["volume"].dtype == float
