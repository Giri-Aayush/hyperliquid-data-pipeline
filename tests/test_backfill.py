"""Edge-case tests for gap backfill from the historical archive."""

import asyncio
from datetime import datetime, timezone

import pandas as pd

from hyperliquid_pipeline.collectors.backfill import backfill_gap
from hyperliquid_pipeline.collectors.realtime_collector import GapEvent


def _ms(dt):
    return int(dt.timestamp() * 1000)


def _trades_df(rows):
    """rows: list of (datetime, price, size, side). Index is tz-naive ms (as the
    real archive produces) so we also exercise the tz-naive -> UTC coercion."""
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        [{"price": p, "size": s, "side": sd} for (_, p, s, sd) in rows],
        index=pd.to_datetime([_ms(dt) for (dt, _, _, _) in rows], unit="ms"),
    )
    df.index.name = "timestamp"
    return df


class _FakeHistorical:
    def __init__(self, data=None, raise_exc=None):
        self.data = data or {}
        self.raise_exc = raise_exc
        self.calls = []

    async def download_historical_data(self, symbols, start_date, end_date, data_types, hours=None):
        self.calls.append({"symbols": symbols, "start_date": start_date,
                           "end_date": end_date, "data_types": data_types, "hours": hours})
        if self.raise_exc:
            raise self.raise_exc
        return self.data


def _gap(symbols=("BTC",)):
    start = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 9, 5, 0, tzinfo=timezone.utc)
    return GapEvent(start=start, end=end, symbols=list(symbols))


def _run(historical, gap, on_point):
    return asyncio.run(backfill_gap(historical, gap, on_point))


def test_recovers_in_window_trades_and_filters_outside():
    gap = _gap()
    in1 = datetime(2024, 1, 1, 9, 2, 0, tzinfo=timezone.utc)
    in2 = datetime(2024, 1, 1, 9, 4, 0, tzinfo=timezone.utc)
    before = datetime(2024, 1, 1, 8, 50, 0, tzinfo=timezone.utc)
    after = datetime(2024, 1, 1, 9, 10, 0, tzinfo=timezone.utc)
    df = _trades_df([
        (before, 100.0, 1.0, "B"),
        (in1, 101.0, 2.0, "A"),
        (in2, 102.0, 1.0, "B"),
        (after, 103.0, 1.0, "A"),
    ])
    hist = _FakeHistorical({"BTC": {"trades": df}})

    got = []
    n = _run(hist, gap, got.append)
    assert n == 2
    assert [p.data["price"] for p in got] == [101.0, 102.0]
    assert all(p.data["backfilled"] is True for p in got)
    assert all(p.timestamp.tzinfo is not None for p in got)  # coerced to aware UTC


def test_empty_archive_recovers_nothing():
    hist = _FakeHistorical({"BTC": {"trades": pd.DataFrame()}})
    got = []
    assert _run(hist, _gap(), got.append) == 0
    assert got == []


def test_missing_symbol_recovers_nothing():
    hist = _FakeHistorical({})  # archive returned nothing for the window
    assert _run(hist, _gap(), lambda p: None) == 0


def test_download_failure_is_swallowed():
    hist = _FakeHistorical(raise_exc=RuntimeError("requester-pays denied"))
    assert _run(hist, _gap(), lambda p: None) == 0


def test_malformed_row_is_skipped():
    gap = _gap()
    in1 = datetime(2024, 1, 1, 9, 2, 0, tzinfo=timezone.utc)
    in2 = datetime(2024, 1, 1, 9, 3, 0, tzinfo=timezone.utc)
    df = pd.DataFrame(
        [{"price": 101.0, "size": 1.0, "side": "B"},
         {"price": None, "size": 1.0, "side": "B"}],  # bad price
        index=pd.to_datetime([_ms(in1), _ms(in2)], unit="ms"),
    )
    hist = _FakeHistorical({"BTC": {"trades": df}})
    got = []
    n = _run(hist, gap, got.append)
    assert n == 1 and got[0].data["price"] == 101.0


def test_multi_symbol():
    gap = _gap(symbols=("BTC", "ETH"))
    t = datetime(2024, 1, 1, 9, 2, 0, tzinfo=timezone.utc)
    hist = _FakeHistorical({
        "BTC": {"trades": _trades_df([(t, 100.0, 1.0, "B")])},
        "ETH": {"trades": _trades_df([(t, 50.0, 2.0, "A")])},
    })
    got = []
    n = _run(hist, gap, got.append)
    assert n == 2
    assert {p.symbol for p in got} == {"BTC", "ETH"}


def test_async_on_point_awaited():
    gap = _gap()
    t = datetime(2024, 1, 1, 9, 2, 0, tzinfo=timezone.utc)
    hist = _FakeHistorical({"BTC": {"trades": _trades_df([(t, 100.0, 1.0, "B")])}})
    got = []

    async def on_point(p):
        got.append(p)

    assert _run(hist, gap, on_point) == 1
    assert len(got) == 1


def test_boundary_excludes_start_includes_end():
    gap = _gap()
    # A trade exactly at gap.start is the last live point we already stored, so
    # it must NOT be replayed (would duplicate). gap.end is included.
    hist = _FakeHistorical({"BTC": {"trades": _trades_df([
        (gap.start, 100.0, 1.0, "B"),   # excluded (already have it live)
        (gap.end, 102.0, 1.0, "A"),     # included
    ])}})
    got = []
    assert _run(hist, gap, got.append) == 1
    assert got[0].data["price"] == 102.0


def test_fetches_only_gap_hours():
    gap = _gap()  # 09:00 -> 09:05 same day
    hist = _FakeHistorical({})
    _run(hist, gap, lambda p: None)
    assert hist.calls[0]["hours"] == [9]  # not all 24 hours


def test_fetches_both_hours_across_midnight():
    start = datetime(2024, 1, 1, 23, 59, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, 0, 1, 0, tzinfo=timezone.utc)
    gap = GapEvent(start=start, end=end, symbols=["BTC"])
    hist = _FakeHistorical({})
    _run(hist, gap, lambda p: None)
    assert hist.calls[0]["hours"] == [0, 23]
    assert hist.calls[0]["start_date"] == "2024-01-01"
    assert hist.calls[0]["end_date"] == "2024-01-02"
