"""Integration tests: gap detection -> backfill -> replay, and the
orchestrator's queue-and-retry handling of archive lag."""

import asyncio
from datetime import datetime, timedelta, timezone

import pandas as pd

from hyperliquid_pipeline.collectors.backfill import backfill_gap
from hyperliquid_pipeline.collectors.realtime_collector import (
    GapEvent,
    HyperliquidWebSocketCollector,
)
from hyperliquid_pipeline.scheduler.orchestrator import DataPipelineOrchestrator


def _ms(dt):
    return int(dt.timestamp() * 1000)


def _trades_df(rows):
    return pd.DataFrame(
        [{"price": p, "size": s, "side": sd} for (_, p, s, sd) in rows],
        index=pd.to_datetime([_ms(dt) for (dt, _, _, _) in rows], unit="ms"),
    )


class _FakeHistorical:
    def __init__(self, data=None):
        self.data = data if data is not None else {}
        self.calls = 0

    async def download_historical_data(self, symbols, start_date, end_date, data_types, hours=None):
        self.calls += 1
        return self.data


def _gap():
    start = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    return GapEvent(start=start, end=start + timedelta(seconds=60), symbols=["BTC"])


def _in_window_data():
    t = datetime(2024, 1, 1, 9, 0, 30, tzinfo=timezone.utc)
    return {"BTC": {"trades": _trades_df([(t, 100.0, 1.0, "B")])}}


# --- detection wired to backfill -------------------------------------------------

def test_detected_gap_triggers_backfill_chain():
    c = HyperliquidWebSocketCollector(["BTC"])
    c.gap_threshold_seconds = 1.0
    hist = _FakeHistorical(_in_window_data())
    recovered = []

    async def on_gap(event):
        await backfill_gap(hist, event, recovered.append)

    c.add_gap_callback(on_gap)
    c._last_disconnect_seen = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
    asyncio.run(c._maybe_emit_gap(datetime(2024, 1, 1, 9, 1, 0, tzinfo=timezone.utc)))

    assert len(recovered) == 1
    assert recovered[0].data["backfilled"] is True


# --- orchestrator queue + retry --------------------------------------------------

def _orch(historical):
    orch = DataPipelineOrchestrator()
    orch.historical_collector = historical
    orch.validation_callback = None   # _replay_point passes the point through
    orch.data_processor = None
    orch.data_logger = None
    return orch


def test_queue_gap_appends_synchronously():
    orch = _orch(_FakeHistorical({}))
    orch._queue_gap(_gap())               # sync, fast — no I/O on the socket path
    assert len(orch.pending_gaps) == 1


def test_queue_gap_respects_cap():
    orch = _orch(_FakeHistorical({}))
    orch.pending_gaps.clear()
    # deque(maxlen=...) drops oldest; shrink the cap for the test
    from collections import deque
    orch.pending_gaps = deque(maxlen=3)
    for _ in range(5):
        orch._queue_gap(_gap())
    assert len(orch.pending_gaps) == 3    # bounded, no unbounded growth


def test_retry_recovers_and_clears_pending():
    hist = _FakeHistorical({})            # empty at first
    orch = _orch(hist)
    orch._queue_gap(_gap())
    assert len(orch.pending_gaps) == 1

    hist.data = _in_window_data()         # archive caught up
    asyncio.run(orch._retry_pending_gaps())
    assert list(orch.pending_gaps) == []  # gap filled, dropped from the queue


def test_retry_drops_aged_out_gaps():
    orch = _orch(_FakeHistorical({}))
    old_start = datetime.now(timezone.utc) - timedelta(days=3)
    orch.pending_gaps.append(
        GapEvent(start=old_start, end=old_start + timedelta(seconds=60), symbols=["BTC"])
    )
    asyncio.run(orch._retry_pending_gaps())
    assert list(orch.pending_gaps) == []  # too old, given up on
    assert orch.historical_collector.calls == 0  # not even attempted


def test_replay_point_routes_through_processor_and_logger():
    orch = DataPipelineOrchestrator()
    orch.validation_callback = None
    processed, logged = [], []

    class _Proc:
        async def process_market_data(self, p):
            processed.append(p)

    class _Logger:
        def log_data_point(self, p):
            logged.append(p)

    orch.data_processor = _Proc()
    orch.data_logger = _Logger()

    from hyperliquid_pipeline.collectors.realtime_collector import MarketDataPoint
    point = MarketDataPoint(
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        symbol="BTC", data_type="trade",
        data={"price": 100.0, "size": 1.0, "backfilled": True},
    )
    asyncio.run(orch._replay_point(point))
    assert len(processed) == 1 and len(logged) == 1
