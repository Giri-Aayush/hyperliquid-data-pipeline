"""Edge-case tests for reconnect gap detection."""

import asyncio
from datetime import datetime, timedelta, timezone

from hyperliquid_pipeline.collectors.realtime_collector import (
    GapEvent,
    HyperliquidWebSocketCollector,
)


def _collector(threshold=5.0, symbols=("BTC",)):
    c = HyperliquidWebSocketCollector(list(symbols))
    c.gap_threshold_seconds = threshold
    return c


def test_first_connect_emits_no_gap():
    c = _collector()
    fired = []
    c.add_gap_callback(fired.append)
    # no prior disconnect recorded
    event = asyncio.run(c._maybe_emit_gap(datetime.now(timezone.utc)))
    assert event is None
    assert fired == []


def test_gap_over_threshold_fires():
    c = _collector(threshold=5.0)
    fired = []
    c.add_gap_callback(fired.append)
    start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(seconds=30)
    c._last_disconnect_seen = start
    event = asyncio.run(c._maybe_emit_gap(end))
    assert isinstance(event, GapEvent)
    assert event.start == start and event.end == end
    assert event.seconds == 30
    assert event.symbols == ["BTC"]
    assert len(fired) == 1


def test_gap_under_threshold_does_not_fire():
    c = _collector(threshold=5.0)
    fired = []
    c.add_gap_callback(fired.append)
    start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    c._last_disconnect_seen = start
    event = asyncio.run(c._maybe_emit_gap(start + timedelta(seconds=2)))
    assert event is None
    assert fired == []


def test_record_disconnect_uses_last_message_time():
    c = _collector()
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    c.last_message_time = ts
    c._record_disconnect()
    assert c._last_disconnect_seen == ts


def test_no_messages_before_drop_yields_no_gap():
    c = _collector()
    fired = []
    c.add_gap_callback(fired.append)
    c.last_message_time = None
    c._record_disconnect()  # records None
    event = asyncio.run(c._maybe_emit_gap(datetime.now(timezone.utc)))
    assert event is None
    assert fired == []


def test_gap_fires_only_once_state_consumed():
    c = _collector(threshold=1.0)
    fired = []
    c.add_gap_callback(fired.append)
    start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    c._last_disconnect_seen = start
    asyncio.run(c._maybe_emit_gap(start + timedelta(seconds=10)))
    # second call without a new disconnect must not re-fire
    event2 = asyncio.run(c._maybe_emit_gap(start + timedelta(seconds=20)))
    assert event2 is None
    assert len(fired) == 1


def test_failing_callback_does_not_block_others():
    c = _collector(threshold=1.0)
    seen = []
    c.add_gap_callback(lambda e: (_ for _ in ()).throw(RuntimeError("boom")))
    c.add_gap_callback(lambda e: seen.append(e))
    c._last_disconnect_seen = datetime(2024, 1, 1, tzinfo=timezone.utc)
    asyncio.run(c._maybe_emit_gap(datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)))
    assert len(seen) == 1


def test_async_gap_callback_is_awaited():
    c = _collector(threshold=1.0)
    seen = []

    async def acb(event):
        seen.append(event)

    c.add_gap_callback(acb)
    c._last_disconnect_seen = datetime(2024, 1, 1, tzinfo=timezone.utc)
    asyncio.run(c._maybe_emit_gap(datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)))
    assert len(seen) == 1
