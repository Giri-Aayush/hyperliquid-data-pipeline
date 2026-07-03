"""Tests for dual timestamps: local receive stamps alongside exchange time.

Contract: MarketDataPoint.timestamp stays exchange time (unchanged); the new
recv_ts_ms/recv_mono_ns fields are stamped when a frame comes through
process_message with receive stamps, are None otherwise, survive the
sanitizer, appear in JSONL, and ride into the Postgres JSONB payload.
"""

import asyncio
import json
from dataclasses import asdict
from datetime import datetime, timezone

from hyperliquid_pipeline.collectors.realtime_collector import (
    DataLogger,
    HyperliquidWebSocketCollector,
    MarketDataPoint,
)
from hyperliquid_pipeline.storage.database import PostgreSQLStorage
from hyperliquid_pipeline.utils.validation import DataSanitizer


EXCHANGE_MS = 1_700_000_000_000
RECV_MS = float(EXCHANGE_MS + 150)  # arrived 150ms after the exchange stamp
RECV_MONO = 123_456_789_000


def _l2_frame():
    return json.dumps({
        "channel": "l2Book",
        "data": {
            "coin": "BTC",
            "time": EXCHANGE_MS,
            "levels": [
                [{"px": "50000.0", "sz": "1.5", "n": 3}],
                [{"px": "50001.0", "sz": "2.0", "n": 2}],
            ],
        },
    })


def test_recv_fields_stamped_through_process_message():
    async def run():
        collector = HyperliquidWebSocketCollector(["BTC"])
        await collector.process_message(
            _l2_frame(), recv_ts_ms=RECV_MS, recv_mono_ns=RECV_MONO
        )
        point = collector._queue.get_nowait()
        assert point.recv_ts_ms == RECV_MS
        assert point.recv_mono_ns == RECV_MONO
        # timestamp semantics unchanged: still the exchange time
        assert int(point.timestamp.timestamp() * 1000) == EXCHANGE_MS
        # the buffered copy is the same object, so it carries the stamps too
        assert collector.orderbook_buffer["BTC"][-1].recv_ts_ms == RECV_MS

    asyncio.run(run())


def test_recv_fields_none_without_stamps():
    """Direct handler calls and unstamped process_message keep None fields —
    the back-compat contract for backfill, historical replay, and tests."""
    async def run():
        collector = HyperliquidWebSocketCollector(["BTC"])
        await collector.process_message(_l2_frame())
        point = collector._queue.get_nowait()
        assert point.recv_ts_ms is None
        assert point.recv_mono_ns is None

    asyncio.run(run())


def test_sanitizer_preserves_recv_fields():
    point = MarketDataPoint(
        timestamp=datetime.fromtimestamp(EXCHANGE_MS / 1000, tz=timezone.utc),
        symbol="BTC",
        data_type="trade",
        data={"price": 50000.0, "size": 0.5, "side": "B", "timestamp_ms": EXCHANGE_MS},
        recv_ts_ms=RECV_MS,
        recv_mono_ns=RECV_MONO,
    )
    sanitized = DataSanitizer().sanitize_trade_data(point)
    assert sanitized is not None
    assert sanitized.recv_ts_ms == RECV_MS
    assert sanitized.recv_mono_ns == RECV_MONO

    book_point = MarketDataPoint(
        timestamp=point.timestamp,
        symbol="BTC",
        data_type="orderbook",
        data={
            "bids": [{"px": "50000.0", "sz": "1.0"}],
            "asks": [{"px": "50001.0", "sz": "1.0"}],
            "timestamp_ms": EXCHANGE_MS,
        },
        recv_ts_ms=RECV_MS,
        recv_mono_ns=RECV_MONO,
    )
    sanitized_book = DataSanitizer().sanitize_orderbook_data(book_point)
    assert sanitized_book is not None
    assert sanitized_book.recv_ts_ms == RECV_MS


def test_jsonl_line_contains_recv_fields(tmp_path):
    logger_ = DataLogger(output_dir=str(tmp_path), object_store=None)
    point = MarketDataPoint(
        timestamp=datetime.fromtimestamp(EXCHANGE_MS / 1000, tz=timezone.utc),
        symbol="BTC",
        data_type="trade",
        data={"price": 50000.0, "size": 0.5, "side": "buy", "timestamp_ms": EXCHANGE_MS},
        recv_ts_ms=RECV_MS,
        recv_mono_ns=RECV_MONO,
    )
    logger_.log_data_point(point)
    logger_.close_all_files()
    lines = list(tmp_path.glob("*.jsonl"))[0].read_text().splitlines()
    record = json.loads(lines[0])
    assert record["recv_ts_ms"] == RECV_MS
    assert record["recv_mono_ns"] == RECV_MONO
    assert record["data"]["timestamp_ms"] == EXCHANGE_MS


def test_postgres_payload_merges_capture_stamps():
    stamped = MarketDataPoint(
        timestamp=datetime.now(timezone.utc), symbol="BTC", data_type="trade",
        data={"price": 1.0}, recv_ts_ms=RECV_MS, recv_mono_ns=RECV_MONO,
    )
    payload = PostgreSQLStorage._jsonb_payload(stamped)
    assert payload["_capture"] == {"recv_ts_ms": RECV_MS, "recv_mono_ns": RECV_MONO}
    assert payload["price"] == 1.0
    assert "_capture" not in stamped.data  # original data dict not mutated

    unstamped = MarketDataPoint(
        timestamp=datetime.now(timezone.utc), symbol="BTC", data_type="trade",
        data={"price": 1.0},
    )
    assert PostgreSQLStorage._jsonb_payload(unstamped) is unstamped.data  # untouched


def test_asdict_roundtrip_includes_recv_fields():
    point = MarketDataPoint(
        timestamp=datetime.now(timezone.utc), symbol="BTC", data_type="trade",
        data={}, recv_ts_ms=RECV_MS, recv_mono_ns=RECV_MONO,
    )
    d = asdict(point)
    assert d["recv_ts_ms"] == RECV_MS
    assert d["recv_mono_ns"] == RECV_MONO
