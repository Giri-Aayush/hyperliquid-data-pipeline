"""Tests for the bbo channel: event-level top-of-book.

Contract: one bbo subscription per symbol (none when subscribe_bbo=False);
messages parse to data_type='bbo' with raw {px,sz,n} level dicts, exchange-ms
timestamp, and either side nullable; unknown coins are filtered; downstream
(sanitizer, processor, Influx conversion) handles the new type gracefully.
"""

import asyncio
import json
from datetime import datetime, timezone

from hyperliquid_pipeline.collectors.realtime_collector import (
    HyperliquidWebSocketCollector,
    MarketDataPoint,
)
from hyperliquid_pipeline.config import settings
from hyperliquid_pipeline.storage.database import InfluxDBStorage
from hyperliquid_pipeline.utils.validation import DataSanitizer

EXCHANGE_MS = 1_700_000_000_000


def _bbo_message(coin="BTC", bid={"px": "50000.0", "sz": "1.5", "n": 3},
                 ask={"px": "50001.0", "sz": "2.0", "n": 2}):
    return {
        "channel": "bbo",
        "data": {"coin": coin, "time": EXCHANGE_MS, "bbo": [bid, ask]},
    }


def test_one_bbo_subscription_per_symbol():
    collector = HyperliquidWebSocketCollector(["BTC", "ETH"])
    subs = collector.create_subscriptions()
    bbo_subs = [s["subscription"] for s in subs if s["subscription"]["type"] == "bbo"]
    assert [s["coin"] for s in bbo_subs] == ["BTC", "ETH"]


def test_no_bbo_subscription_when_disabled():
    original = settings.subscribe_bbo
    try:
        settings.subscribe_bbo = False
        subs = HyperliquidWebSocketCollector(["BTC"]).create_subscriptions()
        assert all(s["subscription"]["type"] != "bbo" for s in subs)
    finally:
        settings.subscribe_bbo = original


def test_parse_full_bbo():
    collector = HyperliquidWebSocketCollector(["BTC"])
    point = collector.process_bbo_message(_bbo_message())
    assert point is not None
    assert point.data_type == "bbo"
    assert int(point.timestamp.timestamp() * 1000) == EXCHANGE_MS
    assert point.data["timestamp_ms"] == EXCHANGE_MS
    # raw level dicts preserved, string prices and order count included
    assert point.data["bid"] == {"px": "50000.0", "sz": "1.5", "n": 3}
    assert point.data["ask"] == {"px": "50001.0", "sz": "2.0", "n": 2}
    assert collector.bbo_buffer["BTC"][-1] is point


def test_parse_bbo_with_null_side():
    collector = HyperliquidWebSocketCollector(["BTC"])
    point = collector.process_bbo_message(_bbo_message(bid=None))
    assert point is not None
    assert point.data["bid"] is None
    assert point.data["ask"]["px"] == "50001.0"


def test_unknown_coin_filtered():
    collector = HyperliquidWebSocketCollector(["BTC"])
    assert collector.process_bbo_message(_bbo_message(coin="DOGE")) is None


def test_bbo_routed_through_process_message_and_stats():
    async def run():
        collector = HyperliquidWebSocketCollector(["BTC"])
        await collector.process_message(json.dumps(_bbo_message()),
                                        recv_ts_ms=float(EXCHANGE_MS + 90),
                                        recv_mono_ns=1)
        point = collector._queue.get_nowait()
        assert point.data_type == "bbo"
        assert point.recv_ts_ms == float(EXCHANGE_MS + 90)
        stats = collector.get_stats()
        assert stats["buffer_sizes"]["BTC"]["bbo"] == 1
        assert stats["latency_ms"]["bbo"]["count"] == 1
        assert collector.get_recent_data("BTC", "bbo") == [point]

    asyncio.run(run())


def test_sanitizer_passes_bbo_through():
    point = MarketDataPoint(
        timestamp=datetime.fromtimestamp(EXCHANGE_MS / 1000, tz=timezone.utc),
        symbol="BTC", data_type="bbo",
        data={"bid": {"px": "1", "sz": "1", "n": 1}, "ask": None,
              "timestamp_ms": EXCHANGE_MS},
    )
    assert DataSanitizer().sanitize_data_point(point) is point


def test_influx_conversion_has_fields():
    storage = InfluxDBStorage()
    point = MarketDataPoint(
        timestamp=datetime.fromtimestamp(EXCHANGE_MS / 1000, tz=timezone.utc),
        symbol="BTC", data_type="bbo",
        data={"bid": {"px": "50000.0", "sz": "1.5", "n": 3},
              "ask": {"px": "50001.0", "sz": "2.0", "n": 2},
              "timestamp_ms": EXCHANGE_MS},
    )
    influx_point = storage._data_point_to_influx_point(point)
    line = influx_point.to_line_protocol()
    assert "best_bid_price=50000" in line
    assert "best_ask_size=2" in line

    # both sides null: still a valid (non-field-less) point
    empty = MarketDataPoint(
        timestamp=point.timestamp, symbol="BTC", data_type="bbo",
        data={"bid": None, "ask": None, "timestamp_ms": EXCHANGE_MS},
    )
    assert "empty_book" in storage._data_point_to_influx_point(empty).to_line_protocol()
