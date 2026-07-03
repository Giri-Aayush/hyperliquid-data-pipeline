"""Tests for the feed-latency histograms.

Contract: LatencyHistogram buckets known deltas exactly, percentiles land on
bucket boundaries, negative deltas (clock skew) are counted separately; the
collector records latency for channels with an exchange timestamp (l2Book,
trades, bbo) and skips allMids/activeAssetCtx, exposing it all via get_stats().
"""

import asyncio
import json

from hyperliquid_pipeline.collectors.realtime_collector import HyperliquidWebSocketCollector
from hyperliquid_pipeline.utils.latency import LatencyHistogram


def test_histogram_buckets_and_stats():
    h = LatencyHistogram(boundaries=(10, 100, 1000))
    for delta in (5, 50, 500, 5000):
        h.record(delta)
    assert h.counts == [1, 1, 1, 1]  # one per bucket incl. overflow
    assert h.count == 4
    assert h.min_ms == 5
    assert h.max_ms == 5000
    snap = h.snapshot()
    assert snap["count"] == 4
    assert snap["mean_ms"] == (5 + 50 + 500 + 5000) / 4


def test_histogram_percentiles_land_on_bucket_bounds():
    h = LatencyHistogram(boundaries=(10, 100, 1000))
    for _ in range(98):
        h.record(50)     # bucket <=100
    h.record(500)        # bucket <=1000
    h.record(2000)       # overflow
    assert h.percentile(0.50) == 100.0
    assert h.percentile(0.99) == 1000.0
    assert h.percentile(1.0) == 2000.0  # overflow reports observed max


def test_negative_deltas_counted_as_clock_skew():
    h = LatencyHistogram()
    h.record(-3.0)
    h.record(7.0)
    snap = h.snapshot()
    assert snap["negative_count"] == 1
    assert snap["count"] == 2  # skewed sample still recorded (clamped to 0)


def test_empty_histogram_snapshot():
    assert LatencyHistogram().snapshot() == {"count": 0}
    assert LatencyHistogram().percentile(0.5) is None


def test_collector_records_latency_for_timestamped_channels():
    exchange_ms = 1_700_000_000_000
    recv_ms = float(exchange_ms + 200)

    async def run():
        collector = HyperliquidWebSocketCollector(["BTC"])

        l2 = json.dumps({
            "channel": "l2Book",
            "data": {"coin": "BTC", "time": exchange_ms,
                     "levels": [[{"px": "1", "sz": "1", "n": 1}],
                                [{"px": "2", "sz": "1", "n": 1}]]},
        })
        trades = json.dumps({
            "channel": "trades",
            "data": [{"coin": "BTC", "px": "1.0", "sz": "1.0", "side": "B",
                      "time": exchange_ms, "tid": 1}],
        })
        mids = json.dumps({"channel": "allMids", "data": {"mids": {"BTC": "1.0"}}})

        # trades goes twice: the first message per coin is the subscription
        # snapshot and is deliberately not recorded (see its dedicated test).
        for frame in (l2, trades, trades, mids):
            await collector.process_message(frame, recv_ts_ms=recv_ms, recv_mono_ns=1)

        stats = collector.get_stats()["latency_ms"]
        assert stats["l2Book"]["count"] == 1
        assert stats["trades"]["count"] == 1
        # 200ms lands in the (100, 200] bucket -> p50 reports its upper bound
        assert stats["l2Book"]["p50_ms"] == 200.0
        # allMids has no exchange timestamp: never measured
        assert "allMids" not in stats

    asyncio.run(run())


def test_first_trades_message_per_coin_not_recorded():
    """The trades subscription replays recent (old) trades first — recording
    them would report seconds of phantom latency. Only live trades count."""
    exchange_ms = 1_700_000_000_000

    def trades_frame(ts):
        return json.dumps({
            "channel": "trades",
            "data": [{"coin": "BTC", "px": "1.0", "sz": "1.0", "side": "B",
                      "time": ts, "tid": ts}],
        })

    async def run():
        collector = HyperliquidWebSocketCollector(["BTC", "ETH"])
        # first BTC trades message: the snapshot — skipped
        await collector.process_message(trades_frame(exchange_ms - 60_000),
                                        recv_ts_ms=float(exchange_ms), recv_mono_ns=1)
        assert collector.get_stats()["latency_ms"]["trades"]["count"] == 0
        # second BTC trades message: live — recorded
        await collector.process_message(trades_frame(exchange_ms),
                                        recv_ts_ms=float(exchange_ms + 100), recv_mono_ns=2)
        stats = collector.get_stats()["latency_ms"]["trades"]
        assert stats["count"] == 1
        assert stats["max_ms"] <= 200  # the 60s-old snapshot never polluted stats
        # the snapshot points themselves still flowed through (stamped, queued)
        assert collector._queue.qsize() == 2

    asyncio.run(run())


def test_unstamped_messages_record_nothing():
    async def run():
        collector = HyperliquidWebSocketCollector(["BTC"])
        l2 = json.dumps({
            "channel": "l2Book",
            "data": {"coin": "BTC", "time": 1_700_000_000_000,
                     "levels": [[{"px": "1", "sz": "1", "n": 1}], []]},
        })
        await collector.process_message(l2)  # no recv stamps (replay/test path)
        assert collector.get_stats()["latency_ms"]["l2Book"] == {"count": 0}

    asyncio.run(run())
