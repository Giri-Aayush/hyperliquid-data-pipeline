"""Tests for the bounded hand-off queue between the socket and the callbacks.

The point of the queue is that the socket read loop never blocks on a slow
callback: under load it drops the oldest points (counted) and keeps draining.
No network — we drive _enqueue/_consume directly.
"""

import asyncio
from datetime import datetime, timezone

from hyperliquid_pipeline.collectors.realtime_collector import (
    HyperliquidWebSocketCollector,
    MarketDataPoint,
)


def _pt(tid):
    return MarketDataPoint(
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        symbol="BTC",
        data_type="trade",
        data={"tid": tid},
    )


def test_enqueue_drops_oldest_when_full():
    c = HyperliquidWebSocketCollector(["BTC"])
    c._queue = asyncio.Queue(maxsize=3)

    for i in range(5):  # 5 into a size-3 queue
        c._enqueue(_pt(i))

    assert c._queue.qsize() == 3
    assert c.dropped_count == 2
    # Oldest two (0, 1) were dropped; the freshest three survive, in order.
    drained = [c._queue.get_nowait().data["tid"] for _ in range(3)]
    assert drained == [2, 3, 4]


def test_enqueue_never_raises_under_burst():
    """The socket-side enqueue must never raise, however far behind we are."""
    c = HyperliquidWebSocketCollector(["BTC"])
    c._queue = asyncio.Queue(maxsize=10)
    for i in range(10_000):  # nobody consuming
        c._enqueue(_pt(i))
    assert c._queue.qsize() == 10
    assert c.dropped_count == 9_990


def test_consumer_runs_sync_and_async_callbacks():
    c = HyperliquidWebSocketCollector(["BTC"])
    seen_sync, seen_async = [], []
    c.add_data_callback(lambda dp: seen_sync.append(dp.data["tid"]))

    async def async_cb(dp):
        seen_async.append(dp.data["tid"])

    c.add_data_callback(async_cb)

    async def run():
        for i in range(4):
            c._enqueue(_pt(i))
        consumer = asyncio.create_task(c._consume())
        await c._queue.join()  # returns once all 4 are processed
        consumer.cancel()
        try:
            await consumer
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    assert seen_sync == [0, 1, 2, 3]
    assert seen_async == [0, 1, 2, 3]  # async callbacks are awaited, not dropped


def test_failing_callback_does_not_stop_the_consumer():
    c = HyperliquidWebSocketCollector(["BTC"])
    seen = []
    c.add_data_callback(lambda dp: (_ for _ in ()).throw(RuntimeError("boom")))
    c.add_data_callback(lambda dp: seen.append(dp.data["tid"]))

    async def run():
        for i in range(3):
            c._enqueue(_pt(i))
        consumer = asyncio.create_task(c._consume())
        await c._queue.join()
        consumer.cancel()
        try:
            await consumer
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    # The second callback still ran for every point despite the first raising.
    assert seen == [0, 1, 2]


def test_get_stats_includes_queue_metrics():
    c = HyperliquidWebSocketCollector(["BTC"])
    c._queue = asyncio.Queue(maxsize=3)
    c._enqueue(_pt(1))
    c._enqueue(_pt(2))

    stats = c.get_stats()
    assert stats["queue_depth"] == 2
    assert stats["queue_maxsize"] == 3
    assert stats["dropped_count"] == 0
