"""Tests for BatchingStorage — buffering writes and flushing them in batches.

The point is to turn one DB round-trip per point into one per batch, off the
hot path. We use a fake inner store that records the batches it receives.
"""

import asyncio
from datetime import datetime, timezone

from hyperliquid_pipeline.collectors.realtime_collector import MarketDataPoint
from hyperliquid_pipeline.storage.database import BatchingStorage, DataStorage


class _FakeStorage(DataStorage):
    """Records each batch handed to store_data_points; no real I/O."""

    def __init__(self):
        self.batches = []
        self.closed = False

    async def store_data_point(self, data_point):  # not used by BatchingStorage
        return True

    async def store_data_points(self, data_points):
        self.batches.append(list(data_points))
        return len(data_points)

    async def get_data(self, symbol, data_type, start_time, end_time):
        return []

    async def close(self):
        self.closed = True


def _pt(i):
    return MarketDataPoint(
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        symbol="BTC",
        data_type="processed",
        data={"tid": i},
    )


def test_buffers_until_flush_then_one_batch():
    async def run():
        fake = _FakeStorage()
        bs = BatchingStorage(fake, batch_size=100, flush_interval=999)
        for i in range(5):
            await bs.store_data_point(_pt(i))
        assert fake.batches == []  # below batch size, no interval -> nothing written
        await bs.flush()
        assert len(fake.batches) == 1
        assert [p.data["tid"] for p in fake.batches[0]] == [0, 1, 2, 3, 4]

    asyncio.run(run())


def test_auto_flush_at_batch_size():
    async def run():
        fake = _FakeStorage()
        bs = BatchingStorage(fake, batch_size=3, flush_interval=999)
        for i in range(7):
            await bs.store_data_point(_pt(i))
        assert [len(b) for b in fake.batches] == [3, 3]  # two full batches, one buffered
        await bs.close()
        assert [len(b) for b in fake.batches] == [3, 3, 1]
        assert fake.closed is True

    asyncio.run(run())


def test_close_flushes_tail_and_closes_inner():
    async def run():
        fake = _FakeStorage()
        bs = BatchingStorage(fake, batch_size=1000, flush_interval=999)
        await bs.store_data_point(_pt(1))
        await bs.close()
        assert [p.data["tid"] for p in fake.batches[0]] == [1]
        assert fake.closed is True

    asyncio.run(run())


def test_periodic_flush_fires():
    async def run():
        fake = _FakeStorage()
        bs = BatchingStorage(fake, batch_size=1000, flush_interval=0.05)
        bs.start()
        await bs.store_data_point(_pt(1))
        await asyncio.sleep(0.15)  # let the background flush fire
        assert fake.batches and fake.batches[0][0].data["tid"] == 1
        await bs.close()

    asyncio.run(run())


def test_store_data_points_batches_at_threshold():
    async def run():
        fake = _FakeStorage()
        bs = BatchingStorage(fake, batch_size=5, flush_interval=999)
        assert await bs.store_data_points([_pt(i) for i in range(3)]) == 3
        assert fake.batches == []  # 3 < 5
        await bs.store_data_points([_pt(i) for i in range(3, 6)])  # now 6 >= 5
        assert sum(len(b) for b in fake.batches) == 6
        await bs.close()

    asyncio.run(run())


def test_get_data_flushes_first():
    async def run():
        fake = _FakeStorage()
        bs = BatchingStorage(fake, batch_size=1000, flush_interval=999)
        await bs.store_data_point(_pt(1))
        await bs.get_data(
            "BTC", "trade",
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        # reads flush pending writes first, so the buffered point is persisted
        assert fake.batches and len(fake.batches[0]) == 1
        await bs.close()

    asyncio.run(run())


# --- hardening: failure, overflow, graceful close --------------------------------

class _SlowStorage(DataStorage):
    """store_data_points takes time, to exercise the close-during-flush race."""

    def __init__(self, delay):
        self.delay = delay
        self.batches = []

    async def store_data_point(self, dp):
        return True

    async def store_data_points(self, dps):
        await asyncio.sleep(self.delay)
        self.batches.append(list(dps))
        return len(dps)

    async def get_data(self, *a):
        return []

    async def close(self):
        pass


class _FlakyStorage(DataStorage):
    """Fails the first N flushes, then succeeds — a transient DB outage."""

    def __init__(self, fail_times):
        self.fail_times = fail_times
        self.batches = []

    async def store_data_point(self, dp):
        return True

    async def store_data_points(self, dps):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("transient backend failure")
        self.batches.append(list(dps))
        return len(dps)

    async def get_data(self, *a):
        return []

    async def close(self):
        pass


def test_failed_flush_requeues_batch_for_retry():
    async def run():
        flaky = _FlakyStorage(fail_times=1)
        bs = BatchingStorage(flaky, batch_size=1000, flush_interval=999)
        for i in range(3):
            await bs.store_data_point(_pt(i))
        await bs.flush()  # first attempt raises -> points re-queued, not dropped
        assert flaky.batches == []
        await bs.flush()  # retry succeeds
        assert [p.data["tid"] for p in flaky.batches[0]] == [0, 1, 2]

    asyncio.run(run())


class _DeadStorage(DataStorage):
    """Never raises but stores nothing — how MultiStorage reports a total outage.

    MultiStorage gathers with return_exceptions=True and each backend swallows
    its own errors, so a full outage surfaces as store_data_points() == 0, not
    as an exception. The batcher must treat that as a failed flush too.
    """

    def __init__(self, dead_flushes):
        self.dead_flushes = dead_flushes
        self.batches = []

    async def store_data_point(self, dp):
        return False

    async def store_data_points(self, dps):
        if self.dead_flushes > 0:
            self.dead_flushes -= 1
            return 0
        self.batches.append(list(dps))
        return len(dps)

    async def get_data(self, *a):
        return []

    async def close(self):
        pass


def test_zero_stored_flush_requeues_batch_for_retry():
    async def run():
        dead = _DeadStorage(dead_flushes=1)
        bs = BatchingStorage(dead, batch_size=1000, flush_interval=999)
        for i in range(3):
            await bs.store_data_point(_pt(i))
        await bs.flush()  # inner returned 0 without raising -> re-queued, not dropped
        assert dead.batches == []
        await bs.flush()  # backends recovered -> original batch lands intact, in order
        assert [p.data["tid"] for p in dead.batches[0]] == [0, 1, 2]

    asyncio.run(run())


def test_buffer_cap_drops_oldest():
    async def run():
        fake = _FakeStorage()
        bs = BatchingStorage(fake, batch_size=10000, flush_interval=999, max_buffer=3)
        for i in range(6):
            await bs.store_data_point(_pt(i))
        await bs.flush()
        assert [p.data["tid"] for p in fake.batches[0]] == [3, 4, 5]  # oldest 3 dropped
        assert bs.dropped_count == 3
        await bs.close()

    asyncio.run(run())


def test_close_does_not_lose_in_flight_batch():
    async def run():
        slow = _SlowStorage(delay=0.1)
        bs = BatchingStorage(slow, batch_size=1000, flush_interval=0.02)
        bs.start()
        for i in range(5):
            await bs.store_data_point(_pt(i))
        await asyncio.sleep(0.04)  # a periodic flush is now mid-write
        await bs.close()           # graceful: waits for the in-flight write
        got = sorted(p.data["tid"] for b in slow.batches for p in b)
        assert got == [0, 1, 2, 3, 4]  # nothing lost to the shutdown race

    asyncio.run(run())


def test_close_is_idempotent_and_rejects_late_writes():
    async def run():
        fake = _FakeStorage()
        bs = BatchingStorage(fake, batch_size=1000, flush_interval=999)
        await bs.store_data_point(_pt(1))
        await bs.close()
        await bs.close()  # second close is a no-op, not an error
        assert fake.closed is True
        # a write after close is rejected, not silently buffered then lost
        assert await bs.store_data_point(_pt(2)) is False

    asyncio.run(run())


def test_asset_ctx_folded_into_processed_point():
    """OI/mark/basis must reach the DB via the processed summary, not just JSONL."""
    import sys
    from datetime import datetime, timezone
    from hyperliquid_pipeline.processors.data_processor import DataProcessor
    from hyperliquid_pipeline.collectors.realtime_collector import MarketDataPoint

    async def run():
        captured = []

        class _Store(DataStorage):
            async def store_data_point(self, dp):
                captured.append(dp)
                return True
            async def store_data_points(self, dps):
                captured.extend(dps)
                return len(dps)
            async def get_data(self, *a):
                return []
            async def close(self):
                pass

        dp = DataProcessor(_Store())
        now = datetime.now(timezone.utc)
        # asset_ctx arrives first, then a trade triggers a processed summary
        await dp.process_market_data(MarketDataPoint(
            timestamp=now, symbol="BTC", data_type="asset_ctx",
            data={"mark_price": 50010.0, "oracle_price": 50000.0,
                  "open_interest": 1234.5, "basis": 10.0, "basis_bps": 2.0},
        ))
        await dp.process_market_data(MarketDataPoint(
            timestamp=now, symbol="BTC", data_type="trade",
            data={"price": 50005.0, "size": 0.1, "side": "buy"},
        ))
        processed = [p for p in captured if p.data_type == "processed"]
        assert processed, "no processed point stored"
        ctx = processed[-1].data.get("asset_ctx")
        assert ctx is not None
        assert ctx["open_interest"] == 1234.5
        assert ctx["basis"] == 10.0

    asyncio.run(run())
