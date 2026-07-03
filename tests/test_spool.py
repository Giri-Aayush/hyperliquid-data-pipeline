"""Tests for the lossless raw-frame spool.

The load-bearing contract: with a tiny drop-oldest processing queue and no
consumer, every raw frame still lands in the spool file — bursts shed load on
the signal path without punching holes in the archive. Plus: envelope
validity, batch writes, hourly rotation with upload, overflow accounting.
"""

import asyncio
import json

from hyperliquid_pipeline.collectors.realtime_collector import HyperliquidWebSocketCollector
from hyperliquid_pipeline.collectors.spool import RawSpool

BASE_MS = 1_700_000_000_000.0  # falls inside some UTC hour


class _FakeStore:
    def __init__(self):
        self.uploads = []

    def put_file(self, path, key):
        self.uploads.append((str(path), key))


def _frame(i):
    return json.dumps({
        "channel": "trades",
        "data": [{"coin": "BTC", "px": "50000.0", "sz": "0.1", "side": "B",
                  "time": int(BASE_MS) + i, "tid": i}],
    })


def test_burst_losslessness_while_processing_queue_drops(tmp_path):
    """50 frames through the read path with a size-3 processing queue and no
    consumer: the queue sheds load, the spool file has all 50."""
    async def run():
        collector = HyperliquidWebSocketCollector(["BTC"])
        collector._queue = asyncio.Queue(maxsize=3)  # tiny hand-off queue
        collector.spool = RawSpool(spool_dir=tmp_path, object_store=None)
        collector.spool.start()

        for i in range(50):
            await collector._on_raw_frame(_frame(i), BASE_MS + i, 1000 + i)

        assert collector.dropped_count > 0  # the signal path did shed load
        await collector.spool.close()

        files = list(tmp_path.rglob("raw_*.jsonl"))
        assert len(files) == 1
        lines = files[0].read_text().splitlines()
        assert len(lines) == 50  # ...but the archive is complete
        # Envelope: valid JSON, raw frame embedded verbatim, stamps present
        record = json.loads(lines[7])
        assert record["recv_ts_ms"] == BASE_MS + 7
        assert record["recv_mono_ns"] == 1007
        assert record["raw"] == json.loads(_frame(7))

    asyncio.run(run())


def test_batch_write_not_per_line(tmp_path):
    """Frames enqueued before the writer starts drain as one batch write."""
    async def run():
        spool = RawSpool(spool_dir=tmp_path, object_store=None)
        for i in range(10):
            spool.enqueue(_frame(i), BASE_MS + i, i)
        spool.start()
        await spool.close()
        assert spool.written_lines == 10
        assert spool.write_batches == 1  # one write() + flush(), not ten

    asyncio.run(run())


def test_hourly_rotation_uploads_finished_file(tmp_path):
    async def run():
        store = _FakeStore()
        spool = RawSpool(spool_dir=tmp_path, object_store=store)
        next_hour_ms = (int(BASE_MS) // 3_600_000 + 1) * 3_600_000
        spool.enqueue(_frame(0), BASE_MS, 0)
        spool.enqueue(_frame(1), float(next_hour_ms + 5), 1)  # crosses the hour
        spool.start()
        await spool.close()

        files = sorted(tmp_path.rglob("raw_*.jsonl"))
        assert len(files) == 2
        assert len(files[0].read_text().splitlines()) == 1
        assert len(files[1].read_text().splitlines()) == 1
        # both files mirrored: the first at rollover, the second at close
        assert len(store.uploads) == 2
        assert all(key.startswith("spool/") for _, key in store.uploads)

    asyncio.run(run())


def test_overflow_counts_and_never_raises(tmp_path):
    async def run():
        spool = RawSpool(spool_dir=tmp_path, object_store=None, queue_maxsize=2)
        results = [spool.enqueue(_frame(i), BASE_MS + i, i) for i in range(5)]
        assert results == [True, True, False, False, False]
        assert spool.spool_dropped == 3
        spool.start()
        await spool.close()
        files = list(tmp_path.rglob("raw_*.jsonl"))
        assert len(files[0].read_text().splitlines()) == 2  # what fit, kept

    asyncio.run(run())


def test_close_is_idempotent_and_rejects_late_frames(tmp_path):
    async def run():
        spool = RawSpool(spool_dir=tmp_path, object_store=None)
        spool.start()
        spool.enqueue(_frame(0), BASE_MS, 0)
        await spool.close()
        await spool.close()  # no-op
        assert spool.enqueue(_frame(1), BASE_MS + 1, 1) is False
        files = list(tmp_path.rglob("raw_*.jsonl"))
        assert len(files[0].read_text().splitlines()) == 1

    asyncio.run(run())


def test_stats_shape(tmp_path):
    async def run():
        spool = RawSpool(spool_dir=tmp_path, object_store=None)
        spool.enqueue(_frame(0), BASE_MS, 0)
        stats = spool.stats()
        assert stats["queued"] == 1
        assert stats["spool_dropped"] == 0
        spool.start()
        await spool.close()
        assert spool.stats()["written_lines"] == 1

    asyncio.run(run())
