"""Tests for QuickNodeFeed: StreamL4Book pb messages -> per-coin L4 books.

No network anywhere: the feed's consume path takes any (a)iterable of
L4BookUpdate messages, so every test drives it with pb objects built
directly. This pins the contract before the paywalled live stream exists —
the moment the plan upgrade lands, stream() reuses exactly this path.
"""

import asyncio
import json

import pytest

from hyperliquid_pipeline.book.diff_parser import UnrecognizedDiffFormat
from hyperliquid_pipeline.collectors._qn_pb import orderbook_pb2 as pb
from hyperliquid_pipeline.collectors.quicknode_feed import QuickNodeFeed
from hyperliquid_pipeline.config import settings


def _order(oid, side, px, sz):
    return pb.L4Order(
        user="0x" + "aa" * 20,
        coin="BTC",
        side=side,
        limit_px=px,
        sz=sz,
        oid=oid,
        timestamp=1764867580000,
        trigger_condition="N/A",
        is_trigger=False,
        trigger_px="",
        is_position_tpsl=False,
        reduce_only=False,
        order_type="Limit",
        tif="Gtc",
    )


def _snapshot(height=100, time_ms=1764867590000):
    return pb.L4BookUpdate(
        snapshot=pb.L4BookSnapshot(
            coin="BTC",
            time=time_ms,
            height=height,
            bids=[_order(1001, "B", "115000.0", "0.5"), _order(1002, "B", "115000.0", "0.3")],
            asks=[_order(2001, "A", "115000.5", "0.4")],
        )
    )


def _event(oid, px, sz=None, side="B", kind="new", orig=None, new=None):
    raw = {"new": {"sz": sz}} if kind == "new" else (
        "remove" if kind == "remove" else {"update": {"origSz": orig, "newSz": new}}
    )
    return {
        "user": "0x" + "bb" * 20, "oid": oid, "coin": "BTC",
        "side": side, "px": px, "raw_book_diff": raw,
    }


def _diff(height, book_diffs, time_ms=None):
    return pb.L4BookUpdate(
        diff=pb.L4BookDiff(
            time=time_ms or height * 1000,
            height=height,
            data=json.dumps({"order_statuses": [], "book_diffs": book_diffs}),
        )
    )


def _consume(feed, updates, coin="BTC"):
    asyncio.run(feed._consume_stream(coin, updates))


def _feed(**kwargs):
    kwargs.setdefault("coins", ["BTC"])
    kwargs.setdefault("endpoint", "test.invalid:443")
    return QuickNodeFeed(**kwargs)


def test_snapshot_then_diffs_end_to_end():
    feed = _feed()
    points = []
    feed.add_data_callback(points.append)
    _consume(
        feed,
        [
            _snapshot(),
            _diff(101, [
                _event(1003, "114999.5", sz="1.0"),
                _event(1001, "115000.0", kind="remove"),
            ]),
            _diff(102, [
                _event(2001, "115000.5", side="A", kind="update", orig="0.4", new="0.15"),
            ]),
        ],
    )

    book = feed.books["BTC"]
    assert book.best_bid() == ("115000.0", 0.3)   # 1001 removed, 1002 remains
    assert book.best_ask() == ("115000.5", 0.15)  # 2001 resized in place
    assert (book.height, book.stale) == (102, False)
    assert book.anomaly_count == 0

    assert [p.data["height"] for p in points] == [101, 102]
    assert all(p.data_type == "book_diff" and p.symbol == "BTC" for p in points)
    assert all(p.recv_ts_ms is not None and p.recv_mono_ns is not None for p in points)

    stats = feed.get_stats()
    assert (stats["blocks"], stats["diffs"], stats["snapshots_loaded"]) == (2, 3, 1)
    assert stats["anomalies_total"] == 0 and stats["stale_coins"] == []


def test_snapshot_preserves_fifo_and_optional_fields():
    feed = _feed()
    _consume(feed, [_snapshot()])
    book = feed.books["BTC"]
    assert len(book) == 3
    assert book.queue_position(1002) == (1, 0.5)  # behind 1001, pb list order
    no_tif = pb.L4BookUpdate(
        snapshot=pb.L4BookSnapshot(
            coin="BTC", time=1, height=200,
            bids=[pb.L4Order(oid=1, side="B", limit_px="1.0", sz="2.0")],
            asks=[],
        )
    )
    _consume(feed, [no_tif])  # unset optional tif and empty strings parse fine
    assert feed.books["BTC"].best_bid() == ("1.0", 2.0)
    assert feed.snapshots_loaded == 2


def test_async_iterator_source_matches_sync():
    """grpc.aio calls are async iterators; the consume path must treat them
    identically to plain lists."""
    updates = [_snapshot(), _diff(101, [_event(7, "115000.0", sz="1.0")])]

    sync_feed = _feed()
    _consume(sync_feed, updates)

    async_feed = _feed()

    async def agen():
        for update in updates:
            yield update

    asyncio.run(async_feed._consume_stream("BTC", agen()))
    assert async_feed.books["BTC"].checksum() == sync_feed.books["BTC"].checksum()
    assert async_feed.get_stats() == sync_feed.get_stats()


def test_resubscribe_snapshot_clears_stale_after_gap():
    feed = _feed()
    _consume(feed, [_snapshot(), _diff(101, [_event(7, "115000.0", sz="1.0")])])
    _consume(feed, [_diff(105, [_event(8, "114999.0", sz="1.0")])])  # gap
    assert feed.books["BTC"].stale is True
    assert feed.get_stats()["stale_coins"] == ["BTC"]

    # Reconnect => the server re-sends a snapshot => the book self-heals.
    _consume(feed, [_snapshot(height=200, time_ms=1764867600000)])
    assert feed.books["BTC"].stale is False
    assert feed.get_stats()["stale_coins"] == []
    assert feed.books["BTC"].height == 200


def test_strict_mode_raises_on_bad_diff_json():
    bad = pb.L4BookUpdate(diff=pb.L4BookDiff(time=1, height=2, data="not json"))
    with pytest.raises(UnrecognizedDiffFormat):
        _consume(_feed(strict=True), [bad])

    tolerant = _feed()
    _consume(tolerant, [bad])
    assert tolerant.parse_skips == 1
    assert tolerant.blocks == 0


def test_bad_block_element_counts_skip_but_block_applies():
    feed = _feed()
    diff = pb.L4BookUpdate(
        diff=pb.L4BookDiff(
            time=101000,
            height=101,
            data=json.dumps({
                "order_statuses": [],
                "book_diffs": [_event(7, "115000.0", sz="1.0"), {"junk": True}],
            }),
        )
    )
    _consume(feed, [diff])
    assert feed.parse_skips == 1
    assert feed.blocks == 1
    assert feed.books["BTC"].best_bid() == ("115000.0", 1.0)


def test_quiet_block_advances_clock_without_emitting():
    feed = _feed()
    points = []
    feed.add_data_callback(points.append)
    _consume(feed, [_snapshot(), _diff(101, [])])
    assert points == []
    assert feed.blocks == 1
    assert feed.books["BTC"].height == 101
    assert feed.books["BTC"].last_update_ms == 101000


def test_empty_oneof_is_counted_not_fatal():
    feed = _feed()
    _consume(feed, [pb.L4BookUpdate(), _snapshot()])
    assert feed.parse_skips == 1
    assert feed.snapshots_loaded == 1  # the stream keeps going


def test_bytes_received_accumulates_message_sizes():
    updates = [_snapshot(), _diff(101, [_event(7, "115000.0", sz="1.0")])]
    feed = _feed()
    _consume(feed, updates)
    assert feed.get_stats()["bytes_received"] == sum(u.ByteSize() for u in updates)
    assert feed.get_stats()["bytes_received"] > 0


def test_callbacks_match_collector_semantics():
    feed = _feed()
    seen_sync, seen_async = [], []

    def boom(_point):
        raise RuntimeError("boom")

    async def async_cb(point):
        seen_async.append(point.data["height"])

    feed.add_data_callback(boom)
    feed.add_data_callback(lambda p: seen_sync.append(p.data["height"]))
    feed.add_data_callback(async_cb)
    _consume(
        feed,
        [
            _snapshot(),
            _diff(101, [_event(7, "115000.0", sz="1.0")]),
            _diff(102, [_event(8, "114999.5", sz="1.0")]),
        ],
    )
    assert seen_sync == [101, 102]
    assert seen_async == [101, 102]


def test_backoff_mirrors_the_collector_convention():
    feed = _feed()
    base = float(settings.websocket_reconnect_delay)
    cap = float(settings.websocket_reconnect_max_delay)
    for _ in range(50):
        assert 0.0 <= feed._next_reconnect_delay(1) <= base
        assert 0.0 <= feed._next_reconnect_delay(3) <= min(cap, base * 4)
        assert 0.0 <= feed._next_reconnect_delay(50) <= cap


def test_stream_requires_an_endpoint():
    feed = QuickNodeFeed(endpoint=None, coins=["BTC"])
    feed.endpoint = None  # regardless of settings, unconfigured must fail loud
    with pytest.raises(ValueError):
        asyncio.run(feed.stream())
