"""Tests for NodeDiffFeed: node raw-book-diff files -> per-coin L4 books.

Fixture-driven (reuses tests/fixtures/book/), no network. Pins the contract
the orchestrator integrates against: collector-identical callback semantics,
one MarketDataPoint per (block, coin present), every-block height continuity
(no false stale on quiet coins), parse-skip accounting, strict passthrough,
and a tail() that survives a missing data dir and follows appends/hour rolls.
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from hyperliquid_pipeline.book.diff_parser import (
    UnrecognizedDiffFormat,
    load_l4_snapshot_file,
)
from hyperliquid_pipeline.book.l4_book import L4Book
from hyperliquid_pipeline.collectors.node_feed import NodeDiffFeed

FIXTURES = Path(__file__).parent / "fixtures" / "book"


def _new_event(coin, oid, px, sz, side="B"):
    return {
        "user": "0x" + "ee" * 20,
        "oid": oid,
        "coin": coin,
        "side": side,
        "px": px,
        "raw_book_diff": {"new": {"sz": sz}},
    }


def _block_line(height, *events):
    return json.dumps(
        {
            "time": height * 1000,
            "height": height,
            "data": {"order_statuses": [], "book_diffs": list(events)},
        }
    )


def _seeded_feed(**kwargs):
    """Feed with BTC book seeded from the snapshot fixture, the way an
    integrator would: books is the exposed consumer surface."""
    feed = NodeDiffFeed(**kwargs)
    book = L4Book("BTC")
    book.load_snapshot(load_l4_snapshot_file(FIXTURES / "snapshot_small.json"))
    feed.books["BTC"] = book
    return feed


def test_replay_from_snapshot_seed_matches_replay_module_numbers():
    feed = _seeded_feed()
    feed.replay_files([FIXTURES / "block_envelope.jsonl"])

    book = feed.books["BTC"]
    assert book.best_bid() == ("115000.0", 0.55)
    assert book.best_ask() == ("115000.5", 0.25)
    assert book.height == 103
    assert book.last_update_ms == 1764867593000
    assert isinstance(book.last_update_ms, int)  # protocol type must not drift
    assert book.stale is False

    stats = feed.get_stats()
    assert stats["blocks"] == 3
    assert stats["diffs"] == 6
    assert stats["parse_skips"] == 0
    assert stats["anomalies_total"] == 0
    assert stats["stale_coins"] == []
    assert stats["current_file"].endswith("block_envelope.jsonl")


def test_replay_unseeded_counts_unknown_oids_as_anomalies():
    """Replaying diffs without their snapshot is legal but must be visible."""
    feed = NodeDiffFeed()
    feed.replay_files([FIXTURES / "block_envelope.jsonl"])
    book = feed.books["BTC"]
    # Only the two fresh orders exist; the updates (2001, 1003) and removes
    # (1001, 2002) all hit unknown oids.
    assert book.best_bid() == ("115000.0", 0.25)
    assert book.best_ask() == ("115000.5", 0.1)
    assert feed.get_stats()["anomalies_total"] == 4


def test_callbacks_match_collector_semantics():
    """Sync + async callbacks run serially per point; one failing callback
    never stops the feed or starves the others (collector parity)."""
    feed = _seeded_feed()
    seen_sync, seen_async = [], []

    def boom(_point):
        raise RuntimeError("boom")

    async def async_cb(point):
        seen_async.append(point)

    feed.add_data_callback(boom)
    feed.add_data_callback(seen_sync.append)
    feed.add_data_callback(async_cb)
    feed.replay_files([FIXTURES / "block_envelope.jsonl"])

    assert [p.data["height"] for p in seen_sync] == [101, 102, 103]
    assert [p.data["height"] for p in seen_async] == [101, 102, 103]

    point = seen_sync[-1]
    assert point.symbol == "BTC"
    assert point.data_type == "book_diff"
    assert point.timestamp == datetime.fromtimestamp(
        1764867593000 / 1000, tz=timezone.utc
    )
    assert point.data["n_diffs"] == 2
    assert point.data["best_bid"] == ("115000.0", 0.55)
    assert point.data["mid"] == 115000.25
    assert point.data["crossed"] is False
    assert point.data["stale"] is False
    # recv stamps are set at line-read time, collector convention.
    assert point.recv_ts_ms is not None
    assert point.recv_mono_ns is not None


def test_coin_filter_restricts_tracking():
    feed = NodeDiffFeed(coins=["BTC"])
    feed.replay_files([FIXTURES / "events_per_line.jsonl"])
    assert set(feed.books) == {"BTC"}
    assert feed.get_stats()["diffs"] == 5  # the CHILLGUY line is filtered

    chillguy_only = NodeDiffFeed(coins=["CHILLGUY"])
    chillguy_only.replay_files([FIXTURES / "events_per_line.jsonl"])
    assert set(chillguy_only.books) == {"CHILLGUY"}
    assert chillguy_only.books["CHILLGUY"].best_bid() == ("1.36", 186910.0)


def test_bare_events_update_books_but_emit_no_points():
    """Points are per (block, coin); bare event lines carry no block."""
    feed = NodeDiffFeed()
    points = []
    feed.add_data_callback(points.append)
    feed.replay_files([FIXTURES / "events_per_line.jsonl"])
    assert points == []
    assert feed.books["BTC"].best_bid() == ("115323.2", 0.707)
    assert feed.books["BTC"].best_ask() is None
    assert feed.get_stats()["diffs"] == 6  # 5 BTC + 1 CHILLGUY, all tracked


def test_every_book_sees_every_block_so_quiet_coins_never_go_stale(tmp_path):
    path = tmp_path / "two_coins.jsonl"
    path.write_text(
        "\n".join(
            [
                _block_line(101, _new_event("ETH", 1, "3000.0", "1.0")),
                _block_line(102, _new_event("BTC", 2, "115000.0", "1.0")),
                _block_line(103, _new_event("ETH", 3, "2999.5", "2.0")),
            ]
        )
        + "\n"
    )
    feed = NodeDiffFeed()
    points = []
    feed.add_data_callback(points.append)
    feed.replay_files([path])

    # ETH skipped block 102 and BTC skipped 101/103, yet neither is stale:
    # both books saw every height via empty batches.
    assert feed.get_stats()["stale_coins"] == []
    assert feed.books["ETH"].height == 103
    assert feed.books["BTC"].height == 103
    assert feed.books["BTC"].last_update_ms == 103000
    # Points only for coins actually present in each block.
    assert [(p.symbol, p.data["height"]) for p in points] == [
        ("ETH", 101), ("BTC", 102), ("ETH", 103),
    ]

    gapped = NodeDiffFeed()
    gapped.replay_files([FIXTURES / "height_gap.jsonl"])
    assert gapped.get_stats()["stale_coins"] == ["BTC"]


def test_parse_skips_counts_dropped_lines_and_block_elements(tmp_path):
    path = tmp_path / "dirty.jsonl"
    path.write_text(
        "garbage line\n"
        + _block_line(
            101,
            _new_event("BTC", 1, "115000.0", "1.0"),
            {"totally": "wrong"},
        )
        + "\n"
    )
    feed = NodeDiffFeed()
    feed.replay_files([path])
    stats = feed.get_stats()
    assert stats["parse_skips"] == 2  # one whole line + one block element
    assert stats["blocks"] == 1      # the dirty block still applied
    assert feed.books["BTC"].best_bid() == ("115000.0", 1.0)


def test_strict_mode_propagates_from_the_parser(tmp_path):
    path = tmp_path / "bad.jsonl"
    path.write_text("garbage line\n")
    feed = NodeDiffFeed(strict=True)
    with pytest.raises(UnrecognizedDiffFormat):
        feed.replay_files([path])


def test_tail_survives_missing_dir_then_follows_appends_and_hour_roll(tmp_path):
    data_dir = tmp_path / "node"

    async def run():
        feed = NodeDiffFeed(data_dir=str(data_dir), poll_interval=0.01)
        points = []
        feed.add_data_callback(points.append)
        task = asyncio.create_task(feed.tail())

        await asyncio.sleep(0.05)  # data dir doesn't exist yet: no crash
        hour_dir = data_dir / "hourly" / "20261203"
        hour_dir.mkdir(parents=True)
        first_hour = hour_dir / "18"
        first_hour.write_text(_block_line(101, _new_event("BTC", 1, "115000.0", "1.0")) + "\n")
        await asyncio.sleep(0.15)

        with first_hour.open("a") as fh:  # live append to the same file
            fh.write(_block_line(102, _new_event("BTC", 2, "115000.5", "1.0", side="A")) + "\n")
        await asyncio.sleep(0.15)

        # New hour, new file: the feed must roll over to it.
        (hour_dir / "19").write_text(
            _block_line(103, _new_event("BTC", 3, "114999.0", "2.0")) + "\n"
        )
        await asyncio.sleep(0.2)

        feed.stop()
        await asyncio.wait_for(task, timeout=2)
        return feed, points

    feed, points = asyncio.run(run())
    assert [p.data["height"] for p in points] == [101, 102, 103]
    assert feed.get_stats()["current_file"].endswith("19")
    assert feed.books["BTC"].height == 103
    assert feed.books["BTC"].stale is False


def test_connect_l4_ws_is_a_documented_stub():
    feed = NodeDiffFeed()
    with pytest.raises(NotImplementedError) as excinfo:
        feed.connect_l4_ws()
    assert "l4Book" in str(excinfo.value)
