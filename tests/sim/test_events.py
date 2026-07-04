"""Tests for sim replay sources.

The load-bearing contract: trades are delivered BEFORE the book state that
already reflects them (same-timestamp ordering), the shared view updates in
place per event, and archive replay (no trades) stays book-only.
"""

import json
from pathlib import Path

import pytest

from hyperliquid_pipeline.sim.events import (
    iter_archive_events,
    iter_capture_events,
    iter_l4_events,
)
from hyperliquid_pipeline.sim.types import BookEvent, TradeEvent

T0 = 1_700_000_000_000


def _write_jsonl(path: Path, records):
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")


def _orderbook_record(t_ms, bid_px, ask_px, symbol="BTC"):
    return {
        "timestamp": "2026-07-03T00:00:00+00:00", "symbol": symbol,
        "data_type": "orderbook",
        "data": {
            "bids": [{"px": str(bid_px), "sz": "2.0", "n": 2}],
            "asks": [{"px": str(ask_px), "sz": "1.0", "n": 1}],
            "timestamp_ms": t_ms,
        },
        "recv_ts_ms": float(t_ms + 300), "recv_mono_ns": 1,
    }


def _trade_record(t_ms, px, side="B", symbol="BTC"):
    return {
        "timestamp": "2026-07-03T00:00:00+00:00", "symbol": symbol,
        "data_type": "trade",
        "data": {"price": px, "size": 0.5, "side": side,
                 "timestamp_ms": t_ms, "trade_id": t_ms},
        "recv_ts_ms": float(t_ms + 300), "recv_mono_ns": 1,
    }


@pytest.fixture
def capture_dir(tmp_path):
    _write_jsonl(tmp_path / "BTC_orderbook_20260703.jsonl", [
        _orderbook_record(T0, 100, 101),
        _orderbook_record(T0 + 1000, 99, 100),   # same ms as the trade below
    ])
    _write_jsonl(tmp_path / "BTC_trade_20260703.jsonl", [
        _trade_record(T0 + 1000, 100.0, side="A"),  # sell aggressor hits the bid
    ])
    return tmp_path


def test_trades_delivered_before_same_timestamp_book(capture_dir):
    events = list(iter_capture_events(capture_dir, "BTC"))
    kinds = [type(e).__name__ for e in events]
    assert kinds == ["BookEvent", "TradeEvent", "BookEvent"]
    trade = events[1]
    assert isinstance(trade, TradeEvent)
    assert trade.side == "A"          # aggressor convention preserved
    assert trade.px == "100.0"
    assert trade.recv_ts_ms == float(T0 + 1300)


def test_shared_view_updates_in_place(capture_dir):
    events = iter_capture_events(capture_dir, "BTC")
    first = next(events)
    assert isinstance(first, BookEvent)
    assert first.view.best_bid() == ("100", 2.0)
    view = first.view
    *_, last = events
    assert last.view is view          # one shared book instance (contract)
    assert view.best_bid() == ("99", 2.0)
    assert last.height is None        # L2 capture carries no heights


def test_other_symbols_filtered(tmp_path):
    _write_jsonl(tmp_path / "BTC_orderbook_20260703.jsonl", [
        _orderbook_record(T0, 100, 101, symbol="ETH"),  # wrong coin inside file
        _orderbook_record(T0 + 1, 100, 101, symbol="BTC"),
    ])
    events = list(iter_capture_events(tmp_path, "BTC"))
    assert len(events) == 1


def test_missing_capture_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(iter_capture_events(tmp_path, "BTC"))


def test_archive_replay_is_book_only(tmp_path):
    # real wrapper format, verbatim shape from the live bucket
    lines = [
        {"time": "2026-04-01T09:00:01.284869503", "ver_num": 1,
         "raw": {"channel": "l2Book", "data": {
             "coin": "SOL", "time": T0,
             "levels": [[{"px": "83.58", "sz": "10.0", "n": 3}],
                        [{"px": "83.59", "sz": "5.0", "n": 1}]]}}},
        {"time": "2026-04-01T09:00:02.0", "ver_num": 1,
         "raw": {"channel": "l2Book", "data": {
             "coin": "SOL", "time": T0 + 700,
             "levels": [[{"px": "83.57", "sz": "9.0", "n": 2}],
                        [{"px": "83.58", "sz": "4.0", "n": 1}]]}}},
    ]
    path = tmp_path / "hour.jsonl"
    _write_jsonl(path, lines)
    events = list(iter_archive_events([path], "SOL"))
    assert all(isinstance(e, BookEvent) for e in events)
    assert len(events) == 2
    assert events[-1].view.best_bid() == ("83.57", 9.0)


def test_l4_replay_carries_batches_and_heights(tmp_path):
    lines = [
        {"time": T0, "height": 100, "data": {"order_statuses": [], "book_diffs": [
            {"user": "0x1", "oid": 1, "coin": "BTC", "side": "B", "px": "100",
             "raw_book_diff": {"new": {"sz": "1.0"}}},
            {"user": "0x1", "oid": 2, "coin": "BTC", "side": "A", "px": "101",
             "raw_book_diff": {"new": {"sz": "2.0"}}},
        ]}},
        {"time": T0 + 100, "height": 101, "data": {"order_statuses": [], "book_diffs": [
            {"user": "0x2", "oid": 3, "coin": "ETH", "side": "B", "px": "50",
             "raw_book_diff": {"new": {"sz": "9.0"}}},   # other coin: filtered
        ]}},
    ]
    path = tmp_path / "diffs.jsonl"
    _write_jsonl(path, lines)
    events = list(iter_l4_events([path], "BTC"))
    assert [e.height for e in events] == [100, 101]      # quiet block still fires
    assert events[0].batch is not None and len(events[0].batch.diffs) == 2
    assert len(events[1].batch.diffs) == 0               # ETH diff filtered out
    assert events[1].view.best_bid() == ("100", 1.0)
    assert events[1].view.stale is False                 # heights consecutive
