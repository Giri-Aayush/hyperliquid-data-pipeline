"""Behavior contract for L4Book: order-level reconstruction with FIFO queues.

Drives the book directly with constructed diffs (no files, no network) and
pins the semantics Track 1 integrates against: the frozen BookView reads,
queue positions through fills and in-place updates, height-gap staleness,
and the never-raise anomaly path.
"""

import pytest

from hyperliquid_pipeline.book.diff_parser import load_l4_snapshot_file
from hyperliquid_pipeline.book.l4_book import L4Book
from hyperliquid_pipeline.book.schemas import BlockDiffBatch, BookDiff, BookView


def _diff(kind, oid, side="B", px="115000.0", coin="BTC", **kw):
    return BookDiff(
        user=None, oid=oid, coin=coin, side=side, px=px, kind=kind, **kw
    )


def _new(oid, sz, side="B", px="115000.0", coin="BTC"):
    return _diff("new", oid, side=side, px=px, coin=coin, sz=sz)


@pytest.fixture
def snapshot_book(fixtures):
    book = L4Book()
    book.load_snapshot(load_l4_snapshot_file(fixtures / "snapshot_small.json"))
    return book


def test_snapshot_load_pins_best_mid_depth(snapshot_book):
    book = snapshot_book
    assert len(book) == 6
    assert book.best_bid() == ("115000.0", 0.8)   # 0.5 + 0.3, two orders
    assert book.best_ask() == ("115000.5", 0.6)   # 0.4 + 0.2
    assert book.mid() == 115000.25
    assert book.is_crossed() is False
    assert book.last_update_ms == 1764867590000
    assert book.height == 100

    depth = book.depth(1)
    assert depth == {
        "bids": [{"px": "115000.0", "sz": 0.8, "n": 2}],
        "asks": [{"px": "115000.5", "sz": 0.6, "n": 2}],
    }
    full = book.depth(10)  # n beyond available levels returns what exists
    assert [lvl["px"] for lvl in full["bids"]] == ["115000.0", "114999.5"]
    assert [lvl["px"] for lvl in full["asks"]] == ["115000.5", "115001.0"]


def test_new_orders_join_the_back_of_the_fifo_queue():
    book = L4Book("BTC")
    book.apply(_new(1, "1.0", side="A", px="115000.5"))
    book.apply(_new(2, "2.0", side="A", px="115000.5"))
    assert book.queue_position(1) == (0, 0.0)
    assert book.queue_position(2) == (1, 1.0)
    assert book.best_ask() == ("115000.5", 3.0)


def test_queue_position_improves_after_a_fill_ahead(snapshot_book):
    book = snapshot_book
    assert book.queue_position(1002) == (1, 0.5)  # behind 1001's 0.5
    book.apply(_diff("remove", 1001))             # order ahead fills/cancels
    assert book.queue_position(1002) == (0, 0.0)  # now first in line
    assert book.best_bid() == ("115000.0", 0.3)
    assert book.queue_position(1001) is None


def test_queue_position_preserved_across_an_update(snapshot_book):
    book = snapshot_book
    assert book.queue_position(2002) == (1, 0.4)
    # 2001 resizes 0.4 -> 0.15 in place: 2002 keeps its slot, size ahead drops.
    book.apply(
        _diff("update", 2001, side="A", px="115000.5", orig_sz="0.4", new_sz="0.15")
    )
    assert book.queue_position(2001) == (0, 0.0)
    assert book.queue_position(2002) == (1, 0.15)
    assert book.best_ask() == ("115000.5", 0.35)
    assert book.anomalies == []


def test_unknown_oid_update_and_remove_counted_not_raised():
    book = L4Book("BTC")
    book.apply(_diff("update", 999, orig_sz="1.0", new_sz="0.5"))
    book.apply(_diff("remove", 998))
    assert [a["type"] for a in book.anomalies] == [
        "unknown_oid_update", "unknown_oid_remove",
    ]
    assert len(book) == 0
    assert book.best_bid() is None


def test_orig_sz_mismatch_flagged_but_new_size_applied():
    book = L4Book("BTC")
    book.apply(_new(1, "0.5"))
    book.apply(_diff("update", 1, orig_sz="0.4", new_sz="0.3"))  # feed disagrees
    assert book.best_bid() == ("115000.0", 0.3)  # feed wins
    assert [a["type"] for a in book.anomalies] == ["orig_sz_mismatch"]


def test_update_to_zero_removes_the_order():
    book = L4Book("BTC")
    book.apply(_new(1, "0.5"))
    book.apply(_diff("update", 1, orig_sz="0.5", new_sz="0"))
    assert len(book) == 0
    assert book.best_bid() is None
    assert [a["type"] for a in book.anomalies] == ["update_to_zero"]


def test_duplicate_new_is_cancel_replace_at_the_back():
    book = L4Book("BTC")
    book.apply(_new(1, "0.5"))
    book.apply(_new(2, "0.3"))
    book.apply(_new(1, "0.2"))  # re-announced oid
    assert [a["type"] for a in book.anomalies] == ["duplicate_new"]
    assert book.queue_position(1) == (1, 0.3)  # lost its slot, behind 2
    assert book.best_bid() == ("115000.0", 0.5)  # 0.3 + 0.2


def test_remove_drops_empty_level_and_best_moves(snapshot_book):
    book = snapshot_book
    book.apply(_diff("remove", 2001, side="A", px="115000.5"))
    book.apply(_diff("remove", 2002, side="A", px="115000.5"))
    assert book.best_ask() == ("115001.0", 2.0)
    assert len(book.depth(10)["asks"]) == 1


def test_crossed_book_is_flagged():
    book = L4Book("BTC")
    book.apply(_new(1, "1.0", side="B", px="115400.0"))
    book.apply(_new(2, "1.0", side="A", px="115399.0"))
    assert book.is_crossed() is True
    assert book.mid() == 115399.5  # reads keep working while crossed


def test_locked_book_counts_as_crossed():
    book = L4Book("BTC")
    book.apply(_new(1, "1.0", side="B", px="115400.0"))
    book.apply(_new(2, "1.0", side="A", px="115400.0"))
    assert book.is_crossed() is True


def test_height_gap_sets_stale_until_snapshot_resync(fixtures):
    book = L4Book("BTC")

    def block(height, *diffs):
        return BlockDiffBatch(time_ms=height * 1000, height=height, diffs=list(diffs))

    book.apply_block(block(101, _new(1, "1.0")))
    assert book.stale is False
    book.apply_block(block(105, _new(2, "1.0", px="114999.0")))  # 102-104 lost
    assert book.stale is True
    assert [a["type"] for a in book.anomalies] == ["height_gap"]
    book.apply_block(block(106, _new(3, "1.0", px="114998.0")))
    assert book.stale is True  # sticky: only a fresh snapshot clears it

    book.load_snapshot(load_l4_snapshot_file(fixtures / "snapshot_small.json"))
    assert book.stale is False
    assert book.anomalies == []
    assert book.height == 100


def test_apply_block_advances_height_and_clock():
    book = L4Book("BTC")
    book.apply_block(BlockDiffBatch(time_ms=1000, height=101, diffs=[_new(1, "1.0")]))
    book.apply_block(BlockDiffBatch(time_ms=2000, height=102, diffs=[]))
    assert (book.height, book.last_update_ms, book.stale) == (102, 2000, False)


def test_bare_apply_does_not_invent_timestamps():
    book = L4Book("BTC")
    book.apply(_new(1, "1.0"))  # bare event lines carry no time
    assert book.last_update_ms == 0
    book.apply(_new(2, "1.0"), time_ms=42)  # feeds that know the time pass it
    assert book.last_update_ms == 42


def test_coin_adopted_then_mismatches_rejected():
    book = L4Book()
    book.apply(_new(1, "1.0", coin="BTC"))
    assert book.coin == "BTC"
    book.apply(_new(2, "1.0", coin="ETH"))
    assert len(book) == 1  # ETH diff not applied
    assert [a["type"] for a in book.anomalies] == ["coin_mismatch"]


def test_empty_book_reads():
    book = L4Book("BTC")
    assert book.best_bid() is None
    assert book.best_ask() is None
    assert book.mid() is None
    assert book.is_crossed() is False
    assert book.depth(3) == {"bids": [], "asks": []}
    assert book.queue_position(1) is None


def test_satisfies_the_bookview_protocol():
    assert isinstance(L4Book("BTC"), BookView)
