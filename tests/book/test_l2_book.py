"""Behavior contract for L2Book: the collector-shaped snapshot book.

Levels here are exactly what process_l2_book_message produces
({"px": str, "sz": str, "n": int}); each update is a full replacement.
The reads must match L4Book's protocol bit-for-bit so the two are
interchangeable behind BookView.
"""

from hyperliquid_pipeline.book.l2_book import L2Book
from hyperliquid_pipeline.book.l4_book import L4Book
from hyperliquid_pipeline.book.schemas import BookDiff, BookView

BIDS = [
    {"px": "115000.0", "sz": "0.8", "n": 2},
    {"px": "114999.5", "sz": "1.0", "n": 1},
]
ASKS = [
    {"px": "115000.5", "sz": "0.6", "n": 2},
    {"px": "115001.0", "sz": "2.0", "n": 1},
]


def _book():
    book = L2Book("BTC")
    book.update_from_snapshot(BIDS, ASKS, time_ms=1764867590000)
    return book


def test_snapshot_feed_pins_best_mid_depth():
    book = _book()
    assert book.best_bid() == ("115000.0", 0.8)
    assert book.best_ask() == ("115000.5", 0.6)
    assert book.mid() == 115000.25
    assert book.is_crossed() is False
    assert book.last_update_ms == 1764867590000
    assert book.depth(1) == {
        "bids": [{"px": "115000.0", "sz": 0.8, "n": 2}],
        "asks": [{"px": "115000.5", "sz": 0.6, "n": 2}],
    }


def test_each_snapshot_replaces_the_whole_book():
    book = _book()
    book.update_from_snapshot(
        [{"px": "116000.0", "sz": "1.5", "n": 3}], [], time_ms=1764867591000
    )
    assert book.best_bid() == ("116000.0", 1.5)
    assert book.best_ask() is None  # old asks are gone, not merged
    assert book.mid() is None
    assert book.last_update_ms == 1764867591000


def test_misordered_payload_still_reads_best_first():
    book = L2Book("BTC")
    book.update_from_snapshot(list(reversed(BIDS)), list(reversed(ASKS)), time_ms=1)
    assert book.best_bid() == ("115000.0", 0.8)
    assert book.best_ask() == ("115000.5", 0.6)


def test_malformed_levels_counted_not_raised():
    book = L2Book("BTC")
    book.update_from_snapshot(
        [{"px": "115000.0", "sz": "0.8", "n": 2}, {"sz": "no px"}, "junk"],
        [{"px": "not a number", "sz": "1", "n": 1}],
        time_ms=1,
    )
    assert book.best_bid() == ("115000.0", 0.8)
    assert book.best_ask() is None
    assert [a["type"] for a in book.anomalies] == ["bad_level"] * 3


def test_empty_book_reads():
    book = L2Book("BTC")
    assert book.best_bid() is None
    assert book.best_ask() is None
    assert book.mid() is None
    assert book.is_crossed() is False
    assert book.depth(3) == {"bids": [], "asks": []}


def test_crossed_snapshot_detected():
    book = L2Book("BTC")
    book.update_from_snapshot(
        [{"px": "115400.0", "sz": "1.0", "n": 1}],
        [{"px": "115399.0", "sz": "1.0", "n": 1}],
        time_ms=1,
    )
    assert book.is_crossed() is True


def test_satisfies_the_bookview_protocol():
    assert isinstance(L2Book("BTC"), BookView)


def test_depth_shape_matches_l4_book_exactly():
    """An L2 feed and an L4 reconstruction of the same book must be
    indistinguishable through BookView."""
    l2 = _book()

    l4 = L4Book("BTC")
    orders = [
        ("B", "115000.0", 1, "0.5"), ("B", "115000.0", 2, "0.3"),
        ("B", "114999.5", 3, "1.0"),
        ("A", "115000.5", 4, "0.4"), ("A", "115000.5", 5, "0.2"),
        ("A", "115001.0", 6, "2.0"),
    ]
    for side, px, oid, sz in orders:
        l4.apply(
            BookDiff(
                user=None, oid=oid, coin="BTC", side=side, px=px, kind="new", sz=sz
            )
        )

    assert l4.depth(5) == l2.depth(5)
    assert l4.best_bid() == l2.best_bid()
    assert l4.best_ask() == l2.best_ask()
    assert l4.mid() == l2.mid()
