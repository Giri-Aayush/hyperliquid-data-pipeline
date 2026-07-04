"""Behavior contract for QueueSim: virtual maker orders on replayed books.

Drives the sim with real L2Book/L4Book instances (the actual BookView
implementations) and hand-computed scripts. Pins the locked v1.1 contract:
trades-before-book ordering, the three L2 cancel bounds and their bracket
property, the reality clamp, EXACT-mode no-double-count, through-fills,
aggressor-side convention, and the stale/crossed accounting.
"""

import pytest

from hyperliquid_pipeline.book.l2_book import L2Book
from hyperliquid_pipeline.book.l4_book import L4Book
from hyperliquid_pipeline.book.schemas import BlockDiffBatch, BookDiff
from hyperliquid_pipeline.sim.queue import QueueSim
from hyperliquid_pipeline.sim.types import QueueBound, TradeEvent

BOUNDS_L2 = (QueueBound.PESSIMISTIC, QueueBound.PRORATA, QueueBound.OPTIMISTIC)


def _l2_view(bid_sz=5.0, ask_sz=4.0, bid_px="100.0", ask_px="100.5", t=1000):
    book = L2Book("BTC")
    book.update_from_snapshot(
        [{"px": bid_px, "sz": str(bid_sz), "n": 3}],
        [{"px": ask_px, "sz": str(ask_sz), "n": 2}],
        time_ms=t,
    )
    return book


def _trade(px, sz, side="A", t=1500, coin="BTC"):
    return TradeEvent(coin=coin, t_ms=t, px=px, sz=sz, side=side)


def _diff(kind, oid, side="B", px="100.0", **kw):
    return BookDiff(user=None, oid=oid, coin="BTC", side=side, px=px, kind=kind, **kw)


def _l2_sim(bound=QueueBound.PESSIMISTIC, **view_kw):
    sim = QueueSim("BTC", bound)
    sim.on_book(_l2_view(**view_kw), None, t_ms=1000)
    return sim


# --- L2 basics ----------------------------------------------------------------


def test_place_snapshots_the_visible_level():
    sim = _l2_sim()
    oid = sim.place("B", "100.0", 1.0, t_ms=1100)
    assert sim.queue_ahead(oid) == 5.0


def test_trade_consumes_ahead_then_fills_us_partially():
    sim = _l2_sim()
    oid = sim.place("B", "100.0", 1.0, t_ms=1100)
    fills = sim.on_trade(_trade("100.0", 5.5))  # taker sell into our bid
    assert len(fills) == 1
    fill = fills[0]
    assert (fill.order_id, fill.side, fill.px, fill.sz) == (oid, "B", "100.0", 0.5)
    assert fill.queue_ahead_at_fill == 5.0
    assert fill.queue_bound == "pessimistic"
    assert fill.mid_at_fill == pytest.approx(100.25)
    assert fill.height is None  # L2 replay carries no block heights
    assert fill.maker is True
    assert sim.queue_ahead(oid) == 0.0

    more = sim.on_trade(_trade("100.0", 0.3, t=1600))
    assert [(f.order_id, f.sz) for f in more] == [(oid, 0.3)]


def test_full_fill_retires_the_order():
    sim = _l2_sim()
    oid = sim.place("B", "100.0", 1.0, t_ms=1100)
    sim.on_trade(_trade("100.0", 6.0))
    assert sim.queue_ahead(oid) is None
    assert sim.cancel(oid, t_ms=1700) is False  # already gone
    assert sim.open_orders() == []
    stats = sim.get_stats()
    assert stats["fills"] == 1 and stats["filled_volume"] == pytest.approx(1.0)


def test_through_trade_fills_fully_regardless_of_queue():
    sim = _l2_sim()
    oid = sim.place("B", "100.0", 1.0, t_ms=1100)
    fills = sim.on_trade(_trade("99.5", 0.1))  # printed BELOW our bid
    assert [(f.order_id, f.sz) for f in fills] == [(oid, 1.0)]
    assert fills[0].queue_ahead_at_fill == 5.0  # recorded, not consumed


def test_aggressor_side_convention_is_not_inverted():
    sim = _l2_sim()
    bid = sim.place("B", "100.0", 1.0, t_ms=1100)
    ask = sim.place("A", "100.5", 1.0, t_ms=1100)

    assert sim.on_trade(_trade("100.0", 9.0, side="B")) == []  # buys hit asks
    assert sim.on_trade(_trade("100.5", 9.0, side="A")) == []  # sells hit bids

    buy_fills = sim.on_trade(_trade("100.5", 4.5, side="B"))
    assert [(f.order_id, f.sz) for f in buy_fills] == [(ask, 0.5)]
    sell_fills = sim.on_trade(_trade("100.0", 5.5, side="A"))
    assert [(f.order_id, f.sz) for f in sell_fills] == [(bid, 0.5)]


def test_foreign_coin_trades_are_ignored():
    sim = _l2_sim()
    sim.place("B", "100.0", 1.0, t_ms=1100)
    assert sim.on_trade(_trade("100.0", 9.0, coin="ETH")) == []


# --- the three L2 bounds -------------------------------------------------------


def _run_cancel_script(bound):
    """place (ahead 5) -> level grows to 9 (4 join behind us) -> shrinks to
    8, then to 5, with no trades: 4 total canceled, location unobservable."""
    sim = _l2_sim(bound)
    oid = sim.place("B", "100.0", 1.0, t_ms=1100)
    sim.on_book(_l2_view(bid_sz=9.0), None, t_ms=2000)
    sim.on_book(_l2_view(bid_sz=8.0), None, t_ms=2500)
    sim.on_book(_l2_view(bid_sz=5.0), None, t_ms=3000)
    return sim, oid


def test_unobservable_cancels_bracket_by_bound():
    sim, oid = _run_cancel_script(QueueBound.PESSIMISTIC)
    # cancels come from behind us first: 4 behind exist -> ahead untouched.
    assert sim.queue_ahead(oid) == 5.0

    sim, oid = _run_cancel_script(QueueBound.OPTIMISTIC)
    # cancels come from ahead first: ahead 5 - 4 = 1.
    assert sim.queue_ahead(oid) == 1.0

    sim, oid = _run_cancel_script(QueueBound.PRORATA)
    # proportional: 9 -> 8 scales ahead by 8/9; 8 -> 5 by 5/8: 5 * 5/9.
    assert sim.queue_ahead(oid) == pytest.approx(5.0 * 5.0 / 9.0)


def test_reality_clamp_forces_pessimistic_down():
    """Pessimistic = cancels behind until reality forces otherwise: once the
    visible level is smaller than our estimate, the estimate must follow."""
    sim = _l2_sim(QueueBound.PESSIMISTIC)
    oid = sim.place("B", "100.0", 1.0, t_ms=1100)  # ahead 5, nothing behind
    sim.on_book(_l2_view(bid_sz=2.0), None, t_ms=2000)  # 3 canceled, all ahead
    assert sim.queue_ahead(oid) == 2.0  # clamped to what visibly exists


def test_level_growth_never_improves_position():
    for bound in BOUNDS_L2:
        sim = _l2_sim(bound)
        oid = sim.place("B", "100.0", 1.0, t_ms=1100)
        sim.on_book(_l2_view(bid_sz=12.0), None, t_ms=2000)
        assert sim.queue_ahead(oid) == 5.0, bound


def test_level_vanishing_zeroes_queue_ahead():
    for bound in BOUNDS_L2:
        sim = _l2_sim(bound)
        oid = sim.place("B", "100.0", 1.0, t_ms=1100)
        book = L2Book("BTC")  # bid level gone entirely, no trades seen
        book.update_from_snapshot([], [{"px": "100.5", "sz": "4", "n": 2}], 2000)
        sim.on_book(book, None, t_ms=2000)
        assert sim.queue_ahead(oid) == 0.0, bound


def test_fill_volume_brackets_across_bounds():
    """Same tape, three sims: optimistic >= prorata >= pessimistic fills."""
    volumes = {}
    for bound in BOUNDS_L2:
        sim = _l2_sim(bound)
        sim.place("B", "100.0", 1.0, t_ms=1100)      # ahead 5
        sim.on_book(_l2_view(bid_sz=8.0), None, 2000)  # +3 behind
        sim.on_book(_l2_view(bid_sz=4.0), None, 3000)  # -4 canceled, unobserved
        fills = sim.on_trade(_trade("100.0", 3.0, t=3500))
        volumes[bound] = sum(f.sz for f in fills)
    assert volumes[QueueBound.OPTIMISTIC] == pytest.approx(1.0)   # ahead 1
    assert volumes[QueueBound.PRORATA] == pytest.approx(0.5)      # ahead 2.5
    assert volumes[QueueBound.PESSIMISTIC] == pytest.approx(0.0)  # ahead 4
    assert (
        volumes[QueueBound.OPTIMISTIC]
        >= volumes[QueueBound.PRORATA]
        >= volumes[QueueBound.PESSIMISTIC]
    )


# --- our own FIFO and interleaving ----------------------------------------------


def test_multiple_own_orders_fill_in_placement_order():
    sim = _l2_sim()
    first = sim.place("B", "100.0", 1.0, t_ms=1100)
    second = sim.place("B", "100.0", 0.5, t_ms=1200)
    fills = sim.on_trade(_trade("100.0", 6.2))
    assert [f.order_id for f in fills] == [first, second]
    assert [f.sz for f in fills] == pytest.approx([1.0, 0.2])
    # queue_ahead_at_fill = REAL volume ahead at trade arrival, per order
    # (our own volume is not queue-ahead): both sat behind the same 5.0.
    assert fills[0].queue_ahead_at_fill == 5.0
    assert fills[1].queue_ahead_at_fill == 5.0


def test_real_joins_interleave_between_our_orders():
    sim = _l2_sim()
    first = sim.place("B", "100.0", 1.0, t_ms=1100)
    sim.on_book(_l2_view(bid_sz=7.0), None, t_ms=2000)  # +2 joined behind first
    second = sim.place("B", "100.0", 1.0, t_ms=2100)
    assert sim.queue_ahead(first) == 5.0
    assert sim.queue_ahead(second) == 7.0  # the 2 joiners are ahead of us

    fills = sim.on_trade(_trade("100.0", 5.5, t=2500))
    assert [(f.order_id, f.sz) for f in fills] == [(first, 0.5)]
    assert sim.queue_ahead(second) == 2.0  # front 5 gone, 2 joiners remain


# --- accounting edges -------------------------------------------------------------


def test_crossed_book_counts_but_never_fills():
    sim = _l2_sim()
    sim.place("B", "100.0", 1.0, t_ms=1100)
    crossed = L2Book("BTC")  # best ask below our bid px, no prints seen
    crossed.update_from_snapshot(
        [{"px": "99.0", "sz": "1", "n": 1}], [{"px": "99.5", "sz": "1", "n": 1}], 2000
    )
    fills = sim.on_book(crossed, None, t_ms=2000)
    assert fills == []
    assert sim.get_stats()["crossed_unfilled"] == 1
    assert sim.get_stats()["open_orders"] == 1


def test_cancel_reasons_and_stale_evictions():
    sim = _l2_sim()
    oid = sim.place("B", "100.0", 1.0, t_ms=1100)
    assert sim.cancel(oid, t_ms=1200) is True
    assert sim.get_stats()["stale_evictions"] == 0

    oid2 = sim.place("B", "100.0", 1.0, t_ms=1300)
    assert sim.cancel(oid2, t_ms=1400, reason="stale") is True
    assert sim.get_stats()["stale_evictions"] == 1
    assert sim.cancel(999, t_ms=1500) is False


def test_place_before_any_book_is_blind_but_safe():
    sim = QueueSim("BTC", QueueBound.PESSIMISTIC)
    oid = sim.place("B", "100.0", 1.0, t_ms=1000)
    assert sim.queue_ahead(oid) == 0.0
    assert sim.get_stats()["placed_blind"] == 1


# --- EXACT mode (L4 replay) ---------------------------------------------------------


def _l4_book():
    book = L4Book("BTC")
    book.apply(_diff("new", 1, sz="0.5"))
    book.apply(_diff("new", 2, sz="0.3"))
    book.apply(_diff("new", 3, side="A", px="100.5", sz="2.0"))
    return book


def _exact_sim():
    book = _l4_book()
    sim = QueueSim("BTC", QueueBound.EXACT)
    sim.on_book(book, BlockDiffBatch(time_ms=100000, height=100, diffs=[]), 100000)
    return sim, book


def test_exact_mode_snapshots_the_real_fifo():
    sim, _ = _exact_sim()
    oid = sim.place("B", "100.0", 1.0, t_ms=100100)
    assert sim.queue_ahead(oid) == pytest.approx(0.8)  # oid1 0.5 + oid2 0.3


def test_exact_intra_block_trades_progress_without_double_count():
    sim, book = _exact_sim()
    oid = sim.place("B", "100.0", 1.0, t_ms=100100)

    assert sim.on_trade(_trade("100.0", 0.6, t=100500)) == []  # dies in queue
    assert sim.queue_ahead(oid) == pytest.approx(0.2)

    fills = sim.on_trade(_trade("100.0", 0.4, t=100600))  # 0.2 ahead + 0.2 us
    assert [(f.order_id, f.sz) for f in fills] == [(oid, pytest.approx(0.2))]
    assert fills[0].queue_ahead_at_fill == pytest.approx(0.2)
    assert fills[0].height == 101  # trades after block 100 belong to block 101

    # Block 101's diffs settle exactly what the trades consumed: both real
    # orders fully traded. Applying them must NOT double-deplete the queue.
    settle = BlockDiffBatch(
        time_ms=101000,
        height=101,
        diffs=[_diff("remove", 1), _diff("remove", 2)],
    )
    book.apply_block(settle)
    sim.on_book(book, settle, t_ms=101000)
    assert sim.queue_ahead(oid) == 0.0

    more = sim.on_trade(_trade("100.0", 0.5, t=101500))  # nothing ahead now
    assert [(f.order_id, f.sz) for f in more] == [(oid, pytest.approx(0.5))]


def test_exact_cancels_and_resizes_track_by_oid():
    sim, book = _exact_sim()
    oid = sim.place("B", "100.0", 1.0, t_ms=100100)

    cancel_ahead = BlockDiffBatch(
        time_ms=101000, height=101, diffs=[_diff("remove", 1)]
    )
    book.apply_block(cancel_ahead)
    sim.on_book(book, cancel_ahead, t_ms=101000)
    assert sim.queue_ahead(oid) == pytest.approx(0.3)

    resize = BlockDiffBatch(
        time_ms=102000,
        height=102,
        diffs=[_diff("update", 2, orig_sz="0.3", new_sz="0.1")],
    )
    book.apply_block(resize)
    sim.on_book(book, resize, t_ms=102000)
    assert sim.queue_ahead(oid) == pytest.approx(0.1)

    join_behind = BlockDiffBatch(
        time_ms=103000, height=103, diffs=[_diff("new", 9, sz="4.0")]
    )
    book.apply_block(join_behind)
    sim.on_book(book, join_behind, t_ms=103000)
    assert sim.queue_ahead(oid) == pytest.approx(0.1)  # joins land behind us

    ghost = BlockDiffBatch(
        time_ms=104000, height=104, diffs=[_diff("remove", 999)]
    )
    book.apply_block(ghost)
    sim.on_book(book, ghost, t_ms=104000)
    assert sim.get_stats()["untracked_diffs"] == 1


def test_open_orders_report_shape():
    sim = _l2_sim()
    oid = sim.place("B", "100.0", 1.0, t_ms=1100)
    (row,) = sim.open_orders()
    assert row == {
        "order_id": oid,
        "side": "B",
        "px": "100.0",
        "sz": 1.0,
        "queue_ahead": 5.0,
    }
