"""Tests for the pure fill math (sim/fills.py): FIFO allocation and the
aggressor/side geometry. Every number is hand-computed; no book state here.
"""

from decimal import Decimal

import pytest

from hyperliquid_pipeline.sim.fills import (
    allocate_at_level,
    resting_side_hit,
    trade_reaches,
    trade_through,
)


def test_aggressor_hits_the_opposite_side():
    assert resting_side_hit("B") == "A"  # taker buy consumes asks
    assert resting_side_hit("A") == "B"  # taker sell consumes bids


def test_trade_reaches_geometry():
    # Resting bid at 100: a sell printing at 99 swept through 100 first.
    assert trade_reaches("B", Decimal("100"), Decimal("99")) is True
    assert trade_reaches("B", Decimal("100"), Decimal("100")) is True
    assert trade_reaches("B", Decimal("100"), Decimal("101")) is False
    # Resting ask at 100: a buy printing at 101 went through 100 first.
    assert trade_reaches("A", Decimal("100"), Decimal("101")) is True
    assert trade_reaches("A", Decimal("100"), Decimal("100")) is True
    assert trade_reaches("A", Decimal("100"), Decimal("99")) is False


def test_trade_through_is_strictly_worse_price():
    assert trade_through("B", Decimal("100"), Decimal("99.5")) is True
    assert trade_through("B", Decimal("100"), Decimal("100")) is False
    assert trade_through("A", Decimal("100"), Decimal("100.5")) is True
    assert trade_through("A", Decimal("100"), Decimal("100")) is False


def test_allocation_consumes_ahead_before_us():
    consumed, fills = allocate_at_level(4.0, 5.0, [(1, 1.0)])
    assert consumed == 4.0 and fills == []  # trade dies inside the queue


def test_allocation_partial_then_spanning_fills():
    consumed, fills = allocate_at_level(5.5, 5.0, [(1, 1.0), (2, 0.5)])
    assert consumed == 5.0
    assert fills == [(1, 0.5)]  # 0.5 spills into us, FIFO

    consumed, fills = allocate_at_level(6.4, 5.0, [(1, 1.0), (2, 0.5)])
    assert consumed == 5.0
    assert [order_id for order_id, _ in fills] == [1, 2]  # spans both, FIFO
    assert [sz for _, sz in fills] == pytest.approx([1.0, 0.4])


def test_allocation_guards():
    assert allocate_at_level(0.0, 5.0, [(1, 1.0)]) == (0.0, [])
    assert allocate_at_level(-1.0, 5.0, [(1, 1.0)]) == (0.0, [])
    # Negative queue-ahead estimates clamp to zero, never "pull" fills.
    consumed, fills = allocate_at_level(0.3, -2.0, [(1, 1.0)])
    assert consumed == 0.0 and fills == [(1, 0.3)]
