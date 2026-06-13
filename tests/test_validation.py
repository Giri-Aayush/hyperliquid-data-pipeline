"""Unit tests for market-data validation."""

from datetime import datetime, timezone

from hyperliquid_pipeline.collectors.realtime_collector import MarketDataPoint
from hyperliquid_pipeline.utils.validation import DataValidator, ValidationLevel


def trade_point(price, size, symbol="BTC"):
    return MarketDataPoint(
        timestamp=datetime.now(timezone.utc),
        symbol=symbol,
        data_type="trade",
        data={"price": price, "size": size, "side": "buy"},
    )


def orderbook_point(bids, asks, symbol="BTC"):
    return MarketDataPoint(
        timestamp=datetime.now(timezone.utc),
        symbol=symbol,
        data_type="orderbook",
        data={
            "bids": [{"px": str(p), "sz": str(s)} for p, s in bids],
            "asks": [{"px": str(p), "sz": str(s)} for p, s in asks],
        },
    )


def messages(results, level=None):
    if level is None:
        return [r.message for r in results]
    return [r.message for r in results if r.level == level]


class TestTradeValidation:
    def test_valid_trade_passes(self):
        v = DataValidator()
        results = v.validate_price_data(trade_point(50000.0, 0.5))
        assert results == []

    def test_negative_price_is_error(self):
        v = DataValidator()
        results = v.validate_price_data(trade_point(-1.0, 0.5))
        assert any("Invalid price" in m for m in messages(results, ValidationLevel.ERROR))

    def test_zero_size_is_error(self):
        v = DataValidator()
        results = v.validate_price_data(trade_point(50000.0, 0.0))
        assert any("Invalid size" in m for m in messages(results, ValidationLevel.ERROR))

    def test_large_price_jump_is_warning(self):
        v = DataValidator()
        assert v.validate_price_data(trade_point(100.0, 1.0)) == []
        # 20% jump exceeds the 10% threshold
        results = v.validate_price_data(trade_point(120.0, 1.0))
        assert any(
            "Large price change" in m
            for m in messages(results, ValidationLevel.WARNING)
        )

    def test_small_price_move_passes(self):
        v = DataValidator()
        v.validate_price_data(trade_point(100.0, 1.0))
        assert v.validate_price_data(trade_point(101.0, 1.0)) == []


class TestOrderbookValidation:
    def test_valid_book_passes(self):
        v = DataValidator()
        results = v.validate_price_data(
            orderbook_point([(100, 1), (99, 2)], [(101, 1), (102, 2)])
        )
        assert results == []

    def test_empty_bids_is_error(self):
        v = DataValidator()
        results = v.validate_price_data(orderbook_point([], [(101, 1)]))
        assert any("Empty bids" in m for m in messages(results, ValidationLevel.ERROR))

    def test_crossed_book_is_critical(self):
        v = DataValidator()
        results = v.validate_price_data(orderbook_point([(102, 1)], [(101, 1)]))
        assert any(
            "Crossed book" in m for m in messages(results, ValidationLevel.CRITICAL)
        )

    def test_unsorted_bids_is_error(self):
        v = DataValidator()
        # bids must be descending; give ascending
        results = v.validate_price_data(orderbook_point([(99, 1), (100, 1)], [(101, 1)]))
        assert any(
            "Bids not sorted" in m for m in messages(results, ValidationLevel.ERROR)
        )

    def test_wide_spread_is_warning(self):
        v = DataValidator()
        # 6% spread exceeds the 5% threshold
        results = v.validate_price_data(orderbook_point([(100, 1)], [(106, 1)]))
        assert any("Wide spread" in m for m in messages(results, ValidationLevel.WARNING))


class TestVolumeValidation:
    def test_volume_spike_is_warning(self):
        v = DataValidator()
        for _ in range(10):
            assert v.validate_volume_data(trade_point(100.0, 1.0)) == []
        results = v.validate_volume_data(trade_point(100.0, 20.0))
        assert any("Volume spike" in m for m in messages(results, ValidationLevel.WARNING))

    def test_normal_volume_passes(self):
        v = DataValidator()
        for _ in range(10):
            v.validate_volume_data(trade_point(100.0, 1.0))
        assert v.validate_volume_data(trade_point(100.0, 2.0)) == []


def test_timestamp_duplicate_scoped_per_data_type():
    """Different feeds for one symbol at the same instant must not flag each
    other as duplicates; a true same-feed repeat still does."""
    from datetime import datetime, timezone

    v = DataValidator()
    ts = datetime.now(timezone.utc)

    def pt(dtype):
        return MarketDataPoint(timestamp=ts, symbol="BTC", data_type=dtype, data={})

    r_trade = v.validate_timestamp_data(pt("trade"))
    assert not any("Duplicate" in x.message for x in r_trade)

    # asset_ctx at the same instant, different feed -> NOT a duplicate
    r_ctx = v.validate_timestamp_data(pt("asset_ctx"))
    assert not any("Duplicate" in x.message for x in r_ctx)

    # a second trade at the same instant, same feed -> IS a duplicate
    r_trade2 = v.validate_timestamp_data(pt("trade"))
    assert any("Duplicate" in x.message for x in r_trade2)
