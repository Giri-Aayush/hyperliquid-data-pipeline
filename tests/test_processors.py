"""Unit tests for OHLCV generation, orderbook metrics, and technical indicators."""

from datetime import datetime, timedelta, timezone

import pytest

from hyperliquid_pipeline.collectors.realtime_collector import MarketDataPoint
from hyperliquid_pipeline.processors.data_processor import (
    OHLCVProcessor,
    OrderBookProcessor,
    TechnicalIndicatorProcessor,
)


def make_trade(symbol, price, size, ts):
    return MarketDataPoint(
        timestamp=ts,
        symbol=symbol,
        data_type="trade",
        data={"price": price, "size": size, "side": "buy"},
    )


def make_orderbook(symbol, bids, asks):
    return MarketDataPoint(
        timestamp=datetime.now(timezone.utc),
        symbol=symbol,
        data_type="orderbook",
        data={
            "bids": [{"px": str(p), "sz": str(s)} for p, s in bids],
            "asks": [{"px": str(p), "sz": str(s)} for p, s in asks],
        },
    )


class TestTimeframeParsing:
    def test_minutes(self):
        assert OHLCVProcessor()._timeframe_to_seconds("1m") == 60
        assert OHLCVProcessor()._timeframe_to_seconds("5m") == 300

    def test_hours_and_days(self):
        assert OHLCVProcessor()._timeframe_to_seconds("1h") == 3600
        assert OHLCVProcessor()._timeframe_to_seconds("4h") == 14400
        assert OHLCVProcessor()._timeframe_to_seconds("1d") == 86400


class TestOHLCVGeneration:
    def test_known_candle_values(self):
        proc = OHLCVProcessor()
        now = datetime.now(timezone.utc)
        prices = [100.0, 110.0, 90.0, 105.0]
        sizes = [1.0, 2.0, 1.0, 1.0]
        for i, (p, s) in enumerate(zip(prices, sizes)):
            proc.add_trade(make_trade("BTC", p, s, now - timedelta(seconds=50 - i * 10)))

        ohlcv = proc.generate_ohlcv("BTC", "1m", end_time=now)

        assert ohlcv["open"] == 100.0
        assert ohlcv["high"] == 110.0
        assert ohlcv["low"] == 90.0
        assert ohlcv["close"] == 105.0
        assert ohlcv["volume"] == 5.0
        assert ohlcv["count"] == 4
        # vwap = (100*1 + 110*2 + 90*1 + 105*1) / 5
        assert ohlcv["vwap"] == pytest.approx(103.0)

    def test_no_trades_returns_none(self):
        proc = OHLCVProcessor()
        assert proc.generate_ohlcv("BTC", "1m") is None

    def test_trades_outside_window_excluded(self):
        proc = OHLCVProcessor()
        now = datetime.now(timezone.utc)
        proc.add_trade(make_trade("BTC", 100.0, 1.0, now - timedelta(seconds=90)))
        # 1m window ending now starts at now-60s; the trade at now-90s is outside
        assert proc.generate_ohlcv("BTC", "1m", end_time=now) is None

    def test_non_trade_data_ignored(self):
        proc = OHLCVProcessor()
        ob = make_orderbook("BTC", [(100, 1)], [(101, 1)])
        proc.add_trade(ob)
        assert "BTC" not in proc.trade_buffers


class TestOrderBookMetrics:
    def test_spread_and_mid(self):
        proc = OrderBookProcessor()
        proc.update_orderbook(make_orderbook("BTC", [(100, 2), (99, 3)], [(101, 1), (102, 4)]))
        m = proc.calculate_metrics("BTC")

        assert m["best_bid"] == 100.0
        assert m["best_ask"] == 101.0
        assert m["mid_price"] == pytest.approx(100.5)
        assert m["spread"] == pytest.approx(1.0)
        assert m["spread_bps"] == pytest.approx(1.0 / 100.5 * 10000)
        assert m["bid_levels"] == 2
        assert m["ask_levels"] == 2

    def test_imbalance(self):
        proc = OrderBookProcessor()
        # bid volume 6, ask volume 2 -> (6-2)/(6+2) = 0.5
        proc.update_orderbook(make_orderbook("BTC", [(100, 6)], [(101, 2)]))
        m = proc.calculate_metrics("BTC")
        assert m["imbalance"] == pytest.approx(0.5)
        assert m["total_bid_volume"] == pytest.approx(6.0)
        assert m["total_ask_volume"] == pytest.approx(2.0)

    def test_depth_uses_top_five_levels(self):
        proc = OrderBookProcessor()
        bids = [(100 - i, 1) for i in range(7)]
        asks = [(101 + i, 2) for i in range(7)]
        proc.update_orderbook(make_orderbook("BTC", bids, asks))
        m = proc.calculate_metrics("BTC")
        assert m["bid_depth_5"] == pytest.approx(5.0)
        assert m["ask_depth_5"] == pytest.approx(10.0)

    def test_unknown_symbol_returns_none(self):
        assert OrderBookProcessor().calculate_metrics("ETH") is None

    def test_non_orderbook_data_ignored(self):
        proc = OrderBookProcessor()
        proc.update_orderbook(make_trade("BTC", 100.0, 1.0, datetime.now(timezone.utc)))
        assert "BTC" not in proc.latest_orderbooks

    def test_imbalance_5_ignores_deep_book_spoof(self):
        """A huge resting order 10 levels down skews the all-levels imbalance
        but must not move the top-5 variant — that's the point of having it."""
        proc = OrderBookProcessor()
        bids = [(100 - i, 1) for i in range(9)] + [(90, 500)]  # spoof at level 10
        asks = [(101 + i, 1) for i in range(10)]
        proc.update_orderbook(make_orderbook("BTC", bids, asks))
        m = proc.calculate_metrics("BTC")
        assert m["imbalance"] > 0.9        # all-levels reading is dominated by the spoof
        assert m["imbalance_5"] == pytest.approx(0.0)  # top-5 doesn't see it

    def test_metrics_read_through_book_view(self):
        """The processor's book is the shared BookView the rest of the system
        types against — the L4 book plugs into the same consumers."""
        from hyperliquid_pipeline.book import BookView

        proc = OrderBookProcessor()
        proc.update_orderbook(make_orderbook("BTC", [(100, 2)], [(101, 1)]))
        book = proc.get_book("BTC")
        assert isinstance(book, BookView)
        assert book.best_bid() == ("100", 2.0)
        assert book.is_crossed() is False
        assert proc.calculate_metrics("BTC")["crossed"] is False
        assert proc.get_book("ETH") is None

    def test_snapshot_replaces_book(self):
        proc = OrderBookProcessor()
        proc.update_orderbook(make_orderbook("BTC", [(100, 2), (99, 1)], [(101, 1)]))
        proc.update_orderbook(make_orderbook("BTC", [(98, 5)], [(99.5, 1)]))
        m = proc.calculate_metrics("BTC")
        assert m["best_bid"] == 98.0   # old levels gone: full replacement
        assert m["bid_levels"] == 1


class TestTechnicalIndicators:
    def test_sma(self):
        proc = TechnicalIndicatorProcessor()
        prices = [float(i) for i in range(1, 11)]
        # mean of last 5 of 1..10 is 8
        assert proc.calculate_sma(prices, 5) == pytest.approx(8.0)

    def test_sma_insufficient_data(self):
        assert TechnicalIndicatorProcessor().calculate_sma([1.0, 2.0], 5) is None

    def test_ema_constant_series(self):
        proc = TechnicalIndicatorProcessor()
        assert proc.calculate_ema([50.0] * 30, 10) == pytest.approx(50.0)

    def test_rsi_all_gains_is_100(self):
        proc = TechnicalIndicatorProcessor()
        prices = [float(i) for i in range(1, 17)]
        assert proc.calculate_rsi(prices, period=14) == 100

    def test_rsi_known_value(self):
        proc = TechnicalIndicatorProcessor()
        # deltas: +1, +2, -1; last 2: gains [2, 0] avg 1, losses [0, 1] avg 0.5
        # rs = 2 -> rsi = 100 - 100/3
        rsi = proc.calculate_rsi([10.0, 11.0, 13.0, 12.0], period=2)
        assert rsi == pytest.approx(100 - 100 / 3)

    def test_rsi_insufficient_data(self):
        assert TechnicalIndicatorProcessor().calculate_rsi([1.0] * 10, period=14) is None

    def test_bollinger_constant_series_collapses(self):
        proc = TechnicalIndicatorProcessor()
        bb = proc.calculate_bollinger_bands([50.0] * 20)
        assert bb["bb_upper"] == pytest.approx(50.0)
        assert bb["bb_middle"] == pytest.approx(50.0)
        assert bb["bb_lower"] == pytest.approx(50.0)
        assert bb["bb_width"] == pytest.approx(0.0)

    def test_bollinger_known_series(self):
        proc = TechnicalIndicatorProcessor()
        prices = [float(i) for i in range(1, 21)]  # 1..20
        bb = proc.calculate_bollinger_bands(prices, period=20, std_dev=2)
        sma = 10.5
        variance = sum((p - sma) ** 2 for p in prices) / 20
        std = variance ** 0.5
        assert bb["bb_middle"] == pytest.approx(sma)
        assert bb["bb_upper"] == pytest.approx(sma + 2 * std)
        assert bb["bb_lower"] == pytest.approx(sma - 2 * std)

    def test_calculate_indicators_requires_history(self):
        proc = TechnicalIndicatorProcessor()
        proc.update_price_data("BTC", {"close": 100.0, "volume": 1.0})
        assert proc.calculate_indicators("BTC") is None

    def test_calculate_indicators_with_history(self):
        proc = TechnicalIndicatorProcessor()
        for i in range(60):
            proc.update_price_data("BTC", {"close": 100.0 + i, "volume": 1.0})
        indicators = proc.calculate_indicators("BTC")
        assert indicators["sma_10"] == pytest.approx(sum(range(150, 160)) / 10)
        assert indicators["rsi"] == 100  # strictly increasing series
        assert indicators["price_change"] == pytest.approx(1.0)


def test_add_trade_evicts_aged_out_trades():
    """add_trade keeps a time-bounded deque, not an ever-growing list."""
    from collections import deque

    proc = OHLCVProcessor(retention=timedelta(seconds=60))
    now = datetime.now(timezone.utc)
    proc.add_trade(make_trade("BTC", 100.0, 1.0, now - timedelta(seconds=120)))  # too old
    proc.add_trade(make_trade("BTC", 101.0, 1.0, now))                           # fresh

    buf = proc.trade_buffers["BTC"]
    assert isinstance(buf, deque)
    assert len(buf) == 1
    assert buf[0].data["price"] == 101.0


def test_generate_ohlcv_handles_out_of_order_trades():
    """A late, out-of-window trade appended after in-window trades must not
    drop the in-window ones (regression: an early-break scan did exactly that)."""
    proc = OHLCVProcessor()
    now = datetime.now(timezone.utc)
    proc.add_trade(make_trade("BTC", 100.0, 1.0, now - timedelta(seconds=40)))
    proc.add_trade(make_trade("BTC", 110.0, 1.0, now - timedelta(seconds=20)))
    proc.add_trade(make_trade("BTC", 120.0, 2.0, now - timedelta(seconds=5)))
    # arrives late and is older than the 1m window, but newer than retention
    proc.add_trade(make_trade("BTC", 999.0, 1.0, now - timedelta(seconds=200)))

    ohlcv = proc.generate_ohlcv("BTC", "1m", end_time=now)
    assert ohlcv is not None
    assert ohlcv["count"] == 3
    assert ohlcv["open"] == 100.0
    assert ohlcv["close"] == 120.0
    assert ohlcv["volume"] == 4.0
