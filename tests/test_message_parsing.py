"""Unit tests for Hyperliquid WebSocket message parsing (no network)."""

from datetime import timezone

from hyperliquid_pipeline.collectors.realtime_collector import (
    HyperliquidWebSocketCollector,
)


def collector(symbols=("BTC",)):
    return HyperliquidWebSocketCollector(list(symbols))


class TestL2BookParsing:
    def test_parses_bids_asks_and_timestamp(self):
        c = collector()
        msg = {
            "channel": "l2Book",
            "data": {
                "coin": "BTC",
                "levels": [
                    [{"px": "50000.0", "sz": "1.5", "n": 3}],
                    [{"px": "50001.0", "sz": "2.0", "n": 1}],
                ],
                "time": 1_700_000_000_000,
            },
        }
        point = c.process_l2_book_message(msg)

        assert point.symbol == "BTC"
        assert point.data_type == "orderbook"
        assert point.data["bids"] == [{"px": "50000.0", "sz": "1.5", "n": 3}]
        assert point.data["asks"] == [{"px": "50001.0", "sz": "2.0", "n": 1}]
        assert point.data["timestamp_ms"] == 1_700_000_000_000
        assert point.timestamp.tzinfo == timezone.utc
        assert int(point.timestamp.timestamp() * 1000) == 1_700_000_000_000

    def test_unknown_coin_returns_none(self):
        c = collector(["BTC"])
        msg = {"data": {"coin": "DOGE", "levels": [[], []], "time": 1}}
        assert c.process_l2_book_message(msg) is None

    def test_point_added_to_buffer(self):
        c = collector()
        msg = {"data": {"coin": "BTC", "levels": [[], []], "time": 1_700_000_000_000}}
        c.process_l2_book_message(msg)
        assert len(c.orderbook_buffer["BTC"]) == 1


class TestTradesParsing:
    def test_parses_trade_fields_as_floats(self):
        c = collector()
        msg = {
            "channel": "trades",
            "data": [
                {
                    "coin": "BTC",
                    "px": "50000.5",
                    "sz": "0.25",
                    "side": "B",
                    "time": 1_700_000_000_000,
                    "tid": 12345,
                }
            ],
        }
        points = c.process_trades_message(msg)

        assert len(points) == 1
        p = points[0]
        assert p.data_type == "trade"
        assert p.data["price"] == 50000.5
        assert isinstance(p.data["price"], float)
        assert p.data["size"] == 0.25
        assert p.data["side"] == "B"
        assert p.data["trade_id"] == 12345

    def test_filters_unknown_coins(self):
        c = collector(["BTC"])
        msg = {
            "data": [
                {"coin": "BTC", "px": "1", "sz": "1", "side": "B", "time": 1, "tid": 1},
                {"coin": "DOGE", "px": "1", "sz": "1", "side": "A", "time": 1, "tid": 2},
            ]
        }
        points = c.process_trades_message(msg)
        assert [p.symbol for p in points] == ["BTC"]

    def test_empty_message_returns_empty_list(self):
        assert collector().process_trades_message({}) == []


class TestTickerParsing:
    def test_parses_mids_for_subscribed_symbols_only(self):
        c = collector(["BTC"])
        msg = {"data": {"mids": {"BTC": "50000.5", "ETH": "3000.1"}}}
        points = c.process_ticker_message(msg)

        assert len(points) == 1
        assert points[0].symbol == "BTC"
        assert points[0].data_type == "ticker"
        assert points[0].data["mid_price"] == 50000.5

    def test_empty_mids_returns_empty_list(self):
        assert collector().process_ticker_message({"data": {}}) == []
