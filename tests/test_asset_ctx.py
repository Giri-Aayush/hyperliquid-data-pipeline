"""Edge-case tests for activeAssetCtx parsing: open interest, mark/oracle/mid
price, funding, premium, and the mark-vs-oracle basis."""

import asyncio
import json

from hyperliquid_pipeline.collectors.realtime_collector import (
    HyperliquidWebSocketCollector,
)


def _collector(symbols=("BTC",)):
    return HyperliquidWebSocketCollector(list(symbols))


def _msg(coin="BTC", **ctx):
    return {"channel": "activeAssetCtx", "data": {"coin": coin, "ctx": ctx}}


def test_full_ctx_parsed_with_basis():
    c = _collector()
    dp = c.process_asset_ctx_message(
        _msg(markPx="50010", oraclePx="50000", midPx="50005",
             openInterest="1234.5", funding="0.0001", premium="0.0002")
    )
    assert dp.data_type == "asset_ctx"
    assert dp.symbol == "BTC"
    assert dp.data["mark_price"] == 50010.0
    assert dp.data["oracle_price"] == 50000.0
    assert dp.data["mid_price"] == 50005.0
    assert dp.data["open_interest"] == 1234.5
    assert dp.data["funding"] == 0.0001
    assert dp.data["premium"] == 0.0002
    assert dp.data["basis"] == 10.0
    assert dp.data["basis_bps"] == 2.0  # 10 / 50000 * 10000


def test_missing_oracle_yields_no_basis():
    c = _collector()
    dp = c.process_asset_ctx_message(_msg(markPx="50000", openInterest="100"))
    assert dp.data["mark_price"] == 50000.0
    assert dp.data["oracle_price"] is None
    assert dp.data["basis"] is None
    assert dp.data["basis_bps"] is None
    assert dp.data["open_interest"] == 100.0  # other fields still captured


def test_missing_mark_yields_no_basis():
    c = _collector()
    dp = c.process_asset_ctx_message(_msg(oraclePx="50000", openInterest="100"))
    assert dp.data["basis"] is None
    assert dp.data["oracle_price"] == 50000.0


def test_zero_oracle_no_divide_by_zero():
    c = _collector()
    dp = c.process_asset_ctx_message(_msg(markPx="50", oraclePx="0"))
    assert dp.data["basis"] == 50.0   # mark - 0
    assert dp.data["basis_bps"] is None  # guarded, no ZeroDivisionError


def test_negative_basis():
    c = _collector()
    dp = c.process_asset_ctx_message(_msg(markPx="49990", oraclePx="50000"))
    assert dp.data["basis"] == -10.0
    assert dp.data["basis_bps"] == -2.0


def test_malformed_numeric_value_becomes_none():
    c = _collector()
    dp = c.process_asset_ctx_message(_msg(markPx="not-a-number", openInterest="100"))
    assert dp.data["mark_price"] is None
    assert dp.data["open_interest"] == 100.0  # parsing one bad field doesn't crash


def test_unknown_coin_returns_none():
    c = _collector(["BTC"])
    assert c.process_asset_ctx_message(_msg(coin="DOGE", markPx="1")) is None


def test_empty_ctx_returns_none():
    c = _collector()
    assert c.process_asset_ctx_message(_msg()) is None  # nothing useful


def test_missing_ctx_key_returns_none():
    c = _collector()
    assert c.process_asset_ctx_message({"data": {"coin": "BTC"}}) is None


def test_point_buffered():
    c = _collector()
    c.process_asset_ctx_message(_msg(markPx="50000", oraclePx="50000"))
    assert len(c.asset_ctx_buffer["BTC"]) == 1


def test_dispatch_routes_active_asset_ctx():
    c = _collector()
    raw = json.dumps(_msg(markPx="50010", oraclePx="50000", openInterest="42"))
    asyncio.run(c.process_message(raw))
    assert len(c.asset_ctx_buffer["BTC"]) == 1
    assert c.asset_ctx_buffer["BTC"][-1].data["open_interest"] == 42.0


def test_boolean_value_rejected():
    # float(True) == 1.0 would otherwise slip a bool through as a price.
    c = _collector()
    dp = c.process_asset_ctx_message(_msg(markPx=True, oraclePx="50000", openInterest="10"))
    assert dp.data["mark_price"] is None
    assert dp.data["open_interest"] == 10.0


def test_buffer_observable_via_accessors():
    c = _collector()
    c.process_asset_ctx_message(_msg(markPx="50000", oraclePx="50000", openInterest="5"))
    recent = c.get_recent_data("BTC", "asset_ctx")
    assert len(recent) == 1 and recent[0].data["open_interest"] == 5.0
    assert c.get_stats()["buffer_sizes"]["BTC"]["asset_ctx"] == 1
