"""Tests for the latency bench math (no network).

Contract: exact nearest-rank percentiles on known samples, clock-offset
adjustment adds the offset (local-behind convention), negative deltas are
counted, report/table shapes are stable, and exchange-timestamp extraction
matches the real message schemas.
"""

from hyperliquid_pipeline.bench.ws_latency import (
    LatencyBench,
    exact_percentile,
    summarize_deltas,
    to_table,
)


def test_exact_percentiles_nearest_rank():
    samples = sorted(float(v) for v in range(1, 101))  # 1..100
    assert exact_percentile(samples, 0.01) == 1.0
    assert exact_percentile(samples, 0.50) == 50.0
    assert exact_percentile(samples, 0.90) == 90.0
    assert exact_percentile(samples, 0.99) == 99.0
    assert exact_percentile(samples, 1.00) == 100.0
    assert exact_percentile([42.0], 0.5) == 42.0
    assert exact_percentile([], 0.5) is None


def test_summarize_known_samples():
    stats = summarize_deltas([100.0, 200.0, 300.0, 400.0])
    assert stats["count"] == 4
    assert stats["negative_count"] == 0
    assert stats["raw"]["min_ms"] == 100.0
    assert stats["raw"]["max_ms"] == 400.0
    assert stats["raw"]["p50_ms"] == 200.0
    assert stats["raw"]["mean_ms"] == 250.0
    assert "adjusted" not in stats  # no offset estimate


def test_offset_adjustment_adds_offset():
    # local clock 50ms behind the server -> true latency = measured + 50
    stats = summarize_deltas([100.0, 200.0], ntp_offset_ms=50.0)
    assert stats["raw"]["p50_ms"] == 100.0
    assert stats["adjusted"]["p50_ms"] == 150.0
    assert stats["adjusted"]["max_ms"] == 250.0


def test_negative_deltas_counted():
    stats = summarize_deltas([-5.0, 10.0, 20.0])
    assert stats["negative_count"] == 1
    assert stats["raw"]["min_ms"] == -5.0  # bench keeps raw values, unclamped


def test_empty_channel():
    assert summarize_deltas([]) == {"count": 0}


def test_exchange_ts_extraction_matches_message_schemas():
    ts = 1_700_000_000_000
    bbo = {"data": {"coin": "BTC", "time": ts, "bbo": [None, None]}}
    l2 = {"data": {"coin": "BTC", "time": ts, "levels": [[], []]}}
    trades = {"data": [{"time": ts, "px": "1"}, {"time": ts + 5, "px": "2"}]}
    mids = {"data": {"mids": {"BTC": "1.0"}}}

    assert LatencyBench._extract_exchange_ms("bbo", bbo) == [float(ts)]
    assert LatencyBench._extract_exchange_ms("l2Book", l2) == [float(ts)]
    assert LatencyBench._extract_exchange_ms("trades", trades) == [float(ts), float(ts + 5)]
    assert LatencyBench._extract_exchange_ms("allMids", mids) == []
    assert LatencyBench._extract_exchange_ms("bbo", {"data": {}}) == []


def test_report_and_table_shapes():
    bench = LatencyBench(ws_url="ws://example/ws", symbols=["BTC"],
                         channels=("bbo",), duration_s=1.0)
    report = bench._build_report({"bbo": [100.0, 120.0]}, 10.0, "2026-07-04T00:00:00+00:00")
    assert report["ws_url"] == "ws://example/ws"
    assert report["ntp_offset_ms"] == 10.0
    assert report["channels"]["bbo"]["count"] == 2
    assert "adjusted" in report["channels"]["bbo"]
    assert "caveat" in report

    table = to_table(report)
    assert "bbo" in table
    assert "caveat" in table

    # offset-unknown path: raw-only, explicit caveat
    report2 = bench._build_report({"bbo": []}, None, "2026-07-04T00:00:00+00:00")
    assert report2["ntp_offset_ms"] is None
    assert "UNKNOWN" in report2["caveat"]
    assert "(no samples)" in to_table(report2)
