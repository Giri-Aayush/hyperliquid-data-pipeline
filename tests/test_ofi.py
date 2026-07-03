"""Tests for the OFI research module: math pinned by hand, offline only.

The OFI cases below are computed by hand from the Cont–Kukanov–Stoikov
best-level formula (equality-inclusive indicators — an unchanged price
contributes the size delta). Synthetic fixtures are written inline; no
network and no dependency on any live capture.
"""

import json
import math

import pytest

from hyperliquid_pipeline.research.ofi import (
    BboEvent,
    aggregate_windows,
    analyze,
    forward_pairs,
    load_bbo_events,
    main,
    ofi_series,
    ols_stats,
)


def _ev(t_ms, bid_px, bid_sz, ask_px, ask_sz):
    return BboEvent(t_ms, bid_px, bid_sz, ask_px, ask_sz)


def test_ofi_hand_computed_cases():
    events = [
        _ev(0, 100.0, 5.0, 100.2, 7.0),    # baseline
        _ev(100, 100.1, 4.0, 100.2, 7.0),  # bid up: +4; ask flat: -7+7=0
        _ev(200, 100.0, 6.0, 100.2, 7.0),  # bid down: -4 (prev size)
        _ev(300, 100.0, 9.0, 100.2, 2.0),  # sizes only: (9-6) + (-2+7) = 8
        _ev(400, 99.9, 1.0, 100.1, 3.0),   # both down: -9 (bid) -3 (ask)
        _ev(500, 99.9, 1.0, 100.3, 6.0),   # ask up: +3 (prev size); bid flat: 0
    ]
    assert ofi_series(events) == [
        (100, 4.0),
        (200, -4.0),
        (300, 8.0),
        (400, -12.0),
        (500, 3.0),
    ]


def test_ofi_needs_two_events():
    assert ofi_series([]) == []
    assert ofi_series([_ev(0, 100.0, 1.0, 100.1, 1.0)]) == []


def test_aggregate_windows_are_non_overlapping_and_sorted():
    ofi_events = [(100, 1.0), (900, 2.0), (1100, 4.0), (2500, -3.0)]
    assert aggregate_windows(ofi_events, 1000) == [
        (0, 3.0),
        (1000, 4.0),
        (2000, -3.0),
    ]
    # A quiet window (nothing in [3000, 4000)) simply does not appear.
    assert aggregate_windows([(4200, 5.0)], 1000) == [(4000, 5.0)]


# A tiny series with known mids for the forward reads:
# mid 100.05 until t=1500 (100.15), then 100.25 from t=2500 on.
FWD_EVENTS = [
    _ev(0, 100.0, 1.0, 100.1, 1.0),
    _ev(500, 100.0, 2.0, 100.1, 1.0),    # e=+1 (size add at bid)
    _ev(1500, 100.1, 1.0, 100.2, 1.0),   # e=+2 (both sides step up)
    _ev(2500, 100.2, 1.0, 100.3, 1.0),   # e=+2
    _ev(3500, 100.2, 1.0, 100.3, 2.0),   # e=-1 (size add at ask)
]


def test_forward_pairs_read_the_mid_as_a_step_function():
    window_ms = 1000
    sums = aggregate_windows(ofi_series(FWD_EVENTS), window_ms)
    assert sums == [(0, 1.0), (1000, 2.0), (2000, 2.0), (3000, -1.0)]

    pairs = forward_pairs(FWD_EVENTS, sums, window_ms, horizon_ms=1000)
    # Windows starting at 2000/3000 need mids past t=3500: not observable,
    # so no forward fill — they produce no pair.
    assert len(pairs) == 2
    assert pairs[0][0] == 1.0 and pairs[0][1] == pytest.approx(0.1)
    assert pairs[1][0] == 2.0 and pairs[1][1] == pytest.approx(0.1)


def test_ols_stats_perfect_line():
    stats = ols_stats([(x, 2.0 * x + 1.0) for x in range(10)])
    assert stats["n"] == 10
    assert stats["slope"] == pytest.approx(2.0)
    assert stats["r"] == pytest.approx(1.0)
    assert stats["t_stat"] == float("inf")


def test_ols_stats_hand_computed_small_case():
    stats = ols_stats([(0, 0.0), (1, 1.0), (2, 0.0), (3, 1.0)])
    assert stats["slope"] == pytest.approx(0.2)
    assert stats["r"] == pytest.approx(1 / math.sqrt(5))
    # t = r * sqrt((n-2) / (1 - r^2)) = (1/sqrt(5)) * sqrt(2 / 0.8)
    assert stats["t_stat"] == pytest.approx((1 / math.sqrt(5)) * math.sqrt(2 / 0.8))


def test_ols_stats_degenerate_inputs():
    assert ols_stats([]) == {"n": 0, "slope": None, "r": None, "t_stat": None}
    assert ols_stats([(1, 2.0), (2, 3.0)])["slope"] is None  # n < 3
    constant_x = ols_stats([(5, 1.0), (5, 2.0), (5, 3.0)])
    assert constant_x["slope"] is None  # no regressor variance
    constant_y = ols_stats([(1, 4.0), (2, 4.0), (3, 4.0)])
    assert constant_y["slope"] == pytest.approx(0.0)
    assert constant_y["r"] is None


def test_analyze_report_shape_and_caveats():
    report = analyze(FWD_EVENTS, symbol="BTC", windows_s=(1.0,), horizons_s=(1.0,))
    assert report["symbol"] == "BTC"
    assert report["events"] == 5
    assert report["duration_s"] == pytest.approx(3.5)
    # one explicit horizon + the next-window read
    assert [row["horizon"] for row in report["results"]] == ["1s", "next(1s)"]
    assert all(
        {"window_s", "horizon", "n", "slope", "r", "t_stat"} <= row.keys()
        for row in report["results"]
    )
    assert report["caveats"]  # the honest part is not optional


# --- loader -------------------------------------------------------------------


def _bbo_line(symbol, t_ms, bid, ask):
    return json.dumps(
        {
            "timestamp": "2026-07-03T19:22:27+00:00",
            "symbol": symbol,
            "data_type": "bbo",
            "data": {"bid": bid, "ask": ask, "timestamp_ms": t_ms},
            "recv_ts_ms": t_ms + 150.5,
            "recv_mono_ns": 1,
        }
    )


def _level(px, sz):
    return {"px": str(px), "sz": str(sz), "n": 1}


def test_loader_parses_bbo_and_orderbook_and_skips_junk(tmp_path):
    path = tmp_path / "capture.jsonl"
    lines = [
        _bbo_line("BTC", 2000, _level(100.1, 2), _level(100.2, 1)),  # out of order
        _bbo_line("BTC", 1000, _level(100.0, 1), _level(100.1, 1)),
        _bbo_line("BTC", 3000, None, _level(100.2, 1)),  # one-sided: skipped
        json.dumps(
            {
                "timestamp": "2026-07-03T19:22:28+00:00",
                "symbol": "ETH",
                "data_type": "orderbook",
                "data": {
                    "bids": [_level(3000.0, 5), _level(2999.5, 9)],
                    "asks": [_level(3000.5, 4)],
                    "timestamp_ms": 1500,
                },
            }
        ),
        json.dumps({"symbol": "BTC", "data_type": "trade", "data": {"px": "1"}}),
        "not json at all",
        "",
    ]
    path.write_text("\n".join(lines) + "\n")

    series = load_bbo_events(str(path))
    assert set(series) == {"BTC", "ETH"}
    # exchange clock ordering, not file order
    assert [e.t_ms for e in series["BTC"]] == [1000, 2000]
    eth = series["ETH"][0]
    assert (eth.bid_px, eth.bid_sz, eth.ask_px) == (3000.0, 5.0, 3000.5)
    assert eth.mid == pytest.approx(3000.25)

    only_btc = load_bbo_events(str(path), symbol="BTC")
    assert set(only_btc) == {"BTC"}


def _archive_line(coin, t_ms, bids, asks):
    """One hyperliquid-archive raw l2Book line (historical_collector shape)."""
    return json.dumps({"time": t_ms, "coin": coin, "levels": [bids, asks]})


ARCHIVE_LINES = [
    _archive_line(
        "BTC", 2000, [_level(100.1, 2), _level(100.0, 9)], [_level(100.2, 1)]
    ),
    _archive_line("BTC", 1000, [_level(100.0, 1)], [_level(100.1, 1)]),
    _archive_line("BTC", 3000, [], [_level(100.2, 1)]),  # empty bid side: skip
    _archive_line("SOL", 1500, [_level(150.0, 3)], [_level(150.1, 4)]),
]


def test_loader_reads_archive_l2_lines(tmp_path):
    path = tmp_path / "9"  # archive hour files are named by hour, no suffix
    path.write_text("\n".join(ARCHIVE_LINES) + "\n")
    series = load_bbo_events(str(path))
    assert set(series) == {"BTC", "SOL"}
    assert [e.t_ms for e in series["BTC"]] == [1000, 2000]
    top = series["BTC"][1]
    assert (top.bid_px, top.bid_sz, top.ask_px) == (100.1, 2.0, 100.2)  # top level
    assert load_bbo_events(str(path), symbol="SOL").keys() == {"SOL"}


def test_loader_reads_real_archive_wrapper_lines(tmp_path):
    """The REAL archive format (verified on Apr-2026 hours): an ISO-ns
    capture timestamp + ver_num wrapping the verbatim WS frame under 'raw'.
    The exchange clock is raw.data.time, NOT the capture timestamp."""
    verbatim = (
        '{"time":"2026-04-01T10:00:00.050123456","ver_num":1,"raw":{"channel":"l2Book",'
        '"data":{"coin":"BTC","time":1775037600050,"levels":'
        '[[{"px":"68614.0","sz":"0.30995","n":4}],[{"px":"68615.0","sz":"13.21825","n":30}]]}}}'
    )
    other_channel = (
        '{"time":"2026-04-01T10:00:00.060","ver_num":1,'
        '"raw":{"channel":"trades","data":{"coin":"BTC"}}}'
    )
    path = tmp_path / "wrapped.jsonl"
    path.write_text(verbatim + "\n" + other_channel + "\n")

    series = load_bbo_events(str(path))
    assert set(series) == {"BTC"}  # non-l2Book channels are skipped
    event = series["BTC"][0]
    assert event.t_ms == 1775037600050  # exchange ms, not the ISO capture time
    assert (event.bid_px, event.bid_sz) == (68614.0, 0.30995)
    assert (event.ask_px, event.ask_sz) == (68615.0, 13.21825)


def test_loader_reads_lz4_archive_hours(tmp_path):
    import lz4.frame

    plain = tmp_path / "9"
    plain.write_text("\n".join(ARCHIVE_LINES) + "\n")
    compressed = tmp_path / "BTC.lz4"
    with lz4.frame.open(compressed, mode="wt") as fh:
        fh.write(plain.read_text())
    assert load_bbo_events(str(compressed)) == load_bbo_events(str(plain))


# --- CLI ----------------------------------------------------------------------


def test_cli_prints_tables_and_writes_json(tmp_path, capsys):
    capture = tmp_path / "bbo.jsonl"
    lines = []
    # 40 events over ~10s: bid size builds, then the mid steps up — enough
    # for every (window, horizon) cell to have data; values not asserted.
    for k in range(40):
        px = 100.0 + 0.1 * (k // 8)
        lines.append(
            _bbo_line("BTC", 250 * k, _level(px, 1 + (k % 3)), _level(px + 0.1, 1))
        )
    capture.write_text("\n".join(lines) + "\n")
    out_json = tmp_path / "report.json"

    exit_code = main(
        [str(capture), "--windows", "1", "--horizons", "1,2", "--output", str(out_json)]
    )
    out = capsys.readouterr().out
    assert exit_code == 0
    assert "OFI read — BTC" in out
    assert "caveats:" in out
    reports = json.loads(out_json.read_text())
    assert len(reports) == 1 and reports[0]["symbol"] == "BTC"
    assert [r["horizon"] for r in reports[0]["results"]] == ["1s", "2s", "next(1s)"]


def test_cli_fails_cleanly_on_unusable_input(tmp_path, capsys):
    empty = tmp_path / "empty.jsonl"
    empty.write_text("garbage\n\n")
    assert main([str(empty)]) == 1
    assert "no usable" in capsys.readouterr().out
