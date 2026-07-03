"""Tests for diff_parser: the only module that knows on-disk node formats.

Everything here pins external format facts: the three verified raw_book_diff
variants, both side encodings, both line shapes (bare event vs block
envelope) auto-detected, strict-mode drift reporting with the offending keys,
lz4 iteration, and snapshot snake/camel key normalization. No network — the
verbatim lines below are copied from the official node data-schema docs.
"""

import json

import lz4.frame
import pytest

from hyperliquid_pipeline.book.diff_parser import (
    UnrecognizedDiffFormat,
    iter_diff_file,
    load_l4_snapshot_file,
    parse_l4_snapshot,
    parse_line,
)
from hyperliquid_pipeline.book.schemas import BlockDiffBatch, BookDiff

# Verbatim examples from the official L1 data-schema docs.
NEW_LINE = '{"user":"0x768484f7e2ebb675c57838366c02ae99ba2a9b08","oid":35061046831,"coin":"CHILLGUY","side":"Bid","px":"1.36","raw_book_diff":{"new":{"sz":"186910.0"}}}'
UPDATE_LINE = '{"user":"0x768484f7e2ebb675c57838366c02ae99ba2a9b08","oid":35061055064,"coin":"BTC","side":"Bid","px":"115323.2","raw_book_diff":{"update":{"origSz":"0.2086","newSz":"0.207"}}}'
REMOVE_LINE = '{"user":"0x768484f7e2ebb675c57838366c02ae99ba2a9b08","oid":300607578684,"coin":"xyz:XYZ100","side":"A","px":"25471.0","raw_book_diff":"remove"}'


def _event(**overrides):
    base = {
        "user": "0xcccccccccccccccccccccccccccccccccccccc01",
        "oid": 7,
        "coin": "BTC",
        "side": "B",
        "px": "115000.0",
        "raw_book_diff": {"new": {"sz": "1.0"}},
    }
    base.update(overrides)
    return json.dumps(base)


def test_parses_all_three_verified_variants():
    new = parse_line(NEW_LINE)
    assert isinstance(new, BookDiff)
    assert new.kind == "new"
    assert (new.oid, new.coin, new.px, new.sz) == (
        35061046831, "CHILLGUY", "1.36", "186910.0",
    )

    update = parse_line(UPDATE_LINE)
    assert update.kind == "update"
    assert (update.orig_sz, update.new_sz) == ("0.2086", "0.207")
    assert update.sz is None

    remove = parse_line(REMOVE_LINE)
    assert remove.kind == "remove"
    assert remove.coin == "xyz:XYZ100"  # coins with colons stay opaque
    assert (remove.sz, remove.orig_sz, remove.new_sz) == (None, None, None)


def test_normalizes_both_side_encodings():
    # Official examples use BOTH "Bid"/"Ask" and "A"/"B" — all map to B|A.
    for wire, expected in (("Bid", "B"), ("Ask", "A"), ("B", "B"), ("A", "A")):
        assert parse_line(_event(side=wire)).side == expected


def test_prices_and_sizes_stay_exact_strings():
    diff = parse_line(_event(px="115323.20", raw_book_diff={"new": {"sz": "0.2086"}}))
    assert diff.px == "115323.20"  # no float round-trip at the schema layer
    assert diff.sz == "0.2086"


def test_autodetects_bare_event_and_block_envelope():
    event_obj = json.loads(_event())
    envelope = json.dumps(
        {
            "time": 1764867591000,
            "height": 101,
            "data": {"order_statuses": [], "book_diffs": [event_obj, event_obj]},
        }
    )
    parsed = parse_line(envelope)
    assert isinstance(parsed, BlockDiffBatch)
    assert (parsed.time_ms, parsed.height, len(parsed.diffs)) == (
        1764867591000, 101, 2,
    )
    assert all(isinstance(d, BookDiff) for d in parsed.diffs)

    assert isinstance(parse_line(_event()), BookDiff)


def test_flat_envelope_variant_also_detected():
    flat = json.dumps(
        {"time": 1, "height": 2, "book_diffs": [json.loads(_event())]}
    )
    parsed = parse_line(flat)
    assert isinstance(parsed, BlockDiffBatch)
    assert len(parsed.diffs) == 1


def test_strict_mode_names_unknown_diff_variant():
    line = _event(raw_book_diff={"modify": {"sz": "1.0"}})
    assert parse_line(line) is None  # tolerant by default
    with pytest.raises(UnrecognizedDiffFormat) as excinfo:
        parse_line(line, strict=True)
    assert "modify" in str(excinfo.value)


def test_strict_mode_names_unknown_and_missing_event_keys():
    unknown = _event(surprise=1)
    assert isinstance(parse_line(unknown), BookDiff)  # extras tolerated
    with pytest.raises(UnrecognizedDiffFormat) as excinfo:
        parse_line(unknown, strict=True)
    assert "unknown:surprise" in str(excinfo.value)

    missing = json.loads(_event())
    del missing["px"]
    assert parse_line(json.dumps(missing)) is None
    with pytest.raises(UnrecognizedDiffFormat) as excinfo:
        parse_line(json.dumps(missing), strict=True)
    assert "missing:px" in str(excinfo.value)


def test_strict_mode_names_missing_envelope_keys():
    envelope = json.dumps({"height": 3, "data": {"book_diffs": []}})
    assert parse_line(envelope) is None
    with pytest.raises(UnrecognizedDiffFormat) as excinfo:
        parse_line(envelope, strict=True)
    assert "missing:time" in str(excinfo.value)


def test_blank_and_garbage_lines():
    assert parse_line("") is None
    assert parse_line("   \n") is None
    assert parse_line("not json at all") is None
    assert parse_line("[1, 2, 3]") is None  # JSON but not a known shape
    with pytest.raises(UnrecognizedDiffFormat):
        parse_line("not json at all", strict=True)
    with pytest.raises(UnrecognizedDiffFormat):
        parse_line("[1, 2, 3]", strict=True)


def test_update_variant_tolerates_snake_case_keys():
    line = _event(raw_book_diff={"update": {"orig_sz": "1.0", "new_sz": "0.5"}})
    diff = parse_line(line)
    assert (diff.kind, diff.orig_sz, diff.new_sz) == ("update", "1.0", "0.5")


def test_iter_diff_file_plain_and_lz4_agree(tmp_path, fixtures):
    plain_path = fixtures / "events_per_line.jsonl"
    plain = list(iter_diff_file(plain_path))
    assert len(plain) == 6
    assert all(isinstance(d, BookDiff) for d in plain)

    lz4_path = tmp_path / "events_per_line.jsonl.lz4"
    with lz4.frame.open(lz4_path, mode="wt") as fh:
        fh.write(plain_path.read_text())
    assert list(iter_diff_file(lz4_path)) == plain


def test_iter_diff_file_skips_blank_and_bad_lines(tmp_path):
    path = tmp_path / "mixed.jsonl"
    path.write_text(f"\n{_event()}\ngarbage\n\n{_event(oid=8)}\n")
    skipped = []
    parsed = list(iter_diff_file(path, on_skip=skipped.append))
    assert [d.oid for d in parsed] == [7, 8]
    # Only the non-blank unparseable line counts as a skip — a dropped line
    # can hide a remove, so feeds need to see it happened.
    assert skipped == ["garbage"]
    with pytest.raises(UnrecognizedDiffFormat):
        list(iter_diff_file(path, strict=True))


def test_load_l4_snapshot_file(fixtures):
    snap = load_l4_snapshot_file(fixtures / "snapshot_small.json")
    assert (snap.coin, snap.height, snap.time_ms) == ("BTC", 100, 1764867590000)
    assert [o.oid for o in snap.bids] == [1001, 1002, 1003]
    assert [o.oid for o in snap.asks] == [2001, 2002, 2003]
    first = snap.bids[0]
    assert (first.side, first.limit_px, first.sz, first.tif) == (
        "B", "115000.0", "0.5", "Gtc",
    )


def test_parse_l4_snapshot_normalizes_camel_case_orders():
    # Node order_statuses use camelCase (limitPx); snapshots use snake_case.
    snap = parse_l4_snapshot(
        {
            "coin": "BTC",
            "time": 5,
            "height": 9,
            "bids": [
                {
                    "user": "0xcccccccccccccccccccccccccccccccccccccc01",
                    "oid": 1,
                    "side": "B",
                    "limitPx": "100.5",
                    "sz": "2.0",
                    "orderType": "Limit",
                    "reduceOnly": True,
                    "isTrigger": False,
                    "triggerPx": None,
                    "isPositionTpsl": False,
                    "triggerCondition": "N/A",
                    "tif": "Alo",
                }
            ],
            "asks": [],
        }
    )
    order = snap.bids[0]
    assert (order.limit_px, order.order_type, order.reduce_only, order.tif) == (
        "100.5", "Limit", True, "Alo",
    )


def test_parse_l4_snapshot_raises_on_structural_drift():
    with pytest.raises(UnrecognizedDiffFormat) as excinfo:
        parse_l4_snapshot({"coin": "BTC", "bids": []})
    assert "missing:asks" in str(excinfo.value)

    with pytest.raises(UnrecognizedDiffFormat) as excinfo:
        parse_l4_snapshot(
            {"coin": "BTC", "bids": [{"oid": 1, "side": "B"}], "asks": []}
        )
    assert "missing:limit_px" in str(excinfo.value)
