"""End-to-end replay: snapshot + recorded diff files -> deterministic state.

Runs only on the synthetic fixtures in tests/fixtures/book/ — the same files
the replay CLI is pointed at in the definition-of-done check. Pins the final
book numbers, the anomaly/stale accounting, and checksum determinism.
"""

import lz4.frame

from hyperliquid_pipeline.book.replay import main, replay


def test_snapshot_plus_blocks_end_to_end(fixtures):
    report = replay(
        str(fixtures / "snapshot_small.json"),
        [str(fixtures / "block_envelope.jsonl")],
    )
    assert report.coin == "BTC"
    assert report.blocks_applied == 3
    assert report.diffs_applied == 6
    assert report.skipped_other_coin == 0
    assert report.anomaly_count == 0
    assert report.stale is False
    assert report.crossed is False
    # After: +1004(0.25) -1001(0.5) on the bid, 2001->0.15 +2004(0.1) -2002 on the ask.
    assert report.best_bid == ("115000.0", 0.55)
    assert report.best_ask == ("115000.5", 0.25)
    assert report.mid == 115000.25
    assert report.height == 103
    assert report.last_update_ms == 1764867593000


def test_checksum_is_deterministic(fixtures):
    snapshot = str(fixtures / "snapshot_small.json")
    diffs = [str(fixtures / "block_envelope.jsonl")]
    first = replay(snapshot, diffs)
    second = replay(snapshot, diffs)
    assert first.checksum == second.checksum
    # ...and actually depends on book state:
    assert first.checksum != replay(None, [str(fixtures / "events_per_line.jsonl")]).checksum


def test_lz4_replay_matches_plain(tmp_path, fixtures):
    plain = fixtures / "block_envelope.jsonl"
    compressed = tmp_path / "block_envelope.jsonl.lz4"
    with lz4.frame.open(compressed, mode="wt") as fh:
        fh.write(plain.read_text())
    snapshot = str(fixtures / "snapshot_small.json")
    assert (
        replay(snapshot, [str(compressed)]).checksum
        == replay(snapshot, [str(plain)]).checksum
    )


def test_bare_events_replay_adopts_coin_and_skips_others(fixtures):
    report = replay(None, [str(fixtures / "events_per_line.jsonl")])
    assert report.coin == "BTC"           # adopted from the first diff seen
    assert report.skipped_other_coin == 1  # the CHILLGUY line
    assert report.blocks_applied == 0      # bare events, no envelopes
    assert report.diffs_applied == 5
    assert report.anomaly_count == 0
    assert report.best_bid == ("115323.2", 0.707)  # 0.207 + 0.5, FIFO intact
    assert report.best_ask is None                 # oid 21 was removed
    assert report.mid is None


def test_height_gap_fixture_marks_stale(fixtures):
    report = replay(None, [str(fixtures / "height_gap.jsonl")])
    assert report.stale is True
    assert "height_gap" in [a["type"] for a in report.anomalies]
    assert report.blocks_applied == 2
    assert report.height == 105


def test_crossed_fixture_reports_crossed(fixtures):
    report = replay(None, [str(fixtures / "crossed_book.jsonl")])
    assert report.crossed is True
    assert report.best_bid == ("115400.0", 1.0)
    assert report.best_ask == ("115399.0", 1.0)


def test_on_block_callback_sees_each_block(fixtures):
    seen = []
    replay(
        str(fixtures / "snapshot_small.json"),
        [str(fixtures / "block_envelope.jsonl")],
        on_block=lambda book, batch: seen.append((batch.height, book.best_bid())),
    )
    assert [height for height, _ in seen] == [101, 102, 103]
    # The callback reads the live book *after* each block is applied.
    assert seen[0][1] == ("115000.0", 1.05)  # 0.5 + 0.3 + new 0.25
    assert seen[-1][1] == ("115000.0", 0.55)


def test_cli_prints_a_sane_report(fixtures, capsys):
    exit_code = main(
        [str(fixtures / "snapshot_small.json"), str(fixtures / "block_envelope.jsonl")]
    )
    out = capsys.readouterr().out
    assert exit_code == 0
    assert "coin:" in out and "BTC" in out
    assert "blocks applied:" in out
    assert "115000.0 x 0.55" in out
    assert "115000.5 x 0.25" in out
    assert "checksum:" in out


def test_cli_dash_starts_from_empty_book(fixtures, capsys):
    exit_code = main(["-", str(fixtures / "events_per_line.jsonl")])
    out = capsys.readouterr().out
    assert exit_code == 0
    assert "BTC" in out
    assert "115323.2 x 0.707" in out
