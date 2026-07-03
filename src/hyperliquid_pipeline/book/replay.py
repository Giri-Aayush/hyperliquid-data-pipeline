"""Deterministic replay of recorded node book data into an :class:`L4Book`.

Library entry point :func:`replay` plus a CLI:

    python -m hyperliquid_pipeline.book.replay <snapshot.json> <diffs ...>

Feed it an optional L4 snapshot and any number of node files (bare-event or
--batch-by-block lines, plain or .lz4, auto-detected per line); it applies
everything in order and reports final book state with a deterministic
checksum — same inputs, same checksum, byte for byte.

Replay is single-coin (node files interleave many coins): the coin comes from
the snapshot, or else the first diff seen; events for other coins are counted
as skipped, not applied and not treated as anomalies.
"""

import argparse
from dataclasses import dataclass
from typing import Any, Callable

from hyperliquid_pipeline.book.diff_parser import (
    iter_diff_file,
    load_l4_snapshot_file,
)
from hyperliquid_pipeline.book.l4_book import L4Book
from hyperliquid_pipeline.book.schemas import BlockDiffBatch, BookDiff


@dataclass
class ReplayReport:
    """Final state and accounting of one replay run."""

    coin: str | None
    blocks_applied: int
    diffs_applied: int
    skipped_other_coin: int
    parse_skips: int  # non-blank lines/elements the tolerant parser dropped
    anomaly_count: int
    anomalies: list[dict]
    stale: bool
    crossed: bool
    best_bid: tuple[str, float] | None
    best_ask: tuple[str, float] | None
    mid: float | None
    height: int | None
    last_update_ms: int
    checksum: str


def replay(
    snapshot_path: str | None,
    diff_paths: list[str],
    on_block: Callable[[L4Book, BlockDiffBatch], Any] | None = None,
) -> ReplayReport:
    """Rebuild a book from a snapshot plus diff files and report the outcome.

    ``on_block`` (if given) is called as ``on_block(book, batch)`` after each
    block is applied — the hook for strategies/metrics that want block-aligned
    reads during replay.
    """
    book = L4Book()
    if snapshot_path:
        book.load_snapshot(load_l4_snapshot_file(snapshot_path))

    coin = book.coin
    blocks_applied = 0
    diffs_applied = 0
    skipped_other_coin = 0
    parse_skips = 0

    def count_skip(_line: str) -> None:
        nonlocal parse_skips
        parse_skips += 1

    for path in diff_paths:
        for item in iter_diff_file(path, on_skip=count_skip):
            if isinstance(item, BookDiff):
                if coin is None:
                    coin = item.coin
                    book.coin = coin
                if item.coin != coin:
                    skipped_other_coin += 1
                    continue
                book.apply(item)
                diffs_applied += 1
            else:  # BlockDiffBatch
                if coin is None and item.diffs:
                    coin = item.diffs[0].coin
                    book.coin = coin
                kept = [d for d in item.diffs if coin is None or d.coin == coin]
                skipped_other_coin += len(item.diffs) - len(kept)
                batch = BlockDiffBatch(
                    time_ms=item.time_ms, height=item.height, diffs=kept
                )
                book.apply_block(batch)
                blocks_applied += 1
                diffs_applied += len(kept)
                if on_block is not None:
                    on_block(book, batch)

    return ReplayReport(
        coin=coin,
        blocks_applied=blocks_applied,
        diffs_applied=diffs_applied,
        skipped_other_coin=skipped_other_coin,
        parse_skips=parse_skips,
        anomaly_count=len(book.anomalies),
        anomalies=list(book.anomalies),
        stale=book.stale,
        crossed=book.is_crossed(),
        best_bid=book.best_bid(),
        best_ask=book.best_ask(),
        mid=book.mid(),
        height=book.height,
        last_update_ms=book.last_update_ms,
        checksum=book.checksum(),
    )


def _format_report(report: ReplayReport) -> str:
    def px_sz(value: tuple[str, float] | None) -> str:
        return "-" if value is None else f"{value[0]} x {value[1]}"

    lines = [
        f"coin:               {report.coin or '-'}",
        f"blocks applied:     {report.blocks_applied}",
        f"diffs applied:      {report.diffs_applied}",
        f"skipped other coin: {report.skipped_other_coin}",
        f"parse skips:        {report.parse_skips}",
        f"anomalies:          {report.anomaly_count}",
        f"stale:              {report.stale}",
        f"crossed:            {report.crossed}",
        f"best bid:           {px_sz(report.best_bid)}",
        f"best ask:           {px_sz(report.best_ask)}",
        f"mid:                {report.mid if report.mid is not None else '-'}",
        f"height:             {report.height if report.height is not None else '-'}",
        f"last update ms:     {report.last_update_ms}",
        f"checksum:           {report.checksum}",
    ]
    if report.anomalies:
        shown = ", ".join(a.get("type", "?") for a in report.anomalies[:5])
        extra = len(report.anomalies) - 5
        lines.append(
            f"anomaly types:      {shown}" + (f" (+{extra} more)" if extra > 0 else "")
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m hyperliquid_pipeline.book.replay",
        description=(
            "Replay Hyperliquid node book data (bare events or block "
            "envelopes, plain or .lz4) into an L4 book and print a report."
        ),
    )
    parser.add_argument(
        "snapshot",
        help="L4 snapshot .json path, or '-' to start from an empty book",
    )
    parser.add_argument(
        "diffs", nargs="+", help="diff files, applied in the order given"
    )
    args = parser.parse_args(argv)

    snapshot_path = None if args.snapshot == "-" else args.snapshot
    print(_format_report(replay(snapshot_path, args.diffs)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
