#!/usr/bin/env python3
"""Validate the book core against REAL Hyperliquid node data.

The diff parser was built from documented formats, but the --batch-by-block
envelope spelling is only partially verified (every assumption is tagged
VERIFY-ON-REAL-DATA in book/diff_parser.py). This script is the moment of
truth: run one real file through strict mode and either everything parses —
assumptions pinned — or UnrecognizedDiffFormat names exactly which keys
drifted, which is a five-minute fix in diff_parser.py.

Two ways in:

    # A file you already have (provider dump, node copy, any source):
    python scripts/validate_book_real_data.py --file path/to/hour_file[.lz4]

    # Straight from Hyperliquid's requester-pays node-data bucket
    # (needs AWS credentials in .env / environment; transfer bills to you):
    python scripts/validate_book_real_data.py --s3 --date 20260701 --hour 12

Exit code 0 = all lines parsed strict + books stayed sane; 1 = format drift
or book-integrity problems (details printed).
"""

import sys
from collections import Counter
from pathlib import Path
from typing import Optional

import typer
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hyperliquid_pipeline.book.diff_parser import (  # noqa: E402
    UnrecognizedDiffFormat,
    iter_diff_file,
)
from hyperliquid_pipeline.book.l4_book import L4Book  # noqa: E402
from hyperliquid_pipeline.book.schemas import BlockDiffBatch, BookDiff  # noqa: E402
from hyperliquid_pipeline.config.settings import settings  # noqa: E402

# VERIFY-ON-REAL-DATA: the bucket's raw-book-diff key layout is not publicly
# pinned; these candidates are tried in order and --discover lists what's
# actually there.
CANDIDATE_PREFIXES = (
    "node_raw_book_diffs/hourly/{date}/{hour}",
    "raw_book_diffs/hourly/{date}/{hour}",
    "node_raw_book_diffs/{date}/{hour}",
)

TRACKED_COINS = ("BTC", "ETH", "SOL")  # the trading universe (perps)


def _validate_file(path: Path, coins: tuple) -> int:
    """Strict-parse one node file and drive L4 books; return exit code."""
    logger.info(f"strict-parsing {path}")
    books = {}
    counts = Counter()
    line_no = 0
    try:
        for item in iter_diff_file(path, strict=True):
            line_no += 1
            if isinstance(item, BookDiff):
                counts["bare_events"] += 1
                if item.coin in coins:
                    books.setdefault(item.coin, L4Book(item.coin)).apply(item)
                    counts["diffs_applied"] += 1
            elif isinstance(item, BlockDiffBatch):
                counts["blocks"] += 1
                per_coin = {}
                for diff in item.diffs:
                    counts["block_diffs_seen"] += 1
                    if diff.coin in coins:
                        per_coin.setdefault(diff.coin, []).append(diff)
                for coin in set(books) | set(per_coin):
                    books.setdefault(coin, L4Book(coin)).apply_block(
                        BlockDiffBatch(item.time_ms, item.height, per_coin.get(coin, []))
                    )
                counts["diffs_applied"] += sum(len(d) for d in per_coin.values())
    except UnrecognizedDiffFormat as drift:
        print("\n=== FORMAT DRIFT FOUND (this is what we came for) ===")
        print(f"after {line_no} parsed lines: {drift}")
        print(f"offending keys: {drift.offending}")
        print("fix location: src/hyperliquid_pipeline/book/diff_parser.py "
              "(the only format-aware module) + a fixture in tests/fixtures/book/")
        return 1

    print("\n=== STRICT PARSE: CLEAN — format assumptions pinned ===")
    for key, value in sorted(counts.items()):
        print(f"{key:>18}: {value}")

    problems = 0
    for coin, book in sorted(books.items()):
        bb, ba = book.best_bid(), book.best_ask()
        anomaly_types = Counter(a.get("type", "?") for a in book.anomalies)
        # A no-snapshot replay legitimately accumulates unknown-oid anomalies
        # (removes/updates for orders that predate the file) — report, don't fail.
        integrity_bad = book.is_crossed()
        problems += integrity_bad
        print(f"\n{coin}: orders={len(book)} best_bid={bb} best_ask={ba} "
              f"crossed={book.is_crossed()} stale={book.stale} "
              f"anomalies={book.anomaly_count} {dict(anomaly_types) or ''}")
        if integrity_bad:
            print(f"  !! {coin} book is crossed — investigate before trusting the core")

    return 1 if problems else 0


def _s3_download(date: str, hour: int, key: Optional[str], max_mb: float,
                 assume_yes: bool, discover: bool, region: str) -> Optional[Path]:
    """Find and download one raw-book-diff file from the node-data bucket."""
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

    # Credentials come from settings (.env) explicitly — boto3's default
    # chain can't read .env. Same pattern as HistoricalDataCollector.
    s3 = boto3.client(
        "s3",
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=region,
    )
    bucket = settings.node_data_bucket
    pay = {"RequestPayer": "requester"}

    try:
        if discover:
            roots = s3.list_objects_v2(Bucket=bucket, Delimiter="/", MaxKeys=50, **pay)
            print(f"top-level prefixes in s3://{bucket}/:")
            for pfx in roots.get("CommonPrefixes", []):
                print(f"  {pfx['Prefix']}")
            return None

        candidates = []
        if key:
            head = s3.head_object(Bucket=bucket, Key=key, **pay)
            candidates = [(key, head["ContentLength"])]
        else:
            for template in CANDIDATE_PREFIXES:
                prefix = template.format(date=date, hour=hour)
                page = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=100, **pay)
                for obj in page.get("Contents", []):
                    candidates.append((obj["Key"], obj["Size"]))
                if candidates:
                    break
        if not candidates:
            logger.error("no objects found under any candidate prefix — "
                         "run with --discover to see the bucket's real layout")
            return None

        candidates.sort(key=lambda pair: pair[1])
        chosen, size = candidates[0]
        size_mb = size / 1e6
        print(f"chosen s3://{bucket}/{chosen} ({size_mb:.1f} MB of {len(candidates)} candidates)")
        if size_mb > max_mb and not assume_yes:
            print(f"object exceeds --max-mb {max_mb} (requester-pays: transfer bills to "
                  f"your AWS account). Re-run with --yes to proceed.")
            return None

        dest = Path("data/node_validation") / Path(chosen).name
        dest.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, chosen, str(dest), ExtraArgs=pay)
        logger.info(f"downloaded to {dest}")
        return dest

    except NoCredentialsError:
        logger.error("no AWS credentials — put AWS_ACCESS_KEY_ID / "
                     "AWS_SECRET_ACCESS_KEY in .env (requester-pays bucket).")
        return None
    except ClientError as e:
        logger.error(f"S3 error: {e}")
        return None


def main(
    file: Optional[Path] = typer.Option(None, "--file", "-f",
                                        help="Validate a local node file (plain or .lz4)"),
    s3: bool = typer.Option(False, "--s3", help="Fetch from the hl-mainnet-node-data bucket"),
    date: str = typer.Option("", "--date", help="UTC date YYYYMMDD (s3 mode)"),
    hour: int = typer.Option(0, "--hour", help="UTC hour 0-23 (s3 mode)"),
    key: str = typer.Option("", "--key", help="Exact S3 key (skips prefix discovery)"),
    discover: bool = typer.Option(False, "--discover", help="List the bucket's top-level layout"),
    max_mb: float = typer.Option(64.0, "--max-mb", help="Download size guard (requester-pays)"),
    yes: bool = typer.Option(False, "--yes", help="Proceed past the size guard"),
    region: str = typer.Option(
        "ap-northeast-1", "--region",
        help="Bucket region (hl-mainnet-node-data lives in Tokyo, per its "
             "x-amz-bucket-region header)",
    ),
):
    """Strict-validate the book core against one real node data file."""
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level: <7} | {message}")

    if file is None and not s3 and not discover:
        typer.echo("need --file <path>, or --s3 --date/--hour (see --help)")
        raise typer.Exit(2)

    if file is None and (s3 or discover):
        if not discover and not (date or key):
            typer.echo("--s3 needs --date YYYYMMDD (and optionally --hour/--key)")
            raise typer.Exit(2)
        file = _s3_download(date, hour, key or None, max_mb, yes, discover, region)
        if file is None:
            raise typer.Exit(1 if not discover else 0)

    raise typer.Exit(_validate_file(file, TRACKED_COINS))


if __name__ == "__main__":
    typer.run(main)
