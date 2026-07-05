# Track 2 brief: build the order-book core package (`book/`) for the Hyperliquid HFT system

You are the second agent on a two-track parallel build. Track 1 (another session, IPC id `s-mr53om6k-u71x`) is upgrading the collector/storage/config layers. **You own ONLY these paths — zero overlap with Track 1:**

- `src/hyperliquid_pipeline/book/` (new package: `__init__.py`, `schemas.py`, `diff_parser.py`, `l4_book.py`, `l2_book.py`, `replay.py`)
- `tests/book/`
- `tests/fixtures/book/`
- Exception: add `sortedcontainers` to `requirements.txt` and `pyproject.toml` — **announce via IPC before touching those two files**.

Do NOT edit any other existing file. Match the repo's test culture: behavior-contract pytest, no network (see `tests/test_backpressure.py` for tone). Python 3.11, src layout, `tests/conftest.py` already handles sys.path.

## Why (context)

This repo is being evolved into an HFT system on Hyperliquid per a design handoff: priority data is (1) event-level BBO, (2) L3/MBO order-by-order data from a non-validating node (`--write-raw-book-diffs`), (3) top 5–10 depth levels. Your package is the **market-state core**: parse node raw book diffs, reconstruct full L4 (order-level) books with queue-position queries, and provide a common `BookView` read interface that an L2 snapshot book also implements — so the rest of the system is book-implementation-agnostic. Track 1 will integrate it after you land (refactoring `OrderBookProcessor` to consume `L2Book`, and adding a `NodeDiffFeed` that drives `L4Book` via your `diff_parser`).

## Verified external formats (cite-checked; encode these exactly)

**Node `--write-raw-book-diffs` events** (verified from https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/nodes/l1-data-schemas — official verbatim examples):
```json
{"user":"0x768484f7e2ebb675c57838366c02ae99ba2a9b08","oid":35061046831,"coin":"CHILLGUY","side":"Bid","px":"1.36","raw_book_diff":{"new":{"sz":"186910.0"}}}
{"user":"0x...","oid":35061055064,"coin":"BTC","side":"Bid","px":"115323.2","raw_book_diff":{"update":{"origSz":"0.2086","newSz":"0.207"}}}
{"user":"0x...","oid":300607578684,"coin":"xyz:XYZ100","side":"A","px":"25471.0","raw_book_diff":"remove"}
```
Three `raw_book_diff` variants: object `{"new":{"sz"}}`, object `{"update":{"origSz","newSz"}}`, **bare string** `"remove"`. Note `side` appears as BOTH `"Bid"/"Ask"` and `"A"/"B"` across official examples — normalize both. Files land at `~/hl/data/node_raw_book_diffs/hourly/{date}/{hour}`, one JSON event per line; with `--batch-by-block`, one block per line.

**Block envelope** (partially verified — from https://www.quicknode.com/docs/hyperliquid/datasets/l4-book): per-block wrapper with `time` (ms), `height` (block height), and a `data` payload containing `order_statuses` and `book_diffs` arrays. The exact on-disk key spellings of the `--batch-by-block` wrapper are NOT fully verified publicly.

**L4 snapshot** (verified from https://github.com/hyperliquid-dex/order_book_server and the QuickNode docs): `{coin, time, height, bids: [Order], asks: [Order]}`, Order = `{user, coin, side: "A"|"B", limit_px: "3167.4", sz: "1.5785", oid: 258166296856, timestamp: 1764867590000, trigger_condition, is_trigger, trigger_px, is_position_tpsl, reduce_only, order_type, tif}`. Snapshot uses **snake_case** (`limit_px`) while node `order_statuses` use **camelCase** (`limitPx`) — normalize both.

## Deliverables

### 1. `schemas.py`
Frozen dataclasses + normalizers:
- `BookDiff(user, oid, coin, side, px, kind, sz, orig_sz, new_sz)` — `kind ∈ {'new','update','remove'}`, `side` normalized to `'B'|'A'`.
- `BlockDiffBatch(time_ms, height, diffs: list[BookDiff])`.
- `L4Order(oid, user, side, limit_px, sz, timestamp, tif, order_type, reduce_only, ...)`.
- `normalize_side()` accepting `"Bid"/"Ask"/"B"/"A"`; key normalization accepting `limit_px`/`limitPx`.
- Keep px/sz as `str` at the schema layer (exact decimal fidelity); convert lazily.

### 2. `diff_parser.py` — the ONLY module that knows on-disk formats
- `parse_line(line, strict=False) -> BookDiff | BlockDiffBatch` with **auto-detection** of line shape (bare event vs block envelope).
- Strict mode raises `UnrecognizedDiffFormat` listing the offending/unknown keys (so the first hour of real node data pins the envelope format in minutes). Every format assumption gets a `# VERIFY-ON-REAL-DATA` comment.
- `iter_diff_file(path)` handling plain and `.lz4` files (lz4 is already a repo dep).
- Snapshot loader: `parse_l4_snapshot(obj) -> ` whatever `L4Book.load_snapshot` takes.

### 3. `l4_book.py` — `L4Book`, one instance per coin
**Frozen read protocol (Track 1 consumes these exact signatures — do not change):**
```python
best_bid() -> tuple[str, float] | None    # (px, total_sz)
best_ask() -> tuple[str, float] | None
mid() -> float | None
depth(n: int) -> dict                     # top-n levels per side
is_crossed() -> bool
last_update_ms: int
```
Plus: `load_snapshot(snapshot)`, `apply(diff: BookDiff)`, `apply_block(batch: BlockDiffBatch)`, `queue_position(oid) -> tuple[int, float] | None` (orders ahead, size ahead at that order's price level), `anomalies: list`, `stale: bool`.

Structures: `dict[oid → OrderRef]`; per side a `sortedcontainers.SortedDict` keyed by price (bids via negated key for descending) → `PriceLevel` = insertion-ordered dict `oid → sz` (Python dict order = FIFO time priority) + cached `total_sz`.

Semantics:
- `new`: append to level (create level if absent).
- `update`: adjust size **in place, preserving queue position** (document this as an assumption — partial fills/size-downs keep priority; revisit when real node data lands).
- `remove`: delete from level; drop empty levels.
- Unknown-oid update/remove: tolerate, count in `anomalies`, never raise.
- `apply_block` with non-consecutive height: set `stale=True`, record anomaly.

### 4. `l2_book.py` — `L2Book`, same read protocol
Fed by `update_from_snapshot(bids, asks, time_ms)` where bids/asks are lists of `{px: str, sz: str, n: int}` — the exact shape the live collector already produces (reference read-only: `process_orderbook_message` in `src/hyperliquid_pipeline/collectors/realtime_collector.py`).

### 5. `replay.py`
`replay(snapshot_path: str | None, diff_paths: list[str], on_block=None) -> ReplayReport` (blocks applied, diffs applied, anomaly count/list, stale flag, final best bid/ask, deterministic book checksum). Plus CLI: `python -m hyperliquid_pipeline.book.replay <snapshot.json> <diffs.jsonl ...>` printing the report.

### 6. Fixtures + tests
Synthetic fixtures in `tests/fixtures/book/`: `events_per_line.jsonl`, `block_envelope.jsonl`, `snapshot_small.json`, a crossed-book scenario, a height-gap scenario.

Tests must pin: all three diff variants; both side encodings; both line shapes auto-detected; strict-mode raise with offending keys; lz4 iteration; snapshot load → best/depth; FIFO queue position (including: position improves after a fill ahead; position preserved across an `update`); crossed-book flag; height gap → stale; unknown-oid counted not raised; deterministic replay checksum end-to-end.

## Definition of done
1. `python -m pytest tests/book/ -v` fully green.
2. `python -m pytest tests/ -v` green (nothing else broken — you shouldn't have touched anything else).
3. `python -m hyperliquid_pipeline.book.replay tests/fixtures/book/snapshot_small.json tests/fixtures/book/block_envelope.jsonl` prints a sane report.
4. IPC-message Track 1 (`s-mr53om6k-u71x`) when done, and before touching `requirements.txt`/`pyproject.toml`. Do not commit — Track 1 coordinates commits.
