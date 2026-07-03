"""Parsers for on-disk Hyperliquid node book data — the ONLY module that knows
raw formats.

Three inputs, all newline-delimited JSON except snapshots:

* Bare events (``--write-raw-book-diffs``): one object per line, with a
  ``raw_book_diff`` payload in three verified variants —
  ``{"new": {"sz"}}``, ``{"update": {"origSz", "newSz"}}``, and the bare
  string ``"remove"``. ``side`` appears as both ``"Bid"/"Ask"`` and
  ``"A"/"B"`` across official examples; both are normalized.
* Block envelopes (``--batch-by-block``): one block per line wrapping a
  ``book_diffs`` array. The exact on-disk wrapper spelling is only partially
  verified publicly — every assumption is tagged ``VERIFY-ON-REAL-DATA``.
* L4 snapshots: ``{coin, time, height, bids, asks}`` with snake_case order
  keys (``limit_px``); camelCase (``limitPx``) is normalized too.

Tolerance model: with ``strict=False`` (default) parsing is stream-tolerant —
unrecognized lines/objects come back as ``None`` and iterators skip them.
With ``strict=True`` any drift raises :class:`UnrecognizedDiffFormat` naming
the offending keys, so running one hour of real node output through strict
mode pins every unverified assumption in minutes.
"""

import json
from pathlib import Path
from typing import Any, Callable, Iterator, TextIO

import lz4.frame

from hyperliquid_pipeline.book.schemas import (
    BlockDiffBatch,
    BookDiff,
    L4Order,
    L4Snapshot,
    normalize_side,
    pick_key,
)


class UnrecognizedDiffFormat(ValueError):
    """A line/object doesn't match any known node book format.

    ``offending`` carries the unknown/missing key names (or a shape
    description) so real data pins the actual format immediately.
    """

    def __init__(self, message: str, offending: list[str] | None = None):
        self.offending = sorted(offending or [])
        if self.offending:
            message = f"{message}: {', '.join(self.offending)}"
        super().__init__(message)


# Verified from official docs verbatim examples.
_EVENT_KEYS = {"user", "oid", "coin", "side", "px", "raw_book_diff"}
_EVENT_REQUIRED = {"oid", "coin", "side", "px", "raw_book_diff"}

# VERIFY-ON-REAL-DATA: wrapper spelling {"time", "height", "data":
# {"order_statuses", "book_diffs"}} comes from QuickNode's L4 dataset docs;
# the node's exact --batch-by-block on-disk keys are not fully verified
# publicly. A flat {"time", "height", "book_diffs"} variant is tolerated.
_ENVELOPE_TOP_KEYS = {"time", "height", "data", "book_diffs", "order_statuses"}
_ENVELOPE_DATA_KEYS = {"order_statuses", "book_diffs"}


def parse_line(
    line: str,
    strict: bool = False,
    on_skip: Callable[[str], None] | None = None,
) -> BookDiff | BlockDiffBatch | None:
    """Parse one line of node output, auto-detecting its shape.

    Returns a :class:`BookDiff` for a bare event line, a
    :class:`BlockDiffBatch` for a ``--batch-by-block`` envelope line, or
    ``None`` for blank/unrecognized lines when ``strict=False``.
    ``on_skip`` fires for each bad *element* dropped inside an otherwise
    parseable block (whole-line drops just return None — the caller sees
    those directly).
    """
    text = line.strip()
    if not text:
        return None
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        if strict:
            raise UnrecognizedDiffFormat(f"line is not JSON ({exc.msg})") from exc
        return None
    return parse_obj(obj, strict=strict, on_skip=on_skip)


def parse_obj(
    obj: Any,
    strict: bool = False,
    on_skip: Callable[[str], None] | None = None,
) -> BookDiff | BlockDiffBatch | None:
    """Auto-detect and parse an already-decoded line object."""
    if isinstance(obj, dict):
        if "raw_book_diff" in obj:
            return _parse_event(obj, strict)
        data = obj.get("data")
        # A quiet block may carry only order_statuses and no book_diffs key;
        # it must still parse (empty batch) so height/time continuity flows —
        # dropping it would false-flag height gaps downstream.
        # VERIFY-ON-REAL-DATA
        if isinstance(data, dict) and ("book_diffs" in data or "order_statuses" in data):
            return _parse_block(obj, data, strict, on_skip)
        # VERIFY-ON-REAL-DATA: flat envelope variant
        if "book_diffs" in obj or ("order_statuses" in obj and "height" in obj):
            return _parse_block(obj, obj, strict, on_skip)
    if strict:
        keys = sorted(obj.keys()) if isinstance(obj, dict) else [type(obj).__name__]
        raise UnrecognizedDiffFormat("unrecognized line shape", keys)
    return None


def _parse_event(obj: dict, strict: bool) -> BookDiff | None:
    """Parse one raw_book_diff event object into a normalized BookDiff."""
    missing = _EVENT_REQUIRED - obj.keys()
    unknown = obj.keys() - _EVENT_KEYS
    if strict and (missing or unknown):
        raise UnrecognizedDiffFormat(
            "event keys drifted from the verified schema",
            [f"missing:{k}" for k in missing] + [f"unknown:{k}" for k in unknown],
        )
    if missing:
        return None

    try:
        side = normalize_side(obj["side"])
        oid = int(obj["oid"])
    except (ValueError, TypeError) as exc:
        if strict:
            raise UnrecognizedDiffFormat(
                "unparseable side/oid", [repr(obj["side"]), repr(obj["oid"])]
            ) from exc
        return None

    parsed = _parse_raw_book_diff(obj["raw_book_diff"], strict)
    if parsed is None:
        return None
    kind, sz, orig_sz, new_sz = parsed

    user = obj.get("user")
    return BookDiff(
        user=str(user) if user is not None else None,
        oid=oid,
        coin=str(obj["coin"]),
        side=side,
        px=str(obj["px"]),
        kind=kind,
        sz=sz,
        orig_sz=orig_sz,
        new_sz=new_sz,
    )


def _parse_raw_book_diff(
    raw: Any, strict: bool
) -> tuple[str, str | None, str | None, str | None] | None:
    """Decode the three verified raw_book_diff variants.

    Returns ``(kind, sz, orig_sz, new_sz)`` or ``None`` (non-strict) for an
    unknown variant. Any fourth variant on real data (e.g. an object-shaped
    remove) surfaces through strict mode.  # VERIFY-ON-REAL-DATA
    """
    if raw == "remove":  # bare-string variant, verified verbatim
        return "remove", None, None, None

    if isinstance(raw, dict) and len(raw) == 1:
        kind, payload = next(iter(raw.items()))
        if kind == "new" and isinstance(payload, dict):
            sz = payload.get("sz")
            unknown = payload.keys() - {"sz"}
            if sz is not None and not unknown:
                return "new", str(sz), None, None
            if strict:
                raise UnrecognizedDiffFormat(
                    "raw_book_diff.new keys drifted",
                    [f"unknown:{k}" for k in unknown]
                    + ([] if sz is not None else ["missing:sz"]),
                )
            return None
        if kind == "update" and isinstance(payload, dict):
            # camelCase verified; snake_case tolerated. VERIFY-ON-REAL-DATA
            orig_sz = pick_key(payload, "origSz", "orig_sz")
            new_sz = pick_key(payload, "newSz", "new_sz")
            unknown = payload.keys() - {"origSz", "newSz", "orig_sz", "new_sz"}
            if orig_sz is not None and new_sz is not None and not unknown:
                return "update", None, str(orig_sz), str(new_sz)
            if strict:
                missing = [k for k, v in (("origSz", orig_sz), ("newSz", new_sz)) if v is None]
                raise UnrecognizedDiffFormat(
                    "raw_book_diff.update keys drifted",
                    [f"unknown:{k}" for k in unknown]
                    + [f"missing:{k}" for k in missing],
                )
            return None

    if strict:
        keys = sorted(raw.keys()) if isinstance(raw, dict) else [repr(raw)]
        raise UnrecognizedDiffFormat("unrecognized raw_book_diff variant", keys)
    return None


def _parse_block(
    obj: dict,
    container: dict,
    strict: bool,
    on_skip: Callable[[str], None] | None = None,
) -> BlockDiffBatch | None:
    """Parse one --batch-by-block envelope line.

    ``container`` is where ``book_diffs`` lives: ``obj["data"]`` in the
    documented nested shape, or ``obj`` itself for the flat variant.
    """
    # VERIFY-ON-REAL-DATA: time/height alias sets are speculative alternates;
    # strict mode still flags non-canonical spellings so real data pins them.
    time_ms = pick_key(obj, "time", "time_ms", "blockTime")
    height = pick_key(obj, "height", "block_height", "blockHeight")

    if strict:
        offending = [f"unknown:{k}" for k in obj.keys() - _ENVELOPE_TOP_KEYS]
        if container is not obj:
            offending += [
                f"unknown:data.{k}" for k in container.keys() - _ENVELOPE_DATA_KEYS
            ]
        if time_ms is None:
            offending.append("missing:time")
        if height is None:
            offending.append("missing:height")
        if offending:
            raise UnrecognizedDiffFormat("block envelope keys drifted", offending)
    if time_ms is None or height is None:
        return None

    raw_diffs = container.get("book_diffs") or []
    if not isinstance(raw_diffs, list):
        if strict:
            raise UnrecognizedDiffFormat(
                "book_diffs is not an array", [type(raw_diffs).__name__]
            )
        return None

    diffs: list[BookDiff] = []
    for element in raw_diffs:
        # VERIFY-ON-REAL-DATA: assumes each book_diffs element is shaped like
        # a bare event ({user, oid, coin, side, px, raw_book_diff}).
        if isinstance(element, dict):
            parsed_event = _parse_event(element, strict)
            if parsed_event is not None:
                diffs.append(parsed_event)
                continue
        if strict:
            raise UnrecognizedDiffFormat(
                "unparseable book_diffs element", [type(element).__name__]
            )
        # Non-strict: skip the bad element, keep the rest of the block — but
        # report it: a silently dropped remove is a phantom resting order.
        if on_skip is not None:
            on_skip(element if isinstance(element, str) else json.dumps(element))

    try:
        return BlockDiffBatch(time_ms=int(time_ms), height=int(height), diffs=diffs)
    except (ValueError, TypeError) as exc:
        if strict:
            raise UnrecognizedDiffFormat(
                "non-numeric time/height", [repr(time_ms), repr(height)]
            ) from exc
        return None


def _open_maybe_lz4(path: Path) -> TextIO:
    """Open plain or .lz4 node files as text (hourly files ship both ways)."""
    if path.suffix == ".lz4":
        return lz4.frame.open(path, mode="rt")  # type: ignore[return-value]
    return open(path, mode="rt")


def iter_diff_file(
    path: str | Path,
    strict: bool = False,
    on_skip: Callable[[str], None] | None = None,
) -> Iterator[BookDiff | BlockDiffBatch]:
    """Yield parsed lines from a node output file, plain or ``.lz4``.

    Node hourly files are one JSON document per line; blank lines are ignored.
    In non-strict mode an unrecognized line is dropped — ``on_skip`` (if
    given) is called with each such line so feeds can count them: a silently
    dropped "remove" would otherwise leave a phantom order in a book with
    anomaly_count still 0. (Bad *elements inside* a block are still dropped
    parser-internally in non-strict mode; use strict=True to surface those.)
    """
    with _open_maybe_lz4(Path(path)) as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            parsed = parse_line(stripped, strict=strict, on_skip=on_skip)
            if parsed is not None:
                yield parsed
            elif on_skip is not None:
                on_skip(stripped)


# --- L4 snapshots ---------------------------------------------------------

_ORDER_REQUIRED = ("oid", "side")  # plus a price and a size alias, checked below


def _parse_order(obj: dict) -> L4Order:
    """Normalize one snapshot/order_statuses order (snake_case or camelCase)."""
    if not isinstance(obj, dict):
        raise UnrecognizedDiffFormat("order is not an object", [type(obj).__name__])
    limit_px = pick_key(obj, "limit_px", "limitPx", "px")
    sz = pick_key(obj, "sz", "size")
    missing = [k for k in _ORDER_REQUIRED if k not in obj]
    if limit_px is None:
        missing.append("limit_px")
    if sz is None:
        missing.append("sz")
    if missing:
        raise UnrecognizedDiffFormat(
            "order missing required keys", [f"missing:{k}" for k in missing]
        )
    try:
        side = normalize_side(obj["side"])
        oid = int(obj["oid"])
    except (ValueError, TypeError) as exc:
        raise UnrecognizedDiffFormat(
            "unparseable order side/oid", [repr(obj["side"]), repr(obj["oid"])]
        ) from exc

    timestamp = pick_key(obj, "timestamp", "time")
    trigger_px = pick_key(obj, "trigger_px", "triggerPx")
    user = obj.get("user")
    return L4Order(
        oid=oid,
        user=str(user) if user is not None else None,
        side=side,
        limit_px=str(limit_px),
        sz=str(sz),
        coin=obj.get("coin"),
        timestamp=int(timestamp) if timestamp is not None else None,
        tif=pick_key(obj, "tif"),
        order_type=pick_key(obj, "order_type", "orderType"),
        reduce_only=bool(pick_key(obj, "reduce_only", "reduceOnly", default=False)),
        is_trigger=bool(pick_key(obj, "is_trigger", "isTrigger", default=False)),
        trigger_condition=pick_key(obj, "trigger_condition", "triggerCondition"),
        trigger_px=str(trigger_px) if trigger_px is not None else None,
        is_position_tpsl=bool(
            pick_key(obj, "is_position_tpsl", "isPositionTpsl", default=False)
        ),
    )


def parse_l4_snapshot(obj: Any) -> L4Snapshot:
    """Parse an L4 snapshot object into what :meth:`L4Book.load_snapshot` takes.

    Snapshots are setup-time input, so structural drift always raises
    :class:`UnrecognizedDiffFormat` (there is no tolerant mode to silently
    build a wrong book from).
    """
    if not isinstance(obj, dict):
        raise UnrecognizedDiffFormat("snapshot is not an object", [type(obj).__name__])
    missing = [k for k in ("coin", "bids", "asks") if k not in obj]
    if missing:
        raise UnrecognizedDiffFormat(
            "snapshot missing required keys", [f"missing:{k}" for k in missing]
        )
    # VERIFY-ON-REAL-DATA: "time" (ms) and "height" spellings per
    # order_book_server / QuickNode docs.
    time_ms = pick_key(obj, "time", "time_ms", default=0)
    height = pick_key(obj, "height", "block_height")
    return L4Snapshot(
        coin=str(obj["coin"]),
        time_ms=int(time_ms),
        height=int(height) if height is not None else None,
        bids=[_parse_order(o) for o in obj["bids"]],
        asks=[_parse_order(o) for o in obj["asks"]],
    )


def load_l4_snapshot_file(path: str | Path) -> L4Snapshot:
    """Read and parse an L4 snapshot from a ``.json`` (or ``.json.lz4``) file."""
    with _open_maybe_lz4(Path(path)) as fh:
        return parse_l4_snapshot(json.load(fh))
