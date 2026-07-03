"""Normalized in-memory schemas for Hyperliquid order-book data.

This is the format-agnostic layer: frozen dataclasses for diffs, orders and
snapshots, plus the two normalizers every parser shares (side encodings and
snake_case/camelCase key drift). On-disk and wire spellings are strictly
diff_parser.py's business — nothing downstream of the parser ever sees a raw
dict.

Prices and sizes stay ``str`` at this layer: Hyperliquid emits exact decimal
strings and the schemas preserve that fidelity end-to-end. Books convert
lazily — Decimal for arithmetic, float only at the BookView read edge.
"""

from dataclasses import dataclass
from typing import Any, Iterator, Protocol, runtime_checkable

BID = "B"
ASK = "A"

# Both encodings appear in official examples: "Bid"/"Ask" in bare node events,
# "A"/"B" in L4 snapshots and some node events. Matched case-insensitively.
_SIDE_ALIASES = {"b": BID, "bid": BID, "a": ASK, "ask": ASK}


def normalize_side(side: Any) -> str:
    """Map any verified side encoding ("Bid"/"Ask"/"B"/"A") to 'B' | 'A'."""
    if isinstance(side, str):
        normalized = _SIDE_ALIASES.get(side.strip().lower())
        if normalized is not None:
            return normalized
    raise ValueError(f"unrecognized side encoding: {side!r}")


def pick_key(obj: dict, *aliases: str, default: Any = None) -> Any:
    """Return the first value present under any alias.

    Absorbs the snake_case/camelCase drift between sources: L4 snapshots use
    ``limit_px`` while node ``order_statuses`` use ``limitPx``, updates use
    ``origSz``/``newSz``, etc.
    """
    for alias in aliases:
        if alias in obj:
            return obj[alias]
    return default


@dataclass(frozen=True, slots=True)
class BookDiff:
    """One normalized raw-book-diff event (node ``--write-raw-book-diffs``).

    ``kind`` is 'new' | 'update' | 'remove'; exactly the fields for that kind
    are populated (``sz`` for new, ``orig_sz``/``new_sz`` for update).
    """

    user: str | None
    oid: int
    coin: str
    side: str  # 'B' | 'A' (normalized)
    px: str
    kind: str  # 'new' | 'update' | 'remove'
    sz: str | None = None
    orig_sz: str | None = None
    new_sz: str | None = None


@dataclass(frozen=True, slots=True)
class BlockDiffBatch:
    """All book diffs for one block (``--batch-by-block`` envelope line)."""

    time_ms: int
    height: int
    diffs: list[BookDiff]


@dataclass(frozen=True, slots=True)
class L4Order:
    """One resting order as it appears in an L4 snapshot / order_statuses."""

    oid: int
    user: str | None
    side: str  # 'B' | 'A' (normalized)
    limit_px: str
    sz: str
    coin: str | None = None
    timestamp: int | None = None
    tif: str | None = None
    order_type: str | None = None
    reduce_only: bool = False
    is_trigger: bool = False
    trigger_condition: str | None = None
    trigger_px: str | None = None
    is_position_tpsl: bool = False


@dataclass(frozen=True, slots=True)
class L4Snapshot:
    """Parsed L4 book snapshot: ``{coin, time, height, bids, asks}``."""

    coin: str
    time_ms: int
    height: int | None
    bids: list[L4Order]
    asks: list[L4Order]

    def orders(self) -> Iterator[L4Order]:
        """All orders, bids first — list order within a level is FIFO priority."""
        yield from self.bids
        yield from self.asks


@runtime_checkable
class BookView(Protocol):
    """Read protocol shared by L4Book and L2Book.

    FROZEN — Track 1 consumes these exact signatures; the rest of the system
    stays book-implementation-agnostic by depending only on this protocol.
    """

    last_update_ms: int

    def best_bid(self) -> tuple[str, float] | None: ...

    def best_ask(self) -> tuple[str, float] | None: ...

    def mid(self) -> float | None: ...

    def depth(self, n: int) -> dict: ...

    def is_crossed(self) -> bool: ...
