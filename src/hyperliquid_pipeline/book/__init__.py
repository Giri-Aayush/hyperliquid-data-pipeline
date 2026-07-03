"""Order-book core for the Hyperliquid HFT system.

Market-state layer, book-implementation-agnostic by design:

* ``schemas``     — normalized frozen dataclasses + the BookView read protocol
* ``diff_parser`` — the only module that knows on-disk node formats
* ``l4_book``     — order-level (L4/MBO) reconstruction, FIFO queue positions
* ``l2_book``     — top-of-book snapshot book, same read protocol
* ``replay``      — deterministic replay engine + CLI

Typical wiring: a feed parses lines via ``parse_line``/``iter_diff_file`` and
drives an ``L4Book``; the live collector drives an ``L2Book``; everything
downstream reads through ``BookView`` and doesn't care which one it holds.
"""

from hyperliquid_pipeline.book.diff_parser import (
    UnrecognizedDiffFormat,
    iter_diff_file,
    load_l4_snapshot_file,
    parse_l4_snapshot,
    parse_line,
    parse_obj,
)
from hyperliquid_pipeline.book.l2_book import L2Book
from hyperliquid_pipeline.book.l4_book import L4Book
from hyperliquid_pipeline.book.schemas import (
    ASK,
    BID,
    BlockDiffBatch,
    BookDiff,
    BookView,
    L4Order,
    L4Snapshot,
    normalize_side,
)

def __getattr__(name: str):
    """Lazy re-export of ReplayReport.

    Importing the replay module eagerly here would double-import it under
    ``python -m hyperliquid_pipeline.book.replay`` (runpy RuntimeWarning).
    ``book.replay`` itself always resolves to the submodule; the callable is
    ``book.replay.replay``.
    """
    if name == "ReplayReport":
        import importlib

        return importlib.import_module("hyperliquid_pipeline.book.replay").ReplayReport
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ASK",
    "BID",
    "BlockDiffBatch",
    "BookDiff",
    "BookView",
    "L2Book",
    "L4Book",
    "L4Order",
    "L4Snapshot",
    "ReplayReport",
    "UnrecognizedDiffFormat",
    "iter_diff_file",
    "load_l4_snapshot_file",
    "normalize_side",
    "parse_l4_snapshot",
    "parse_line",
    "parse_obj",
    "replay",
]
