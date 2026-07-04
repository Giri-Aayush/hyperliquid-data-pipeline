"""Replay sources: normalized market-event streams for the simulator.

Turns recorded data into the contract's event stream (sim.types), honoring
the engine's ordering guarantee at the source: for any timestamp, trades are
delivered before the book state that already embeds their effects.

Sources and their fidelity:

* research-capture directory (DataLogger JSONL): 'orderbook' snapshots drive
  an L2Book (full replacement per event) and 'trade' lines become
  TradeEvents — the full fills-capable L2 replay.
* archive l2Book hours: books only (the archive publishes no trades), so a
  replay from archive alone is signal-only — the engine must refuse to
  simulate fills without a trade stream.
* node/QuickNode L4 diff files: BlockDiffBatches drive an L4Book with exact
  block heights (batch carried on each BookEvent per the contract). Trades
  come from a paired capture file when available.
"""

import json
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Union

from ..book import L2Book, L4Book
from ..book.diff_parser import iter_diff_file
from ..book.schemas import BlockDiffBatch
from .types import BookEvent, TradeEvent

MarketEvent = Union[BookEvent, TradeEvent]

# Sort rank on timestamp ties: trades strictly before the book state that
# already reflects them (contract ordering, pinned in sim.types docstring).
_TRADE_RANK = 0
_BOOK_RANK = 1


def _read_jsonl(path: Path) -> Iterator[dict]:
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def _capture_records(paths: Iterable[Path], coin: str) -> List[tuple]:
    """Collect (t_ms, rank, payload) tuples from capture JSONL files."""
    keyed = []
    for path in paths:
        for record in _read_jsonl(path):
            if record.get("symbol") != coin:
                continue
            data = record.get("data") or {}
            t_ms = data.get("timestamp_ms")
            if not t_ms:
                continue
            data_type = record.get("data_type")
            if data_type == "trade":
                keyed.append((int(t_ms), _TRADE_RANK, record))
            elif data_type == "orderbook":
                keyed.append((int(t_ms), _BOOK_RANK, record))
    keyed.sort(key=lambda item: (item[0], item[1]))
    return keyed


def iter_capture_events(
    capture_dir: Union[str, Path], coin: str
) -> Iterator[MarketEvent]:
    """Replay a research-capture directory for one coin.

    Yields TradeEvents and BookEvents in contract order. The BookEvent view
    is one shared L2Book instance, updated in place per snapshot — consume in
    order, do not retain (contract).
    """
    capture_dir = Path(capture_dir)
    paths = sorted(capture_dir.glob(f"{coin}_orderbook_*.jsonl")) + sorted(
        capture_dir.glob(f"{coin}_trade_*.jsonl")
    )
    if not paths:
        raise FileNotFoundError(
            f"no {coin} orderbook/trade JSONL under {capture_dir}"
        )

    book = L2Book(coin=coin)
    for t_ms, rank, record in _capture_records(paths, coin):
        data = record["data"]
        if rank == _TRADE_RANK:
            yield TradeEvent(
                coin=coin,
                t_ms=t_ms,
                px=str(data["price"]),
                sz=float(data["size"]),
                side=str(data.get("side", "")),  # aggressor, per HL ws
                tid=data.get("trade_id") or None,
                recv_ts_ms=record.get("recv_ts_ms"),
            )
        else:
            book.update_from_snapshot(
                data.get("bids", []), data.get("asks", []), t_ms
            )
            yield BookEvent(
                coin=coin,
                t_ms=t_ms,
                height=None,  # L2 capture carries no block heights
                view=book,
                batch=None,
                recv_ts_ms=record.get("recv_ts_ms"),
            )


def iter_archive_events(
    paths: Iterable[Union[str, Path]], coin: str
) -> Iterator[BookEvent]:
    """Replay archive l2Book hours (books only — the archive has no trades).

    Fills cannot be simulated from this source alone; the engine enforces
    that. Lines are the real wrapper format; reuse of the OFI loader was
    considered, but the book here must be a BookView, so we parse minimally.
    """
    book = L2Book(coin=coin)
    import lz4.frame

    for path in paths:
        path = Path(path)
        opener = lz4.frame.open(path, "rt") if path.suffix == ".lz4" else open(path)
        with opener as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                raw = record.get("raw")
                data = (raw or {}).get("data") if isinstance(raw, dict) else record
                if not isinstance(data, dict) or data.get("coin") != coin:
                    continue
                levels = data.get("levels") or [[], []]
                t_ms = int(data.get("time", 0))
                book.update_from_snapshot(
                    levels[0] if len(levels) > 0 else [],
                    levels[1] if len(levels) > 1 else [],
                    t_ms,
                )
                yield BookEvent(
                    coin=coin, t_ms=t_ms, height=None, view=book, batch=None
                )


def iter_l4_events(
    diff_paths: Iterable[Union[str, Path]],
    coin: str,
    snapshot: Optional[object] = None,
) -> Iterator[BookEvent]:
    """Replay node/QuickNode L4 diff files into exact BookEvents.

    Every block yields a BookEvent carrying its BlockDiffBatch (contract:
    EXACT-mode queue state is maintained from book diffs, and quiet blocks
    still fire). Diffs for other coins are filtered out but the block still
    advances this book's height continuity.
    """
    book = L4Book(coin)
    if snapshot is not None:
        book.load_snapshot(snapshot)

    for path in diff_paths:
        for item in iter_diff_file(path):
            if not isinstance(item, BlockDiffBatch):
                continue  # bare events carry no clock; engine replay is per block
            kept = [d for d in item.diffs if d.coin == coin]
            batch = BlockDiffBatch(time_ms=item.time_ms, height=item.height, diffs=kept)
            book.apply_block(batch)
            yield BookEvent(
                coin=coin,
                t_ms=int(item.time_ms),
                height=int(item.height),
                view=book,
                batch=batch,
            )
