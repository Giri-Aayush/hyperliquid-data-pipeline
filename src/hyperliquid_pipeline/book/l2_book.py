"""Top-of-book (L2) snapshot book implementing the same BookView read protocol.

Fed by the live collector's ``l2Book`` payloads (see
``process_l2_book_message`` in ``collectors/realtime_collector.py``): each
message is a full snapshot of the visible levels, bids/asks as lists of
``{"px": str, "sz": str, "n": int}``, so every update *replaces* the whole
book. L2 carries no per-order identity — there is no ``queue_position``
here; consumers that need one use :class:`L4Book`.

Because the read protocol is identical, the rest of the system can hold a
``BookView`` and not care which implementation is behind it.
"""

from decimal import Decimal, InvalidOperation
from typing import Any

from hyperliquid_pipeline.book.schemas import BID

# Same memory-bound policy as L4Book: keep the newest dicts, count them all.
ANOMALY_KEEP = 1000


class _Level:
    __slots__ = ("px", "price", "size", "n")

    def __init__(self, px: str, size: Decimal, n: int):
        self.px = px
        self.price = Decimal(px)  # validated numeric form; px stays exact
        self.size = size
        self.n = n


class L2Book:
    """Aggregated price-level book for a single coin, replaced per snapshot."""

    def __init__(self, coin: str | None = None):
        self.coin = coin
        self.last_update_ms: int = 0
        self.stale: bool = False  # parity with L4Book; L2 snapshots never gap
        self.anomalies: list[dict] = []  # newest ANOMALY_KEEP entries
        self.anomaly_count: int = 0  # true total, immune to the cap
        self._bids: list[_Level] = []  # best first
        self._asks: list[_Level] = []  # best first

    # --- feeding ----------------------------------------------------------

    def update_from_snapshot(
        self, bids: list[dict], asks: list[dict], time_ms: int
    ) -> None:
        """Replace the whole book with one l2Book snapshot.

        ``bids``/``asks`` are the collector's level dicts
        (``{"px": str, "sz": str, "n": int}``). Malformed levels are skipped
        and counted in ``anomalies``, never raised.
        """
        self._bids = self._parse_side(bids, side=BID)
        self._asks = self._parse_side(asks, side="A")
        self.last_update_ms = int(time_ms)

    # --- frozen BookView read protocol (identical to L4Book) ---------------

    def best_bid(self) -> tuple[str, float] | None:
        if not self._bids:
            return None
        level = self._bids[0]
        return (level.px, float(level.size))

    def best_ask(self) -> tuple[str, float] | None:
        if not self._asks:
            return None
        level = self._asks[0]
        return (level.px, float(level.size))

    def mid(self) -> float | None:
        if not self._bids or not self._asks:
            return None
        return float((self._bids[0].price + self._asks[0].price) / 2)

    def depth(self, n: int) -> dict:
        """Top-``n`` levels per side, best first — same shape as L4Book.depth."""
        count = max(n, 0)
        return {
            "bids": [self._level_entry(lvl) for lvl in self._bids[:count]],
            "asks": [self._level_entry(lvl) for lvl in self._asks[:count]],
        }

    def is_crossed(self) -> bool:
        """True when best bid >= best ask (locked or crossed)."""
        if not self._bids or not self._asks:
            return False
        return self._bids[0].price >= self._asks[0].price

    # --- internals ----------------------------------------------------------

    @staticmethod
    def _level_entry(level: _Level) -> dict:
        return {"px": level.px, "sz": float(level.size), "n": level.n}

    def _anomaly(self, kind: str, **fields: Any) -> None:
        self.anomaly_count += 1
        self.anomalies.append({"type": kind, **fields})
        if len(self.anomalies) > ANOMALY_KEEP:
            del self.anomalies[: len(self.anomalies) - ANOMALY_KEEP]

    def _parse_side(self, levels: list[dict] | None, side: str) -> list[_Level]:
        parsed: list[_Level] = []
        for raw in levels or []:
            try:
                parsed.append(
                    _Level(str(raw["px"]), Decimal(str(raw["sz"])), int(raw.get("n", 1)))
                )
            except (KeyError, TypeError, ValueError, InvalidOperation):
                self._anomaly("bad_level", detail=repr(raw))
        # The feed already orders levels best-first; re-sort defensively so a
        # misordered payload can't silently corrupt best/mid reads.
        parsed.sort(key=lambda lvl: lvl.price, reverse=(side == BID))
        return parsed
