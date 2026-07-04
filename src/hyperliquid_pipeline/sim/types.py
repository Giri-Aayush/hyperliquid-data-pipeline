"""Frozen contract types for the event-driven maker simulator.

LOCKED 2026-07-04 (docs/maker-backtester-design.md, review amendments §4).
Both halves of sim/ import from here; changing anything below is a joint
design decision, not an edit.

Engine guarantees the queue/fill core relies on (pinned here because they
are part of the contract, not implementation detail):

* All of block N's TradeEvents are delivered BEFORE its BookEvent — block N's
  book diffs already embed the removals caused by block N's trades, so any
  other order double-counts queue depletion.
* BookEvents fire for EVERY block, including quiet ones.
* place()/cancel() are only invoked between blocks, never mid-block, and the
  engine owns ALL latency: by the time QueueSim sees an action, it has
  "arrived" — QueueSim is latency-free.
* On a stale book (height gap) the engine cancels every virtual order; the
  queue core never sees actions against a stale view.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:  # protocol only; no runtime dependency on the book package
    from ..book.schemas import BlockDiffBatch, BookView


class QueueBound(str, Enum):
    """Where unobservable cancels land relative to our virtual order.

    PESSIMISTIC — every cancel lands behind us: queue-ahead decays through
        trades only. A genuine floor (same-price joins cannot jump ahead).
        The decision rule: a policy must clear THIS bound.
    PRORATA — cancels distributed proportionally (queue_ahead / level size).
        The central estimate, not a bound.
    OPTIMISTIC — every cancel lands ahead of us. A genuine ceiling; kill-fast
        diagnostic only. A policy that only works here does not work.
    EXACT — real L4 data: queue-ahead is ground truth from order-level diffs.
    """

    PESSIMISTIC = "pessimistic"
    PRORATA = "prorata"
    OPTIMISTIC = "optimistic"
    EXACT = "exact"


@dataclass
class BookEvent:
    """One block's book state, delivered after that block's trades.

    ``view`` is live shared state — consume in delivery order, do not retain
    across events. ``batch`` carries the block's order-level diffs in L4/EXACT
    mode and is None in L2 mode.
    """

    coin: str
    t_ms: int
    height: Optional[int]
    view: "BookView"
    batch: Optional["BlockDiffBatch"] = None
    recv_ts_ms: Optional[float] = None


@dataclass
class TradeEvent:
    """One print. ``side`` is the AGGRESSOR side, per the Hyperliquid WS
    convention: 'B' = taker buy, which consumes ASK queues; 'A' = taker sell,
    which consumes BID queues. Inverting this is the classic fills bug —
    QueueSim matches a trade against resting orders on the OPPOSITE side.
    """

    coin: str
    t_ms: int
    px: str
    sz: float
    side: str  # aggressor: 'B' consumes asks, 'A' consumes bids
    tid: Optional[int] = None
    recv_ts_ms: Optional[float] = None


@dataclass
class Fill:
    """One (partial) fill of a virtual resting order.

    ``side`` is OUR resting side (not the aggressor). ``queue_ahead_at_fill``
    is the modeled queue still ahead when the print reached us — the
    fill-quality distribution is the whole point of queue modeling. The mid
    Δt after the fill (adverse selection) is deliberately NOT here: the report
    computes it from the stream so Fill stays causal.
    """

    order_id: int
    coin: str
    side: str  # our resting side: 'B' bid / 'A' ask
    px: str
    sz: float
    t_ms: int
    height: Optional[int]
    queue_bound: str
    queue_ahead_at_fill: float
    mid_at_fill: Optional[float]
    maker: bool = True
