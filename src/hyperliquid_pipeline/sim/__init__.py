"""Event-driven maker simulator (design: docs/maker-backtester-design.md).

Deliberately separate from the bar-level ``backtest/`` — this package
answers microstructure questions (queue position, adverse selection,
latency) that bar data cannot.

Module split per the locked v1.1 design: ``queue``/``fills`` are the
microstructure core (virtual orders overlaid on replayed books, fill
allocation); ``events``/``engine``/``policy``/``report`` are the replay
loop, latency model, quoting policies, and accounting. The contract types
between the halves live in ``sim.types`` (LOCKED — joint change only).
"""

from .types import BookEvent, Fill, QueueBound, TradeEvent

__all__ = ["BookEvent", "Fill", "QueueBound", "TradeEvent"]
