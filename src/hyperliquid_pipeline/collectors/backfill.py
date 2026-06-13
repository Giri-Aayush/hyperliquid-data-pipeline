"""Backfill data missed during a reconnect gap, from the historical archive.

When the live WebSocket drops and reconnects, the trade feed has a hole (the
orderbook self-heals from snapshots, but trades in the gap are gone). This pulls
that window from Hyperliquid's archive and replays it through the same processing
path as live data.

Best-effort by nature: the archive publishes with a lag, so a just-missed window
often isn't available yet — the caller retries later until it appears or ages out.
"""

import asyncio
import inspect
from datetime import timedelta, timezone
from typing import Any, Awaitable, Callable, List, Union

import pandas as pd
from loguru import logger

from .realtime_collector import GapEvent, MarketDataPoint

OnPoint = Callable[[MarketDataPoint], Union[None, Awaitable[None]]]

# Yield to the event loop every N rows so a large backfill doesn't starve the
# live consumer while it iterates the archive DataFrame.
_YIELD_EVERY = 2000


def _gap_hours(start, end) -> List[int]:
    """The UTC hours the gap spans (usually 1, at most a handful)."""
    hours = set()
    t = start.replace(minute=0, second=0, microsecond=0)
    while t <= end:
        hours.add(t.hour)
        t += timedelta(hours=1)
    return sorted(hours)


def _to_utc(ts: Any):
    """Coerce a (possibly tz-naive) DataFrame index value to an aware UTC datetime."""
    try:
        t = pd.Timestamp(ts).to_pydatetime()
    except Exception:
        return None
    if t.tzinfo is None:  # the archive stores naive ms timestamps; treat as UTC
        return t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc)


async def backfill_gap(historical: Any, gap: GapEvent, on_point: OnPoint) -> int:
    """Fetch trades in the gap window from the archive and replay them.

    Args:
        historical: a HistoricalDataCollector (anything with an async
            ``download_historical_data(symbols, start_date, end_date, data_types)``).
        gap: the missed window.
        on_point: called with each recovered MarketDataPoint (sync or async).

    Returns:
        The number of points recovered (0 if the archive doesn't have the window
        yet, which is normal for a fresh gap).
    """
    log = logger.bind(component="backfill")
    start, end = gap.start, gap.end
    start_date = start.strftime("%Y-%m-%d")
    end_date = end.strftime("%Y-%m-%d")

    try:
        data = await historical.download_historical_data(
            symbols=gap.symbols,
            start_date=start_date,
            end_date=end_date,
            data_types=["trades"],
            hours=_gap_hours(start, end),  # only the gap's hours, not all 24
        )
    except Exception as e:
        log.error(f"Backfill download failed for {start_date}..{end_date}: {e}")
        return 0

    recovered = 0
    seen = 0
    for symbol, types in (data or {}).items():
        df = types.get("trades") if types else None
        if df is None or getattr(df, "empty", True):
            continue
        for idx, row in df.iterrows():
            seen += 1
            if seen % _YIELD_EVERY == 0:
                await asyncio.sleep(0)  # let the live consumer run
            ts = _to_utc(idx)
            # Exclusive lower bound: a trade exactly at gap.start is the last
            # live message we already stored — don't replay it as a duplicate.
            if ts is None or not (start < ts <= end):
                continue
            price = row.get("price")
            size = row.get("size")
            # Skip rows with missing/NaN price or size (a None in a numeric
            # column reads back as NaN, which float() would happily accept).
            if pd.isna(price) or pd.isna(size):
                continue
            try:
                point = MarketDataPoint(
                    timestamp=ts,
                    symbol=symbol,
                    data_type="trade",
                    data={
                        "price": float(price),
                        "size": float(size),
                        "side": row.get("side", ""),
                        "backfilled": True,
                    },
                )
            except (KeyError, TypeError, ValueError):
                continue  # skip a malformed row rather than abort the backfill
            result = on_point(point)
            if inspect.isawaitable(result):
                await result
            recovered += 1

    if recovered:
        log.info(
            f"Backfilled {recovered} trades for gap "
            f"{start.isoformat()} -> {end.isoformat()}"
        )
    else:
        log.warning(
            f"No archived data yet for gap {start.isoformat()} -> {end.isoformat()} "
            "(archive may lag); will retry"
        )
    return recovered
