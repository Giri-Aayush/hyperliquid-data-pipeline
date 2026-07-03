"""Tests for orchestrator shutdown and the --historical wiring.

stop() must cancel the realtime task that start() awaits — otherwise the
process hangs after "graceful shutdown" begins — and must be safe to call
before start() or twice.
"""

import asyncio
import signal

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from hyperliquid_pipeline.config import settings
from hyperliquid_pipeline.scheduler.orchestrator import DataPipelineOrchestrator


def _make_orchestrator():
    """Build an orchestrator, restoring the process signal handlers it replaces."""
    prev_int = signal.getsignal(signal.SIGINT)
    prev_term = signal.getsignal(signal.SIGTERM)
    orch = DataPipelineOrchestrator()
    signal.signal(signal.SIGINT, prev_int)
    signal.signal(signal.SIGTERM, prev_term)
    return orch


def test_stop_before_start_is_a_noop():
    async def run():
        orch = _make_orchestrator()
        await asyncio.wait_for(orch.stop(), timeout=1.0)  # nothing initialized -> no-op

    asyncio.run(run())


def test_stop_cancels_realtime_task():
    async def run():
        orch = _make_orchestrator()
        # Stand in for start_with_reconnect(): a task that never finishes on its own.
        orch._realtime_task = asyncio.create_task(asyncio.sleep(999))
        task = orch._realtime_task
        await asyncio.wait_for(orch.stop(), timeout=1.0)  # must not hang
        assert task.cancelled()
        assert orch._realtime_task is None
        # Second stop is a no-op, not an error.
        await asyncio.wait_for(orch.stop(), timeout=1.0)

    asyncio.run(run())


def test_signal_handler_keeps_shutdown_task_referenced():
    async def run():
        orch = _make_orchestrator()
        orch._signal_handler(signal.SIGTERM, None)
        assert orch._shutdown_task is not None  # referenced -> can't be GC'd mid-run
        await asyncio.wait_for(orch._shutdown_task, timeout=1.0)

    asyncio.run(run())


def test_historical_disabled_skips_daily_job():
    original = settings.historical_enabled
    try:
        settings.historical_enabled = False
        orch = _make_orchestrator()
        orch.scheduler = AsyncIOScheduler()
        orch._setup_scheduled_jobs()
        assert orch.scheduler.get_job('daily_historical_collection') is None
        # The rest of the control plane is untouched, including gap backfill.
        assert orch.scheduler.get_job('gap_backfill_retry') is not None
        assert orch.scheduler.get_job('system_health_check') is not None
    finally:
        settings.historical_enabled = original


def test_historical_enabled_registers_daily_job():
    original = settings.historical_enabled
    try:
        settings.historical_enabled = True
        orch = _make_orchestrator()
        orch.scheduler = AsyncIOScheduler()
        orch._setup_scheduled_jobs()
        assert orch.scheduler.get_job('daily_historical_collection') is not None
    finally:
        settings.historical_enabled = original
