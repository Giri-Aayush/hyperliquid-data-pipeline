#!/usr/bin/env python3
"""Research capture: persist live market data to per-stream JSONL files.

Unlike `run_pipeline.py test-realtime` (which only logs receipt), this wires
the DataLogger so every point — bbo, orderbook, trades, asset ctx — lands in
JSONL with dual timestamps, ready for signal research. Set SPOOL_ENABLED=true
to also keep the lossless raw-frame WAL.

    python scripts/capture.py --symbols BTC,ETH --duration 3600 \\
        --output data/research_capture
"""

import asyncio
import sys
from pathlib import Path

import typer
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hyperliquid_pipeline.collectors.realtime_collector import (  # noqa: E402
    DataLogger,
    HyperliquidWebSocketCollector,
)


def main(
    symbols: str = typer.Option("BTC,ETH", "--symbols", "-s", help="Comma-separated symbols"),
    duration: float = typer.Option(3600.0, "--duration", "-d", help="Capture window in seconds"),
    output: Path = typer.Option(
        Path("data/research_capture"), "--output", "-o", help="JSONL output directory"
    ),
    stats_every: float = typer.Option(60.0, "--stats-every", help="Stats log cadence (seconds)"),
):
    """Capture live feeds to JSONL for research."""
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="{time:HH:mm:ss} | {level: <7} | {message}")

    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]

    async def run():
        collector = HyperliquidWebSocketCollector(symbol_list)
        data_logger = DataLogger(output_dir=str(output))
        collector.add_data_callback(data_logger.log_data_point)

        async def log_stats():
            while True:
                await asyncio.sleep(stats_every)
                stats = collector.get_stats()
                latency = stats.get("latency_ms") or {}
                bbo = latency.get("bbo") or {}
                logger.info(
                    f"captured={stats['message_count']} dropped={stats['dropped_count']} "
                    f"bbo_p50={bbo.get('p50_ms')}ms "
                    f"spool={stats.get('spool')}"
                )

        stats_task = asyncio.create_task(log_stats())
        logger.info(f"Capturing {symbol_list} for {duration:.0f}s -> {output}")
        try:
            await asyncio.wait_for(collector.start_with_reconnect(), timeout=duration)
        except asyncio.TimeoutError:
            logger.info("Capture window complete")
        finally:
            stats_task.cancel()
            data_logger.close_all_files()
            final = collector.get_stats()
            logger.info(f"Final: messages={final['message_count']} "
                        f"dropped={final['dropped_count']} buffers={final['buffer_sizes']}")

    asyncio.run(run())


if __name__ == "__main__":
    typer.run(main)
