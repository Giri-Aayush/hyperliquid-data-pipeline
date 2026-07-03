#!/usr/bin/env python3
"""Benchmark feed latency against a Hyperliquid websocket endpoint.

Run from anywhere; re-run the identical command from a colocated host later
(only HYPERLIQUID_WS_URL changes) to compare reports:

    python scripts/bench_ws_latency.py --symbols BTC,ETH --duration 60 \\
        --output latency_report.json
"""

import asyncio
import json
import sys
from pathlib import Path

import typer
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hyperliquid_pipeline.bench.ws_latency import LatencyBench, to_table  # noqa: E402


def main(
    symbols: str = typer.Option("BTC,ETH", "--symbols", "-s", help="Comma-separated symbols"),
    duration: float = typer.Option(60.0, "--duration", "-d", help="Window in seconds"),
    url: str = typer.Option(None, "--url", help="WebSocket endpoint (default: configured)"),
    channels: str = typer.Option(
        "bbo,l2Book,trades", "--channels", help="Comma-separated channels to measure"
    ),
    ntp_server: str = typer.Option(None, "--ntp-server", help="NTP server for clock-offset probe"),
    output: Path = typer.Option(None, "--output", "-o", help="Write the JSON report here"),
):
    """Measure exchange-timestamp -> local-receive latency per channel."""
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    bench = LatencyBench(
        ws_url=url,
        symbols=[s.strip() for s in symbols.split(",") if s.strip()],
        channels=tuple(c.strip() for c in channels.split(",") if c.strip()),
        duration_s=duration,
        ntp_server=ntp_server,
    )
    report = asyncio.run(bench.run())

    print(to_table(report))
    if output:
        output.write_text(json.dumps(report, indent=2))
        print(f"\nJSON report written to {output}")


if __name__ == "__main__":
    typer.run(main)
