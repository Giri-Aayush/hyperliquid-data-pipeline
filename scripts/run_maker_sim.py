#!/usr/bin/env python3
"""Run the maker simulator grid over a research capture.

For each coin: 3 queue bounds x N latency scenarios, independent passes
(fills change inventory, inventory changes the policy — grid cells must not
share state). Prints one decomposition table per coin; trust only what
clears PESSIMISTIC.

    python scripts/run_maker_sim.py --capture data/research_capture \\
        --coins BTC,ETH,SOL --latencies 400,200
"""

import json
import sys
from pathlib import Path

import typer
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hyperliquid_pipeline.sim.engine import Engine, EngineConfig  # noqa: E402
from hyperliquid_pipeline.sim.events import iter_capture_events  # noqa: E402
from hyperliquid_pipeline.sim.policy import ReferenceOfiPolicy  # noqa: E402
from hyperliquid_pipeline.sim.queue import QueueSim  # noqa: E402
from hyperliquid_pipeline.sim.report import decompose, render  # noqa: E402
from hyperliquid_pipeline.sim.types import QueueBound  # noqa: E402

# Per-coin quote size, chosen for roughly comparable notional (~$1k).
DEFAULT_SIZES = {"BTC": 0.01, "ETH": 0.3, "SOL": 7.0}
BOUNDS = (QueueBound.PESSIMISTIC, QueueBound.PRORATA, QueueBound.OPTIMISTIC)


def main(
    capture: Path = typer.Option(Path("data/research_capture"), "--capture"),
    coins: str = typer.Option("BTC,ETH,SOL", "--coins"),
    latencies: str = typer.Option("400,200", "--latencies", help="submit delays, ms"),
    maker_fee_bps: float = typer.Option(1.5, "--maker-fee-bps"),
    funding_hourly: float = typer.Option(1.25e-5, "--funding-hourly",
                                         help="measured hourly rate (longs pay)"),
    output: Path = typer.Option(None, "--output", help="JSON report path"),
):
    """Grid-run the reference OFI maker over captured data."""
    logger.remove()
    logger.add(sys.stderr, level="WARNING")

    all_reports = {}
    for coin in [c.strip() for c in coins.split(",") if c.strip()]:
        decomps = []
        for delay_ms in [float(d) for d in latencies.split(",")]:
            for bound in BOUNDS:
                events = iter_capture_events(capture, coin)  # fresh stream per pass
                engine = Engine(
                    QueueSim(coin, bound),
                    ReferenceOfiPolicy(quote_size=DEFAULT_SIZES.get(coin, 1.0)),
                    EngineConfig(
                        submit_delay_ms=delay_ms,
                        maker_fee_bps=maker_fee_bps,
                        funding_rate_hourly=funding_hourly,
                    ),
                )
                decomps.append(decompose(engine.run(events)))
        print(render(decomps))
        print()
        all_reports[coin] = [d.__dict__ for d in decomps]

    if output:
        output.write_text(json.dumps(all_reports, indent=2, default=str))
        print(f"JSON written to {output}")


if __name__ == "__main__":
    typer.run(main)
