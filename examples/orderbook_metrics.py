"""Live orderbook microstructure for one symbol.

Subscribes to the Hyperliquid l2Book feed and prints rolling spread, depth,
and imbalance off each snapshot. No keys, no databases.

    python examples/orderbook_metrics.py --symbol BTC --duration 30
"""

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from loguru import logger

from hyperliquid_pipeline.collectors.realtime_collector import (
    HyperliquidWebSocketCollector,
)
from hyperliquid_pipeline.processors.data_processor import OrderBookProcessor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--duration", type=int, default=30, help="seconds")
    args = parser.parse_args()

    logger.remove()  # we print our own table; silence the collector's logs

    collector = HyperliquidWebSocketCollector([args.symbol])
    book = OrderBookProcessor()

    header = f"{'time':<10}{'mid':>13}{'spread(bps)':>13}{'imbalance':>12}{'depth@5 bid/ask':>22}"
    print(header)
    print("-" * len(header))

    def on_data(point) -> None:
        if point.data_type != "orderbook":
            return
        book.update_orderbook(point)
        m = book.calculate_metrics(args.symbol)
        if not m:
            return
        print(
            f"{datetime.now():%H:%M:%S}  "
            f"{m['mid_price']:>11,.2f}  "
            f"{m['spread_bps']:>11.2f}  "
            f"{m['imbalance']:>+10.3f}  "
            f"{m['bid_depth_5']:>9.2f} / {m['ask_depth_5']:<8.2f}"
        )

    collector.add_data_callback(on_data)

    async def run() -> None:
        try:
            await asyncio.wait_for(
                collector.start_with_reconnect(), timeout=args.duration
            )
        except asyncio.TimeoutError:
            pass

    asyncio.run(run())


if __name__ == "__main__":
    main()
