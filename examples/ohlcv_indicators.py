"""Run the indicator engine over a price series and print the result.

Deterministic (fixed seed), no network. Generates a synthetic close series,
feeds it through the same TechnicalIndicatorProcessor the live pipeline uses,
and prints RSI, EMAs, and Bollinger bands once enough history exists.

    python examples/ohlcv_indicators.py
"""

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hyperliquid_pipeline.processors.data_processor import (
    TechnicalIndicatorProcessor,
)


def synthetic_closes(n: int = 80, start: float = 50_000.0) -> list[float]:
    rng = np.random.default_rng(7)
    steps = rng.normal(0, 1, n) * 120  # ~0.24% per-bar moves on a 50k price
    return list(start + np.cumsum(steps))


def main() -> None:
    proc = TechnicalIndicatorProcessor()
    closes = synthetic_closes()

    header = f"{'bar':>4}{'close':>12}{'rsi':>8}{'ema_10':>12}{'bb_upper':>12}{'bb_lower':>12}"
    print(header)
    print("-" * len(header))

    for i, close in enumerate(closes):
        proc.update_price_data("BTC", {"close": close, "volume": 1.0})
        ind = proc.calculate_indicators("BTC")
        if not ind:  # needs 20 bars before Bollinger/RSI are defined
            continue
        if i % 5 != 0:  # print every 5th bar to keep it readable
            continue
        print(
            f"{i:>4}{close:>12,.2f}{ind.get('rsi', float('nan')):>8.1f}"
            f"{ind.get('ema_10', float('nan')):>12,.2f}"
            f"{ind.get('bb_upper', float('nan')):>12,.2f}"
            f"{ind.get('bb_lower', float('nan')):>12,.2f}"
        )


if __name__ == "__main__":
    main()
