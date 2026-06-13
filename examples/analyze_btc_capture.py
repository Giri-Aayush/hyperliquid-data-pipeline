"""Capture a live window of BTC perps data and chart its microstructure.

Connects to Hyperliquid, records the orderbook, trades, and asset context for a
fixed window, then computes and plots mid price, spread (bps), orderbook
imbalance, and the mark-vs-oracle basis (bps). Writes a PNG and prints a summary.

    python examples/analyze_btc_capture.py --duration 300 --out docs/btc-microstructure.png

Needs matplotlib (pip install matplotlib).
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import matplotlib
matplotlib.use("Agg")  # headless: render to file, no display
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from loguru import logger

from hyperliquid_pipeline.collectors.realtime_collector import HyperliquidWebSocketCollector
from hyperliquid_pipeline.processors.data_processor import OrderBookProcessor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--duration", type=int, default=300, help="capture seconds")
    parser.add_argument("--out", default="docs/btc-microstructure.png")
    parser.add_argument("--summary", default="docs/btc-microstructure.json")
    args = parser.parse_args()

    logger.remove()
    collector = HyperliquidWebSocketCollector([args.symbol])
    book = OrderBookProcessor()

    book_rows, ctx_rows, trade_rows = [], [], []

    def on_data(point) -> None:
        if point.data_type == "orderbook":
            book.update_orderbook(point)
            m = book.calculate_metrics(args.symbol)
            if m:
                book_rows.append({"time": point.timestamp, "mid": m["mid_price"],
                                  "spread_bps": m["spread_bps"], "imbalance": m["imbalance"]})
        elif point.data_type == "asset_ctx":
            d = point.data
            ctx_rows.append({"time": point.timestamp, "mark": d["mark_price"],
                             "oracle": d["oracle_price"], "basis_bps": d["basis_bps"],
                             "open_interest": d["open_interest"], "funding": d["funding"]})
        elif point.data_type == "trade":
            trade_rows.append({"time": point.timestamp, "price": point.data["price"],
                               "size": point.data["size"]})

    collector.add_data_callback(on_data)

    async def run() -> None:
        try:
            await asyncio.wait_for(collector.start_with_reconnect(), timeout=args.duration)
        except asyncio.TimeoutError:
            pass

    print(f"Capturing {args.symbol} for {args.duration}s ...")
    asyncio.run(run())

    book_df = pd.DataFrame(book_rows).set_index("time") if book_rows else pd.DataFrame()
    ctx_df = pd.DataFrame(ctx_rows).set_index("time") if ctx_rows else pd.DataFrame()
    if book_df.empty:
        print("No orderbook data captured; aborting.")
        return

    # Resample to a regular 1s grid for clean plotting.
    book_1s = book_df.resample("1s").mean().dropna(subset=["mid"])
    ctx_1s = ctx_df.resample("1s").last().dropna(subset=["basis_bps"]) if not ctx_df.empty else pd.DataFrame()

    summary = {
        "symbol": args.symbol,
        "captured_utc": [str(book_df.index.min()), str(book_df.index.max())],
        "duration_s": args.duration,
        "orderbook_snapshots": int(len(book_df)),
        "trades": int(len(trade_rows)),
        "mid_first": float(book_df["mid"].iloc[0]),
        "mid_last": float(book_df["mid"].iloc[-1]),
        "spread_bps_median": float(book_df["spread_bps"].median()),
        "spread_bps_p95": float(book_df["spread_bps"].quantile(0.95)),
        "imbalance_mean": float(book_df["imbalance"].mean()),
        "imbalance_abs_mean": float(book_df["imbalance"].abs().mean()),
    }
    if not ctx_df.empty:
        summary.update({
            "basis_bps_min": float(ctx_df["basis_bps"].min()),
            "basis_bps_max": float(ctx_df["basis_bps"].max()),
            "basis_bps_mean": float(ctx_df["basis_bps"].mean()),
            "open_interest_last": float(ctx_df["open_interest"].iloc[-1]),
        })

    Path(args.summary).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary).write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))

    panels = 3 + (0 if ctx_1s.empty else 1)
    fig, axes = plt.subplots(panels, 1, figsize=(10, 2.2 * panels), sharex=True)
    fig.suptitle(f"{args.symbol} perps microstructure — live capture "
                 f"({book_df.index.min():%Y-%m-%d %H:%M}–{book_df.index.max():%H:%M} UTC)",
                 fontsize=12, y=0.995)

    axes[0].plot(book_1s.index, book_1s["mid"], color="#1f77b4", lw=1.0)
    axes[0].set_ylabel("mid ($)")
    axes[1].plot(book_1s.index, book_1s["spread_bps"], color="#7f7f7f", lw=0.8)
    axes[1].set_ylabel("spread (bps)")
    axes[2].axhline(0, color="#cccccc", lw=0.6)
    axes[2].fill_between(book_1s.index, book_1s["imbalance"], 0, color="#2ca02c", alpha=0.5)
    axes[2].set_ylabel("OB imbalance")
    if not ctx_1s.empty:
        axes[3].axhline(0, color="#cccccc", lw=0.6)
        axes[3].plot(ctx_1s.index, ctx_1s["basis_bps"], color="#d62728", lw=1.0)
        axes[3].set_ylabel("mark−oracle (bps)")

    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    axes[-1].set_xlabel("time (UTC)")
    for ax in axes:
        ax.grid(alpha=0.2)
    fig.tight_layout()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=130, bbox_inches="tight")
    print(f"\nWrote chart -> {args.out}")


if __name__ == "__main__":
    main()
