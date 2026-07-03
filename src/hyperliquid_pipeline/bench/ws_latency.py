"""Feed-latency benchmark against a Hyperliquid websocket endpoint.

Measures exchange-event-timestamp -> local-receive-time deltas per channel
over a fixed window, with exact percentiles (a bench can afford to keep raw
samples, unlike the capture hot path which uses bucketed histograms).

The same command re-runs unchanged from a colocated Tokyo host later — only
HYPERLIQUID_WS_URL changes — so reports are directly comparable across hosts.

Caveat baked into every report: the delta includes local clock offset. A
single SNTP probe estimates that offset so reports show both raw and
offset-adjusted numbers; treat sub-10ms differences as noise unless the host
runs disciplined NTP/PTP.
"""

import asyncio
import json
import math
import socket
import struct
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import websockets
from loguru import logger

from ..config import settings

DEFAULT_CHANNELS = ("bbo", "l2Book", "trades")

_NTP_DELTA = 2_208_988_800  # seconds between the 1900 (NTP) and 1970 (Unix) epochs


def ntp_offset(server: Optional[str] = None, timeout: float = 3.0) -> Optional[float]:
    """Estimate the local clock offset in ms with one SNTP query (stdlib only).

    Positive offset = local clock is behind the NTP server. Returns None on
    any failure (no network, blocked UDP/123, bad reply) — the report then
    shows raw deltas with an explicit "offset unknown" caveat.
    """
    server = server or settings.bench_ntp_server
    packet = b"\x1b" + 47 * b"\x00"  # LI=0, VN=3, Mode=3 (client)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(timeout)
            t0 = time.time()
            sock.sendto(packet, (server, 123))
            data, _ = sock.recvfrom(512)
            t3 = time.time()
        if len(data) < 48:
            return None
        words = struct.unpack("!12I", data[:48])

        def _ts(word_index: int) -> float:
            return (words[word_index] - _NTP_DELTA) + words[word_index + 1] / 2**32

        t1 = _ts(8)   # server receive time
        t2 = _ts(10)  # server transmit time
        offset_seconds = ((t1 - t0) + (t2 - t3)) / 2
        return offset_seconds * 1000.0
    except Exception:
        return None


def exact_percentile(sorted_samples: Sequence[float], q: float) -> Optional[float]:
    """Nearest-rank percentile over pre-sorted samples."""
    if not sorted_samples:
        return None
    rank = max(1, math.ceil(q * len(sorted_samples)))
    return sorted_samples[min(rank, len(sorted_samples)) - 1]


def summarize_deltas(
    deltas_ms: Sequence[float], ntp_offset_ms: Optional[float] = None
) -> Dict[str, Any]:
    """Exact stats for one channel's latency samples.

    'raw' is what was measured. 'adjusted' corrects for the estimated clock
    offset (positive offset = local clock behind the server, so measured
    deltas understate true latency and the offset is ADDED); present only
    when an offset estimate exists.
    """
    if not deltas_ms:
        return {"count": 0}
    ordered = sorted(deltas_ms)
    raw = {
        "min_ms": ordered[0],
        "p1_ms": exact_percentile(ordered, 0.01),
        "p50_ms": exact_percentile(ordered, 0.50),
        "p90_ms": exact_percentile(ordered, 0.90),
        "p95_ms": exact_percentile(ordered, 0.95),
        "p99_ms": exact_percentile(ordered, 0.99),
        "max_ms": ordered[-1],
        "mean_ms": sum(ordered) / len(ordered),
    }
    summary: Dict[str, Any] = {
        "count": len(ordered),
        "negative_count": sum(1 for d in ordered if d < 0),
        "raw": {k: round(v, 3) for k, v in raw.items()},
    }
    if ntp_offset_ms is not None:
        # local clock behind by `offset` -> true latency = measured + offset
        summary["adjusted"] = {k: round(v + ntp_offset_ms, 3) for k, v in raw.items()}
    return summary


class LatencyBench:
    """Standalone feed-latency measurement over a fixed window.

    Deliberately reuses nothing stateful from the collector: a minimal socket
    loop that stamps time.time() immediately after each frame arrives, BEFORE
    json.loads — the same stamping convention the capture path uses, so bench
    numbers and live capture stats are comparable.
    """

    def __init__(
        self,
        ws_url: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        channels: Sequence[str] = DEFAULT_CHANNELS,
        duration_s: float = 60.0,
        ntp_server: Optional[str] = None,
    ):
        self.ws_url = ws_url or settings.hyperliquid_ws_url
        self.symbols = symbols or ["BTC", "ETH"]
        self.channels = tuple(channels)
        self.duration_s = duration_s
        self.ntp_server = ntp_server or settings.bench_ntp_server
        self.logger = logger.bind(component="ws_latency_bench")

    def _subscriptions(self) -> List[Dict[str, Any]]:
        subs = []
        for channel in self.channels:
            for symbol in self.symbols:
                subs.append({
                    "method": "subscribe",
                    "subscription": {"type": channel, "coin": symbol},
                })
        return subs

    @staticmethod
    def _extract_exchange_ms(channel: str, message: Dict[str, Any]) -> List[float]:
        """Exchange event timestamps (ms) carried by one message."""
        data = message.get("data")
        if channel in ("bbo", "l2Book"):
            if isinstance(data, dict) and data.get("time"):
                return [float(data["time"])]
        elif channel == "trades":
            if isinstance(data, list):
                return [float(t["time"]) for t in data if t.get("time")]
        return []

    async def run(self) -> Dict[str, Any]:
        """Collect deltas for duration_s and return the report dict."""
        offset_ms = await asyncio.get_event_loop().run_in_executor(
            None, ntp_offset, self.ntp_server
        )
        if offset_ms is None:
            self.logger.warning(f"NTP probe against {self.ntp_server} failed; raw numbers only")

        deltas: Dict[str, List[float]] = {channel: [] for channel in self.channels}
        primed_trades: set = set()  # first trades msg per coin = old-trades snapshot
        started_utc = datetime.now(timezone.utc).isoformat()

        self.logger.info(f"Connecting to {self.ws_url} for {self.duration_s:.0f}s")
        async with websockets.connect(
            self.ws_url, ping_interval=30, ping_timeout=10, close_timeout=10, max_size=10**7
        ) as ws:
            for sub in self._subscriptions():
                await ws.send(json.dumps(sub))
            loop = asyncio.get_event_loop()
            deadline = loop.time() + self.duration_s
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    frame = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                recv_ms = time.time() * 1000  # stamp BEFORE parsing
                try:
                    message = json.loads(frame)
                except json.JSONDecodeError:
                    continue
                channel = message.get("channel", "")
                if channel in deltas:
                    if channel == "trades":
                        # Skip the per-coin subscription snapshot: it carries
                        # OLD trades whose timestamps would read as seconds of
                        # phantom latency (same convention as the collector).
                        coins = {
                            t.get("coin") for t in message.get("data", [])
                            if isinstance(t, dict)
                        }
                        if not coins <= primed_trades:
                            primed_trades |= coins
                            continue
                    for exchange_ms in self._extract_exchange_ms(channel, message):
                        deltas[channel].append(recv_ms - exchange_ms)

        return self._build_report(deltas, offset_ms, started_utc)

    def _build_report(
        self,
        deltas: Dict[str, List[float]],
        offset_ms: Optional[float],
        started_utc: str,
    ) -> Dict[str, Any]:
        return {
            "host": socket.gethostname(),
            "ws_url": self.ws_url,
            "symbols": self.symbols,
            "duration_s": self.duration_s,
            "started_utc": started_utc,
            "ntp_server": self.ntp_server,
            "ntp_offset_ms": round(offset_ms, 3) if offset_ms is not None else None,
            "channels": {
                channel: summarize_deltas(samples, offset_ms)
                for channel, samples in deltas.items()
            },
            "caveat": (
                "delta = exchange block timestamp -> local wall clock at socket read; "
                "includes local clock offset. "
                + (
                    f"NTP-estimated offset {offset_ms:+.1f}ms is applied in 'adjusted'."
                    if offset_ms is not None
                    else "NTP offset UNKNOWN (probe failed): raw numbers only."
                )
            ),
        }


def to_table(report: Dict[str, Any]) -> str:
    """Human-readable summary of a bench report."""
    lines = [
        f"Feed latency vs {report['ws_url']}",
        f"host={report['host']}  window={report['duration_s']:.0f}s  "
        f"symbols={','.join(report['symbols'])}  "
        f"ntp_offset={report['ntp_offset_ms']}ms",
        "",
        f"{'channel':<10}{'count':>7}{'neg':>5}{'p1':>9}{'p50':>9}{'p90':>9}{'p95':>9}{'p99':>9}{'max':>10}",
        "-" * 77,
    ]
    for channel, stats in report["channels"].items():
        if stats.get("count", 0) == 0:
            lines.append(f"{channel:<10}{0:>7}   (no samples)")
            continue
        block = stats.get("adjusted") or stats["raw"]
        lines.append(
            f"{channel:<10}{stats['count']:>7}{stats['negative_count']:>5}"
            f"{block['p1_ms']:>9.1f}{block['p50_ms']:>9.1f}{block['p90_ms']:>9.1f}"
            f"{block['p95_ms']:>9.1f}{block['p99_ms']:>9.1f}{block['max_ms']:>10.1f}"
        )
    lines += ["", f"caveat: {report['caveat']}"]
    return "\n".join(lines)
