"""In-process latency histograms, cheap enough for the socket hot path.

Feed latency = local receive time minus the exchange's event timestamp. On
Hyperliquid the floor is one HyperBFT consensus round, so the interesting
range is roughly 10ms-10s; buckets are log-spaced over it. The layout
(boundaries + per-bucket counts + count/sum) is Prometheus-shaped, so a real
exporter later is a thin adapter over `snapshot()`.
"""

import bisect
from typing import Any, Dict, Optional, Sequence

# Upper bucket boundaries in milliseconds; anything above the last lands in an
# overflow bucket reported via max_ms.
DEFAULT_BUCKETS_MS: Sequence[float] = (
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000
)


class LatencyHistogram:
    """Fixed-bucket histogram; record() is one bisect plus a few increments.

    No per-event allocation: the bucket list is preallocated and bisect runs at
    C speed, so recording on every websocket frame costs effectively nothing
    next to json.loads.
    """

    __slots__ = (
        "boundaries", "counts", "count", "total_ms",
        "min_ms", "max_ms", "negative_count",
    )

    def __init__(self, boundaries: Sequence[float] = DEFAULT_BUCKETS_MS):
        self.boundaries = tuple(boundaries)
        self.counts = [0] * (len(self.boundaries) + 1)  # +1 = overflow bucket
        self.count = 0
        self.total_ms = 0.0
        self.min_ms: Optional[float] = None
        self.max_ms: Optional[float] = None
        # Exchange timestamp ahead of the local clock — clock offset, not
        # negative latency. Counted separately, recorded as 0ms.
        self.negative_count = 0

    def record(self, delta_ms: float):
        """Record one latency observation in milliseconds."""
        if delta_ms < 0:
            self.negative_count += 1
            delta_ms = 0.0
        idx = bisect.bisect_left(self.boundaries, delta_ms)
        self.counts[idx] += 1
        self.count += 1
        self.total_ms += delta_ms
        if self.min_ms is None or delta_ms < self.min_ms:
            self.min_ms = delta_ms
        if self.max_ms is None or delta_ms > self.max_ms:
            self.max_ms = delta_ms

    def percentile(self, q: float) -> Optional[float]:
        """Approximate percentile: the upper boundary of the bucket holding it.

        Bucket-resolution accuracy is plenty for spotting a latency regression;
        the bench harness keeps raw samples when exact percentiles matter.
        """
        if self.count == 0:
            return None
        target = q * self.count
        cumulative = 0
        for i, bucket_count in enumerate(self.counts):
            cumulative += bucket_count
            if cumulative >= target:
                if i < len(self.boundaries):
                    return float(self.boundaries[i])
                break
        return float(self.max_ms)  # landed in the overflow bucket

    def snapshot(self) -> Dict[str, Any]:
        """Summary stats for get_stats()/logging."""
        if self.count == 0:
            return {"count": 0}
        return {
            "count": self.count,
            "mean_ms": round(self.total_ms / self.count, 3),
            "p50_ms": self.percentile(0.50),
            "p95_ms": self.percentile(0.95),
            "p99_ms": self.percentile(0.99),
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "negative_count": self.negative_count,
        }
