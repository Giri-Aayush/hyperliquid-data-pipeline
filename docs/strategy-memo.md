# Strategy-class memo: market-making first

**Date:** 2026-07-04 · **Universe:** BTC, ETH, SOL perpetuals on Hyperliquid ·
**Status:** SIGNED OFF 2026-07-04. This closes the decision the original
design handoff deferred: the system builds toward **market-making**.

## Recommendation

**Market-making first.** The measured signal is real but small — exactly the
shape that pays a maker (who uses it to dodge adverse selection while
collecting spread) and starves a taker (who must pay fees larger than the
signal). Taking stays on the shelf unless/until we find dislocations an order
of magnitude larger or reach fee tiers near zero.

## Evidence

Two independent samples, one method (Cont–Kukanov–Stoikov order-flow imbalance
at top of book, `research/ofi.py`, Newey–West t-stats):

**Sample A — live capture, 2026-07-03, 60 min, event-level bbo (zero drops):**

| coin | 1s OFI → next 1s | t-NW | decile monotonicity | extreme-decile edge |
|---|---|---|---|---|
| BTC | r = 0.244 | 6.4 | 6/9 up | −0.27 → +0.40 bps |
| ETH | r = 0.280 | 5.7 | 6/9 up | −0.29 → +0.50 bps |
| SOL | r = 0.222 | 9.9 | 7/9 up | −0.49 → +0.76 bps |

**Sample B — archive, 2026-04-01, 3 hours × 3 coins, real L2 snapshots:**

| coin | 1s OFI → next 1s | t-NW | decile monotonicity | 5s extreme edge |
|---|---|---|---|---|
| BTC | r = 0.247 | 17.7 | 8/9 up | −0.61 → +0.53 bps |
| ETH | r = 0.227 | 13.5 | 8/9 up | −0.55 → +0.63 bps |
| SOL | r = 0.332 | 23.3 | 9/9 up | −0.66 → +0.64 bps |

Same signature three months apart: strong at 1s, decaying to noise by 30s.
Decile tables are monotone through zero — imbalance genuinely orders the
distribution of the next move, it isn't an artifact of outliers.

**Funding regime** (live session): all three coins at ~+11%/yr-equivalent
hourly funding (longs pay); basis mostly within ±5 bps with brief dislocations
(SOL touched −27 bps). Mild tailwind for short-leaning maker inventory.

**Latency baseline**: bbo p50 ≈ 370 ms (NTP-adjusted) from the current host to
the public gateway; the venue floor is one consensus round (~200 ms colocated,
per docs — to be measured from Tokyo).

## Why this kills taking and feeds making

The tradable edge at seconds-scale horizons is **≤ ~1 bps at the extreme
deciles**. Hyperliquid base fees are ~4.5 bps taker / ~1.5 bps maker (volume
tiers lower; verify the current schedule before sizing anything). A taker
crossing the spread pays 4.5 bps to chase 0.5–1 bps of expected move: dead on
arrival, before latency and adverse selection.

A maker's economics invert: revenue is spread capture (plus rebates at high
tiers), and the dominant cost is **adverse selection — getting filled just
before the price moves through you**. The OFI decile table is literally a map
of that risk: quotes should lean away from strong same-side imbalance and
lean in when imbalance favors the fill. The signal doesn't need to out-earn
fees; it needs to improve fill selection. Small-but-real is enough.

Market-making priorities also match what's already built: exact queue-position
modeling (`L4Book.queue_position`), event-level capture, and latency
measurement. The handoff's own analysis said MM prioritizes the L4/queue
investment — that investment is done and audited.

## What making needs next (in order)

1. **Real L4 data** — QuickNode plan upgrade (consumer ready: Task B in
   progress) or the Tokyo node. Pins queue-position modeling to reality.
2. **Longer captures** — multi-day, multi-regime; one hour + three April hours
   is a directional read, not a distribution.
3. **Event-driven maker backtester** — book replay with queue simulation,
   maker/taker fees, latency; the bar-level engine cannot evaluate this. This
   is the next major build phase.
4. **Tokyo latency parity** — everyone tuned sits at the same floor; queue
   discipline decides the rest.
5. **Funding-aware inventory policy** — the +11%/yr regime is a real input to
   quote skew.

## Caveats (they ship in every report this memo cites)

Short samples, one venue, autocorrelated windows (HAC helps, cannot cure a
single regime), no fees/latency/queueing modeled in the regression itself.
This memo picks a *direction of investment*, not a strategy parameterization.
