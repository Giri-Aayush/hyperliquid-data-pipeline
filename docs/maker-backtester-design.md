# Event-driven maker backtester — design (v1.1, contract LOCKED)

**Status:** jointly reviewed and locked 2026-07-04 (CTO draft, SDE review
amendments accepted) · **Phase:** post-strategy-sign-off (market-making,
docs/strategy-memo.md) · **Package:** `src/hyperliquid_pipeline/sim/` —
deliberately separate from the bar-level `backtest/`.

## Review amendments (accepted, superseding the draft below where they differ)

1. **Queue bounds corrected:** pro-rata is a CENTRAL estimate, not a bound.
   True bracket: PESSIMISTIC = cancels all land behind us (floor; queue-ahead
   decays via trades only) · PRORATA = central · OPTIMISTIC = cancels all land
   ahead (ceiling). One estimator parameterized by cancel location; decision
   rule: a policy must clear PESSIMISTIC. When real L4 lands, the empirical
   cancel-ahead fraction becomes a calibrated config value.
2. **One QueueSim instance per (coin, bound); grid cells run as independent
   passes** — fills diverge across bounds, so inventory and policy behavior
   diverge; tagging fills in a shared pass would contaminate counterfactuals.
3. **Block-replay pins:** engine delivers all of block N's trades BEFORE
   on_book(N) (book diffs already embed trade effects — double-count guard);
   in EXACT mode queue-ahead is maintained from book diffs only, trades only
   compute OUR fills; on_book fires for every block including quiet ones and
   carries the BlockDiffBatch in L4 mode; on `view.stale` the engine cancels
   all virtual orders (stale_evictions counted). TradeEvent.side is the
   AGGRESSOR side ('B' taker buy consumes ask queues). Book-only crossings do
   not fill in v1 (crossed_unfilled counter): no print, no fill.
4. **Frozen signatures** (see SDE review, IPC 2026-07-04): BookEvent,
   TradeEvent, QueueBound{PESSIMISTIC,PRORATA,OPTIMISTIC,EXACT},
   QueueSim.place/cancel/on_trade/on_book/queue_ahead/open_orders/get_stats,
   Fill(order_id, coin, side=our resting side, px, sz, t_ms, height,
   queue_bound, queue_ahead_at_fill, mid_at_fill, maker). QueueSim is
   latency-free; latency is 100% engine-side. The Δt-later mid for adverse
   selection is computed by the report from the stream, keeping Fill causal.

## What it must answer

Can an OFI-aware quoting policy on BTC/ETH/SOL perps earn spread net of
adverse selection, fees, and latency — and how does that change with queue
priority? The bar engine cannot say; this simulator exists to say it honestly.

## Architecture

```
 event sources                 engine                        accounting
 ─────────────                 ──────                        ──────────
 capture JSONL (bbo/l2/trades) ─┐   ┌─ latency model          fills ledger
 archive hours (l2Book)        ─┼──▶│  (δ_submit, δ_feed,     PnL decomposition:
 L4 diffs (QN feed / node)     ─┘   │   block-quantized)       spread capture
                                    │                          − adverse selection
             MakerPolicy.on_event ◀─┤  QueueSim                − fees (+rebates)
             (BookView, signals,    │  (virtual orders in      ± inventory mark
              inventory, clock)     │   the real book: L2      ± funding accrual
             → quotes/cancels ─────▶│   estimated / L4 exact)
                                    └─ FillModel (trades cross the queue)
```

### 1. Event sources (`sim/events.py`)
One normalized stream: `BookEvent` (state replace/diff) and `TradeEvent`
(px, sz, side, t). Sources: research-capture JSONL (bbo + orderbook + trades),
archive l2Book hours (books only — the archive has no trades, so archive-only
runs disable fill simulation and are signal-replay only), and L4 block diffs
when real order-level data lands. Exchange clock drives the sim; recv stamps
drive the feed-latency model when present.

### 2. Market state
The existing book core, unmodified: `L2Book`/`L4Book` behind `BookView`.

### 3. QueueSim (`sim/queue.py`) — the heart
Our quotes are *virtual orders* overlaid on the replayed book. Two fidelity
modes, same API:

- **L4 mode (exact):** virtual order takes a real FIFO slot; queue-ahead is
  `L4Book.queue_position` ground truth; cancels/fills ahead replay exactly.
- **L2 mode (estimated — all we have until real L4 data):** join at the back
  of the visible level (queue_ahead = level size at join). Queue-ahead decays
  on (a) trades at our price — observable; (b) cancels ahead — NOT observable
  in L2, so it is a modeling assumption. Run BOTH bounds every time:
  `pessimistic` (cancels always behind us) and `optimistic` (pro-rata share of
  level shrinkage). Reports must show both; if a policy only works under the
  optimistic bound, it doesn't work.

### 4. Latency + block quantization (engine)
Actions submitted at t take effect at t+δ_submit; the policy sees data δ_feed
old. Defaults from measured reality: δ ≈ 400 ms today, 200 ms Tokyo scenario —
both run by default so every result is a pair (here / colocated). Fills and
book changes land on block boundaries, matching the venue.

### 5. FillModel (`sim/fills.py`)
A trade of size s at our price consumes queue-ahead first, then fills us
(partials fine). Price trading *through* our level fills us fully at our
price. No self-impact (we assume our size doesn't move the book — documented;
fine for research sizing). Every fill records the mid at fill and the mid
Δt later (adverse-selection accounting, Δt configurable, default 5s — chosen
because the OFI study shows the information horizon dies by ~30s).

### 6. MakerPolicy (`sim/policy.py`)
`on_event(book: BookView, signals, inventory, clock) -> [QuoteAction]`
(place/cancel/replace per side with px/sz). Signals: windowed OFI from
`research/ofi.py` plumbed in causally (only past events), funding rate from
asset_ctx. Reference policy shipped: symmetric quotes at touch ± k ticks with
OFI-conditioned skew and inventory bands — the null hypothesis every fancier
policy must beat.

### 7. Accounting (`sim/report.py`)
Per run: PnL decomposed into spread capture, adverse selection, inventory
mark, funding, fees/rebates; fill rate, quote uptime, time-at-touch,
inventory distribution, max drawdown. Both queue bounds × both latency
scenarios = a 2×2 grid per policy per coin. JSON + table, caveats embedded,
same honesty rules as the OFI reports.

## Split (proposed)

- **SDE:** `sim/queue.py` + `sim/fills.py` + their tests — the microstructure
  core: pure, deterministic, contract-heavy (FIFO discipline, both L2 bounds,
  partial fills, through-fills, block alignment).
- **CTO:** `sim/events.py`, `sim/engine.py`, `sim/policy.py`, `sim/report.py`,
  integration tests, and a demo run on the captured hour.
- **Frozen contract between the halves:** `BookEvent`/`TradeEvent` dataclasses,
  `QueueSim.place/cancel/on_trade/on_book -> [Fill]`, and the `Fill` record
  (px, sz, side, queue_bound, t, mid_at_fill). Exact signatures agreed in
  review before either side writes code.

## Non-goals (v1)

Self-impact, multi-venue, order-type zoo (post-only/IOC only), spot, live
trading. The simulator's output is a research verdict, not an execution
system.
