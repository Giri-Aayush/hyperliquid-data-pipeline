# Brief: v2 parameter sweep with an every-day, every-regime gate

**Status: OWNED BY CTO, in build 2026-07-05** (original SDE session ended
before acceptance was actioned — check `git log -- src/hyperliquid_pipeline/sim/sweep.py`
before picking this up in a new session).

## Goal

One question, answered honestly: **is there any WidthPolicy parameter region
that clears the PESSIMISTIC queue bound, net of flat 1.5 bp maker fees, on
EVERY captured day and in EVERY volatility regime — not on average?**
Averages hide the day that kills a $150 account.

## Spec (jointly designed with the SDE before their session ended)

- Grid: `WidthPolicy(width_ticks ∈ {0,1,2,4,8}, skew_gain ∈ {0,1,2},
  funding_tilt_ticks ∈ {0,1})` × 3 queue bounds × every capture day × 3 coins.
  Quote sizes per coin as in `scripts/run_maker_sim.py`.
- Capture days discovered by glob under `data/daily_captures/*/` (new days
  join automatically as the daily capture accrues).
- Regime labels: 5-minute windows labeled calm/mid/volatile by realized-vol
  terciles **pooled per coin across all days** (so "volatile" means the same
  thing on every day). Window PnL from the engine's per-block equity series.
- Gate per (coin, params): PASS iff pessimistic-bound net PnL > 0 for the
  minimum day AND the minimum regime bucket. Min-day and min-regime are
  reported, never averages alone.
- **Minimum-days rule: below 5 captured days every verdict prints as
  ADVISORY** ("insufficient tape for a live decision") — never as PASS.
- Fees: gate at flat Tier-0 1.5 bp maker (per docs/research/fee-schedule.md —
  rebate tiers are whale-only). Referral-discounted 1.44 bp is a sensitivity
  ROW computed analytically (fees are linear in notional; no rerun), never
  the gate.
- Latency: single 400 ms scenario — the L2 tape cannot resolve latency
  differences (proven in the v1 run), so sweeping it would be theater.

## Deliverables

`sim/sweep.py` (discovery, grid runner, regime labeling, gate report, CLI)
+ `tests/sim/test_sweep.py` (synthetic multi-day tapes pinning: discovery,
ADVISORY below min-days, gate failure on one bad day, fee-sensitivity
arithmetic, regime tercile labeling).
