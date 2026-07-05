# Market-making theory → implementable takeaways for policy v3

Author: Researcher/Analyst. All formulas quoted from the cited primary sources,
fetched live 2026-07-06. Where a formula matters it is reproduced verbatim and
every term is mapped to something we can compute from our tape (per-block mid,
realized vol, fill rate, signed inventory). No training-data numbers.

**Vocabulary map (used throughout).** The theory's symbols and our measurables:
`s` = per-block mid; `σ` = realized vol (same time unit as the horizon); `q` =
signed inventory (contracts, or notional/mark); `γ` = risk-aversion knob (free,
we tune it); `T−t` = risk horizon to flatten; `λ(δ)` = fill intensity at quote
distance `δ` from mid — this is exactly our measured fills/s bucketed by depth;
`A, k` = the two constants of that fill curve, obtained by regressing log fill
rate on depth.

---

## Executive summary (5 lines)

1. **Avellaneda–Stoikov gives us two concrete levers**: quote around an
   inventory-shifted *reservation price* `r = mid − q·γ·σ²·(T−t)` instead of the
   mid, and set the half-spread from **volatility and the fill-rate curve**, not
   a fixed bp. In their own sim this cuts inventory variance ~65% at
   near-identical PnL.
2. **Glosten–Milgrom explains why v1 lost**: in a pure adverse-selection world
   transaction prices are a martingale, so there is *no* spread to capture —
   the entire spread a rational maker quotes is compensation for informed flow.
   Spread capture is not a source of profit; it is a fee you charge for toxicity.
3. **The decisive empirical result** (Binance BTC-perp, 2025): naive two-sided
   making loses ~60% of capital (Sharpe −109 over 3 days) **even with a −0.5 bp
   maker rebate**; the profit only appears once a short-horizon *reversal signal*
   is layered on. We pay a +1.5 bp maker fee, i.e. strictly worse than that
   losing baseline.
4. **Honest small-account finding**: at flat Tier-0 fees, positive-fee MM is not
   profitable on spread capture alone at any of our sizes. It requires a genuine
   short-horizon alpha (our OFI signal) driving quote placement. The
   market-making machinery is the *execution vehicle*; the OFI edge is the
   *profit source*.
5. **v3 should therefore be an OFI-conditioned, inventory-skewed quoter** with a
   vol-scaled spread and a hard inventory cap — not a symmetric spread-capture
   bot. The five design implications below are each tied to a formula or result.

---

## v3 design implications — ranked, testable, each tied to a result

> This is the section the SDE builds against. Each item names the formula it
> comes from, the code change, and the measurement that confirms it worked.

**D1 — Quote around an inventory-skewed reservation price, not the mid.**
*From:* A-S reservation price, eq. (29): `r(s,q,t) = s − q·γ·σ²·(T−t)`.
*Change:* compute `r` per block from mid, signed inventory `q`, realized `σ²`,
and a horizon `T−t`; place both quotes symmetrically around `r`, not around mid.
Long inventory pushes `r` below mid → the ask gets keener, the bid backs off, so
fills mean-revert the position. *Test:* A-S's own 1000-path sim (Table 1, γ=0.1)
shows the inventory strategy cuts std(final inventory) from **8.4 → 2.9** vs a
mid-symmetric quoter at essentially equal profit (65.0 vs 68.4). Replicate on
tape: expect a large drop in inventory variance at comparable gross PnL.

**D2 — Set the half-spread from volatility and the fill-rate curve, per coin,
per block — not a hardcoded bp.**
*From:* A-S optimal spread eq. (30): `δᵃ + δᵇ = γσ²(T−t) + (2/γ)·ln(1 + γ/k)`;
GLFT closed form: `half-spread = c₁ + (Δ/2)·σ·c₂` with
`c₁ = (1/(ξΔ))·ln(1 + ξΔ/k)`. *Change:* estimate `A, k` for each coin by
regressing `log(fill rate)` on quote depth `δ` (the model is `λ(δ)=A·e^{−kδ}`,
so `log λ = log A − kδ` is a straight line — slope `−k`, intercept `log A`); then
the spread widens with `σ` and with `1/k` (thin/steep books → wider). *Test:*
recompute spread each block from live `σ` and the fitted `k`; confirm realized
fill rate matches the `A·e^{−kδ}` prediction and that spread widens in
high-vol blocks. This replaces any static "quote N ticks off mid."

**D3 — Condition the spread (and quote/no-quote) on OFI toxicity.**
*From:* Glosten–Milgrom — the spread *is* the adverse-selection cost
`Ψ = E[V | buy] − E[V | sell]`; the standard defense is to widen or fade when
perceived toxicity rises. *Change:* map our short-horizon OFI signal to a
toxicity score; widen the half-spread (or pull the exposed side entirely) when
OFI predicts an adverse move, tighten when it predicts reversal. *Test:*
bucket post-fill drift by OFI decile at fill time; a working gate makes
post-fill drift on filled orders materially less negative in the toxic deciles.
This is the direct hook into what the SDE already measures (queue_ahead vs
post-fill drift).

**D4 — Do not expect spread capture to pay the fee. Make the OFI reversal signal
the profit source; the maker quote is just how we express it.**
*From:* The Market Maker's Dilemma (Binance BTC-perp): naive two-sided making
loses ~60% of capital, Sharpe −109, **even with a −0.5 bp rebate**; "spreads
alone cannot overcome negative drift"; profit appears only with a
reversal-prediction model. *Change:* v3's decision to quote a side must be
gated by the OFI signal's directional/reversal call, not by "the spread is
positive." *Test:* A/B the policy with the OFI gate on vs off; only the
gated variant should show positive post-fee PnL. This is the single most
decision-relevant change in the doc.

**D5 — Hard inventory cap with accelerating skew near the bound.**
*From:* A-S inventory-sensitivity term `θ² = −½·γσ²(T−t)` (quotes get
aggressively one-sided as `|q|` grows) and the infinite-horizon bound
`ω = ½·γ²σ²·(q_max+1)²`; GLFT enforces an explicit inventory limit. *Change:*
set `q_max` from capital and max leverage; as `|q| → q_max`, let the skew term
dominate so the reducing side is quoted at/through mid and the adding side is
pulled. *Test:* inventory histogram stays inside `±q_max` with no fat tail; no
single adverse fill can breach the risk budget.

---

## 1. Avellaneda–Stoikov (2008) — the optimal-quoting core

Source: Avellaneda, M. & Stoikov, S., "High-frequency trading in a limit order
book", *Quantitative Finance* 8(3), 217–224 (2008). Formulas below are quoted
from the paper (equation numbers are the paper's).

### 1.1 Setup — every term in our vocabulary

- **Mid-price** (their eq. 1): `dS_u = σ dW_u`, arithmetic Brownian motion,
  constant `σ`. Crucially the maker "has no opinion on the drift" — the base
  model is pure inventory control, *no alpha*. (This is exactly why §3 matters:
  the base A-S maker is not trying to predict price, only to manage inventory
  risk while collecting spread.) → **our `S` = per-block mid; `σ` = realized vol.**
- **Objective**: maximize `E[−exp(−γ(X_T + q_T S_T))]` — exponential (CARA)
  utility of terminal wealth, `γ` the risk-aversion coefficient. → **`γ` is our
  single tuning knob; larger `γ` = more inventory-averse = tighter, more skewed.**
- **Fill intensity** (their eq. 12): a market order lifts our quote at Poisson
  rate `λ(δ) = A·exp(−k·δ)`, decreasing in the distance `δ` of our quote from
  mid, with `A = Λ/α` and `k = αK` where `Λ` is the market-order frequency and
  `α` the power-law tail of order size. → **`λ(δ)` is our measured fills/s as a
  function of depth; `A, k` come from the log-linear fit in D2.**

### 1.2 Reservation (indifference) price — the inventory skew

The frozen-inventory reservation bid/ask (their eqs. 6, 7):

```
rᵃ(s,q,t) = s + (1 − 2q)·(γσ²(T−t)/2)
rᵇ(s,q,t) = s + (−1 − 2q)·(γσ²(T−t)/2)
```

and their average, the **reservation price** (eq. 8, and again eq. 29 in the
tradable approximation):

```
r(s,q,t) = s − q·γ·σ²·(T−t)
```

Reading it in our terms: start at the mid `s`; shift **down** by
`q·γ·σ²·(T−t)`. If we are long (`q>0`) `r` sits below mid — we *want* to sell,
so we lean our quotes down to get hit on the ask. Short (`q<0`) → `r` above mid,
leaning to buy. The shift grows with risk aversion `γ`, with variance `σ²`, and
with remaining horizon `(T−t)` (more time = more inventory risk to hedge
against). At `t→T` the shift vanishes: near the horizon there's no time for
inventory to hurt you, so you quote symmetrically.

### 1.3 Optimal spread

The optimal total spread around the reservation price (their eq. 30):

```
δᵃ + δᵇ = γσ²(T−t) + (2/γ)·ln(1 + γ/k)
```

Two additive pieces, both implementable:
- `γσ²(T−t)` — an **inventory-risk** component: wider when vol is high and the
  horizon is long.
- `(2/γ)·ln(1 + γ/k)` — a **market-structure** component set purely by the
  fill-curve steepness `k` and risk aversion `γ`; independent of inventory. A
  steeper fill curve (large `k`: fills fall off fast with depth) → narrower
  optimal spread; more risk aversion → wider.

The quotes are then `p^b = r − δᵇ` and `p^a = r + δᵃ`. Because the spread is
symmetric about `r` but `r` itself is skewed by inventory, the *net* effect is
an asymmetric pair of quotes around the mid — which is the whole point.

### 1.4 What the paper shows it buys you

Their 1000-path simulation (`s=100, T=1, σ=2, γ=0.1, k=1.5, A=140`):

| Strategy (γ=0.1) | Avg spread | Profit | Std(Profit) | Std(final q) |
|---|---|---|---|---|
| Inventory (skewed) | 1.49 | 65.0 | 6.6 | **2.9** |
| Symmetric (around mid) | 1.49 | 68.4 | 12.7 | **8.4** |

Same average spread; the inventory strategy gives up ~5% of mean profit to
roughly **halve profit variance and cut inventory variance ~65%**. That is the
trade A-S actually makes: it is a *risk-reduction* technology, not an
alpha-generation one. (Verbatim from the paper: the inventory strategy "obtains
a P&L profile with a much smaller variance.")

### 1.5 Suggested starting parameters for BTC/ETH/SOL perps

These are *starting points to calibrate on our tape*, not settled values:
- **`σ`**: realized vol estimated per-coin from per-block mid returns, in the
  same time unit as `(T−t)`. BTC/ETH/SOL differ; estimate each separately.
- **`k, A`**: fit `log(fill rate) = log A − k·δ` per coin from our capture
  (we already log fills/s and can bucket by quote depth). Our measured activity
  (2026-07-03 capture: fills/s BTC 4.51 / ETH 1.86 / SOL 2.23) sets `Λ` and thus
  `A`; the depth→fill falloff sets `k`.
- **`γ`**: the free knob. Start small (their γ=0.1 is "close to risk neutral";
  γ=1 is "very risk averse") and raise it until inventory variance sits inside
  the capital budget. `γ` and `q_max` are the two safety dials.
- **`T−t`**: crypto perps trade continuously with no terminal `T`, so the raw
  `(T−t)` is ill-defined — use the GLFT stationary limit (§2), i.e. treat the
  horizon as a fixed inventory-flattening timescale rather than a countdown.

---

## 2. Guéant–Lehalle–Fernandez-Tapia — the closed form we should actually code

Source: Guéant, Lehalle & Fernandez-Tapia, "Dealing with the inventory risk: a
solution to the market making problem", *Mathematics and Financial Economics*
(2013); arXiv:1105.3115. Practitioner formulas cross-checked against the
`hftbacktest` GLFT tutorial.

**Why we need it.** A-S's `(T−t)` term goes to zero at the horizon, which is
undefined for a continuously-trading perp desk. GLFT solves the same control
problem but takes `T→∞`, yielding **stationary, inventory-explicit closed-form
quotes** — no countdown, directly codeable. Optimal quote depths (their (4.6)/
(4.7), simplified):

```
δᵇ*(q) = c₁ + (Δ/2)·σ·c₂ + q·σ·c₂        (bid distance from mid)
δᵃ*(q) = c₁ + (Δ/2)·σ·c₂ − q·σ·c₂        (ask distance from mid)

c₁ = (1/(ξΔ))·ln(1 + ξΔ/k)
c₂ = √( (γ/(2AΔk))·(1 + ξΔ/k)^(k/(ξΔ)+1) )
```

Decomposition, in our terms:
- **half-spread** = `c₁ + (Δ/2)·σ·c₂` — the symmetric part, scales with `σ`.
- **inventory skew** = `σ·c₂` **per unit inventory** — subtract `q·σ·c₂` from the
  ask distance and add it to the bid distance; identical role to A-S's
  reservation-price shift but in stationary form.
- `Δ` = order size, `ξ` a risk parameter (≈ `γ` in the common special case),
  `A, k` the same fill-curve constants as A-S.

**Calibration is the same regression as D2**: `λ = A·e^{−kδ}` ⇒ regress
`log(observed fill rate)` on quote depth, slope `−k`, intercept `log A`. This is
the single estimation step that feeds both the spread and the skew.

**Recommendation:** code the *GLFT stationary form* (constant half-spread from
vol + fitted `k`, linear inventory skew `q·σ·c₂`), not the raw A-S countdown.
It is the same economics without the ill-defined horizon.

---

## 3. Adverse selection — why a naive maker structurally loses

### 3.1 Glosten–Milgrom (1985): the spread is the toxicity price

Source: Glosten, L. & Milgrom, P., "Bid, ask and transaction prices in a
specialist market with heterogeneously informed traders", *J. Financial
Economics* 14(1), 71–100 (1985).

A risk-neutral market maker who earns *zero* expected profit still must quote a
strictly positive spread. The quotes are conditional expectations of the true
value `V` given the *direction* of the arriving order:

```
ask A_t = E[V | buy order arrives]
bid B_t = E[V | sell order arrives]
adverse-selection spread  Ψ = E[V | buy] − E[V | sell]
```

The logic: a buy order is *evidence* the trader might know something bullish, so
the rational ask is the value conditional on that evidence — above the
unconditional mid. The spread is exactly the expected loss to informed traders,
recovered from uninformed ones. The MM Bayesian-updates beliefs after every
trade, and — the key consequence for us — **transaction prices form a
martingale**: `E[p_{k+1} | S_k] = p_k`.

**What this means for v1/v2.** In the pure Glosten–Milgrom world there is *no
predictable price move to capture* — the mid you'd earn your spread against is a
martingale, and every fill you get is, on average, adverse (you buy just before
the martingale ticks down, sell just before it ticks up). The spread you quote
is not profit; it is the premium that just offsets that adverse selection. A
maker with **no information edge** breaks even *before* fees and loses by exactly
the fee after them. This is the theoretical statement of why our measured
"spread capture ≈ 0.1 bp ≪ 1.5 bp maker fee" is not a tuning problem — it is the
model working as designed.

### 3.2 Toxic flow, VPIN, and the standard defenses

When some flow is more informed than the rest ("toxic"), the break-even spread
is not constant — it should rise with perceived toxicity. The practitioner
metric is **VPIN** (Volume-Synchronized Probability of Informed Trading): a
volume-bucketed order-imbalance measure that estimates the share of flow that is
informed. Empirically VPIN rises ahead of volatility spikes and price jumps
(documented for BTC spot). The standard maker defenses, all of which map onto
our knobs:

1. **Toxicity-conditioned spread** — widen when the toxicity estimate is high
   (this is D3; the "widen with adverse selection" prescription is the direct
   GM consequence).
2. **Quote fading / one-sided pulling** — when short-horizon flow predicts an
   adverse move, pull the exposed side rather than just widening it.
3. **Volatility- and intensity-conditioned spread** — already captured by the
   `γσ²(T−t)` term in A-S / the `σ·c₂` scaling in GLFT.

Our **OFI signal is precisely a toxicity/direction estimator** at short horizon.
The connection to what the SDE measures — `queue_ahead` (how much size is in
front of us, i.e. fill probability) vs `post-fill drift` (how the mid moves after
we fill, i.e. realized adverse selection) — is exactly the Glosten–Milgrom
mechanism made measurable: high fill probability co-occurs with worse post-fill
drift. See §4.

---

## 4. The small-account, flat-fee reality — the honest finding

**The question:** is there a published treatment of profitable market-making at
retail size and flat (non-rebate) fees, or does the honest literature say
positive-fee MM needs either rebates or a genuine short-horizon alpha layered on?

**The answer, stated plainly: the literature does not support profitable
spread-capture MM at flat positive fees. It requires a real short-horizon
predictive signal. Our OFI edge is exactly that signal, and without it v3 will
lose for the same structural reason v1 did.**

The decisive study is **"The Market Maker's Dilemma: Navigating the Fill
Probability vs. Post-Fill Returns Trade-Off"** (arXiv:2502.18625), run on
**Binance BTC-USDT perpetual** — the most liquid crypto venue, tighter and
cheaper than anything we trade. Their findings:

- **A structural negative correlation between fill likelihood and post-fill
  returns.** Orders in high-fill-probability positions (short near-side queue,
  large opposite queue) fill precisely when the price is about to move against
  them. Verbatim: "a negative correlation between maker fill likelihood and
  post-fill returns." This *is* Glosten–Milgrom adverse selection, measured.
- **Fill-probability model** (OLS, R²=0.946):
  `z = 0.5649 + 0.0159·Q_near + 0.1013·Q_opp − 0.3166·imb` — near-side queue and
  order-book imbalance dominate. Fill probabilities span ~30% to >90% purely on
  queue geometry. (This is the theory behind our `queue_ahead` feature.)
- **Post-fill returns by queue position**: front-of-queue fills average
  −0.058 bp; back-of-queue fills average **−0.775 bp** — orders at the back of
  the queue underperform by ~0.7 bp *systematically*. You either fill early
  (toxic) or wait at the back (adverse). There is no free spread.
- **Naive two-sided making loses ~60% of capital, Sharpe −109 over three days —
  and that is WITH a −0.5 bp maker rebate.** "All imbalance-based strategies —
  whether maker or taker — perform poorly, with negative returns across the
  board." "Spreads alone cannot overcome negative drift."
- **Even a profitable-looking taker signal dies on fees**: their imbalance taker
  strategy is "highly profitable before fees (around +1 bp per roundtrip)" but
  "becomes unprofitable after paying the 1.5 bp taker fee."
- **The only thing that works is an alpha signal**: they build a
  reversal-prediction model (logistic regression on price dynamics, queue
  imbalance history, and order-flow features) and conclude that "identifying
  such reversals offers a potential resolution to the fundamental challenge
  facing maker orders." Profit comes from *predicting when the book imbalance is
  wrong*, not from quoting a spread.

**Now apply it to us.** That study had a **−0.5 bp maker rebate** and still
lost on spread capture; per our own `fee-schedule.md`, we pay a **+1.5 bp maker
fee** with no reachable rebate tier at $150 capital. We are 2.0 bp/side worse
than the losing baseline in that paper. There is no configuration of A-S
spread/skew parameters that manufactures 2 bp of edge from thin air — A-S is a
variance-reducer (§1.4), not an alpha source, and it explicitly assumes zero
drift.

Corroborating theory: A-S itself models a driftless mid and delivers *variance
reduction, not positive drift capture*; the make/take-fee literature (e.g.
"Subsidizing Liquidity", and the agent-based make-taker studies) finds that
rebate structures are what make pure liquidity provision viable, and that
"the naive strategy, which is prima facie appealing because it receives the
rebate and benefits from the spread, is in fact highly unprofitable."

### 4.1 What this means for v3, concretely

- The market-making stack (reservation price, vol-scaled spread, inventory skew,
  inventory cap) is worth building — but as the **execution/risk layer**, not the
  profit engine. It controls *how* we sit in the book and *how* we bleed
  inventory risk, and A-S/GLFT prove it does that well.
- The **profit must come from the OFI signal** deciding *when* and *which side*
  to quote — i.e. counter-trading imbalance only when the signal predicts the
  imbalance is about to reverse. This is D3+D4.
- If, on tape, the OFI-gated maker still does not clear +1.5 bp/side after fees,
  the honest conclusion the literature would predict is that flat-fee maker MM is
  not viable at our size, and the edge (if any) is a *taker* expression of the
  OFI signal that must clear the 4.5 bp taker fee — a much higher bar the same
  paper shows is easy to fail. That is the number to respect, not wish away.

---

## Sources (all fetched live 2026-07-06)

- Avellaneda, M. & Stoikov, S. (2008), "High-frequency trading in a limit order
  book", *Quantitative Finance* 8(3), 217–224. Primary PDF (NYU):
  https://math.nyu.edu/~avellane/HighFrequencyTrading.pdf
- Guéant, O., Lehalle, C.-A. & Fernandez-Tapia, J. (2013), "Dealing with the
  inventory risk: a solution to the market making problem", *Mathematics and
  Financial Economics*; arXiv:1105.3115 — https://arxiv.org/pdf/1105.3115 ;
  Springer: https://link.springer.com/article/10.1007/s11579-012-0087-0
- GLFT closed-form practitioner reference (hftbacktest tutorial):
  https://hftbacktest.readthedocs.io/en/py-v2.0.0/tutorials/GLFT%20Market%20Making%20Model%20and%20Grid%20Trading.html
- Glosten, L. & Milgrom, P. (1985), "Bid, ask and transaction prices in a
  specialist market with heterogeneously informed traders", *J. Financial
  Economics* 14(1), 71–100. Overview + formulas:
  https://www.tradicted.com/research/glosten-bid-ask-1985/ ; original:
  https://www.sciencedirect.com/science/article/pii/0304405X85900443
- "The Market Maker's Dilemma: Navigating the Fill Probability vs. Post-Fill
  Returns Trade-Off" (2025), arXiv:2502.18625 — https://arxiv.org/html/2502.18625
- VPIN / order-flow toxicity (crypto): CoinAPI glossary
  https://www.coinapi.io/learn/glossary/order-flow-toxicity ; "Bitcoin wild
  moves: Evidence from order flow toxicity and price jumps", *ScienceDirect*
  https://www.sciencedirect.com/science/article/pii/S0275531925004192
- Make/take-fee & liquidity-subsidy context: "Optimal make-take fees for market
  making regulation", arXiv:1805.02741 https://arxiv.org/pdf/1805.02741 ;
  "Subsidizing Liquidity: The Impact of Make/Take Fees on Market Quality"
  https://www.researchgate.net/publication/228261376

*Labeled assumptions: (a) our fill-rate anchors are from the 2026-07-03 3-coin
capture and must be re-fit per coin before use; (b) γ, q_max, and the horizon
timescale are tuning knobs, not settled values; (c) the Binance-perp study's
fee/liquidity regime is more favorable than ours, so its negative result is a
lower bound on our difficulty, not an upper bound.*
