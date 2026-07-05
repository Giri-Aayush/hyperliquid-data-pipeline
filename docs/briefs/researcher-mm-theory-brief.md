# Brief: market-making theory → implementable takeaways (Researcher)

**Claim this role:** IPC-message the CTO (s-mr53om6k-u71x) as
"[Researcher/Analyst]" to take it.
**Why now:** our empirical runs measured the problem precisely — at Tier-0
fees, touch-joining loses because spread capture (~0.1 bp) is ~10× below the
maker fee (1.5 bp), and fills suffer adverse selection during the latency
window. The v2 fix (quote wider) helps but hasn't cleared the gate. Before we
guess at v3, we should stand on the shoulders of the people who solved this
formally. This is pure $0 desk research and directly feeds the SDE's v3 policy
work.

**Boundary:** `docs/research/market-making-theory.md` only. Live web fetches,
cite every source, verbatim formulas where they matter.

## What to deliver — a synthesis with IMPLEMENTABLE takeaways, not a lit dump

1. **Avellaneda–Stoikov (2008) optimal market making.** Extract the concrete
   mechanics we could code as a policy: the reservation price (mid shifted by
   inventory × risk × variance × time), the optimal bid/ask spread formula
   (its dependence on volatility, order-arrival intensity, risk aversion), and
   how inventory drives skew. Give the actual formulas and define every term
   in our vocabulary (we have per-block mid, realized vol, fill rates,
   inventory). What would the parameters be for BTC/ETH/SOL perps?

2. **Adverse selection: Glosten–Milgrom & the toxic-flow problem.** Why
   informed flow structurally makes a naive maker lose, and the standard
   defenses: spread widening with perceived toxicity, quote fading, and
   trading-intensity/volatility-conditioned spreads. Connect to what the SDE
   is measuring (queue_ahead vs post-fill drift).

3. **The small-account reality.** How do the models change when you cannot
   reach rebate tiers and pay flat taker/maker? Is there any published
   treatment of profitable MM at retail size and flat fees, or is the honest
   finding that positive-fee MM needs either rebates or a genuine short-horizon
   alpha (like our OFI signal) layered on top? Say what the literature actually
   supports — including if the answer is discouraging.

4. **One-page "v3 design implications"** at the top: 3–5 concrete, testable
   policy changes the theory recommends, ranked, each tied to a formula or
   result above. This is the part the SDE will actually use.

**DoD:** `docs/research/market-making-theory.md` complete, executive summary
(5 lines) + the v3-implications section up top; DONE report to the CTO. You do
NOT commit — the CTO reviews and commits.
