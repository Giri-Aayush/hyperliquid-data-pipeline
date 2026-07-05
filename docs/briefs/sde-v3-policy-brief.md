# Brief: v3 policy exploration + adverse-selection diagnostics (SDE)

**Claim this role:** IPC-message the CTO (s-mr53om6k-u71x) as "[SDE]" to take it.
**Nature of this work (read first):** the project is gated on TAPE (accruing,
needs calendar days) and CAPITAL (user's call). This is **sharpen-the-axe**
work on *existing* data — it improves the odds when tape/capital arrive; it
does not unblock the project. **Everything you produce stays ADVISORY** — we
have ~1 session-day of tape and overfitting to it is the central trap.
Success = finding the *shape* of a viable policy and understanding *where the
loss comes from*, NOT declaring a winner.

**Boundary:** `sim/policy.py` (new policy classes only, don't break
WidthPolicy/ReferenceOfiPolicy), a new `sim/diagnostics.py`, `scripts/` for
analysis runners, `tests/` for the above. Do NOT touch the frozen contract
(`sim/types.py`), the queue core (`sim/queue.py`), or the engine.

## Tasks, in priority order

1. **Adverse-selection joint distribution** (`sim/diagnostics.py` + a script).
   The v1/v2 runs showed negative spread capture — fills land worse than mid.
   Build the diagnostic that answers *are our fills the toxic ones?*: for each
   fill, join `queue_ahead_at_fill` against the post-fill mid drift at Δt
   (1s/5s). Bucket by queue_ahead and by regime. Output: does drift worsen
   when we're at the front of the queue (classic adverse selection) vs the
   back? This decides whether wider quoting can escape the loss or whether the
   flow is uniformly toxic. Report as a table; no policy change yet.

2. **Regime-conditional quoting.** The v2 run found losses concentrate in
   *volatile* buckets (ETH w8 min-regime worse than min-day). Add a policy
   variant that widens or stops quoting in the volatile tercile (reuse the
   sweep's regime labeling). Test on existing tape via the sweep. This is the
   single most promising lead from the real-data runs.

3. **Spread-floor filter + finer width grid.** At Tier-0 fees, joining a
   1-tick spread guarantees capture < fee — add a "don't quote when spread ≤
   N ticks" gate to WidthPolicy (or a subclass) and extend the sweep's width
   grid finer around where the gradient flattened (w4–w16).

4. **(If time) gap-backfill redesign** — self-contained, no tape needed. The
   archive has no trades, so the reconnect gap-backfill can't recover them
   (currently warns loudly). Redesign it to recover missed frames from the
   lossless spool instead of the archive. `collectors/backfill.py` +
   `collectors/spool.py` read path + tests.

**DoD:** tests green, full suite green, a short findings note appended to
`docs/maker-backtester-design.md` (clearly labeled ADVISORY / 1-day), and a
DONE report to the CTO with a suggested commit message. You do NOT commit —
the CTO reviews and commits.
