# Hyperliquid non-validating node: runbook + cost analysis

**Status: COMPLETE (decision-ready-later per capital constraint).** All lanes done; remaining open items are measure-on-deploy questions explicitly labeled in-text, none blocking the recommendation.
Author: Researcher/Analyst · Date: 2026-07-04, finalized 2026-07-05 · All prices/specs fetched live; nothing from model training data. AWS pricing from AWS's own bulk files (2026-06-25/06-30 versions); bare-metal from provider pricing pages 2026-07-05.

**Decision this feeds:** run our own non-validating node in Tokyo vs pay a provider, for (a) raw L4 order data (`--write-raw-book-diffs`) driving L4Book queue modeling, (b) mempool streaming (`split_client_blocks`) + priority-fee tuning — **(b) is node-only; no provider sells it**. We trade BTC/ETH/SOL perps. Latency context: public-WS baseline ~370 ms p50 from current location; validators cluster in AWS Tokyo (ap-northeast-1); colocated floor ~200 ms per docs, higher under load per Glassnode's monitor.

---

## 1. Runbook — non-validating mainnet node

Source for the whole section unless noted: [hyperliquid-dex/node README](https://github.com/hyperliquid-dex/node) (fetched 2026-07-04); auxiliary facts from `README_misc.md` and `pruner/` in the same repo.

### 1.1 Requirements

| Item | Official value |
|---|---|
| Hardware (non-validator) | **16 vCPU, 128 GB RAM, 500 GB SSD** (validator: 32 vCPU / 128 GB / 1 TB) |
| OS | "Currently only Ubuntu 24.04 is supported." |
| Placement | "For lowest latency, run the node in Tokyo, Japan." |
| Ports | "Ports 4001 and 4002 are used for gossip and must be open to the public." |
| Gossip peers | Default 8 for non-validators, configurable 8–100 (`README_misc.md`) |
| Bandwidth | **Not stated in official docs**; also **not found** in community (§3.7). Bounded structurally in §3.5. |

Placement note: everything latency-sensitive in this system keys off proximity to the validator set in AWS Tokyo. A node outside Tokyo still syncs fine but forfeits the entire latency motivation — you'd pay node costs to see data later than a Tokyo box would.

### 1.2 Install and run

```bash
# 1. binary + signature verification
curl https://binaries.hyperliquid.xyz/Mainnet/hl-visor > ~/hl-visor && chmod a+x ~/hl-visor
gpg --import pub_key.asc     # key ships in the repo
curl https://binaries.hyperliquid.xyz/Mainnet/hl-visor.asc > hl-visor.asc
gpg --verify hl-visor.asc hl-visor

# 2. config
echo '{"chain": "Mainnet"}' > ~/visor.json
# non-validators on mainnet MUST provide root peers:
echo '{"root_node_ips": [{"Ip": "<root-ip>"}], "try_new_peers": false, "chain": "Mainnet"}' \
  > ~/override_gossip_config.json   # README lists ~28 operator-maintained root IPs (mostly JP/SG/DE)

# 3. run (with the flags we need — see 1.3)
~/hl-visor run-non-validator --write-raw-book-diffs --write-order-statuses \
    --write-fills --batch-by-block --disable-output-file-buffering
```

Repo also ships `Dockerfile` + `docker-compose.yml` (`docker compose build && docker compose up -d`), and `README_misc.md` shows a systemd unit with `Restart=always` / `RestartSec=10`.

**Initial sync:** the docs do **not** document a snapshot mechanism. Official wording: "It may take a while as the node navigates the network to find an appropriate peer to stream from. Logs such as `applied block X` indicate streaming live data." Real-world sync times: **not found** in any public source (§3.7).

**Mempool streaming (the node-only capability):** "Latency sensitive users can set `split_client_blocks: true` in `~/override_gossip_config.json` to stream uncommitted mempool transactions to `~/hl/data/mempool_txs/{date}`. These transactions are eagerly broadcasted by nodes before they are committed." Related: gossip-auction priority ordering opt-in via `~/hl/file_mod_time_tracker/node_gossip_priority_config.json` → `{"enabled": true}`.

### 1.3 The flags we need — exact documented semantics

| Flag | Official semantics (verbatim where quoted) |
|---|---|
| `--write-raw-book-diffs` | "Writes every L1 order diff to `~/hl/data/node_raw_book_diffs/hourly/{date}/{hour}`. Note that raw book diffs can be a substantial amount of data." |
| `--write-order-statuses` | "Writes every L1 order status to `~/hl/data/node_order_statuses/hourly/{date}/{hour}`. Note that orders can be a substantial amount of data." |
| `--write-fills` | Streams fills (API format) to `node_fills/hourly`; also TWAP statuses; overrides `--write-trades`. |
| `--batch-by-block` | "Writes the above files with one block per line … schema is `{local_time, block_time, block_number, events}`." **Required by order_book_server.** |
| `--stream-with-block-info` | Per-event writes with the same schema as `--batch-by-block` (lower latency alternative when we consume files directly). |
| `--disable-output-file-buffering` | "Flush each line immediately when writing output files. This reduces latency but leads to more disk IO operations." |

**Per-coin filtering: none.** No flag filters output to specific coins — the node writes **all coins**; disk must be sized for the full universe, our 3-coin cut happens downstream.

### 1.4 order_book_server (self-hosted L4 endpoint)

Source: [hyperliquid-dex/order_book_server README](https://github.com/hyperliquid-dex/order_book_server) (fetched 2026-07-04).

- Rust websocket server: `cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000`; tunable `--websocket-compression-level`.
- Exposes `l2book` (up to 100 levels), `trades`, and **`l4book` — initial snapshot then per-block order diffs, per-coin subscriptions** (`"coin": "<symbol>"`). This is the self-hosted equivalent of QuickNode's StreamL4Book.
- Requires the node to run with fills + order statuses + raw book diffs, **batched by block**; "the current implementation batches node outputs by block, making the order book a few milliseconds slower than a streaming implementation."
- Watchdog quirk: exits ~5 s after events stop flowing — needs a process supervisor.
- Hardware needs: not stated; it's a file-tailing fan-out server, budget ~1–2 cores + a few GB RAM on the same box (assumption, labeled).

### 1.5 Upgrades

- The visor "spawns and manages the child node process" and "will verify `hl-node` automatically and will not upgrade on verification failure" — i.e. **hl-visor auto-downloads and hot-swaps node binaries**; gpg verification gates the swap.
- **Release cadence and miss-an-upgrade behavior are not documented, and not published by the community either** — GitHub issue #112 shows operators asking for version-tracking that doesn't exist (§3.7). This unpredictability is the dominant ops risk (§6); fork-off-vs-stall behavior is *not found*.

### 1.6 Disk management

- Official: "With default settings, the network will generate around **100 GB of logs per day**, so it is recommended to archive or delete old files." Note this is **default settings** — our flags add raw book diffs + order statuses on top ("substantial amount of data" ×2). Measured/community GB/day with our flags: **not found** — no operator has published a figure with these flags on (§3.7). Planning number: 150–300 GB/day all-coins (labeled assumption).
- Official pruner (repo `pruner/`): cron `0 3 * * *` → `prune.sh` deletes files in `~/hl/data` older than **48 h** (excludes `visor_child_stderr`). So the official operating model is: **~2 days hot retention on local disk, archive-out anything you want to keep** (for us: R2, see §4).
- State snapshots every 10k blocks land in `periodic_abci_states/{date}/{height}.rmp` (these are what pruning keeps bounded).

---

## 2. Provider path — QuickNode Hypercore gRPC (verified 2026-07-04)

Source: [quicknode.com/docs/hyperliquid/grpc-api](https://www.quicknode.com/docs/hyperliquid/grpc-api) — quotes re-verified first-hand, not just agent-reported.

- **Gating:** "Access to `/hypercore` (JSON-RPC and WebSocket) and HyperCore gRPC streaming requires a **Quicknode Build plan or higher**." (This matches our free-trial `PERMISSION_DENIED` at stream-open.) Build = **$49/mo** ($42 annual), 80M credits included, overage **$0.62/1M credits** ([pricing](https://www.quicknode.com/pricing)).
- **Metering — the 6× trap:** standard methods (Ping, StreamBlocks, StreamData) bill "0.1 MB = 10 API credits" (100 credits/MB); **OrderBook methods (incl. StreamL4Book) bill "0.0165 MB = 10 API credits" ≈ 606 credits/MB — 6.06× the standard rate.**
  - Build's 80M credits ≈ **~132 GB/mo** of L4 stream; overage ≈ **$0.38/GB**.
- **Open question (swings burn ~3×):** whether the meter bills compressed (zstd on channel, ~70% reduction claimed in their examples) or uncompressed bytes — **not documented** on the grpc-api or pricing pages (checked directly). Plan of record (CTO): approve the $49 Build upgrade and **measure** — 10 min/coin, client-side `bytes_received` counted both raw and zstd — rather than estimate.
- Streams/limits: Build allows 5 concurrent gRPC streams, 5 named filters — 3 coins fit on one stream via coin filters ([pricing-faq](https://www.quicknode.com/docs/hyperliquid/pricing-faq)).
- Plan ladder if burn exceeds Build: Accelerate $249/450M cr (~742 GB), Scale $499/950M (~1.57 TB), Business $999/2B (~3.3 TB).

**Competing L4 providers (for the table):**

| Provider | Offer | Price | Note |
|---|---|---|---|
| Hydromancer | L4 streaming, ~135 ms, marketed at MMs | **$300/mo** Starter (tier-gating of L4 unverified) | [hydromancer.xyz](https://hydromancer.xyz) — newer shop; reliability/track-record unproven, no public SLA found |
| Dwellir | L4 on dedicated cluster (WS+gRPC, unmetered) | **$4,000/mo** | $199 tier is L2-only ([dwellir.com/hyperliquid-orderbook](https://www.dwellir.com/hyperliquid-orderbook)) |
| CoinAPI / 0xArchive | L4 live feed / L4 reconstruction | prices unverified (403/no page) | — |
| Chainstack / Alchemy / dRPC | RPC only, **no L4** | — | Chainstack's own 2026 comparison names QuickNode as the only general RPC shop with Hyperliquid gRPC streaming |

**What NO provider sells:** mempool streaming (`split_client_blocks`) and gossip-priority tuning. If strategy work confirms we need pre-commit order flow, a node is unavoidable regardless of this table.

---

## 3. L4 data-volume model → QuickNode credit burn

**No measured L4 bytes exist yet on our side** (the free-trial probe was rejected at stream-open; 0 bytes received). Until the Build-plan measurement lands, the model below is structural, every input labeled:

- Wire structure ([vendored `protos/orderbook.proto`]): `L4BookDiff = {time: uint64, height: uint64, data: string}` where `data` is JSON `{order_statuses, book_diffs}` → per-block wire ≈ JSON payload + ~20 B framing.
- Element sizes (official verbatim examples): one `book_diffs` element ≈ **170–220 B** JSON; one snapshot `L4Order` ≈ **250–350 B** JSON.
- Our measured activity anchors (60-min capture, 2026-07-03): fills/s BTC 4.51 / ETH 1.86 / SOL 2.23; BBO updates/s 7.10 / 6.42 / 5.79; blocks ~1.5–2/s; books ~700–2,000 resting orders/coin. Scaling: BTC ≈ 2× SOL, ETH ≈ 0.85× SOL.
- The unknown multiplier: L4 order events (place/cancel/modify) per fill. Crypto perp books typically run **10–50× more order events than fills** (assumption — to be replaced by the Build-plan measurement; community disk numbers turned out to be *not found*, §3.7, so measurement is the only path to the real multiplier).

Worked range, 3 coins, uncompressed JSON:
- Total fills ≈ 8.6/s → order events ≈ 86–430/s → at ~195 B/event ≈ **17–84 KB/s ≈ 1.4–7.2 GB/day ≈ 44–220 GB/mo** (+ snapshots on reconnect, negligible at steady state).
- **If metered uncompressed:** 44–220 GB/mo ≈ 27M–133M credits ≈ fits Build (80M) only at the low end; overage $0–33/mo at Build → realistic bill **$49–85/mo**.
- **If metered zstd-compressed (~70% off):** 13–66 GB/mo ≈ 8M–40M credits → comfortably inside Build → **$49/mo flat**.
- Pathological tails (event storms, 100×+ multipliers) blow past Build; ladder cell: Accelerate $249 covers ~742 GB/mo.

**Bracketed conclusion for the TCO table: QuickNode-only ≈ $49–$249/mo, most-likely $49–85/mo — pending direct measurement under the stated assumptions above.**

---

## 3.5 AWS ap-northeast-1 pricing (fetched 2026-07-04 from AWS's own bulk pricing files)

Sources: on-demand — [ec2.shop](https://ec2.shop?region=ap-northeast-1) (mirrors AWS API); Savings Plans — [AWS savingsPlan bulk file, version 2026-06-30](https://pricing.us-east-1.amazonaws.com/savingsPlan/v1.0/aws/AWSComputeSavingsPlan/current/region_index.json); EBS — [calculator.aws ebs.json](https://calculator.aws/pricing/2.0/meteredUnitMaps/ec2/USD/current/ebs.json); data transfer — [AWSDataTransfer offer file, version 2026-06-25](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/region_index.json).

### Instances (Linux, ap-northeast-1, $/hr → $/mo at 730h)

| Instance | vCPU/RAM | Storage | On-demand | 1-yr No-Up **Compute SP** | 1-yr No-Up **EC2 Instance SP** |
|---|---|---|---|---|---|
| **r7i.4xlarge** (= official spec) | 16 / 128 GB | EBS only | $1.2768 → $932 | $0.92504 → **$675** | $0.8446 → $617 |
| **i4i.4xlarge** (spec + local NVMe) | 16 / 128 GB | **1× 3.75 TB NVMe** | $1.61 → $1,175 | $1.241 → **$906** | $1.049 → $766 |
| m7i.4xlarge (RAM below spec) | 16 / 64 GB | EBS only | $1.0416 → $760 | $0.78895 → $576 | $0.69017 → $504 |
| m7i.8xlarge | 32 / 128 GB | EBS only | $2.0832 → $1,521 | $1.5779 → $1,152 | $1.38033 → $1,008 |
| r7i.8xlarge | 32 / 256 GB | EBS only | $2.5536 → $1,864 | $1.85008 → $1,351 | $1.68921 → $1,233 |
| i4i.8xlarge | 32 / 256 GB | 2× 3.75 TB NVMe | $3.221 → $2,351 | $2.482 → $1,812 | $2.098 → $1,532 |
| i7ie.3xlarge (CPU below spec) | 12 / 96 GB | 1× 7.5 TB NVMe | $1.8354 → $1,340 | $1.4308 → $1,044 | $1.2529 → $915 |

### EBS (Tokyo)

- gp3: **$0.096/GB-mo**; extra IOPS above 3,000 free: $0.006/IOPS-mo; extra throughput above 125 MB/s free: $49.152/GiBps-mo.
  - 1 TB gp3 @ 500 MB/s + 10k IOPS ≈ 98 + 42 + 18 ≈ **$158/mo**; 2 TB same perf ≈ $256/mo.
- io2: $0.142/GB-mo + $0.074/IOPS-mo (tiering down above 32k) — overkill here; gp3 or local NVMe wins.

### Network egress (Tokyo → internet) — the wildcard

Tiered: **$0.114/GB first 10 TB/mo**, $0.089 next 40 TB, $0.086 next 100 TB, $0.084 above; first 100 GB/mo free (global free tier). Same-region transfer (inter-AZ or via public IP within ap-northeast-1) is **$0.01/GB each direction** — relevant because validators/peers cluster IN AWS Tokyo, so some gossip may bill at 1¢ not 11.4¢.

| Gossip egress/mo | Monthly egress bill |
|---|---|
| 2 TB | ~$222 |
| 5 TB | ~$572 |
| 10 TB | ~$1,156 |
| 20 TB | ~$2,067 |
| 50 TB | ~$4,801 |

Real node egress TB/mo: **not publicly reported** (see §3.7 — checked operator guides, GitHub, community; genuine "not found"). Structural reasoning to bound it, labeled assumption: a non-validating node is a **leaf data consumer**, not a relay. Its heavy traffic is *inbound* (receiving the block stream that produces ~100 GB/day of logs ≈ ~3 TB/mo in), and **inbound is free on AWS**. *Outbound* (the billed direction) for a private node is just gossip overhead to 8 peers + keepalives — our `order_book_server` consumer is localhost, so it generates zero WAN egress. **If we do not serve data to external clients, AWS egress is plausibly 1–3 TB/mo (~$115–345), not the 10–50 TB catastrophe** — the earlier worry only materializes if the node fans data out over the WAN. This flips the analysis: egress is a *bounded* risk on AWS for our private use, and bare-metal's included 20 TB removes even that. Still worth measuring on day 1 (same measure-first discipline as QuickNode).

### AWS option totals (instance + disk, before egress)

- **i4i.4xlarge, 1-yr Compute SP: ~$915/mo** (local NVMe covers 48-h retention with big headroom; + ~$10 root gp3)
- **r7i.4xlarge, 1-yr Compute SP + 1 TB fast gp3: ~$833/mo**
- On-demand versions: $1,333 / $1,090 respectively.

## 3.6 Bare-metal Tokyo pricing (fetched 2026-07-05)

Source: [latitude.sh/pricing](https://www.latitude.sh/pricing), [latitude.sh/pricing/networking](https://www.latitude.sh/pricing/networking), [vultr.com/pricing](https://www.vultr.com/pricing/). Latitude confirmed to operate **Tokyo (3 DC locations)** ([datacenters.com/providers/latitude-sh](https://www.datacenters.com/providers/latitude-sh)); per-plan Tokyo live stock not verified from the pricing page — confirm in dashboard before commit `[not verified]`.

| Provider / plan | CPU | RAM | NVMe | Included egress | Monthly $ |
|---|---|---|---|---|---|
| **Latitude m4.metal.medium** | AMD 9124, 16c @ 3 GHz | **128 GB** | 2× 1.9 TB | **20 TB out / ∞ in** | **$456** |
| Latitude f4.metal.medium | AMD 4564P, 16c @ 4.5 GHz | 128 GB | 2× 1.9 TB | 20 TB out / ∞ in | $555 |
| Latitude m4.metal.large | AMD 9254, 24c @ 2.9 GHz | 384 GB | 2× 3.8 TB | 20 TB out / ∞ in | $1,482 |
| Vultr Bare Metal (Tokyo) | Xeon/EPYC, varies | varies | NVMe | pooled; **Tokyo overage $0.05/GB** | from $120 (entry); spec-match ~$300–600 `[not verified exact]` |

- **m4.metal.medium matches the official node spec (16 core / 128 GB / fast NVMe) almost exactly, at $456/mo, with 20 TB/mo egress included** — the same 20 TB costs ~$2,067 on AWS. Latitude's Japan overage beyond 20 TB is **$5.40/TB** (i.e. $0.0054/GB, ~21× cheaper than AWS's $0.114/GB) ([pricing/networking](https://www.latitude.sh/pricing/networking)).
- Proximity: bare-metal in a Tokyo DC is a few ms from AWS ap-northeast-1 (where validators run) vs sub-ms in-AWS — a small latency give-up for a large cost saving. No provider advertised direct AWS-Tokyo peering explicitly `[not verified]`.
- OVH: has APAC but **no confirmed Tokyo bare-metal** in this pass `[not verified]`. Equinix Metal: **sunset** (deprecated), skip.

## 3.7 Community-reported operational reality (lane 4) — what's public vs "not found"

Method: searched operator guides, GitHub issues, provider blogs (2026-07-05). Labeled [official]/[community]/**not found**.

- **Disk GB/day with heavy flags:** [official] ~100 GB/day at *default* settings; the two flags we need are each documented only as "a substantial amount of data." **No operator has published a GB/day figure with `--write-raw-book-diffs` + `--write-order-statuses` on** — genuine *not found*. Planning number stays **150–300 GB/day all-coins** (labeled assumption). Sources: [node README](https://github.com/hyperliquid-dex/node), [operator guide (Manuel/Medium)](https://medium.com/@manuelbagoole/demystifying-hyperliquid-how-to-run-a-node-without-losing-your-mind-06e1c12b216c).
- **Gossip egress TB/month:** **not found** in any public source — the single most-wanted number is unpublished. Bounded structurally in §3.5 instead.
- **Initial sync mechanism/time:** **not found** — docs and guides only say it "may take a while… `applied block X` indicates live." No S3-snapshot bucket or wall-clock time published.
- **Upgrade cadence & miss-behavior:** [community] GitHub issue [#112](https://github.com/hyperliquid-dex/node/issues/112) (Sep 2025) — operators explicitly **ask for GitHub releases + a way to monitor hl-node/hl-visor versions**, i.e. version tracking is opaque and there is no published cadence. [official] hl-visor "will not upgrade on verification failure." Fork-off vs stall on a missed upgrade: **not found** (documented behavior absent). Practical read: **cadence is unpredictable and unannounced → this is the main ops risk** (see §6).
- **Latency:** [community, Dwellir] gossip-priority ≈ "10 ms reduction per slot" (slot 0 first, slot 4 last); order-priority ≈ "45 ms reduction per 1 bp" of priority fee, up to ~360 ms at max 8 bp; block finality "median 0.2 s, p99 < 0.5 s"; mempool via `split_client_blocks` gives "hundreds of milliseconds" of pre-finality visibility ([dwellir.com/blog/hyperliquid-priority-fees](https://www.dwellir.com/blog/hyperliquid-priority-fees)). Direct node-local-vs-QuickNode-vs-public-WS millisecond comparison: **not found** as a published measurement — our own measure-first plan will produce it.
- **Published node cost breakdowns:** **not found** (no operator has posted a real monthly bill).

## 4. Self-hosted data volumes and archive knock-on

- Node writes **all coins** (no filter): official baseline ~100 GB/day default settings; the two flags add "substantial" data each but **no operator has published a combined GB/day** (§3.7, *not found*). Planning number: **150–300 GB/day all-coins** (labeled assumption: raw diffs + statuses comparable to or larger than default logs; resolve by measuring on deploy).
- Official pruner keeps 48 h hot → **local disk needs ≈ 2 × daily rate + state + headroom ≈ 500 GB–1 TB NVMe**, matching the official 500 GB spec only if pruning runs reliably; 1 TB is the safe size with our flags. Latitude's 2×1.9 TB and AWS i4i's 3.75 TB NVMe both clear this comfortably.
- Our 3-coin extract archived to R2: scaled by activity share, order 5–20 GB/day compressed `[assumption; refine with real ratio]`.
- R2 pricing (verified [developers.cloudflare.com/r2/pricing](https://developers.cloudflare.com/r2/pricing/), 2026-07-04): standard **$0.015/GB-mo**, infrequent access $0.01/GB-mo, **egress free**, Class A ops $4.50/M. At 300 GB/mo archive growth the 6-mo cumulative bill is ~$50 total — noise.
- **Knock-on that isn't noise:** pushing the archive OUT of AWS bills as internet egress ($0.114/GB) → 300 GB/mo ≈ +$34/mo on AWS; $0 extra on bare-metal with included bandwidth (another point for bare-metal if egress reports come back high).

---

## 5. TCO — 6-month table

Ranges reflect the labeled egress assumption from §3.5 (private leaf node ≈ 1–3 TB/mo billed egress on AWS; bare-metal includes 20 TB so egress is $0 within budget). Prices all cited in §2–§3.6.

| Option | Monthly $ | 6-mo TCO | Latency | Mempool / gossip-tuning? | Notes |
|---|---|---|---|---|---|
| **Bare-metal — Latitude m4.metal.medium (Tokyo)** | **$456** | **$2,736** | few ms from AWS-Tokyo validators | ✅ | spec-match; 20 TB egress included; overage $5.40/TB; **cheapest node path** |
| AWS r7i.4xlarge 1-yr SP + 1 TB gp3 + egress | $948–1,178 | $5,688–7,068 | best (in-region, sub-ms) | ✅ | egress 1–3 TB/mo assumed; blows up only if we fan data out over WAN |
| AWS i4i.4xlarge 1-yr SP (local NVMe) + egress | $1,030–1,260 | $6,180–7,560 | best | ✅ | 3.75 TB NVMe included |
| QuickNode Build (+ measured overage) | $49–249 (ML $49–85) | $294–1,494 (ML $294–510) | measure vs our ~370 ms WS baseline | ❌ | no mempool, no gossip-priority tuning |
| Hydromancer | $300 | $1,800 | ~135 ms claimed | ❌ | unproven shop, no SLA found |
| Dwellir dedicated | $4,000 | $24,000 | colo option | ❌ | unmetered L4, whale-priced |

**Node vs provider is not apples-to-apples:** the node paths (top three) deliver capability the provider paths structurally cannot — mempool streaming (`split_client_blocks`) and gossip-auction priority tuning. QuickNode/Hydromancer/Dwellir only deliver *committed* L4 data.

## 6. Ops burden

- **Upgrade risk is the dominant burden.** Binary cadence is unannounced and unmonitorable from upstream (GitHub issue #112, §3.7); hl-visor auto-pulls and gpg-verifies, but there's no published SLA on how fast you must adopt or what happens if you lag. Mitigation: pin a version-watch (poll the binaries endpoint / a community version feed), alert on drift. **Estimate 1–3 hrs/week** steady-state babysitting, spikier around upgrades — labeled estimate, no operator has published actual hours (§3.7 *not found*).
- **Supervision plumbing (all documented facts):** systemd `Restart=always`/`RestartSec=10` for hl-visor; the official pruner cron (03:00 daily, 48 h retention) must be confirmed running or disk fills at ~150–300 GB/day; `order_book_server` self-exits ~5 s after events stop, so it needs its own supervisor + alert.
- **Failure modes:** falling behind the chain (recovery mechanism undocumented — likely resync, time *not found*); disk-full if pruner/cron breaks; missed upgrade → possible stall (fork-off behavior *not found*). None are turnkey; all are standard-SRE manageable but **not zero-touch**.
- **Provider paths (QuickNode/Hydromancer) ops burden ≈ 0** — that's their real value at small scale.

## 7. Recommendation

**Now (capital-constrained, decision deferred): stay on QuickNode.** When capital allows spending ~$50/mo on data, buy **QuickNode Build ($49/mo)** and immediately run the measure-first pass (10 min/coin, client-side raw + zstd byte counts) — it delivers L4 data for queue modeling *and* resolves the two open unknowns (real L4 GB/mo → true credit burn; the compressed-vs-uncompressed metering question) for the price of one month. Most-likely steady-state $49–85/mo. This is the lowest-capital way to unblock the L4Book work.

**Later, when the node's unique capability is needed** (i.e. when strategy work confirms we want mempool/pre-commit order flow and priority-fee tuning — which *no provider sells*): **run our own node on Latitude m4.metal.medium in Tokyo, $456/mo.** It is the cheapest node path, matches spec, and its included 20 TB egress eliminates AWS's single biggest cost wildcard. Choose AWS only if sub-ms in-region latency proves to matter more than the ~$400–800/mo premium — a question our own latency measurement (node vs QuickNode vs public WS) should decide, not a guess.

**Do not** pay for Hydromancer or Dwellir: Hydromancer is unproven with no SLA, and Dwellir's $4k/mo only makes sense at institutional scale.

**One-line for the capital plan:** $0 today → $49/mo (QuickNode Build, measure-first) when the L4 work is funded → $456/mo (Latitude Tokyo node) only when mempool/priority-fee capability becomes strategy-critical.

---
*Open items carried forward (all labeled in-text): real L4 GB/mo + QuickNode metering basis (resolved by the Build measurement); node egress TB/mo, disk GB/day with heavy flags, sync time, upgrade cadence/fork behavior (resolved by running a node and by continued community monitoring). None block the recommendation above.*
