# hyperliquid-data-pipeline

[![tests](https://github.com/Giri-Aayush/hyperliquid-data-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/Giri-Aayush/hyperliquid-data-pipeline/actions/workflows/tests.yml)
![python](https://img.shields.io/badge/python-3.11%2B-blue)
![license](https://img.shields.io/badge/license-MIT-green)

Collects market data from Hyperliquid — both live over WebSocket and historical from the S3 archive — turns it into OHLCV candles, indicators, and orderbook metrics, and writes it to whichever store you point it at. I built it to feed my own backtests.

```
 WebSocket (live) ─┐
                   ├─▶  collect ──▶ process ──▶ validate ──▶  PostgreSQL · InfluxDB · Redis · Parquet
 S3 archive (past) ─┘                  │
                                       └── OHLCV · RSI/EMA/Bollinger · spread, depth, imbalance
```

## Try it in 30 seconds

No keys, no databases. Connects to the public WebSocket and prints live BTC data:

```bash
git clone https://github.com/Giri-Aayush/hyperliquid-data-pipeline.git
cd hyperliquid-data-pipeline
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python scripts/run_pipeline.py test-realtime --symbols BTC --duration 30
```

```
| INFO | Connecting to wss://api.hyperliquid.xyz/ws
| INFO | WebSocket connected successfully
| INFO | Sent 3 subscriptions
| INFO | Received orderbook for BTC
| INFO | Received trade for BTC
| INFO | Received ticker for BTC
```

## What's in it

| Module | Does |
|---|---|
| `collectors/realtime_collector.py` | WebSocket feeds — l2Book, trades, mids, user events — with reconnect and bounded buffers |
| `collectors/historical_collector.py` | Pulls L2 snapshots and trades from the `hyperliquid-archive` S3 bucket (LZ4, requester-pays) |
| `processors/data_processor.py` | Trades → OHLCV with VWAP; SMA/EMA/RSI/Bollinger; orderbook spread (bps), depth, imbalance |
| `utils/validation.py` | Crossed books, bad sort order, price jumps, volume spikes, stale and duplicate points |
| `storage/database.py` | One interface, four backends: PostgreSQL (asyncpg), InfluxDB, Redis, Parquet. Use any subset |
| `scheduler/orchestrator.py` | APScheduler jobs for daily history pulls and quality reports; clean shutdown on signals |

## Running the full thing

The demo above needs nothing. Beyond that there are two optional pieces.

**History from S3.** The archive bucket is requester-pays, so the transfer shows up on your AWS bill. Put `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in `.env`, then:

```bash
.venv/bin/python scripts/run_pipeline.py collect-historical \
  --symbols BTC,ETH --start-date 2024-01-01 --end-date 2024-01-07
```

**Databases.** With nothing configured it writes JSONL and Parquet under `data/`. To use the servers:

```bash
docker run -d --name hl-postgres -e POSTGRES_DB=hyperliquid_data \
  -e POSTGRES_USER=hyperliquid -e POSTGRES_PASSWORD=change_me -p 5432:5432 postgres:15
docker run -d --name hl-influx -p 8086:8086 influxdb:2.7
docker run -d --name hl-redis  -p 6379:6379 redis:7
```

Copy `.env.example` to `.env`, fill in what you actually run, then:

```bash
.venv/bin/python scripts/run_pipeline.py start
```

## Layout

```
src/hyperliquid_pipeline/
├── collectors/    realtime (WebSocket) + historical (S3)
├── processors/    OHLCV, indicators, orderbook metrics
├── storage/       PostgreSQL, InfluxDB, Redis, Parquet
├── scheduler/     APScheduler orchestration
├── utils/         validation, quality reports
└── config/        pydantic-settings (.env)
scripts/run_pipeline.py   CLI: start · collect-historical · test-realtime
tests/                    unit tests, no network
```

## Tests

```bash
.venv/bin/python -m pytest tests/ -v
```

Cover the parts where the math has to be right: OHLCV and VWAP, orderbook spread/depth/imbalance against numbers worked out by hand, RSI and Bollinger edge cases, the validator's error paths, and WebSocket parsing against real Hyperliquid payloads. Nothing in the suite touches the network.

## Worth knowing

- The l2Book feed sends full snapshots per update, not deltas — metrics are computed per snapshot.
- S3 history costs real money (requester-pays). Start with a few days.
- Indicators run over in-memory history, so restarting the process resets their state.
- Hyperliquid only. The collectors are written against its API on purpose.

## License

MIT. Research tooling, not trading advice.
