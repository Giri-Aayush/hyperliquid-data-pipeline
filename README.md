# Hyperliquid Data Pipeline

Market-data infrastructure for Hyperliquid: historical L2 orderbook and trade ingestion from the official S3 archives, real-time WebSocket feeds, OHLCV and indicator generation, orderbook metrics, and persistence across PostgreSQL, InfluxDB, Redis, and Parquet. Built for backtesting and automated strategy research.

## 30-second demo

No credentials, no databases. This connects to Hyperliquid's public WebSocket and streams live BTC orderbook updates, trades, and tickers:

```bash
git clone https://github.com/Giri-Aayush/hyperliquid-data-pipeline.git
cd hyperliquid-data-pipeline
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python scripts/run_pipeline.py test-realtime --symbols BTC --duration 30
```

You should see orderbook, trade, and ticker messages within a couple of seconds:

```
| INFO | Connecting to wss://api.hyperliquid.xyz/ws
| INFO | WebSocket connected successfully
| INFO | Sent 3 subscriptions
| INFO | Received orderbook for BTC
| INFO | Received trade for BTC
| INFO | Received ticker for BTC
```

## What it does

- **Real-time collection** (`collectors/realtime_collector.py`): WebSocket subscriptions for L2 orderbook updates, trade feeds, mid-price tickers, and user events, with automatic reconnection and sliding-window buffers.
- **Historical collection** (`collectors/historical_collector.py`): downloads L2 book snapshots and trades from the `hyperliquid-archive` S3 bucket (LZ4-compressed, requester pays), with concurrent workers.
- **Processing** (`processors/data_processor.py`): trade-to-OHLCV conversion for multiple timeframes with VWAP, technical indicators (SMA, EMA, RSI, Bollinger Bands), and orderbook metrics (spread in bps, depth at top 5 levels, bid/ask imbalance).
- **Validation** (`utils/validation.py`): price and size sanity checks, crossed-book detection, sort-order verification, price-jump and volume-spike flagging, freshness and duplicate checks.
- **Storage** (`storage/database.py`): pluggable backends behind one interface. PostgreSQL (async via asyncpg), InfluxDB, Redis, and Parquet files. Run any subset; each backend is enabled by its config being present.
- **Scheduling** (`scheduler/orchestrator.py`): APScheduler jobs for daily historical pulls and quality reports, with graceful shutdown on signals.

## Full pipeline setup

The demo above needs nothing. The full pipeline has two optional dependencies:

**Historical data (AWS).** Hyperliquid's archive bucket is requester-pays, so S3 transfer costs land on your AWS account. Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`, then:

```bash
.venv/bin/python scripts/run_pipeline.py collect-historical \
  --symbols BTC,ETH --start-date 2024-01-01 --end-date 2024-01-07
```

**Databases.** Without any database the pipeline writes JSONL and Parquet to `data/`. To enable the server backends:

```bash
docker run -d --name hl-postgres -e POSTGRES_DB=hyperliquid_data \
  -e POSTGRES_USER=hyperliquid -e POSTGRES_PASSWORD=change_me -p 5432:5432 postgres:15
docker run -d --name hl-influx -p 8086:8086 influxdb:2.7
docker run -d --name hl-redis -p 6379:6379 redis:7
```

Copy `.env.example` to `.env`, fill in what you run, and start everything:

```bash
.venv/bin/python scripts/run_pipeline.py start
```

## Project structure

```
src/hyperliquid_pipeline/
├── collectors/    # WebSocket (realtime) and S3 (historical) ingestion
├── processors/    # OHLCV, indicators, orderbook metrics
├── storage/       # PostgreSQL, InfluxDB, Redis, Parquet backends
├── scheduler/     # APScheduler orchestration
├── utils/         # data validation and quality reports
└── config/        # pydantic-settings configuration (.env)
scripts/run_pipeline.py   # CLI: start, collect-historical, test-realtime
tests/                    # unit tests (processors, validation, parsing)
```

## Tests

```bash
.venv/bin/python -m pytest tests/ -v
```

Unit tests cover OHLCV math (including VWAP), orderbook metrics against hand-computed values, RSI and Bollinger edge cases, the validator's error and warning paths, and WebSocket message parsing with realistic Hyperliquid payloads. No test touches the network.

## Limitations

- The WebSocket l2Book feed delivers book snapshots per update, not incremental deltas; metrics are computed per snapshot.
- Historical S3 ingestion costs real money (requester pays). Start with a short date range.
- Indicator computation is rolling-window over in-memory history; restarting the process resets indicator state.
- One exchange. The collectors are Hyperliquid-specific by design.

## License

MIT. This is research tooling, not trading advice; use at your own risk.
