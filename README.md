# Hyperliquid Data Collection Pipeline

A comprehensive data collection and processing pipeline for Hyperliquid mainnet data, designed for backtesting and automated trading strategies.

## Features

### Data Collection
- **Historical Data**: Downloads from Hyperliquid S3 archives (`s3://hyperliquid-archive/`)
  - L2 orderbook snapshots
  - Trade data 
  - Asset contexts
  - Node fills and transactions
- **Real-time Data**: WebSocket subscriptions for live market data
  - Orderbook updates
  - Trade feeds
  - Ticker data
  - User events

### Data Processing
- **OHLCV Generation**: Converts trade data to candlestick data for multiple timeframes
- **Technical Indicators**: RSI, moving averages, Bollinger Bands, momentum indicators
- **Orderbook Metrics**: Spread analysis, depth calculations, imbalance detection
- **Data Validation**: Quality checks, outlier detection, data sanitization

### Storage Options
- **PostgreSQL**: Structured data storage with full SQL capabilities
- **InfluxDB**: Time-series optimized storage for high-frequency data
- **Redis**: Fast caching layer for real-time data access
- **Parquet Files**: Compressed columnar storage for historical data

### Automation & Monitoring
- **Scheduled Jobs**: Automated daily historical data collection
- **Data Quality Reports**: Regular validation and quality metrics
- **Health Monitoring**: System status checks and alerting
- **Graceful Error Handling**: Automatic reconnection and error recovery

## Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <repository-url>
cd hype

# Install dependencies
pip install -r requirements.txt

# Or using Poetry
poetry install
```

### 2. Configuration

```bash
# Generate sample configuration
python scripts/run_pipeline.py generate-config

# Edit the .env file with your settings
cp .env.example .env
vim .env
```

### 3. AWS Configuration (for historical data)

You need AWS credentials to download historical data from Hyperliquid's S3 bucket:

```bash
# Configure AWS CLI
aws configure
# Enter your AWS access key ID and secret access key
# Set region to us-east-1
# Set output format to json

# Note: You will be charged for data transfer costs as a requester
```

### 4. Database Setup (Optional)

For full functionality, set up the databases:

```bash
# PostgreSQL
docker run -d --name hyperliquid-postgres \
  -e POSTGRES_DB=hyperliquid_data \
  -e POSTGRES_USER=hyperliquid \
  -e POSTGRES_PASSWORD=your_password \
  -p 5432:5432 postgres:15

# InfluxDB
docker run -d --name hyperliquid-influxdb \
  -p 8086:8086 \
  influxdb:2.7

# Redis
docker run -d --name hyperliquid-redis \
  -p 6379:6379 redis:7
```

### 5. Run the Pipeline

```bash
# Start the full pipeline
python scripts/run_pipeline.py start

# Or collect historical data only
python scripts/run_pipeline.py collect-historical \
  --symbols BTC,ETH,SOL \
  --start-date 2024-01-01 \
  --end-date 2024-01-07

# Test real-time collection
python scripts/run_pipeline.py test-realtime \
  --symbols BTC,ETH \
  --duration 60
```

## Project Structure

```
hype/
├── src/hyperliquid_pipeline/
│   ├── collectors/           # Data collection modules
│   │   ├── historical_collector.py    # S3 historical data
│   │   └── realtime_collector.py      # WebSocket real-time data
│   ├── processors/          # Data processing modules
│   │   └── data_processor.py          # OHLCV, indicators, metrics
│   ├── storage/             # Storage backends
│   │   └── database.py                # PostgreSQL, InfluxDB, Redis
│   ├── scheduler/           # Orchestration and scheduling
│   │   └── orchestrator.py            # Main pipeline controller
│   ├── utils/               # Utilities
│   │   └── validation.py              # Data validation and quality
│   └── config/              # Configuration
│       └── settings.py                # Settings management
├── data/                    # Data storage
│   ├── historical/          # Historical data files
│   ├── realtime/           # Real-time data logs
│   └── processed/          # Processed data files
├── logs/                   # Log files
├── scripts/                # CLI scripts
│   └── run_pipeline.py     # Main CLI interface
├── tests/                  # Test files
└── config/                 # Configuration files
```

## Usage Examples

### Historical Data Collection

```python
from hyperliquid_pipeline.collectors import HistoricalDataCollector

collector = HistoricalDataCollector()

# Download last 7 days of data
data = await collector.download_historical_data(
    symbols=['BTC', 'ETH', 'SOL'],
    start_date='2024-01-01',
    end_date='2024-01-07',
    data_types=['l2Book', 'trades']
)

# Save to parquet files
collector.save_to_parquet(data, Path('./data/processed'))
```

### Real-time Data Collection

```python
from hyperliquid_pipeline.collectors import HyperliquidWebSocketCollector

collector = HyperliquidWebSocketCollector(['BTC', 'ETH'])

# Add callback for processing data
def process_data(data_point):
    print(f"Received {data_point.data_type} for {data_point.symbol}")

collector.add_data_callback(process_data)

# Start collection
await collector.start_with_reconnect()
```

### Data Processing

```python
from hyperliquid_pipeline.processors import DataProcessor
from hyperliquid_pipeline.storage import MultiStorage, RedisStorage, InfluxDBStorage

# Set up storage
redis_storage = RedisStorage()
influx_storage = InfluxDBStorage()
storage = MultiStorage([redis_storage, influx_storage])

# Create processor
processor = DataProcessor(storage)

# Process market data
await processor.process_market_data(market_data_point)
```

## Available Data Types

### Historical Data (S3)
- **L2 Orderbook** (`l2Book`): Full orderbook snapshots
- **Trades** (`trades`): All executed trades
- **Asset Contexts** (`asset_ctxs`): Market metadata and pricing info
- **Node Fills** (`node_fills`): Detailed fill information

### Real-time Data (WebSocket)
- **Orderbook Updates**: Live L2 book changes
- **Trade Feeds**: Real-time trade execution data
- **Ticker Data**: Mid prices and market summaries
- **User Events**: Account-specific notifications (if configured)

### Processed Data
- **OHLCV**: Candlestick data for multiple timeframes (1m, 5m, 15m, 1h, 4h, 1d)
- **Technical Indicators**: SMA, EMA, RSI, Bollinger Bands
- **Orderbook Metrics**: Spread, depth, imbalance, price levels
- **Trade Analytics**: Volume profiles, price momentum, trade flow

## Configuration Options

Key configuration variables in `.env`:

```bash
# Data Collection
COLLECT_SYMBOLS=BTC,ETH,SOL,ARB,AVAX,MATIC,OP,LTC,LINK,UNI
REAL_TIME_ENABLED=true
HISTORICAL_START_DATE=2023-01-01

# Storage
POSTGRES_URL=postgresql://user:pass@localhost:5432/hyperliquid_data
INFLUXDB_URL=http://localhost:8086
REDIS_URL=redis://localhost:6379

# AWS (for historical data)
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret

# Logging
LOG_LEVEL=INFO
LOG_ROTATION=1 day
LOG_RETENTION=30 days
```

## Data Quality & Validation

The pipeline includes comprehensive data validation:

- **Price Validation**: Checks for reasonable price changes and valid price levels
- **Volume Validation**: Detects volume spikes and anomalies
- **Timestamp Validation**: Ensures data freshness and prevents duplicates
- **Orderbook Validation**: Verifies proper price ordering and spread consistency
- **Quality Metrics**: Completeness, accuracy, and freshness scores

Access quality reports:

```python
from hyperliquid_pipeline.utils import DataValidator

validator = DataValidator()
report = validator.generate_quality_report(['BTC', 'ETH'])
print(report)
```

## Monitoring & Alerting

- **Health Checks**: Regular system status monitoring
- **Performance Metrics**: Message throughput, error rates, uptime
- **Quality Reports**: Data completeness and accuracy tracking
- **Log Aggregation**: Structured logging with rotation and retention

## Performance Considerations

- **Concurrent Downloads**: Configurable workers for historical data collection
- **Memory Management**: Sliding window buffers for real-time data
- **Storage Optimization**: Multiple storage backends for different use cases
- **Connection Pooling**: Efficient database connection management
- **Data Compression**: LZ4 decompression for historical archives

## Cost Considerations

- **AWS Data Transfer**: You pay for data transfer from S3 (requester pays)
- **Storage Costs**: Local storage for historical data can grow quickly
- **Database Resources**: InfluxDB and PostgreSQL require adequate resources
- **Bandwidth**: Real-time WebSocket connections consume bandwidth

## Troubleshooting

### Common Issues

1. **AWS Permissions**: Ensure your AWS credentials have S3 access
2. **Database Connections**: Check that databases are running and accessible  
3. **WebSocket Disconnections**: The pipeline automatically reconnects
4. **Disk Space**: Monitor available space for data storage
5. **Memory Usage**: Large historical downloads may require sufficient RAM

### Logs

Check logs for detailed error information:

```bash
# Main pipeline logs
tail -f logs/pipeline.log

# Quality reports
ls logs/quality_reports/

# Health status
ls logs/health/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This software is for educational and research purposes. Use at your own risk. The authors are not responsible for any financial losses incurred through the use of this software.

## Support

For questions and support:
- Check the logs for error details
- Review the configuration settings
- Ensure all dependencies are properly installed
- Verify database and AWS connectivity