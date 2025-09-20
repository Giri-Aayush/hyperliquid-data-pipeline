#!/usr/bin/env python3
"""CLI script to run the Hyperliquid data pipeline."""

import asyncio
import sys
from pathlib import Path
import typer
from loguru import logger

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hyperliquid_pipeline.scheduler.orchestrator import DataPipelineOrchestrator
from hyperliquid_pipeline.config import settings

app = typer.Typer(help="Hyperliquid Data Collection Pipeline")


@app.command()
def start(
    symbols: str = typer.Option(
        None,
        "--symbols",
        "-s", 
        help="Comma-separated list of symbols to collect (default: from config)"
    ),
    realtime: bool = typer.Option(
        True,
        "--realtime/--no-realtime",
        help="Enable real-time data collection"
    ),
    historical: bool = typer.Option(
        True,
        "--historical/--no-historical", 
        help="Enable historical data collection"
    ),
    log_level: str = typer.Option(
        "INFO",
        "--log-level",
        "-l",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
):
    """Start the data collection pipeline."""
    
    # Configure logging
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
    # Add file logging
    log_file = settings.logs_path / "pipeline.log"
    logger.add(
        log_file,
        level=log_level,
        rotation="1 day",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )
    
    # Override symbols if provided
    if symbols:
        settings.collect_symbols = [s.strip() for s in symbols.split(",")]
    
    # Override settings
    settings.real_time_enabled = realtime
    
    logger.info(f"Starting pipeline with symbols: {settings.collect_symbols}")
    logger.info(f"Real-time enabled: {realtime}")
    logger.info(f"Historical enabled: {historical}")
    
    async def run_pipeline():
        orchestrator = DataPipelineOrchestrator()
        try:
            await orchestrator.initialize()
            await orchestrator.start()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
        finally:
            await orchestrator.stop()
    
    # Run the pipeline
    asyncio.run(run_pipeline())


@app.command()
def collect_historical(
    symbols: str = typer.Option(
        "BTC,ETH,SOL", 
        "--symbols",
        "-s",
        help="Comma-separated list of symbols"
    ),
    start_date: str = typer.Option(
        None,
        "--start-date",
        help="Start date (YYYY-MM-DD)"
    ),
    end_date: str = typer.Option(
        None,
        "--end-date", 
        help="End date (YYYY-MM-DD)"
    ),
    data_types: str = typer.Option(
        "trades,l2Book",
        "--data-types",
        help="Comma-separated data types"
    ),
    workers: int = typer.Option(
        4,
        "--workers",
        "-w",
        help="Number of concurrent workers"
    )
):
    """Collect historical data only."""
    
    from hyperliquid_pipeline.collectors.historical_collector import HistoricalDataCollector
    from datetime import datetime, timedelta
    
    # Configure logging
    logger.remove()
    logger.add(sys.stdout, level="INFO")
    
    # Parse inputs
    symbol_list = [s.strip() for s in symbols.split(",")]
    data_type_list = [d.strip() for d in data_types.split(",")]
    
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    if not end_date:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    async def collect_data():
        collector = HistoricalDataCollector()
        
        logger.info(f"Collecting data for {symbol_list} from {start_date} to {end_date}")
        
        data = await collector.download_historical_data(
            symbols=symbol_list,
            start_date=start_date,
            end_date=end_date,
            data_types=data_type_list,
            max_workers=workers
        )
        
        # Save to parquet
        output_dir = settings.historical_data_path / "manual_collection" / f"{start_date}_to_{end_date}"
        collector.save_to_parquet(data, output_dir)
        
        logger.info(f"Data saved to {output_dir}")
    
    asyncio.run(collect_data())


@app.command()
def test_realtime(
    symbols: str = typer.Option(
        "BTC,ETH",
        "--symbols", 
        "-s",
        help="Comma-separated list of symbols"
    ),
    duration: int = typer.Option(
        60,
        "--duration",
        "-d",
        help="Test duration in seconds"
    )
):
    """Test real-time data collection."""
    
    from hyperliquid_pipeline.collectors.realtime_collector import HyperliquidWebSocketCollector
    
    # Configure logging
    logger.remove()
    logger.add(sys.stdout, level="INFO")
    
    symbol_list = [s.strip() for s in symbols.split(",")]
    
    async def test_realtime_collection():
        collector = HyperliquidWebSocketCollector(symbol_list)
        
        # Add simple logging callback
        def log_data(data_point):
            logger.info(f"Received {data_point.data_type} for {data_point.symbol}")
        
        collector.add_data_callback(log_data)
        
        logger.info(f"Testing real-time collection for {symbol_list} for {duration} seconds")
        
        # Start collection with timeout
        try:
            await asyncio.wait_for(
                collector.start_with_reconnect(),
                timeout=duration
            )
        except asyncio.TimeoutError:
            logger.info("Test completed")
        
        # Print stats
        stats = collector.get_stats()
        logger.info(f"Final stats: {stats}")
    
    asyncio.run(test_realtime_collection())


@app.command()
def generate_config():
    """Generate a sample configuration file."""
    
    config_content = """# Hyperliquid Data Pipeline Configuration

# Copy this file to .env and update the values

# Hyperliquid API Configuration
HYPERLIQUID_API_URL=https://api.hyperliquid.xyz
HYPERLIQUID_WALLET_ADDRESS=your_wallet_address_here
HYPERLIQUID_PRIVATE_KEY=your_private_key_here

# AWS Configuration for Historical Data
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1

# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=hyperliquid_data
POSTGRES_USER=hyperliquid
POSTGRES_PASSWORD=your_postgres_password

# InfluxDB Configuration (Time Series)
INFLUXDB_URL=http://localhost:8086
INFLUXDB_TOKEN=your_influxdb_token
INFLUXDB_ORG=hyperliquid
INFLUXDB_BUCKET=market_data

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=your_redis_password
REDIS_DB=0

# Data Collection Settings
COLLECT_SYMBOLS=BTC,ETH,SOL,ARB,AVAX,MATIC,OP,LTC,LINK,UNI
HISTORICAL_START_DATE=2023-01-01
REAL_TIME_ENABLED=true
WEBSOCKET_RECONNECT_DELAY=5

# Monitoring & Alerts
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id

# Logging
LOG_LEVEL=INFO
LOG_ROTATION=1 day
LOG_RETENTION=30 days
"""
    
    config_file = Path(".env")
    config_file.write_text(config_content)
    
    typer.echo(f"Sample configuration written to {config_file}")
    typer.echo("Please update the values in .env before running the pipeline")


@app.command()
def status():
    """Check pipeline status."""
    
    # This would connect to a running pipeline and get status
    # For now, just show configuration
    typer.echo("Pipeline Configuration:")
    typer.echo(f"  Data root path: {settings.data_root_path}")
    typer.echo(f"  Symbols: {settings.collect_symbols}")
    typer.echo(f"  Real-time enabled: {settings.real_time_enabled}")
    typer.echo(f"  Log level: {settings.log_level}")


if __name__ == "__main__":
    app()