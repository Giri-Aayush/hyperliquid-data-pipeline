"""Configuration settings for the Hyperliquid data pipeline."""

from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path
import os


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Hyperliquid API Configuration
    hyperliquid_api_url: str = Field(default="https://api.hyperliquid.xyz")
    # WebSocket endpoint. Default is the public gateway; point it at a
    # colocated node's websocket (or the order_book_server) to cut the
    # CloudFront + shared-API-server hops.
    hyperliquid_ws_url: str = Field(default="wss://api.hyperliquid.xyz/ws")
    hyperliquid_wallet_address: Optional[str] = None
    hyperliquid_private_key: Optional[str] = None
    
    # AWS Configuration
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_default_region: str = Field(default="us-east-1")
    
    # Database Configuration
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_db: str = Field(default="hyperliquid_data")
    postgres_user: str = Field(default="hyperliquid")
    postgres_password: Optional[str] = None
    
    # InfluxDB Configuration
    influxdb_url: str = Field(default="http://localhost:8086")
    influxdb_token: Optional[str] = None
    influxdb_org: str = Field(default="hyperliquid")
    influxdb_bucket: str = Field(default="market_data")
    
    # Redis Configuration
    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)
    redis_password: Optional[str] = None
    redis_db: int = Field(default=0)
    
    # Data Storage Paths
    data_root_path: Path = Field(default=Path("./data"))
    historical_data_path: Path = Field(default=Path("./data/historical"))
    real_time_data_path: Path = Field(default=Path("./data/realtime"))
    logs_path: Path = Field(default=Path("./logs"))
    
    # Monitoring & Alerts
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    prometheus_port: int = Field(default=8000)
    
    # Data Collection Settings
    # The trading universe is BTC, ETH, SOL PERPETUALS (user decision,
    # 2026-07-04) — capture depth on what we'll trade beats breadth we won't.
    # Bare coin names on the Hyperliquid WS are the perps (spot uses
    # @index/pair identifiers), and activeAssetCtx here is the perp schema.
    collect_symbols: str = Field(default="BTC,ETH,SOL")
    historical_start_date: str = Field(default="2023-01-01")
    real_time_enabled: bool = Field(default=True)
    # Gate for the scheduled/initial historical S3 pulls (the CLI --historical
    # flag sets this). Gap backfill is unaffected — it serves the live archive.
    historical_enabled: bool = Field(default=True)
    # Subscribe to the bbo channel (event-level top-of-book, pushed only when
    # the best bid/ask changes on a block). Cheapest, freshest price signal.
    subscribe_bbo: bool = Field(default=True)
    # Reconnect backoff: full jitter, uniform(0, min(max_delay, delay·2^n)).
    # websocket_reconnect_delay is the base (first-retry bound), max is the cap.
    websocket_reconnect_delay: int = Field(default=5)
    websocket_reconnect_max_delay: float = Field(default=60.0)
    # Minimum reconnect gap (seconds) worth backfilling from the archive. Tiny
    # gaps aren't worth a requester-pays pull and the archive won't have them yet.
    websocket_gap_threshold_seconds: float = Field(default=5.0)
    # How long to keep retrying a gap backfill before giving up (the archive
    # publishes with a lag, so a fresh gap often can't be filled immediately).
    gap_backfill_max_age_seconds: float = Field(default=86400.0)
    # Cap on queued gaps awaiting backfill, so a sustained archive outage with a
    # flapping socket can't grow the queue without bound (drops oldest).
    gap_max_pending: int = Field(default=1000)
    # Bounded hand-off queue between the socket read loop and the processing
    # callbacks. If consumers fall behind under load, the oldest points are
    # dropped (and counted) so the socket is always drained promptly.
    websocket_queue_maxsize: int = Field(default=10000)
    # Storage writes are batched: flush when the buffer hits storage_batch_size
    # or every storage_flush_interval seconds, whichever comes first. Turns one
    # DB round-trip per point into one per batch.
    storage_batch_size: int = Field(default=500)
    storage_flush_interval: float = Field(default=1.0)
    # Cap on the batching buffer. If the DB can't keep up, the oldest buffered
    # points are dropped (and counted) past this, bounding memory.
    storage_max_buffer: int = Field(default=50000)

    # Lossless capture spool: every raw websocket frame is written to an
    # hourly JSONL file, stamped with local receive time, BEFORE parsing and
    # independent of the drop-oldest processing queue — so bursts can't punch
    # holes in the research archive. Off by default (research pipeline
    # behavior unchanged); turn on for capture hosts.
    spool_enabled: bool = Field(default=False)
    spool_dir: Path = Field(default=Path("./data/spool"))
    # Hand-off queue to the spool writer. Sized so market bursts can't fill it
    # (~minutes of full-rate flow); if it ever overflows the writer is starved
    # (disk failure) and drops are counted as an alarm condition.
    spool_queue_maxsize: int = Field(default=500_000)
    spool_flush_interval: float = Field(default=1.0)

    # Node-data feed: replay/tail raw book diffs from a non-validating node
    # (--write-raw-book-diffs) through the L4 book. Off until a node exists.
    node_feed_enabled: bool = Field(default=False)
    node_data_dir: str = Field(default="")

    # NTP server used by the latency bench to estimate local clock offset.
    bench_ntp_server: str = Field(default="pool.ntp.org")

    # QuickNode Hypercore gRPC (StreamL4Book etc., protos/orderbook.proto).
    # host:port (usually :10000) + the endpoint's auth token. Note: Hypercore
    # streams need a paid QuickNode plan; free trial is rejected server-side.
    quicknode_grpc_endpoint: str = Field(default="")
    quicknode_grpc_token: Optional[str] = None
    
    # Logging
    log_level: str = Field(default="INFO")
    log_rotation: str = Field(default="1 day")
    log_retention: str = Field(default="30 days")
    
    # S3 Archive Settings (Hyperliquid's source bucket — AWS, requester-pays)
    hyperliquid_archive_bucket: str = Field(default="hyperliquid-archive")
    node_data_bucket: str = Field(default="hl-mainnet-node-data")
    request_payer: str = Field(default="requester")

    # Object store (your own S3-compatible bucket: Cloudflare R2, AWS S3, Backblaze, MinIO).
    # Used to cache raw archive pulls and store processed output. Zero egress on R2.
    # backend: "auto" (S3 if a bucket is set, else disabled), "s3", "local", or "none".
    object_store_backend: str = Field(default="auto")
    object_store_endpoint_url: Optional[str] = None  # e.g. https://<acct>.r2.cloudflarestorage.com (blank = AWS S3)
    object_store_bucket: Optional[str] = None
    object_store_access_key_id: Optional[str] = None
    object_store_secret_access_key: Optional[str] = None
    object_store_region: str = Field(default="auto")  # R2 wants "auto"
    object_store_prefix: str = Field(default="")  # optional key prefix, e.g. "hyperliquid/"
    object_store_local_path: Path = Field(default=Path("./data/object_store"))
    
    class Config:
        """Pydantic config."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Create directories if they don't exist
        self.data_root_path.mkdir(parents=True, exist_ok=True)
        self.historical_data_path.mkdir(parents=True, exist_ok=True)
        self.real_time_data_path.mkdir(parents=True, exist_ok=True)
        self.logs_path.mkdir(parents=True, exist_ok=True)
    
    @property
    def postgres_url(self) -> str:
        """Get PostgreSQL connection URL."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
    
    @property
    def redis_url(self) -> str:
        """Get Redis connection URL."""
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"
    
    @property
    def symbols_list(self) -> List[str]:
        """Get collect_symbols as a list."""
        return [s.strip() for s in self.collect_symbols.split(",")]

    @property
    def object_store_kind(self) -> str:
        """Resolve which object-store backend to use: 's3', 'local', or 'none'.

        "auto" -> 's3' when a bucket is configured, otherwise 'none'.
        "r2" is accepted as a friendly alias for the S3-compatible client.
        """
        backend = (self.object_store_backend or "auto").strip().lower()
        if backend == "auto":
            return "s3" if self.object_store_bucket else "none"
        if backend == "r2":  # Cloudflare R2 is just an S3-compatible endpoint
            return "s3"
        return backend


# Global settings instance
settings = Settings()