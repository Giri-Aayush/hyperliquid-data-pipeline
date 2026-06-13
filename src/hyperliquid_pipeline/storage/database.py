"""Database storage implementations for market data."""

import asyncio
from abc import ABC, abstractmethod
from collections import deque
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime, timezone
import json

# Database libraries
import redis
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import JSONB

from loguru import logger
from ..config import settings
from ..collectors.realtime_collector import MarketDataPoint


# SQLAlchemy Base
Base = declarative_base()


class MarketDataTable(Base):
    """SQLAlchemy table for market data."""
    __tablename__ = 'market_data'
    
    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
    timestamp = sa.Column(sa.DateTime(timezone=True), nullable=False, index=True)
    symbol = sa.Column(sa.String(20), nullable=False, index=True)
    data_type = sa.Column(sa.String(20), nullable=False, index=True)
    data = sa.Column(JSONB, nullable=False)
    created_at = sa.Column(sa.DateTime(timezone=True), default=datetime.now(timezone.utc))
    
    __table_args__ = (
        sa.Index('idx_symbol_type_timestamp', 'symbol', 'data_type', 'timestamp'),
    )


class DataStorage(ABC):
    """Abstract base class for data storage."""
    
    @abstractmethod
    async def store_data_point(self, data_point: MarketDataPoint) -> bool:
        """Store a single data point."""
        pass
    
    @abstractmethod
    async def store_data_points(self, data_points: List[MarketDataPoint]) -> int:
        """Store multiple data points."""
        pass
    
    @abstractmethod
    async def get_data(
        self, 
        symbol: str, 
        data_type: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[MarketDataPoint]:
        """Retrieve data points."""
        pass
    
    @abstractmethod
    async def close(self):
        """Close storage connections."""
        pass


class PostgreSQLStorage(DataStorage):
    """PostgreSQL storage for market data."""
    
    def __init__(self):
        """Initialize PostgreSQL storage."""
        self.engine = None
        self.session_factory = None
        self.logger = logger.bind(component="postgresql_storage")
    
    async def initialize(self):
        """Initialize database connection and create tables."""
        try:
            # Create async engine
            database_url = settings.postgres_url.replace('postgresql://', 'postgresql+asyncpg://')
            self.engine = create_async_engine(
                database_url,
                echo=False,
                pool_size=10,
                max_overflow=20
            )
            
            # Create session factory
            self.session_factory = sessionmaker(
                self.engine, 
                class_=AsyncSession, 
                expire_on_commit=False
            )
            
            # Create tables
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            self.logger.info("PostgreSQL storage initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize PostgreSQL storage: {e}")
            raise
    
    async def store_data_point(self, data_point: MarketDataPoint) -> bool:
        """Store a single data point."""
        try:
            async with self.session_factory() as session:
                db_record = MarketDataTable(
                    timestamp=data_point.timestamp,
                    symbol=data_point.symbol,
                    data_type=data_point.data_type,
                    data=data_point.data
                )
                session.add(db_record)
                await session.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to store data point: {e}")
            return False
    
    async def store_data_points(self, data_points: List[MarketDataPoint]) -> int:
        """Store multiple data points."""
        try:
            async with self.session_factory() as session:
                db_records = [
                    MarketDataTable(
                        timestamp=dp.timestamp,
                        symbol=dp.symbol,
                        data_type=dp.data_type,
                        data=dp.data
                    )
                    for dp in data_points
                ]
                session.add_all(db_records)
                await session.commit()
                return len(db_records)
                
        except Exception as e:
            self.logger.error(f"Failed to store data points: {e}")
            return 0
    
    async def get_data(
        self, 
        symbol: str, 
        data_type: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[MarketDataPoint]:
        """Retrieve data points."""
        try:
            async with self.session_factory() as session:
                query = sa.select(MarketDataTable).where(
                    MarketDataTable.symbol == symbol,
                    MarketDataTable.data_type == data_type,
                    MarketDataTable.timestamp >= start_time,
                    MarketDataTable.timestamp <= end_time
                ).order_by(MarketDataTable.timestamp)
                
                result = await session.execute(query)
                records = result.scalars().all()
                
                return [
                    MarketDataPoint(
                        timestamp=record.timestamp,
                        symbol=record.symbol,
                        data_type=record.data_type,
                        data=record.data
                    )
                    for record in records
                ]
                
        except Exception as e:
            self.logger.error(f"Failed to retrieve data: {e}")
            return []
    
    async def close(self):
        """Close database connections."""
        if self.engine:
            await self.engine.dispose()


class InfluxDBStorage(DataStorage):
    """InfluxDB storage for time-series market data."""
    
    def __init__(self):
        """Initialize InfluxDB storage."""
        self.client = None
        self.write_api = None
        self.logger = logger.bind(component="influxdb_storage")
    
    async def initialize(self):
        """Initialize InfluxDB connection."""
        try:
            self.client = InfluxDBClient(
                url=settings.influxdb_url,
                token=settings.influxdb_token,
                org=settings.influxdb_org
            )
            
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            
            self.logger.info("InfluxDB storage initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize InfluxDB storage: {e}")
            raise
    
    def _data_point_to_influx_point(self, data_point: MarketDataPoint) -> Point:
        """Convert MarketDataPoint to InfluxDB Point."""
        point = Point(data_point.data_type)
        point.tag("symbol", data_point.symbol)
        point.time(data_point.timestamp)
        
        # Add fields based on data type
        if data_point.data_type == 'orderbook':
            # Store best bid/ask
            bids = data_point.data.get('bids', [])
            asks = data_point.data.get('asks', [])
            
            if bids:
                point.field("best_bid_price", float(bids[0]['px']))
                point.field("best_bid_size", float(bids[0]['sz']))
            
            if asks:
                point.field("best_ask_price", float(asks[0]['px']))
                point.field("best_ask_size", float(asks[0]['sz']))
            
            point.field("bid_levels", len(bids))
            point.field("ask_levels", len(asks))
            
        elif data_point.data_type == 'trade':
            point.field("price", data_point.data.get('price', 0.0))
            point.field("size", data_point.data.get('size', 0.0))
            point.tag("side", data_point.data.get('side', ''))
            
        elif data_point.data_type == 'ticker':
            point.field("mid_price", data_point.data.get('mid_price', 0.0))
        
        return point
    
    async def store_data_point(self, data_point: MarketDataPoint) -> bool:
        """Store a single data point."""
        try:
            point = self._data_point_to_influx_point(data_point)
            self.write_api.write(bucket=settings.influxdb_bucket, record=point)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store data point in InfluxDB: {e}")
            return False
    
    async def store_data_points(self, data_points: List[MarketDataPoint]) -> int:
        """Store multiple data points."""
        try:
            points = [self._data_point_to_influx_point(dp) for dp in data_points]
            self.write_api.write(bucket=settings.influxdb_bucket, record=points)
            return len(points)
            
        except Exception as e:
            self.logger.error(f"Failed to store data points in InfluxDB: {e}")
            return 0
    
    async def get_data(
        self, 
        symbol: str, 
        data_type: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[MarketDataPoint]:
        """Retrieve data points."""
        try:
            query = f'''
            from(bucket: "{settings.influxdb_bucket}")
            |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
            |> filter(fn: (r) => r._measurement == "{data_type}")
            |> filter(fn: (r) => r.symbol == "{symbol}")
            '''
            
            query_api = self.client.query_api()
            tables = query_api.query(query)
            
            # Convert results back to MarketDataPoint
            data_points = []
            for table in tables:
                for record in table.records:
                    # Reconstruct data from InfluxDB record
                    data = {record['_field']: record['_value']}
                    
                    data_point = MarketDataPoint(
                        timestamp=record['_time'],
                        symbol=record['symbol'],
                        data_type=record['_measurement'],
                        data=data
                    )
                    data_points.append(data_point)
            
            return data_points
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve data from InfluxDB: {e}")
            return []
    
    async def close(self):
        """Close InfluxDB connections."""
        if self.client:
            self.client.close()


class RedisStorage(DataStorage):
    """Redis storage for caching recent market data."""
    
    def __init__(self):
        """Initialize Redis storage."""
        self.client = None
        self.logger = logger.bind(component="redis_storage")
    
    async def initialize(self):
        """Initialize Redis connection."""
        try:
            self.client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                password=settings.redis_password,
                db=settings.redis_db,
                decode_responses=True
            )
            
            # Test connection
            await asyncio.get_event_loop().run_in_executor(None, self.client.ping)
            
            self.logger.info("Redis storage initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize Redis storage: {e}")
            raise
    
    def _get_key(self, symbol: str, data_type: str) -> str:
        """Get Redis key for symbol and data type."""
        return f"market_data:{symbol}:{data_type}"
    
    async def store_data_point(self, data_point: MarketDataPoint) -> bool:
        """Store a single data point."""
        try:
            key = self._get_key(data_point.symbol, data_point.data_type)
            
            # Store as JSON with timestamp as score in sorted set
            data_json = json.dumps({
                'timestamp': data_point.timestamp.isoformat(),
                'data': data_point.data
            })
            
            score = data_point.timestamp.timestamp()
            
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.client.zadd(key, {data_json: score})
            )
            
            # Keep only recent data (last 1000 points)
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.client.zremrangebyrank(key, 0, -1001)
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store data point in Redis: {e}")
            return False
    
    async def store_data_points(self, data_points: List[MarketDataPoint]) -> int:
        """Store multiple data points."""
        success_count = 0
        for data_point in data_points:
            if await self.store_data_point(data_point):
                success_count += 1
        return success_count
    
    async def get_data(
        self, 
        symbol: str, 
        data_type: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[MarketDataPoint]:
        """Retrieve data points."""
        try:
            key = self._get_key(symbol, data_type)
            
            start_score = start_time.timestamp()
            end_score = end_time.timestamp()
            
            # Get data from sorted set by score range
            results = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.client.zrangebyscore(key, start_score, end_score)
            )
            
            data_points = []
            for result in results:
                data = json.loads(result)
                data_point = MarketDataPoint(
                    timestamp=datetime.fromisoformat(data['timestamp']),
                    symbol=symbol,
                    data_type=data_type,
                    data=data['data']
                )
                data_points.append(data_point)
            
            return data_points
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve data from Redis: {e}")
            return []
    
    async def get_latest(self, symbol: str, data_type: str, count: int = 1) -> List[MarketDataPoint]:
        """Get latest N data points."""
        try:
            key = self._get_key(symbol, data_type)
            
            # Get latest data points
            results = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.client.zrevrange(key, 0, count - 1)
            )
            
            data_points = []
            for result in results:
                data = json.loads(result)
                data_point = MarketDataPoint(
                    timestamp=datetime.fromisoformat(data['timestamp']),
                    symbol=symbol,
                    data_type=data_type,
                    data=data['data']
                )
                data_points.append(data_point)
            
            return data_points
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve latest data from Redis: {e}")
            return []
    
    async def close(self):
        """Close Redis connections."""
        if self.client:
            self.client.close()


class MultiStorage(DataStorage):
    """Multi-backend storage that writes to multiple storage systems."""
    
    def __init__(self, storages: List[DataStorage]):
        """Initialize multi-storage.
        
        Args:
            storages: List of storage backends
        """
        self.storages = storages
        self.logger = logger.bind(component="multi_storage")
    
    async def initialize(self):
        """Initialize all storage backends."""
        for storage in self.storages:
            await storage.initialize()
    
    async def store_data_point(self, data_point: MarketDataPoint) -> bool:
        """Store data point to all backends."""
        if not self.storages:
            return False
        results = await asyncio.gather(
            *[storage.store_data_point(data_point) for storage in self.storages],
            return_exceptions=True
        )

        success_count = sum(1 for result in results if result is True)
        return success_count > 0

    async def store_data_points(self, data_points: List[MarketDataPoint]) -> int:
        """Store data points to all backends."""
        if not self.storages:
            return 0
        results = await asyncio.gather(
            *[storage.store_data_points(data_points) for storage in self.storages],
            return_exceptions=True
        )

        # Return the maximum count stored across all backends (0 if all failed)
        counts = [result for result in results if isinstance(result, int)]
        return max(counts) if counts else 0
    
    async def get_data(
        self, 
        symbol: str, 
        data_type: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[MarketDataPoint]:
        """Retrieve data from the first available backend."""
        for storage in self.storages:
            try:
                data = await storage.get_data(symbol, data_type, start_time, end_time)
                if data:
                    return data
            except Exception as e:
                self.logger.warning(f"Failed to get data from storage {type(storage).__name__}: {e}")
        
        return []
    
    async def close(self):
        """Close all storage backends."""
        await asyncio.gather(
            *[storage.close() for storage in self.storages],
            return_exceptions=True
        )


class BatchingStorage(DataStorage):
    """Buffers writes and flushes them as batches, off the ingest hot path.

    Wraps another DataStorage (typically MultiStorage). ``store_data_point``
    appends to an in-memory buffer and returns immediately; the buffer is
    flushed to the inner store when it reaches ``batch_size`` or every
    ``flush_interval`` seconds via a background task. This turns one DB
    round-trip per point into one per batch — the difference between keeping up
    with a busy market and falling behind.
    """

    def __init__(
        self,
        inner: DataStorage,
        batch_size: int = 500,
        flush_interval: float = 1.0,
        max_buffer: int = 50_000,
    ):
        self.inner = inner
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_buffer = max_buffer
        # deque so overflow eviction (popleft) is O(1).
        self._buffer: "deque[MarketDataPoint]" = deque()
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._flush_task: Optional[asyncio.Task] = None
        self._closed = False
        self.dropped_count = 0
        self.logger = logger.bind(component="batching_storage")

    async def initialize(self):
        """Initialize the inner store and start the periodic flush task."""
        await self.inner.initialize()
        self.start()

    def start(self):
        """Start the periodic flush task. Call from within a running event loop."""
        if self._flush_task is None and not self._closed:
            self._flush_task = asyncio.create_task(self._flush_loop())

    async def _flush_loop(self):
        # Sleep on the stop event so close() can wake us immediately, and so we
        # never get cancelled mid-write (which would lose the in-flight batch).
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.flush_interval)
            except asyncio.TimeoutError:
                pass
            await self.flush()

    def _enforce_cap_locked(self):
        """Drop oldest points if the buffer is over capacity. Caller holds the lock."""
        over = len(self._buffer) - self.max_buffer
        if over > 0:
            for _ in range(over):
                self._buffer.popleft()
            self.dropped_count += over
            if self.dropped_count % 10_000 < over:
                self.logger.warning(
                    f"Storage buffer over {self.max_buffer}; dropping oldest points "
                    f"(total dropped: {self.dropped_count}) — is the database keeping up?"
                )

    async def store_data_point(self, data_point: MarketDataPoint) -> bool:
        if self._closed:
            return False
        async with self._lock:
            self._buffer.append(data_point)
            self._enforce_cap_locked()
            full = len(self._buffer) >= self.batch_size
        if full:
            await self.flush()
        return True

    async def store_data_points(self, data_points: List[MarketDataPoint]) -> int:
        if self._closed:
            return 0
        async with self._lock:
            self._buffer.extend(data_points)
            self._enforce_cap_locked()
            full = len(self._buffer) >= self.batch_size
        if full:
            await self.flush()
        return len(data_points)

    async def flush(self):
        """Write the buffered points to the inner store as a single batch.

        On failure the batch is put back at the front of the buffer so the next
        flush retries it, rather than silently dropping data; the buffer cap then
        bounds memory if the backend stays down.
        """
        async with self._lock:
            if not self._buffer:
                return
            batch = list(self._buffer)
            self._buffer.clear()
        try:
            await self.inner.store_data_points(batch)
        except Exception as e:
            async with self._lock:
                self._buffer.extendleft(reversed(batch))  # restore original order at front
                self._enforce_cap_locked()
            self.logger.error(f"Batch flush failed, re-queued {len(batch)} points for retry: {e}")

    async def get_data(
        self,
        symbol: str,
        data_type: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[MarketDataPoint]:
        """Flush pending writes, then read through to the inner store."""
        await self.flush()
        return await self.inner.get_data(symbol, data_type, start_time, end_time)

    async def close(self):
        """Stop the flush task gracefully, flush the tail, and close the inner store.

        Idempotent. Signals the loop to stop (rather than cancelling it
        mid-write, which would lose the in-flight batch), waits for it to finish,
        then does a final flush.
        """
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        if self._flush_task is not None:
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None
        await self.flush()
        await self.inner.close()