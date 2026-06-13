"""Data processing pipeline for market data."""

import asyncio
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from collections import deque
from loguru import logger
import json
from pathlib import Path

from ..collectors.realtime_collector import MarketDataPoint
from ..storage.database import DataStorage, MultiStorage, BatchingStorage, RedisStorage, InfluxDBStorage, PostgreSQLStorage
from ..config import settings


@dataclass
class ProcessedData:
    """Processed market data."""
    symbol: str
    timestamp: datetime
    data_type: str
    ohlcv: Optional[Dict[str, float]] = None
    orderbook_metrics: Optional[Dict[str, float]] = None
    trade_metrics: Optional[Dict[str, float]] = None
    technical_indicators: Optional[Dict[str, float]] = None


class OHLCVProcessor:
    """Processes trade data into OHLCV candles."""
    
    # Hard safety cap per symbol so a burst inside the retention window can't
    # grow the buffer without bound; time-based eviction is the primary mechanism.
    MAX_TRADES_PER_SYMBOL = 200_000

    def __init__(self, timeframes: List[str] = None, retention: timedelta = timedelta(hours=1)):
        """Initialize OHLCV processor.

        Args:
            timeframes: List of timeframes (e.g., ['1m', '5m', '1h'])
            retention: how far back to keep trades for candle generation
        """
        self.timeframes = timeframes or ['1m', '5m', '15m', '1h', '4h', '1d']
        # deque per symbol: append-right on arrival, evict-left by age — O(1)
        # amortized, instead of rebuilding a list on every trade.
        self.trade_buffers: Dict[str, deque] = {}
        self.retention = retention
        self.logger = logger.bind(component="ohlcv_processor")
    
    def _timeframe_to_seconds(self, timeframe: str) -> int:
        """Convert timeframe string to seconds."""
        unit = timeframe[-1].lower()
        value = int(timeframe[:-1])
        
        multipliers = {
            's': 1,
            'm': 60,
            'h': 3600,
            'd': 86400
        }
        
        return value * multipliers.get(unit, 60)
    
    def add_trade(self, trade_data: MarketDataPoint):
        """Add trade data to buffer.
        
        Args:
            trade_data: Trade data point
        """
        if trade_data.data_type != 'trade':
            return

        symbol = trade_data.symbol
        buffer = self.trade_buffers.get(symbol)
        if buffer is None:
            buffer = deque(maxlen=self.MAX_TRADES_PER_SYMBOL)
            self.trade_buffers[symbol] = buffer

        buffer.append(trade_data)

        # Evict aged-out trades from the front. Trades arrive in time order, so
        # this pops only the few that just expired — O(1) amortized per add,
        # versus rebuilding the whole list (which was O(n) per trade -> O(n^2)).
        cutoff_time = datetime.now(timezone.utc) - self.retention
        while buffer and buffer[0].timestamp <= cutoff_time:
            buffer.popleft()
    
    def generate_ohlcv(self, symbol: str, timeframe: str, end_time: datetime = None) -> Optional[Dict[str, float]]:
        """Generate OHLCV data for a symbol and timeframe.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe string (e.g., '1m', '5m')
            end_time: End time for the candle
            
        Returns:
            OHLCV dictionary or None
        """
        if symbol not in self.trade_buffers:
            return None
        
        if end_time is None:
            end_time = datetime.now(timezone.utc)
        
        interval_seconds = self._timeframe_to_seconds(timeframe)
        start_time = end_time - timedelta(seconds=interval_seconds)

        # Collect trades in [start, end). Scan the whole buffer rather than
        # breaking early: exchange trade timestamps aren't strictly monotonic, so
        # a late-arriving trade can sit out of order in the deque and an early
        # break would silently drop in-window trades before it. The buffer is
        # bounded by retention, and add_trade is now O(1), so this stays cheap.
        relevant_trades = [
            trade for trade in self.trade_buffers[symbol]
            if start_time <= trade.timestamp < end_time
        ]

        if not relevant_trades:
            return None
        
        # Extract prices and volumes
        prices = [trade.data['price'] for trade in relevant_trades]
        volumes = [trade.data['size'] for trade in relevant_trades]
        
        ohlcv = {
            'open': prices[0],
            'high': max(prices),
            'low': min(prices),
            'close': prices[-1],
            'volume': sum(volumes),
            'count': len(prices),
            'vwap': sum(p * v for p, v in zip(prices, volumes)) / sum(volumes) if sum(volumes) > 0 else prices[-1]
        }
        
        return ohlcv


class OrderBookProcessor:
    """Processes orderbook data to extract metrics."""
    
    def __init__(self):
        """Initialize orderbook processor."""
        self.latest_orderbooks: Dict[str, MarketDataPoint] = {}
        self.logger = logger.bind(component="orderbook_processor")
    
    def update_orderbook(self, orderbook_data: MarketDataPoint):
        """Update latest orderbook for a symbol.
        
        Args:
            orderbook_data: Orderbook data point
        """
        if orderbook_data.data_type != 'orderbook':
            return
        
        self.latest_orderbooks[orderbook_data.symbol] = orderbook_data
    
    def calculate_metrics(self, symbol: str) -> Optional[Dict[str, float]]:
        """Calculate orderbook metrics for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Orderbook metrics dictionary or None
        """
        if symbol not in self.latest_orderbooks:
            return None
        
        orderbook = self.latest_orderbooks[symbol]
        bids = orderbook.data.get('bids', [])
        asks = orderbook.data.get('asks', [])
        
        if not bids or not asks:
            return None
        
        # Extract prices and sizes
        bid_prices = [float(level.get('px', 0)) for level in bids]
        bid_sizes = [float(level.get('sz', 0)) for level in bids]
        ask_prices = [float(level.get('px', 0)) for level in asks]
        ask_sizes = [float(level.get('sz', 0)) for level in asks]
        
        # Calculate metrics
        best_bid = bid_prices[0] if bid_prices else 0
        best_ask = ask_prices[0] if ask_prices else 0
        mid_price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else 0
        spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0
        spread_bps = (spread / mid_price) * 10000 if mid_price > 0 else 0
        
        # Depth calculations
        total_bid_volume = sum(bid_sizes)
        total_ask_volume = sum(ask_sizes)
        
        # Weighted average prices for depth
        if len(bid_prices) >= 5:
            bid_depth_5 = sum(bid_sizes[:5])
            ask_depth_5 = sum(ask_sizes[:5])
        else:
            bid_depth_5 = total_bid_volume
            ask_depth_5 = total_ask_volume
        
        # Imbalance
        imbalance = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume) if (total_bid_volume + total_ask_volume) > 0 else 0
        
        metrics = {
            'best_bid': best_bid,
            'best_ask': best_ask,
            'mid_price': mid_price,
            'spread': spread,
            'spread_bps': spread_bps,
            'total_bid_volume': total_bid_volume,
            'total_ask_volume': total_ask_volume,
            'bid_depth_5': bid_depth_5,
            'ask_depth_5': ask_depth_5,
            'imbalance': imbalance,
            'bid_levels': len(bids),
            'ask_levels': len(asks)
        }
        
        return metrics


class TechnicalIndicatorProcessor:
    """Calculates technical indicators from OHLCV data."""
    
    def __init__(self):
        """Initialize technical indicator processor."""
        self.price_history: Dict[str, List[float]] = {}
        self.volume_history: Dict[str, List[float]] = {}
        self.max_history = 200  # Keep last 200 periods
        self.logger = logger.bind(component="technical_processor")
    
    def update_price_data(self, symbol: str, ohlcv: Dict[str, float]):
        """Update price and volume history.
        
        Args:
            symbol: Trading symbol
            ohlcv: OHLCV data
        """
        if symbol not in self.price_history:
            self.price_history[symbol] = []
            self.volume_history[symbol] = []
        
        self.price_history[symbol].append(ohlcv['close'])
        self.volume_history[symbol].append(ohlcv['volume'])
        
        # Keep only recent history
        if len(self.price_history[symbol]) > self.max_history:
            self.price_history[symbol] = self.price_history[symbol][-self.max_history:]
            self.volume_history[symbol] = self.volume_history[symbol][-self.max_history:]
    
    def calculate_sma(self, prices: List[float], period: int) -> Optional[float]:
        """Calculate Simple Moving Average."""
        if len(prices) < period:
            return None
        return sum(prices[-period:]) / period
    
    def calculate_ema(self, prices: List[float], period: int) -> Optional[float]:
        """Calculate Exponential Moving Average."""
        if len(prices) < period:
            return None
        
        multiplier = 2 / (period + 1)
        ema = prices[0]
        
        for price in prices[1:]:
            ema = (price * multiplier) + (ema * (1 - multiplier))
        
        return ema
    
    def calculate_rsi(self, prices: List[float], period: int = 14) -> Optional[float]:
        """Calculate Relative Strength Index."""
        if len(prices) < period + 1:
            return None
        
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def calculate_bollinger_bands(self, prices: List[float], period: int = 20, std_dev: float = 2) -> Optional[Dict[str, float]]:
        """Calculate Bollinger Bands."""
        if len(prices) < period:
            return None
        
        recent_prices = prices[-period:]
        sma = sum(recent_prices) / period
        variance = sum((p - sma) ** 2 for p in recent_prices) / period
        std = variance ** 0.5
        
        return {
            'bb_upper': sma + (std * std_dev),
            'bb_middle': sma,
            'bb_lower': sma - (std * std_dev),
            'bb_width': (std * std_dev * 2) / sma * 100 if sma > 0 else 0
        }
    
    def calculate_indicators(self, symbol: str) -> Optional[Dict[str, float]]:
        """Calculate all technical indicators for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Technical indicators dictionary or None
        """
        if symbol not in self.price_history or len(self.price_history[symbol]) < 20:
            return None
        
        prices = self.price_history[symbol]
        volumes = self.volume_history[symbol]
        
        indicators = {}
        
        # Moving Averages
        for period in [10, 20, 50]:
            sma = self.calculate_sma(prices, period)
            if sma:
                indicators[f'sma_{period}'] = sma
            
            ema = self.calculate_ema(prices, period)
            if ema:
                indicators[f'ema_{period}'] = ema
        
        # RSI
        rsi = self.calculate_rsi(prices)
        if rsi:
            indicators['rsi'] = rsi
        
        # Bollinger Bands
        bb = self.calculate_bollinger_bands(prices)
        if bb:
            indicators.update(bb)
        
        # Price momentum
        if len(prices) >= 2:
            indicators['price_change'] = prices[-1] - prices[-2]
            indicators['price_change_pct'] = (prices[-1] - prices[-2]) / prices[-2] * 100 if prices[-2] > 0 else 0
        
        # Volume indicators
        if len(volumes) >= 10:
            indicators['volume_sma_10'] = sum(volumes[-10:]) / 10
            if indicators['volume_sma_10'] > 0:
                indicators['volume_ratio'] = volumes[-1] / indicators['volume_sma_10']
        
        return indicators


class DataProcessor:
    """Main data processing pipeline."""
    
    def __init__(self, storage: DataStorage):
        """Initialize data processor.
        
        Args:
            storage: Storage backend for processed data
        """
        self.storage = storage
        self.ohlcv_processor = OHLCVProcessor()
        self.orderbook_processor = OrderBookProcessor()
        self.technical_processor = TechnicalIndicatorProcessor()
        # Latest per-symbol asset context (mark/oracle/OI/funding/basis), folded
        # into the processed point so it lands in the DB, not just JSONL.
        self.latest_asset_ctx: Dict[str, Dict[str, Any]] = {}

        self.logger = logger.bind(component="data_processor")
        
        # Processing callbacks
        self.processing_callbacks: List[Callable[[ProcessedData], None]] = []
    
    def add_processing_callback(self, callback: Callable[[ProcessedData], None]):
        """Add callback for processed data.
        
        Args:
            callback: Function to call with processed data
        """
        self.processing_callbacks.append(callback)
    
    async def process_market_data(self, data_point: MarketDataPoint):
        """Process incoming market data.
        
        Args:
            data_point: Raw market data point
        """
        try:
            # Update processors with new data
            if data_point.data_type == 'trade':
                self.ohlcv_processor.add_trade(data_point)
            elif data_point.data_type == 'orderbook':
                self.orderbook_processor.update_orderbook(data_point)
            elif data_point.data_type == 'asset_ctx':
                self.latest_asset_ctx[data_point.symbol] = data_point.data

            # Generate processed data every minute for trades
            if data_point.data_type == 'trade':
                await self._process_symbol_data(data_point.symbol, data_point.timestamp)
        
        except Exception as e:
            self.logger.error(f"Error processing market data: {e}")
    
    async def _process_symbol_data(self, symbol: str, timestamp: datetime):
        """Process all data for a symbol at a given timestamp.
        
        Args:
            symbol: Trading symbol
            timestamp: Processing timestamp
        """
        try:
            # Generate OHLCV for 1-minute timeframe
            ohlcv = self.ohlcv_processor.generate_ohlcv(symbol, '1m', timestamp)
            
            # Calculate orderbook metrics
            orderbook_metrics = self.orderbook_processor.calculate_metrics(symbol)
            
            # Update technical indicators with new OHLCV
            technical_indicators = None
            if ohlcv:
                self.technical_processor.update_price_data(symbol, ohlcv)
                technical_indicators = self.technical_processor.calculate_indicators(symbol)
            
            asset_ctx = self.latest_asset_ctx.get(symbol)

            # Create processed data point
            processed_data = ProcessedData(
                symbol=symbol,
                timestamp=timestamp,
                data_type='processed',
                ohlcv=ohlcv,
                orderbook_metrics=orderbook_metrics,
                technical_indicators=technical_indicators
            )

            # Store processed data
            processed_data_point = MarketDataPoint(
                timestamp=timestamp,
                symbol=symbol,
                data_type='processed',
                data={
                    'ohlcv': ohlcv,
                    'orderbook_metrics': orderbook_metrics,
                    'technical_indicators': technical_indicators,
                    'asset_ctx': asset_ctx
                }
            )
            
            await self.storage.store_data_point(processed_data_point)
            
            # Call processing callbacks
            for callback in self.processing_callbacks:
                try:
                    callback(processed_data)
                except Exception as e:
                    self.logger.error(f"Error in processing callback: {e}")
        
        except Exception as e:
            self.logger.error(f"Error processing symbol data for {symbol}: {e}")
    
    async def bulk_process_historical_data(self, historical_data: Dict[str, Dict[str, pd.DataFrame]]):
        """Process historical data in bulk.
        
        Args:
            historical_data: Dictionary with symbol -> data_type -> DataFrame structure
        """
        self.logger.info("Starting bulk processing of historical data")
        
        for symbol, data_types in historical_data.items():
            self.logger.info(f"Processing historical data for {symbol}")
            
            # Process trades data to generate OHLCV
            if 'trades' in data_types and not data_types['trades'].empty:
                df = data_types['trades']
                
                # Convert to MarketDataPoint objects and process
                for _, row in df.iterrows():
                    trade_data = MarketDataPoint(
                        timestamp=row.name,  # Index is timestamp
                        symbol=symbol,
                        data_type='trade',
                        data={
                            'price': row['price'],
                            'size': row['size'],
                            'side': row['side']
                        }
                    )
                    
                    self.ohlcv_processor.add_trade(trade_data)
                
                # Generate OHLCV for different timeframes
                for timeframe in ['1m', '5m', '15m', '1h']:
                    self.logger.info(f"Generating {timeframe} OHLCV for {symbol}")
                    
                    # Generate OHLCV for each time interval
                    start_time = df.index.min()
                    end_time = df.index.max()
                    
                    interval_seconds = self.ohlcv_processor._timeframe_to_seconds(timeframe)
                    current_time = start_time
                    
                    while current_time < end_time:
                        next_time = current_time + timedelta(seconds=interval_seconds)
                        ohlcv = self.ohlcv_processor.generate_ohlcv(symbol, timeframe, next_time)
                        
                        if ohlcv:
                            # Update technical indicators
                            self.technical_processor.update_price_data(symbol, ohlcv)
                            technical_indicators = self.technical_processor.calculate_indicators(symbol)
                            
                            # Store processed data
                            processed_data_point = MarketDataPoint(
                                timestamp=next_time,
                                symbol=symbol,
                                data_type=f'ohlcv_{timeframe}',
                                data={
                                    'ohlcv': ohlcv,
                                    'technical_indicators': technical_indicators
                                }
                            )
                            
                            await self.storage.store_data_point(processed_data_point)
                        
                        current_time = next_time
        
        self.logger.info("Completed bulk processing of historical data")


async def create_storage_backends() -> DataStorage:
    """Create and initialize whichever storage backends are configured and reachable.

    Every backend is optional: a backend that isn't configured is skipped, and one
    that fails to connect is logged and skipped rather than crashing the pipeline.
    With nothing configured/reachable you get an empty MultiStorage — the pipeline
    still runs, and raw data is preserved by the DataLogger (JSONL, optionally to R2).

    The backends are wrapped in a BatchingStorage so the hot path buffers writes
    and flushes them in batches. Call ``close()`` on the result to flush the tail.

    Returns:
        A started BatchingStorage wrapping the active backends (possibly zero).
    """
    storages = []

    async def _try(name: str, storage: DataStorage):
        try:
            await storage.initialize()
            storages.append(storage)
        except Exception as e:
            logger.warning(f"{name} storage unavailable, skipping: {e}")

    # Redis cache — always attempted; initialize() pings and raises if Redis
    # isn't reachable, in which case _try logs and skips it. Point REDIS_HOST at
    # your instance to enable it.
    await _try("Redis", RedisStorage())

    # InfluxDB for time-series data
    if settings.influxdb_token:
        await _try("InfluxDB", InfluxDBStorage())

    # PostgreSQL for structured data
    if settings.postgres_password:
        await _try("PostgreSQL", PostgreSQLStorage())

    if not storages:
        logger.warning(
            "No storage backends active — processed points won't be persisted to a DB. "
            "Raw data is still written by the DataLogger. Configure Redis/Postgres/InfluxDB to enable."
        )
        # Nothing to batch to; skip the wrapper (and its background flush task).
        return MultiStorage(storages)

    batching = BatchingStorage(
        MultiStorage(storages),
        batch_size=settings.storage_batch_size,
        flush_interval=settings.storage_flush_interval,
        max_buffer=settings.storage_max_buffer,
    )
    batching.start()  # we're in a running loop here
    return batching


async def main():
    """Example usage of the data processor."""
    # Create storage backends
    storage = await create_storage_backends()
    
    # Create data processor
    processor = DataProcessor(storage)
    
    # Add processing callback for logging
    def log_processed_data(processed_data: ProcessedData):
        logger.info(f"Processed data for {processed_data.symbol}: {processed_data.ohlcv}")
    
    processor.add_processing_callback(log_processed_data)
    
    # Example: Process some sample trade data
    sample_trade = MarketDataPoint(
        timestamp=datetime.now(timezone.utc),
        symbol='BTC',
        data_type='trade',
        data={
            'price': 50000.0,
            'size': 0.1,
            'side': 'buy'
        }
    )
    
    await processor.process_market_data(sample_trade)
    
    # Close storage
    await storage.close()


if __name__ == "__main__":
    asyncio.run(main())