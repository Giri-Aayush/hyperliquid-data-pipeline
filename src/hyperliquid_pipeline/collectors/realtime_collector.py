"""Real-time data collector for Hyperliquid WebSocket feeds."""

import asyncio
import json
import websockets
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Callable, Optional, Any
from dataclasses import dataclass, asdict
from loguru import logger
import time
from collections import deque
import aiohttp
import ssl

from ..config import settings


@dataclass
class MarketDataPoint:
    """Single market data point."""
    timestamp: datetime
    symbol: str
    data_type: str  # 'orderbook', 'trade', 'ticker', 'funding'
    data: Dict[str, Any]


@dataclass
class WebSocketConfig:
    """WebSocket connection configuration."""
    url: str
    subscriptions: List[Dict[str, Any]]
    reconnect_delay: int = 5
    ping_interval: int = 30
    ping_timeout: int = 10


class HyperliquidWebSocketCollector:
    """Real-time data collector using Hyperliquid WebSocket API."""
    
    def __init__(self, symbols: List[str] = None):
        """Initialize the WebSocket collector.
        
        Args:
            symbols: List of symbols to collect data for
        """
        self.symbols = symbols or settings.symbols_list
        self.ws_url = "wss://api.hyperliquid.xyz/ws"
        self.logger = logger.bind(component="realtime_collector")
        
        # Data buffers
        self.orderbook_buffer: Dict[str, deque] = {symbol: deque(maxlen=1000) for symbol in self.symbols}
        self.trades_buffer: Dict[str, deque] = {symbol: deque(maxlen=10000) for symbol in self.symbols}
        self.ticker_buffer: Dict[str, deque] = {symbol: deque(maxlen=1000) for symbol in self.symbols}
        self.funding_buffer: Dict[str, deque] = {symbol: deque(maxlen=100) for symbol in self.symbols}
        
        # Connection state
        self.is_connected = False
        self.websocket = None
        self.subscriptions = []
        
        # Callbacks for data processing
        self.data_callbacks: List[Callable[[MarketDataPoint], None]] = []
        
        # Performance tracking
        self.message_count = 0
        self.last_message_time = None
        self.connection_start_time = None
        
    def add_data_callback(self, callback: Callable[[MarketDataPoint], None]):
        """Add a callback function to process incoming data.
        
        Args:
            callback: Function that processes MarketDataPoint
        """
        self.data_callbacks.append(callback)
    
    def create_subscriptions(self) -> List[Dict[str, Any]]:
        """Create WebSocket subscription messages.
        
        Returns:
            List of subscription dictionaries
        """
        subscriptions = []
        
        # Subscribe to L2 orderbook for each symbol
        for symbol in self.symbols:
            subscriptions.append({
                "method": "subscribe",
                "subscription": {
                    "type": "l2Book",
                    "coin": symbol
                }
            })
        
        # Subscribe to trades for each symbol
        for symbol in self.symbols:
            subscriptions.append({
                "method": "subscribe", 
                "subscription": {
                    "type": "trades",
                    "coin": symbol
                }
            })
        
        # Subscribe to all tickers
        subscriptions.append({
            "method": "subscribe",
            "subscription": {
                "type": "allMids"
            }
        })
        
        # Subscribe to user events (if wallet configured)
        if settings.hyperliquid_wallet_address:
            subscriptions.append({
                "method": "subscribe",
                "subscription": {
                    "type": "user",
                    "user": settings.hyperliquid_wallet_address
                }
            })
        
        return subscriptions
    
    async def send_subscription(self, websocket, subscription: Dict[str, Any]):
        """Send a subscription message.
        
        Args:
            websocket: WebSocket connection
            subscription: Subscription dictionary
        """
        try:
            await websocket.send(json.dumps(subscription))
            self.logger.debug(f"Sent subscription: {subscription}")
        except Exception as e:
            self.logger.error(f"Failed to send subscription {subscription}: {e}")
    
    def process_l2_book_message(self, message: Dict[str, Any]) -> Optional[MarketDataPoint]:
        """Process L2 orderbook message.
        
        Args:
            message: WebSocket message
            
        Returns:
            MarketDataPoint or None
        """
        try:
            data = message.get('data', {})
            coin = data.get('coin')
            levels = data.get('levels', [[], []])
            time_ms = data.get('time', int(time.time() * 1000))
            
            if not coin or coin not in self.symbols:
                return None
            
            orderbook_data = {
                'bids': levels[0] if len(levels) > 0 else [],
                'asks': levels[1] if len(levels) > 1 else [],
                'timestamp_ms': time_ms
            }
            
            data_point = MarketDataPoint(
                timestamp=datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc),
                symbol=coin,
                data_type='orderbook',
                data=orderbook_data
            )
            
            # Add to buffer
            self.orderbook_buffer[coin].append(data_point)
            
            return data_point
            
        except Exception as e:
            self.logger.error(f"Error processing L2 book message: {e}")
            return None
    
    def process_trades_message(self, message: Dict[str, Any]) -> List[MarketDataPoint]:
        """Process trades message.
        
        Args:
            message: WebSocket message
            
        Returns:
            List of MarketDataPoint objects
        """
        try:
            data_points = []
            trades_data = message.get('data', [])
            
            for trade in trades_data:
                coin = trade.get('coin')
                if not coin or coin not in self.symbols:
                    continue
                
                trade_data = {
                    'price': float(trade.get('px', 0)),
                    'size': float(trade.get('sz', 0)),
                    'side': trade.get('side', ''),
                    'timestamp_ms': trade.get('time', int(time.time() * 1000)),
                    'trade_id': trade.get('tid', '')
                }
                
                data_point = MarketDataPoint(
                    timestamp=datetime.fromtimestamp(trade_data['timestamp_ms'] / 1000, tz=timezone.utc),
                    symbol=coin,
                    data_type='trade',
                    data=trade_data
                )
                
                # Add to buffer
                self.trades_buffer[coin].append(data_point)
                data_points.append(data_point)
            
            return data_points
            
        except Exception as e:
            self.logger.error(f"Error processing trades message: {e}")
            return []
    
    def process_ticker_message(self, message: Dict[str, Any]) -> List[MarketDataPoint]:
        """Process all mids (ticker) message.
        
        Args:
            message: WebSocket message
            
        Returns:
            List of MarketDataPoint objects
        """
        try:
            data_points = []
            mids_data = message.get('data', {}).get('mids', {})
            timestamp = datetime.now(tz=timezone.utc)
            
            for coin, mid_price in mids_data.items():
                if coin not in self.symbols:
                    continue
                
                ticker_data = {
                    'mid_price': float(mid_price),
                    'timestamp_ms': int(timestamp.timestamp() * 1000)
                }
                
                data_point = MarketDataPoint(
                    timestamp=timestamp,
                    symbol=coin,
                    data_type='ticker',
                    data=ticker_data
                )
                
                # Add to buffer
                self.ticker_buffer[coin].append(data_point)
                data_points.append(data_point)
            
            return data_points
            
        except Exception as e:
            self.logger.error(f"Error processing ticker message: {e}")
            return []
    
    def process_user_message(self, message: Dict[str, Any]) -> Optional[MarketDataPoint]:
        """Process user events message.
        
        Args:
            message: WebSocket message
            
        Returns:
            MarketDataPoint or None
        """
        try:
            data = message.get('data', {})
            timestamp = datetime.now(tz=timezone.utc)
            
            data_point = MarketDataPoint(
                timestamp=timestamp,
                symbol='USER_EVENT',
                data_type='user_event',
                data=data
            )
            
            return data_point
            
        except Exception as e:
            self.logger.error(f"Error processing user message: {e}")
            return None
    
    async def process_message(self, message_str: str):
        """Process incoming WebSocket message.
        
        Args:
            message_str: Raw message string
        """
        try:
            message = json.loads(message_str)
            channel = message.get('channel', '')
            
            data_points = []
            
            # Route message based on channel
            if channel == 'l2Book':
                data_point = self.process_l2_book_message(message)
                if data_point:
                    data_points.append(data_point)
            
            elif channel == 'trades':
                data_points.extend(self.process_trades_message(message))
            
            elif channel == 'allMids':
                data_points.extend(self.process_ticker_message(message))
            
            elif channel == 'user':
                data_point = self.process_user_message(message)
                if data_point:
                    data_points.append(data_point)
            
            # Call data callbacks
            for data_point in data_points:
                for callback in self.data_callbacks:
                    try:
                        callback(data_point)
                    except Exception as e:
                        self.logger.error(f"Error in data callback: {e}")
            
            # Update metrics
            self.message_count += len(data_points)
            self.last_message_time = datetime.now(tz=timezone.utc)
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON message: {e}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
    
    async def connect(self):
        """Establish WebSocket connection and subscribe to feeds."""
        try:
            self.logger.info(f"Connecting to {self.ws_url}")
            
            async with websockets.connect(
                self.ws_url,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=10,
                max_size=10**7  # 10MB max message size
            ) as websocket:
                self.websocket = websocket
                self.is_connected = True
                self.connection_start_time = datetime.now(tz=timezone.utc)
                
                self.logger.info("WebSocket connected successfully")
                
                # Send subscriptions
                subscriptions = self.create_subscriptions()
                for subscription in subscriptions:
                    await self.send_subscription(websocket, subscription)
                    await asyncio.sleep(0.1)  # Small delay between subscriptions
                
                self.logger.info(f"Sent {len(subscriptions)} subscriptions")
                
                # Listen for messages
                async for message in websocket:
                    await self.process_message(message)
                    
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("WebSocket connection closed")
        except Exception as e:
            self.logger.error(f"WebSocket connection error: {e}")
        finally:
            self.is_connected = False
            self.websocket = None
    
    async def start_with_reconnect(self):
        """Start WebSocket collector with automatic reconnection."""
        while True:
            try:
                await self.connect()
            except Exception as e:
                self.logger.error(f"Connection failed: {e}")
            
            if not self.is_connected:
                self.logger.info(f"Reconnecting in {settings.websocket_reconnect_delay} seconds...")
                await asyncio.sleep(settings.websocket_reconnect_delay)
    
    def get_recent_data(self, symbol: str, data_type: str, limit: int = 100) -> List[MarketDataPoint]:
        """Get recent data from buffers.
        
        Args:
            symbol: Symbol to get data for
            data_type: Type of data ('orderbook', 'trade', 'ticker')
            limit: Maximum number of records to return
            
        Returns:
            List of recent MarketDataPoint objects
        """
        if data_type == 'orderbook':
            buffer = self.orderbook_buffer.get(symbol, deque())
        elif data_type == 'trade':
            buffer = self.trades_buffer.get(symbol, deque())
        elif data_type == 'ticker':
            buffer = self.ticker_buffer.get(symbol, deque())
        else:
            return []
        
        return list(buffer)[-limit:]
    
    def get_latest_orderbook(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get the latest orderbook for a symbol.
        
        Args:
            symbol: Symbol to get orderbook for
            
        Returns:
            Latest orderbook data or None
        """
        recent_data = self.get_recent_data(symbol, 'orderbook', 1)
        if recent_data:
            return recent_data[0].data
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get collector statistics.
        
        Returns:
            Dictionary with statistics
        """
        uptime = None
        if self.connection_start_time:
            uptime = (datetime.now(tz=timezone.utc) - self.connection_start_time).total_seconds()
        
        buffer_sizes = {}
        for symbol in self.symbols:
            buffer_sizes[symbol] = {
                'orderbook': len(self.orderbook_buffer.get(symbol, [])),
                'trades': len(self.trades_buffer.get(symbol, [])),
                'ticker': len(self.ticker_buffer.get(symbol, []))
            }
        
        return {
            'is_connected': self.is_connected,
            'uptime_seconds': uptime,
            'message_count': self.message_count,
            'last_message_time': self.last_message_time,
            'buffer_sizes': buffer_sizes,
            'symbols': self.symbols
        }


class DataLogger:
    """Logs market data to files."""
    
    def __init__(self, output_dir: str = None):
        """Initialize data logger.
        
        Args:
            output_dir: Directory to save data files
        """
        self.output_dir = Path(output_dir) if output_dir else settings.real_time_data_path
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # File handles for different data types
        self.file_handles = {}
        self.logger = logger.bind(component="data_logger")
    
    def log_data_point(self, data_point: MarketDataPoint):
        """Log a data point to file.
        
        Args:
            data_point: MarketDataPoint to log
        """
        try:
            # Create filename based on date and data type
            date_str = data_point.timestamp.strftime("%Y%m%d")
            filename = f"{data_point.symbol}_{data_point.data_type}_{date_str}.jsonl"
            filepath = self.output_dir / filename
            
            # Open file if not already open
            if filepath not in self.file_handles:
                self.file_handles[filepath] = open(filepath, 'a')
            
            # Write data point as JSON line
            data_dict = asdict(data_point)
            data_dict['timestamp'] = data_point.timestamp.isoformat()
            
            self.file_handles[filepath].write(json.dumps(data_dict) + '\n')
            self.file_handles[filepath].flush()
            
        except Exception as e:
            self.logger.error(f"Error logging data point: {e}")
    
    def close_all_files(self):
        """Close all open file handles."""
        for handle in self.file_handles.values():
            try:
                handle.close()
            except:
                pass
        self.file_handles.clear()


async def main():
    """Example usage of the real-time collector."""
    # Create collector for major symbols
    symbols = ['BTC', 'ETH', 'SOL', 'ARB']
    collector = HyperliquidWebSocketCollector(symbols)
    
    # Create data logger
    data_logger = DataLogger()
    
    # Add logging callback
    collector.add_data_callback(data_logger.log_data_point)
    
    # Add stats logging callback
    async def log_stats():
        while True:
            await asyncio.sleep(60)  # Log stats every minute
            stats = collector.get_stats()
            logger.info(f"Collector stats: {stats}")
    
    # Start both tasks
    try:
        await asyncio.gather(
            collector.start_with_reconnect(),
            log_stats()
        )
    except KeyboardInterrupt:
        logger.info("Shutting down collector...")
    finally:
        data_logger.close_all_files()


if __name__ == "__main__":
    asyncio.run(main())