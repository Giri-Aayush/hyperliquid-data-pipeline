"""Real-time data collector for Hyperliquid WebSocket feeds."""

import asyncio
import inspect
import json
import random
import websockets
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Callable, Optional, Any, Tuple, TYPE_CHECKING
from dataclasses import dataclass, asdict
from loguru import logger
import time
from collections import deque

from ..config import settings
from ..utils.latency import LatencyHistogram
from .spool import RawSpool

if TYPE_CHECKING:  # annotation only; runtime import is deferred to avoid a cycle
    from ..storage.object_store import ObjectStore


@dataclass
class MarketDataPoint:
    """Single market data point.

    timestamp is exchange time where the feed provides one (l2Book, trades,
    bbo), local time otherwise. The recv_* fields are stamped at the socket
    read, before parsing: wall-clock ms for measuring feed latency against the
    exchange timestamp, and a monotonic ns stamp for jitter analysis immune to
    clock steps. None for points not built from a live socket frame (backfill,
    tests, historical replay).
    """
    timestamp: datetime
    symbol: str
    data_type: str  # 'orderbook', 'trade', 'ticker', 'funding', 'bbo', ...
    data: Dict[str, Any]
    recv_ts_ms: Optional[float] = None
    recv_mono_ns: Optional[int] = None


@dataclass
class GapEvent:
    """A window of missed live data, detected after a reconnect."""
    start: datetime
    end: datetime
    symbols: List[str]

    @property
    def seconds(self) -> float:
        return (self.end - self.start).total_seconds()


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
    
    def __init__(self, symbols: List[str] = None, ws_url: str = None):
        """Initialize the WebSocket collector.

        Args:
            symbols: List of symbols to collect data for
            ws_url: WebSocket endpoint override; defaults to the configured one
                (the public gateway, or a colocated node's websocket)
        """
        self.symbols = symbols or settings.symbols_list
        self.ws_url = ws_url or settings.hyperliquid_ws_url
        self.logger = logger.bind(component="realtime_collector")
        
        # Data buffers
        self.orderbook_buffer: Dict[str, deque] = {symbol: deque(maxlen=1000) for symbol in self.symbols}
        self.trades_buffer: Dict[str, deque] = {symbol: deque(maxlen=10000) for symbol in self.symbols}
        self.ticker_buffer: Dict[str, deque] = {symbol: deque(maxlen=1000) for symbol in self.symbols}
        self.funding_buffer: Dict[str, deque] = {symbol: deque(maxlen=100) for symbol in self.symbols}
        # Per-asset context: mark/oracle/mid price, open interest, funding, premium.
        self.asset_ctx_buffer: Dict[str, deque] = {symbol: deque(maxlen=1000) for symbol in self.symbols}
        # Event-level top-of-book (bbo channel): one entry per BBO change.
        # Deeper ring than the snapshot buffers — this is the highest-value feed.
        self.bbo_buffer: Dict[str, deque] = {symbol: deque(maxlen=10000) for symbol in self.symbols}
        
        # Connection state
        self.is_connected = False
        self.websocket = None
        self.subscriptions = []
        
        # Callbacks for data processing
        self.data_callbacks: List[Callable[[MarketDataPoint], None]] = []

        # Bounded hand-off queue: the socket read loop only parses and enqueues,
        # a separate consumer task runs the callbacks. This keeps a slow callback
        # (disk flush, DB write) from ever stalling the socket. On overflow the
        # oldest points are dropped so the freshest data always gets through.
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=settings.websocket_queue_maxsize)
        self.dropped_count = 0

        # Lossless capture spool: raw frames are written to hourly JSONL files
        # independent of (and before) the drop-oldest queue above, so load
        # shedding on the processing path can't punch holes in the archive.
        self.spool: Optional[RawSpool] = RawSpool() if settings.spool_enabled else None

        # Gap detection: callbacks fired with a GapEvent when a reconnect leaves
        # a window of missed data. _last_disconnect_seen is the last message time
        # before a drop, used to measure the gap on the next successful connect.
        self.gap_callbacks: List[Callable[["GapEvent"], None]] = []
        self.gap_threshold_seconds = settings.websocket_gap_threshold_seconds
        self._last_disconnect_seen: Optional[datetime] = None

        # Performance tracking
        self.message_count = 0
        self.last_message_time = None
        self.connection_start_time = None

        # Feed latency (exchange event time -> local receive time), per channel.
        # Only channels that carry an exchange timestamp are measured; allMids
        # and activeAssetCtx don't, so they're skipped by design.
        self._latency: Dict[str, LatencyHistogram] = {
            channel: LatencyHistogram() for channel in ('l2Book', 'trades', 'bbo')
        }
        # The first trades message per coin on a connection is a snapshot of
        # recent (old) trades; recording those would poison the latency stats
        # with multi-second phantom deltas. Cleared on every (re)connect.
        self._trades_latency_primed: set = set()
        
    def add_data_callback(self, callback: Callable[[MarketDataPoint], None]):
        """Add a callback function to process incoming data.

        Args:
            callback: Function that processes MarketDataPoint
        """
        self.data_callbacks.append(callback)

    def add_gap_callback(self, callback: Callable[["GapEvent"], None]):
        """Add a callback fired with a GapEvent when a reconnect leaves a gap.

        The callback may be sync or async (an awaitable return is awaited).
        """
        self.gap_callbacks.append(callback)
    
    def create_subscriptions(self) -> List[Dict[str, Any]]:
        """Create WebSocket subscription messages.
        
        Returns:
            List of subscription dictionaries
        """
        subscriptions = []

        # Subscribe to event-level top-of-book first (highest-priority data:
        # pushed per block, only when the best bid/ask actually changes).
        if settings.subscribe_bbo:
            for symbol in self.symbols:
                subscriptions.append({
                    "method": "subscribe",
                    "subscription": {
                        "type": "bbo",
                        "coin": symbol
                    }
                })

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

        # Subscribe to per-asset context (mark/oracle/mid price, open interest,
        # funding, premium) for each symbol.
        for symbol in self.symbols:
            subscriptions.append({
                "method": "subscribe",
                "subscription": {
                    "type": "activeAssetCtx",
                    "coin": symbol
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

    def process_asset_ctx_message(self, message: Dict[str, Any]) -> Optional[MarketDataPoint]:
        """Process an activeAssetCtx message: mark/oracle/mid price, open
        interest, funding, premium, and the mark-vs-oracle basis.

        Args:
            message: WebSocket message

        Returns:
            MarketDataPoint or None
        """
        try:
            data = message.get('data', {})
            coin = data.get('coin')
            if not coin or coin not in self.symbols:
                return None

            ctx = data.get('ctx') or {}

            def _num(key):
                value = ctx.get(key)
                # Reject None and bools (float(True) == 1.0 would slip through).
                if value is None or isinstance(value, bool):
                    return None
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            mark = _num('markPx')
            oracle = _num('oraclePx')
            mid = _num('midPx')
            open_interest = _num('openInterest')
            funding = _num('funding')
            premium = _num('premium')

            # Nothing useful in this message (e.g. empty ctx) — skip it.
            if all(v is None for v in (mark, oracle, mid, open_interest, funding, premium)):
                return None

            # Basis: how far the mark price sits from the oracle (index) price.
            basis = None
            basis_bps = None
            if mark is not None and oracle is not None:
                basis = mark - oracle
                if oracle > 0:
                    basis_bps = basis / oracle * 10000

            timestamp = datetime.now(tz=timezone.utc)
            ctx_data = {
                'mark_price': mark,
                'oracle_price': oracle,
                'mid_price': mid,
                'open_interest': open_interest,
                'funding': funding,
                'premium': premium,
                'basis': basis,
                'basis_bps': basis_bps,
                'timestamp_ms': int(timestamp.timestamp() * 1000),
            }

            data_point = MarketDataPoint(
                timestamp=timestamp,
                symbol=coin,
                data_type='asset_ctx',
                data=ctx_data,
            )

            self.asset_ctx_buffer.setdefault(coin, deque(maxlen=1000)).append(data_point)
            return data_point

        except Exception as e:
            self.logger.error(f"Error processing asset ctx message: {e}")
            return None

    def process_bbo_message(self, message: Dict[str, Any]) -> Optional[MarketDataPoint]:
        """Process a bbo message: event-level top-of-book, pushed per block
        only when the best bid/ask changes.

        data schema: {coin, time (ms), bbo: [bid|null, ask|null]}, each level
        {px, sz, n}. Either side can be null (empty side of book). Raw level
        dicts are kept (string prices), matching the l2Book style.

        Args:
            message: WebSocket message

        Returns:
            MarketDataPoint or None
        """
        try:
            data = message.get('data', {})
            coin = data.get('coin')
            if not coin or coin not in self.symbols:
                return None

            bbo = data.get('bbo') or [None, None]
            time_ms = data.get('time', int(time.time() * 1000))

            bbo_data = {
                'bid': bbo[0] if len(bbo) > 0 else None,
                'ask': bbo[1] if len(bbo) > 1 else None,
                'timestamp_ms': time_ms,
            }

            data_point = MarketDataPoint(
                timestamp=datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc),
                symbol=coin,
                data_type='bbo',
                data=bbo_data,
            )

            self.bbo_buffer.setdefault(coin, deque(maxlen=10000)).append(data_point)
            return data_point

        except Exception as e:
            self.logger.error(f"Error processing bbo message: {e}")
            return None

    def _record_disconnect(self):
        """Remember the last message time at disconnect, to measure the next gap."""
        self._last_disconnect_seen = self.last_message_time

    async def _maybe_emit_gap(self, reconnected_at: datetime) -> Optional["GapEvent"]:
        """Fire gap callbacks if the reconnect left a gap over the threshold.

        Called once per successful (re)connect. Returns the GapEvent if one was
        emitted, else None. Consumes _last_disconnect_seen so a gap fires once.
        """
        start = self._last_disconnect_seen
        self._last_disconnect_seen = None
        if start is None:
            return None  # first connect, or we never received data before the drop

        gap_seconds = (reconnected_at - start).total_seconds()
        if gap_seconds < self.gap_threshold_seconds:
            return None

        event = GapEvent(start=start, end=reconnected_at, symbols=list(self.symbols))
        self.logger.warning(
            f"Detected {gap_seconds:.1f}s data gap after reconnect "
            f"({start.isoformat()} -> {reconnected_at.isoformat()}); requesting backfill"
        )
        for callback in self.gap_callbacks:
            try:
                result = callback(event)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                self.logger.error(f"Error in gap callback: {e}")
        return event

    async def _on_raw_frame(
        self,
        message_str: str,
        recv_ts_ms: float,
        recv_mono_ns: int,
    ):
        """Handle one raw frame from the socket: spool first (lossless, before
        any parsing can fail or shed load), then parse and route."""
        if self.spool is not None:
            self.spool.enqueue(message_str, recv_ts_ms, recv_mono_ns)
        await self.process_message(message_str, recv_ts_ms=recv_ts_ms, recv_mono_ns=recv_mono_ns)

    async def process_message(
        self,
        message_str: str,
        recv_ts_ms: Optional[float] = None,
        recv_mono_ns: Optional[int] = None,
    ):
        """Process incoming WebSocket message.

        Args:
            message_str: Raw message string
            recv_ts_ms: wall-clock ms stamped at the socket read, before parsing
            recv_mono_ns: monotonic ns stamped at the same instant
        """
        try:
            message = json.loads(message_str)
            channel = message.get('channel', '')

            data_points = []
            
            # Route message based on channel
            if channel == 'bbo':
                data_point = self.process_bbo_message(message)
                if data_point:
                    data_points.append(data_point)

            elif channel == 'l2Book':
                data_point = self.process_l2_book_message(message)
                if data_point:
                    data_points.append(data_point)
            
            elif channel == 'trades':
                data_points.extend(self.process_trades_message(message))
            
            elif channel == 'allMids':
                data_points.extend(self.process_ticker_message(message))

            elif channel == 'activeAssetCtx':
                data_point = self.process_asset_ctx_message(message)
                if data_point:
                    data_points.append(data_point)

            elif channel == 'user':
                data_point = self.process_user_message(message)
                if data_point:
                    data_points.append(data_point)
            
            # Stamp local receive time on every point (the buffers hold the same
            # objects, so buffered copies carry the stamps too), and record feed
            # latency for channels that carry an exchange timestamp.
            if recv_ts_ms is not None:
                histogram = self._latency.get(channel)
                skip_symbols = set()
                if channel == 'trades':
                    # Don't record the per-coin subscription snapshot (old trades).
                    skip_symbols = {
                        dp.symbol for dp in data_points
                        if dp.symbol not in self._trades_latency_primed
                    }
                    self._trades_latency_primed.update(dp.symbol for dp in data_points)
                for data_point in data_points:
                    data_point.recv_ts_ms = recv_ts_ms
                    data_point.recv_mono_ns = recv_mono_ns
                    if histogram is not None and data_point.symbol not in skip_symbols:
                        exchange_ms = data_point.data.get('timestamp_ms')
                        if exchange_ms:
                            histogram.record(recv_ts_ms - exchange_ms)

            # Hand off to the processing queue. The consumer task runs the
            # callbacks, so a slow callback can never stall the socket read loop.
            for data_point in data_points:
                self._enqueue(data_point)

            # Update metrics
            self.message_count += len(data_points)
            self.last_message_time = datetime.now(tz=timezone.utc)
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON message: {e}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
    
    def _enqueue(self, data_point: MarketDataPoint):
        """Put a data point on the processing queue, dropping the oldest if full.

        Called from the socket read loop, so it never blocks: when the queue is
        full we discard the oldest queued point to make room for the newest,
        keeping latency bounded and the socket drained. Drops are counted.
        """
        try:
            self._queue.put_nowait(data_point)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:
                pass
            self.dropped_count += 1
            if self.dropped_count % 1000 == 1:
                self.logger.warning(
                    f"Processing queue full; dropping oldest data points "
                    f"(total dropped: {self.dropped_count})"
                )
            try:
                self._queue.put_nowait(data_point)
            except asyncio.QueueFull:
                pass

    async def _consume(self):
        """Drain the queue and run the callbacks, off the socket read loop.

        Callbacks may be sync or return an awaitable; awaitables are awaited
        here (serially) so storage writes apply backpressure to the queue, not
        to the socket. A failing callback is logged and never stops the loop.
        """
        while True:
            data_point = await self._queue.get()
            try:
                for callback in self.data_callbacks:
                    try:
                        result = callback(data_point)
                        if inspect.isawaitable(result):
                            await result
                    except Exception as e:
                        self.logger.error(f"Error in data callback: {e}")
            finally:
                self._queue.task_done()

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

                # New connection: trades snapshots will be re-sent per coin.
                self._trades_latency_primed.clear()

                # If a prior connection dropped, measure the gap and fire backfill.
                await self._maybe_emit_gap(self.connection_start_time)

                # Send subscriptions
                subscriptions = self.create_subscriptions()
                for subscription in subscriptions:
                    await self.send_subscription(websocket, subscription)
                    await asyncio.sleep(0.1)  # Small delay between subscriptions

                self.logger.info(f"Sent {len(subscriptions)} subscriptions")

                # Listen for messages. Stamp receive time BEFORE parsing so the
                # stamp reflects the wire, not json.loads.
                async for message in websocket:
                    await self._on_raw_frame(message, time.time() * 1000, time.monotonic_ns())

        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("WebSocket connection closed")
        except Exception as e:
            self.logger.error(f"WebSocket connection error: {e}")
        finally:
            self.is_connected = False
            self.websocket = None
            # Remember when data stopped, so the next connect can size the gap.
            self._record_disconnect()
    
    def _next_reconnect_delay(self, consecutive_failures: int) -> float:
        """Full-jitter exponential backoff delay for the next reconnect attempt.

        uniform(0, min(cap, base * 2^(n-1))) — the first retry is bounded by the
        base delay, each further consecutive failure doubles the bound up to the
        cap. Jitter avoids synchronized reconnect stampedes after an outage.
        """
        base = float(settings.websocket_reconnect_delay)
        cap = float(settings.websocket_reconnect_max_delay)
        exponent = max(0, consecutive_failures - 1)
        bound = min(cap, base * (2 ** exponent))
        return random.uniform(0, bound)

    async def start_with_reconnect(self):
        """Start WebSocket collector with automatic reconnection.

        The consumer task runs for the whole session (across reconnects) and is
        cancelled when this coroutine is stopped or cancelled. Reconnects use
        full-jitter exponential backoff; the failure streak resets only once a
        connection actually delivers a message, so a connect-accept-then-drop
        loop cannot hammer the endpoint at the base rate.
        """
        consumer = asyncio.create_task(self._consume())
        if self.spool is not None:
            self.spool.start()
        consecutive_failures = 0
        try:
            while True:
                messages_before = self.message_count
                try:
                    await self.connect()
                except Exception as e:
                    self.logger.error(f"Connection failed: {e}")

                if self.message_count > messages_before:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1

                if not self.is_connected:
                    delay = self._next_reconnect_delay(consecutive_failures)
                    self.logger.info(f"Reconnecting in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)
        finally:
            consumer.cancel()
            try:
                await consumer
            except asyncio.CancelledError:
                pass
            if self.spool is not None:
                try:
                    await self.spool.close()  # drain fully, finalize + upload
                except Exception as e:
                    self.logger.error(f"Error closing spool: {e}")

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
        elif data_type == 'asset_ctx':
            buffer = self.asset_ctx_buffer.get(symbol, deque())
        elif data_type == 'bbo':
            buffer = self.bbo_buffer.get(symbol, deque())
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
                'ticker': len(self.ticker_buffer.get(symbol, [])),
                'asset_ctx': len(self.asset_ctx_buffer.get(symbol, [])),
                'bbo': len(self.bbo_buffer.get(symbol, []))
            }
        
        return {
            'is_connected': self.is_connected,
            'uptime_seconds': uptime,
            'message_count': self.message_count,
            'last_message_time': self.last_message_time,
            'queue_depth': self._queue.qsize(),
            'queue_maxsize': self._queue.maxsize,
            'dropped_count': self.dropped_count,
            'buffer_sizes': buffer_sizes,
            'symbols': self.symbols,
            'latency_ms': {
                channel: histogram.snapshot()
                for channel, histogram in self._latency.items()
            },
            'spool': self.spool.stats() if self.spool is not None else None,
        }


class DataLogger:
    """Logs market data to files."""
    
    def __init__(self, output_dir: str = None, object_store: "Optional[ObjectStore]" = None):
        """Initialize data logger.

        Args:
            output_dir: Directory to save data files
            object_store: optional S3-compatible sink for finished JSONL files;
                defaults to the configured store. Pass explicitly in tests.
        """
        self.output_dir = Path(output_dir) if output_dir else settings.real_time_data_path
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Open file handle per filepath, plus the current filepath per
        # (symbol, data_type) so a date rollover can finalize the prior day.
        self.file_handles = {}
        self._current: Dict[Tuple[str, str], Path] = {}
        self.logger = logger.bind(component="data_logger")

        if object_store is not None:
            self.object_store = object_store
        else:
            from ..storage.object_store import get_object_store  # deferred: import cycle
            self.object_store = get_object_store()

    def _object_key(self, filepath: Path) -> str:
        """Object-store key for a JSONL file.

        Namespaced by output_dir so two loggers writing different directories
        don't collide on basename alone (uploads are whole-object overwrites).
        """
        return f"realtime/{self.output_dir.name}/{filepath.name}"

    def _finalize_file(self, filepath: Path):
        """Close a file's handle and mirror it to the object store, if configured."""
        handle = self.file_handles.pop(filepath, None)
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
        if self.object_store:
            try:
                self.object_store.put_file(filepath, self._object_key(filepath))
            except Exception as e:
                self.logger.error(f"Failed to upload {filepath} to object store: {e}")

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

            # On a date rollover, finalize and upload the completed day's file
            # now, so a later crash can't lose an already-finished day.
            stream = (data_point.symbol, data_point.data_type)
            prev = self._current.get(stream)
            if prev is not None and prev != filepath:
                self._finalize_file(prev)

            # Open file if not already open
            if filepath not in self.file_handles:
                self.file_handles[filepath] = open(filepath, 'a')
            self._current[stream] = filepath

            # Write data point as JSON line
            data_dict = asdict(data_point)
            data_dict['timestamp'] = data_point.timestamp.isoformat()

            self.file_handles[filepath].write(json.dumps(data_dict) + '\n')
            self.file_handles[filepath].flush()

        except Exception as e:
            self.logger.error(f"Error logging data point: {e}")

    def close_all_files(self):
        """Close all open file handles, uploading each to the object store if configured."""
        for filepath in list(self.file_handles.keys()):
            self._finalize_file(filepath)
        self._current.clear()


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