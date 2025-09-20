#!/usr/bin/env python3
"""Monitor SOL data collection and display live stats."""

import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from hyperliquid_pipeline.collectors.realtime_collector import HyperliquidWebSocketCollector
from hyperliquid_pipeline.utils.validation import ValidationCallback
from loguru import logger


class SOLDataMonitor:
    """Real-time SOL data monitor with statistics."""
    
    def __init__(self):
        """Initialize the monitor."""
        self.collector = HyperliquidWebSocketCollector(['SOL'])
        self.validation_callback = ValidationCallback()
        
        # Statistics
        self.stats = {
            'start_time': datetime.now(timezone.utc),
            'total_messages': 0,
            'trades': 0,
            'orderbook_updates': 0,
            'ticker_updates': 0,
            'validation_errors': 0,
            'validation_warnings': 0,
            'latest_price': None,
            'price_range': {'high': 0, 'low': float('inf')},
            'last_trade_time': None,
            'message_rate': 0  # messages per second
        }
        
        # Price tracking
        self.recent_prices = []
        self.recent_volumes = []
        
        # Set up data callback
        self.collector.add_data_callback(self.process_data_point)
    
    def process_data_point(self, data_point):
        """Process incoming data points and update statistics."""
        try:
            # Validate data
            validated_data = self.validation_callback(data_point)
            if not validated_data:
                self.stats['validation_errors'] += 1
                return
            
            # Update general stats
            self.stats['total_messages'] += 1
            self.stats['last_message_time'] = data_point.timestamp
            
            # Update by data type
            if data_point.data_type == 'trade':
                self.stats['trades'] += 1
                self.stats['last_trade_time'] = data_point.timestamp
                
                # Update price statistics
                price = data_point.data.get('price', 0)
                volume = data_point.data.get('size', 0)
                
                if price > 0:
                    self.stats['latest_price'] = price
                    self.stats['price_range']['high'] = max(self.stats['price_range']['high'], price)
                    self.stats['price_range']['low'] = min(self.stats['price_range']['low'], price)
                    
                    # Keep recent data for analysis
                    self.recent_prices.append(price)
                    self.recent_volumes.append(volume)
                    
                    # Keep only last 100 data points
                    if len(self.recent_prices) > 100:
                        self.recent_prices = self.recent_prices[-100:]
                        self.recent_volumes = self.recent_volumes[-100:]
                        
            elif data_point.data_type == 'orderbook':
                self.stats['orderbook_updates'] += 1
                
            elif data_point.data_type == 'ticker':
                self.stats['ticker_updates'] += 1
            
            # Check for validation warnings
            validation_results = self.validation_callback.validator.validation_results
            recent_results = [r for r in validation_results[-10:] if r.data_point == data_point]
            for result in recent_results:
                if result.level.value == 'warning':
                    self.stats['validation_warnings'] += 1
            
        except Exception as e:
            logger.error(f"Error processing data point: {e}")
            self.stats['validation_errors'] += 1
    
    def calculate_metrics(self):
        """Calculate derived metrics."""
        now = datetime.now(timezone.utc)
        uptime = (now - self.stats['start_time']).total_seconds()
        
        # Message rate
        if uptime > 0:
            self.stats['message_rate'] = self.stats['total_messages'] / uptime
        
        # Price volatility (if we have enough data)
        volatility = 0
        if len(self.recent_prices) > 1:
            prices = self.recent_prices
            avg_price = sum(prices) / len(prices)
            volatility = (max(prices) - min(prices)) / avg_price * 100 if avg_price > 0 else 0
        
        # Volume metrics
        total_volume = sum(self.recent_volumes)
        avg_volume = total_volume / len(self.recent_volumes) if self.recent_volumes else 0
        
        return {
            'uptime_seconds': uptime,
            'price_volatility_pct': volatility,
            'total_volume': total_volume,
            'avg_volume': avg_volume
        }
    
    def print_stats(self):
        """Print current statistics."""
        metrics = self.calculate_metrics()
        
        print("\n" + "="*60)
        print(f"SOL Data Monitor - {datetime.now().strftime('%H:%M:%S')}")
        print("="*60)
        
        # Connection status
        collector_stats = self.collector.get_stats()
        print(f"📡 Connection: {'🟢 Connected' if collector_stats['is_connected'] else '🔴 Disconnected'}")
        print(f"⏱️  Uptime: {metrics['uptime_seconds']:.1f}s")
        
        # Message statistics
        print(f"\n📊 Messages:")
        print(f"   Total: {self.stats['total_messages']:,}")
        print(f"   Rate: {self.stats['message_rate']:.1f}/sec")
        print(f"   Trades: {self.stats['trades']:,}")
        print(f"   Orderbook: {self.stats['orderbook_updates']:,}")
        print(f"   Tickers: {self.stats['ticker_updates']:,}")
        
        # Price information
        if self.stats['latest_price']:
            print(f"\n💰 SOL Price:")
            print(f"   Current: ${self.stats['latest_price']:.4f}")
            print(f"   Range: ${self.stats['price_range']['low']:.4f} - ${self.stats['price_range']['high']:.4f}")
            print(f"   Volatility: {metrics['price_volatility_pct']:.2f}%")
        
        # Volume information
        if self.recent_volumes:
            print(f"\n📈 Volume:")
            print(f"   Total: {metrics['total_volume']:.4f} SOL")
            print(f"   Average: {metrics['avg_volume']:.4f} SOL")
        
        # Data quality
        print(f"\n✅ Data Quality:")
        print(f"   Errors: {self.stats['validation_errors']}")
        print(f"   Warnings: {self.stats['validation_warnings']}")
        
        # Last update times
        if self.stats['last_trade_time']:
            last_trade_age = (datetime.now(timezone.utc) - self.stats['last_trade_time']).total_seconds()
            print(f"   Last trade: {last_trade_age:.1f}s ago")
    
    async def run_monitor(self, duration_minutes: int = 5):
        """Run the data monitor for specified duration."""
        print(f"🚀 Starting SOL data monitor for {duration_minutes} minutes...")
        print("Press Ctrl+C to stop early")
        
        # Start collector in background
        collector_task = asyncio.create_task(self.collector.start_with_reconnect())
        
        # Stats printing loop
        end_time = datetime.now(timezone.utc).timestamp() + (duration_minutes * 60)
        
        try:
            while datetime.now(timezone.utc).timestamp() < end_time:
                self.print_stats()
                await asyncio.sleep(5)  # Update every 5 seconds
                
        except KeyboardInterrupt:
            print("\n\n⏹️  Monitor stopped by user")
        
        finally:
            collector_task.cancel()
            try:
                await collector_task
            except asyncio.CancelledError:
                pass
        
        # Final stats
        print("\n" + "="*60)
        print("📋 FINAL STATISTICS")
        print("="*60)
        self.print_stats()
        
        # Data quality report
        if self.validation_callback.validator.validation_results:
            quality_report = self.validation_callback.validator.generate_quality_report(['SOL'])
            print(f"\n📊 Quality Report:")
            sol_quality = quality_report['symbols'].get('SOL', {})
            if sol_quality:
                print(f"   Completeness: {sol_quality.get('completeness_ratio', 0):.1%}")
                print(f"   Accuracy: {sol_quality.get('accuracy_score', 0):.1%}")


async def main():
    """Main function."""
    # Configure logging to be less verbose
    logger.remove()
    logger.add(sys.stderr, level="WARNING", format="<level>{level}</level>: {message}")
    
    monitor = SOLDataMonitor()
    
    # Run for 5 minutes by default
    duration = 5
    if len(sys.argv) > 1:
        try:
            duration = int(sys.argv[1])
        except ValueError:
            print("Usage: python monitor_sol_data.py [duration_in_minutes]")
            sys.exit(1)
    
    await monitor.run_monitor(duration)


if __name__ == "__main__":
    asyncio.run(main())