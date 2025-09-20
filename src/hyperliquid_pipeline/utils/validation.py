"""Data validation and quality checks for market data."""

import asyncio
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import numpy as np
from loguru import logger

from ..collectors.realtime_collector import MarketDataPoint


class ValidationLevel(Enum):
    """Validation severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    level: ValidationLevel
    message: str
    data_point: Optional[MarketDataPoint] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class DataQualityMetrics:
    """Data quality metrics for a symbol."""
    symbol: str
    total_points: int
    error_count: int
    warning_count: int
    missing_data_periods: List[Tuple[datetime, datetime]]
    duplicate_count: int
    outlier_count: int
    data_freshness: timedelta
    completeness_ratio: float
    accuracy_score: float


class DataValidator:
    """Validates market data quality and consistency."""
    
    def __init__(self):
        """Initialize data validator."""
        self.logger = logger.bind(component="data_validator")
        
        # Historical data for comparison
        self.price_history: Dict[str, List[float]] = {}
        self.volume_history: Dict[str, List[float]] = {}
        self.timestamp_history: Dict[str, List[datetime]] = {}
        
        # Validation thresholds
        self.price_change_threshold = 0.10  # 10% max price change
        self.volume_spike_threshold = 10.0   # 10x volume spike
        self.data_freshness_threshold = timedelta(minutes=5)
        self.duplicate_time_threshold = timedelta(milliseconds=100)
        
        # Track validation results
        self.validation_results: List[ValidationResult] = []
    
    def validate_price_data(self, data_point: MarketDataPoint) -> List[ValidationResult]:
        """Validate price-related data.
        
        Args:
            data_point: Market data point to validate
            
        Returns:
            List of validation results
        """
        results = []
        symbol = data_point.symbol
        
        if data_point.data_type == 'trade':
            price = data_point.data.get('price', 0)
            size = data_point.data.get('size', 0)
            
            # Basic value checks
            if price <= 0:
                results.append(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message=f"Invalid price: {price}",
                    data_point=data_point
                ))
            
            if size <= 0:
                results.append(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message=f"Invalid size: {size}",
                    data_point=data_point
                ))
            
            # Price change validation
            if symbol in self.price_history and self.price_history[symbol]:
                last_price = self.price_history[symbol][-1]
                price_change = abs(price - last_price) / last_price if last_price > 0 else 0
                
                if price_change > self.price_change_threshold:
                    results.append(ValidationResult(
                        level=ValidationLevel.WARNING,
                        message=f"Large price change: {price_change:.2%}",
                        data_point=data_point,
                        metadata={'last_price': last_price, 'current_price': price}
                    ))
            
            # Update price history
            if symbol not in self.price_history:
                self.price_history[symbol] = []
            self.price_history[symbol].append(price)
            
            # Keep only recent history
            if len(self.price_history[symbol]) > 1000:
                self.price_history[symbol] = self.price_history[symbol][-1000:]
        
        elif data_point.data_type == 'orderbook':
            bids = data_point.data.get('bids', [])
            asks = data_point.data.get('asks', [])
            
            # Orderbook structure validation
            if not bids:
                results.append(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message="Empty bids in orderbook",
                    data_point=data_point
                ))
            
            if not asks:
                results.append(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message="Empty asks in orderbook",
                    data_point=data_point
                ))
            
            # Price ordering validation
            if bids:
                bid_prices = [float(level.get('px', 0)) for level in bids]
                if bid_prices != sorted(bid_prices, reverse=True):
                    results.append(ValidationResult(
                        level=ValidationLevel.ERROR,
                        message="Bids not sorted in descending order",
                        data_point=data_point
                    ))
            
            if asks:
                ask_prices = [float(level.get('px', 0)) for level in asks]
                if ask_prices != sorted(ask_prices):
                    results.append(ValidationResult(
                        level=ValidationLevel.ERROR,
                        message="Asks not sorted in ascending order",
                        data_point=data_point
                    ))
            
            # Spread validation
            if bids and asks:
                best_bid = float(bids[0].get('px', 0))
                best_ask = float(asks[0].get('px', 0))
                
                if best_bid >= best_ask:
                    results.append(ValidationResult(
                        level=ValidationLevel.CRITICAL,
                        message=f"Crossed book: bid {best_bid} >= ask {best_ask}",
                        data_point=data_point
                    ))
                
                spread = best_ask - best_bid
                spread_pct = spread / best_bid if best_bid > 0 else 0
                
                if spread_pct > 0.05:  # 5% spread
                    results.append(ValidationResult(
                        level=ValidationLevel.WARNING,
                        message=f"Wide spread: {spread_pct:.2%}",
                        data_point=data_point
                    ))
        
        return results
    
    def validate_volume_data(self, data_point: MarketDataPoint) -> List[ValidationResult]:
        """Validate volume-related data.
        
        Args:
            data_point: Market data point to validate
            
        Returns:
            List of validation results
        """
        results = []
        symbol = data_point.symbol
        
        if data_point.data_type == 'trade':
            volume = data_point.data.get('size', 0)
            
            # Volume spike detection
            if symbol in self.volume_history and self.volume_history[symbol]:
                recent_volumes = self.volume_history[symbol][-10:]  # Last 10 trades
                avg_volume = sum(recent_volumes) / len(recent_volumes)
                
                if volume > avg_volume * self.volume_spike_threshold:
                    results.append(ValidationResult(
                        level=ValidationLevel.WARNING,
                        message=f"Volume spike: {volume:.4f} vs avg {avg_volume:.4f}",
                        data_point=data_point,
                        metadata={'volume': volume, 'avg_volume': avg_volume}
                    ))
            
            # Update volume history
            if symbol not in self.volume_history:
                self.volume_history[symbol] = []
            self.volume_history[symbol].append(volume)
            
            # Keep only recent history
            if len(self.volume_history[symbol]) > 1000:
                self.volume_history[symbol] = self.volume_history[symbol][-1000:]
        
        return results
    
    def validate_timestamp_data(self, data_point: MarketDataPoint) -> List[ValidationResult]:
        """Validate timestamp-related data.
        
        Args:
            data_point: Market data point to validate
            
        Returns:
            List of validation results
        """
        results = []
        symbol = data_point.symbol
        timestamp = data_point.timestamp
        
        # Future timestamp check
        now = datetime.now(timezone.utc)
        if timestamp > now + timedelta(seconds=10):
            results.append(ValidationResult(
                level=ValidationLevel.ERROR,
                message=f"Future timestamp: {timestamp}",
                data_point=data_point
            ))
        
        # Data freshness check
        if now - timestamp > self.data_freshness_threshold:
            results.append(ValidationResult(
                level=ValidationLevel.WARNING,
                message=f"Stale data: {now - timestamp}",
                data_point=data_point
            ))
        
        # Duplicate timestamp check
        if symbol in self.timestamp_history:
            for hist_timestamp in self.timestamp_history[symbol]:
                if abs((timestamp - hist_timestamp).total_seconds()) < self.duplicate_time_threshold.total_seconds():
                    results.append(ValidationResult(
                        level=ValidationLevel.WARNING,
                        message=f"Duplicate timestamp: {timestamp}",
                        data_point=data_point
                    ))
                    break
        
        # Update timestamp history
        if symbol not in self.timestamp_history:
            self.timestamp_history[symbol] = []
        self.timestamp_history[symbol].append(timestamp)
        
        # Keep only recent history
        if len(self.timestamp_history[symbol]) > 100:
            self.timestamp_history[symbol] = self.timestamp_history[symbol][-100:]
        
        return results
    
    def validate_data_point(self, data_point: MarketDataPoint) -> List[ValidationResult]:
        """Validate a complete data point.
        
        Args:
            data_point: Market data point to validate
            
        Returns:
            List of validation results
        """
        all_results = []
        
        try:
            # Basic structure validation
            if not data_point.symbol:
                all_results.append(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message="Missing symbol",
                    data_point=data_point
                ))
            
            if not data_point.data_type:
                all_results.append(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message="Missing data_type",
                    data_point=data_point
                ))
            
            if not data_point.data:
                all_results.append(ValidationResult(
                    level=ValidationLevel.ERROR,
                    message="Missing data",
                    data_point=data_point
                ))
            
            # Specific validations
            all_results.extend(self.validate_price_data(data_point))
            all_results.extend(self.validate_volume_data(data_point))
            all_results.extend(self.validate_timestamp_data(data_point))
            
        except Exception as e:
            all_results.append(ValidationResult(
                level=ValidationLevel.CRITICAL,
                message=f"Validation error: {e}",
                data_point=data_point
            ))
        
        # Store results
        self.validation_results.extend(all_results)
        
        # Log critical and error results
        for result in all_results:
            if result.level in [ValidationLevel.CRITICAL, ValidationLevel.ERROR]:
                self.logger.error(f"Validation {result.level.value}: {result.message}")
            elif result.level == ValidationLevel.WARNING:
                self.logger.warning(f"Validation {result.level.value}: {result.message}")
        
        return all_results
    
    def get_quality_metrics(self, symbol: str, hours: int = 24) -> DataQualityMetrics:
        """Calculate data quality metrics for a symbol.
        
        Args:
            symbol: Trading symbol
            hours: Number of hours to analyze
            
        Returns:
            Data quality metrics
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        # Filter results for the symbol and time period
        relevant_results = [
            result for result in self.validation_results
            if result.data_point and 
               result.data_point.symbol == symbol and
               result.data_point.timestamp >= cutoff_time
        ]
        
        total_points = len(relevant_results)
        error_count = len([r for r in relevant_results if r.level == ValidationLevel.ERROR])
        warning_count = len([r for r in relevant_results if r.level == ValidationLevel.WARNING])
        
        # Calculate completeness and accuracy
        completeness_ratio = 1.0 - (error_count / total_points) if total_points > 0 else 0.0
        accuracy_score = 1.0 - ((error_count + warning_count * 0.5) / total_points) if total_points > 0 else 0.0
        
        # Data freshness
        if symbol in self.timestamp_history and self.timestamp_history[symbol]:
            latest_timestamp = max(self.timestamp_history[symbol])
            data_freshness = datetime.now(timezone.utc) - latest_timestamp
        else:
            data_freshness = timedelta(hours=24)  # Unknown
        
        return DataQualityMetrics(
            symbol=symbol,
            total_points=total_points,
            error_count=error_count,
            warning_count=warning_count,
            missing_data_periods=[],  # Would need more complex analysis
            duplicate_count=len([r for r in relevant_results if "duplicate" in r.message.lower()]),
            outlier_count=len([r for r in relevant_results if "spike" in r.message.lower() or "change" in r.message.lower()]),
            data_freshness=data_freshness,
            completeness_ratio=completeness_ratio,
            accuracy_score=accuracy_score
        )
    
    def generate_quality_report(self, symbols: List[str] = None) -> Dict[str, Any]:
        """Generate a comprehensive data quality report.
        
        Args:
            symbols: List of symbols to include (default: all)
            
        Returns:
            Quality report dictionary
        """
        if symbols is None:
            symbols = list(set([
                result.data_point.symbol for result in self.validation_results
                if result.data_point
            ]))
        
        report = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'symbols': {},
            'summary': {
                'total_symbols': len(symbols),
                'total_validation_results': len(self.validation_results),
                'error_count': len([r for r in self.validation_results if r.level == ValidationLevel.ERROR]),
                'warning_count': len([r for r in self.validation_results if r.level == ValidationLevel.WARNING]),
                'critical_count': len([r for r in self.validation_results if r.level == ValidationLevel.CRITICAL])
            }
        }
        
        for symbol in symbols:
            metrics = self.get_quality_metrics(symbol)
            report['symbols'][symbol] = {
                'total_points': metrics.total_points,
                'error_count': metrics.error_count,
                'warning_count': metrics.warning_count,
                'completeness_ratio': metrics.completeness_ratio,
                'accuracy_score': metrics.accuracy_score,
                'data_freshness_seconds': metrics.data_freshness.total_seconds(),
                'outlier_count': metrics.outlier_count,
                'duplicate_count': metrics.duplicate_count
            }
        
        return report
    
    def clear_old_results(self, hours: int = 24):
        """Clear validation results older than specified hours.
        
        Args:
            hours: Number of hours to keep
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        self.validation_results = [
            result for result in self.validation_results
            if result.data_point and result.data_point.timestamp >= cutoff_time
        ]


class DataSanitizer:
    """Sanitizes and cleans market data."""
    
    def __init__(self):
        """Initialize data sanitizer."""
        self.logger = logger.bind(component="data_sanitizer")
    
    def sanitize_trade_data(self, data_point: MarketDataPoint) -> Optional[MarketDataPoint]:
        """Sanitize trade data.
        
        Args:
            data_point: Trade data point
            
        Returns:
            Sanitized data point or None if invalid
        """
        if data_point.data_type != 'trade':
            return data_point
        
        try:
            # Extract and validate price and size
            price = float(data_point.data.get('price', 0))
            size = float(data_point.data.get('size', 0))
            side = data_point.data.get('side', '').lower()
            
            # Basic validation
            if price <= 0 or size <= 0:
                self.logger.warning(f"Invalid trade data: price={price}, size={size}")
                return None
            
            if side not in ['buy', 'sell', 'bid', 'ask']:
                self.logger.warning(f"Unknown trade side: {side}")
                # Try to infer or set default
                side = 'unknown'
            
            # Create sanitized data point
            sanitized_data = {
                'price': round(price, 8),  # Round to 8 decimal places
                'size': round(size, 8),
                'side': side,
                'timestamp_ms': data_point.data.get('timestamp_ms', int(data_point.timestamp.timestamp() * 1000))
            }
            
            return MarketDataPoint(
                timestamp=data_point.timestamp,
                symbol=data_point.symbol,
                data_type=data_point.data_type,
                data=sanitized_data
            )
            
        except Exception as e:
            self.logger.error(f"Error sanitizing trade data: {e}")
            return None
    
    def sanitize_orderbook_data(self, data_point: MarketDataPoint) -> Optional[MarketDataPoint]:
        """Sanitize orderbook data.
        
        Args:
            data_point: Orderbook data point
            
        Returns:
            Sanitized data point or None if invalid
        """
        if data_point.data_type != 'orderbook':
            return data_point
        
        try:
            bids = data_point.data.get('bids', [])
            asks = data_point.data.get('asks', [])
            
            # Sanitize bids
            sanitized_bids = []
            for bid in bids:
                try:
                    price = float(bid.get('px', 0))
                    size = float(bid.get('sz', 0))
                    if price > 0 and size > 0:
                        sanitized_bids.append({
                            'px': round(price, 8),
                            'sz': round(size, 8)
                        })
                except (ValueError, TypeError):
                    continue
            
            # Sanitize asks
            sanitized_asks = []
            for ask in asks:
                try:
                    price = float(ask.get('px', 0))
                    size = float(ask.get('sz', 0))
                    if price > 0 and size > 0:
                        sanitized_asks.append({
                            'px': round(price, 8),
                            'sz': round(size, 8)
                        })
                except (ValueError, TypeError):
                    continue
            
            # Sort bids (descending) and asks (ascending)
            sanitized_bids.sort(key=lambda x: x['px'], reverse=True)
            sanitized_asks.sort(key=lambda x: x['px'])
            
            # Validate orderbook
            if not sanitized_bids or not sanitized_asks:
                self.logger.warning(f"Empty orderbook after sanitization")
                return None
            
            if sanitized_bids[0]['px'] >= sanitized_asks[0]['px']:
                self.logger.warning(f"Crossed book after sanitization")
                return None
            
            sanitized_data = {
                'bids': sanitized_bids,
                'asks': sanitized_asks,
                'timestamp_ms': data_point.data.get('timestamp_ms', int(data_point.timestamp.timestamp() * 1000))
            }
            
            return MarketDataPoint(
                timestamp=data_point.timestamp,
                symbol=data_point.symbol,
                data_type=data_point.data_type,
                data=sanitized_data
            )
            
        except Exception as e:
            self.logger.error(f"Error sanitizing orderbook data: {e}")
            return None
    
    def sanitize_data_point(self, data_point: MarketDataPoint) -> Optional[MarketDataPoint]:
        """Sanitize any type of data point.
        
        Args:
            data_point: Data point to sanitize
            
        Returns:
            Sanitized data point or None if invalid
        """
        if data_point.data_type == 'trade':
            return self.sanitize_trade_data(data_point)
        elif data_point.data_type == 'orderbook':
            return self.sanitize_orderbook_data(data_point)
        else:
            return data_point  # No sanitization needed for other types


# Validation callback for real-time data
class ValidationCallback:
    """Callback for validating real-time data."""
    
    def __init__(self):
        """Initialize validation callback."""
        self.validator = DataValidator()
        self.sanitizer = DataSanitizer()
        self.logger = logger.bind(component="validation_callback")
    
    def __call__(self, data_point: MarketDataPoint) -> Optional[MarketDataPoint]:
        """Validate and sanitize a data point.
        
        Args:
            data_point: Data point to process
            
        Returns:
            Sanitized data point or None if invalid
        """
        # First sanitize the data
        sanitized_data = self.sanitizer.sanitize_data_point(data_point)
        
        if sanitized_data is None:
            return None
        
        # Then validate the sanitized data
        validation_results = self.validator.validate_data_point(sanitized_data)
        
        # Check if there are any critical errors
        critical_errors = [r for r in validation_results if r.level == ValidationLevel.CRITICAL]
        
        if critical_errors:
            self.logger.error(f"Critical validation errors for {data_point.symbol}: {[r.message for r in critical_errors]}")
            return None
        
        return sanitized_data


async def main():
    """Example usage of validation components."""
    validator = DataValidator()
    sanitizer = DataSanitizer()
    
    # Example trade data
    trade_data = MarketDataPoint(
        timestamp=datetime.now(timezone.utc),
        symbol='BTC',
        data_type='trade',
        data={
            'price': 50000.0,
            'size': 0.1,
            'side': 'buy'
        }
    )
    
    # Validate data
    results = validator.validate_data_point(trade_data)
    print(f"Validation results: {len(results)}")
    
    # Generate quality report
    report = validator.generate_quality_report(['BTC'])
    print(f"Quality report: {report}")


if __name__ == "__main__":
    asyncio.run(main())