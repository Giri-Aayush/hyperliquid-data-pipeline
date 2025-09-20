"""Historical data collector for Hyperliquid S3 archives."""

import asyncio
import lz4.frame
import boto3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Generator
from dataclasses import dataclass
from loguru import logger
import json
from concurrent.futures import ThreadPoolExecutor
import time

from ..config import settings


@dataclass
class HistoricalDataRequest:
    """Request for historical data download."""
    symbol: str
    date: str  # Format: YYYYMMDD
    hour: int
    data_type: str  # 'l2Book', 'trades', 'candles'
    
    
@dataclass
class S3Location:
    """S3 location for historical data."""
    bucket: str
    key: str
    

class HistoricalDataCollector:
    """Collects historical data from Hyperliquid S3 archives."""
    
    def __init__(self):
        """Initialize the historical data collector."""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_default_region
        )
        self.local_data_path = settings.historical_data_path
        self.logger = logger.bind(component="historical_collector")
        
    def generate_date_range(
        self, 
        start_date: str, 
        end_date: Optional[str] = None
    ) -> Generator[str, None, None]:
        """Generate date range for data collection.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (default: yesterday)
            
        Yields:
            Date strings in YYYYMMDD format
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        if end_date is None:
            end = datetime.now() - timedelta(days=1)  # Yesterday
        else:
            end = datetime.strptime(end_date, "%Y-%m-%d")
            
        current = start
        while current <= end:
            yield current.strftime("%Y%m%d")
            current += timedelta(days=1)
    
    def get_s3_location(self, request: HistoricalDataRequest) -> S3Location:
        """Get S3 location for a data request.
        
        Args:
            request: Historical data request
            
        Returns:
            S3Location with bucket and key
        """
        if request.data_type in ['l2Book', 'trades']:
            # Market data format: s3://hyperliquid-archive/market_data/[date]/[hour]/[datatype]/[coin].lz4
            key = f"market_data/{request.date}/{request.hour}/{request.data_type}/{request.symbol}.lz4"
            return S3Location(bucket=settings.hyperliquid_archive_bucket, key=key)
        elif request.data_type == 'asset_ctxs':
            # Asset context format: s3://hyperliquid-archive/asset_ctxs/[date].csv.lz4
            key = f"asset_ctxs/{request.date}.csv.lz4"
            return S3Location(bucket=settings.hyperliquid_archive_bucket, key=key)
        elif request.data_type == 'node_fills':
            # Node fills format: s3://hl-mainnet-node-data/node_fills_by_block/[date]/[file]
            key = f"node_fills_by_block/{request.date}/"
            return S3Location(bucket=settings.node_data_bucket, key=key)
        else:
            raise ValueError(f"Unknown data type: {request.data_type}")
    
    def download_file(self, s3_location: S3Location, local_path: Path) -> bool:
        """Download a file from S3.
        
        Args:
            s3_location: S3 location to download from
            local_path: Local path to save the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create parent directories
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Download with requester pays
            self.s3_client.download_file(
                Bucket=s3_location.bucket,
                Key=s3_location.key,
                Filename=str(local_path),
                ExtraArgs={'RequestPayer': settings.request_payer}
            )
            
            self.logger.info(f"Downloaded {s3_location.bucket}/{s3_location.key} to {local_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to download {s3_location.bucket}/{s3_location.key}: {e}")
            return False
    
    def decompress_lz4_file(self, compressed_path: Path, output_path: Path) -> bool:
        """Decompress an LZ4 file.
        
        Args:
            compressed_path: Path to compressed LZ4 file
            output_path: Path for decompressed output
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(compressed_path, 'rb') as compressed_file:
                with open(output_path, 'wb') as output_file:
                    decompressed_data = lz4.frame.decompress(compressed_file.read())
                    output_file.write(decompressed_data)
            
            # Remove compressed file to save space
            compressed_path.unlink()
            
            self.logger.info(f"Decompressed {compressed_path} to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to decompress {compressed_path}: {e}")
            return False
    
    def process_l2_book_data(self, file_path: Path) -> pd.DataFrame:
        """Process L2 orderbook data file.
        
        Args:
            file_path: Path to the data file
            
        Returns:
            Processed DataFrame
        """
        try:
            data_records = []
            
            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line.strip())
                        data_records.append({
                            'timestamp': record.get('time', 0),
                            'symbol': record.get('coin', ''),
                            'bids': record.get('levels', [[]])[0],
                            'asks': record.get('levels', [[], []])[1],
                        })
            
            df = pd.DataFrame(data_records)
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to process L2 book data from {file_path}: {e}")
            return pd.DataFrame()
    
    def process_trades_data(self, file_path: Path) -> pd.DataFrame:
        """Process trades data file.
        
        Args:
            file_path: Path to the data file
            
        Returns:
            Processed DataFrame
        """
        try:
            data_records = []
            
            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line.strip())
                        data_records.append({
                            'timestamp': record.get('time', 0),
                            'symbol': record.get('coin', ''),
                            'price': float(record.get('px', 0)),
                            'size': float(record.get('sz', 0)),
                            'side': record.get('side', ''),
                        })
            
            df = pd.DataFrame(data_records)
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df.set_index('timestamp', inplace=True)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to process trades data from {file_path}: {e}")
            return pd.DataFrame()
    
    async def download_historical_data(
        self,
        symbols: List[str],
        start_date: str,
        end_date: Optional[str] = None,
        data_types: List[str] = None,
        max_workers: int = 4
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Download historical data for multiple symbols and dates.
        
        Args:
            symbols: List of symbols to download
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            data_types: List of data types to download
            max_workers: Maximum number of concurrent downloads
            
        Returns:
            Dictionary with symbol -> data_type -> DataFrame structure
        """
        if data_types is None:
            data_types = ['l2Book', 'trades']
        
        results = {symbol: {dt: pd.DataFrame() for dt in data_types} for symbol in symbols}
        
        # Generate all download requests
        requests = []
        for symbol in symbols:
            for date_str in self.generate_date_range(start_date, end_date):
                for data_type in data_types:
                    if data_type in ['l2Book', 'trades']:
                        # Market data available hourly
                        for hour in range(24):
                            requests.append(HistoricalDataRequest(
                                symbol=symbol,
                                date=date_str,
                                hour=hour,
                                data_type=data_type
                            ))
                    else:
                        # Asset contexts are daily
                        requests.append(HistoricalDataRequest(
                            symbol=symbol,
                            date=date_str,
                            hour=0,
                            data_type=data_type
                        ))
        
        self.logger.info(f"Downloading {len(requests)} historical data files...")
        
        # Process downloads with thread pool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            loop = asyncio.get_event_loop()
            
            async def download_and_process(request: HistoricalDataRequest):
                """Download and process a single request."""
                try:
                    s3_location = self.get_s3_location(request)
                    
                    # Local file paths
                    compressed_path = (
                        self.local_data_path / 
                        request.data_type / 
                        request.symbol / 
                        f"{request.date}_{request.hour:02d}.lz4"
                    )
                    decompressed_path = compressed_path.with_suffix('')
                    
                    # Download file
                    success = await loop.run_in_executor(
                        executor, 
                        self.download_file, 
                        s3_location, 
                        compressed_path
                    )
                    
                    if not success:
                        return None
                    
                    # Decompress file
                    success = await loop.run_in_executor(
                        executor,
                        self.decompress_lz4_file,
                        compressed_path,
                        decompressed_path
                    )
                    
                    if not success:
                        return None
                    
                    # Process data based on type
                    if request.data_type == 'l2Book':
                        df = await loop.run_in_executor(
                            executor,
                            self.process_l2_book_data,
                            decompressed_path
                        )
                    elif request.data_type == 'trades':
                        df = await loop.run_in_executor(
                            executor,
                            self.process_trades_data,
                            decompressed_path
                        )
                    else:
                        df = pd.DataFrame()  # Handle other data types
                    
                    return request, df
                    
                except Exception as e:
                    self.logger.error(f"Error processing request {request}: {e}")
                    return None
            
            # Execute all downloads concurrently
            tasks = [download_and_process(req) for req in requests]
            completed_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Aggregate results
            for result in completed_results:
                if result and not isinstance(result, Exception):
                    request, df = result
                    if not df.empty:
                        # Append to existing data
                        existing_df = results[request.symbol][request.data_type]
                        if existing_df.empty:
                            results[request.symbol][request.data_type] = df
                        else:
                            results[request.symbol][request.data_type] = pd.concat([existing_df, df]).sort_index()
        
        self.logger.info("Historical data download completed")
        return results
    
    def save_to_parquet(self, data: Dict[str, Dict[str, pd.DataFrame]], output_dir: Path):
        """Save processed data to Parquet files.
        
        Args:
            data: Dictionary with symbol -> data_type -> DataFrame structure
            output_dir: Output directory for Parquet files
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for symbol, data_types in data.items():
            symbol_dir = output_dir / symbol
            symbol_dir.mkdir(exist_ok=True)
            
            for data_type, df in data_types.items():
                if not df.empty:
                    output_path = symbol_dir / f"{data_type}.parquet"
                    df.to_parquet(output_path)
                    self.logger.info(f"Saved {symbol} {data_type} data to {output_path}")


async def main():
    """Example usage of the historical data collector."""
    collector = HistoricalDataCollector()
    
    # Download last 7 days of data for major symbols
    symbols = ['BTC', 'ETH', 'SOL']
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d")
    
    data = await collector.download_historical_data(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        data_types=['l2Book', 'trades']
    )
    
    # Save to parquet
    output_dir = settings.historical_data_path / "processed"
    collector.save_to_parquet(data, output_dir)


if __name__ == "__main__":
    asyncio.run(main())