#!/usr/bin/env python3
"""Quick setup script for SOL data collection pipeline."""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import os

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def check_dependencies():
    """Check if required dependencies are installed."""
    print("🔍 Checking dependencies...")
    
    try:
        import boto3
        import lz4
        import pandas
        import websockets
        import loguru
        print("✅ All Python dependencies installed")
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Run: pip install -r requirements.txt")
        return False

def check_aws_config():
    """Check AWS configuration."""
    print("🔍 Checking AWS configuration...")
    
    try:
        result = subprocess.run(['aws', 'configure', 'list'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ AWS CLI configured")
            return True
        else:
            print("❌ AWS CLI not configured")
            return False
    except FileNotFoundError:
        print("❌ AWS CLI not installed")
        print("Install with: pip install awscli")
        print("Then run: aws configure")
        return False

def setup_directories():
    """Create necessary directories."""
    print("📁 Setting up directories...")
    
    directories = [
        Path("./data"),
        Path("./data/historical"),
        Path("./data/realtime"), 
        Path("./data/processed"),
        Path("./logs")
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"✅ Created {directory}")

def test_sol_realtime():
    """Test SOL real-time data collection."""
    print("🔄 Testing SOL real-time data collection (30 seconds)...")
    
    async def run_test():
        try:
            from hyperliquid_pipeline.collectors.realtime_collector import HyperliquidWebSocketCollector
            from loguru import logger
            
            collector = HyperliquidWebSocketCollector(['SOL'])
            
            message_count = 0
            data_types_seen = set()
            
            def count_messages(data_point):
                nonlocal message_count, data_types_seen
                message_count += 1
                data_types_seen.add(data_point.data_type)
                if message_count <= 5:  # Show first 5 messages
                    print(f"📊 Received {data_point.data_type} for {data_point.symbol}")
            
            collector.add_data_callback(count_messages)
            
            # Run for 30 seconds
            try:
                await asyncio.wait_for(
                    collector.start_with_reconnect(),
                    timeout=30
                )
            except asyncio.TimeoutError:
                pass
            
            print(f"✅ Test completed:")
            print(f"   - Messages received: {message_count}")
            print(f"   - Data types: {', '.join(data_types_seen)}")
            
            return message_count > 0
            
        except Exception as e:
            print(f"❌ Real-time test failed: {e}")
            return False
    
    return asyncio.run(run_test())

def test_sol_historical():
    """Test SOL historical data collection."""
    print("🔄 Testing SOL historical data collection (small sample)...")
    
    async def run_test():
        try:
            from hyperliquid_pipeline.collectors.historical_collector import HistoricalDataCollector
            
            collector = HistoricalDataCollector()
            
            # Try to download just 1 hour of data from yesterday
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            
            print(f"   Downloading SOL data for {yesterday} (first hour only)...")
            
            # Test with minimal request first
            from hyperliquid_pipeline.collectors.historical_collector import HistoricalDataRequest
            
            request = HistoricalDataRequest(
                symbol='SOL',
                date=yesterday.replace('-', ''),
                hour=9,  # 9 AM UTC - usually active
                data_type='trades'
            )
            
            s3_location = collector.get_s3_location(request)
            print(f"   S3 Location: {s3_location.bucket}/{s3_location.key}")
            
            # Try to check if file exists without downloading
            try:
                response = collector.s3_client.head_object(
                    Bucket=s3_location.bucket,
                    Key=s3_location.key,
                    RequestPayer='requester'
                )
                file_size = response['ContentLength']
                print(f"✅ Historical data available (file size: {file_size:,} bytes)")
                return True
                
            except Exception as s3_error:
                if "404" in str(s3_error):
                    print(f"⚠️  No data available for {yesterday} hour 9")
                    print("   This is normal - not all hours have data")
                    return True  # Not an error, just no data for that hour
                else:
                    print(f"❌ S3 access error: {s3_error}")
                    return False
            
        except Exception as e:
            print(f"❌ Historical test failed: {e}")
            return False
    
    return asyncio.run(run_test())

def generate_sample_usage():
    """Generate sample usage commands."""
    print("\n🚀 SOL Pipeline Setup Complete!")
    print("\nNext steps:")
    print("\n1. Start real-time SOL data collection:")
    print("   python scripts/run_pipeline.py start --symbols SOL")
    
    print("\n2. Collect recent SOL historical data:")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    print(f"   python scripts/run_pipeline.py collect-historical \\")
    print(f"     --symbols SOL \\")
    print(f"     --start-date {week_ago} \\")
    print(f"     --end-date {yesterday}")
    
    print("\n3. Test SOL real-time for 1 minute:")
    print("   python scripts/run_pipeline.py test-realtime --symbols SOL --duration 60")
    
    print("\n4. Check collected data:")
    print("   ls -la data/historical/")
    print("   ls -la data/realtime/")
    
    print("\n📁 Data will be stored in:")
    print(f"   - Historical: {Path('./data/historical').absolute()}")
    print(f"   - Real-time: {Path('./data/realtime').absolute()}")
    print(f"   - Logs: {Path('./logs').absolute()}")

def main():
    """Main setup function."""
    print("🔧 Setting up Hyperliquid SOL Data Pipeline")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        return False
    
    # Set up directories
    setup_directories()
    
    # Check AWS (optional for real-time only)
    aws_ok = check_aws_config()
    if not aws_ok:
        print("⚠️  AWS not configured - historical data collection will not work")
        print("   Real-time data collection will still work")
    
    # Test real-time collection
    print("\n" + "=" * 50)
    if test_sol_realtime():
        print("✅ SOL real-time data collection working!")
    else:
        print("❌ SOL real-time data collection failed")
        return False
    
    # Test historical collection (only if AWS is configured)
    if aws_ok:
        print("\n" + "=" * 50)
        if test_sol_historical():
            print("✅ SOL historical data access working!")
        else:
            print("❌ SOL historical data access failed")
    
    # Generate usage examples
    generate_sample_usage()
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        if success:
            print("\n🎉 SOL pipeline setup completed successfully!")
        else:
            print("\n❌ Setup encountered issues - check the errors above")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⏹️  Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error during setup: {e}")
        sys.exit(1)