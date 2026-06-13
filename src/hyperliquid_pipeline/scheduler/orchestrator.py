"""Main orchestrator for the Hyperliquid data collection pipeline."""

import asyncio
import signal
import sys
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from pathlib import Path
import json
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ..config import settings
from ..collectors.historical_collector import HistoricalDataCollector
from ..collectors.realtime_collector import HyperliquidWebSocketCollector, DataLogger, GapEvent
from ..collectors.backfill import backfill_gap
from ..processors.data_processor import DataProcessor, create_storage_backends
from ..utils.validation import ValidationCallback
from ..storage.database import DataStorage


class DataPipelineOrchestrator:
    """Main orchestrator for the data collection pipeline."""
    
    def __init__(self):
        """Initialize the orchestrator."""
        self.logger = logger.bind(component="orchestrator")
        
        # Components
        self.historical_collector: Optional[HistoricalDataCollector] = None
        self.realtime_collector: Optional[HyperliquidWebSocketCollector] = None
        self.data_processor: Optional[DataProcessor] = None
        self.storage: Optional[DataStorage] = None
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.data_logger: Optional[DataLogger] = None
        self.validation_callback: Optional[ValidationCallback] = None
        
        # Reconnect gaps the archive didn't have yet, awaiting retry. Bounded so
        # a sustained outage with a flapping socket can't grow it without bound.
        self.pending_gaps: "deque[GapEvent]" = deque(maxlen=settings.gap_max_pending)

        # State tracking
        self.is_running = False
        self.start_time: Optional[datetime] = None
        self.stats = {
            'messages_processed': 0,
            'errors_encountered': 0,
            'last_data_time': None,
            'uptime_seconds': 0
        }
        
        # Signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        asyncio.create_task(self.stop())
    
    async def initialize(self):
        """Initialize all components."""
        try:
            self.logger.info("Initializing data pipeline orchestrator...")
            
            # Initialize storage backends
            self.storage = await create_storage_backends()
            self.logger.info("Storage backends initialized")
            
            # Initialize data processor
            self.data_processor = DataProcessor(self.storage)
            self.logger.info("Data processor initialized")
            
            # Initialize validation callback
            self.validation_callback = ValidationCallback()
            
            # Initialize historical data collector
            self.historical_collector = HistoricalDataCollector()
            self.logger.info("Historical data collector initialized")
            
            # Initialize real-time collector
            self.realtime_collector = HyperliquidWebSocketCollector(settings.symbols_list)
            
            # Set up data flow: validation -> processing -> storage.
            # Async so the collector's consumer awaits it — backpressure lands on
            # the bounded queue (drop-oldest) instead of spawning an unbounded
            # number of tasks per message under load.
            async def validated_data_callback(data_point):
                """Process validated data."""
                try:
                    # Validate and sanitize
                    validated_data = self.validation_callback(data_point)
                    if validated_data:
                        # Process the data
                        await self.data_processor.process_market_data(validated_data)

                        # Update stats
                        self.stats['messages_processed'] += 1
                        self.stats['last_data_time'] = datetime.now(timezone.utc)

                except Exception as e:
                    self.stats['errors_encountered'] += 1
                    self.logger.error(f"Error in data callback: {e}")
            
            self.realtime_collector.add_data_callback(validated_data_callback)

            # Initialize data logger for raw data backup
            self.data_logger = DataLogger()
            self.realtime_collector.add_data_callback(self.data_logger.log_data_point)

            # On a reconnect gap, queue it (fast, synchronous — never blocks the
            # socket). The periodic retry job does the actual archive backfill.
            self.realtime_collector.add_gap_callback(self._queue_gap)

            # Initialize scheduler
            self.scheduler = AsyncIOScheduler()
            self._setup_scheduled_jobs()
            
            self.logger.info("All components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize orchestrator: {e}")
            raise
    
    def _setup_scheduled_jobs(self):
        """Set up scheduled jobs."""
        if not self.scheduler:
            return
        
        # Daily historical data collection (at 1 AM UTC)
        self.scheduler.add_job(
            self._collect_historical_data,
            trigger=CronTrigger(hour=1, minute=0),
            id='daily_historical_collection',
            name='Daily Historical Data Collection',
            max_instances=1
        )
        
        # Data quality report generation (every 6 hours)
        self.scheduler.add_job(
            self._generate_quality_report,
            trigger=IntervalTrigger(hours=6),
            id='quality_report_generation',
            name='Data Quality Report Generation',
            max_instances=1
        )
        
        # System health check (every 30 minutes)
        self.scheduler.add_job(
            self._health_check,
            trigger=IntervalTrigger(minutes=30),
            id='system_health_check', 
            name='System Health Check',
            max_instances=1
        )
        
        # Cleanup old data (daily at 2 AM UTC)
        self.scheduler.add_job(
            self._cleanup_old_data,
            trigger=CronTrigger(hour=2, minute=0),
            id='daily_cleanup',
            name='Daily Data Cleanup',
            max_instances=1
        )
        
        # Stats logging (every 5 minutes)
        self.scheduler.add_job(
            self._log_stats,
            trigger=IntervalTrigger(minutes=5),
            id='stats_logging',
            name='Statistics Logging',
            max_instances=1
        )

        # Retry gap backfills the archive didn't have yet (it publishes with a lag).
        self.scheduler.add_job(
            self._retry_pending_gaps,
            trigger=IntervalTrigger(minutes=15),
            id='gap_backfill_retry',
            name='Reconnect Gap Backfill Retry',
            max_instances=1
        )

    async def _replay_point(self, point):
        """Persist a backfilled (past) point: validate, store, and log it.

        Deliberately does NOT run it through data_processor.process_market_data.
        That processor holds live, present-stream state (the OHLCV buffer and
        rolling indicators); replaying a stale trade into it would corrupt the
        current candle and races the live consumer's ordering. A recovered trade
        belongs to a past window, so we just record it (storage + JSONL).
        """
        validated = self.validation_callback(point) if self.validation_callback else point
        if not validated:
            return
        if self.storage:
            await self.storage.store_data_point(validated)
        if self.data_logger:
            self.data_logger.log_data_point(validated)
        # Count backfilled points so stats/health stay accurate.
        self.stats['messages_processed'] += 1
        self.stats['last_data_time'] = datetime.now(timezone.utc)

    async def _attempt_backfill(self, gap: GapEvent) -> int:
        """Try to backfill one gap; returns points recovered (0 = not yet in archive)."""
        if not self.historical_collector:
            return 0
        try:
            return await backfill_gap(self.historical_collector, gap, self._replay_point)
        except Exception as e:
            self.logger.error(f"Gap backfill errored for {gap.start} -> {gap.end}: {e}")
            return 0

    def _queue_gap(self, gap: GapEvent):
        """Gap callback: queue the gap for the retry job to backfill.

        Synchronous and fast so it never blocks the socket. The archive lags
        real time anyway, so a fresh gap usually can't be filled immediately —
        the periodic retry job handles it once the data lands.
        """
        self.pending_gaps.append(gap)  # bounded deque; drops oldest if saturated
        self.logger.info(
            f"Queued gap {gap.start.isoformat()} -> {gap.end.isoformat()} for backfill "
            f"({len(self.pending_gaps)} pending)"
        )

    async def _retry_pending_gaps(self):
        """Re-attempt queued gaps; drop ones that succeed or age past the limit.

        Takes ownership of the current queue up front and re-appends failures, so
        gaps detected concurrently (during the awaits) land in the fresh queue and
        are never overwritten.
        """
        if not self.pending_gaps:
            return
        batch = list(self.pending_gaps)
        self.pending_gaps.clear()
        now = datetime.now(timezone.utc)
        max_age = settings.gap_backfill_max_age_seconds
        for gap in batch:
            if (now - gap.end).total_seconds() > max_age:
                self.logger.warning(
                    f"Giving up on gap {gap.start.isoformat()} -> {gap.end.isoformat()} "
                    f"(older than {max_age:.0f}s, never appeared in the archive)"
                )
                continue
            recovered = await self._attempt_backfill(gap)
            if recovered == 0:
                self.pending_gaps.append(gap)
    
    async def _collect_historical_data(self):
        """Scheduled historical data collection."""
        try:
            self.logger.info("Starting scheduled historical data collection...")
            
            # Collect data for yesterday
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
            
            data = await self.historical_collector.download_historical_data(
                symbols=settings.symbols_list,
                start_date=yesterday,
                end_date=yesterday,
                data_types=['l2Book', 'trades'],
                max_workers=2  # Reduced workers for scheduled job
            )
            
            # Process historical data
            if self.data_processor:
                await self.data_processor.bulk_process_historical_data(data)
            
            # Save raw data to parquet
            output_dir = settings.historical_data_path / "processed" / yesterday
            self.historical_collector.save_to_parquet(data, output_dir)
            
            self.logger.info(f"Completed historical data collection for {yesterday}")
            
        except Exception as e:
            self.logger.error(f"Error in scheduled historical data collection: {e}")
    
    async def _generate_quality_report(self):
        """Generate and save data quality report."""
        try:
            if not self.validation_callback:
                return
            
            self.logger.info("Generating data quality report...")
            
            report = self.validation_callback.validator.generate_quality_report(settings.symbols_list)
            
            # Save report to file
            report_dir = settings.logs_path / "quality_reports"
            report_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            report_file = report_dir / f"quality_report_{timestamp}.json"
            
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            # Log summary
            summary = report['summary']
            self.logger.info(
                f"Quality report generated: "
                f"{summary['total_symbols']} symbols, "
                f"{summary['error_count']} errors, "
                f"{summary['warning_count']} warnings"
            )
            
        except Exception as e:
            self.logger.error(f"Error generating quality report: {e}")
    
    async def _health_check(self):
        """Perform system health check."""
        try:
            health_status = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'components': {}
            }
            
            # Check real-time collector
            if self.realtime_collector:
                collector_stats = self.realtime_collector.get_stats()
                health_status['components']['realtime_collector'] = {
                    'status': 'healthy' if collector_stats['is_connected'] else 'unhealthy',
                    'uptime_seconds': collector_stats['uptime_seconds'],
                    'message_count': collector_stats['message_count'],
                    'last_message_time': collector_stats['last_message_time']
                }
            
            # Check storage backends
            health_status['components']['storage'] = {
                'status': 'healthy' if self.storage else 'unhealthy'
            }
            
            # Check data processor
            health_status['components']['data_processor'] = {
                'status': 'healthy' if self.data_processor else 'unhealthy'
            }
            
            # Update uptime
            if self.start_time:
                self.stats['uptime_seconds'] = (datetime.now(timezone.utc) - self.start_time).total_seconds()
            
            # Log health status
            unhealthy_components = [
                name for name, status in health_status['components'].items()
                if status['status'] == 'unhealthy'
            ]
            
            if unhealthy_components:
                self.logger.warning(f"Unhealthy components: {unhealthy_components}")
            else:
                self.logger.info("All components healthy")
            
            # Save health report
            health_dir = settings.logs_path / "health"
            health_dir.mkdir(exist_ok=True)
            
            health_file = health_dir / f"health_{datetime.now(timezone.utc).strftime('%Y%m%d')}.json"
            
            # Append to daily health file
            health_records = []
            if health_file.exists():
                with open(health_file, 'r') as f:
                    health_records = json.load(f)
            
            health_records.append(health_status)
            
            with open(health_file, 'w') as f:
                json.dump(health_records, f, indent=2, default=str)
            
        except Exception as e:
            self.logger.error(f"Error in health check: {e}")
    
    async def _cleanup_old_data(self):
        """Clean up old data files and logs."""
        try:
            self.logger.info("Starting data cleanup...")
            
            # Clean up old validation results
            if self.validation_callback:
                self.validation_callback.validator.clear_old_results(hours=24)
            
            # Clean up old log files (keep last 30 days)
            log_dirs = [
                settings.logs_path / "quality_reports",
                settings.logs_path / "health", 
                settings.real_time_data_path
            ]
            
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
            
            for log_dir in log_dirs:
                if not log_dir.exists():
                    continue
                
                for file_path in log_dir.iterdir():
                    if file_path.is_file():
                        file_time = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
                        if file_time < cutoff_date:
                            file_path.unlink()
                            self.logger.debug(f"Deleted old file: {file_path}")
            
            self.logger.info("Data cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error in data cleanup: {e}")
    
    async def _log_stats(self):
        """Log system statistics."""
        try:
            # Update uptime
            if self.start_time:
                self.stats['uptime_seconds'] = (datetime.now(timezone.utc) - self.start_time).total_seconds()
            
            # Get real-time collector stats
            collector_stats = {}
            if self.realtime_collector:
                collector_stats = self.realtime_collector.get_stats()
            
            # Log comprehensive stats
            self.logger.info(
                f"Pipeline Stats - "
                f"Uptime: {self.stats['uptime_seconds']:.0f}s, "
                f"Messages: {self.stats['messages_processed']}, "
                f"Errors: {self.stats['errors_encountered']}, "
                f"Connected: {collector_stats.get('is_connected', False)}, "
                f"Last Data: {self.stats['last_data_time']}"
            )
            
        except Exception as e:
            self.logger.error(f"Error logging stats: {e}")
    
    async def start(self):
        """Start the data pipeline."""
        try:
            if self.is_running:
                self.logger.warning("Pipeline is already running")
                return
            
            self.logger.info("Starting Hyperliquid data pipeline...")
            self.start_time = datetime.now(timezone.utc)
            self.is_running = True
            
            # Start scheduler
            if self.scheduler:
                self.scheduler.start()
                self.logger.info("Scheduler started")
            
            # Start real-time data collection
            if self.realtime_collector and settings.real_time_enabled:
                self.logger.info("Starting real-time data collection...")
                realtime_task = asyncio.create_task(self.realtime_collector.start_with_reconnect())
            else:
                realtime_task = None
            
            # Initial historical data collection (last 7 days if no data exists)
            await self._initial_data_collection()
            
            self.logger.info("Data pipeline started successfully")
            
            # Keep the pipeline running
            if realtime_task:
                try:
                    await realtime_task
                except asyncio.CancelledError:
                    self.logger.info("Real-time collection cancelled")
            else:
                # If no real-time collection, just keep the scheduler running
                while self.is_running:
                    await asyncio.sleep(1)
            
        except Exception as e:
            self.logger.error(f"Error starting pipeline: {e}")
            await self.stop()
            raise
    
    async def _initial_data_collection(self):
        """Perform initial historical data collection if needed."""
        try:
            # Check if we have recent data
            data_exists = False
            processed_dir = settings.historical_data_path / "processed"
            
            if processed_dir.exists():
                # Check for recent data files
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=2)
                for file_path in processed_dir.rglob("*.parquet"):
                    file_time = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
                    if file_time > cutoff_date:
                        data_exists = True
                        break
            
            if not data_exists:
                self.logger.info("No recent historical data found, performing initial collection...")
                
                # Collect last 7 days
                end_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
                start_date = (datetime.now(timezone.utc) - timedelta(days=8)).strftime("%Y-%m-%d")
                
                data = await self.historical_collector.download_historical_data(
                    symbols=settings.symbols_list[:3],  # Start with fewer symbols
                    start_date=start_date,
                    end_date=end_date,
                    data_types=['trades'],  # Start with trades only
                    max_workers=2
                )
                
                # Process and save data
                if self.data_processor:
                    await self.data_processor.bulk_process_historical_data(data)
                
                output_dir = settings.historical_data_path / "processed" / "initial"
                self.historical_collector.save_to_parquet(data, output_dir)
                
                self.logger.info("Initial historical data collection completed")
            else:
                self.logger.info("Recent historical data found, skipping initial collection")
                
        except Exception as e:
            self.logger.error(f"Error in initial data collection: {e}")
    
    async def stop(self):
        """Stop the data pipeline."""
        try:
            self.logger.info("Stopping data pipeline...")
            self.is_running = False
            
            # Stop scheduler
            if self.scheduler and self.scheduler.running:
                self.scheduler.shutdown(wait=True)
                self.logger.info("Scheduler stopped")
            
            # Close data logger
            if self.data_logger:
                self.data_logger.close_all_files()
            
            # Close storage backends
            if self.storage:
                await self.storage.close()
                self.logger.info("Storage backends closed")
            
            self.logger.info("Data pipeline stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error stopping pipeline: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current pipeline status.
        
        Returns:
            Status dictionary
        """
        status = {
            'is_running': self.is_running,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'uptime_seconds': self.stats['uptime_seconds'],
            'stats': self.stats.copy(),
            'components': {
                'historical_collector': self.historical_collector is not None,
                'realtime_collector': self.realtime_collector is not None,
                'data_processor': self.data_processor is not None,
                'storage': self.storage is not None,
                'scheduler': self.scheduler is not None and self.scheduler.running if self.scheduler else False
            }
        }
        
        # Add real-time collector stats if available
        if self.realtime_collector:
            status['realtime_stats'] = self.realtime_collector.get_stats()
        
        return status


async def main():
    """Main entry point for the data pipeline."""
    orchestrator = DataPipelineOrchestrator()
    
    try:
        # Initialize and start the pipeline
        await orchestrator.initialize()
        await orchestrator.start()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
    finally:
        await orchestrator.stop()


if __name__ == "__main__":
    # Configure logging
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        level=settings.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
    # Add file logging
    log_file = settings.logs_path / "pipeline.log"
    logger.add(
        log_file,
        level=settings.log_level,
        rotation=settings.log_rotation,
        retention=settings.log_retention,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
    )
    
    # Run the pipeline
    asyncio.run(main())