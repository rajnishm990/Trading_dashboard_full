import asyncio
from typing import List
import logging
from datetime import datetime

from backend.core.config import settings
from backend.core.redis_client import consume_ticks, acknowledge_messages
from backend.core.database import bulk_insert_ticks

logger = logging.getLogger(__name__)


class BatchProcessor:
    """Process ticks in batches to avoid DB bombardment"""
    
    def __init__(self):
        self.consumer_name = f"batch_processor_{datetime.now().timestamp()}"
        self.buffer = []
        self.running = False
        self.stats = {
            "batches_processed": 0,
            "ticks_inserted": 0,
            "errors": 0
        }
        self.last_flush = datetime.now()
    
    async def start(self):
        """Start batch processing"""
        self.running = True
        logger.info(f"Starting Batch Processor (batch_size={settings.BATCH_SIZE})")
        
        # Run both consumer and flush tasks
        await asyncio.gather(
            self.consume_loop(),
            self.flush_loop(),
            self.report_stats(),
            return_exceptions=True
        )
    
    async def consume_loop(self):
        """Continuously consume from Redis stream"""
        while self.running:
            try:
                # Consume messages from Redis stream
                ticks, message_ids = await consume_ticks(
                    consumer_group=settings.INGESTOR_CONSUMER_GROUP,
                    consumer_name=self.consumer_name,
                    count=100,  # Read up to 100 messages at a time
                    block=1000  # Block for 1 second max
                )
                
                if ticks:
                    self.buffer.extend(ticks)
                    
                    # Acknowledge messages immediately after buffering
                    await acknowledge_messages(
                        settings.INGESTOR_CONSUMER_GROUP,
                        message_ids
                    )
                    
                    # Flush if buffer is full
                    if len(self.buffer) >= settings.BATCH_SIZE:
                        await self.flush_buffer()
                
            except Exception as e:
                logger.error(f"Error in consume loop: {e}")
                self.stats["errors"] += 1
                await asyncio.sleep(1)
    
    async def flush_loop(self):
        """Periodically flush buffer based on timeout"""
        while self.running:
            await asyncio.sleep(settings.BATCH_TIMEOUT_SECONDS)
            
            # Flush if buffer has data and timeout exceeded
            time_since_flush = (datetime.now() - self.last_flush).total_seconds()
            if self.buffer and time_since_flush >= settings.BATCH_TIMEOUT_SECONDS:
                await self.flush_buffer()
    
    async def flush_buffer(self):
        """Flush buffered ticks to database"""
        if not self.buffer:
            return
        
        try:
            # Take current buffer and clear it
            ticks_to_insert = self.buffer.copy()
            self.buffer.clear()
            
            # Bulk insert to TimescaleDB
            await bulk_insert_ticks(ticks_to_insert)
            
            # Update stats
            self.stats["batches_processed"] += 1
            self.stats["ticks_inserted"] += len(ticks_to_insert)
            self.last_flush = datetime.now()
            
            logger.debug(f"Flushed batch of {len(ticks_to_insert)} ticks")
            
        except Exception as e:
            logger.error(f"Failed to flush buffer: {e}")
            self.stats["errors"] += 1
            # Put ticks back in buffer on failure
            self.buffer.extend(ticks_to_insert)
    
    async def report_stats(self):
        """Periodically report processing statistics"""
        while self.running:
            await asyncio.sleep(30)
            logger.info(
                f"Batch Processor Stats - Batches: {self.stats['batches_processed']}, "
                f"Ticks: {self.stats['ticks_inserted']}, "
                f"Buffer: {len(self.buffer)}, "
                f"Errors: {self.stats['errors']}"
            )
    
    async def stop(self):
        """Stop processor and flush remaining data"""
        self.running = False
        if self.buffer:
            await self.flush_buffer()
        logger.info("Batch Processor stopped")
    
    def get_stats(self) -> dict:
        """Get current statistics"""
        return {
            **self.stats,
            "buffer_size": len(self.buffer)
        }