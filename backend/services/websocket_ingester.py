import asyncio
import websockets
import json
from datetime import datetime
from typing import Set
import logging

from backend.core.config import settings
from backend.core.redis_client import publish_tick

logger = logging.getLogger(__name__)


class WebSocketIngestor:
    """Ingest real-time tick data from Binance WebSocket"""
    
    def __init__(self):
        self.active_symbols: Set[str] = set(settings.symbols_list)
        self.connections = {}
        self.running = False
        self.stats = {
            "ticks_received": 0,
            "ticks_published": 0,
            "errors": 0
        }
    
    async def start(self):
        """Start ingesting data for all symbols"""
        self.running = True
        logger.info(f"Starting WebSocket Ingestor for symbols: {self.active_symbols}")
        
        # Create tasks for each symbol
        tasks = [
            self.ingest_symbol(symbol)
            for symbol in self.active_symbols
        ]
        
        # Also start stats reporter
        tasks.append(self.report_stats())
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def ingest_symbol(self, symbol: str):
        """Ingest data for a single symbol"""
        url = f"{settings.BINANCE_WS_URL}/{symbol}@trade"
        
        while self.running:
            try:
                async with websockets.connect(url) as ws:
                    logger.info(f"✅ Connected to {symbol} stream")
                    self.connections[symbol] = ws
                    
                    while self.running:
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                            await self.process_message(symbol, message)
                        except asyncio.TimeoutError:
                            # Send ping to keep connection alive
                            await ws.ping()
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"Connection closed for {symbol}, reconnecting...")
                            break
                        
            except Exception as e:
                logger.error(f"Error in {symbol} stream: {e}")
                self.stats["errors"] += 1
                await asyncio.sleep(5)  # Wait before reconnecting
    
    async def process_message(self, symbol: str, message: str):
        """Process incoming WebSocket message"""
        try:
            data = json.loads(message)
            
            # Binance futures trade stream format
            if data.get("e") == "trade":
                tick = self.normalize_tick(data)
                self.stats["ticks_received"] += 1
                
                # Publish to Redis stream
                await publish_tick(tick)
                self.stats["ticks_published"] += 1
                
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            self.stats["errors"] += 1
    
    def normalize_tick(self, data: dict) -> dict:
        """Normalize Binance tick data to standard format"""
        # Binance trade event: {e: 'trade', E: event_time, s: symbol, p: price, q: quantity, T: trade_time}
        timestamp = datetime.fromtimestamp(data["T"] / 1000)
        
        return {
            "symbol": data["s"].lower(),
            "time": timestamp.isoformat(),
            "price": float(data["p"]),
            "size": float(data["q"])
        }
    
    async def report_stats(self):
        """Periodically report ingestion statistics"""
        while self.running:
            await asyncio.sleep(30)
            logger.info(
                f"Ingestor Stats - Received: {self.stats['ticks_received']}, "
                f"Published: {self.stats['ticks_published']}, "
                f"Errors: {self.stats['errors']}"
            )
    
    async def add_symbol(self, symbol: str):
        """Dynamically add a new symbol to ingest"""
        symbol = symbol.lower()
        if symbol not in self.active_symbols:
            self.active_symbols.add(symbol)
            asyncio.create_task(self.ingest_symbol(symbol))
            logger.info(f"Added symbol: {symbol}")
    
    async def remove_symbol(self, symbol: str):
        """Remove a symbol from ingestion"""
        symbol = symbol.lower()
        if symbol in self.active_symbols:
            self.active_symbols.remove(symbol)
            if symbol in self.connections:
                await self.connections[symbol].close()
                del self.connections[symbol]
            logger.info(f"Removed symbol: {symbol}")
    
    async def stop(self):
        """Stop all ingestion"""
        self.running = False
        for ws in self.connections.values():
            await ws.close()
        logger.info("WebSocket Ingestor stopped")
    
    def get_stats(self) -> dict:
        """Get current statistics"""
        return {
            **self.stats,
            "active_symbols": list(self.active_symbols),
            "connections": len(self.connections)
        }