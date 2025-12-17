import asyncio
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from backend.core.config import settings
from backend.core.database import init_db
from backend.core.redis_client import init_redis
from backend.services.websocket_ingestor import WebSocketIngestor
from backend.services.analytics_engine import AnalyticsEngine
from backend.api.server import create_app
import uvicorn
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def startup_services():
    """Initialize all services"""
    logger.info("Starting Quant Analytics Platform...")
    
    # Initialize database
    logger.info("Initializing TimescaleDB...")
    await init_db()
    
    # Initialize Redis
    logger.info("Connecting to Redis...")
    await init_redis()
    
    # Start WebSocket Ingestor
    logger.info("Starting WebSocket Ingestor...")
    ingestor = WebSocketIngestor()
    asyncio.create_task(ingestor.start())
    
    # Start Analytics Engine
    logger.info("Starting Analytics Engine...")
    analytics = AnalyticsEngine()
    asyncio.create_task(analytics.start())
    
    logger.info("All services started successfully!")


async def main():
    """Main application entry point"""
    try:
        # Start all background services
        await startup_services()
        
        # Create FastAPI app
        app = create_app()
        
        # Run server
        config = uvicorn.Config(
            app,
            host=settings.API_HOST,
            port=settings.API_PORT,
            log_level="info",
            access_log=True
        )
        server = uvicorn.Server(config)
        
        logger.info(f"API Server running on http://{settings.API_HOST}:{settings.API_PORT}")
        logger.info(f" Frontend available at http://{settings.API_HOST}:{settings.API_PORT}")
        logger.info("API Docs at http://{settings.API_HOST}:{settings.API_PORT}/docs")
        
        await server.serve()
        
    except KeyboardInterrupt:
        logger.info("Shutting down ...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())