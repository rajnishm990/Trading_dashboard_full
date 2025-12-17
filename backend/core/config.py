from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List
import os


class Settings(BaseSettings):
    """Application settings from environment variables"""
    
    # Database
    DATABASE_URL: str = Field(
        default="postgresql://quantuser:quantpass123@localhost:5432/quant_analytics",
        description="PostgreSQL/TimescaleDB connection URL"
    )
    
    # Redis
    REDIS_URL: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL"
    )
    REDIS_STREAM_NAME: str = Field(
        default="ticks_stream",
        description="Redis stream name for tick data"
    )
    REDIS_ALERT_STREAM: str = Field(
        default="alerts_stream",
        description="Redis stream for alerts"
    )
    
    # API
    API_HOST: str = Field(default="0.0.0.0", description="API host")
    API_PORT: int = Field(default=8000, description="API port")
    
    # WebSocket
    BINANCE_WS_URL: str = Field(
        default="wss://fstream.binance.com/ws",
        description="Binance WebSocket URL"
    )
    DEFAULT_SYMBOLS: str = Field(
        default="btcusdt,ethusdt,bnbusdt",
        description="Comma-separated default symbols"
    )
    
    # Batch Processing
    BATCH_SIZE: int = Field(
        default=500,
        description="Number of ticks to batch before DB insert"
    )
    BATCH_TIMEOUT_SECONDS: float = Field(
        default=1.0,
        description="Max seconds to wait before flushing batch"
    )
    
    # Analytics
    ROLLING_WINDOW_DEFAULT: int = Field(
        default=20,
        description="Default rolling window for analytics"
    )
    RESAMPLE_INTERVALS: str = Field(
        default="1s,1m,5m",
        description="Comma-separated resample intervals"
    )
    
    # Alerts
    ALERT_CHECK_INTERVAL: float = Field(
        default=0.5,
        description="Alert check interval in seconds"
    )
    
    # Consumer Groups
    INGESTOR_CONSUMER_GROUP: str = "ingestor_group"
    ANALYTICS_CONSUMER_GROUP: str = "analytics_group"
    ALERT_CONSUMER_GROUP: str = "alert_group"
    
    @property
    def symbols_list(self) -> List[str]:
        """Get symbols as list"""
        return [s.strip().lower() for s in self.DEFAULT_SYMBOLS.split(",")]
    
    @property
    def intervals_list(self) -> List[str]:
        """Get resample intervals as list"""
        return [i.strip() for i in self.RESAMPLE_INTERVALS.split(",")]
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()