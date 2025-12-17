from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Float, Integer, Boolean, DateTime, Text, JSON
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from .config import settings
import logging

logger = logging.getLogger(__name__)

# Convert postgresql:// to postgresql+asyncpg://
db_url = settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

# Create async engine
engine = create_async_engine(
    db_url,
    echo=False,
    pool_size=20,
    max_overflow=40,
    pool_pre_ping=True,
)

# Session factory
async_session_maker = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()


class Tick(Base):
    """Raw tick data model"""
    __tablename__ = "ticks"
    
    time = Column(DateTime(timezone=True), primary_key=True)
    symbol = Column(String, primary_key=True)
    price = Column(Float, nullable=False)
    size = Column(Float, nullable=False)


class OHLCV(Base):
    """OHLCV data model"""
    __tablename__ = "ohlcv"
    
    time = Column(DateTime(timezone=True), primary_key=True)
    symbol = Column(String, primary_key=True)
    interval = Column(String, primary_key=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    trade_count = Column(Integer, nullable=False)


class Analytics(Base):
    """Analytics results model"""
    __tablename__ = "analytics"
    
    time = Column(DateTime(timezone=True), primary_key=True)
    symbol1 = Column(String, primary_key=True)
    symbol2 = Column(String, primary_key=True, default="")
    metric_type = Column(String, primary_key=True)
    value = Column(Float)
    metadata = Column(JSONB)


class Alert(Base):
    """Alert configuration model"""
    __tablename__ = "alerts"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    alert_type = Column(String, nullable=False)
    symbol1 = Column(String, nullable=False)
    symbol2 = Column(String)
    condition = Column(String, nullable=False)
    threshold = Column(Float, nullable=False)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    triggered_at = Column(DateTime(timezone=True))


class AlertHistory(Base):
    """Alert history model"""
    __tablename__ = "alert_history"
    
    time = Column(DateTime(timezone=True), primary_key=True)
    alert_id = Column(Integer, primary_key=True)
    value = Column(Float, nullable=False)
    metadata = Column(JSONB)


async def init_db():
    """Initialize database connection"""
    try:
        async with engine.begin() as conn:
            # Test connection
            await conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection established")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise


async def get_session() -> AsyncSession:
    """Get async database session"""
    async with async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


from sqlalchemy import text

async def bulk_insert_ticks(ticks: list):
    """Bulk insert ticks efficiently"""
    if not ticks:
        return
    
    async with async_session_maker() as session:
        try:
            # Prepare values for bulk insert
            values = []
            for tick in ticks:
                values.append(f"('{tick['time']}', '{tick['symbol']}', {tick['price']}, {tick['size']})")
            
            query = f"""
                INSERT INTO ticks (time, symbol, price, size)
                VALUES {','.join(values)}
                ON CONFLICT (time, symbol) DO NOTHING
            """
            
            await session.execute(text(query))
            await session.commit()
            logger.debug(f"Inserted {len(ticks)} ticks")
        except Exception as e:
            logger.error(f"Bulk insert failed: {e}")
            await session.rollback()


async def get_latest_ticks(symbol: str, limit: int = 1000):
    """Get latest ticks for a symbol"""
    async with async_session_maker() as session:
        query = text("""
            SELECT time, symbol, price, size
            FROM ticks
            WHERE symbol = :symbol
            ORDER BY time DESC
            LIMIT :limit
        """)
        result = await session.execute(query, {"symbol": symbol, "limit": limit})
        return result.fetchall()


async def get_ohlcv_data(symbol: str, interval: str, limit: int = 1000):
    """Get OHLCV data for analysis"""
    async with async_session_maker() as session:
        # Query the continuous aggregate views
        view_name = f"ohlcv_{interval}"
        query = text(f"""
            SELECT time, symbol, open, high, low, close, volume, trade_count
            FROM {view_name}
            WHERE symbol = :symbol
            ORDER BY time DESC
            LIMIT :limit
        """)
        result = await session.execute(query, {"symbol": symbol, "limit": limit})
        return result.fetchall()


async def store_analytics(analytics_data: dict):
    """Store computed analytics"""
    async with async_session_maker() as session:
        try:
            analytic = Analytics(**analytics_data)
            session.add(analytic)
            await session.commit()
        except Exception as e:
            logger.error(f"Failed to store analytics: {e}")
            await session.rollback()