import redis.asyncio as aioredis
from .config import settings
import json
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Global Redis client
redis_client: Optional[aioredis.Redis] = None


async def init_redis():
    """Initialize Redis connection"""
    global redis_client
    try:
        redis_client = await aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            max_connections=50
        )
        await redis_client.ping()
        logger.info("✅ Redis connection established")
        
        # Create consumer groups if they don't exist
        await create_consumer_groups()
        
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        raise


async def create_consumer_groups():
    """Create Redis consumer groups for streams"""
    groups = [
        (settings.REDIS_STREAM_NAME, settings.INGESTOR_CONSUMER_GROUP),
        (settings.REDIS_STREAM_NAME, settings.ANALYTICS_CONSUMER_GROUP),
        (settings.REDIS_ALERT_STREAM, settings.ALERT_CONSUMER_GROUP),
    ]
    
    for stream, group in groups:
        try:
            await redis_client.xgroup_create(
                stream, group, id="0", mkstream=True
            )
            logger.info(f"Created consumer group: {group} for stream: {stream}")
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group {group} already exists")
            else:
                logger.error(f"Failed to create consumer group {group}: {e}")


async def publish_tick(tick: Dict):
    """Publish tick data to Redis stream"""
    try:
        message_id = await redis_client.xadd(
            settings.REDIS_STREAM_NAME,
            {
                "symbol": tick["symbol"],
                "time": tick["time"],
                "price": str(tick["price"]),
                "size": str(tick["size"])
            },
            maxlen=100000  # Keep last 100k ticks in stream
        )
        return message_id
    except Exception as e:
        logger.error(f"Failed to publish tick: {e}")
        return None


async def publish_alert(alert: Dict):
    """Publish alert to Redis alert stream"""
    try:
        message_id = await redis_client.xadd(
            settings.REDIS_ALERT_STREAM,
            {
                "alert_id": str(alert["alert_id"]),
                "alert_type": alert["alert_type"],
                "symbol": alert["symbol"],
                "value": str(alert["value"]),
                "threshold": str(alert["threshold"]),
                "message": alert.get("message", ""),
                "timestamp": alert["timestamp"]
            }
        )
        return message_id
    except Exception as e:
        logger.error(f"Failed to publish alert: {e}")
        return None


async def consume_ticks(consumer_group: str, consumer_name: str, count: int = 10, block: int = 1000):
    """Consume tick data from Redis stream"""
    try:
        messages = await redis_client.xreadgroup(
            groupname=consumer_group,
            consumername=consumer_name,
            streams={settings.REDIS_STREAM_NAME: ">"},
            count=count,
            block=block
        )
        
        ticks = []
        message_ids = []
        
        for stream_name, stream_messages in messages:
            for message_id, data in stream_messages:
                message_ids.append(message_id)
                ticks.append({
                    "symbol": data["symbol"],
                    "time": data["time"],
                    "price": float(data["price"]),
                    "size": float(data["size"])
                })
        
        return ticks, message_ids
    except Exception as e:
        logger.error(f"Failed to consume ticks: {e}")
        return [], []


async def acknowledge_messages(consumer_group: str, message_ids: List[str]):
    """Acknowledge processed messages"""
    if not message_ids:
        return
    
    try:
        await redis_client.xack(
            settings.REDIS_STREAM_NAME,
            consumer_group,
            *message_ids
        )
    except Exception as e:
        logger.error(f"Failed to acknowledge messages: {e}")


async def get_stream_info():
    """Get information about Redis streams"""
    try:
        info = await redis_client.xinfo_stream(settings.REDIS_STREAM_NAME)
        return {
            "length": info.get("length", 0),
            "groups": info.get("groups", 0),
            "first_entry": info.get("first-entry"),
            "last_entry": info.get("last-entry")
        }
    except Exception as e:
        logger.error(f"Failed to get stream info: {e}")
        return {}


async def set_cache(key: str, value: any, expiry: int = 300):
    """Set cache value with expiry"""
    try:
        await redis_client.setex(key, expiry, json.dumps(value))
    except Exception as e:
        logger.error(f"Failed to set cache: {e}")


async def get_cache(key: str):
    """Get cache value"""
    try:
        value = await redis_client.get(key)
        if value:
            return json.loads(value)
        return None
    except Exception as e:
        logger.error(f"Failed to get cache: {e}")
        return None


async def publish_to_channel(channel: str, message: Dict):
    """Publish message to Redis pub/sub channel"""
    try:
        await redis_client.publish(channel, json.dumps(message))
    except Exception as e:
        logger.error(f"Failed to publish to channel: {e}")


def get_redis_client() -> aioredis.Redis:
    """Get Redis client instance"""
    return redis_client