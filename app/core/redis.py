import redis.asyncio as redis
from typing import Optional, Any
import orjson
from app.core.config import settings

_redis_client: Optional[redis.Redis] = None


def get_redis() -> redis.Redis:
    """
    Singleton async Redis client.
    Optimized for high-throughput real-time systems.
    """
    global _redis_client

    if _redis_client is None:
        _redis_client = redis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=False,  # ⚠️ important for performance
            max_connections=100,
            socket_timeout=10,
            socket_connect_timeout=10,
            retry_on_timeout=True,
        )

    return _redis_client


# ─────────────────────────────────────────────
# JSON HELPERS (fast serialization)
# ─────────────────────────────────────────────

async def redis_set_json(key: str, value: Any, ex: int | None = None):
    r = get_redis()
    await r.set(key, orjson.dumps(value), ex=ex)


async def redis_get_json(key: str):
    r = get_redis()
    data = await r.get(key)
    return orjson.loads(data) if data else None


async def redis_publish(channel: str, value: Any):
    r = get_redis()
    await r.publish(channel, orjson.dumps(value))


async def redis_lpush_json(key: str, value: Any, max_len: int = 500):
    r = get_redis()
    await r.lpush(key, orjson.dumps(value))
    await r.ltrim(key, 0, max_len)


async def redis_lrange_json(key: str, start=0, end=100):
    r = get_redis()
    data = await r.lrange(key, start, end)
    return [orjson.loads(x) for x in data]


# ─────────────────────────────────────────────
# SYNC CLIENT (CELERY)
# ─────────────────────────────────────────────

_redis_sync_client = None


def get_redis_sync():
    global _redis_sync_client

    if _redis_sync_client is None:
        import redis as _redis_sync

        _redis_sync_client = _redis_sync.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            max_connections=20,
        )

    return _redis_sync_client