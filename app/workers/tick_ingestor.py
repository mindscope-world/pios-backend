# app/workers/tick_ingestor.py
"""
Tick Ingestor — publishes raw ticks from exchanges to Redis.

This is the ONLY component that writes to Redis streams.
All other components READ from them.

Redis stream design:
  Key:    ticks:{symbol_id}          e.g. ticks:42
  MAXLEN: STREAM_MAX_LEN (100k)      hard cap — oldest entries auto-evicted
  TTL:    STREAM_TTL_SECS (86400)    key expires 24h after last tick

Memory ceiling: 50 symbols × 100k ticks × ~200 bytes = ~1GB max
For t3.small (2GB RAM), this is safe alongside the app process.

Also pushes to `db_tick_buffer` list for the market_db_writer to consume.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from app.core.config import settings
from app.core.redis import get_redis

log = logging.getLogger(__name__)


async def publish_tick(tick: dict) -> None:
    """
    Publish one validated tick from an exchange adapter.

    tick must contain at minimum:
      symbol_id: int
      price:     str | float
      volume:    str | float
      time:      ISO-8601 string
      side:      "buy" | "sell" | "neutral"
      source:    str  (exchange name)
    """
    redis      = get_redis()
    symbol_id  = tick.get("symbol_id")

    if not symbol_id:
        log.warning(f"publish_tick: missing symbol_id — dropped: {tick}")
        return

    # Ensure time is always a clean ISO string
    if "time" not in tick or not tick["time"]:
        tick["time"] = datetime.now(timezone.utc).isoformat()

    stream_key = f"ticks:{symbol_id}"
    payload    = {k: str(v) for k, v in tick.items()}   # Redis requires strings

    # ── Publish to per-symbol stream (capped ring buffer) ─────
    await redis.xadd(
        stream_key,
        payload,
        maxlen=settings.STREAM_MAX_LEN,
        approximate=True,   # ~MAXLEN is faster; exact count not critical
    )
    # Refresh 24h TTL on every write
    await redis.expire(stream_key, settings.STREAM_TTL_SECS)

    # ── Also push to writer queue for DB aggregation ───────────
    await redis.rpush("db_tick_buffer", json.dumps(tick))

    log.debug(f"Published tick: symbol={symbol_id} price={tick.get('price')}")