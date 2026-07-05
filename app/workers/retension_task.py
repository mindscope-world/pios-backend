
# app/workers/retention_task.py
"""
Retention & Rollup Task.

Runs two loops concurrently:
  1. Hourly rollup:   1-min candles  →  1-hour candles (before they're pruned)
  2. Daily pruning:   delete rows older than retention window

Retention policy:
  candles_1m     90 days   (config: RETAIN_CANDLES_1M_DAYS)
  candles_1h    730 days   (config: RETAIN_CANDLES_1H_DAYS)
  dq_events      30 days   (config: RETAIN_DQ_EVENTS_DAYS)
  market_ticks   48 hours  (config: RETAIN_MARKET_TICKS_HOURS) — high-volume raw
                            ticks, kept only long enough for tick-level analytics
                            (OFI, LOF, regime); candles are the long-term record.

EC2 notes:
  - Both loops use raw SQL (no ORM overhead) for bulk deletes
  - Rollup uses INSERT ... ON CONFLICT DO NOTHING (idempotent)
  - Daily pruning runs at 03:00 UTC to avoid market hours
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta

from sqlalchemy import text

from app.core.config import settings
from app.core.database import get_engine

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Rollup SQL: 1-min → 1-hour candles
# ─────────────────────────────────────────────────────────────────────────────
_ROLLUP_SQL = text("""
INSERT INTO candles_1h
    (time, symbol_id, open, high, low, close, volume, tick_count)
SELECT
    date_trunc('hour', time)                              AS time,
    symbol_id,
    (array_agg(open  ORDER BY time))[1]                   AS open,
    max(high)                                             AS high,
    min(low)                                              AS low,
    (array_agg(close ORDER BY time DESC))[1]              AS close,
    sum(volume)                                           AS volume,
    sum(tick_count)                                       AS tick_count
FROM candles_1m
WHERE
    time >= :from_ts
    AND time < :to_ts
GROUP BY
    date_trunc('hour', time),
    symbol_id
ON CONFLICT (symbol_id, time) DO NOTHING
""")


async def _rollup_last_hour() -> None:
    """Roll up the previous complete hour of 1-min candles into candles_1h."""
    now     = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    to_ts   = now
    from_ts = now - timedelta(hours=1)

    try:
        async with get_engine().connect() as conn:
            result = await conn.execute(
                _ROLLUP_SQL,
                {"from_ts": from_ts, "to_ts": to_ts},
            )
            await conn.commit()
            log.info(f"Rollup: inserted {result.rowcount} 1h candles "
                     f"({from_ts.strftime('%H:%M')} → {to_ts.strftime('%H:%M')} UTC)")
    except Exception as e:
        log.error(f"Rollup error: {e}", exc_info=True)


async def _prune_tables() -> None:
    """Delete rows older than retention window from time-series tables."""
    now = datetime.now(timezone.utc)
    policy = {
        "candles_1m":   timedelta(days=settings.RETAIN_CANDLES_1M_DAYS),
        "candles_1h":   timedelta(days=settings.RETAIN_CANDLES_1H_DAYS),
        "dq_events":    timedelta(days=settings.RETAIN_DQ_EVENTS_DAYS),
        "market_ticks": timedelta(hours=settings.RETAIN_MARKET_TICKS_HOURS),
    }
    try:
        async with get_engine().connect() as conn:
            for table, delta in policy.items():
                cutoff = now - delta
                result = await conn.execute(
                    text(f"DELETE FROM {table} WHERE time < :cutoff"),
                    {"cutoff": cutoff},
                )
                log.info(f"Pruned {result.rowcount} rows from {table} "
                         f"(cutoff={cutoff.date()})")
            await conn.commit()
    except Exception as e:
        log.error(f"Pruning error: {e}", exc_info=True)


# ─────────────────────────────────────────────────────────────────────────────
# Public task loops — started by orchestrator
# ─────────────────────────────────────────────────────────────────────────────

async def rollup_loop() -> None:
    """Roll up 1-min → 1-hour candles every hour."""
    while True:
        await asyncio.sleep(3600)
        await _rollup_last_hour()


async def retention_loop() -> None:
    """
    Run retention pruning once per day.
    Waits until 03:00 UTC then prunes, repeats every 24h.
    """
    while True:
        now    = datetime.now(timezone.utc)
        target = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        wait = (target - now).total_seconds()
        log.info(f"Retention: next prune in {wait/3600:.1f}h (at {target} UTC)")
        await asyncio.sleep(wait)
        await _prune_tables()