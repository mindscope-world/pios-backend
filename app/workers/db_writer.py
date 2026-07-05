"""
Batched DB writer.

Receives completed candles and DQ events from the market_db_writer loop.
Writes them in bulk using a single session per batch — minimises
connection-pool pressure on small EC2.

What gets written:
  Candle1m   — one row per completed 1-min bucket per symbol
  DQEvent    — one row per FLAG or REJECT tick (sparse)
  MarketTick — NOT written here; handled by regime_scan_task (hourly)

What does NOT get written:
  Raw ticks  — they stay in Redis streams only
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.models.all_models import Candle1m, DQEvent

log = logging.getLogger(__name__)


async def flush_candles(candles: list[dict]) -> None:
    """
    Upsert a batch of completed 1-min candles.
    Uses ON CONFLICT DO NOTHING so re-delivery is safe (idempotent).
    """
    if not candles:
        return
    try:
        async with get_session_factory()() as session:
            for c in candles:
                stmt = pg_insert(Candle1m).values(
                    time       = c["time"],
                    symbol_id  = c["symbol_id"],
                    open       = c["open"],
                    high       = c["high"],
                    low        = c["low"],
                    close      = c["close"],
                    volume     = c["volume"],
                    tick_count = c["tick_count"],
                    has_dq     = c["has_dq"],
                ).on_conflict_do_nothing(
                    index_elements=["symbol_id", "time"]
                )
                await session.execute(stmt)
            await session.commit()
            log.debug(f"Flushed {len(candles)} candles to DB")
    except Exception as e:
        log.error(f"flush_candles error: {e}", exc_info=True)


async def flush_dq_events(events: list[dict]) -> None:
    """
    Persist DQ FLAG/REJECT events.
    Called from the main writer loop whenever dq_result != PASS.
    """
    if not events:
        return
    try:
        async with get_session_factory()() as session:
            session.add_all([
                DQEvent(
                    time        = _parse_time(e.get("time")),
                    symbol_id   = e.get("symbol_id"),
                    event_type  = e["dq_result"],
                    module      = "TICK_VALIDATOR",
                    severity    = "WARN" if e["dq_result"] == "FLAG" else "ERROR",
                    reason      = ", ".join(e.get("flags", [])),
                    raw_payload = {
                        "flags": e.get("flags", []),
                        "price": e.get("price"),
                    },
                )
                for e in events
            ])
            await session.commit()
            log.debug(f"Flushed {len(events)} DQ events to DB")
    except Exception as e:
        log.error(f"flush_dq_events error: {e}", exc_info=True)


def _parse_time(ts) -> datetime:
    if not ts:
        return datetime.now(timezone.utc)
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(ts))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)