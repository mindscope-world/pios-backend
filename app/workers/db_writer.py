"""
Batched DB writer.

Receives completed candles, DQ events, and raw ticks from the market_db_writer
loop. Writes them in bulk using a single session per batch — minimises
connection-pool pressure on small EC2.

What gets written:
  Candle1m   — one row per completed 1-min bucket per symbol
  DQEvent    — one row per FLAG or REJECT tick (sparse)
  MarketTick — one row per tick that wasn't REJECTed by the DQ pipeline
               (PASS + FLAG, matching the candle aggregator's own gate).
               Retention is short (RETAIN_MARKET_TICKS_HOURS) -- see
               retension_task.py -- this is a rolling window for tick-level
               analytics (OFI, LOF, regime), not a permanent tick archive.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session_factory
from app.models.all_models import Candle1m, DQEvent, MarketTick

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


def _dq_module_for_flags(flags: list[str]) -> str:
    """
    Which DQ check actually fired, for DQEvent.module. dq_pipeline.check()
    returns one flag list per tick that can only ever contain flags from a
    single check today (each check that flags returns immediately, except
    the timestamp corrector which falls through to the spike/volume checks
    below it) — priority order here matches evaluation order in
    dq_pipeline.py, so the earliest-evaluated check that produced a flag is
    reported as the module, which is always the correct (and in today's
    pipeline, the only) one.
    """
    for flag in flags:
        if flag in ("ZERO_PRICE", "NEGATIVE_VOLUME", "MISSING_TIMESTAMP"):
            return "TICK_VALIDATOR"
        if flag == "DUPLICATE":
            return "DUPLICATE_FILTER"
        if flag.startswith("TIMESTAMP_CORRECTED"):
            return "TIMESTAMP_CORRECTOR"
        if flag.startswith("SPIKE_") or flag.startswith("VOL_OUTLIER_"):
            return "OUTLIER_DETECTOR"
    return "TICK_VALIDATOR"  # defensive default — shouldn't be reached with a non-empty flag list


async def flush_dq_events(events: list[dict]) -> None:
    """
    Persist DQ FLAG/REJECT events.
    Called from the main writer loop whenever dq_result != PASS.

    module is derived from which check actually produced the flag (see
    _dq_module_for_flags) rather than a hardcoded "TICK_VALIDATOR" — before
    this fix, every flagged/rejected tick was recorded under TICK_VALIDATOR
    regardless of whether a duplicate filter, spike detector, or volume
    outlier check was the one that actually fired, which silently zeroed
    out the per-module pass/flag/reject rates the DQ dashboard reports for
    every module except TICK_VALIDATOR.
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
                    module      = _dq_module_for_flags(e.get("flags", [])),
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


async def flush_ticks(ticks: list[dict]) -> None:
    """
    Bulk-insert raw ticks that the DQ pipeline didn't REJECT (PASS + FLAG --
    same gate the candle aggregator uses). No unique constraint on
    market_ticks, so this is a plain insert, not an upsert.
    """
    if not ticks:
        return
    try:
        async with get_session_factory()() as session:
            session.add_all([
                MarketTick(
                    time       = _parse_time(t.get("time")),
                    symbol_id  = t["symbol_id"],
                    price      = t["price"],
                    volume     = t.get("volume", 0),
                    side       = t.get("side"),
                    exchange   = t.get("source", "unknown"),
                    dq_result  = t.get("dq_result", "PASS"),
                    flags      = t.get("flags") or None,
                )
                for t in ticks
            ])
            await session.commit()
            log.debug(f"Flushed {len(ticks)} ticks to DB")
    except Exception as e:
        log.error(f"flush_ticks error: {e}", exc_info=True)


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