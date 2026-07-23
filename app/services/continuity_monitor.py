# app/services/continuity_monitor.py
"""
Continuity Monitor — Guide Chapter 7's fifth Data Quality check.

Per-tick checks (schema/sanity, duplicate, timestamp drift, price/volume
outlier) live in app/workers/dq_pipeline.py and run inline in the tick
consumer. Continuity is different in kind: it's a *watchdog*, not a per-tick
check — it can only be detected by the absence of ticks over time, so it
runs as its own periodic background task, same shape as conditional_orders.py
/ position_marks.py.

Every CONTINUITY_CHECK_INTERVAL_SECS this loop:
  1. Reads each active symbol's most recent MarketTick.time (the same
     DB-persisted source of truth compute_feed_health already uses, not a
     separate in-memory tick counter — so this survives a worker restart
     without re-learning feed state from scratch).
  2. Tracks a small in-memory OK/STALE/UNKNOWN state per symbol so it only
     alerts on a genuine OK→STALE transition, not on startup for a symbol
     that was already stale (or never ingested) before this monitor ever
     ran — that would just be alert noise, not a detected gap.
  3. On a genuine gap (OK→STALE): writes a DQEvent (module=CONTINUITY_MONITOR,
     event_type=GAP_DETECTED) and an Alert, matching the guide's "raises an
     alert... and dependent strategies are automatically paused for that
     instrument until the feed resumes."
  4. On recovery (STALE→OK, only for a symbol this monitor itself alerted
     on): resolves the DQEvent(s) and the Alert.

"Paused" is enforced separately, not by this file: command_center_service.py
computes the same staleness condition from the ticks it already fetched for
the decision pipeline and passes feed_stale into
quant_engine.build_quant_core_gates, which forces BLOCK regardless of every
other gate. That path is synchronous with decision-making and DB-backed, so
it works correctly even though this monitor and the decision pipeline can
run in different processes (API vs. intelligence worker) with no shared
memory.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.all_models import Alert, DQEvent, MarketTick, Symbol

log = logging.getLogger(__name__)

# In-memory per-symbol state: "unknown" (not yet observed a clean reading,
# so a first-ever stale reading isn't alerted), "ok", or "stale". Reset on
# process restart by design — see the docstring above for why gating itself
# doesn't depend on this dict surviving a restart.
_state: dict[int, str] = {}


async def _sweep() -> None:
    threshold = max(1, int(settings.CONTINUITY_GAP_THRESHOLD_SECS))
    now = datetime.now(timezone.utc)

    async with AsyncSessionLocal() as db:
        symbols = (
            await db.execute(select(Symbol).where(Symbol.is_active.is_(True)))
        ).scalars().all()

        for sym in symbols:
            latest = (
                await db.execute(
                    select(MarketTick.time)
                    .where(MarketTick.symbol_id == sym.id)
                    .order_by(MarketTick.time.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()

            lag_secs = (now - latest).total_seconds() if latest else None
            is_stale = lag_secs is None or lag_secs > threshold
            prev = _state.get(sym.id, "unknown")

            if prev == "unknown":
                # First observation of this symbol — record a baseline,
                # don't alert either way (we don't know if a stale reading
                # here is a genuine new gap or just "never fed").
                _state[sym.id] = "stale" if is_stale else "ok"
                continue

            if is_stale and prev == "ok":
                _state[sym.id] = "stale"
                reason = (
                    f"No tick for {lag_secs:.0f}s (threshold {threshold}s)"
                    if lag_secs is not None
                    else "No tick ever recorded for this symbol"
                )
                db.add(DQEvent(
                    symbol_id=sym.id,
                    event_type="GAP_DETECTED",
                    module="CONTINUITY_MONITOR",
                    severity="CRITICAL",
                    reason=reason,
                    raw_payload={"lag_secs": lag_secs, "threshold_secs": threshold},
                ))
                db.add(Alert(
                    severity="P2", source="DATA_QUALITY", category="FEED_GAP",
                    title=f"{sym.symbol} feed gap detected",
                    message=(
                        f"{sym.symbol} ({sym.exchange}) has not produced a tick in "
                        f"over {threshold}s — {reason}. New trade decisions for "
                        f"this symbol are blocked until the feed resumes."
                    ),
                    symbol_id=sym.id,
                    meta={"lag_secs": lag_secs, "threshold_secs": threshold},
                ))
                await db.commit()
                log.warning("continuity_monitor: gap detected for %s — %s", sym.symbol, reason)

            elif not is_stale and prev == "stale":
                _state[sym.id] = "ok"
                await db.execute(
                    DQEvent.__table__.update()
                    .where(
                        DQEvent.symbol_id == sym.id,
                        DQEvent.module == "CONTINUITY_MONITOR",
                        DQEvent.event_type == "GAP_DETECTED",
                        DQEvent.resolved.is_(False),
                    )
                    .values(resolved=True, resolved_at=now)
                )
                await db.execute(
                    Alert.__table__.update()
                    .where(
                        Alert.symbol_id == sym.id,
                        Alert.category == "FEED_GAP",
                        Alert.auto_resolved.is_(False),
                    )
                    .values(auto_resolved=True, resolved_at=now)
                )
                await db.commit()
                log.info("continuity_monitor: %s feed recovered", sym.symbol)


async def run_continuity_monitor() -> None:
    """Loop entrypoint — never raises; per-pass errors are logged and retried."""
    interval = max(1, int(settings.CONTINUITY_CHECK_INTERVAL_SECS))
    log.info("Continuity monitor started (every %ss, gap threshold %ss)",
              interval, settings.CONTINUITY_GAP_THRESHOLD_SECS)
    while True:
        try:
            await _sweep()
        except asyncio.CancelledError:
            log.info("Continuity monitor stopped")
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("continuity_monitor: pass failed: %s", e)
        await asyncio.sleep(interval)
