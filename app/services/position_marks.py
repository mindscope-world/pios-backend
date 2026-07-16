# app/services/position_marks.py
"""
Mark-to-market job — periodic revaluation of open positions.

apply_fill_to_position marks unrealized P&L at the *last fill price*, so a
dormant position's unrealized P&L goes stale until its next fill. Every
MARK_TO_MARKET_INTERVAL_SECS this loop fetches one live mark per distinct
open-position symbol (market_data_service.get_live_ticker — the same
domain-routed source everything else uses) and re-marks every open position:

    unrealized = (mark - avg_cost) * qty      for LONG
                 (avg_cost - mark) * qty      for SHORT

Symbols whose price fetch fails (provider down, no venue) are skipped that
pass — the position keeps its previous mark rather than being zeroed, and
the fill-time mark remains the floor guarantee. After a pass commits, each
affected trader gets one positions WS event so open Portfolio/Execution
screens refresh without polling.

Deliberately does NOT write PnL snapshots: those are the equity curve's
fill-driven history; a 60s cron writing per-user rows would flood it.
Portfolio metrics read unrealized P&L live from Position rows, so marking
the rows is enough.
"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.all_models import Position
from app.services.market_data_service import get_live_ticker
from app.services.trade_events import publish_position_event

log = logging.getLogger(__name__)


async def _mark_price(symbol: str) -> float | None:
    try:
        t = await get_live_ticker(symbol)
    except Exception as e:  # noqa: BLE001
        log.debug("position_marks: ticker %s failed: %s", symbol, e)
        return None
    if t.get("error"):
        return None
    last = t.get("last")
    if last:
        return float(last)
    bid, ask = float(t.get("bid") or 0), float(t.get("ask") or 0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2
    return None


async def mark_open_positions() -> int:
    """One revaluation pass. Returns the number of positions re-marked."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Position).options(selectinload(Position.symbol)).where(Position.is_open.is_(True))
        )
        positions = result.scalars().all()
        if not positions:
            return 0

        marks: dict[str, float | None] = {}
        touched_users: set = set()
        remarked = 0
        for pos in positions:
            symbol_name = getattr(pos.symbol, "symbol", None)
            if not symbol_name:
                continue
            if symbol_name not in marks:
                marks[symbol_name] = await _mark_price(symbol_name)
            mark = marks[symbol_name]
            if mark is None or mark <= 0:
                continue
            direction = 1 if pos.side == "LONG" else -1
            new_unrealized = (mark - float(pos.avg_cost)) * float(pos.qty) * direction
            if abs(new_unrealized - float(pos.unrealized_pnl or 0)) < 1e-9:
                continue
            pos.unrealized_pnl = new_unrealized
            touched_users.add(pos.user_id)
            remarked += 1

        if not remarked:
            return 0
        await db.commit()

    for user_id in touched_users:
        await publish_position_event(user_id, symbol_name=None)
    return remarked


async def run_position_marks() -> None:
    """Loop entrypoint — never raises; per-pass errors are logged and retried."""
    interval = max(10, int(settings.MARK_TO_MARKET_INTERVAL_SECS))
    log.info("Mark-to-market job started (every %ss)", interval)
    while True:
        try:
            n = await mark_open_positions()
            if n:
                log.info("position_marks: re-marked %d open positions", n)
        except asyncio.CancelledError:
            log.info("Mark-to-market job stopped")
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("position_marks: pass failed: %s", e)
        await asyncio.sleep(interval)
