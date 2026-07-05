# app/services/risk_service.py
"""
Risk Service — Production Grade

Computes:
  - Historical VaR/CVaR from real PnL snapshot returns  (scipy/numpy)
  - Parametric VaR using GARCH-estimated volatility     (arch via quant_engine)
  - Real-time drawdown from PnLSnapshot equity curve
  - Daily loss from today\'s fills
  - Leverage from open positions mark-to-market
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta, date

import numpy as np
import structlog
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.all_models import (
    Order, Position, KillSwitchEvent, RiskLimit, Alert,
    Fill, PnLSnapshot, MarketTick,
)
from app.schemas.all_schemas import RiskMetricsOut, KillSwitchRequest
from app.services.audit_service import write_audit
from app.services.broker_service import get_adapter
from app.core.config import settings

logger = structlog.get_logger()


# NOTE: parameter order/typing follows the rest of the codebase's endpoint->service
# convention (db first, primitives for the acting user) -- see order_service.py's
# submit_order/cancel_order. Keep new service functions in this file consistent with
# this so a future mismatch with the call site fails at review time, not at runtime.
async def trigger_kill_switch(
    db: AsyncSession,
    data: KillSwitchRequest,
    user_id: uuid.UUID,
    user_email: str,
) -> KillSwitchEvent:
    """Cancel all open orders (DB + broker) and mark positions closed. Atomic."""
    open_orders = (await db.execute(
        select(Order)
        # events must be eager-loaded: Order.transition() appends to it, and an
        # implicit lazy-load on a freshly-queried async ORM object raises
        # MissingGreenlet rather than silently fetching it.
        .options(selectinload(Order.broker), selectinload(Order.events))
        .where(
            Order.user_id == user_id,
            Order.status.in_(["NEW", "SUBMITTED", "PARTIAL"]),
        )
    )).scalars().all()
    for order in open_orders:
        if order.broker_order_id:
            try:
                adapter = get_adapter(order.broker)
                await adapter.cancel_order(order.broker_order_id)
            except Exception:
                # One broker failure shouldn't abort the sweep -- the order is still
                # marked cancelled locally below and the kill switch keeps going.
                logger.warning(
                    "kill_switch_broker_cancel_failed",
                    order_id=str(order.id), broker_id=str(order.broker_id),
                    exc_info=True,
                )
        order.transition("CANCELLED", "Kill switch triggered")
        order.cancelled_at = datetime.now(timezone.utc)

    open_positions = (await db.execute(
        select(Position).where(Position.user_id == user_id, Position.is_open.is_(True))
    )).scalars().all()
    for pos in open_positions:
        pos.is_open   = False
        pos.closed_at = datetime.now(timezone.utc)

    event = KillSwitchEvent(
        triggered_by     = user_id,
        trigger_source   = "manual",
        reason           = data.reason,
        orders_cancelled = len(open_orders),
        positions_closed = len(open_positions),
        status           = "COMPLETE",
        completed_at     = datetime.now(timezone.utc),
    )
    db.add(event)
    db.add(Alert(
        severity = "P1", source = "RISK", category = "KILL_SWITCH",
        title    = f"Kill switch triggered by {user_email}",
        message  = data.reason,
    ))
    await db.flush()
    await write_audit(
        db, action="KILL_SWITCH", resource_type="system",
        resource_id=str(event.id), actor_id=user_id, actor_email=user_email,
        after_state={"orders_cancelled": len(open_orders), "positions_closed": len(open_positions)},
    )
    return event


async def compute_risk_metrics(db: AsyncSession, user_id: uuid.UUID) -> RiskMetricsOut:
    """
    Compute live risk metrics.
    VaR/CVaR from historical PnL return distribution (scipy/numpy).
    Falls back to GARCH parametric VaR if insufficient history.
    """
    positions = (await db.execute(
        select(Position).where(Position.user_id == user_id, Position.is_open.is_(True))
    )).scalars().all()

    snap = (await db.execute(
        select(PnLSnapshot)
        .where(PnLSnapshot.user_id == user_id)
        .order_by(PnLSnapshot.snapshot_at.desc())
        .limit(1)
    )).scalar_one_or_none()
    total_equity = float(snap.total_equity) if snap else 100_000.0

    # ── Historical VaR from 30-day equity returns ─────────────────────────
    snaps_30d = (await db.execute(
        select(PnLSnapshot)
        .where(
            PnLSnapshot.user_id == user_id,
            PnLSnapshot.snapshot_at >= datetime.now(timezone.utc) - timedelta(days=30),
        )
        .order_by(PnLSnapshot.snapshot_at)
    )).scalars().all()

    if len(snaps_30d) >= 10:
        eq  = np.array([float(s.total_equity) for s in snaps_30d])
        ret = np.diff(eq) / eq[:-1]
        var95_pct = float(np.percentile(ret, 5))
        var99_pct = float(np.percentile(ret, 1))
        tail      = ret[ret <= var95_pct]
        cvar_pct  = float(np.mean(tail)) if len(tail) else var95_pct * 1.3
        var95 = abs(var95_pct) * total_equity
        var99 = abs(var99_pct) * total_equity
        cvar  = abs(cvar_pct)  * total_equity
        peaks  = np.maximum.accumulate(eq)
        dds    = (eq - peaks) / peaks * 100
        drawdown_current = float(dds[-1])
    else:
        # Parametric VaR via GARCH vol
        try:
            from app.models.all_models import Symbol
            sym = (await db.execute(
                select(Symbol).where(Symbol.is_active.is_(True)).limit(1)
            )).scalar_one_or_none()
            if not sym:
                raise ValueError("no symbol")
            ticks = list(reversed((await db.execute(
                select(MarketTick)
                .where(MarketTick.symbol_id == sym.id)
                .order_by(MarketTick.time.desc())
                .limit(100)
            )).scalars().all()))
            prices = [float(t.price) for t in ticks]
            from app.services.quant_engine import estimate_volatility_garch
            vol_data  = estimate_volatility_garch(prices)
            daily_vol = vol_data["daily_vol"] / 100
            var95 = round(daily_vol * 1.645 * total_equity, 2)
            var99 = round(daily_vol * 2.326 * total_equity, 2)
            cvar  = round(daily_vol * 2.063 * total_equity, 2)
        except Exception:
            var95 = round(total_equity * 0.025, 2)
            var99 = round(total_equity * 0.038, 2)
            cvar  = round(total_equity * 0.046, 2)
        drawdown_current = 0.0

    # ── Daily loss from today fills ───────────────────────────────────────
    today_start = datetime.combine(date.today(), datetime.min.time()).replace(tzinfo=timezone.utc)
    today_fills = (await db.execute(
        select(Fill)
        .join(Order, Fill.order_id == Order.id)
        .where(Order.user_id == user_id, Fill.filled_at >= today_start)
    )).scalars().all()
    daily_pnl = -sum(float(f.commission) + float(f.funding_cost) for f in today_fills)

    # ── Portfolio notional + leverage (mark-to-market) ────────────────────
    margin_used = float(sum(float(p.margin_used) for p in positions))
    notional    = 0.0
    for pos in positions:
        tick = (await db.execute(
            select(MarketTick)
            .where(MarketTick.symbol_id == pos.symbol_id)
            .order_by(MarketTick.time.desc())
            .limit(1)
        )).scalar_one_or_none()
        mtm = float(tick.price) if tick else float(pos.avg_cost)
        notional += float(pos.qty) * mtm

    leverage     = round(notional / total_equity, 4) if total_equity else 0.0
    margin_avail = round(total_equity - margin_used, 2)

    # ── Risk limits ───────────────────────────────────────────────────────
    limits = {
        lim.limit_type: float(lim.limit_value)
        for lim in (await db.execute(
            select(RiskLimit).where(RiskLimit.is_active.is_(True))
        )).scalars().all()
    }
    daily_loss_limit = limits.get("daily_loss_limit", settings.DEFAULT_DAILY_LOSS_LIMIT)
    drawdown_limit   = limits.get("max_drawdown_pct",  settings.DEFAULT_MAX_DRAWDOWN_PCT)

    triggers_today = (await db.execute(
        select(func.count(KillSwitchEvent.id))
        .where(KillSwitchEvent.triggered_by == user_id, KillSwitchEvent.created_at >= today_start)
    )).scalar_one() or 0

    return RiskMetricsOut(
        var95            = round(var95, 2),
        var99            = round(var99, 2),
        cvar             = round(cvar, 2),
        drawdown_current = round(drawdown_current, 4),
        drawdown_limit   = drawdown_limit,
        leverage         = leverage,
        margin_used      = round(margin_used, 2),
        margin_avail     = margin_avail,
        daily_loss       = round(daily_pnl, 2),
        daily_loss_limit = daily_loss_limit,
        kill_switch_armed= True,
        triggers_today   = triggers_today,
    )
