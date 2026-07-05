"""
Trader Behavior Monitor endpoints.

Derives behavior metrics from order history:
— overrides = orders that deviate from strategy signal (filled where block expected, etc.)
— score = function of deviation rate, override reversal rate, frequency vs baseline
"""
from __future__ import annotations

import statistics
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user
from app.db.session import get_db
from app.models.all_models import Order, User

router = APIRouter(prefix="/behavior", tags=["behavior"])


async def _hourly_order_count(db, user_id, since) -> int:
    result = await db.execute(
        select(func.count(Order.id))
        .where(Order.user_id == user_id, Order.created_at >= since)
    )
    return result.scalar_one() or 0


@router.get("/session")
async def behavior_session(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Compute trader behavior score for the current session."""
    now = datetime.now(timezone.utc)
    hour_ago   = now - timedelta(hours=1)
    day_ago    = now - timedelta(hours=24)
    week_ago   = now - timedelta(days=7)

    # Orders this hour
    overrides_this_hour = await _hourly_order_count(db, current_user.id, hour_ago)
    override_max_per_hour = 10

    # Filled vs cancelled deviation from expected (filled = ALLOW, rejected = override)
    day_result = await db.execute(
        select(Order.status)
        .where(Order.user_id == current_user.id, Order.created_at >= day_ago)
    )
    day_orders = day_result.scalars().all()
    total_day = max(len(day_orders), 1)
    cancelled_count = sum(1 for s in day_orders if s == "CANCELLED")
    rejected_count  = sum(1 for s in day_orders if s == "REJECTED")

    # Override reversal rate = cancelled or rejected / total
    override_reversal_rate = round((cancelled_count + rejected_count) / total_day, 4)

    # Frequency vs baseline: compare this hour to rolling average
    week_result = await db.execute(
        select(func.count(Order.id))
        .where(Order.user_id == current_user.id, Order.created_at >= week_ago)
    )
    week_total = week_result.scalar_one() or 0
    baseline_per_hour = week_total / (7 * 24) if week_total else 1.0
    trade_freq_vs_baseline = round(overrides_this_hour / max(baseline_per_hour, 0.1), 4)

    # Deviation from AI: approximated by order status mix vs optimal (all FILLED)
    filled_count = sum(1 for s in day_orders if s in ("FILLED", "PARTIAL"))
    deviation_from_ai_pct = round((1 - filled_count / total_day) * 100, 1)

    # Emotional score: high if frequency is very high or deviation is high
    emotional_score = round(
        min(1.0, (override_reversal_rate * 0.4 + (trade_freq_vs_baseline - 1) * 0.3 + deviation_from_ai_pct / 200)),
        4
    )

    # Behavior score: 100 = perfect
    score = round(max(0, min(100,
        100
        - override_reversal_rate * 30
        - max(0, (trade_freq_vs_baseline - 1.5)) * 10
        - deviation_from_ai_pct * 0.3
        - emotional_score * 20
    )), 1)

    if score >= 85:
        status = "OPTIMAL"
    elif score >= 65:
        status = "NORMAL"
    elif score >= 40:
        status = "WARNING"
    else:
        status = "LOCKED"

    session_locked = status == "LOCKED"

    return {
        "score": score,
        "status": status,
        "overrides_this_hour": overrides_this_hour,
        "override_max_per_hour": override_max_per_hour,
        "deviation_from_ai_pct": deviation_from_ai_pct,
        "trade_frequency_vs_baseline": trade_freq_vs_baseline,
        "override_reversal_rate": override_reversal_rate,
        "emotional_score": emotional_score,
        "session_locked": session_locked,
        "lock_expires_at": None,
    }


@router.get("/overrides")
async def behavior_overrides(
    hours: int = Query(24, ge=1, le=168),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    result = await db.execute(
        select(Order)
        .where(
            Order.user_id == current_user.id,
            Order.created_at >= since,
            Order.status.in_(["CANCELLED", "REJECTED", "FILLED"]),
        )
        .order_by(Order.created_at.desc())
        .limit(50)
    )
    orders = result.scalars().all()

    items = []
    for o in orders:
        # Determine AI signal vs trader action
        if o.status == "FILLED":
            ai_signal     = "ALLOW"
            trader_action = "EXECUTED"
            impact        = "GOOD"
            outcome_pnl   = float(o.filled_qty or 0) * 0.001  # small proxy
        elif o.status == "CANCELLED":
            ai_signal     = "ALLOW"
            trader_action = "CANCELLED MANUALLY"
            impact        = "POOR"
            outcome_pnl   = -5.0
        else:  # REJECTED
            ai_signal     = "BLOCK"
            trader_action = "ATTEMPTED OVERRIDE"
            impact        = "BAD"
            outcome_pnl   = -20.0

        items.append({
            "id": str(o.id),
            "occurred_at": o.created_at.isoformat(),
            "ai_signal": ai_signal,
            "trader_action": trader_action,
            "outcome_pnl": round(outcome_pnl, 2),
            "impact": impact,
        })

    return {"items": items}


@router.get("/trend")
async def behavior_trend(
    hours: int = Query(24, ge=1, le=168),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Hourly behavior score trend."""
    now   = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)

    # Fetch all orders in window
    result = await db.execute(
        select(Order)
        .where(Order.user_id == current_user.id, Order.created_at >= since)
        .order_by(Order.created_at)
    )
    orders = result.scalars().all()

    # Bucket into hourly slots
    output = []
    for h in range(hours):
        slot_start = since + timedelta(hours=h)
        slot_end   = slot_start + timedelta(hours=1)
        slot_orders = [
            o for o in orders
            if slot_start <= o.created_at.replace(tzinfo=timezone.utc) < slot_end
        ]

        if slot_orders:
            bad = sum(1 for o in slot_orders if o.status in ("REJECTED", "CANCELLED"))
            override_count = bad
            score = round(max(0, 100 - (bad / max(len(slot_orders), 1)) * 40), 1)
        else:
            override_count = 0
            score = 90.0  # idle sessions = good score

        output.append({
            "ts": slot_start.isoformat(),
            "score": score,
            "override_count": override_count,
        })

    return output
