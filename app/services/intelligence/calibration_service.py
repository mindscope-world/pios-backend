"""
§3.3 Calibration digest -- backend aggregation over a rolling window.

"Eligible setups taken vs. skipped" needs a persisted decision history to
count "eligible" against. The only one that exists is `QuantDecision`
(app/models/all_models.py, built for V10.1 PRS) -- but it's recorded by
the intelligence worker for the SYSTEM user across all symbols (throttled
to one row per symbol per ~5 min, see prs_service.py), not per-trader.
There is also no causal link from a QuantDecision row to a specific
Order: command_center_service.py's `strategy_id`/`strategy_name` fields
are cosmetic display data (the caller's most-recently-updated Strategy),
not a real link between "this setup" and "this order".

So this deliberately does NOT claim to compute a true "taken vs skipped"
rate per individual setup -- that data doesn't exist. Instead it reports
two honestly independent counts over the same window (system-wide
eligible/non-eligible setups, and the requesting user's own order
activity) side by side, plus a caveated approximate ratio, rather than
fabricating a per-setup join.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.all_models import QuantDecision, Order

NON_ELIGIBLE_DECISIONS = ("BLOCK", "WAIT", "REDUCE")


async def compute_calibration_digest(db: AsyncSession, user_id, hours: int = 24) -> dict:
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    eligible_setups = (await db.execute(
        select(func.count()).select_from(QuantDecision)
        .where(QuantDecision.time >= since, QuantDecision.decision == "ALLOW")
    )).scalar_one()

    non_eligible_setups = (await db.execute(
        select(func.count()).select_from(QuantDecision)
        .where(QuantDecision.time >= since, QuantDecision.decision.in_(NON_ELIGIBLE_DECISIONS))
    )).scalar_one()

    orders_placed_by_you = (await db.execute(
        select(func.count()).select_from(Order)
        .where(Order.user_id == user_id, Order.created_at >= since)
    )).scalar_one()

    by_symbol_rows = (await db.execute(
        select(QuantDecision.symbol_id, func.count())
        .where(QuantDecision.time >= since, QuantDecision.decision == "ALLOW")
        .group_by(QuantDecision.symbol_id)
    )).all()

    return {
        "window_hours": hours,
        "eligible_setups": eligible_setups,
        "non_eligible_setups": non_eligible_setups,
        "orders_placed_by_you": orders_placed_by_you,
        "eligible_setups_by_symbol_id": {row[0]: row[1] for row in by_symbol_rows},
        "approx_activity_ratio": (
            round(orders_placed_by_you / eligible_setups, 4) if eligible_setups else None
        ),
        "note": (
            "eligible_setups/non_eligible_setups are system-wide QuantDecision counts "
            "(the worker's recorded decisions, not yours specifically); "
            "orders_placed_by_you is your own order activity in the same window. There "
            "is no causal link between a specific decision and a specific order in this "
            "codebase, so approx_activity_ratio is an approximate correlate, not a true "
            "taken-vs-skipped rate per setup."
        ),
    }
