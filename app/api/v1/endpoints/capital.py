# app/api/v1/endpoints/capital.py
"""
Capital Allocation endpoints.

Derives allocation from open positions (grouped by base asset)
and PnL snapshots.  Rebalance triggers an async job placeholder
that queues a backtest-style recalculation.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, require_admin
from app.db.session import get_db
from app.models.all_models import Position, PnLSnapshot, Symbol, User

router = APIRouter(prefix="/capital", tags=["capital"])


@router.get("/allocation")
async def capital_allocation(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # Latest equity snapshot
    snap_result = await db.execute(
        select(PnLSnapshot)
        .where(PnLSnapshot.user_id == current_user.id)
        .order_by(PnLSnapshot.snapshot_at.desc())
        .limit(1)
    )
    snap = snap_result.scalar_one_or_none()
    total_equity = float(snap.total_equity) if snap else 100_000.0
    cash_balance = float(snap.cash_balance) if snap else total_equity * 0.20

    # Open positions
    pos_result = await db.execute(
        select(Position)
        .where(Position.user_id == current_user.id, Position.is_open.is_(True))
    )
    positions = pos_result.scalars().all()

    # Fetch symbol info
    sym_ids = list({p.symbol_id for p in positions})
    sym_map: dict[int, Symbol] = {}
    if sym_ids:
        sres = await db.execute(select(Symbol).where(Symbol.id.in_(sym_ids)))
        for s in sres.scalars().all():
            sym_map[s.id] = s

    # Group by base asset
    asset_exposure: dict[str, float] = {}
    for p in positions:
        sym = sym_map.get(p.symbol_id)
        if not sym:
            continue
        base = sym.base_asset.upper()
        exposure = float(p.qty) * float(p.avg_cost)
        asset_exposure[base] = asset_exposure.get(base, 0) + exposure

    deployed = sum(asset_exposure.values())
    cash_pct  = round((cash_balance / total_equity) * 100, 2) if total_equity else 20.0
    deployed_pct = round((deployed / total_equity) * 100, 2) if total_equity else 0.0

    # ── HRP target weights via quant_engine ──────────────────────────────
    # Build return series per asset from recent ticks
    from app.models.all_models import MarketTick
    returns_matrix: dict[str, list[float]] = {}
    for sym_obj in [s for s in (await db.execute(
        select(Symbol).where(Symbol.is_active.is_(True))
    )).scalars().all()]:
        base = sym_obj.base_asset.upper()
        if base not in asset_exposure:
            continue
        ticks_r = await db.execute(
            select(MarketTick)
            .where(MarketTick.symbol_id == sym_obj.id)
            .order_by(MarketTick.time.desc())
            .limit(100)
        )
        t_list = list(reversed(ticks_r.scalars().all()))
        if len(t_list) >= 10:
            prices = [float(t.price) for t in t_list]
            import numpy as np
            rets = list(np.diff(np.log(np.array(prices) + 1e-10)))
            returns_matrix[base] = rets

    from app.services.quant_engine import compute_hrp_allocation
    if len(returns_matrix) >= 2:
        hrp_weights = compute_hrp_allocation(returns_matrix)
        # Scale to deployed capital
        deployed_total = sum(asset_exposure.values()) or 1.0
        total_deployed_pct = deployed_pct
    else:
        n = max(len(asset_exposure), 1)
        hrp_weights = {k: 1.0/n for k in asset_exposure}
        total_deployed_pct = deployed_pct

    slices = []
    for asset, exposure in sorted(asset_exposure.items(), key=lambda x: -x[1]):
        current_pct = round((exposure / total_equity) * 100, 2) if total_equity else 0.0
        # HRP target weight × deployed pct
        raw_w = hrp_weights.get(asset, 1.0 / max(len(asset_exposure), 1))
        target_pct = round(raw_w * total_deployed_pct, 2)
        gmig_modifier = 1.0
        slices.append({
            "asset": asset,
            "target_pct": target_pct,
            "current_pct": current_pct,
            "value_usd": round(exposure, 2),
            "gmig_modifier": gmig_modifier,
        })

    # Cash slice
    slices.append({
        "asset": "USDT",
        "target_pct": round(cash_pct, 2),
        "current_pct": cash_pct,
        "value_usd": round(cash_balance, 2),
        "gmig_modifier": 1.0,
    })

    rebalance_needed = any(
        abs(s["current_pct"] - s["target_pct"]) > 5.0
        for s in slices
    )

    # Last rebalance: last time a kill switch or manual close happened
    from app.models.all_models import KillSwitchEvent
    ks_result = await db.execute(
        select(KillSwitchEvent)
        .where(KillSwitchEvent.triggered_by == current_user.id)
        .order_by(KillSwitchEvent.created_at.desc())
        .limit(1)
    )
    ks = ks_result.scalar_one_or_none()
    last_rebalanced = ks.created_at.isoformat() if ks else (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()

    return {
        "total_equity": round(total_equity, 2),
        "cash_pct": cash_pct,
        "deployed_pct": deployed_pct,
        "rebalance_needed": rebalance_needed,
        "last_rebalanced_at": last_rebalanced,
        "slices": slices,
    }


@router.post("/rebalance")
async def trigger_rebalance(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Enqueue a capital rebalance job.
    In production this would dispatch a Celery task.
    We create a synthetic BacktestJob as a rebalance job record.
    """
    from app.models.all_models import BacktestJob, Strategy
    from app.services.audit_service import write_audit

    # Find any strategy to attach to
    strat_result = await db.execute(
        select(Strategy)
        .where(Strategy.created_by == admin.id)
        .limit(1)
    )
    strategy = strat_result.scalar_one_or_none()

    if not strategy:
        # Create a synthetic job_id without persisting
        job_id = str(uuid.uuid4())
        return {"job_id": job_id}

    job = BacktestJob(
        strategy_id=strategy.id,
        submitted_by=admin.id,
        status="QUEUED",
        progress_pct=0,
        start_date=datetime.now(timezone.utc).date().isoformat(),
        end_date=datetime.now(timezone.utc).date().isoformat(),
        symbols=["REBALANCE"],
        cost_model="FULL",
        config={"type": "REBALANCE"},
    )
    db.add(job)

    await write_audit(db, "REBALANCE_TRIGGERED", "capital", str(admin.id),
                      actor_id=admin.id, actor_email=admin.email)
    await db.commit()
    await db.refresh(job)

    return {"job_id": str(job.id)}
