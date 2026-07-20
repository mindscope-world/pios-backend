"""
Capital Allocation endpoints.

Derives allocation from open positions (grouped by base asset)
and PnL snapshots.  Rebalance triggers an async job placeholder
that queues a backtest-style recalculation.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.all_models import Position, PnLSnapshot, Symbol, User, MarketTick, KillSwitchEvent, Strategy
from app.services.intelligence.cross_market_service import compute_gmig_snapshot
from app.services.quant_engine import compute_hrp_allocation
from app.services.intelligence import clock_bands as clock_bands_service
from app.helpers.helpers import primary_symbol, latest_regime


async def compute_capital_allocation(
    current_user: User,
    db: AsyncSession,
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
    sym_map: dict[str, Symbol] = {}
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

    # ── V10.4 D.2 -- per-AlphaClock exposure ──────────────────────────────
    # Positions only carry a strategy_id (and therefore a clock, via
    # Strategy.config.alpha_clock) when the order that created them was
    # placed with a strategy attached -- most positions won't be tagged, and
    # that's an honest state (see clock_bands.constrain, which reports 0%
    # rather than hiding an untagged/unconfigured clock).
    strategy_ids = list({p.strategy_id for p in positions if p.strategy_id})
    strategy_clock: dict[uuid.UUID, str | None] = {}
    if strategy_ids:
        strat_res = await db.execute(select(Strategy).where(Strategy.id.in_(strategy_ids)))
        for strat in strat_res.scalars().all():
            strategy_clock[strat.id] = (strat.config or {}).get("alpha_clock")

    clock_exposure_pct: dict[str, float] = {}
    for p in positions:
        clock = strategy_clock.get(p.strategy_id) if p.strategy_id else None
        if not clock:
            continue
        exposure = float(p.qty) * float(p.avg_cost)
        pct = (exposure / total_equity) * 100 if total_equity else 0.0
        clock_exposure_pct[clock] = clock_exposure_pct.get(clock, 0.0) + pct

    # ── HRP target weights via quant_engine ──────────────────────────────
    # Build return series per asset from recent ticks
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

    if len(returns_matrix) >= 2:
        hrp_weights = compute_hrp_allocation(returns_matrix)
        # Scale to deployed capital
        deployed_total = sum(asset_exposure.values()) or 1.0
        total_deployed_pct = deployed_pct
    else:
        n = max(len(asset_exposure), 1)
        hrp_weights = {k: 1.0/n for k in asset_exposure}
        total_deployed_pct = deployed_pct

    # Real cross-market modifier per asset (compute_gmig_snapshot only covers
    # forex pairs, so most crypto/equity assets fall back to 1.0 = neutral,
    # i.e. no signal available -- rather than always being 1.0 regardless of
    # whether any computation ever ran).
    gmig_lookup: dict[str, float] = {}
    try:
        gmig = await compute_gmig_snapshot(current_user, db)
        for m in gmig.get("modifiers", []):
            base = m["symbol"].split("/")[0].upper()
            gmig_lookup[base] = m["modifier"]
    except Exception:  # noqa: BLE001
        pass

    slices = []
    for asset, exposure in sorted(asset_exposure.items(), key=lambda x: -x[1]):
        current_pct = round((exposure / total_equity) * 100, 2) if total_equity else 0.0
        # HRP target weight × deployed pct
        raw_w = hrp_weights.get(asset, 1.0 / max(len(asset_exposure), 1))
        target_pct = round(raw_w * total_deployed_pct, 2)
        slices.append({
            "asset": asset,
            "target_pct": target_pct,
            "current_pct": current_pct,
            "value_usd": round(exposure, 2),
            "gmig_modifier": gmig_lookup.get(asset, 1.0),
        })

    # Cash slice — no cross-market modifier applies to cash.
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
    ks_result = await db.execute(
        select(KillSwitchEvent)
        .where(KillSwitchEvent.triggered_by == current_user.id)
        .order_by(KillSwitchEvent.created_at.desc())
        .limit(1)
    )
    ks = ks_result.scalar_one_or_none()
    last_rebalanced = ks.created_at.isoformat() if ks else (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()

    # V10.4 D.2 -- clamp per-clock exposure against admin-configured bands
    # for the current regime (reuses the same latest_regime/primary_symbol
    # helpers regime_service.py already uses -- no new regime detection).
    regime_label = None
    sym = await primary_symbol(db)
    if sym:
        regime_row = await latest_regime(db, sym.id)
        if regime_row:
            regime_label = regime_row.regime_label
    bands = await clock_bands_service.get_active_bands(db)
    clock_band_result = {
        "regime": clock_bands_service.V104_REGIME_MAP.get(regime_label) if regime_label else None,
        "clocks": clock_bands_service.constrain(clock_exposure_pct, regime_label, bands),
    }

    return {
        "total_equity": round(total_equity, 2),
        "cash_pct": cash_pct,
        "deployed_pct": deployed_pct,
        "rebalance_needed": rebalance_needed,
        "last_rebalanced_at": last_rebalanced,
        "slices": slices,
        "clock_bands": clock_band_result,
    }


async def compute_rebalance(
    admin: User,
    db: AsyncSession,
):
    """
    Enqueue a capital rebalance job: creates a BacktestJob record (the only
    job-tracking table available) and dispatches rebalance_portfolio_task,
    which recomputes real HRP target weights and writes them onto the job's
    full_report -- matching run_backtest_task's dispatch pattern.
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
        # BacktestJob.strategy_id is NOT NULL -- with no strategy to attach to
        # there's nowhere to persist a job row, so this returns an unpersisted
        # id rather than dispatching work with nothing to track it against.
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
    await db.flush()
    job_id = str(job.id)

    try:
        from app.workers.backtest_worker import rebalance_portfolio_task
        task = rebalance_portfolio_task.delay(job_id)
        job.celery_task_id = task.id
    except Exception:
        # If Celery isn't running, mark for manual run (matches strategies.py's
        # backtest-dispatch fallback).
        job.celery_task_id = "no-celery"

    await write_audit(db, "REBALANCE_TRIGGERED", "capital", str(admin.id),
                      actor_id=admin.id, actor_email=admin.email)
    await db.commit()
    await db.refresh(job)

    return {"job_id": str(job.id)}
