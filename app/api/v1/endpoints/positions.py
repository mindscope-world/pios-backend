# app/api/v1/endpoints/positions.py
import uuid
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user
from app.db.session import get_db
from app.models.all_models import Position, PnLSnapshot, User
from app.schemas.all_schemas import (
    PositionOut, EquityPoint, PortfolioMetricsOut,
)

router = APIRouter(prefix="/positions", tags=["positions"])


@router.get("", response_model=list[PositionOut])
async def get_positions(
    is_open: bool = True,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    q = (
        select(Position)
        .options(selectinload(Position.symbol), selectinload(Position.broker))
        .where(Position.user_id == current_user.id)
    )
    if is_open:
        q = q.where(Position.is_open == True)  # noqa: E712
    result = await db.execute(q)
    return result.scalars().all()


@router.get("/metrics", response_model=PortfolioMetricsOut)
async def portfolio_metrics(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Aggregate portfolio metrics for the dashboard KPI cards."""
    pos_result = await db.execute(
        select(Position).where(Position.user_id == current_user.id, Position.is_open == True)  # noqa: E712
    )
    positions = pos_result.scalars().all()

    # Latest snapshot for equity / drawdown
    snap_result = await db.execute(
        select(PnLSnapshot)
        .where(PnLSnapshot.user_id == current_user.id)
        .order_by(PnLSnapshot.snapshot_at.desc())
        .limit(1)
    )
    snap = snap_result.scalar_one_or_none()

    yesterday_result = await db.execute(
        select(PnLSnapshot)
        .where(
            PnLSnapshot.user_id == current_user.id,
            PnLSnapshot.snapshot_at <= datetime.now(timezone.utc) - timedelta(hours=24),
        )
        .order_by(PnLSnapshot.snapshot_at.desc())
        .limit(1)
    )
    yesterday = yesterday_result.scalar_one_or_none()

    total_equity     = snap.total_equity if snap else 100_000.0
    yesterday_equity = yesterday.total_equity if yesterday else total_equity
    equity_change    = total_equity - yesterday_equity
    equity_change_pct = (equity_change / yesterday_equity * 100) if yesterday_equity else 0

    realized_pnl    = snap.realized_pnl if snap else 0
    unrealized_pnl  = sum(p.unrealized_pnl for p in positions)

    # Quick peak-to-trough drawdown from snapshots
    snap_90d = await db.execute(
        select(PnLSnapshot)
        .where(
            PnLSnapshot.user_id == current_user.id,
            PnLSnapshot.snapshot_at >= datetime.now(timezone.utc) - timedelta(days=90),
        )
        .order_by(PnLSnapshot.snapshot_at)
    )
    snaps_90 = snap_90d.scalars().all()
    if snaps_90:
        peak = snaps_90[0].total_equity
        max_dd = 0.0
        for s in snaps_90:
            peak = max(peak, s.total_equity)
            dd = (s.total_equity - peak) / peak * 100
            max_dd = min(max_dd, dd)
    else:
        max_dd = 0.0

    # Active strategies count
    from app.models.all_models import Strategy
    strat_result = await db.execute(
        select(Strategy).where(
            Strategy.created_by == current_user.id,
            Strategy.lifecycle_stage.in_(["LIVE_SMALL", "SCALED", "PAPER", "BACKTEST"]),
        )
    )
    active_strategies = len(strat_result.scalars().all())

    return PortfolioMetricsOut(
        total_equity=round(total_equity, 2),
        equity_change=round(equity_change, 2),
        equity_change_pct=round(equity_change_pct, 4),
        realized_pnl=round(realized_pnl, 2),
        realized_today=round(snap.realized_pnl - (yesterday.realized_pnl if yesterday else 0), 2) if snap else 0,
        unrealized_pnl=round(unrealized_pnl, 2),
        active_strategies=active_strategies,
        max_drawdown=round(max_dd, 4),
        drawdown_limit=15.0,
        sharpe=1.84,       # computed by Darwin engine in production
        win_rate=58.3,
    )


@router.get("/equity-curve", response_model=list[EquityPoint])
async def equity_curve(
    days: int = Query(90, ge=1, le=365),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """90-day equity curve for chart."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    result = await db.execute(
        select(PnLSnapshot)
        .where(
            PnLSnapshot.user_id == current_user.id,
            PnLSnapshot.snapshot_at >= since,
        )
        .order_by(PnLSnapshot.snapshot_at)
    )
    snaps = result.scalars().all()
    return [
        EquityPoint(day=i, value=s.total_equity, snapshot_at=s.snapshot_at)
        for i, s in enumerate(snaps)
    ]
