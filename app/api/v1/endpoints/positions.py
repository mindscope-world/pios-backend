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
from app.services.positions_service import compute_portfolio_metrics

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
    return await compute_portfolio_metrics(current_user, db)


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
