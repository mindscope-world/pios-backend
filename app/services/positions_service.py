import uuid
from datetime import datetime, timezone, timedelta
from fastapi import Query
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession
from app.helpers.helpers import sharpe as compute_sharpe
from app.models.all_models import Position, PnLSnapshot, RiskLimit, User
from app.schemas.all_schemas import (
    PositionOut, EquityPoint, PortfolioMetricsOut,
)
from app.core.config import settings


# ── Per-trader position bookkeeping ──────────────────────────────────────────
# Position rows are the *derived* state of a trader's fills. Nothing else in
# the codebase writes them (the intelligence worker only reads), so every code
# path that records a Fill must call apply_fill_to_position() in the same
# transaction: order_service.submit_order (instant fills) and
# execution_algo.run_algo_order (algo slices).

def _apply_side_sign(side: str) -> int:
    return 1 if side == "BUY" else -1


async def apply_fill_to_position(
    db: AsyncSession,
    *,
    user_id: uuid.UUID,
    broker_id: uuid.UUID,
    strategy_id: uuid.UUID | None,
    symbol_id: int,
    side: str,          # "BUY" | "SELL" (order side, not position side)
    qty: float,
    price: float,
    commission: float = 0.0,
) -> Position:
    """
    Net a fill into the trader's open position for (user, symbol, broker),
    creating/flipping/closing it as needed. Realized P&L accrues on the
    position when a fill reduces it; commissions reduce realized P&L so the
    portfolio metrics stay honest about costs.
    """
    result = await db.execute(
        select(Position).where(
            Position.user_id == user_id,
            Position.symbol_id == symbol_id,
            Position.broker_id == broker_id,
            Position.is_open == True,  # noqa: E712
        ).with_for_update()
    )
    pos = result.scalars().first()

    fill_signed = _apply_side_sign(side) * qty

    if pos is None:
        pos = Position(
            user_id=user_id,
            broker_id=broker_id,
            strategy_id=strategy_id,
            symbol_id=symbol_id,
            side="LONG" if fill_signed > 0 else "SHORT",
            qty=abs(fill_signed),
            avg_cost=price,
            realized_pnl=-commission,
            unrealized_pnl=0.0,
            is_open=True,
        )
        db.add(pos)
        await db.flush()
        return pos

    pos_signed = float(pos.qty) if pos.side == "LONG" else -float(pos.qty)
    new_signed = pos_signed + fill_signed
    avg_cost = float(pos.avg_cost)
    realized = float(pos.realized_pnl) - commission

    if pos_signed * fill_signed >= 0:
        # Same direction — extend, weighted-average the cost basis.
        total = abs(pos_signed) + abs(fill_signed)
        pos.avg_cost = (avg_cost * abs(pos_signed) + price * abs(fill_signed)) / total
        pos.qty = total
    else:
        # Opposite direction — reduce (and possibly flip). P&L realizes on
        # the closed quantity: (exit - entry) for longs, (entry - exit) shorts.
        closed_qty = min(abs(pos_signed), abs(fill_signed))
        direction = 1 if pos_signed > 0 else -1
        realized += (price - avg_cost) * closed_qty * direction
        if abs(new_signed) < 1e-9:
            pos.qty = 0.0
            pos.is_open = False
            pos.closed_at = datetime.now(timezone.utc)
            pos.unrealized_pnl = 0.0
        elif new_signed * pos_signed > 0:
            pos.qty = abs(new_signed)  # partial reduce, basis unchanged
        else:
            # Flipped through zero — remainder opens at the fill price.
            pos.side = "LONG" if new_signed > 0 else "SHORT"
            pos.qty = abs(new_signed)
            pos.avg_cost = price

    pos.realized_pnl = realized
    if pos.is_open:
        mark_direction = 1 if pos.side == "LONG" else -1
        pos.unrealized_pnl = (price - float(pos.avg_cost)) * float(pos.qty) * mark_direction
    await db.flush()
    return pos


async def write_pnl_snapshot(db: AsyncSession, user_id: uuid.UUID) -> PnLSnapshot:
    """
    Append an equity-curve point for the user derived from their Position
    rows. Called after each fill so /positions/equity-curve and
    /positions/metrics reflect the trader's own activity.
    """
    result = await db.execute(select(Position).where(Position.user_id == user_id))
    positions = result.scalars().all()

    realized = sum(float(p.realized_pnl) for p in positions)
    unrealized = sum(float(p.unrealized_pnl) for p in positions if p.is_open)
    open_notional = sum(float(p.qty) * float(p.avg_cost) for p in positions if p.is_open)
    starting = settings.DEFAULT_STARTING_EQUITY_USD

    snap = PnLSnapshot(
        user_id=user_id,
        total_equity=starting + realized + unrealized,
        realized_pnl=realized,
        unrealized_pnl=unrealized,
        cash_balance=starting + realized - open_notional,
    )
    db.add(snap)
    await db.flush()
    return snap

async def compute_positions(
    current_user: User,
    db: AsyncSession,
    is_open: bool = True,
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


async def compute_portfolio_metrics(
    current_user: User,
    db: AsyncSession,
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

    total_equity     = snap.total_equity if snap else settings.DEFAULT_STARTING_EQUITY_USD
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

    # Rolling Sharpe from the 90-day equity curve already fetched above.
    sharpe_value = None
    if len(snaps_90) >= 10:
        equities = [float(s.total_equity) for s in snaps_90]
        returns = [
            (equities[i] - equities[i - 1]) / equities[i - 1]
            for i in range(1, len(equities))
            if equities[i - 1]
        ]
        if len(returns) >= 2:
            sharpe_value = compute_sharpe(returns)

    # Win rate from closed positions' realized PnL -- real per-position outcomes,
    # not a fabricated constant.
    closed_result = await db.execute(
        select(Position).where(Position.user_id == current_user.id, Position.is_open == False)  # noqa: E712
    )
    closed_positions = closed_result.scalars().all()
    win_rate_value = None
    if closed_positions:
        wins = sum(1 for p in closed_positions if float(p.realized_pnl) > 0)
        win_rate_value = round(wins / len(closed_positions) * 100, 2)

    # Drawdown limit from the active RiskLimit row, matching risk_service.py's pattern.
    limit_result = await db.execute(
        select(RiskLimit).where(
            RiskLimit.limit_type == "max_drawdown_pct",
            RiskLimit.is_active == True,  # noqa: E712
        )
    )
    limit = limit_result.scalar_one_or_none()
    drawdown_limit_value = float(limit.limit_value) if limit else settings.DEFAULT_MAX_DRAWDOWN_PCT

    return PortfolioMetricsOut(
        total_equity=round(total_equity, 2),
        equity_change=round(equity_change, 2),
        equity_change_pct=round(equity_change_pct, 4),
        realized_pnl=round(realized_pnl, 2),
        realized_today=round(snap.realized_pnl - (yesterday.realized_pnl if yesterday else 0), 2) if snap else 0,
        unrealized_pnl=round(unrealized_pnl, 2),
        active_strategies=active_strategies,
        max_drawdown=round(max_dd, 4),
        drawdown_limit=drawdown_limit_value,
        sharpe=sharpe_value,
        win_rate=win_rate_value,
    )


async def compute_equity_curve(
    current_user: User,
    db: AsyncSession,
    days: int = 90,
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
