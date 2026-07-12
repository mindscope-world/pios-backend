from datetime import datetime, timedelta, timezone
import math
import statistics
from typing import Any
import uuid
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import HTTPException

from app.models.all_models import MarketTick, Position, RegimeState, Symbol


def safe_ms(ts: datetime) -> float:
    """Age of a DB timestamp in milliseconds, tz-safe."""
    now = datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (now - ts).total_seconds() * 1000

def sharpe(returns: list[float]) -> float:
    if len(returns) < 2:
        return 0.0
    mu = statistics.mean(returns)
    sd = statistics.stdev(returns)
    return round((mu / sd) * math.sqrt(252), 4) if sd else 0.0


def max_drawdown(equity: list[float]) -> float:
    peak = -math.inf
    max_dd = 0.0
    for v in equity:
        peak = max(peak, v)
        dd = (v - peak) / peak if peak else 0.0
        max_dd = min(max_dd, dd)
    return round(max_dd * 100, 4)


async def primary_symbol(db: AsyncSession) -> Symbol | None:
    """Return the most-traded active symbol in the last 24 h."""
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    result = await db.execute(
        select(Symbol.id, func.count(MarketTick.id).label("cnt"))
        .join(MarketTick, MarketTick.symbol_id == Symbol.id)
        .where(Symbol.is_active.is_(True), MarketTick.time >= since)
        .group_by(Symbol.id)
        .order_by(desc("cnt"))
        .limit(1)
    )
    row = result.first()
    if not row:
        result2 = await db.execute(select(Symbol).where(Symbol.is_active.is_(True)).limit(1))
        return result2.scalar_one_or_none()
    result3 = await db.execute(select(Symbol).where(Symbol.id == row[0]))
    return result3.scalar_one_or_none()


async def get_symbol_by_name(db: AsyncSession, symbol: str) -> Symbol:
    result = await db.execute(select(Symbol).where(Symbol.symbol == symbol))
    sym = result.scalar_one_or_none()
    if not sym:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found")
    return sym


async def recent_ticks(db: AsyncSession, symbol_id: int, limit: int = 200) -> list[MarketTick]:
    result = await db.execute(
        select(MarketTick)
        .where(MarketTick.symbol_id == symbol_id)
        .order_by(MarketTick.time.desc())
        .limit(limit)
    )
    ticks = result.scalars().all()
    return list(reversed(ticks))  # chronological order


async def latest_regime(db: AsyncSession, symbol_id: int) -> RegimeState | None:
    result = await db.execute(
        select(RegimeState)
        .where(RegimeState.symbol_id == symbol_id)
        .order_by(RegimeState.time.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def open_positions(db: AsyncSession, user_id: uuid.UUID) -> list[Position]:
    result = await db.execute(
        select(Position)
        .where(Position.user_id == user_id, Position.is_open.is_(True))
    )
    return result.scalars().all()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
 
 
def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
 
 
async def get_primary_with_ticks(
    db: AsyncSession,
    n_ticks: int = 200,
) -> tuple[Symbol | None, list]:
    """Return (symbol_row, ticks); both are empty/None-safe."""
    sym = await primary_symbol(db)
    if sym is None:
        return None, []
    ticks = await recent_ticks(db, sym.id, n_ticks)
    return sym, ticks or []


async def get_symbol_with_ticks(
    db: AsyncSession,
    symbol: str | None,
    n_ticks: int = 200,
) -> tuple[Symbol | None, list]:
    """
    Like get_primary_with_ticks, but honors an explicit symbol name when
    given (falling back to the primary symbol when None). Worker-safe: an
    unknown symbol returns (None, []) rather than raising HTTPException.
    """
    if not symbol:
        return await get_primary_with_ticks(db, n_ticks)
    result = await db.execute(select(Symbol).where(Symbol.symbol == symbol))
    sym = result.scalar_one_or_none()
    if sym is None:
        return None, []
    ticks = await recent_ticks(db, sym.id, n_ticks)
    return sym, ticks or []