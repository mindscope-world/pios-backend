from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func
from sqlalchemy.orm import aliased
from sqlalchemy.sql import over
from app.core import deps
from app.core.deps import require_quant
from app.models.all_models import MarketTick, Symbol, User

router = APIRouter(prefix="/market", tags=["market-data"])


@router.get("/ticks/{base}/{quote}")
async def get_latest_ticks(
    base: str,
    quote: str,
    limit: int = 50,
    db: AsyncSession = Depends(deps.get_db),
):
    symbol = f"{base}/{quote}"  # or f"{base}-{quote}" depending on DB

    stmt = (
        select(MarketTick)
        .join(Symbol)
        .where(Symbol.symbol == symbol)
        .order_by(desc(MarketTick.time))
        .limit(limit)
    )
    result = await db.execute(stmt)
    return result.scalars().all()


@router.get("/tickers")
async def get_ticker_snapshots(
    symbols: list[str] = Query(..., min_length=1, max_length=20),
    db: AsyncSession = Depends(deps.get_db),
):
    """Latest price snapshot for multiple symbols (Powers TopBar)."""

    results = []

    for sym in symbols:

        stmt = (
            select(MarketTick)
            .join(Symbol)
            .where(Symbol.symbol == sym)
            .order_by(desc(MarketTick.time))
            .limit(2)  # 👈 IMPORTANT: latest + previous
        )

        ticks = (await db.execute(stmt)).scalars().all()

        if not ticks:
            continue

        latest = ticks[0]
        prev = ticks[1] if len(ticks) > 1 else None

        change_pct = 0.0

        if prev and prev.price != 0:
            change_pct = ((latest.price - prev.price) / prev.price) * 100

        results.append({
            "symbol": sym,
            "price": latest.price,
            "change_pct": round(change_pct, 4),
        })

    return results


@router.get("/tickers/latest")
async def get_latest_tickers(
    symbols: list[str] | None = Query(None),
    db: AsyncSession = Depends(deps.get_db),
):
    # Window function: rank ticks per symbol (latest first)
    ranked = select(
        MarketTick.id,
        MarketTick.symbol_id,
        MarketTick.price,
        MarketTick.time,
        func.row_number()
        .over(
            partition_by=MarketTick.symbol_id,
            order_by=MarketTick.time.desc()
        )
        .label("rnk")
    ).subquery()

    latest = aliased(ranked)
    previous = aliased(ranked)

    stmt = (
        select(
            Symbol.symbol,
            latest.c.price.label("latest_price"),
            previous.c.price.label("prev_price"),
        )
        .join(Symbol, Symbol.id == latest.c.symbol_id)
        .outerjoin(
            previous,
            (previous.c.symbol_id == latest.c.symbol_id)
            & (previous.c.rnk == 2),
        )
        .where(latest.c.rnk == 1)
    )

    # Optional filter
    if symbols:
        stmt = stmt.where(Symbol.symbol.in_(symbols))

    result = await db.execute(stmt)
    rows = result.all()

    output = []
    for symbol, latest_price, prev_price in rows:
        change_pct = 0.0

        if prev_price and prev_price != 0:
            change_pct = ((latest_price - prev_price) / prev_price) * 100

        output.append({
            "symbol": symbol,
            "price": latest_price,
            "change_pct": round(change_pct, 4),
        })

    return output


@router.post("/backfill")
async def trigger_historical_backfill(
    symbol: str = Query(...),
    days: int = Query(30, ge=1, le=180),
    timeframe: str = Query("5m"),
    exchange_id: str | None = Query(None),
    _: User = Depends(require_quant),
):
    """
    Queue a historical backfill (Guide Ch.12 — "historical data depth for
    research"): candle-derived synthetic ticks, written with candle_ref set
    so they're distinguishable from real streamed ticks. See
    historical_backfill_service.py for exactly what this does and does not
    reconstruct. Admin/quant-gated — a bulk import against a live vendor
    API is an operator action, not something any authenticated user should
    be able to fire at will.
    """
    from app.workers.backtest_worker import historical_backfill_task
    try:
        task = historical_backfill_task.delay(symbol, days=days, timeframe=timeframe, exchange_id=exchange_id)
        return {"queued": True, "symbol": symbol, "days": days, "timeframe": timeframe, "celery_task_id": task.id}
    except Exception:
        from app.services.historical_backfill_service import backfill_symbol
        result = backfill_symbol(symbol, days=days, timeframe=timeframe, exchange_id=exchange_id)
        return {"queued": False, "ran_inline": True, **result}
