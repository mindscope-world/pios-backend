"""
Execution Quality / TCA Lite  +  Data Integrity Monitor service.

TCA is derived from Fill records (actual slippage vs expected).
Data integrity is derived from MarketTick recency and DQ scores.

All functions accept plain typed parameters — no FastAPI Query/Depends.
No user filtering — caller/channel layer handles that.
"""
from __future__ import annotations

import statistics
from datetime import datetime, timezone, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.all_models import Fill, Order, Symbol, MarketTick


# ─────────────────────────────────────────────────────────────────────────────
# Data Integrity
# ─────────────────────────────────────────────────────────────────────────────

async def compute_data_integrity_status(db: AsyncSession) -> dict:
    """
    Returns feed staleness and sync-drift for ALL active symbols.
    No user filtering — caller/channel layer handles that.
    """
    result  = await db.execute(select(Symbol).where(Symbol.is_active.is_(True)))
    symbols = result.scalars().all()

    now       = datetime.now(timezone.utc)
    feeds     = []
    latencies = []

    for sym in symbols:
        tick_result = await db.execute(
            select(MarketTick)
            .where(MarketTick.symbol_id == sym.id)
            .order_by(MarketTick.time.desc())
            .limit(2)
        )
        ticks = tick_result.scalars().all()

        if ticks:
            latest  = ticks[0]
            t_aware = latest.time if latest.time.tzinfo else latest.time.replace(tzinfo=timezone.utc)
            age_ms  = (now - t_aware).total_seconds() * 1000
            drift_ms = (
                abs((ticks[0].time - ticks[1].time).total_seconds() * 1000 - 1000)
                if len(ticks) >= 2 else 0.0
            )
        else:
            age_ms   = 999_999.0
            drift_ms = 0.0

        staleness_limit_ms = 5_000.0
        if age_ms < staleness_limit_ms * 0.5:
            staleness = "OK"
        elif age_ms < staleness_limit_ms:
            staleness = "WARN"
        else:
            staleness = "STALE"

        latencies.append(age_ms)
        feeds.append({
            "symbol":       sym.symbol,
            "last_tick_at": ticks[0].time.isoformat() if ticks else now.isoformat(),
            "age_ms":       round(age_ms, 1),
            "staleness":    staleness,
            "sync_drift_ms": round(drift_ms, 1),
        })

    return {
        "overall_healthy":    all(f["staleness"] == "OK" for f in feeds),
        "overall_latency_ms": round(statistics.mean(latencies), 1) if latencies else 9999.0,
        "sync_drift_ms":      round(statistics.mean([f["sync_drift_ms"] for f in feeds]), 1) if feeds else 0.0,
        "staleness_limit_ms": 5000.0,
        "feeds":              feeds,
    }


async def compute_feed_latency_chart(
    db: AsyncSession,
    symbols: list[str],
    samples: int = 20,
) -> list[dict]:
    """
    Returns per-tick latency samples for the requested symbols.
    samples clamped to [5, 200] defensively.
    No user filtering — caller/channel layer handles that.
    """
    samples = max(5, min(200, samples))
    output  = []

    for sym_str in symbols:
        sym_result = await db.execute(select(Symbol).where(Symbol.symbol == sym_str))
        sym        = sym_result.scalar_one_or_none()
        if not sym:
            continue

        ticks_result = await db.execute(
            select(MarketTick)
            .where(MarketTick.symbol_id == sym.id)
            .order_by(MarketTick.time.desc())
            .limit(samples)
        )
        ticks = list(reversed(ticks_result.scalars().all()))
        now   = datetime.now(timezone.utc)

        for t in ticks:
            t_aware = t.time if t.time.tzinfo else t.time.replace(tzinfo=timezone.utc)
            output.append({
                "ts":         t.time.isoformat(),
                "symbol":     sym_str,
                "latency_ms": round((now - t_aware).total_seconds() * 1000, 1),
            })

    output.sort(key=lambda x: x["ts"])
    return output


# ─────────────────────────────────────────────────────────────────────────────
# TCA (Transaction Cost Analysis)
# ─────────────────────────────────────────────────────────────────────────────

async def compute_tca_summary(
    db: AsyncSession,
    user_id: int,
    hours: int = 24,
) -> dict:
    """
    Returns TCA breakdown for a specific user's fills.
    hours clamped to [1, 168] defensively.
    user_id kept as an explicit parameter — TCA is inherently per-user financial data.
    """
    hours = max(1, min(168, hours))
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    result = await db.execute(
        select(Fill, Order, Symbol)
        .join(Order, Fill.order_id == Order.id)
        .join(Symbol, Fill.symbol_id == Symbol.id)
        .where(
            Order.user_id == user_id,
            Fill.filled_at >= since,
        )
        .order_by(Fill.filled_at.desc())
    )
    rows = result.all()

    total_expected_slip = 0.0
    total_actual_slip   = 0.0
    total_half_spread   = 0.0
    total_fees          = 0.0
    total_funding       = 0.0
    trades              = []

    for fill, order, sym in rows:
        qty          = float(fill.qty)
        fill_price   = float(fill.price)
        commission   = float(fill.commission)
        actual_slip  = float(fill.slippage_bps or 0) * fill_price / 10_000
        spread_cost  = float(fill.spread_cost_bps or 0) * fill_price / 10_000
        funding      = float(fill.funding_cost)
        expected_slip = spread_cost * 0.5

        total_expected_slip += expected_slip * qty
        total_actual_slip   += actual_slip * qty
        total_half_spread   += spread_cost * qty
        total_fees          += commission
        total_funding       += funding

        ratio      = (actual_slip / expected_slip) if expected_slip > 0 else 1.0
        exec_score = round(max(0, min(100, 100 - (ratio - 1) * 50)), 1)
        venue      = (order.algo_config or {}).get("algo_type", "Market")

        trades.append({
            "order_id":               str(order.id),
            "symbol":                 sym.symbol,
            "side":                   order.side,
            "qty":                    qty,
            "size_unit":              sym.base_asset,
            "entry_price":            fill_price,
            "expected_slippage_usd":  round(expected_slip * qty, 4),
            "actual_slippage_usd":    round(actual_slip * qty, 4),
            "execution_score_ai":     exec_score,
            "venue":                  venue,
            "latency_ms":             0,   # not stored per-fill yet
            "executed_at":            fill.filled_at.isoformat(),
        })

    notional       = sum(float(r[0].qty) * float(r[0].price) for r in rows) or 1.0
    total_cost_bps = round((total_fees + total_actual_slip) / notional * 10_000, 2)

    return {
        "expected_slippage_usd": round(total_expected_slip, 4),
        "actual_slippage_usd":   round(total_actual_slip, 4),
        "half_spread_usd":       round(total_half_spread, 4),
        "exchange_fee_usd":      round(total_fees, 4),
        "funding_impact_usd":    round(total_funding, 4),
        "total_cost_bps":        total_cost_bps,
        "trades":                trades,
    }


async def compute_slippage_chart(
    db: AsyncSession,
    user_id: int,
    hours: int = 24,
) -> list[dict]:
    """
    Returns per-fill expected vs actual slippage for chart rendering.
    hours clamped to [1, 168] defensively.
    user_id kept as an explicit parameter — slippage data is per-user.
    """
    hours = max(1, min(168, hours))
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    result = await db.execute(
        select(Fill, Order, Symbol)
        .join(Order, Fill.order_id == Order.id)
        .join(Symbol, Fill.symbol_id == Symbol.id)
        .where(Order.user_id == user_id, Fill.filled_at >= since)
        .order_by(Fill.filled_at)
    )
    rows = result.all()

    output = []
    for fill, order, sym in rows:
        fill_price  = float(fill.price)
        actual_slip = float(fill.slippage_bps or 0) * fill_price / 10_000
        spread_cost = float(fill.spread_cost_bps or 0) * fill_price / 10_000
        expected    = spread_cost * 0.5
        qty         = float(fill.qty)
        label       = f"{sym.base_asset}({'B' if order.side == 'BUY' else 'S'})"

        output.append({
            "label":    label,
            "expected": round(expected * qty, 4),
            "actual":   round(actual_slip * qty, 4),
        })

    return output


async def compute_ticks_by_id(
    db: AsyncSession,
    symbol_id: int,
    limit: int = 50,
) -> list[dict]:
    """
    Returns latest ticks for a symbol by numeric DB id.
    limit clamped to [1, 500] defensively.
    No user filtering — caller/channel layer handles that.
    """
    limit  = max(1, min(500, limit))
    result = await db.execute(
        select(MarketTick)
        .where(MarketTick.symbol_id == symbol_id)
        .order_by(MarketTick.time.desc())
        .limit(limit)
    )
    ticks = result.scalars().all()

    return [
        {
            "id":        t.id,
            "time":      t.time.isoformat(),
            "symbol_id": t.symbol_id,
            "price":     str(t.price),
            "volume":    str(t.volume),
            "side":      t.side,
            "dq_result": t.dq_result,
        }
        for t in ticks
    ]