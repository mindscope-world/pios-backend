"""
Decision Intelligence, Regime, OFI, GMIG, Monte Carlo, Why-Not-Trade,
Adaptation Feed, Alpha Factory, Signal Conflict, Feature Store endpoints.

All data is derived from live DB records (market_ticks, regime_states,
positions, orders, strategies, fills, backtest_jobs).  No stubs.
"""
from __future__ import annotations

import asyncio
import logging
import json as _json
import math
import statistics
import uuid
from datetime import datetime, timezone, timedelta
from typing import AsyncGenerator

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user
from app.core.redis import get_intelligence_key
from app.db.session import get_db
from app.helpers.helpers import get_symbol_by_name, latest_regime, open_positions, primary_symbol, recent_ticks, safe_ms
from app.models.all_models import (
    MarketTick, Symbol, RegimeState, Position, Order,
    Strategy, BacktestJob, Fill, PnLSnapshot, Alert, User,
)
from app.services.intelligence.decision_service import compute_decision_current
from app.services.intelligence.montecarlo_service import compute_monte_carlo, compute_monte_carlo_auto
from app.services.intelligence.ofi_service import compute_ofi, compute_ofi_chart, compute_ofi_signal_auto
from app.services.intelligence.signal_conflict_service import compute_signal_conflict, compute_signal_conflict_auto
from app.services.intelligence.cross_market_service import compute_gmig_enhanced
from app.services.market_data_service import (
    get_live_ticker, get_ohlcv, get_orderbook, get_recent_trades,
    compute_technical_indicators, get_multi_asset_snapshot,
    get_market_breadth, get_funding_rates, live_price_stream,
    CORE_CRYPTO_SYMBOLS, CORE_FOREX_PAIRS, CORE_STOCK_TICKERS,
    DEFAULT_CRYPTO_EXCHANGE,
)

router = APIRouter(prefix="/intelligence", tags=["intelligence"])

# This was `from asyncio import log` (the asyncio.log *module*, an IDE
# auto-import accident) — every log.debug() call then raised AttributeError,
# and since several sit inside the SSE generators' exception handlers, a
# routine handled error (e.g. a 3s ticker timeout) killed the whole stream.
log = logging.getLogger(__name__)


def _normalize_symbol_key(symbol: str) -> str:
    """Matches app/workers/intelligence_worker.py's normalize_symbol() -- Redis
    keys are written with slashes stripped, e.g. "BTC/USDT" -> "BTCUSDT"."""
    return symbol.replace("/", "")


async def _resolve_symbol_key(db: AsyncSession, symbol: str | None) -> str | None:
    """
    Resolve a symbol query param (or the DB's primary symbol if none given) to the
    normalized form intelligence_worker.py uses as its Redis key suffix. Returns
    None if there's no symbol to resolve to (no param and no primary symbol yet).
    """
    if symbol:
        return _normalize_symbol_key(symbol)
    sym_db = await primary_symbol(db)
    return _normalize_symbol_key(sym_db.symbol) if sym_db else None


async def _get_worker_cached(key_prefix: str, db: AsyncSession, symbol: str | None) -> dict:
    """
    Shared fetch path for the intelligence_worker.py-populated Redis endpoints below.
    Returns the cached payload, or a clean {"error": ...} dict (never raises) if the
    symbol can't be resolved or the worker hasn't populated this key yet.
    """
    key = await _resolve_symbol_key(db, symbol)
    if not key:
        return {"error": "no_market_data"}
    data = await get_intelligence_key(key_prefix, key)
    return data if data is not None else {"error": "not_yet_computed", "symbol": key}


# ─────────────────────────────────────────────────────────────────────────────
# § 1  DECISION CONTEXT
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/decision/current")
async def descision_current(
    symbol: str | None = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Full quant-core decision pipeline, computed live (not worker-cached)."""
    return await compute_decision_current(current_user, db, symbol=symbol)

@router.get("/decision/feed")
async def decision_feed(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("decision_feed", db, symbol)

# ─────────────────────────────────────────────────────────────────────────────
# § 2  REGIME
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/regime/current")
async def regime_current(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # intelligence_worker.py stores compute_regime_current()'s output under the
    # "regime_history" key prefix (its own local variable is named regime_history).
    return await _get_worker_cached("regime_history", db, symbol)


@router.get("/regime/trend")
async def regime_trend(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("regime_trend", db, symbol)


# ─────────────────────────────────────────────────────────────────────────────
# § 3  ORDER FLOW INTELLIGENCE (OFI)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/ofi")
async def ofi_signal(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # intelligence_worker.py stores compute_ofi()'s output under "order_flow".
    return await _get_worker_cached("order_flow", db, symbol)

@router.get("/ofi/chart")
async def ofi_chart(
    symbol: str | None = Query(None, description="Optional -- auto-selects most active"),
    limit: int = Query(60, ge=10, le=500),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Not worker-cached -- computed live per request."""
    return await compute_ofi_chart(current_user, db, symbol=symbol, limit=limit)


# ─────────────────────────────────────────────────────────────────────────────
# § 4  CROSS-MARKET INTELLIGENCE (GMIG)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/gmig/snapshot")
async def gmig_snapshot(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("gmig_snapshot", db, symbol)


@router.get("/gmig/radar")
async def gmig_radar(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("gmig_radar", db, symbol)


# ─────────────────────────────────────────────────────────────────────────────
# § 5  MONTE CARLO
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/montecarlo")
async def monte_carlo(
    symbol: str | None = Query(None),
    simulations: int = Query(2000, ge=100, le=5000),
    horizon_days: int = Query(30, ge=5, le=90),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Not worker-cached -- computed live per request."""
    return await compute_monte_carlo(
        current_user, db, symbol=symbol, simulations=simulations, horizon_days=horizon_days,
    )


# ─────────────────────────────────────────────────────────────────────────────
# § 8  ADAPTATION FEED
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/adaptation/feed")
async def adaptation_feed(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("adaptation_feed", db, symbol)


@router.get("/adaptation/active")
async def adaptation_active(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("adaptation_active", db, symbol)


@router.get("/adaptation/drift")
async def adaptation_drift(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("adaptation_drift", db, symbol)


# ─────────────────────────────────────────────────────────────────────────────
# § 12  ALPHA FACTORY + DARWIN
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/alpha/state")
async def alpha_factory_state(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # intelligence_worker.py stores compute_alpha_factory_state()'s output under
    # the "alpha_state" key prefix (its own local variable is named alpha_state).
    return await _get_worker_cached("alpha_state", db, symbol)


@router.get("/alpha/darwin")
async def alpha_darwin(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("alpha_darwin", db, symbol)


# ─────────────────────────────────────────────────────────────────────────────
# § 13  SIGNAL CONFLICT + REJECTION STATS
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/signal-conflict")
async def signal_conflict(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Not worker-cached -- computed live per request (auto-detects primary symbol)."""
    return await compute_signal_conflict(current_user, db)


@router.get("/rejection-stats")
async def rejection_stats(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    since = datetime.now(timezone.utc) - timedelta(hours=24)

    # Count rejected orders by reason
    result = await db.execute(
        select(Order.reject_reason, func.count(Order.id).label("cnt"))
        .where(
            Order.user_id == current_user.id,
            Order.status == "REJECTED",
            Order.created_at >= since,
        )
        .group_by(Order.reject_reason)
        .order_by(desc("cnt"))
    )
    rows = result.all()

    stats = [
        {"reason": r.reject_reason or "Unknown", "count_24h": r.cnt}
        for r in rows
    ]

    # Add regime-based blocks from risk limits breaches (from alerts)
    alert_result = await db.execute(
        select(Alert.category, func.count(Alert.id).label("cnt"))
        .where(Alert.created_at >= since, Alert.severity.in_(["P1", "P2"]))
        .group_by(Alert.category)
        .order_by(desc("cnt"))
    )
    for row in alert_result.all():
        stats.append({"reason": f"Risk Alert: {row.category}", "count_24h": row.cnt})

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# § 14  FEATURE STORE
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/features")
async def features(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("features", db, symbol)


# ─────────────────────────────────────────────────────────────────────────────
# § NEW — COMMAND CENTER  /intelligence/command-center/current
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/command-center/current")
async def command(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("command_center", db, symbol)


# ─────────────────────────────────────────────────────────────────────────────
# § NEW — QUANT CORE GATES  /intelligence/quant-core/gates
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/quant-core/gates")
async def quant_core_gates(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns the 8-gate Quant Core pipeline state as a flat list
    for the Quant Core screen gate cards.
    Uses full quant engine (HMM + GARCH + OFI + LOF).
    """
    from app.services.quant_engine import build_quant_core_gates

    sym = await primary_symbol(db)
    if not sym:
        return []

    ticks    = await recent_ticks(db, sym.id, 200)
    regime   = await latest_regime(db, sym.id)
    positions = await open_positions(db, current_user.id)
    prices   = [float(t.price)  for t in ticks]
    volumes  = [float(t.volume) for t in ticks]
    sides    = [str(t.side or "") for t in ticks]
    exposure = min(1.0, sum(float(p.qty) * float(p.avg_cost) for p in positions) / 100_000)

    _, _, gates, _ = build_quant_core_gates(
        prices, volumes, sides,
        regime_override=regime.regime_label if regime else None,
        positions_exposure=exposure,
    )

    # Reshape for frontend gate card format
    return [
        {
            "id": g["id"],
            "label": g["name"],
            "status": g["status"],
            "value": g.get("confidence"),
            "type": "confidence" if g.get("confidence") is not None else "status",
            "passed": g["passed"],
            "detail": g["detail"],
            "latency_ms": g["latency_ms"],
        }
        for g in gates
    ]


# ─────────────────────────────────────────────────────────────────────────────
# § NEW — SCENARIOS / SIMULATIONS  /intelligence/scenarios/simulations
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/scenarios/simulations")
async def scenarios_simulations(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("scenarios", db, symbol)


# ─────────────────────────────────────────────────────────────────────────────
# § NEW — DECISION TRACES  /intelligence/traces
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/traces")
async def decision_traces(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("decision_traces", db, symbol)

# ─────────────────────────────────────────────────────────────────────────────
# § UPDATED — WHY NOT TRADE  /intelligence/why-not-trade  (no required params)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/ofi/auto")
async def ofi_auto_signal(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Not worker-cached -- auto-detects primary symbol, no params required."""
    return await compute_ofi_signal_auto(current_user, db)

# ─────────────────────────────────────────────────────────────────────────────
# § UPDATED — MONTE CARLO (no required symbol param)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/montecarlo/auto")
async def monte_carlo_auto(
    simulations: int = Query(2000, ge=100, le=5000),
    horizon_days: int = Query(30, ge=5, le=90),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Not worker-cached -- auto-detects primary symbol, no symbol param required."""
    return await compute_monte_carlo_auto(
        current_user, db, simulations=simulations, horizon_days=horizon_days,
    )


# ─────────────────────────────────────────────────────────────────────────────
# § UPDATED — SIGNAL CONFLICT (no required symbol param)
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/signal-conflict/auto")
async def signal_conflict_auto(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Not worker-cached -- auto-detects primary symbol, no params required."""
    return await compute_signal_conflict_auto(current_user, db)


# ═══════════════════════════════════════════════════════════════════════════════
# ── LIVE MARKET DATA ENDPOINTS ────────────────────────────────────────────────
# All endpoints below provide real-time data from globally-available exchanges.
# Exchanges used: KuCoin, OKX, Kraken, Bybit (no Binance - global coverage).
# ═══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# OHLCV — Candlestick chart data with technical indicator overlay
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# ORDER BOOK — L2 depth with liquidity analytics
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/market/orderbook")
async def market_orderbook(
    symbol: str | None  = Query(None),
    exchange: str | None= Query(None),
    depth: int          = Query(20, ge=5, le=50),
    current_user: User  = Depends(get_current_user),
    db: AsyncSession    = Depends(get_db),
):
    """
    Real-time L2 order book with bid/ask walls, imbalance, slippage estimates.
    Computes weighted mid-price and $50k market impact.
    """
    if not symbol:
        sym_db = await primary_symbol(db)
        symbol = sym_db.symbol if sym_db else "BTC/USDT"

    ob = await get_orderbook(symbol, exchange_id=exchange, depth=depth)
    return ob


# ─────────────────────────────────────────────────────────────────────────────
# LIVE TICKER — best bid/ask across venues
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/market/ticker")
async def market_ticker(
    symbol: str | None = Query(None),
    exchanges: str | None = Query(None, description="Comma-separated: kucoin,okx,kraken"),
    current_user: User  = Depends(get_current_user),
    db: AsyncSession    = Depends(get_db),
):
    """
    Live ticker with best bid/ask aggregated across multiple venues.
    Returns 24h stats, VWAP, price per exchange for venue comparison.
    """
    if not symbol:
        sym_db = await primary_symbol(db)
        symbol = sym_db.symbol if sym_db else "BTC/USDT"

    ex_list = [e.strip() for e in exchanges.split(",")] if exchanges else None
    ticker = await get_live_ticker(symbol, exchanges=ex_list)
    return ticker


# ─────────────────────────────────────────────────────────────────────────────
# RECENT TRADES — trade tape with aggressor analysis
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/market/trades")
async def market_trades(
    symbol: str | None  = Query(None),
    exchange: str | None= Query(None),
    limit: int          = Query(50, ge=10, le=200),
    current_user: User  = Depends(get_current_user),
    db: AsyncSession    = Depends(get_db),
):
    """
    Recent trade tape with buy/sell classification.
    Includes aggressor analysis and block trade detection.
    """
    if not symbol:
        sym_db = await primary_symbol(db)
        symbol = sym_db.symbol if sym_db else "BTC/USDT"

    trades = await get_recent_trades(symbol, exchange_id=exchange, limit=limit)

    # Block trade detection (>10× average size)
    if trades:
        avg_size = sum(t.get("amount", 0) for t in trades) / len(trades)
        block_threshold = avg_size * 10
        for t in trades:
            t["is_block_trade"] = t.get("amount", 0) >= block_threshold

        buy_vol  = sum(t.get("amount", 0) for t in trades if t.get("side") == "BUY")
        sell_vol = sum(t.get("amount", 0) for t in trades if t.get("side") == "SELL")
        imbalance = (buy_vol - sell_vol) / max(buy_vol + sell_vol, 1e-9)

        summary = {
            "total_trades":    len(trades),
            "buy_trades":      sum(1 for t in trades if t.get("side") == "BUY"),
            "sell_trades":     sum(1 for t in trades if t.get("side") == "SELL"),
            "buy_volume":      round(buy_vol, 6),
            "sell_volume":     round(sell_vol, 6),
            "imbalance":       round(imbalance, 4),
            "aggressor_bias":  "BUY_HEAVY" if imbalance > 0.3 else "SELL_HEAVY" if imbalance < -0.3 else "BALANCED",
            "block_trades":    sum(1 for t in trades if t.get("is_block_trade")),
            "avg_trade_size":  round(avg_size, 6),
        }
    else:
        summary = {}

    return {
        "symbol":  symbol,
        "trades":  trades,
        "summary": summary,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# TECHNICAL INDICATORS — standalone indicator endpoint
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/market/indicators")
async def market_indicators(
    symbol: str | None  = Query(None),
    timeframe: str      = Query("1h"),
    limit: int          = Query(200, ge=50, le=500),
    current_user: User  = Depends(get_current_user),
    db: AsyncSession    = Depends(get_db),
):
    """
    Comprehensive technical indicator suite for any symbol.
    RSI, MACD, Bollinger Bands, ATR, Stochastic, CCI, Williams %R,
    OBV, CMF, MFI, VWAP, Pivot Points, EMA stack, composite signal.
    Fetches live OHLCV from exchange and computes all indicators.
    """
    if not symbol:
        sym_db = await primary_symbol(db)
        symbol = sym_db.symbol if sym_db else "BTC/USDT"

    candles = await get_ohlcv(symbol, timeframe=timeframe, limit=limit)

    if not candles:
        # Fallback to DB ticks
        sym_db = await get_symbol_by_name(db, symbol) if symbol else await primary_symbol(db)
        if sym_db:
            ticks   = await recent_ticks(db, sym_db.id, limit)
            closes  = [float(t.price)  for t in ticks]
            volumes = [float(t.volume) for t in ticks]
            highs   = closes
            lows    = closes
        else:
            return {"error": "no_data"}
    else:
        closes  = [c["close"]  for c in candles]
        highs   = [c["high"]   for c in candles]
        lows    = [c["low"]    for c in candles]
        volumes = [c["volume"] for c in candles]

    indicators = compute_technical_indicators(closes, volumes, highs, lows)

    # Add trading advice
    bias = indicators.get("composite_bias", "NEUTRAL")
    rsi  = indicators.get("rsi_14", 50)
    adv  = {
        "STRONG_BULL": f"Strong bullish momentum. RSI={rsi:.0f}. Consider long entry on pullbacks.",
        "BULL":        f"Bullish bias. RSI={rsi:.0f}. Wait for MACD confirmation.",
        "NEUTRAL":     f"Mixed signals. RSI={rsi:.0f}. No clear directional edge.",
        "BEAR":        f"Bearish pressure. RSI={rsi:.0f}. Short bias — confirm with volume.",
        "STRONG_BEAR": f"Strong bearish momentum. RSI={rsi:.0f}. Avoid longs; tighten stops.",
    }.get(bias, "No advisory signal.")

    return {
        "symbol":     symbol,
        "timeframe":  timeframe,
        "data_points":len(closes),
        "indicators": indicators,
        "advisory":   adv,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# MULTI-ASSET SNAPSHOT — cross-asset dashboard
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/market/snapshot")
async def market_snapshot(
    crypto: str | None  = Query(None, description="Comma-separated: BTC/USDT,ETH/USDT"),
    forex: str | None   = Query(None, description="Comma-separated: EUR/USD,GBP/USD"),
    stocks: str | None  = Query(None, description="Comma-separated: AAPL,MSFT,SPY"),
    current_user: User  = Depends(get_current_user),
):
    """
    Cross-asset snapshot: crypto + forex + equities in one call.
    Returns price, 24h change, volume, direction for each asset.
    Ideal for the TopBar ticker strip and dashboard overview.
    """
    crypto_list  = [s.strip() for s in crypto.split(",")]  if crypto  else None
    forex_list   = [s.strip() for s in forex.split(",")]   if forex   else None
    stocks_list  = [s.strip() for s in stocks.split(",")]  if stocks  else None

    snapshot = await get_multi_asset_snapshot(crypto_list, forex_list, stocks_list)
    return snapshot


# ─────────────────────────────────────────────────────────────────────────────
# MARKET BREADTH — macro sentiment + fear/greed
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/market/breadth")
async def market_breadth_endpoint(
    current_user: User = Depends(get_current_user),
):
    """
    Macro market health: VIX level, S&P 500, BTC dominance,
    fear/greed proxy, yield curve. Refreshes every ~60s.
    """
    breadth = await get_market_breadth()
    return breadth


# ─────────────────────────────────────────────────────────────────────────────
# FUNDING RATES — perpetual futures sentiment
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/market/funding")
async def market_funding(
    symbols: str | None = Query(None, description="Comma-separated perp symbols"),
    current_user: User  = Depends(get_current_user),
):
    """
    Perpetual futures funding rates from Bybit/OKX.
    Positive = long bias (longs pay shorts), negative = short bias.
    Annualised rate helps spot crowded trades.
    """
    sym_list = [s.strip() for s in symbols.split(",")] if symbols else None
    rates    = await get_funding_rates(sym_list)
    return {
        "funding_rates": rates,
        "interpretation": "Positive rate = market is long-biased; longs pay shorts. High positive rates (>0.1%/8h) = crowded long.",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# ENHANCED OFI — now with live orderbook + recent trades
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/ofi/enhanced")
async def ofi_enhanced(
    symbol: str | None = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    compute_ofi() already *is* the enhanced version (DB tick history + live L2
    orderbook + recent trade tape) -- see ofi_service.compute_ofi's docstring.
    Computed live per request rather than worker-cached, for freshness.
    """
    return await compute_ofi(current_user, db, symbol=symbol)

# ─────────────────────────────────────────────────────────────────────────────
# ENHANCED GMIG — with live cross-market prices
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/gmig/enhanced")
async def gmig_enhanced(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Not worker-cached -- computed live per request."""
    return await compute_gmig_enhanced(current_user, db)


# ─────────────────────────────────────────────────────────────────────────────
# WHY-NOT-TRADE — enhanced with live market context
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/why-not-trade")
async def why_not_trade(
    symbol: str | None = Query(None),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await _get_worker_cached("why_not_trade", db, symbol)

# ─────────────────────────────────────────────────────────────────────────────
# WEBSOCKET — Real-time price stream
# ─────────────────────────────────────────────────────────────────────────────

@router.websocket("/ws/market")
async def websocket_market(
    websocket: WebSocket,
    token: str | None = Query(None),
):
    """
    WebSocket real-time market data stream.
    Connect: ws://host/api/v1/intelligence/ws/market?token=<jwt>

    Send JSON to subscribe/unsubscribe:
      {"action": "subscribe",   "symbols": ["BTC/USDT", "ETH/USDT"]}
      {"action": "unsubscribe", "symbols": ["BTC/USDT"]}
      {"action": "ping"}

    Receive JSON messages per tick:
      {type, symbol, price, bid, ask, change_pct, volume, exchange, ts}

    Also emits "analytics" messages every 30s with regime + OFI + technicals.
    """
    from app.core.deps import get_db
    from app.core.security import decode_token
    from jose import JWTError

    # Auth via query param JWT (EventSource/WS can't set headers easily)
    if not token:
        await websocket.close(code=4001, reason="Missing auth token")
        return
    try:
        payload   = decode_token(token)
        user_id   = payload.get("sub")
        if not user_id:
            raise ValueError("no sub")
    except (JWTError, ValueError):
        await websocket.close(code=4003, reason="Invalid token")
        return

    await websocket.accept()

    subscribed: set[str] = set(CORE_CRYPTO_SYMBOLS[:3])  # default symbols
    last_analytics = 0.0

    async def send_analytics():
        """Every 30s: push regime + OFI + technicals summary."""
        from app.db.session import AsyncSessionLocal
        try:
            async with AsyncSessionLocal() as db_sess:
                primary = await primary_symbol(db_sess)
                if not primary:
                    return
                ticks   = await recent_ticks(db_sess, primary.id, 100)
                regime  = await latest_regime(db_sess, primary.id)
                prices  = [float(t.price)  for t in ticks]
                volumes = [float(t.volume) for t in ticks]
                tech    = compute_technical_indicators(prices, volumes) if len(prices) >= 14 else {}
                await websocket.send_json({
                    "type":    "analytics",
                    "symbol":  primary.symbol,
                    "regime":  regime.regime_label if regime else "UNKNOWN",
                    "regime_conf": round(float(regime.confidence) * 100, 1) if regime else 0,
                    "rsi_14":  tech.get("rsi_14"),
                    "macd_cross": tech.get("macd_cross"),
                    "composite_bias": tech.get("composite_bias"),
                    "ts":      datetime.now(timezone.utc).isoformat(),
                })
        except Exception as e:
            log.debug(f"WS analytics error: {e}")

    try:
        while True:
            # Non-blocking check for client messages
            try:
                msg = await asyncio.wait_for(websocket.receive_json(), timeout=0.1)
                action = msg.get("action", "")
                if action == "subscribe":
                    subscribed.update(msg.get("symbols", []))
                    await websocket.send_json({"type": "subscribed", "symbols": list(subscribed)})
                elif action == "unsubscribe":
                    for s in msg.get("symbols", []):
                        subscribed.discard(s)
                    await websocket.send_json({"type": "unsubscribed", "symbols": list(subscribed)})
                elif action == "ping":
                    await websocket.send_json({"type": "pong", "ts": datetime.now(timezone.utc).isoformat()})
            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                break

            # Fetch and broadcast prices for all subscribed symbols
            for sym in list(subscribed):
                try:
                    ticker = await asyncio.wait_for(get_live_ticker(sym), timeout=2.0)
                    if ticker.get("last"):
                        await websocket.send_json({
                            "type":       "price",
                            "symbol":     sym,
                            "price":      ticker["last"],
                            "bid":        ticker.get("bid"),
                            "ask":        ticker.get("ask"),
                            "spread_pct": ticker.get("spread_pct"),
                            "change_pct": ticker.get("change_pct_24h"),
                            "volume":     ticker.get("volume_24h"),
                            "sources":    ticker.get("sources", []),
                            "ts":         datetime.now(timezone.utc).isoformat(),
                        })
                except Exception as e:
                    log.debug(f"WS price {sym}: {e}")

            # Analytics push every 30s
            now_ts = datetime.now(timezone.utc).timestamp()
            if now_ts - last_analytics >= 30:
                await send_analytics()
                last_analytics = now_ts

            await asyncio.sleep(2.0)  # 2s polling interval

    except WebSocketDisconnect:
        log.info(f"WS market client disconnected: {user_id}")
    except Exception as e:
        log.error(f"WS market error: {e}")
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# SSE STREAM — Real-time price + analytics for browser EventSource
# ─────────────────────────────────────────────────────────────────────────────

_STREAM_INTERVAL  = 3.0   # seconds between price updates
_HEARTBEAT_SECS   = 25.0  # SSE keepalive


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {_json.dumps(data, default=str)}\n\n"


async def _market_stream_generator(
    symbols: list[str],
    db: AsyncSession,
    current_user: "User",
) -> AsyncGenerator[str, None]:
    """
    SSE generator: price updates + analytics every N seconds.
    Symbol discovery: market_ticks JOIN symbols (no hardcoded list).
    """
    from app.services.quant_engine import detect_signal_conflicts

    last_heartbeat = 0.0
    last_analytics = 0.0
    last_why_not   = 0.0
    import time

    while True:
        now = time.monotonic()

        # Heartbeat
        if now - last_heartbeat >= _HEARTBEAT_SECS:
            yield _sse("heartbeat", {"ts": datetime.now(timezone.utc).isoformat()})
            last_heartbeat = now

        # Price updates for each symbol
        for sym_str in symbols:
            try:
                ticker = await asyncio.wait_for(get_live_ticker(sym_str), timeout=3.0)
                if ticker.get("last"):
                    yield _sse("price", {
                        "symbol":      sym_str,
                        "price":       ticker["last"],
                        "bid":         ticker.get("bid"),
                        "ask":         ticker.get("ask"),
                        "spread_pct":  ticker.get("spread_pct"),
                        "change_pct":  ticker.get("change_pct_24h"),
                        "volume_24h":  ticker.get("volume_24h"),
                        "vwap":        ticker.get("vwap"),
                        "high_24h":    ticker.get("high_24h"),
                        "low_24h":     ticker.get("low_24h"),
                        "sources":     ticker.get("sources", []),
                        "ts":          datetime.now(timezone.utc).isoformat(),
                    })
            except Exception as e:
                log.debug(f"SSE price {sym_str}: {e}")

        # Analytics every 30s: regime + technicals + OFI
        if now - last_analytics >= 30:
            try:
                primary = await primary_symbol(db)
                if primary:
                    ticks   = await recent_ticks(db, primary.id, 200)
                    regime  = await latest_regime(db, primary.id)
                    prices  = [float(t.price) for t in ticks]
                    volumes = [float(t.volume) for t in ticks]
                    tech    = compute_technical_indicators(prices, volumes) if len(prices) >= 14 else {}
                    conf    = detect_signal_conflicts(prices) if len(prices) >= 30 else {"level": "NONE"}
                    yield _sse("analytics", {
                        "symbol":          primary.symbol,
                        "regime":          regime.regime_label if regime else "UNKNOWN",
                        "regime_conf":     round(float(regime.confidence) * 100, 1) if regime else 0,
                        "rsi_14":          tech.get("rsi_14"),
                        "rsi_signal":      tech.get("rsi_signal"),
                        "macd_cross":      tech.get("macd_cross"),
                        "bb_signal":       tech.get("bb_signal"),
                        "composite_bias":  tech.get("composite_bias"),
                        "composite_signal":tech.get("composite_signal"),
                        "signal_conflict": conf["level"],
                        "ts":              datetime.now(timezone.utc).isoformat(),
                    })
                last_analytics = now
            except Exception as e:
                log.debug(f"SSE analytics: {e}")

        # Why-Not-Trade update every 60s
        if now - last_why_not >= 60:
            try:
                since = datetime.now(timezone.utc) - timedelta(hours=6)
                rows  = (await db.execute(
                    select(Symbol, func.count(MarketTick.id).label("cnt"))
                    .join(MarketTick, MarketTick.symbol_id == Symbol.id)
                    .where(Symbol.is_active.is_(True), MarketTick.time >= since)
                    .group_by(Symbol.id).order_by(desc("cnt")).limit(3)
                )).all()
                positions = await open_positions(db, current_user.id)
                for sym_row, _ in rows:
                    ticks  = await recent_ticks(db, sym_row.id, 50)
                    regime = await latest_regime(db, sym_row.id)
                    rl     = regime.regime_label if regime else "RANGE"
                    decision = "BLOCK" if rl == "CRISIS" else "WARN" if rl == "BEAR" else "ALLOW"
                    if decision != "ALLOW":
                        yield _sse("why_not_trade", {
                            "symbol":   sym_row.symbol,
                            "decision": decision,
                            "regime":   rl,
                            "title":    f"{sym_row.symbol}: {decision} — {rl} regime",
                            "ts":       datetime.now(timezone.utc).isoformat(),
                        })
                last_why_not = now
            except Exception as e:
                log.debug(f"SSE why-not-trade: {e}")

        await asyncio.sleep(_STREAM_INTERVAL)


@router.get(
    "/stream",
    response_class=StreamingResponse,
    summary="SSE real-time market stream",
)
async def market_stream(
    symbols: str | None = Query(None, description="Comma-separated: BTC/USDT,ETH/USDT,EUR/USD"),
    current_user: User  = Depends(get_current_user),
    db: AsyncSession    = Depends(get_db),
):
    """
    Server-Sent Events stream for real-time market data.
    Emits: price, analytics, why_not_trade, heartbeat events.
    Auto-discovers symbols from DB market_ticks if not specified.
    Reconnects automatically on the client side (EventSource behaviour).

    Connect:
      const es = new EventSource('/api/v1/intelligence/stream?symbols=BTC/USDT,ETH/USDT',
                                  { headers: { Authorization: 'Bearer ...' } })
      es.addEventListener('price',     e => console.log(JSON.parse(e.data)))
      es.addEventListener('analytics', e => console.log(JSON.parse(e.data)))
    """
    sym_list: list[str]
    if symbols:
        sym_list = [s.strip() for s in symbols.split(",")]
    else:
        # Auto-discover from DB
        since = datetime.now(timezone.utc) - timedelta(hours=6)
        rows  = (await db.execute(
            select(Symbol, func.count(MarketTick.id).label("cnt"))
            .join(MarketTick, MarketTick.symbol_id == Symbol.id)
            .where(Symbol.is_active.is_(True), MarketTick.time >= since)
            .group_by(Symbol.id).order_by(desc("cnt")).limit(5)
        )).all()
        sym_list = [row[0].symbol for row in rows] if rows else CORE_CRYPTO_SYMBOLS[:3]

    return StreamingResponse(
        _market_stream_generator(sym_list, db, current_user),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# NOTIFICATION STREAM — push alerts for constraint changes (SSE)
# ─────────────────────────────────────────────────────────────────────────────

_NOTIFICATION_POLL  = 15
_NOTIFICATION_BEAT  = 30


async def _notification_generator(
    db: AsyncSession,
    current_user: User,
) -> AsyncGenerator[str, None]:
    prev_decisions: dict[str, str] = {}
    prev_regimes:   dict[str, str] = {}
    last_hb = 0.0
    import time

    while True:
        now_ts = time.monotonic()
        if now_ts - last_hb >= _NOTIFICATION_BEAT:
            yield _sse("heartbeat", {"ts": datetime.now(timezone.utc).isoformat()})
            last_hb = now_ts

        since = datetime.now(timezone.utc) - timedelta(hours=6)
        try:
            rows = (await db.execute(
                select(Symbol, func.count(MarketTick.id).label("cnt"))
                .join(MarketTick, MarketTick.symbol_id == Symbol.id)
                .where(Symbol.is_active.is_(True), MarketTick.time >= since)
                .group_by(Symbol.id).order_by(desc("cnt")).limit(10)
            )).all()
        except Exception:
            await asyncio.sleep(_NOTIFICATION_POLL)
            continue

        positions = await open_positions(db, current_user.id)
        from app.services.quant_engine import compute_ofi_signals, detect_outlier_ticks

        for sym_row, tick_count in rows:
            try:
                ticks  = await recent_ticks(db, sym_row.id, 100)
                regime = await latest_regime(db, sym_row.id)
                prices = [float(t.price) for t in ticks]
                volumes= [float(t.volume) for t in ticks]
                sides  = [str(t.side or "") for t in ticks]

                # Determine decision
                rl = regime.regime_label if regime else "RANGE"
                if rl == "CRISIS":
                    decision = "BLOCK"
                elif rl == "BEAR":
                    decision = "WAIT"
                else:
                    decision = "ALLOW"
                    if prices and len(prices) >= 10:
                        tick_dicts = [{"price": p, "volume": v, "side": s} for p, v, s in zip(prices, volumes, sides)]
                        ofi = compute_ofi_signals(tick_dicts)
                        if ofi["stop_hunt_probability"] > 0.65:
                            decision = "WAIT"

                sym_str   = sym_row.symbol
                prev_dec  = prev_decisions.get(sym_str)
                if prev_dec is not None and prev_dec != decision:
                    if decision == "BLOCK":
                        yield _sse("why_not_trade", {
                            "type": "BLOCK", "symbol": sym_str,
                            "decision": decision, "regime": rl,
                            "title": f"⛔ {sym_str} — Blocked",
                            "body":  f"Regime shift to {rl}. No new entries.",
                            "ts":    datetime.now(timezone.utc).isoformat(),
                        })
                    elif decision == "ALLOW" and prev_dec in ("BLOCK", "WAIT"):
                        yield _sse("why_not_trade", {
                            "type": "CLEAR", "symbol": sym_str,
                            "decision": decision, "regime": rl,
                            "title": f"✅ {sym_str} — Constraints cleared",
                            "body":  "Conditions normalised. Full sizing restored.",
                            "ts":    datetime.now(timezone.utc).isoformat(),
                        })
                prev_decisions[sym_str] = decision

                # Regime change
                prev_rl = prev_regimes.get(str(sym_row.id))
                if prev_rl and prev_rl != rl:
                    yield _sse("regime_change", {
                        "symbol": sym_str, "from_regime": prev_rl, "to_regime": rl,
                        "confidence": round(float(regime.confidence) * 100, 1) if regime else 0,
                        "title": f"📊 {sym_str}: {prev_rl} → {rl}",
                        "ts":    datetime.now(timezone.utc).isoformat(),
                    })
                if regime:
                    prev_regimes[str(sym_row.id)] = rl

                # Feed staleness
                if ticks:
                    age_ms = safe_ms(ticks[-1].time)
                    if age_ms > 30_000:
                        yield _sse("feed_stale", {
                            "symbol": sym_str, "age_ms": round(age_ms),
                            "title": f"📡 {sym_str} feed stale ({round(age_ms/1000)}s)",
                            "ts":    datetime.now(timezone.utc).isoformat(),
                        })
            except Exception:
                continue

        # Kill switch check
        from app.models.all_models import KillSwitchEvent
        ks = (await db.execute(
            select(KillSwitchEvent)
            .where(KillSwitchEvent.created_at >= datetime.now(timezone.utc) - timedelta(minutes=5))
            .order_by(KillSwitchEvent.created_at.desc()).limit(1)
        )).scalar_one_or_none()
        if ks:
            yield _sse("kill_switch", {
                "severity": "CRITICAL", "title": "🔴 Kill Switch Armed",
                "body": ks.reason[:200], "ts": datetime.now(timezone.utc).isoformat(),
            })

        await asyncio.sleep(_NOTIFICATION_POLL)


@router.get("/notifications/stream", response_class=StreamingResponse)
async def notification_stream(
    current_user: User = Depends(get_current_user),
    db: AsyncSession   = Depends(get_db),
):
    """SSE push notification stream for constraint changes, regime shifts, feed outages."""
    return StreamingResponse(
        _notification_generator(db, current_user),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@router.get("/notifications/latest")
async def notification_latest(
    limit: int = Query(10, ge=1, le=50),
    current_user: User = Depends(get_current_user),
    db: AsyncSession   = Depends(get_db),
):
    """Polling snapshot of current constraint severity across all active symbols."""
    since     = datetime.now(timezone.utc) - timedelta(hours=6)
    positions = await open_positions(db, current_user.id)
    rows      = (await db.execute(
        select(Symbol, func.count(MarketTick.id).label("cnt"))
        .join(MarketTick, MarketTick.symbol_id == Symbol.id)
        .where(Symbol.is_active.is_(True), MarketTick.time >= since)
        .group_by(Symbol.id).order_by(desc("cnt")).limit(min(limit * 2, 20))
    )).all()

    notifications = []
    for sym_row, count in rows:
        try:
            ticks  = await recent_ticks(db, sym_row.id, 50)
            regime = await latest_regime(db, sym_row.id)
            rl     = regime.regime_label if regime else "RANGE"
            conf   = round(float(regime.confidence) * 100, 1) if regime else 0.0
            age_ms = safe_ms(ticks[-1].time) if ticks else 9999999
            has_block = rl in ("CRISIS",) or age_ms > 30_000
            has_warn  = rl in ("BEAR", "RANGE") or age_ms > 2000
            notifications.append({
                "symbol":       sym_row.symbol,
                "symbol_id":    sym_row.id,
                "asset_class":  sym_row.asset_class,
                "exchange":     sym_row.exchange,
                "regime":       rl,
                "regime_conf":  conf,
                "decision":     "BLOCK" if has_block else "WARN" if has_warn else "ALLOW",
                "severity":     "BLOCK" if has_block else "WARN" if has_warn else "OK",
                "feed_age_ms":  round(age_ms, 0),
                "tick_count":   count,
                "evaluated_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception:
            continue

    notifications.sort(key=lambda x: (0 if x["severity"] == "BLOCK" else 1 if x["severity"] == "WARN" else 2, -x["tick_count"]))
    return {
        "notifications": notifications[:limit],
        "has_blocks": any(n["severity"] == "BLOCK" for n in notifications),
        "has_warns":  any(n["severity"] == "WARN"  for n in notifications),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }