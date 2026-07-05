from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, Query
from gunicorn.config import User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.helpers.helpers import latest_regime, primary_symbol, recent_ticks, get_symbol_by_name, now_iso
from app.models.all_models import RegimeState
from app.services.market_data_service import compute_technical_indicators, get_live_ticker


async def compute_regime_current(
    current_user,
    db,
    symbol: str | None  = None
):
    if symbol:
        sym = await get_symbol_by_name(db, symbol)
    else:
        sym = await primary_symbol(db)
    if not sym:
        return {"error": "no_market_data", "evaluated_at": now_iso()}

    regime = await latest_regime(db, sym.id)
    if not regime:
        return {"error": "no_regime_data", "symbol": sym.symbol, "evaluated_at": now_iso()}

    # Duration: count consecutive rows with same label
    hist_result = await db.execute(
        select(RegimeState)
        .where(RegimeState.symbol_id == sym.id)
        .order_by(RegimeState.time.desc())
        .limit(200)
    )
    all_regimes = hist_result.scalars().all()

    duration_bars = 1
    for r in all_regimes[1:]:
        if r.regime_label == regime.regime_label:
            duration_bars += 1
        else:
            break

    # size multiplier by regime
    mult_map = {"BULL": 1.0, "BEAR": 0.6, "RANGE": 0.8, "CRISIS": 0.3, "RECOVERY": 0.7}
    size_mult = mult_map.get(regime.regime_label, 0.8)

    # history: group consecutive same-label blocks
    history = []
    seen: dict = {}
    prev_label = None
    block_start = None
    for r in reversed(all_regimes):
        if r.regime_label != prev_label:
            if prev_label is not None and block_start:
                history.append({
                    "regime": prev_label,
                    "started_at": block_start.isoformat(),
                    "ended_at": r.time.isoformat(),
                    "duration_bars": seen.get(prev_label, 1),
                    "avg_return": round(mult_map.get(prev_label, 0.8) - 1, 4),
                })
            block_start = r.time
            seen[r.regime_label] = 1
        else:
            seen[r.regime_label] = seen.get(r.regime_label, 0) + 1
        prev_label = r.regime_label
    # add current open block
    history.append({
        "regime": regime.regime_label,
        "started_at": block_start.isoformat() if block_start else regime.time.isoformat(),
        "ended_at": None,
        "duration_bars": duration_bars,
        "avg_return": round(size_mult - 1, 4),
    })

    conf = float(regime.confidence)

    # Live market enrichment — ticker + technicals
    live_ticker = {}
    tech: dict = {}
    try:
        live_ticker = await get_live_ticker(sym.symbol)
        ticks_all = await recent_ticks(db, sym.id, 100)
        if len(ticks_all) >= 14:
            prices_all  = [float(t.price)  for t in ticks_all]
            volumes_all = [float(t.volume) for t in ticks_all]
            tech = await compute_technical_indicators(prices_all, volumes_all)
    except Exception:
        pass

    # Regime-based trading advisory
    advisories = {
        "BULL":     "Trend-following strategies preferred. Scale into longs on pullbacks. Size at 100%.",
        "BEAR":     "Short bias or cash. Reduce long exposure 40%. Use tighter stops.",
        "RANGE":    "Mean-reversion strategies. Sell highs, buy lows within range. Size at 80%.",
        "CRISIS":   "All new entries blocked. Close risky positions. Capital preservation mode.",
        "RECOVERY": "Cautious re-entry. Start rebuilding longs gradually. Size at 70%.",
    }

    return {
        "symbol":         sym.symbol,
        "asset_class":    sym.asset_class,
        "exchange":       sym.exchange,
        "regime":         regime.regime_label,
        "confidence":     round(conf * 100, 1),
        "confidence_low": round(max(0, conf - 0.1) * 100, 1),
        "confidence_high":round(min(1, conf + 0.1) * 100, 1),
        "size_multiplier":size_mult,
        "duration_bars":  duration_bars,
        "detected_at":    regime.time.isoformat(),
        "history":        history[-10:],
        "live_price":     live_ticker.get("last"),
        "change_pct_24h": live_ticker.get("change_pct_24h"),
        "volume_24h":     live_ticker.get("volume_24h"),
        "technicals": {
            "rsi_14":         tech.get("rsi_14"),
            "rsi_signal":     tech.get("rsi_signal"),
            "macd_cross":     tech.get("macd_cross"),
            "composite_bias": tech.get("composite_bias"),
            "bb_signal":      tech.get("bb_signal"),
            "atr_pct":        tech.get("atr_pct"),
        },
        "advisory":       advisories.get(regime.regime_label, "Monitor closely."),
        "regime_color":   {"BULL": "green", "BEAR": "red", "RANGE": "yellow",
                           "CRISIS": "red", "RECOVERY": "blue"}.get(regime.regime_label, "gray"),
    }


async def compute_regime_trend(
    db: AsyncSession,
    hours: int = 24,
    symbol: str | None = None,   # ← drop Query(None)
):
    if symbol:
        sym = await get_symbol_by_name(db, symbol)
    else:
        sym = await primary_symbol(db)
    if not sym:
        return []

    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    result = await db.execute(   # ← missing await
        select(RegimeState)
        .where(RegimeState.symbol_id == sym.id, RegimeState.time >= since)
        .order_by(RegimeState.time)
    )
    rows = result.scalars().all()

    output = []
    for r in rows:
        conf = float(r.confidence)
        conflict = round(max(0, 1.0 - abs(conf - 0.5) * 4) * 100, 1)
        output.append({
            "ts":             r.time.isoformat(),
            "confidence_pct": round(conf * 100, 1),
            "conflict_level": conflict,
        })

    return output