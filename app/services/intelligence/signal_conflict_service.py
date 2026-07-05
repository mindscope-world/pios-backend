from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from app.helpers.helpers import latest_regime, primary_symbol, recent_ticks, now_iso, get_primary_with_ticks, safe_float
from app.models.all_models import User
from app.services.quant_engine import detect_signal_conflicts

async def compute_signal_conflict(current_user: User, db: AsyncSession) -> dict:
    """
    Multi-timeframe momentum + regime conflict detection for the primary symbol.
    Auto-detects primary symbol — no symbol param required.
 
    Returns conflict level, penalty, and a list of conflicting signal pairs.
    Never raises.
    """
    _EMPTY = {
        "level": "NONE",
        "confidence_penalty_pct": 0,
        "conflicting_signals": [],
        "evaluated_at": now_iso(),
    }
 
    try:
        sym, ticks = await get_primary_with_ticks(db, 100)
 
        if sym is None or not ticks:
            return {**_EMPTY, "error": "no_data"}
 
        regime = await latest_regime(db, sym.id)
        prices = [safe_float(t.price) for t in ticks]
 
        if len(prices) < 6:
            return {**_EMPTY, "symbol": sym.symbol, "error": "insufficient_ticks"}
 
        def _momentum(window: int) -> float:
            if len(prices) < window + 1:
                return 0.0
            base = prices[-window - 1]
            return 0.0 if base == 0 else (prices[-1] - base) / base * 100
 
        m5  = _momentum(5)
        m20 = _momentum(20)
        m60 = _momentum(60)
 
        conflicts: list[dict] = []
 
        # Short vs medium momentum
        if len(prices) >= 21 and (m5 > 0) != (m20 > 0):
            conflicts.append({
                "signal_a": "Short-term momentum (5)",
                "signal_b": "Medium-term momentum (20)",
                "detail": (
                    f"Short {m5:+.2f}% vs Medium {m20:+.2f}% — directional conflict"
                ),
            })
 
        # Medium vs long momentum
        if len(prices) >= 61 and (m20 > 0) != (m60 > 0):
            conflicts.append({
                "signal_a": "Medium-term momentum (20)",
                "signal_b": "Long-term momentum (60)",
                "detail": (
                    f"Medium {m20:+.2f}% vs Long {m60:+.2f}% — trend conflict"
                ),
            })
 
        # Regime vs short-term momentum
        regime_label = regime.regime_label if regime else "RANGE"
        regime_bullish = regime_label in ("BULL", "RECOVERY")
        price_rising   = m5 > 0
        if regime_bullish != price_rising:
            conflicts.append({
                "signal_a": f"Regime ({regime_label})",
                "signal_b": "Short-term momentum (5)",
                "detail": (
                    f"Regime implies {'LONG' if regime_bullish else 'SHORT'} "
                    f"but price is trending {'up' if price_rising else 'down'}"
                ),
            })
 
        # Also run quant-engine's own conflict detector for a richer signal set
        try:
            engine_conflicts = detect_signal_conflicts(prices)
            # Merge engine conflicts that aren't duplicates
            existing_pairs = {
                (c["signal_a"], c["signal_b"]) for c in conflicts
            }
            for ec in engine_conflicts.get("conflicting_signals", []):
                pair = (ec.get("signal_a", ""), ec.get("signal_b", ""))
                if pair not in existing_pairs:
                    conflicts.append(ec)
                    existing_pairs.add(pair)
        except Exception:  # noqa: BLE001
            pass  # engine detector is additive; failure is non-fatal
 
        n = len(conflicts)
        if n == 0:
            level, penalty = "NONE", 0
        elif n == 1:
            level, penalty = "LOW", 5
        elif n == 2:
            level, penalty = "MEDIUM", 15
        else:
            level, penalty = "HIGH", 30
 
        return {
            "symbol": sym.symbol,
            "level": level,
            "confidence_penalty_pct": penalty,
            "conflicting_signals": conflicts,
            "momentum": {"m5": round(m5, 4), "m20": round(m20, 4), "m60": round(m60, 4)},
            "regime": regime_label,
            "evaluated_at": now_iso(),
        }
 
    except Exception as exc:  # noqa: BLE001
        return {**_EMPTY, "error": str(exc)}
    

async def compute_signal_conflict_auto(
    current_user: User,
    db: AsyncSession,
):
    """Signal conflict detection for primary symbol — no params required."""
    sym   = await primary_symbol(db)
    ticks = await recent_ticks(db, sym.id, 100) if sym else []
    prices = [float(t.price) for t in ticks]
    return detect_signal_conflicts(prices)