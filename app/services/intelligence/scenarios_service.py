from sqlalchemy.ext.asyncio import AsyncSession
from app.helpers.helpers import get_primary_with_ticks, get_symbol_with_ticks, now_iso, safe_float
from app.models.all_models import User
from app.services.quant_engine import estimate_volatility_garch, run_monte_carlo

async def compute_scenarios(current_user: User, db: AsyncSession, symbol: str | None = None) -> dict:
    """
    Bull / base / bear scenario cards + stress-test table.
    Powered by Monte Carlo (numpy log-normal paths) with GARCH vol.
    Uses the explicit `symbol` when given (the worker passes each symbol it
    caches under), auto-detecting the primary symbol otherwise.

    Returns a fully-populated dict; never raises.
    """
    try:
        sym, ticks = await get_symbol_with_ticks(db, symbol, 500)
 
        if sym is None:
            return {
                "error": "no_primary_symbol",
                "cards": [],
                "stress_tests": [],
                "evaluated_at": now_iso(),
            }
 
        prices = [safe_float(t.price) for t in ticks]
 
        # Graceful degradation: fewer than 20 ticks → synthetic flat prices
        if len(prices) < 20:
            return {
                "error": "insufficient_price_history",
                "symbol": sym.symbol,
                "tick_count": len(prices),
                "cards": [],
                "stress_tests": [],
                "evaluated_at": now_iso(),
            }
 
        vol_data = estimate_volatility_garch(prices)
        daily_vol = safe_float(vol_data.get("daily_vol"), 1.0)
 
        mc = run_monte_carlo(
            prices,
            n_sims=10_000,
            horizon_days=30,
            vol_override=daily_vol / 100,
        )
 
        cards = [
            {
                "label": c["label"],
                "return_val": f"{'+' if c['return_pct'] >= 0 else ''}{c['return_pct']:.2f}%",
                "probability": c["probability_pct"],
                "description": c["description"],
                "type": c["label"].lower(),
            }
            for c in mc.get("cases", [])
        ]
 
        stress = [
            {
                "scenario": s["name"],
                "trigger": s["trigger"],
                "pnl": s["expected_pnl"],
                "drawdown": s["max_dd_pct"],
                "status": "KILL_SWITCH" if s["kill_switch_fires"] else "MANAGED",
            }
            for s in mc.get("stress_tests", [])
        ]
 
        return {
            "symbol": sym.symbol,
            "cards": cards,
            "stress_tests": stress,
            "p5": mc.get("p5_return_pct"),
            "p50": mc.get("p50_return_pct"),
            "p95": mc.get("p95_return_pct"),
            "histogram": mc.get("histogram", []),
            "sim_count": mc.get("sim_count", 2000),
            "vol_engine": vol_data.get("engine", "unknown"),
            "run_at": mc.get("run_at", now_iso()),
            "evaluated_at": now_iso(),
        }
 
    except Exception as exc:  # noqa: BLE001
        return {
            "error": str(exc),
            "cards": [],
            "stress_tests": [],
            "evaluated_at": now_iso(),
        }