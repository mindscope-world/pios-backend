from fastapi import Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.helpers.helpers import open_positions, primary_symbol, recent_ticks, get_symbol_by_name
from app.models.all_models import User
from app.services.quant_engine import estimate_volatility_garch, run_monte_carlo


async def compute_monte_carlo(current_user, db,  symbol: str | None = None,  simulations: int = Query(10_000, ge=100, le=20_000),
    horizon_days: int = Query(30, ge=5, le=90)):
    sym = await get_symbol_by_name(db, symbol) if symbol else await primary_symbol(db)
    if not sym:
        return {"error": "no_primary_symbol", "sim_count": simulations, "horizon_days": horizon_days}

    ticks = await recent_ticks(db, sym.id, 500)

    if len(ticks) < 20:
        return {
            "error": "insufficient_tick_data",
            "symbol": sym.symbol,
            "tick_count": len(ticks),
            "sim_count": simulations,
            "horizon_days": horizon_days,
        }

    prices = [float(t.price) for t in ticks]

    # Delegate to the real engine (quant_engine.run_monte_carlo) instead of
    # re-deriving log-normal paths + stress tests inline here -- this used to
    # be a second, drifted copy of that logic whose "stress tests" were
    # hardcoded percentages of total_exposure (-20%/-10%/-7%) rather than
    # anything the simulation itself produced. Dollar exposure now only
    # scales the real simulated percentile outputs.
    positions = await open_positions(db, current_user.id)
    total_exposure = sum(float(p.qty) * float(p.avg_cost) for p in positions)

    mc = run_monte_carlo(prices, n_sims=simulations, horizon_days=horizon_days)
    if mc.get("error"):
        return {**mc, "sim_count": simulations, "horizon_days": horizon_days}

    stress_tests = [
        {
            "name": s["name"],
            "trigger": s["trigger"],
            "expected_pnl": round(total_exposure * s["expected_pnl"] / 100, 2) if total_exposure else s["expected_pnl"],
            "max_dd_pct": s["max_dd_pct"],
            "kill_switch_fires": s["kill_switch_fires"],
        }
        for s in mc.get("stress_tests", [])
    ]

    return {
        "sim_count": mc["sim_count"],
        "horizon_days": mc["horizon_days"],
        "p5_return_pct": mc["p5_return_pct"],
        "p50_return_pct": mc["p50_return_pct"],
        "p95_return_pct": mc["p95_return_pct"],
        "histogram": mc["histogram"],
        "cases": mc["cases"],
        "stress_tests": stress_tests,
        "run_at": mc["run_at"],
    }

async def compute_monte_carlo_auto(
    current_user: User,
    db: AsyncSession,
    simulations: int = Query(10_000, ge=100, le=20_000),
    horizon_days: int = Query(30, ge=5, le=90),
):
    """Monte Carlo with auto-detected primary symbol."""
    sym   = await primary_symbol(db)
    if not sym:
        return {"error": "no_primary_symbol", "sim_count": simulations, "horizon_days": horizon_days}

    ticks = await recent_ticks(db, sym.id, 500)
    prices = [float(t.price) for t in ticks]
    if len(prices) < 20:
        return {"error": "insufficient_price_data", "symbol": sym.symbol, "tick_count": len(prices)}

    vol   = estimate_volatility_garch(prices)
    mc    = run_monte_carlo(prices, n_sims=simulations, horizon_days=horizon_days,
                             vol_override=vol["daily_vol"] / 100)
    mc["symbol"] = sym.symbol if sym else "UNKNOWN"
    mc["vol_engine"] = vol.get("engine")
    return mc