import math
import statistics

from fastapi import HTTPException, Query
from datetime import datetime, timezone, timedelta
import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession

from app.helpers.helpers import open_positions, primary_symbol, recent_ticks, get_symbol_by_name
from app.models.all_models import User
from app.services.quant_engine import estimate_volatility_garch, run_monte_carlo


async def compute_monte_carlo(current_user, db,  symbol: str | None = None,  simulations: int = Query(2000, ge=100, le=5000),
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
    log_returns = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices)) if prices[i-1] > 0]

    mu    = statistics.mean(log_returns)
    sigma = statistics.stdev(log_returns) if len(log_returns) > 1 else 0.001

    # Run MC paths (vectorised with numpy for speed)
    rng = np.random.default_rng(seed=42)
    shocks = rng.normal(mu, sigma, (simulations, horizon_days))
    paths  = np.exp(np.cumsum(shocks, axis=1))
    final_returns = (paths[:, -1] - 1) * 100  # percent

    p5  = round(float(np.percentile(final_returns, 5)), 2)
    p50 = round(float(np.percentile(final_returns, 50)), 2)
    p95 = round(float(np.percentile(final_returns, 95)), 2)

    # Histogram (20 buckets)
    counts, edges = np.histogram(final_returns, bins=20)
    histogram = [
        {"return_pct": round(float((edges[i] + edges[i+1]) / 2), 2), "count": int(counts[i])}
        for i in range(len(counts))
    ]

    # Scenario probabilities
    bull_prob = round(float((final_returns > p50 + sigma * 100).mean() * 100), 1)
    bear_prob = round(float((final_returns < 0).mean() * 100), 1)
    base_prob = round(100 - bull_prob - bear_prob, 1)

    cases = [
        {"label": "BULL", "return_pct": p95, "probability_pct": bull_prob, "max_dd": round(sigma * 100 * -0.3, 2), "description": "Momentum continuation, strong regime"},
        {"label": "BASE", "return_pct": p50, "probability_pct": max(0, base_prob), "max_dd": round(sigma * 100 * -0.8, 2), "description": "Mean-reversion to historical drift"},
        {"label": "BEAR", "return_pct": p5,  "probability_pct": bear_prob, "max_dd": round(p5 * 1.5, 2), "description": "Risk-off sell pressure, macro shock"},
    ]

    # Stress tests using extreme quantiles
    crash_return = float(np.percentile(final_returns, 1))
    positions = await open_positions(db, current_user.id)
    total_exposure = sum(float(p.qty) * float(p.avg_cost) for p in positions)

    stress_tests = [
        {
            "name": "Flash Crash −20%",
            "trigger": "Sudden liquidity withdrawal",
            "expected_pnl": round(total_exposure * -0.20, 2),
            "max_dd_pct": round(crash_return * 1.2, 2),
            "kill_switch_fires": crash_return < -15,
        },
        {
            "name": "Macro Shock −10%",
            "trigger": "Fed rate surprise / geopolitical event",
            "expected_pnl": round(total_exposure * -0.10, 2),
            "max_dd_pct": round(crash_return * 0.6, 2),
            "kill_switch_fires": crash_return < -25,
        },
        {
            "name": "Correlation Spike",
            "trigger": "All assets move together",
            "expected_pnl": round(total_exposure * -0.07, 2),
            "max_dd_pct": round(abs(p5) * 0.4, 2),
            "kill_switch_fires": False,
        },
    ]

    return {
        "sim_count": simulations,
        "horizon_days": horizon_days,
        "p5_return_pct": p5,
        "p50_return_pct": p50,
        "p95_return_pct": p95,
        "histogram": histogram,
        "cases": cases,
        "stress_tests": stress_tests,
        "run_at": datetime.now(timezone.utc).isoformat(),
    }

async def compute_monte_carlo_auto(
    current_user: User,
    db: AsyncSession,
    simulations: int = Query(2000, ge=100, le=5000),
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