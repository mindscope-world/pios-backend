import asyncio
from datetime import datetime, timezone

from fastapi import HTTPException
import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.helpers.helpers import latest_regime, open_positions, primary_symbol, recent_ticks, get_primary_with_ticks, get_symbol_with_ticks, safe_float, now_iso, safe_ms, is_feed_stale
from app.core.deps import get_current_user
from app.db.session import get_db
from app.models.all_models import PnLSnapshot, Strategy
from app.services.intelligence.behavior_service import compute_behavior_session
from app.services.market_data_service import get_live_ticker, get_orderbook, get_recent_trades
from app.services.quant_engine import build_quant_core_gates, estimate_volatility_garch, compute_technical_indicators, detect_signal_conflicts


# async def compute_command_center_current(current_user, db):
#     """
#     Aggregate summary for Command Center dashboard.
#     Runs full 8-gate quant pipeline and packages all KPIs in one call.
#     """
    
#     sym = await primary_symbol(db)
#     if not sym:
#         raise HTTPException(status_code=503, detail="No market data available")

#     ticks    = await recent_ticks(db, sym.id, 200)
#     regime   = await latest_regime(db, sym.id)
#     positions = await open_positions(db, current_user.id)

#     prices  = [float(t.price)  for t in ticks] if ticks else []
#     volumes = [float(t.volume) for t in ticks] if ticks else []
#     sides   = [str(t.side or "") for t in ticks] if ticks else []

#     total_exposure = sum(float(p.qty) * float(p.avg_cost) for p in positions)
#     snap = (await db.execute(
#         select(PnLSnapshot)
#         .where(PnLSnapshot.user_id == current_user.id)
#         .order_by(PnLSnapshot.snapshot_at.desc())
#         .limit(1)
#     )).scalar_one_or_none()
#     total_equity = float(snap.total_equity) if snap else 100_000.0
#     exposure_pct = min(1.0, total_exposure / max(total_equity, 1))

#     decision, confidence, gates, size_info = build_quant_core_gates(
#         prices, volumes, sides,
#         regime_override=regime.regime_label if regime else None,
#         positions_exposure=exposure_pct,
#     )

#     regime_data = {
#         "label": regime.regime_label if regime else "RANGE",
#         "confidence": round(float(regime.confidence) * 100, 1) if regime else 50.0,
#     }

#     return {
#         "decision": decision,
#         "confidence": confidence,
#         "symbol": sym.symbol,
#         "regime": regime_data,
#         "gates": gates,
#         "size_info": size_info,
#         "portfolio": {
#             "total_equity": round(total_equity, 2),
#             "open_positions": len(positions),
#             "exposure_pct": round(exposure_pct * 100, 2),
#         },
#         "evaluated_at": datetime.now(timezone.utc).isoformat(),
#     }
async def compute_command_center_current(current_user, db, symbol: str | None = None) -> dict:
    """
    Full Quant Core decision pipeline for one symbol (explicit `symbol`, or
    the primary symbol when None). Worker-friendly: no FastAPI deps.
    Never raises.

    The worker calls this once per symbol and caches under that symbol's
    key — before `symbol` existed it always computed the primary symbol,
    so command_center:EURUSD contained BTC/USDT data.
    """
    try:
        sym, ticks = await get_symbol_with_ticks(db, symbol, 200)
        if sym is None:
            return {
                "error": "no_primary_symbol",
                "decision": "WAIT",
                "evaluated_at": now_iso(),
            }
 
        regime    = await latest_regime(db, sym.id)
        positions = await open_positions(db, current_user.id)
 
        prices  = [safe_float(t.price)  for t in ticks]
        volumes = [safe_float(t.volume) for t in ticks]
        sides   = [str(t.side or "")     for t in ticks]
 
        snap = (
            await db.execute(
                select(PnLSnapshot)
                .where(PnLSnapshot.user_id == current_user.id)
                .order_by(PnLSnapshot.snapshot_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
 
        total_equity   = safe_float(snap.total_equity if snap else None, 100_000.0)
        total_exposure = sum(
            safe_float(p.qty) * safe_float(p.avg_cost) for p in positions
        )
        exposure_pct = min(1.0, total_exposure / max(total_equity, 1))
 
        # Parallel live-market fetch — all failures are non-fatal
        live_ticker, live_ob, live_trades = {}, {}, []
        try:
            results = await asyncio.gather(
                get_live_ticker(sym.symbol),
                get_orderbook(sym.symbol, depth=10),
                get_recent_trades(sym.symbol, limit=20),
                return_exceptions=True,
            )
            live_ticker = results[0] if isinstance(results[0], dict) else {}
            live_ob     = results[1] if isinstance(results[1], dict) else {}
            live_trades = results[2] if isinstance(results[2], list) else []
        except Exception:  # noqa: BLE001
            pass
 
        # Technical indicators (need ≥14 prices)
        tech = (
            compute_technical_indicators(prices, volumes)
            if len(prices) >= 14
            else {}
        )
 
        # Quant Core 8-gate pipeline
        decision, confidence, gates, size_info = build_quant_core_gates(
            prices,
            volumes,
            sides,
            regime_override=regime.regime_label if regime else None,
            positions_exposure=exposure_pct,
            feed_stale=is_feed_stale(ticks),
        )
 
        # Signal conflicts
        sig_conflict = (
            detect_signal_conflicts(prices)
            if len(prices) >= 30
            else {"level": "NONE", "conflicting_signals": []}
        )
 
        # GARCH + scenario percentiles
        if len(prices) >= 10:
            rets     = np.diff(np.log(np.array(prices) + 1e-10))
            p50      = round(float(np.percentile(rets, 50)) * 100, 3)
            bear_ret = round(float(np.percentile(rets,  5)) * 100, 3)
            bull_ret = round(float(np.percentile(rets, 95)) * 100, 3)
            vol_data = estimate_volatility_garch(prices)
        else:
            p50 = bear_ret = bull_ret = 0.0
            vol_data = {"annualised_vol": 0, "daily_vol": 0, "engine": "N/A"}
 
        regime_label = regime.regime_label if regime else "RANGE"
        regime_conf  = safe_float(regime.confidence if regime else None, 0.5)
 
        strat = None
        try:
            strat = (
                await db.execute(
                    select(Strategy)
                    .where(Strategy.created_by == current_user.id)
                    .order_by(Strategy.updated_at.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
        except Exception:  # noqa: BLE001
            pass

        # Real behavior score (override rate, frequency vs baseline, deviation
        # from AI) instead of a fixed constant -- see behavior_service.py.
        # None (rather than a fake number) if it can't be computed.
        behavior_score = None
        try:
            behavior = await compute_behavior_session(current_user, db)
            behavior_score = behavior["score"]
        except Exception:  # noqa: BLE001
            pass

        # Live trade-flow analysis
        trade_flow: dict = {}
        if live_trades:
            buy_vol_live  = sum(t.get("amount", 0) for t in live_trades if t.get("side") == "BUY")
            sell_vol_live = sum(t.get("amount", 0) for t in live_trades if t.get("side") == "SELL")
            buy_count     = sum(1 for t in live_trades if t.get("side") == "BUY")
            sell_count    = sum(1 for t in live_trades if t.get("side") == "SELL")
            last5_prices  = [t["price"] for t in live_trades[-5:] if t.get("price")]
            denom = max(buy_vol_live + sell_vol_live, 1e-9)
            trade_flow = {
                "buy_count":       buy_count,
                "sell_count":      sell_count,
                "buy_volume":      round(buy_vol_live,  6),
                "sell_volume":     round(sell_vol_live, 6),
                "imbalance_ratio": round((buy_vol_live - sell_vol_live) / denom, 4),
                "aggressor_bias": (
                    "BUY_HEAVY"  if buy_count  > sell_count * 1.3 else
                    "SELL_HEAVY" if sell_count > buy_count  * 1.3 else
                    "BALANCED"
                ),
                "price_momentum": (
                    round(
                        (last5_prices[-1] - last5_prices[0]) / last5_prices[0] * 100, 4
                    )
                    if len(last5_prices) >= 2
                    else 0
                ),
            }
 
        _MULT_MAP = {
            "BULL": 1.0, "BEAR": 0.6, "RANGE": 0.8,
            "CRISIS": 0.3, "RECOVERY": 0.7,
        }
 
        return {
            "symbol":          sym.symbol,
            "asset_class":     sym.asset_class,
            "exchange":        sym.exchange,
            "strategy_id":     str(strat.id) if strat else "auto",
            "strategy_name":   strat.name    if strat else "System Auto",
            "decision":        decision,
            "final_size_lot":  size_info["final_size_lot"],
            "base_size_lot":   size_info["base_size_lot"],
            "confidence":      confidence,
            "confidence_low":  round(max(0,   confidence - 12), 1),
            "confidence_high": round(min(100, confidence + 12), 1),
            "signal_conflict": sig_conflict["level"],
            "conflict_detail": sig_conflict.get("conflicting_signals", []),
            "execution_path":  size_info["execution_path"],
            "live_market": {
                "price":          live_ticker.get("last"),
                "bid":            live_ticker.get("bid"),
                "ask":            live_ticker.get("ask"),
                "spread_pct":     live_ticker.get("spread_pct"),
                "change_pct_24h": live_ticker.get("change_pct_24h"),
                "volume_24h":     live_ticker.get("volume_24h"),
                "vwap":           live_ticker.get("vwap"),
                "high_24h":       live_ticker.get("high_24h"),
                "low_24h":        live_ticker.get("low_24h"),
                "sources":        live_ticker.get("sources", []),
            },
            "orderbook": {
                "bid_depth_usd":     live_ob.get("bid_depth_usd"),
                "ask_depth_usd":     live_ob.get("ask_depth_usd"),
                "imbalance":         live_ob.get("imbalance"),
                "spread_bps":        live_ob.get("spread_bps"),
                "liquidity_score":   live_ob.get("liquidity_score"),
                "slippage_buy_pct":  live_ob.get("slippage_buy_pct"),
                "slippage_sell_pct": live_ob.get("slippage_sell_pct"),
                "weighted_mid":      live_ob.get("weighted_mid"),
            },
            "trade_flow": trade_flow,
            "technicals": {k: tech.get(k) for k in (
                "rsi_14", "rsi_signal", "macd", "macd_signal", "macd_cross",
                "macd_hist", "ema_9", "ema_21", "ema_50", "sma_20",
                "bb_upper", "bb_mid", "bb_lower", "bb_signal", "bb_width",
                "atr_14", "atr_pct", "stoch_k", "stoch_d", "stoch_signal",
                "cci_20", "williams_r", "obv", "cmf", "cmf_signal",
                "mfi", "mfi_signal", "vwap", "composite_signal", "composite_bias",
            )},
            "volatility": {
                "annualised_vol_pct": vol_data.get("annualised_vol"),
                "daily_vol_pct":      vol_data.get("daily_vol"),
                "engine":             vol_data.get("engine"),
                "vol_regime": (
                    "HIGH"   if (safe_float(vol_data.get("annualised_vol")) > 80)  else
                    "MEDIUM" if (safe_float(vol_data.get("annualised_vol")) > 40)  else
                    "LOW"
                ),
            },
            "regime": {
                "label":       regime_label,
                "confidence":  round(regime_conf * 100, 1),
                "size_mult":   _MULT_MAP.get(regime_label, 0.8),
                "detected_at": regime.time.isoformat() if regime else None,
            },
            "scenario_p50_pct":  p50,
            "scenario_bear_pct": bear_ret,
            "scenario_bull_pct": bull_ret,
            "risk_state": (
                "CRITICAL" if regime_label == "CRISIS"  else
                "ELEVATED" if regime_label == "BEAR"    else
                "NORMAL"
            ),
            "portfolio": {
                "total_equity":   round(total_equity,   2),
                "total_exposure": round(total_exposure, 2),
                "exposure_pct":   round(exposure_pct * 100, 2),
                "open_positions": len(positions),
            },
            "gates":           gates,
            "behavior_score":  behavior_score,
            "data_latency_ms": round(safe_float(safe_ms(ticks[-1].time)), 1) if ticks else 9999,
            "evaluated_at":    now_iso(),
        }
 
    except Exception as exc:  # noqa: BLE001
        return {
            "error": str(exc),
            "decision": "WAIT",
            "evaluated_at": now_iso(),
        }
 