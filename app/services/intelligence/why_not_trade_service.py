from datetime import datetime, timedelta, timezone

from redis import asyncio
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from app.helpers.helpers import latest_regime, open_positions, get_primary_with_ticks, safe_float, now_iso, get_symbol_by_name, safe_ms
from app.models.all_models import KillSwitchEvent, MarketTick, Strategy, Symbol, User
from app.services.market_data_service import get_live_ticker, get_orderbook
from app.services.quant_engine import compute_ofi_signals, detect_outlier_ticks


async def compute_why_not_trade(current_user, db):
    """
    Why-Not-Trade constraint engine for the primary symbol.
 
    Evaluates 7 constraint layers + live spread/liquidity overlay.
    Symbol selection and limit filtering happen at the Redis/worker level.
 
    Returns a single symbol result dict; never raises.
    """
    try:
        positions = await open_positions(db, current_user.id)
 
        sym, ticks = await get_primary_with_ticks(db, 100)
        if sym is None:
            return {
                "error": "no_primary_symbol",
                "final_decision": "BLOCK",
                "constraints": [],
                "evaluated_at": now_iso(),
            }
 
        sym_str  = sym.symbol
        regime   = await latest_regime(db, sym.id)
        prices   = [safe_float(t.price)  for t in ticks]
        volumes  = [safe_float(t.volume) for t in ticks]
        sides    = [str(t.side or "")     for t in ticks]
 
        # Live data — failures are non-fatal
        live_ticker: dict = {}
        live_ob: dict     = {}
        try:
            live_ticker, live_ob = await asyncio.gather(
                get_live_ticker(sym_str),
                get_orderbook(sym_str, depth=5),
                return_exceptions=False,
            )
        except Exception:  # noqa: BLE001
            pass
        live_ticker = live_ticker if isinstance(live_ticker, dict) else {}
        live_ob     = live_ob     if isinstance(live_ob,     dict) else {}
 
        constraints: list[dict]   = []
        final_decision: str       = "ALLOW"
        size_impact: float        = 0.0
 
        # ── C1: HMM Regime ────────────────────────────────────────────────
        _MULT_MAP = {
            "BULL": 1.0, "BEAR": 0.6, "RANGE": 0.8,
            "CRISIS": 0.3, "RECOVERY": 0.7,
        }
        if regime:
            rl   = regime.regime_label
            conf = round(safe_float(regime.confidence) * 100, 1)
            if rl == "CRISIS":
                constraints.append({
                    "id": "C1", "icon": "🚨", "severity": "BLOCK",
                    "title": "Crisis Regime Active",
                    "body": f"HMM: CRISIS at {conf}% confidence. All new positions blocked.",
                    "size_impact_pct": 0,
                    "advisory": "Exit all positions. Stay flat until regime clears.",
                })
                final_decision = "BLOCK"
            elif rl == "BEAR":
                constraints.append({
                    "id": "C1", "icon": "🐻", "severity": "WARN",
                    "title": "Bear Regime — Size −40%",
                    "body": f"HMM: BEAR at {conf}% confidence. Regime multiplier 0.6×.",
                    "size_impact_pct": -40,
                    "advisory": "Reduce position size. Prefer short setups or cash.",
                })
                size_impact -= 40
            elif rl == "RANGE":
                constraints.append({
                    "id": "C1", "icon": "〰️", "severity": "INFO",
                    "title": "Range Regime — Size −20%",
                    "body": f"HMM: RANGE at {conf}% confidence. Mean-reverting environment.",
                    "size_impact_pct": -20,
                    "advisory": "Prefer range-bound strategies. Avoid breakout trades.",
                })
                size_impact -= 20
 
        # ── C2: LOF Data Quality ──────────────────────────────────────────
        if prices:
            try:
                dq = detect_outlier_ticks(prices, volumes)
                if safe_float(dq.get("dq_score"), 100) < 80:
                    n_out = len(dq.get("outlier_indices", []))
                    constraints.append({
                        "id": "C2", "icon": "⚠️", "severity": "WARN",
                        "title": f"Data Quality Degraded ({dq['dq_score']:.0f}%)",
                        "body": (
                            f"LOF outlier detection: {n_out} anomalous ticks "
                            f"in last {len(prices)}."
                        ),
                        "size_impact_pct": -20,
                        "advisory": (
                            "Increase DQ monitoring. "
                            "Reduce size until feed stabilises."
                        ),
                    })
                    size_impact -= 20
            except Exception:  # noqa: BLE001
                pass  # DQ check is advisory; don't block on its own failure
 
        # ── C3: Feed Staleness ────────────────────────────────────────────
        if ticks:
            lag_ms = safe_float(safe_ms(ticks[-1].time))
            if lag_ms > 30_000:
                constraints.append({
                    "id": "C3", "icon": "🔴", "severity": "BLOCK",
                    "title": f"Feed Down ({round(lag_ms / 1000)}s stale)",
                    "body": (
                        f"No tick data from {sym.exchange} for "
                        f"{round(lag_ms / 1000)}s. Price risk extreme."
                    ),
                    "size_impact_pct": 0,
                    "advisory": "Do not trade. Feed is down. Await data recovery.",
                })
                final_decision = "BLOCK"
            elif lag_ms > 2_000:
                constraints.append({
                    "id": "C3", "icon": "⏱️", "severity": "WARN",
                    "title": f"Feed Latency {round(lag_ms / 1000, 1)}s",
                    "body": f"Last tick {round(lag_ms / 1000, 1)}s ago. Staleness risk.",
                    "size_impact_pct": -15,
                    "advisory": "Consider waiting for feed to stabilise.",
                })
                size_impact -= 15
 
        # ── C4: OFI Stop-Hunt / Liquidity Vacuum ─────────────────────────
        if len(prices) >= 10:
            try:
                tick_dicts = [
                    {"price": p, "volume": v, "side": s}
                    for p, v, s in zip(prices, volumes, sides)
                ]
                ofi = compute_ofi_signals(tick_dicts)
                if safe_float(ofi.get("stop_hunt_probability")) > 0.5:
                    constraints.append({
                        "id": "C4", "icon": "🎣", "severity": "WARN",
                        "title": (
                            f"Stop-Hunt Pattern "
                            f"({round(safe_float(ofi['stop_hunt_probability']) * 100)}%)"
                        ),
                        "body": (
                            f"Sell dominance {safe_float(ofi['vol_delta_divergence']):.2f}, "
                            f"vacuum {safe_float(ofi['liquidity_vacuum']):.2f}."
                        ),
                        "size_impact_pct": -25,
                        "advisory": (
                            "Wait for stop-hunt to clear. "
                            "Enter after volume normalises."
                        ),
                    })
                    size_impact -= 25
                if safe_float(ofi.get("liquidity_vacuum")) > 0.6:
                    constraints.append({
                        "id": "C4b", "icon": "🌪️", "severity": "WARN",
                        "title": "Liquidity Vacuum",
                        "body": (
                            "Low volume vs price range. "
                            "Wide spread risk. Slippage elevated."
                        ),
                        "size_impact_pct": -15,
                        "advisory": (
                            "Use limit orders only. "
                            "Market orders will suffer high slippage."
                        ),
                    })
                    size_impact -= 15
            except Exception:  # noqa: BLE001
                pass
 
        # ── C5: Live Spread / Liquidity ───────────────────────────────────
        spread_bps = safe_float(live_ob.get("spread_bps"))
        liq_score  = live_ob.get("liquidity_score")
        if spread_bps > 30:
            constraints.append({
                "id": "C5", "icon": "📊", "severity": "WARN",
                "title": f"Wide Spread ({spread_bps:.1f} bps)",
                "body": (
                    f"Current bid-ask spread is {spread_bps:.1f} bps. "
                    "Execution cost elevated."
                ),
                "size_impact_pct": -10,
                "advisory": (
                    f"Use limit orders. "
                    f"Expect {round(spread_bps / 2, 1)} bps adverse selection."
                ),
            })
            size_impact -= 10
        elif liq_score is not None and safe_float(liq_score) < 20:
            constraints.append({
                "id": "C5b", "icon": "💧", "severity": "WARN",
                "title": f"Low Liquidity (score {safe_float(liq_score):.0f}/100)",
                "body": (
                    f"Orderbook depth thin. "
                    f"Slippage: buy {safe_float(live_ob.get('slippage_buy_pct')):.3f}%."
                ),
                "size_impact_pct": -20,
                "advisory": "Reduce size significantly. Use TWAP/VWAP execution.",
            })
            size_impact -= 20
 
        # ── C6: Position Concentration ────────────────────────────────────
        pos_in_sym = [p for p in positions if p.symbol_id == sym.id]
        if len(pos_in_sym) >= 2:
            constraints.append({
                "id": "C6", "icon": "📦", "severity": "INFO",
                "title": f"Concentration ({len(pos_in_sym)} positions)",
                "body": f"{len(pos_in_sym)} open positions in {sym_str}.",
                "size_impact_pct": -10,
                "advisory": (
                    "Consider netting existing positions before adding more."
                ),
            })
            size_impact -= 10
 
        # ── C7: Kill Switch ───────────────────────────────────────────────
        try:
            ks = (
                await db.execute(
                    __import__("sqlalchemy", fromlist=["select"])
                    .select(KillSwitchEvent)
                    .where(
                        KillSwitchEvent.triggered_by == current_user.id,
                        KillSwitchEvent.created_at
                        >= datetime.now(timezone.utc) - timedelta(hours=24),
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
        except Exception:  # noqa: BLE001
            ks = None
 
        if ks:
            triggered_h = round(
                safe_float(safe_ms(ks.created_at)) / 3_600_000, 1
            )
            constraints.append({
                "id": "C7", "icon": "🔴", "severity": "BLOCK",
                "title": "Kill Switch (last 24h)",
                "body": (
                    f"Triggered {triggered_h}h ago. "
                    "Manual review required."
                ),
                "size_impact_pct": 0,
                "advisory": (
                    "No automated trading until manual sign-off completed."
                ),
            })
            final_decision = "BLOCK"
 
        # ── All-clear ─────────────────────────────────────────────────────
        if not constraints:
            constraints.append({
                "id": "C0", "icon": "✅", "severity": "INFO",
                "title": "All Clear",
                "body": (
                    f"No active constraints for {sym_str}. "
                    "Full sizing available."
                ),
                "size_impact_pct": 0,
                "advisory": (
                    "System is green. Execute according to strategy rules."
                ),
            })
 
        # ── Lot sizing ────────────────────────────────────────────────────
        base_lot = round(
            max(0.001, 0.05 * (1 - min(0.9, len(positions) * 0.05))), 6
        )
        net_lot = round(base_lot * max(0.0, 1 + size_impact / 100), 6)
        if final_decision == "ALLOW" and net_lot <= 0:
            final_decision = "BLOCK"
 
        # Latest strategy (informational — no filtering here)
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
 
        return {
            "symbol": sym_str,
            "symbol_id": sym.id,
            "asset_class": sym.asset_class,
            "exchange": sym.exchange,
            "strategy_id": str(strat.id) if strat else "auto",
            "final_decision": final_decision,
            "net_size_lot": net_lot,
            "base_size_lot": base_lot,
            "size_impact_pct": round(size_impact, 1),
            "live_price": live_ticker.get("last"),
            "live_spread_bps": spread_bps,
            "live_liquidity": liq_score,
            "constraints": constraints,
            "block_count": sum(1 for c in constraints if c["severity"] == "BLOCK"),
            "warn_count":  sum(1 for c in constraints if c["severity"] == "WARN"),
            "evaluated_at": now_iso(),
        }
 
    except Exception as exc:  # noqa: BLE001
        return {
            "error": str(exc),
            "final_decision": "BLOCK",
            "constraints": [],
            "evaluated_at": now_iso(),
        }