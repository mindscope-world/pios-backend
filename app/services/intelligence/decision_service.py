"""
Decision compute services — pure computation layer.

Contract:
  • Signatures: (current_user, db, **explicit_params) — no FastAPI Depends/Query.
  • limit, symbol, and other filters are explicit parameters with defaults,
    NOT hardcoded inside the function and NOT pulled from FastAPI Query().
  • Never raises; always returns a serialisable dict.
  • All DB calls are async (await db.execute(...)).
  • asyncio.gather is awaited — never called without await.
  • Imports are real; no gunicorn.config.User, no numba.np, no tomlkit.datetime.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import numpy as np
from sqlalchemy import select

from app.db.session import AsyncSession
from app.models.all_models import Fill, Order, PnLSnapshot, Strategy, Symbol
from app.models.all_models import User
from app.services.market_data_service import (
    get_live_ticker,
    get_orderbook,
    get_recent_trades,
)
from app.services.quant_engine import (
    build_quant_core_gates,
    compute_technical_indicators,
    detect_signal_conflicts,
    estimate_volatility_garch,
)
from app.services.intelligence.behavior_service import compute_behavior_session
from app.helpers.helpers import (
    get_symbol_by_name,
    latest_regime,
    open_positions,
    primary_symbol,
    recent_ticks,
    safe_ms,
)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        f = float(value)
        return default if (f != f) else f   # NaN guard without math import
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# compute_decision_current
# ─────────────────────────────────────────────────────────────────────────────

async def compute_decision_current(
    current_user: User,
    db: AsyncSession,
    *,
    symbol: str | None = None,
) -> dict:
    """
    Full Quant Core decision pipeline with live market data enrichment.

    Parameters
    ----------
    symbol:
        Optional symbol name.  When None the primary symbol is auto-detected.
    """
    try:
        # ── Symbol & tick data ────────────────────────────────────────────────
        sym = (
            await get_symbol_by_name(db, symbol)
            if symbol
            else await primary_symbol(db)
        )
        if not sym:
            return {
                "error": "no_market_data",
                "decision": "WAIT",
                "evaluated_at": _now_iso(),
            }

        ticks     = await recent_ticks(db, sym.id, 200)
        regime    = await latest_regime(db, sym.id)
        positions = await open_positions(db, current_user.id)

        prices  = [_safe_float(t.price)  for t in ticks] if ticks else []
        volumes = [_safe_float(t.volume) for t in ticks] if ticks else []
        sides   = [str(t.side or "")     for t in ticks] if ticks else []

        # ── Portfolio snapshot ────────────────────────────────────────────────
        snap = (
            await db.execute(
                select(PnLSnapshot)
                .where(PnLSnapshot.user_id == current_user.id)
                .order_by(PnLSnapshot.snapshot_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

        total_equity   = _safe_float(snap.total_equity if snap else None, 100_000.0)
        total_exposure = sum(_safe_float(p.qty) * _safe_float(p.avg_cost) for p in positions)
        exposure_pct   = min(1.0, total_exposure / max(total_equity, 1.0))

        # ── Live market data — all failures are non-fatal ─────────────────────
        live_ticker: dict = {}
        live_ob: dict     = {}
        live_trades: list = []
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

        # ── Technical indicators ──────────────────────────────────────────────
        tech = (
            compute_technical_indicators(prices, volumes)
            if len(prices) >= 14
            else {}
        )

        # ── Quant Core 8-gate pipeline ────────────────────────────────────────
        decision, confidence, gates, size_info = build_quant_core_gates(
            prices,
            volumes,
            sides,
            regime_override=regime.regime_label if regime else None,
            positions_exposure=exposure_pct,
        )

        # ── Signal conflicts ──────────────────────────────────────────────────
        sig_conflict = (
            detect_signal_conflicts(prices)
            if len(prices) >= 30
            else {"level": "NONE", "conflicting_signals": []}
        )

        # ── Scenario projections (GARCH + MC percentiles) ─────────────────────
        if len(prices) >= 10:
            rets     = np.diff(np.log(np.array(prices, dtype=float) + 1e-10))
            p50      = round(float(np.percentile(rets, 50)) * 100, 3)
            bear_ret = round(float(np.percentile(rets,  5)) * 100, 3)
            bull_ret = round(float(np.percentile(rets, 95)) * 100, 3)
            vol_data = estimate_volatility_garch(prices)
        else:
            p50, bear_ret, bull_ret = 0.0, -2.0, 2.0
            vol_data = {"annualised_vol": 0, "daily_vol": 0, "engine": "N/A"}

        regime_label = regime.regime_label if regime else "RANGE"
        regime_conf  = _safe_float(regime.confidence if regime else None, 0.5)

        # ── Latest strategy (informational) ───────────────────────────────────
        strat = (
            await db.execute(
                select(Strategy)
                .where(Strategy.created_by == current_user.id)
                .order_by(Strategy.updated_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

        # ── Live trade-flow analysis ──────────────────────────────────────────
        trade_flow: dict = {}
        if live_trades:
            buy_vol   = sum(t.get("amount", 0) for t in live_trades if t.get("side") == "BUY")
            sell_vol  = sum(t.get("amount", 0) for t in live_trades if t.get("side") == "SELL")
            buy_count = sum(1 for t in live_trades if t.get("side") == "BUY")
            sell_count= sum(1 for t in live_trades if t.get("side") == "SELL")
            last5     = [t["price"] for t in live_trades[-5:] if t.get("price")]
            denom     = max(buy_vol + sell_vol, 1e-9)
            trade_flow = {
                "buy_count":       buy_count,
                "sell_count":      sell_count,
                "buy_volume":      round(buy_vol,  6),
                "sell_volume":     round(sell_vol, 6),
                "imbalance_ratio": round((buy_vol - sell_vol) / denom, 4),
                "aggressor_bias": (
                    "BUY_HEAVY"  if buy_count  > sell_count * 1.3 else
                    "SELL_HEAVY" if sell_count > buy_count  * 1.3 else
                    "BALANCED"
                ),
                "price_momentum": (
                    round((last5[-1] - last5[0]) / last5[0] * 100, 4)
                    if len(last5) >= 2 else 0
                ),
            }

        _MULT = {"BULL": 1.0, "BEAR": 0.6, "RANGE": 0.8, "CRISIS": 0.3, "RECOVERY": 0.7}

        # Real behavior score (override rate, frequency vs baseline, deviation
        # from AI) instead of a fixed constant -- see behavior_service.py.
        # None (rather than a fake number) if it can't be computed.
        behavior_score = None
        try:
            behavior = await compute_behavior_session(current_user, db)
            behavior_score = behavior["score"]
        except Exception:  # noqa: BLE001
            pass

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
                    "HIGH"   if _safe_float(vol_data.get("annualised_vol")) > 80  else
                    "MEDIUM" if _safe_float(vol_data.get("annualised_vol")) > 40  else
                    "LOW"
                ),
            },
            "regime": {
                "label":       regime_label,
                "confidence":  round(regime_conf * 100, 1),
                "size_mult":   _MULT.get(regime_label, 0.8),
                "detected_at": regime.time.isoformat() if regime else None,
            },
            "scenario_p50_pct":  p50,
            "scenario_bear_pct": bear_ret,
            "scenario_bull_pct": bull_ret,
            "risk_state": (
                "CRITICAL" if regime_label == "CRISIS" else
                "ELEVATED" if regime_label == "BEAR"   else
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
            "data_latency_ms": round(_safe_float(safe_ms(ticks[-1].time)), 1) if ticks else 9999,
            "evaluated_at":    _now_iso(),
        }

    except Exception as exc:  # noqa: BLE001
        return {
            "error": str(exc),
            "decision": "WAIT",
            "evaluated_at": _now_iso(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# compute_decision_feed
# ─────────────────────────────────────────────────────────────────────────────

async def compute_decision_feed(
    current_user: User,
    db: AsyncSession,
    *,
    limit: int = 50,
) -> dict:
    """
    Recent decision history derived from filled / cancelled orders.

    Parameters
    ----------
    limit:
        Maximum number of orders to return.  Filtering happens here at the
        DB query level, not post-hoc on a full table scan.
    """
    try:
        # ── Orders — filtered and limited at query time ────────────────────────
        orders = (
            await db.execute(
                select(Order)
                .where(Order.user_id == current_user.id)
                .order_by(Order.created_at.desc())
                .limit(limit)                       # ← filtering at DB level
            )
        ).scalars().all()

        if not orders:
            return {
                "items": [],
                "summary": {
                    "total_orders": 0, "filled": 0, "blocked": 0,
                    "fill_rate_pct": 0.0, "total_pnl_usd": 0.0,
                },
            }

        # ── Symbol lookup (batch, one query) ─────────────────────────────────
        sym_ids = list({o.symbol_id for o in orders})
        sym_map: dict[int, str] = {}
        if sym_ids:
            for s in (
                await db.execute(select(Symbol).where(Symbol.id.in_(sym_ids)))
            ).scalars().all():
                sym_map[s.id] = s.symbol

        # ── Fills lookup (batch, one query) ───────────────────────────────────
        order_ids = [o.id for o in orders]
        fills_map: dict[str, list] = {}
        if order_ids:
            for f in (
                await db.execute(
                    select(Fill).where(Fill.order_id.in_(order_ids))
                )
            ).scalars().all():
                fills_map.setdefault(str(f.order_id), []).append(f)

        # ── Build items ───────────────────────────────────────────────────────
        items: list[dict] = []
        for o in orders:
            sym_str = sym_map.get(o.symbol_id, "UNKNOWN")
            base    = sym_str.split("/")[0] if "/" in sym_str else sym_str
            o_fills = fills_map.get(str(o.id), [])

            if o.status == "FILLED":
                decision, block_reason = "ALLOW", None
            elif o.status == "REJECTED":
                decision, block_reason = "BLOCK", (o.reject_reason or "Risk check failed")
            elif o.status == "CANCELLED":
                decision, block_reason = "WAIT", "Order cancelled"
            else:
                decision, block_reason = "ALLOW", None

            # Fills → avg fill price, commission, realised P&L
            avg_fill_px:      float | None = None
            total_commission: float | None = None
            fill_pnl:         float | None = None

            if o_fills:
                total_qty = sum(_safe_float(f.qty) for f in o_fills) or 1e-9
                avg_fill_px = round(
                    sum(_safe_float(f.price) * _safe_float(f.qty) for f in o_fills)
                    / total_qty,
                    8,
                )
                total_commission = round(
                    sum(_safe_float(f.commission) for f in o_fills), 4
                )
                if o.price and avg_fill_px is not None:
                    ref_px = _safe_float(o.avg_fill_price or avg_fill_px)
                    fill_pnl = round(
                        (ref_px - _safe_float(o.price))
                        * _safe_float(o.filled_qty or o.qty),
                        4,
                    )

            items.append({
                "id":              str(o.id),
                "evaluated_at":    o.created_at.isoformat(),
                "filled_at":       o.filled_at.isoformat() if o.filled_at else None,
                "symbol":          sym_str,
                "side":            o.side,
                "order_type":      o.order_type,
                "decision":        decision,
                "final_size_lot":  _safe_float(o.qty)        if o.qty        else None,
                "filled_qty":      _safe_float(o.filled_qty) if o.filled_qty else None,
                "size_unit":       base,
                "requested_price": _safe_float(o.price)      if o.price      else None,
                "avg_fill_price":  avg_fill_px,
                "slippage_bps": (
                    round(
                        (avg_fill_px - _safe_float(o.price))
                        / _safe_float(o.price) * 10_000,
                        2,
                    )
                    if avg_fill_px and o.price
                    else None
                ),
                "commission_usd": total_commission,
                "realised_pnl":   fill_pnl,
                "block_reason":   block_reason,
                "reject_reason":  o.reject_reason,
                "confidence":     82.0 if decision == "ALLOW" else 45.0,
                "status":         o.status,
                "fill_count":     len(o_fills),
            })

        # ── Summary stats ─────────────────────────────────────────────────────
        filled  = [i for i in items if i["decision"] == "ALLOW"]
        blocked = [i for i in items if i["decision"] == "BLOCK"]
        total_pnl = round(sum(i["realised_pnl"] or 0 for i in items), 4)

        return {
            "items": items,
            "summary": {
                "total_orders":  len(items),
                "filled":        len(filled),
                "blocked":       len(blocked),
                "fill_rate_pct": round(len(filled) / max(len(items), 1) * 100, 1),
                "total_pnl_usd": total_pnl,
            },
        }

    except Exception as exc:  # noqa: BLE001
        return {
            "error": str(exc),
            "items": [],
            "summary": {
                "total_orders": 0, "filled": 0, "blocked": 0,
                "fill_rate_pct": 0.0, "total_pnl_usd": 0.0,
            },
        }


# ─────────────────────────────────────────────────────────────────────────────
# compute_decision_traces
# ─────────────────────────────────────────────────────────────────────────────

async def compute_decision_traces(
    current_user: User,
    db: AsyncSession,
    *,
    limit: int = 50,
) -> dict:
    """
    Recent decision traces: each order annotated with Quant Core gate
    logic path and mitigation strategy.

    Parameters
    ----------
    limit:
        Maximum number of traces to return.  Applied at DB query time.
    """
    try:
        # ── Orders — filtered and limited at query time ────────────────────────
        orders = (
            await db.execute(
                select(Order)
                .where(Order.user_id == current_user.id)
                .order_by(Order.created_at.desc())
                .limit(limit)                       # ← filtering at DB level
            )
        ).scalars().all()

        if not orders:
            return {"traces": []}

        # ── Symbol lookup (batch, one query) ─────────────────────────────────
        sym_ids = list({o.symbol_id for o in orders})
        sym_map: dict[int, str] = {}
        if sym_ids:
            for s in (
                await db.execute(select(Symbol).where(Symbol.id.in_(sym_ids)))
            ).scalars().all():
                sym_map[s.id] = s.symbol

        # ── Build traces ──────────────────────────────────────────────────────
        traces: list[dict] = []
        for o in orders:
            sym_str = sym_map.get(o.symbol_id, "UNKNOWN")

            # Real per-order data: risk_check's recorded pass/fail checks (from
            # order_service._risk_check()) and the transition trail
            # Order.transition() records in state_history -- both persisted per
            # order, unlike the fixed D3.x logic strings / confidence constants
            # this replaces (there is no per-order record of the 8-gate quant
            # core pipeline -- it's computed live/on-demand, not persisted at
            # order submission time -- so this is the closest real substitute).
            risk_check   = o.risk_check or {}
            checks       = risk_check.get("checks", [])
            passed_count = sum(1 for c in checks if c.get("passed"))
            total_checks = len(checks) or 1

            if o.state_history:
                logic = " → ".join(f"{h.get('from') or 'NEW'}→{h['to']}" for h in o.state_history)
            else:
                logic = f"NEW→{o.status}"

            if o.status in ("FILLED", "PARTIAL"):
                decision   = "ALLOW"
                confidence = round(70 + 30 * (passed_count / total_checks), 1)
                mitigation = "Sniper limit order with 0.5σ buffer"
            elif o.status == "REJECTED":
                decision   = "BLOCK"
                reason     = o.reject_reason or "Risk limit"
                failed_n   = total_checks - passed_count
                confidence = round(max(10.0, 70 - 30 * (failed_n / total_checks)), 1)
                logic      = f"{logic} :: {reason}"
                mitigation = "Order rejected — no position taken"
            else:
                decision   = "REDUCE"
                confidence = round(50 + 20 * (passed_count / total_checks), 1)
                mitigation = "Position size reduced 40% for regime"

            traces.append({
                "id":                  str(o.id),
                "timestamp":           o.created_at.isoformat(),
                "symbol":              sym_str,
                "side":                o.side if o.side in ("BUY", "SELL") else "FLAT",
                "decision":            decision,
                "confidence":          confidence,
                "variance":            round(abs(_safe_float(o.qty) * 0.05), 4),
                "size":                _safe_float(o.qty),
                "logic_path":          logic,
                "mitigation_strategy": mitigation,
            })

        return {"traces": traces}

    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc), "traces": []}