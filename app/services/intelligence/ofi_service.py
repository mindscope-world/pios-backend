from fastapi import HTTPException, Query
import asyncio
from datetime import datetime, timedelta, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from app.helpers.helpers import latest_regime, open_positions, primary_symbol, recent_ticks, get_symbol_by_name, safe_ms
from app.core.deps import get_current_user
from app.db.session import get_db
from app.models.all_models import User
from app.services.market_data_service import get_orderbook, get_recent_trades
from app.services.quant_engine import compute_ofi_signals


# async def compute_ofi_signal(
#     symbol,
#     current_user,
#     db,
# ):
#     sym = await get_symbol_by_name(db, symbol) if symbol else await primary_symbol(db)
#     if not sym:
#         raise HTTPException(status_code=503, detail="No market data available")
#     ticks = await recent_ticks(db, sym.id, 200)
#     if not ticks:
#         raise HTTPException(status_code=503, detail="No tick data for symbol")

#     buy_vol  = sum(float(t.volume) for t in ticks if (t.side or "").upper() in ("BUY", "BID"))
#     sell_vol = sum(float(t.volume) for t in ticks if (t.side or "").upper() in ("SELL", "ASK"))
#     total_vol = buy_vol + sell_vol or 1.0

#     prices = [float(t.price) for t in ticks]
#     price_range = (max(prices) - min(prices)) / max(prices) if max(prices) else 0

#     inst_absorption  = round(min(buy_vol / total_vol, 1.0), 4)
#     liq_vacuum       = round(min(price_range * 15, 1.0), 4)
#     stop_hunt_prob   = round(max(0.0, min(1.0, (sell_vol / total_vol) * (1 + liq_vacuum))), 4)

#     # Vol-delta divergence: if price up but sell vol dominant
#     last_price = float(ticks[-1].price)
#     first_price = float(ticks[0].price)
#     price_dir_up = last_price > first_price
#     vol_dir_up   = buy_vol > sell_vol
#     vol_delta_div = round(0.8 if price_dir_up != vol_dir_up else 0.15, 4)

#     # Net modifier
#     net_mod = round(
#         (inst_absorption - 0.5) * 0.2 +
#         -liq_vacuum * 0.15 +
#         -stop_hunt_prob * 0.2 +
#         -vol_delta_div * 0.1,
#         4
#     )
#     if stop_hunt_prob > 0.7:
#         decision = "BLOCK"
#     elif liq_vacuum > 0.5 or vol_delta_div > 0.6:
#         decision = "CAUTION"
#     else:
#         decision = "ALLOW"

#     # Latest position qty as proxy for allowed lot
#     positions = await open_positions(db, current_user.id)
#     base_lot = max(0.001, sum(float(p.qty) for p in positions) * 0.05)
#     allowed  = round(base_lot * max(0.3, 1 + net_mod), 6)

#     latest = ticks[-1]
#     return {
#         "symbol": sym.symbol,
#         "institutional_absorption": inst_absorption,
#         "liquidity_vacuum": liq_vacuum,
#         "stop_hunt_probability": stop_hunt_prob,
#         "vol_delta_divergence": vol_delta_div,
#         "net_modifier": net_mod,
#         "decision": decision,
#         "allowed_size_lot": allowed,
#         "latency_ms": round(safe_ms(latest.time), 1),
#         "evaluated_at": datetime.now(timezone.utc).isoformat(),
#     }

async def compute_ofi_chart(
    current_user: User,
    db: AsyncSession,
    symbol: str | None = Query(None, description="Optional — auto-selects most active"),
    limit: int = Query(60, ge=10, le=500),
):
    sym = await get_symbol_by_name(db, symbol) if symbol else await primary_symbol(db)
    if not sym:
        return []
    ticks = await recent_ticks(db, sym.id, limit)

    output = []
    for t in ticks:
        vol = float(t.volume)
        side_up = (t.side or "").upper() in ("BUY", "BID")
        bid_delta = vol if side_up else 0.0
        ask_delta = vol if not side_up else 0.0
        output.append({
            "ts": t.time.isoformat(),
            "price": float(t.price),
            "bid_delta": round(bid_delta, 6),
            "ask_delta": round(ask_delta, 6),
            "net_flow": round(bid_delta - ask_delta, 6),
        })
    return output


async def compute_ofi(
    current_user: User,
    db: AsyncSession,
    symbol: str | None  = Query(None)
):
    """
    Enhanced OFI combining DB tick history + live L2 orderbook + recent trade tape.
    Computes institutional absorption, stop-hunt probability, liquidity vacuum,
    block trade activity, and live bid/ask imbalance.
    """

    sym_db = await get_symbol_by_name(db, symbol) if symbol else await primary_symbol(db)
    if not sym_db:
        return {"error": "no_market_data"}

    sym_str = sym_db.symbol

    # Parallel fetch
    ticks_t  = asyncio.create_task(recent_ticks(db, sym_db.id, 200))
    ob_t     = asyncio.create_task(get_orderbook(sym_str, depth=20))
    trades_t = asyncio.create_task(get_recent_trades(sym_str, limit=50))

    ticks, ob, live_trades = await asyncio.gather(ticks_t, ob_t, trades_t, return_exceptions=True)
    ticks       = ticks       if isinstance(ticks, list) else []
    ob          = ob          if isinstance(ob,   dict)  else {}
    live_trades = live_trades if isinstance(live_trades, list) else []

    # DB-based OFI
    prices  = [float(t.price)  for t in ticks]
    volumes = [float(t.volume) for t in ticks]
    sides   = [str(t.side or "") for t in ticks]
    tick_dicts = [{"price": p, "volume": v, "side": s} for p, v, s in zip(prices, volumes, sides)]
    db_ofi = compute_ofi_signals(tick_dicts) if tick_dicts else {}

    # Live trade OFI
    live_ofi: dict = {}
    if live_trades:
        buy_vol  = sum(t.get("amount", 0) for t in live_trades if t.get("side") == "BUY")
        sell_vol = sum(t.get("amount", 0) for t in live_trades if t.get("side") == "SELL")
        total    = buy_vol + sell_vol or 1.0
        avg_sz   = sum(t.get("amount", 0) for t in live_trades) / len(live_trades)
        blocks   = [t for t in live_trades if t.get("amount", 0) >= avg_sz * 10]
        live_ofi = {
            "buy_volume":       round(buy_vol, 6),
            "sell_volume":      round(sell_vol, 6),
            "net_flow":         round(buy_vol - sell_vol, 6),
            "imbalance":        round((buy_vol - sell_vol) / total, 4),
            "block_trades":     len(blocks),
            "block_volume":     round(sum(b.get("amount", 0) for b in blocks), 6),
            "aggressor_bias":   "BUY_HEAVY" if buy_vol > sell_vol * 1.3 else "SELL_HEAVY" if sell_vol > buy_vol * 1.3 else "BALANCED",
        }

    # Composite OFI decision
    book_imb = ob.get("imbalance", 0)
    db_net   = db_ofi.get("net_modifier", 0)
    combined_mod = round(book_imb * 0.4 + db_net * 0.6, 4)

    positions = await open_positions(db, current_user.id)
    base_lot  = round(max(0.001, 0.05 * (1 - min(0.9, len(positions) * 0.05))), 6)
    allowed   = round(base_lot * max(0.3, 1 + combined_mod), 6)

    if db_ofi.get("stop_hunt_probability", 0) > 0.65 or ob.get("spread_bps", 0) > 50:
        final_decision = "BLOCK"
    elif abs(combined_mod) > 0.2:
        final_decision = "CAUTION"
    else:
        final_decision = "ALLOW"

    return {
        "symbol":                 sym_str,
        "asset_class":            sym_db.asset_class,
        "db_ofi":                 db_ofi,
        "live_ofi":               live_ofi,
        "orderbook_imbalance":    ob.get("imbalance"),
        "orderbook_spread_bps":   ob.get("spread_bps"),
        "orderbook_liquidity":    ob.get("liquidity_score"),
        "slippage_buy_pct":       ob.get("slippage_buy_pct"),
        "slippage_sell_pct":      ob.get("slippage_sell_pct"),
        "combined_net_modifier":  combined_mod,
        "final_decision":         final_decision,
        "allowed_size_lot":       allowed,
        "latency_ms":             round(safe_ms(ticks[-1].time), 1) if ticks else 9999,
        "evaluated_at":           datetime.now(timezone.utc).isoformat(),
    }

async def compute_ofi_signal_auto(
    current_user: User,
    db: AsyncSession,
):
    """Auto-detect primary symbol and return OFI signal (no params required)."""
    sym   = await primary_symbol(db)
    ticks = await recent_ticks(db, sym.id, 200) if sym else []
    prices  = [float(t.price)  for t in ticks]
    volumes = [float(t.volume) for t in ticks]
    sides   = [str(t.side or "") for t in ticks]
    tick_dicts = [{"price": p, "volume": v, "side": s} for p, v, s in zip(prices, volumes, sides)]
    ofi = compute_ofi_signals(tick_dicts)
    positions = await open_positions(db, current_user.id)
    base_lot  = round(max(0.001, 0.05), 6)
    allowed   = round(base_lot * max(0.3, 1 + ofi["net_modifier"]), 6)
    return {
        "symbol": sym.symbol if sym else "UNKNOWN",
        "institutional_absorption": ofi["institutional_absorption"],
        "liquidity_vacuum": ofi["liquidity_vacuum"],
        "stop_hunt_probability": ofi["stop_hunt_probability"],
        "vol_delta_divergence": ofi["vol_delta_divergence"],
        "net_modifier": ofi["net_modifier"],
        "decision": ofi["decision"],
        "allowed_size_lot": allowed,
        "latency_ms": round(safe_ms(ticks[-1].time), 1) if ticks else 9999,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
    }
