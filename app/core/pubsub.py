import asyncio
import orjson
from app.core.redis import get_redis
from app.services.websocket.manager import manager

CHANNELS = [
    "why_not_trade",
    "command_center",
    "scenarios",
    "decision_feed",
    "decision_traces",
    "order_flow",
    "features",
    "gmig_snapshot",
    "gmig_radar",
    "regime_history",
    "regime_trend",
    "alpha_state",
    "alpha_darwin",
    "adaptation_feed",
    "adaptation_active",
    "adaptation_drift",
    "behavior_session",
    "behavior_trend",
    "behavior_overrides",
    "risk_metrics",
    "capital_allocation",
    "capital_rebalance",
    "data_integrity",
    "data_quality",
    "positions",
    "portfolio_metrics",
    "equity_curve",
    "strategies",
    "alerts",
    "trading_view_ticks",
    "market_ticks",
    "market_candles",
]


async def start_redis_listener():
    r = get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(*CHANNELS)

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue

        channel = msg["channel"].decode()
        data = orjson.loads(msg["data"])

        # Normalize to match what clients subscribe with (BTC/USDT → BTCUSDT)
        symbol = (data.get("symbol") or "").replace("/", "").upper()
        user_id = data.get("user_id")

        if channel == "market_ticks":
            payload = {
                "channel": "ticks",
                "type": "tick",
                **data,
            }
            if user_id:
                await manager.send_to_user(user_id, "ticks", symbol, payload)
            else:
                await manager.broadcast_symbol("ticks", symbol, payload)
            continue

        if channel == "market_candles":
            payload = {
                "channel": "candles",
                "type": "candle",
                **data,
            }
            if user_id:
                await manager.send_to_user(user_id, "candles", symbol, payload)
            else:
                await manager.broadcast_symbol("candles", symbol, payload)
            continue

        if user_id:
            await manager.send_to_user(user_id, channel, symbol, data)
        else:
            await manager.broadcast_symbol(channel, symbol, data)