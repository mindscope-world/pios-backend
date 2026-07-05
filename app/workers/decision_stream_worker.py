import asyncio
import orjson
import logging
import numpy as np
from redis.asyncio import Redis

from app.services.quant_engine import (
    build_quant_core_gates,
    estimate_volatility_garch,
    detect_signal_conflicts,
)

redis = Redis.from_url("redis://redis:6379")


async def get_ticks(symbol: str):
    data = await redis.lrange(f"ticks:{symbol}", 0, 200)
    return [orjson.loads(x) for x in data]


async def compute(symbol: str):
    ticks = await get_ticks(symbol)

    if len(ticks) < 20:
        return

    prices = [t["price"] for t in ticks]
    volumes = [t["qty"] for t in ticks]
    sides = [t["side"] for t in ticks]

    # === YOUR ORIGINAL LOGIC ===

    decision, confidence, gates, size_info = build_quant_core_gates(
        prices,
        volumes,
        sides,
        regime_override=None,
        positions_exposure=0.2,
    )

    sig_conflict = detect_signal_conflicts(prices)

    rets = np.diff(np.log(np.array(prices) + 1e-10))
    p50 = float(np.percentile(rets, 50)) * 100

    vol_data = estimate_volatility_garch(prices)

    payload = {
        "symbol": symbol,
        "decision": decision,
        "confidence": confidence,
        "gates": gates,
        "volatility": vol_data,
        "scenario_p50_pct": p50,
        "evaluated_at": asyncio.get_event_loop().time(),
    }

    # store snapshot (non-blocking)
    try:
        await redis.set(f"decision:{symbol}", orjson.dumps(payload), ex=2)
    except Exception as e:
        logging.getLogger(__name__).error(f"Redis SET error for decision:{symbol}: {e}")

    # push to websocket layer (log and continue on failure)
    try:
        await redis.publish("signals", orjson.dumps(payload))
    except Exception as e:
        logging.getLogger(__name__).error(f"Redis PUBLISH error for signals ({symbol}): {e}")


async def loop():
    symbols = ["BTCUSDT", "ETHUSDT"]

    while True:
        await asyncio.gather(*(compute(s) for s in symbols))
        await asyncio.sleep(0.01)  # 10ms loop


if __name__ == "__main__":
    asyncio.run(loop())