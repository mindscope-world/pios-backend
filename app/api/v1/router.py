from fastapi import APIRouter

from app.api.v1.endpoints import (
    auth, users, market, brokers, orders,
    positions, strategies, risk,
    alerts, audit, data_quality,
    intelligence, behavior, capital, websocket,
    mt5_bridge,
)
from app.api.v1.endpoints.execution_quality import (
    data_router,
    tca_router,
    market_ticks_router,
)

api_router = APIRouter()

# ── Existing endpoints ─────────────────────────────────────────────────────────
api_router.include_router(auth.router)
api_router.include_router(users.router)
api_router.include_router(brokers.router)
api_router.include_router(market.router)
api_router.include_router(orders.router)
api_router.include_router(positions.router)
api_router.include_router(strategies.router)
api_router.include_router(risk.router)
api_router.include_router(alerts.router)
api_router.include_router(audit.router)
api_router.include_router(data_quality.router)

# ── New endpoints (§1-§15 of api.ts) ─────────────────────────────────────────
api_router.include_router(intelligence.router)   # /intelligence/*
api_router.include_router(behavior.router)       # /behavior/*
api_router.include_router(capital.router)        # /capital/*
api_router.include_router(data_router)           # /data/integrity/*
api_router.include_router(tca_router)            # /execution/tca/*
api_router.include_router(market_ticks_router)   # /market/ticks/{symbol_id}

# ── WebSocket endpoints ──────────────────────────────────────────────────────
api_router.include_router(websocket.router)     # /ws
api_router.include_router(mt5_bridge.router)    # /ws/mt5/{broker_id} -- MT5 EA bridge
