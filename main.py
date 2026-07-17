# main.py
"""
Pi OS FastAPI Application
Run: uvicorn main:app --reload --port 9000
"""
import asyncio
from contextlib import asynccontextmanager
import contextlib

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
import structlog

from app.core.config import settings
from app.api.v1.router import api_router
from app.core.middleware import ExcludePathsGZipMiddleware
from app.core.pubsub import start_redis_listener
from app.db.session import engine, Base
from app.models.all_models import InvalidTransitionError
from app.services.broker_service import UnsupportedBrokerError

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────────────────────
    logger.info("Pi OS API starting", version=settings.APP_VERSION, env=settings.ENVIRONMENT)

    # Start Redis pub/sub listener in background
    # ✅ Store task reference to prevent garbage collection
    app.state.redis_listener = asyncio.create_task(start_redis_listener())
    logger.info("Redis listener started")

    # Reconcile open Alpaca orders with the broker (resting LIMITs that fill
    # after submit-time polling, broker-side cancels) — see alpaca_fill_sync.py
    from app.services.alpaca_fill_sync import run_alpaca_fill_sync
    app.state.alpaca_fill_sync = asyncio.create_task(run_alpaca_fill_sync())

    # Trade-update WebSocket stream — instant fills/cancels; the poll loop
    # above remains the safety net (see alpaca_trade_stream.py)
    from app.services.alpaca_trade_stream import run_alpaca_trade_streams
    app.state.alpaca_trade_stream = asyncio.create_task(run_alpaca_trade_streams())

    # Reconcile open MT5 orders with the terminal (pending LIMIT/STOP fills,
    # broker-side cancels) — poll safety net behind the EA's ORDER_UPDATE
    # push; see mt5_fill_sync.py
    from app.services.mt5_fill_sync import run_mt5_fill_sync
    app.state.mt5_fill_sync = asyncio.create_task(run_mt5_fill_sync())

    # Conditional-order engine — STOP_LIMIT triggers + OCO linked legs
    # (see conditional_orders.py)
    from app.services.conditional_orders import run_conditional_orders
    app.state.conditional_orders = asyncio.create_task(run_conditional_orders())

    # Mark-to-market — periodic revaluation of open positions against live
    # prices so unrealized P&L doesn't stale between fills (position_marks.py)
    from app.services.position_marks import run_position_marks
    app.state.position_marks = asyncio.create_task(run_position_marks())

    logger.info("Pi OS API ready")
    yield

    # ── Shutdown ──────────────────────────────────────────────────────────────
    for task_name in ("redis_listener", "alpaca_fill_sync", "alpaca_trade_stream",
                      "mt5_fill_sync", "conditional_orders", "position_marks"):
        task = getattr(app.state, task_name, None)
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    await engine.dispose()
    logger.info("Pi OS API shutdown complete")


# ── App factory ───────────────────────────────────────────────────────────────

def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(
        ExcludePathsGZipMiddleware,
        excluded_paths={
            "/api/v1/intelligence/stream",
            "/api/v1/intelligence/notifications/stream",
        },
        minimum_size=1000,
    )

    # Routes
    app.include_router(api_router, prefix="/api/v1")

    @app.exception_handler(InvalidTransitionError)
    async def invalid_transition_handler(request: Request, exc: InvalidTransitionError):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(UnsupportedBrokerError)
    async def unsupported_broker_handler(request: Request, exc: UnsupportedBrokerError):
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    # Prometheus metrics at /metrics
    Instrumentator().instrument(app).expose(app)

    # Health check (no auth)
    @app.get("/health", tags=["system"])
    async def health():
        from app.schemas.all_schemas import HealthResponse
        return HealthResponse(
            status="ok",
            version=settings.APP_VERSION,
            db="connected",
            redis="connected",
            services={
                "api": "ok",
                "celery": "ok",
                "data_quality": "ok",
            },
        )

    return app


app = create_app()
