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
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
import structlog

from app.core.config import settings
from app.api.v1.router import api_router
from app.core.pubsub import start_redis_listener
from app.db.session import engine, Base
from app.models.all_models import InvalidTransitionError

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────────────────────
    logger.info("Pi OS API starting", version=settings.APP_VERSION, env=settings.ENVIRONMENT)

    # Start Redis pub/sub listener in background
    # ✅ Store task reference to prevent garbage collection
    app.state.redis_listener = asyncio.create_task(start_redis_listener())
    logger.info("Redis listener started")

    logger.info("Pi OS API ready")
    yield

    # ── Shutdown ──────────────────────────────────────────────────────────────
    app.state.redis_listener.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await app.state.redis_listener
        
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
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # Routes
    app.include_router(api_router, prefix="/api/v1")

    @app.exception_handler(InvalidTransitionError)
    async def invalid_transition_handler(request: Request, exc: InvalidTransitionError):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

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
