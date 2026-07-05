# app/core/database.py
"""
Single async SQLAlchemy engine for the entire process.

Pool sizing for small EC2 (t3.small / t3.medium):
  pool_size=5      — persistent connections held open
  max_overflow=5   — burst connections (released after use)
  pool_timeout=30  — wait up to 30s for a connection before erroring
  pool_recycle=1800 — recycle connections every 30min (avoids stale TCP)

Total max DB connections = 10. Postgres default is 100, so this is safe
even with multiple worker processes on the same instance.
"""
from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,       # detect stale connections
            pool_size=5,
            max_overflow=5,
            pool_timeout=30,
            pool_recycle=1800,
            echo=False,               # set True to log SQL during debugging
        )
    return _engine


def get_session_factory() -> async_sessionmaker:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,   # avoid lazy-load issues after commit
        )
    return _session_factory


async def get_db() -> AsyncSession:
    """
    FastAPI dependency. Yields one session per request, auto-closes on exit.
    Usage:
        async def my_endpoint(db: AsyncSession = Depends(get_db)):
    """
    async with get_session_factory()() as session:
        yield session


async def dispose_engine() -> None:
    """Call at app shutdown to cleanly close all DB connections."""
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None