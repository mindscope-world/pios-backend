import asyncio
import logging

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from app.core.config import settings
from app.core.redis import get_redis

from app.workers.orchestrator import run_market_workers


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingestor")


async def main():
    logger.info("🚀 Starting Market Ingestion Worker...")

    # ─────────────────────────────────────────────
    # CORE INFRASTRUCTURE
    # ─────────────────────────────────────────────
    engine = create_async_engine(
        settings.DATABASE_URL,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )

    redis = get_redis()

    # ─────────────────────────────────────────────
    # DB SESSION
    # ─────────────────────────────────────────────
    async with AsyncSession(engine) as session:
        try:
            await run_market_workers(session, redis)

        except asyncio.CancelledError:
            logger.warning("🛑 Worker shutdown requested")
            raise

        except Exception as e:
            logger.exception(f"❌ Fatal worker error: {e}")

        finally:
            await engine.dispose()
            logger.info("🧹 DB engine disposed")


# ─────────────────────────────────────────────────────────────
# CONTAINER ENTRYPOINT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("🛑 Worker stopped manually")