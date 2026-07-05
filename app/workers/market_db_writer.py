"""
Market Data Writer — main consumer loop.

Flow:
  Redis stream  →  DQ check  →  CandleAggregator
                              →  DQEvent buffer
                              →  flush to DB in batches

Kafka consumer (optional) feeds into the same DQ + aggregator pipeline.

EC2 sizing notes:
  - Single asyncio task; no threads
  - DB writes are batched (DQ_BATCH_SIZE ticks trigger one session)
  - Redis BLPOP blocks up to BLPOP_TIMEOUT seconds — near-zero CPU when quiet
  - Memory: ~5MB steady state for rolling windows + open candle buckets

Redis timeout notes:
  - socket_timeout on the client MUST be > BLPOP_TIMEOUT, otherwise the
    socket tears down before BLPOP returns naturally on an empty queue.
  - Recommended: socket_timeout = BLPOP_TIMEOUT + 2  (headroom for latency)
  - get_redis() sets socket_timeout=7 to match BLPOP_TIMEOUT=5.
"""
from __future__ import annotations

import asyncio
import json
import logging

from redis.exceptions import ConnectionError, TimeoutError, ResponseError

from app.core.config import settings
from app.workers.candle_aggregator import aggregator
from app.workers.dq_pipeline import dq
from app.workers.db_writer import flush_candles, flush_dq_events

log = logging.getLogger(__name__)

_BLPOP_TIMEOUT   = 5    # seconds BLPOP waits for an item before returning None
_REDIS_RECONNECT = 3    # seconds to wait before retrying after a Redis error


async def market_db_writer(redis) -> None:
    """
    Primary loop: drains the Redis `db_tick_buffer` list.
    Spawns Kafka consumer task if KAFKA_BOOTSTRAP_SERVERS is set.
    """
    if settings.kafka_enabled:
        asyncio.create_task(_kafka_consumer())

    completed_candles: list[dict] = []
    dq_events:         list[dict] = []
    tick_count = 0

    while True:
        try:
            item = await redis.blpop("db_tick_buffer", timeout=_BLPOP_TIMEOUT)

            if not item:
                # Normal BLPOP timeout — queue was empty; flush stale candles
                await _flush(completed_candles, dq_events)
                completed_candles.clear()
                dq_events.clear()
                tick_count = 0
                continue

            _, raw = item
            tick = json.loads(raw)

            if not tick.get("symbol_id"):
                log.debug(f"Dropped tick — no symbol_id: {tick}")
                continue

            # ── DQ check ──────────────────────────────────────
            dq_result, flags = dq.check(tick)
            tick["dq_result"] = dq_result
            tick["flags"]     = flags

            # ── Record DQ event if needed ──────────────────────
            if dq_result in ("FLAG", "REJECT"):
                dq_events.append(tick)

            # ── Feed valid ticks into candle aggregator ────────
            if dq_result != "REJECT":
                done = aggregator.ingest(tick)
                completed_candles.extend(done)

            # ── Batch flush ────────────────────────────────────
            tick_count += 1
            if tick_count >= settings.DQ_BATCH_SIZE:
                await _flush(completed_candles, dq_events)
                completed_candles.clear()
                dq_events.clear()
                tick_count = 0

        except asyncio.CancelledError:
            log.info("market_db_writer shutting down — flushing open candles")
            final = aggregator.flush_all()
            await flush_candles(final)
            raise

        # ── Redis connection/timeout errors ───────────────────
        # Raised when socket_timeout < BLPOP_TIMEOUT, or Redis restarts.
        # Log and retry — do not crash the writer task.
        except (TimeoutError, ConnectionError) as e:
            log.warning(
                f"market_db_writer Redis error: {e} — "
                f"retrying in {_REDIS_RECONNECT}s\n"
                "Tip: ensure socket_timeout > BLPOP_TIMEOUT in get_redis() "
                f"(socket_timeout should be >= {_BLPOP_TIMEOUT + 2})"
            )
            await asyncio.sleep(_REDIS_RECONNECT)

        except ResponseError as e:
            log.error(f"market_db_writer Redis response error: {e} — retrying in {_REDIS_RECONNECT}s")
            await asyncio.sleep(_REDIS_RECONNECT)

        except Exception as e:
            log.error(f"market_db_writer error: {e}", exc_info=True)
            await asyncio.sleep(1)


async def _flush(candles: list[dict], dq_events: list[dict]) -> None:
    """Run candle and DQ event flushes concurrently."""
    await asyncio.gather(
        flush_candles(candles),
        flush_dq_events(dq_events),
        return_exceptions=True,
    )


async def _kafka_consumer() -> None:
    """
    Optional Kafka consumer — mirrors Redis consumer logic.
    Feeds the same DQ pipeline and candle aggregator.
    Enabled only when KAFKA_BOOTSTRAP_SERVERS is set.
    """
    try:
        from aiokafka import AIOKafkaConsumer
    except ImportError:
        log.error("aiokafka not installed — Kafka consumer disabled")
        return

    consumer = AIOKafkaConsumer(
        settings.KAFKA_TICK_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    log.info(f"Kafka consumer started on {settings.KAFKA_BOOTSTRAP_SERVERS}")

    completed_candles: list[dict] = []
    dq_events:         list[dict] = []
    tick_count = 0

    try:
        async for msg in consumer:
            tick = msg.value
            if not tick.get("symbol_id"):
                continue

            dq_result, flags = dq.check(tick)
            tick["dq_result"] = dq_result
            tick["flags"]     = flags

            if dq_result in ("FLAG", "REJECT"):
                dq_events.append(tick)

            if dq_result != "REJECT":
                completed_candles.extend(aggregator.ingest(tick))

            tick_count += 1
            if tick_count >= settings.DQ_BATCH_SIZE:
                await _flush(completed_candles, dq_events)
                completed_candles.clear()
                dq_events.clear()
                tick_count = 0

    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()