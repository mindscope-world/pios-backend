# app/services/publisher.py
"""
Market tick publisher.
HOT PATH:  Redis pub/sub  (real-time WebSocket consumers)
COLD PATH: Redis list     (DB writer buffer)
KAFKA:     Optional Kafka producer (set KAFKA_BOOTSTRAP_SERVERS in .env)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger(__name__)

KAFKA_ENABLED = bool(os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
_kafka_producer = None


class Publisher:
    def __init__(self, redis):
        self.redis = redis
        self._kafka = None

    async def _get_kafka(self):
        global _kafka_producer
        if not KAFKA_ENABLED:
            return None
        if _kafka_producer is None:
            try:
                from aiokafka import AIOKafkaProducer
                _kafka_producer = AIOKafkaProducer(
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    compression_type="gzip",
                    linger_ms=5,        # micro-batch for throughput
                    acks="1",           # leader ack — balances latency/durability
                )
                await _kafka_producer.start()
                log.info("Kafka producer started")
            except Exception as e:
                log.warning(f"Kafka producer init failed: {e}")
                _kafka_producer = None
        return _kafka_producer

    async def publish(self, tick: dict):
        # Ensure time field is set
        if "time" not in tick:
            tick["time"] = datetime.now(timezone.utc).isoformat()

        payload = json.dumps(tick)

        # HOT PATH — Redis pub/sub (WebSocket subscribers, dashboard live feeds)
        try:
            await self.redis.publish("market_ticks", payload)
        except Exception as e:
            log.error(f"Redis publish error: {e}")

        # COLD PATH — Redis list (DB writer batch buffer)
        try:
            await self.redis.rpush("db_tick_buffer", payload)
        except Exception as e:
            log.error(f"Redis rpush error: {e}")

        # KAFKA PATH — optional durable event log
        if KAFKA_ENABLED:
            try:
                producer = await self._get_kafka()
                if producer:
                    topic = os.getenv("KAFKA_TICK_TOPIC", "market.ticks.raw")
                    sym_key = str(tick.get("symbol", tick.get("symbol_id", "unknown")))
                    await producer.send(topic, value=tick, key=sym_key.encode())
            except Exception as e:
                log.warning(f"Kafka publish error: {e}")

    async def close(self):
        global _kafka_producer
        if _kafka_producer:
            try:
                await _kafka_producer.stop()
            except Exception:
                pass
            _kafka_producer = None
