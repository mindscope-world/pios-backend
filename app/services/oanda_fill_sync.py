# app/services/oanda_fill_sync.py
"""
Background broker↔app order reconciliation for OANDA (v20 REST).

OandaAdapter.submit_order reports MARKET fills inline (the orderFillTransaction
in the create response), but a LIMIT/STOP resting PENDING at OANDA fills
broker-side later. OANDA has no push channel wired here, so unlike MT5
(EA push + poll) this is poll-only: every OANDA_FILL_SYNC_INTERVAL_SECS,
open (SUBMITTED/PARTIAL) non-algo orders on OANDA brokers are re-read from
the broker via OandaAdapter.get_order and reconciled through the shared
core (broker_fill_sync.sync_order_against_broker — row lock, delta pricing,
post-commit WS nudge).

No connectivity gate like MT5's registry check: REST is always dialable, and
a credential failure just means get_order returns None and the order is
retried next pass. Under multiple workers every worker polls — the row lock
plus cumulative-delta logic make overlapping passes converge on the same
state (second reader sees filled_qty already advanced, delta 0).
"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.all_models import Broker, Order, OrderStatus
from app.services.broker_fill_sync import ALGO_TYPES, sync_order_against_broker

log = logging.getLogger(__name__)


async def sweep_once(source: str = "oanda-sync") -> int:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order.id)
            .join(Broker, Broker.id == Order.broker_id)
            .where(
                Order.status.in_((OrderStatus.SUBMITTED, OrderStatus.PARTIAL)),
                Order.broker_order_id.isnot(None),
                Order.order_type.notin_(ALGO_TYPES),
                Broker.broker_type == "OANDA",
            )
        )
        candidates = [row[0] for row in result.all()]

    synced = 0
    for order_id in candidates:
        try:
            await sync_order_against_broker(order_id, source=source)
            synced += 1
        except Exception as e:  # noqa: BLE001
            log.warning("oanda_fill_sync: order %s failed: %s", order_id, e)
    return synced


async def run_oanda_fill_sync() -> None:
    """Loop entrypoint — never raises; per-order errors are logged and the
    order is retried on the next pass."""
    interval = max(5, int(settings.OANDA_FILL_SYNC_INTERVAL_SECS))
    log.info("OANDA fill-sync loop started (every %ss)", interval)
    while True:
        try:
            await sweep_once()
        except asyncio.CancelledError:
            log.info("OANDA fill-sync loop stopped")
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("oanda_fill_sync: pass failed: %s", e)
        await asyncio.sleep(interval)
