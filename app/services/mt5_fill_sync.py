# app/services/mt5_fill_sync.py
"""
Background broker↔app order reconciliation for MT5 (EA bridge).

MT5Adapter.submit_order reports MARKET fills inline (the EA's ORDER_RESULT
carries the deal price), but a LIMIT/STOP that rests as a pending order in
the terminal comes back SUBMITTED — and previously stayed SUBMITTED in the
app forever after MT5 triggered it, because the adapter had no fill-sync
path. Two paths close that gap, mirroring the Alpaca model (trade-update
stream + poll safety net):

  push — the EA's OnTradeTransaction pushes an unsolicited ORDER_UPDATE
         frame the moment a deal executes against one of its tickets (or a
         pending order dies broker-side: cancelled/expired/rejected). It
         carries the actual print price/volume, so the Fill row records the
         real execution price. MT5Connection._dispatch routes those frames
         to handle_order_update() below.
  poll — every MT5_FILL_SYNC_INTERVAL_SECS this loop scans open
         (SUBMITTED/PARTIAL) non-algo orders on MT5 brokers whose EA is
         currently connected and asks the terminal for each ticket's state
         (GET_ORDER → ORDER_STATUS). This catches whatever the push missed:
         fills that landed while the EA was reconnecting, or an ORDER_UPDATE
         that raced the submit path before broker_order_id was committed.

The poll fallback has no per-execution feed, but MT5's ORDER_STATUS reply
carries the cumulative filled qty and average price, so the newly-filled
delta's true average is backed out from the two cumulative notionals rather
than blended at the broker's running average.

The reconciliation core (row lock, delta pricing, post-commit WS nudge)
lives in broker_fill_sync.sync_order_against_broker — shared with
oanda_fill_sync. Algo orders (TWAP/VWAP/ICEBERG) are excluded — their
slices are marketable and their parent status belongs to execution_algo.py.
"""
from __future__ import annotations

import asyncio
import logging
import uuid

from sqlalchemy import select

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.all_models import Broker, Order, OrderStatus
from app.services.broker_fill_sync import ALGO_TYPES, sync_order_against_broker
from app.services.brokers.mt5.adapter import mt5_registry

log = logging.getLogger(__name__)


async def _find_open_order_id(broker_id: str, ticket: str):
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order.id).where(
                Order.broker_id == uuid.UUID(broker_id),
                Order.broker_order_id == str(ticket),
                Order.status.in_((OrderStatus.SUBMITTED, OrderStatus.PARTIAL)),
            )
        )
        return result.scalars().first()


async def handle_order_update(broker_id: str, msg: dict) -> None:
    """One unsolicited EA ORDER_UPDATE frame → the reconciliation core.

    Called from MT5Connection._dispatch (as a task — dispatch is sync).
    Unknown tickets are dropped: manual terminal trades and other-EA orders
    legitimately produce events for orders this app never placed.
    """
    ticket = str(msg.get("ticket") or "")
    if not ticket:
        return

    order_id = await _find_open_order_id(broker_id, ticket)
    if order_id is None:
        # The EA fires OnTradeTransaction the instant a marketable LIMIT
        # executes — sometimes before order_service has committed
        # broker_order_id. One short retry covers that race; anything still
        # unmatched is a foreign ticket (or already terminal) and the poll
        # loop remains the safety net regardless.
        await asyncio.sleep(2.0)
        order_id = await _find_open_order_id(broker_id, ticket)
        if order_id is None:
            log.debug("[MT5:%s] ORDER_UPDATE for unknown ticket %s ignored", broker_id, ticket)
            return

    broker_state = {
        "status":     str(msg.get("status") or "").upper(),
        "filled_qty": msg.get("filled_qty"),
        "avg_price":  msg.get("avg_fill_price"),
    }
    execution = None
    if float(msg.get("fill_qty") or 0) > 0 and float(msg.get("fill_price") or 0) > 0:
        execution = {"qty": float(msg["fill_qty"]), "price": float(msg["fill_price"])}
    await sync_order_against_broker(
        order_id, broker_state=broker_state, source="EA push", execution=execution
    )


async def sweep_once(source: str = "fill-sync") -> int:
    """One reconciliation pass over every open MT5 order whose EA is
    currently connected. Disconnected terminals are skipped silently —
    an offline EA is a normal state, not an error, and the orders are
    picked up as soon as it reconnects."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order.id, Order.broker_id)
            .join(Broker, Broker.id == Order.broker_id)
            .where(
                Order.status.in_((OrderStatus.SUBMITTED, OrderStatus.PARTIAL)),
                Order.broker_order_id.isnot(None),
                Order.order_type.notin_(ALGO_TYPES),
                Broker.broker_type == "MT5",
            )
        )
        candidates = result.all()

    synced = 0
    for order_id, broker_id in candidates:
        # Deliberately the LOCAL registry, not ea_connected_anywhere(): every
        # worker runs this loop, and gating on the local socket makes exactly
        # one worker (the socket holder) poll each EA — relaying from all of
        # them would just multiply GET_ORDER traffic at the terminal.
        conn = mt5_registry.get(str(broker_id))
        if conn is None or not conn.is_connected:
            continue
        try:
            await sync_order_against_broker(order_id, source=source)
            synced += 1
        except Exception as e:  # noqa: BLE001
            log.warning("mt5_fill_sync: order %s failed: %s", order_id, e)
    return synced


async def run_mt5_fill_sync() -> None:
    """Loop entrypoint — never raises; per-order errors are logged and the
    order is retried on the next pass."""
    interval = max(5, int(settings.MT5_FILL_SYNC_INTERVAL_SECS))
    log.info("MT5 fill-sync loop started (every %ss)", interval)
    while True:
        try:
            await sweep_once()
        except asyncio.CancelledError:
            log.info("MT5 fill-sync loop stopped")
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("mt5_fill_sync: pass failed: %s", e)
        await asyncio.sleep(interval)
