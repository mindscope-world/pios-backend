"""
OrderExecutionService
=====================
Orchestrates the full order lifecycle:
  1. Risk Gate allocates order as PENDING
  2. BrokerRouter routes to correct adapter (awaits nonce Future)
  3. DB Engine mutates state to FILLED / REJECTED
  4. WSManager broadcasts event to all subscribed UI widgets
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID

from app.db.order_store import order_store
from app.models.all_models import Order, OrderEvent, OrderStatus, PlaceOrderRequest
from app.services.brokers.broker_router import broker_router
from app.services.brokers.risk_gate import risk_gate
from app.services.websocket.manager import ws_manager

log = logging.getLogger(__name__)

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)

class OrderExecutionService:

    async def submit(
        self,
        req: PlaceOrderRequest,
        user_id: UUID,
    ) -> tuple[Order, str | None]:
        """
        Full pipeline: Risk Gate → Broker Router → DB mutation → WS fan-out.
        Returns (order, error_message).
        """

        # ── 1. Pre-trade risk gate — allocates as NEW ─────────────────────
        order, rejection = await risk_gate.allocate(req, user_id)

        if order is None:
            # Synthetic rejected order for WS broadcast (not persisted)
            rejected = Order(
                user_id    = user_id,
                broker_id  = req.broker_id,
                symbol_id  = req.symbol_id,
                symbol     = req.symbol,
                side       = req.side,
                order_type = req.order_type,
                qty        = req.qty,
                status     = OrderStatus.REJECTED,
                reject_reason = rejection,
                risk_check = {"passed": False, "reject_reason": rejection},
            )
            await ws_manager.publish_order_event(user_id, {
                "type":   "order_rejected",
                "order":  rejected.model_dump(mode="json"),
                "reason": rejection,
            })
            return rejected, rejection

        # Broadcast NEW
        await ws_manager.publish_order_event(user_id, {
            "type":  "order_new",
            "order": order.model_dump(mode="json"),
        })

        # ── 2. Transition to SUBMITTED ────────────────────────────────────
        order = order.transition(OrderStatus.SUBMITTED)
        order = order.model_copy(update={"submitted_at": _now()})
        await order_store.save(order)

        await ws_manager.publish_order_event(user_id, {
            "type":  "order_submitted",
            "order": order.model_dump(mode="json"),
        })

        # ── 3. Route to broker adapter (awaits nonce Future) ──────────────
        result = await broker_router.route_order(order)

        # ── 4. Mutate DB state ────────────────────────────────────────────
        if result.success:
            order = order.transition(OrderStatus.FILLED)
            order = order.model_copy(update={
                "broker_order_id": result.broker_order_id,
                "avg_fill_price":  result.avg_fill_price,
                "filled_qty":      result.filled_qty or order.qty,
                "filled_at":       _now(),
            })
            event_type = "order_filled"
            error_msg  = None
        else:
            order = order.transition(OrderStatus.REJECTED, reason=result.error_message)
            order = order.model_copy(update={"reject_reason": result.error_message})
            event_type = "order_rejected"
            error_msg  = result.error_message

        await order_store.save(order)

        log.info(
            "Order %s → %s  broker_order_id=%s  avg_fill_price=%s",
            order.id, order.status,
            result.broker_order_id, result.avg_fill_price,
        )

        # ── 5. Fan-out to subscribed UI widgets ───────────────────────────
        await ws_manager.publish_order_event(user_id, {
            "type":            event_type,
            "order":           order.model_dump(mode="json"),
            "avg_fill_price":  result.avg_fill_price,
            "broker_order_id": result.broker_order_id,
            "error":           result.error_message,
        })

        return order, error_msg


# Singleton
execution_service = OrderExecutionService()