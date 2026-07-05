"""
Pre-Trade Risk Gate
===================
Validates every order before it touches the broker wire.
Allocates the order as NEW (matching ORM default) in the DB if it passes.

risk_check dict written to Order.risk_check matches the ORM JSONB column.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID

from app.db.order_store import order_store
from app.models.all_models import Order, OrderStatus, PlaceOrderRequest

log = logging.getLogger(__name__)

RISK_CONFIG = {
    "max_qty":                  100.0,
    "max_notional_usd":         10_000_000,
    "max_open_orders_per_user": 50,
    "symbol_whitelist":         None,   # None = allow all
}


# ── Rule interface ────────────────────────────────────────────────────────────

class RiskRule(ABC):
    @abstractmethod
    async def check(self, req: PlaceOrderRequest, user_id: UUID) -> Optional[str]:
        """Return rejection reason string, or None if passes."""


class MaxQtyRule(RiskRule):
    async def check(self, req: PlaceOrderRequest, user_id: UUID) -> Optional[str]:
        if req.qty > RISK_CONFIG["max_qty"]:
            return f"qty {req.qty} exceeds max {RISK_CONFIG['max_qty']}"
        return None


class SymbolWhitelistRule(RiskRule):
    async def check(self, req: PlaceOrderRequest, user_id: UUID) -> Optional[str]:
        wl = RISK_CONFIG["symbol_whitelist"]
        if wl and req.symbol not in wl:
            return f"Symbol {req.symbol} not in whitelist"
        return None


class MaxOpenOrdersRule(RiskRule):
    async def check(self, req: PlaceOrderRequest, user_id: UUID) -> Optional[str]:
        orders = await order_store.list_for_user(user_id)
        open_count = sum(1 for o in orders if o.status in OrderStatus.OPEN)
        limit = RISK_CONFIG["max_open_orders_per_user"]
        if open_count >= limit:
            return f"Max open orders ({limit}) reached"
        return None


class IdempotencyRule(RiskRule):
    async def check(self, req: PlaceOrderRequest, user_id: UUID) -> Optional[str]:
        if req.client_order_id is None:
            return None
        existing = await order_store.get_by_client_order_id(req.client_order_id)
        if existing:
            return f"Duplicate client_order_id {req.client_order_id} (order {existing.id})"
        return None


# ── Gate orchestrator ─────────────────────────────────────────────────────────

class PreTradeRiskGate:
    def __init__(self):
        self._rules: List[RiskRule] = [
            SymbolWhitelistRule(),
            MaxQtyRule(),
            MaxOpenOrdersRule(),
            IdempotencyRule(),
        ]

    async def evaluate(
        self,
        req: PlaceOrderRequest,
        user_id: UUID,
    ) -> tuple[bool, Optional[str]]:
        for rule in self._rules:
            reason = await rule.check(req, user_id)
            if reason:
                log.warning("Risk gate blocked %s for user %s: %s", req.symbol, user_id, reason)
                return False, reason
        return True, None

    async def allocate(
        self,
        req: PlaceOrderRequest,
        user_id: UUID,
    ) -> tuple[Optional[Order], Optional[str]]:
        """
        Run risk checks then write a NEW order to the DB.
        Returns (order, None) on success or (None, reason) on rejection.

        risk_check snapshot is stored in Order.risk_check (JSONB).
        """
        passed, reason = await self.evaluate(req, user_id)

        risk_snapshot = {
            "passed":        passed,
            "rules_checked": [r.__class__.__name__ for r in self._rules],
            "reject_reason": reason,
            "evaluated_at":  datetime.now(tz=timezone.utc).isoformat(),
        }

        if not passed:
            return None, reason

        order = Order(
            client_order_id = req.client_order_id,
            user_id         = user_id,
            broker_id       = req.broker_id,
            strategy_id     = req.strategy_id,
            symbol_id       = req.symbol_id,
            symbol          = req.symbol,         # bridge-only field
            side            = req.side,
            order_type      = req.order_type,
            time_in_force   = req.time_in_force,
            qty             = req.qty,
            price           = req.price,
            stop_price      = req.stop_price,
            status          = OrderStatus.NEW,
            algo_config     = {"magic": req.magic, "comment": req.comment},
            risk_check      = risk_snapshot,
        )
        await order_store.create(order)
        log.info(
            "Allocated NEW order %s (%s %s x%s)",
            order.id, order.side, order.symbol, order.qty,
        )
        return order, None


# Singleton
risk_gate = PreTradeRiskGate()