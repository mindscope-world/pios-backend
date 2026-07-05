"""
DB Engine — async in-memory order registry.
Replace with SQLAlchemy + Postgres for production.

Index keys aligned to ORM:
  primary:          id          (uuid.UUID)
  unique secondary: client_order_id (str | None)
  secondary:        broker_order_id (str | None)
  secondary:        user_id     (uuid.UUID)
  secondary:        nonce       (str, bridge-only)
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import UUID

from app.models.all_models import Order, OrderStatus


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


class OrderStore:
    def __init__(self):
        self._orders:         Dict[UUID, Order]       = {}
        self._by_nonce:       Dict[str, UUID]         = {}   # nonce → id
        self._by_user:        Dict[UUID, List[UUID]]  = defaultdict(list)
        self._by_client_oid:  Dict[str, UUID]         = {}   # client_order_id → id
        self._lock = asyncio.Lock()

    # ── Write ──────────────────────────────────────────────────────────────

    async def create(self, order: Order) -> Order:
        async with self._lock:
            self._orders[order.id] = order
            self._by_nonce[order.nonce] = order.id
            self._by_user[order.user_id].append(order.id)
            if order.client_order_id:
                self._by_client_oid[order.client_order_id] = order.id
        return order

    async def update_status(
        self,
        order_id: UUID,
        status: str,
        **kwargs,
    ) -> Optional[Order]:
        async with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                return None
            updated = order.model_copy(update={
                "status":     status,
                "updated_at": _now(),
                **kwargs,
            })
            # Keep state_history if caller passed transition() result instead
            self._orders[order_id] = updated
            return updated

    async def update_by_nonce(
        self,
        nonce: str,
        status: str,
        **kwargs,
    ) -> Optional[Order]:
        order_id = self._by_nonce.get(nonce)
        if order_id is None:
            return None
        return await self.update_status(order_id, status, **kwargs)

    async def save(self, order: Order) -> Order:
        """Persist a fully-mutated Order instance (e.g. after transition())."""
        async with self._lock:
            self._orders[order.id] = order
        return order

    # ── Read ───────────────────────────────────────────────────────────────

    async def get(self, order_id: UUID) -> Optional[Order]:
        return self._orders.get(order_id)

    async def get_by_nonce(self, nonce: str) -> Optional[Order]:
        oid = self._by_nonce.get(nonce)
        return self._orders.get(oid) if oid else None

    async def get_by_client_order_id(self, client_order_id: str) -> Optional[Order]:
        oid = self._by_client_oid.get(client_order_id)
        return self._orders.get(oid) if oid else None

    async def list_for_user(self, user_id: UUID, limit: int = 100) -> List[Order]:
        ids = self._by_user.get(user_id, [])[-limit:]
        return [self._orders[i] for i in ids if i in self._orders]

    async def all_orders(self) -> List[Order]:
        return list(self._orders.values())


# Singleton
order_store = OrderStore()