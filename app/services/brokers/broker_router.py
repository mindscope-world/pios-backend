"""
BrokerConnectionRouter — Active Connection Matrix
==================================================
Routes orders to the correct broker adapter.
Key: (broker_type: str, broker_id: uuid.UUID)

The broker_id comes from the Broker ORM row — the same UUID stored in
Order.broker_id.  The router looks up the live adapter by that key.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, Dict, Optional, Tuple
from uuid import UUID

import websockets

from app.services.brokers.mt5.adapter import MT5BridgeAdapter
from app.models.all_models import BrokerType, ExecutionResult, Order

log = logging.getLogger(__name__)


class BrokerAdapter:
    """Abstract interface all broker adapters must satisfy."""
    async def execute_order(self, order: Order, timeout: float = 10.0) -> ExecutionResult:
        raise NotImplementedError

    async def cancel_order(self, broker_order_id: str, nonce: str, timeout: float = 10.0) -> ExecutionResult:
        raise NotImplementedError

    @property
    def is_connected(self) -> bool:
        return False


class BrokerConnectionRouter:
    """
    Keyed by (broker_type_str, broker_id_str).
    broker_id_str is the str() of the Broker.id UUID so it survives
    JSON round-trips in the HANDSHAKE frame.
    """

    def __init__(
        self,
        on_tick: Optional[Callable[[dict], None]] = None,
        on_position_update: Optional[Callable[[dict], None]] = None,
    ):
        self._connections: Dict[Tuple[str, str], BrokerAdapter] = {}
        self._on_tick = on_tick
        self._on_position_update = on_position_update
        self._lock = asyncio.Lock()

    # ── Routing ───────────────────────────────────────────────────────────────

    def _key(self, broker_type: str, broker_id: UUID) -> Tuple[str, str]:
        return (broker_type, str(broker_id))

    def get_adapter(self, broker_type: str, broker_id: UUID) -> Optional[BrokerAdapter]:
        return self._connections.get(self._key(broker_type, broker_id))

    async def route_order(self, order: Order) -> ExecutionResult:
        # Resolve broker_type from order.algo_config or default to MT5
        broker_type = (order.algo_config or {}).get("broker_type", BrokerType.MT5)
        adapter = self.get_adapter(broker_type, order.broker_id)
        if adapter is None:
            return ExecutionResult(
                success=False,
                error_message=(
                    f"No active connection for broker_type={broker_type} "
                    f"broker_id={order.broker_id}"
                ),
            )
        if not adapter.is_connected:
            return ExecutionResult(
                success=False,
                error_message=f"Broker {order.broker_id} not connected",
            )
        return await adapter.execute_order(order)

    # ── MT5 EA WebSocket handler ──────────────────────────────────────────────

    async def handle_mt5_connection(self, ws) -> None:
        """
        EA sends HANDSHAKE with broker_id (UUID string from the Broker row).
        """
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            handshake = json.loads(raw)
        except (asyncio.TimeoutError, json.JSONDecodeError) as exc:
            log.error("MT5 handshake failed: %s", exc)
            await ws.close()
            return

        if handshake.get("type") != "HANDSHAKE":
            log.error("Expected HANDSHAKE, got: %s", handshake.get("type"))
            await ws.close()
            return

        # broker_id is the UUID string from Broker.id
        broker_id_str = handshake.get("broker_id") or handshake.get("account_id", "")
        try:
            broker_id = UUID(broker_id_str)
        except (ValueError, AttributeError):
            log.error("Invalid broker_id in HANDSHAKE: %r", broker_id_str)
            await ws.close()
            return

        key = self._key(BrokerType.MT5, broker_id)

        async with self._lock:
            adapter = MT5BridgeAdapter(
                account_id         = broker_id_str,
                on_tick            = self._on_tick,
                on_position_update = self._on_position_update,
            )
            self._connections[key] = adapter

        log.info("MT5 adapter registered for broker_id=%s", broker_id_str)
        await adapter.attach(ws)

        if adapter._receive_task:
            try:
                await adapter._receive_task
            except Exception:
                pass

        async with self._lock:
            self._connections.pop(key, None)

    # ── Status ────────────────────────────────────────────────────────────────

    def connection_status(self) -> dict:
        return {
            f"{btype}/{bid}": adapter.is_connected
            for (btype, bid), adapter in self._connections.items()
        }


# Singleton
broker_router = BrokerConnectionRouter()