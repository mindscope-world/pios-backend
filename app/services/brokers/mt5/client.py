import asyncio
import json
from fastapi import WebSocket
from typing import Dict
from services.brokers.mt5.models import MT5OrderRequest, MT5GatewayResponse

class MT5BridgeRegistry:
    def __init__(self):
        # Maps user_id -> active EA WebSocket connections
        self.active_bridges: Dict[str, WebSocket] = {}
        # Tracks pending orders awaiting an execution reply from the terminal: ticket_id -> Future
        self.pending_executions: Dict[str, asyncio.Future] = {}

    async def register_bridge(self, user_id: str, ws: WebSocket):
        self.active_bridges[user_id] = ws
        print(f"📡 MT5 EA Execution Bridge Linked: user_id={user_id}")

    def unregister_bridge(self, user_id: str):
        if user_id in self.active_bridges:
            del self.active_bridges[user_id]
            print(f"🛑 MT5 EA Execution Bridge Unlinked: user_id={user_id}")

    async def send_order_to_ea(self, user_id: str, client_order_id: str, order: MT5OrderRequest) -> MT5GatewayResponse:
        """
        Routes execution instructions downstream to the listening Expert Advisor.
        """
        ws = self.active_bridges.get(user_id)
        if not ws:
            return MT5GatewayResponse(status="FAILED", comment="Expert Advisor bridge offline.")

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.pending_executions[client_order_id] = future

        # Construct payload targeting the EA wrapper script
        payload = {
            "command": "EXECUTE_ORDER",
            "client_order_id": client_order_id,
            "details": order.model_dump()
        }

        try:
            await ws.send_json(payload)
            # Await execution callback from the MT5 Terminal via the EA with a 15-second timeout
            response_data = await asyncio.wait_for(future, timeout=15.0)
            return MT5GatewayResponse(**response_data)
        except asyncio.TimeoutError:
            return MT5GatewayResponse(status="FAILED", comment="Execution timeout from MT5 terminal.")
        finally:
            self.pending_executions.pop(client_order_id, None)

    def resolve_execution(self, client_order_id: str, data: dict):
        """
        Invoked when the incoming WebSocket payload processes a transaction result.
        """
        future = self.pending_executions.get(client_order_id)
        if future and not future.done():
            future.set_result(data)

# Global broker bridge instance
mt5_bridge_registry = MT5BridgeRegistry()