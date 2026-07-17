"""
MT5 broker bridge.
===================
MT5 has no public REST/WS trading API -- execution happens through an
Expert Advisor (EA) running inside the trader's MT5 terminal, which opens a
WebSocket connection *to us* (see app/api/v1/endpoints/mt5_bridge.py) and
stays connected for the life of the terminal session. That's the reverse of
every other adapter in broker_service.py, which dials out to a broker's
REST API per call -- there's nothing to "connect to" here, only a
connection to wait for.

Three pieces:
  MT5Connection     -- one live EA WebSocket, keyed by broker_id. Owns the
                       correlation-id/future bookkeeping used to match a
                       PLACE_ORDER request to its async ORDER_RESULT reply.
  MT5BridgeRegistry -- process-wide singleton mapping broker_id (str) to
                       MT5Connection. The WS endpoint registers/unregisters
                       connections here; MT5Adapter looks them up here.
  MT5Adapter        -- duck-types broker_service.BrokerAdapter (not a
                       subclass -- see broker_service.py for why). Built
                       fresh per call like every other adapter, but every
                       method delegates to the shared registry for the
                       actual long-lived, EA-initiated connection.

KNOWN DEPLOYMENT CONSTRAINT: mt5_registry is an in-process singleton, not
backed by Redis like the app's other cross-worker fan-out (see "Redis
listener" at startup). Dockerfile runs `uvicorn --workers 2` -- an EA
connecting via /ws/mt5/{broker_id} lands on one worker's registry, and an
order HTTP request handled by the *other* worker will see "EA not
connected" even though it genuinely is, just to a sibling process. This
fails safe (a clear rejection, not a hang or corrupted order) but is not
correct under >1 worker without either sticky routing on broker_id at the
LB/ingress layer, or relaying PLACE_ORDER/ORDER_RESULT through Redis
pub/sub the way the rest of the app already does. Pin MT5 traffic to a
single worker (or a dedicated process) until that relay exists.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from app.models.all_models import Broker, ExecutionResult, Order
from app.schemas.all_schemas import BrokerTestResult

log = logging.getLogger(__name__)

# Fire-and-forget ORDER_UPDATE handler tasks — referenced here so the event
# loop can't garbage-collect them mid-flight (same discipline as main.py's
# app.state task refs).
_push_tasks: set = set()


def _order_to_ea_payload(order: Order, correlation_id: str) -> dict:
    """Convert an Order (or execution_algo._SliceOrder) to the EA wire format."""
    algo = order.algo_config or {}
    payload: Dict[str, Any] = {
        "type":           "PLACE_ORDER",
        "correlation_id": correlation_id,
        "order_id":       str(order.id),
        "symbol":         order.symbol.symbol,
        "action":         order.side,        # "BUY" | "SELL"
        "order_type":     order.order_type,  # "MARKET" | "LIMIT" | ...
        "volume":         order.qty,         # MT5 calls it volume
        "magic":          algo.get("magic", 0),
        "comment":        algo.get("comment", ""),
    }
    if order.price is not None:
        payload["price"] = order.price
    if order.stop_price is not None:
        payload["stop_price"] = order.stop_price
    return payload


def _parse_result(raw: dict) -> ExecutionResult:
    if raw.get("success"):
        return ExecutionResult(
            success         = True,
            broker_order_id = str(raw["ticket"]) if raw.get("ticket") else None,
            avg_fill_price  = raw.get("avg_fill_price") or raw.get("fill_price"),
            filled_qty      = raw.get("filled_qty") or raw.get("fill_volume"),
            commission      = raw.get("commission"),
            raw             = raw,
        )
    return ExecutionResult(
        success       = False,
        error_code    = raw.get("error_code"),
        error_message = raw.get("error_message", "Unknown error from EA"),
        raw           = raw,
    )


class MT5Connection:
    """One live EA WebSocket for a single broker_id."""

    def __init__(self, broker_id: str, ws: WebSocket):
        self.broker_id = broker_id
        self._ws = ws
        self._pending: Dict[str, asyncio.Future] = {}
        self._connected = True
        self.connected_at = datetime.now(timezone.utc)

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def send_request(self, payload: dict, timeout: float = 10.0) -> dict:
        if not self._connected:
            raise ConnectionError(f"MT5 EA disconnected for broker {self.broker_id}")
        correlation_id = payload["correlation_id"]
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[correlation_id] = fut
        try:
            await self._ws.send_json(payload)
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending.pop(correlation_id, None)

    async def receive_loop(self) -> None:
        """Drains inbound EA frames until the socket closes. Run as the
        WS endpoint's main loop -- resolves pending futures as replies land."""
        try:
            while True:
                msg = await self._ws.receive_json()
                self._dispatch(msg)
        except WebSocketDisconnect:
            pass
        finally:
            self._connected = False
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("MT5 EA disconnected"))
            self._pending.clear()

    def _dispatch(self, msg: dict) -> None:
        msg_type = msg.get("type")
        if msg_type in ("ORDER_RESULT", "CANCEL_RESULT", "ACCOUNT_INFO", "POSITIONS",
                        "ORDER_STATUS", "PONG"):
            correlation_id = msg.get("correlation_id")
            fut = self._pending.get(correlation_id)
            if fut and not fut.done():
                fut.set_result(msg)
        elif msg_type == "ORDER_UPDATE":
            # Unsolicited push from the EA's OnTradeTransaction — a pending
            # order filled (real print price inline) or died broker-side.
            # Imported lazily: mt5_fill_sync imports this module at top level.
            from app.services.mt5_fill_sync import handle_order_update
            task = asyncio.create_task(handle_order_update(self.broker_id, msg))
            _push_tasks.add(task)
            task.add_done_callback(_push_tasks.discard)
        else:
            log.warning("[MT5:%s] Unknown message type: %s", self.broker_id, msg_type)


class MT5BridgeRegistry:
    """Process-wide singleton: broker_id (str) -> MT5Connection."""

    def __init__(self):
        self._connections: Dict[str, MT5Connection] = {}

    def get(self, broker_id: str) -> Optional[MT5Connection]:
        return self._connections.get(broker_id)

    async def register(self, broker_id: str, ws: WebSocket) -> MT5Connection:
        conn = MT5Connection(broker_id, ws)
        self._connections[broker_id] = conn
        log.info("[MT5:%s] EA connected", broker_id)
        return conn

    def unregister(self, broker_id: str) -> None:
        if self._connections.pop(broker_id, None) is not None:
            log.info("[MT5:%s] EA disconnected", broker_id)

    def status(self) -> dict:
        return {bid: conn.is_connected for bid, conn in self._connections.items()}


mt5_registry = MT5BridgeRegistry()


class MT5Adapter:
    """
    Every call here depends on an EA already being connected via
    /ws/mt5/{broker_id} -- there's no way to "dial out" to a MetaTrader
    terminal. Calls made while disconnected fail fast with a clear message
    instead of hanging until an HTTP timeout.
    """

    def __init__(self, broker: Broker, credentials: dict):
        self.broker = broker
        self.creds = credentials

    def _connection(self) -> MT5Connection:
        conn = mt5_registry.get(str(self.broker.id))
        if conn is None or not conn.is_connected:
            raise ConnectionError(
                f"MT5 EA not connected for broker {self.broker.id} -- "
                f"pair the terminal via /ws/mt5/{self.broker.id} first."
            )
        return conn

    async def test_connection(self) -> BrokerTestResult:
        conn = mt5_registry.get(str(self.broker.id))
        if conn is None or not conn.is_connected:
            return BrokerTestResult(success=False, latency_ms=None, message="MT5 EA not connected")
        t0 = time.perf_counter()
        try:
            await conn.send_request({"type": "PING", "correlation_id": f"ping-{t0}"}, timeout=5.0)
        except (asyncio.TimeoutError, ConnectionError):
            pass  # EA may not echo a correlated PONG -- absence isn't fatal, the socket is up
        latency = (time.perf_counter() - t0) * 1000
        return BrokerTestResult(success=True, latency_ms=round(latency, 2), message="EA connected")

    async def get_account(self) -> dict:
        conn = self._connection()
        corr = f"acct-{time.time()}"
        try:
            return await conn.send_request({"type": "GET_ACCOUNT", "correlation_id": corr})
        except asyncio.TimeoutError:
            return {}

    async def submit_order(self, order: Order) -> dict:
        conn = self._connection()
        correlation_id = order.client_order_id or str(order.id)
        payload = _order_to_ea_payload(order, correlation_id)
        try:
            raw = await conn.send_request(payload, timeout=10.0)
        except asyncio.TimeoutError:
            result = ExecutionResult(success=False, error_message="MT5 response timeout")
        else:
            result = _parse_result(raw)

        if not result.success:
            raise ConnectionError(result.error_message or "MT5 order rejected")
        return {
            "broker_order_id": result.broker_order_id,
            "status":          "FILLED" if result.filled_qty else "SUBMITTED",
            "avg_price":       result.avg_fill_price,
        }

    async def cancel_order(self, broker_order_id: str) -> dict:
        conn = self._connection()
        payload = {
            "type":             "CANCEL_ORDER",
            "correlation_id":   f"cancel-{broker_order_id}",
            "broker_order_id":  broker_order_id,
        }
        try:
            raw = await conn.send_request(payload, timeout=10.0)
        except asyncio.TimeoutError:
            return {"status": "CANCEL_TIMEOUT"}
        result = _parse_result(raw)
        return {"status": "CANCELLED" if result.success else "CANCEL_FAILED"}

    async def get_positions(self) -> list[dict]:
        conn = self._connection()
        try:
            result = await conn.send_request({"type": "GET_POSITIONS", "correlation_id": f"pos-{time.time()}"})
        except asyncio.TimeoutError:
            return []
        return result.get("positions", [])

    async def get_order(self, broker_order_id: str) -> Optional[dict]:
        """Current state of one ticket, in the shape mt5_fill_sync expects:
        {status, filled_qty, avg_price}. Status is app-vocabulary (SUBMITTED/
        PARTIAL/FILLED/CANCELLED/EXPIRED/REJECTED, or UNKNOWN when the EA
        can't place the ticket). None on timeout — the sync loop retries."""
        conn = self._connection()
        payload = {
            "type":            "GET_ORDER",
            "correlation_id":  f"stat-{broker_order_id}-{time.time()}",
            "broker_order_id": broker_order_id,
        }
        try:
            raw = await conn.send_request(payload, timeout=10.0)
        except asyncio.TimeoutError:
            return None
        return {
            "status":     raw.get("status"),
            "filled_qty": raw.get("filled_qty"),
            "avg_price":  raw.get("avg_fill_price"),
        }

    async def get_fills(self, since: datetime | None = None) -> list[dict]:
        # MARKET fills arrive inline on ORDER_RESULT; resting LIMIT/STOP
        # fills sync via mt5_fill_sync.py (EA ORDER_UPDATE push + GET_ORDER
        # poll), which applies them straight to orders/positions — nothing
        # consumes a raw fill list for MT5.
        return []
