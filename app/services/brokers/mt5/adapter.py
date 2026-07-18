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

MULTI-WORKER DEPLOYMENT: the EA's WebSocket lands on exactly one worker
process, but order HTTP requests round-robin across all of them (Dockerfile
runs `uvicorn --workers 2`). Cross-worker requests are relayed over Redis,
mirroring the app's existing pub/sub fan-out:

  presence  -- the socket-holding worker keeps `mt5:ea:{broker_id}` alive
               (short-TTL key, heartbeat-refreshed) so any worker can tell
               "connected somewhere" from "genuinely offline".
  relay     -- that worker also serves `mt5:req:{broker_id}`: other workers
               publish {payload, reply_channel} there and await the reply on
               a per-request channel. MT5Adapter._send prefers the local
               socket (no Redis hop when the request lands on the right
               worker) and falls back to the relay when presence says the EA
               is paired to a sibling process.

The fill-sync poll loop (mt5_fill_sync.sweep_once) deliberately still gates
on the *local* registry, so exactly one worker -- the socket holder -- polls
each EA; relaying it from every worker would only multiply EA traffic.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid as uuid_mod
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import orjson
from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from app.core.redis import get_redis
from app.models.all_models import Broker, ExecutionResult, Order
from app.schemas.all_schemas import BrokerTestResult

log = logging.getLogger(__name__)

# ── Cross-worker relay (Redis) ────────────────────────────────────────────────

WORKER_ID = uuid_mod.uuid4().hex[:8]  # per-process, for presence/debugging

_EA_PRESENCE_TTL_SECS = 15   # presence key expiry — a dead worker's claim ages out
_EA_PRESENCE_BEAT_SECS = 5   # heartbeat refresh interval (must be < TTL)
_RELAY_REPLY_MARGIN_SECS = 2.0  # extra wait over the EA timeout for the Redis hop


def _presence_key(broker_id: str) -> str:
    return f"mt5:ea:{broker_id}"


def _request_channel(broker_id: str) -> str:
    return f"mt5:req:{broker_id}"


async def ea_connected_anywhere(broker_id: str) -> bool:
    """True when an EA for this broker is paired to this worker or a sibling."""
    conn = mt5_registry.get(broker_id)
    if conn is not None and conn.is_connected:
        return True
    try:
        return bool(await get_redis().exists(_presence_key(broker_id)))
    except Exception:  # Redis down → only local knowledge remains
        return False


async def relay_request(broker_id: str, payload: dict, timeout: float = 10.0) -> dict:
    """Send one EA request via whichever sibling worker holds the socket.

    Raises the same things MT5Connection.send_request does — TimeoutError
    when nothing answers in time, ConnectionError when the serving worker
    reports the EA gone — so callers can't tell local from relayed.
    """
    r = get_redis()
    reply_channel = f"mt5:resp:{uuid_mod.uuid4().hex}"
    pubsub = r.pubsub()
    await pubsub.subscribe(reply_channel)
    try:
        receivers = await r.publish(
            _request_channel(broker_id),
            orjson.dumps({"payload": payload, "reply_channel": reply_channel, "timeout": timeout}),
        )
        if receivers == 0:
            raise ConnectionError(f"MT5 EA disconnected for broker {broker_id}")
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout + _RELAY_REPLY_MARGIN_SECS
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError()
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=min(remaining, 1.0))
            if msg is None or msg.get("type") != "message":
                continue
            out = orjson.loads(msg["data"])
            if out.get("ok"):
                return out["data"]
            if out.get("error") == "timeout":
                raise asyncio.TimeoutError()
            raise ConnectionError(out.get("error") or "MT5 relay error")
    finally:
        with contextlib.suppress(Exception):
            await pubsub.unsubscribe(reply_channel)
            await pubsub.aclose()

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
    """Process-wide singleton: broker_id (str) -> MT5Connection.

    Registration also advertises the connection to sibling workers: a
    presence key in Redis plus a served request channel (see module
    docstring). Both live exactly as long as the socket."""

    def __init__(self):
        self._connections: Dict[str, MT5Connection] = {}
        self._relay_tasks: Dict[str, asyncio.Task] = {}

    def get(self, broker_id: str) -> Optional[MT5Connection]:
        return self._connections.get(broker_id)

    async def register(self, broker_id: str, ws: WebSocket) -> MT5Connection:
        conn = MT5Connection(broker_id, ws)
        self._connections[broker_id] = conn
        old = self._relay_tasks.pop(broker_id, None)
        if old is not None:
            old.cancel()
        self._relay_tasks[broker_id] = asyncio.create_task(self._serve_relay(broker_id, conn))
        log.info("[MT5:%s] EA connected (worker %s)", broker_id, WORKER_ID)
        return conn

    def unregister(self, broker_id: str) -> None:
        if self._connections.pop(broker_id, None) is not None:
            log.info("[MT5:%s] EA disconnected", broker_id)
        task = self._relay_tasks.pop(broker_id, None)
        if task is not None:
            task.cancel()

    def status(self) -> dict:
        return {bid: conn.is_connected for bid, conn in self._connections.items()}

    async def _serve_relay(self, broker_id: str, conn: MT5Connection) -> None:
        """Keep the presence key fresh and answer sibling workers' relayed
        requests for as long as `conn` lives. Cancelled by unregister()."""
        r = get_redis()
        pubsub = r.pubsub()
        try:
            await pubsub.subscribe(_request_channel(broker_id))
            last_beat = 0.0
            while conn.is_connected:
                now = asyncio.get_event_loop().time()
                if now - last_beat >= _EA_PRESENCE_BEAT_SECS:
                    await r.set(_presence_key(broker_id), WORKER_ID, ex=_EA_PRESENCE_TTL_SECS)
                    last_beat = now
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg is None or msg.get("type") != "message":
                    continue
                try:
                    req = orjson.loads(msg["data"])
                except Exception:
                    continue
                # Serve concurrently — send_request correlates by id, so
                # overlapping in-flight requests are fine.
                task = asyncio.create_task(self._answer_relay(conn, req))
                _push_tasks.add(task)
                task.add_done_callback(_push_tasks.discard)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — Redis outage: local path still works
            log.warning("[MT5:%s] relay serve loop died: %s", broker_id, e)
        finally:
            with contextlib.suppress(Exception):
                await r.delete(_presence_key(broker_id))
            with contextlib.suppress(Exception):
                await pubsub.unsubscribe(_request_channel(broker_id))
                await pubsub.aclose()

    @staticmethod
    async def _answer_relay(conn: MT5Connection, req: dict) -> None:
        reply_channel = req.get("reply_channel")
        payload = req.get("payload") or {}
        timeout = float(req.get("timeout") or 10.0)
        try:
            data = await conn.send_request(payload, timeout=timeout)
            out: dict = {"ok": True, "data": data}
        except asyncio.TimeoutError:
            out = {"ok": False, "error": "timeout"}
        except Exception as e:  # noqa: BLE001
            out = {"ok": False, "error": str(e) or "MT5 EA request failed"}
        if reply_channel:
            with contextlib.suppress(Exception):
                await get_redis().publish(reply_channel, orjson.dumps(out))


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

    async def _send(self, payload: dict, timeout: float = 10.0) -> dict:
        """One EA request — local socket when this worker holds it, Redis
        relay when a sibling does, ConnectionError when nobody does."""
        broker_id = str(self.broker.id)
        conn = mt5_registry.get(broker_id)
        if conn is not None and conn.is_connected:
            return await conn.send_request(payload, timeout=timeout)
        if await ea_connected_anywhere(broker_id):
            return await relay_request(broker_id, payload, timeout=timeout)
        raise ConnectionError(
            f"MT5 EA not connected for broker {broker_id} -- "
            f"pair the terminal via /ws/mt5/{broker_id} first."
        )

    async def test_connection(self) -> BrokerTestResult:
        if not await ea_connected_anywhere(str(self.broker.id)):
            return BrokerTestResult(success=False, latency_ms=None, message="MT5 EA not connected")
        t0 = time.perf_counter()
        try:
            await self._send({"type": "PING", "correlation_id": f"ping-{t0}"}, timeout=5.0)
        except asyncio.TimeoutError:
            pass  # EA may not echo a correlated PONG -- absence isn't fatal, the socket is up
        except ConnectionError:
            return BrokerTestResult(success=False, latency_ms=None, message="MT5 EA not connected")
        latency = (time.perf_counter() - t0) * 1000
        return BrokerTestResult(success=True, latency_ms=round(latency, 2), message="EA connected")

    async def get_account(self) -> dict:
        corr = f"acct-{time.time()}"
        try:
            return await self._send({"type": "GET_ACCOUNT", "correlation_id": corr})
        except asyncio.TimeoutError:
            return {}

    async def submit_order(self, order: Order) -> dict:
        correlation_id = order.client_order_id or str(order.id)
        payload = _order_to_ea_payload(order, correlation_id)
        try:
            raw = await self._send(payload, timeout=10.0)
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
        payload = {
            "type":             "CANCEL_ORDER",
            "correlation_id":   f"cancel-{broker_order_id}",
            "broker_order_id":  broker_order_id,
        }
        try:
            raw = await self._send(payload, timeout=10.0)
        except asyncio.TimeoutError:
            return {"status": "CANCEL_TIMEOUT"}
        result = _parse_result(raw)
        return {"status": "CANCELLED" if result.success else "CANCEL_FAILED"}

    async def get_positions(self) -> list[dict]:
        try:
            result = await self._send({"type": "GET_POSITIONS", "correlation_id": f"pos-{time.time()}"})
        except asyncio.TimeoutError:
            return []
        return result.get("positions", [])

    async def get_order(self, broker_order_id: str) -> Optional[dict]:
        """Current state of one ticket, in the shape mt5_fill_sync expects:
        {status, filled_qty, avg_price}. Status is app-vocabulary (SUBMITTED/
        PARTIAL/FILLED/CANCELLED/EXPIRED/REJECTED, or UNKNOWN when the EA
        can't place the ticket). None on timeout — the sync loop retries."""
        payload = {
            "type":            "GET_ORDER",
            "correlation_id":  f"stat-{broker_order_id}-{time.time()}",
            "broker_order_id": broker_order_id,
        }
        try:
            raw = await self._send(payload, timeout=10.0)
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
