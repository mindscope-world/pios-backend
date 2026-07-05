"""
MT5BridgeAdapter
================
Bi-directional WebSocket stream between this Python server and the MT5 EA.

Field mapping — Order  →  EA wire frame:
  order.id            →  "order_id"       (our internal UUID)
  order.nonce         →  "nonce"          (matches asyncio.Future)
  order.symbol        →  "symbol"
  order.side          →  "action"         (BUY | SELL)
  order.order_type    →  "order_type"     (MARKET | LIMIT | STOP | STOP_LIMIT)
  order.qty           →  "volume"         (MT5 uses "volume")
  order.price         →  "price"
  order.stop_price    →  "stop_price"
  algo_config.magic   →  "magic"
  algo_config.comment →  "comment"

ExecutionResult field mapping — EA response  →  Order column:
  "ticket"         →  broker_order_id   (str)
  "avg_fill_price" →  avg_fill_price
  "filled_qty"     →  filled_qty
  "commission"     →  commission  (not an ORM column, carried in fills)
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Dict, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from app.models.all_models import ExecutionResult, Order

log = logging.getLogger(__name__)


def _order_to_ea_payload(order: Order) -> dict:
    """Convert internal Order to the MT5 EA wire format."""
    algo = order.algo_config or {}
    payload: Dict[str, Any] = {
        "type":       "PLACE_ORDER",
        "nonce":      order.nonce,
        "order_id":   str(order.id),
        "symbol":     order.symbol,
        "action":     order.side,           # "BUY" | "SELL"
        "order_type": order.order_type,     # "MARKET" | "LIMIT" | ...
        "volume":     order.qty,            # MT5 calls it volume
        "magic":      algo.get("magic", 0),
        "comment":    algo.get("comment", ""),
    }
    if order.price is not None:
        payload["price"] = order.price
    if order.stop_price is not None:
        payload["stop_price"] = order.stop_price
    return payload


class MT5BridgeAdapter:
    """One instance per MT5 terminal connection."""

    def __init__(
        self,
        account_id: str,
        on_tick: Optional[Callable[[dict], None]] = None,
        on_position_update: Optional[Callable[[dict], None]] = None,
    ):
        self.account_id = account_id
        self._ws: Optional[websockets.WebSocketServerProtocol] = None
        self._pending_futures: Dict[str, asyncio.Future] = {}
        self._connected = asyncio.Event()
        self._on_tick = on_tick
        self._on_position_update = on_position_update
        self._receive_task: Optional[asyncio.Task] = None

    # ── Connection lifecycle ──────────────────────────────────────────────────

    async def attach(self, ws) -> None:
        self._ws = ws
        self._connected.set()
        log.info("[MT5:%s] EA connected", self.account_id)
        self._receive_task = asyncio.create_task(self._receive_loop())

    async def detach(self) -> None:
        self._connected.clear()
        self._ws = None
        for nonce, fut in list(self._pending_futures.items()):
            if not fut.done():
                fut.set_exception(ConnectionError("MT5 EA disconnected"))
        self._pending_futures.clear()
        log.warning("[MT5:%s] EA disconnected", self.account_id)

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set() and self._ws is not None

    # ── Order execution ───────────────────────────────────────────────────────

    async def execute_order(self, order: Order, timeout: float = 10.0) -> ExecutionResult:
        if not self.is_connected:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)

        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_futures[order.nonce] = fut

        payload = _order_to_ea_payload(order)
        try:
            await self._ws.send(json.dumps(payload))
            log.debug("[MT5:%s] → %s", self.account_id, payload)
            raw_result = await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_futures.pop(order.nonce, None)
            return ExecutionResult(success=False, error_message="MT5 response timeout")
        except Exception as exc:
            self._pending_futures.pop(order.nonce, None)
            return ExecutionResult(success=False, error_message=str(exc))

        return self._parse_result(raw_result)

    async def cancel_order(self, broker_order_id: str, nonce: str, timeout: float = 10.0) -> ExecutionResult:
        if not self.is_connected:
            return ExecutionResult(success=False, error_message="Not connected")

        fut = asyncio.get_event_loop().create_future()
        self._pending_futures[nonce] = fut

        await self._ws.send(json.dumps({
            "type":            "CANCEL_ORDER",
            "nonce":           nonce,
            "broker_order_id": broker_order_id,    # was "ticket" — now aligned
        }))
        try:
            raw = await asyncio.wait_for(fut, timeout=timeout)
            return self._parse_result(raw)
        except asyncio.TimeoutError:
            self._pending_futures.pop(nonce, None)
            return ExecutionResult(success=False, error_message="Cancel timeout")

    # ── Inbound message loop ──────────────────────────────────────────────────

    async def _receive_loop(self) -> None:
        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                    log.debug("[MT5:%s] ← %s", self.account_id, msg)
                    await self._dispatch(msg)
                except json.JSONDecodeError:
                    log.error("[MT5:%s] Bad JSON: %s", self.account_id, raw[:200])
        except ConnectionClosed:
            await self.detach()

    async def _dispatch(self, msg: dict) -> None:
        msg_type = msg.get("type")

        if msg_type == "ORDER_RESULT":
            nonce = msg.get("nonce")
            fut = self._pending_futures.pop(nonce, None)
            if fut and not fut.done():
                fut.set_result(msg)

        elif msg_type == "TICK":
            if self._on_tick:
                self._on_tick(msg)

        elif msg_type == "POSITION_UPDATE":
            if self._on_position_update:
                self._on_position_update(msg)

        elif msg_type == "PONG":
            pass

        else:
            log.warning("[MT5:%s] Unknown type: %s", self.account_id, msg_type)

    # ── Result parsing ────────────────────────────────────────────────────────

    @staticmethod
    def _parse_result(raw: dict) -> ExecutionResult:
        if raw.get("success"):
            return ExecutionResult(
                success        = True,
                broker_order_id = str(raw["ticket"]) if raw.get("ticket") else None,
                avg_fill_price  = raw.get("avg_fill_price") or raw.get("fill_price"),
                filled_qty      = raw.get("filled_qty")     or raw.get("fill_volume"),
                commission      = raw.get("commission"),
                raw             = raw,
            )
        return ExecutionResult(
            success       = False,
            error_code    = raw.get("error_code"),
            error_message = raw.get("error_message", "Unknown error from EA"),
            raw           = raw,
        )

    async def ping(self) -> None:
        if self._ws:
            await self._ws.send(json.dumps({"type": "PING"}))