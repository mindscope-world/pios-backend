# app/services/trade_events.py
"""
User-scoped realtime trade events over the existing Redis → WS bridge.

app/core/pubsub.py subscribes to these Redis channels and forwards each
message to app/services/websocket/manager.py: payloads carrying a "user_id"
go only to that user's sockets (manager.send_to_user); payloads without one
broadcast to every subscriber of the (channel, symbol) bucket.

Two wire-format rules matter here (see channelSocket.ts client-side):

- The frontend subscribes to these channels with symbol "" (account-scoped,
  not per-symbol), so payloads must NOT carry a "symbol" key — pubsub.py
  normalizes data["symbol"] into the bucket key and a real symbol would route
  the message to a bucket nobody subscribed to. The human-readable symbol
  travels as "symbol_name" instead.

- Publishing must never break the order path: fills/cancels have already
  committed by the time these fire, so failures are logged and swallowed.
"""
from __future__ import annotations

import logging
import uuid

import orjson

from app.core.redis import get_redis

log = logging.getLogger(__name__)


async def _publish(channel: str, payload: dict) -> None:
    try:
        await get_redis().publish(channel, orjson.dumps(payload))
    except Exception as e:  # pragma: no cover - Redis outage must not 500 an order
        log.warning("trade_events: publish to %r failed: %s", channel, e)


async def publish_order_event(
    user_id: uuid.UUID,
    *,
    order_id: uuid.UUID | None = None,
    status: str,
    symbol_name: str | None = None,
    filled_qty: float | None = None,
) -> None:
    """Order lifecycle change (submit/fill/slice/cancel) for one trader.
    order_id is None for bulk changes (kill switch cancelling everything)."""
    await _publish("orders", {
        "user_id": str(user_id),
        "event": "order_update",
        "order_id": str(order_id) if order_id else None,
        "status": status,
        "symbol_name": symbol_name,
        "filled_qty": filled_qty,
    })


async def publish_position_event(user_id: uuid.UUID, *, symbol_name: str | None = None) -> None:
    """The trader's positions/equity changed — clients refetch, no payload state."""
    await _publish("positions", {
        "user_id": str(user_id),
        "event": "position_update",
        "symbol_name": symbol_name,
    })


async def publish_alert_event(*, severity: str, title: str) -> None:
    """A new Alert row exists. Alerts are global (no user_id column) — broadcast."""
    await _publish("alerts", {
        "event": "alert_created",
        "severity": severity,
        "title": title,
    })
