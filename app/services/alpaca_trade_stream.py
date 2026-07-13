# app/services/alpaca_trade_stream.py
"""
Alpaca trade-update WebSocket stream — instant order-state sync.

The 15s fill-sync poller (alpaca_fill_sync.py) makes order state *converge*;
this stream makes it *immediate*: Alpaca pushes a trade_update event on every
execution/cancel/rejection, and each event is fed into the same
`_sync_order()` reconciliation core the poller uses (row lock, delta Fill,
position netting, WS push), labeled "trade-update stream" in the state
history. The poller stays running as the safety net for anything the stream
misses (disconnects, events for orders placed while the socket was down).

One stream per distinct credential set: Alpaca allows a single trade-update
connection per account, and several Broker rows may share the same keys
(every connection made from this dev environment does). The manager
re-reads the active ALPACA brokers every ALPACA_STREAM_REFRESH_SECS,
starts streams for new credential sets, stops streams whose brokers are
gone, and each stream reconnects with backoff on error.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging

from sqlalchemy import select

from app.core.config import settings
from app.core.security import decrypt_credentials
from app.db.session import AsyncSessionLocal
from app.models.all_models import Broker, Order, OrderStatus

log = logging.getLogger(__name__)

_REFRESH_SECS = 60
_BACKOFF_START = 5
_BACKOFF_MAX = 120


async def _handle_trade_update(update) -> None:
    """One Alpaca trade_update event → the shared reconciliation core."""
    from app.services.alpaca_fill_sync import _sync_order

    o = getattr(update, "order", None)
    if o is None:
        return
    broker_order_id = str(o.id)
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order.id).where(
                Order.broker_order_id == broker_order_id,
                Order.status.in_((OrderStatus.SUBMITTED, OrderStatus.PARTIAL)),
            )
        )
        row = result.first()
    if row is None:
        return  # not an app order (e.g. placed outside), or already terminal
    broker_state = {
        "status": str(getattr(o.status, "value", o.status)).upper(),
        "filled_qty": float(o.filled_qty or 0),
        "avg_price": float(o.filled_avg_price or 0),
    }
    # fill / partial_fill events carry the actual execution print — hand it
    # through so the Fill row records the print price, not the running avg
    execution = None
    if getattr(update, "price", None) and getattr(update, "qty", None):
        execution = {
            "price": float(update.price),
            "qty": float(update.qty),
            "id": str(getattr(update, "execution_id", "") or ""),
        }
    await _sync_order(
        row[0], broker_state=broker_state, source="trade-update stream", execution=execution
    )
    log.info("alpaca_trade_stream: %s event for order %s applied",
             getattr(update, "event", "?"), row[0])


async def _run_one_stream(api_key: str, api_secret: str, paper: bool) -> None:
    """
    Connect + consume one credential set's stream, reconnecting forever.

    Deliberately does NOT use TradingStream._run_forever(): its internal
    retry loop reconnects ~instantly (asyncio.sleep(0.01)) with no backoff,
    which trips Alpaca's connection rate limit and then keeps the resulting
    HTTP 429 alive indefinitely (observed live). Driving _start_ws()
    (connect + auth + listen) and _consume() directly routes every failure
    through this exponential backoff instead.
    """
    from alpaca.trading.stream import TradingStream

    backoff = _BACKOFF_START
    while True:
        stream = TradingStream(api_key, api_secret, paper=paper)
        stream.subscribe_trade_updates(_handle_trade_update)
        try:
            log.info("alpaca_trade_stream: connecting (paper=%s, key=%s…)", paper, api_key[:6])
            await stream._start_ws()
            stream._running = True
            log.info("alpaca_trade_stream: connected (paper=%s)", paper)
            backoff = _BACKOFF_START
            # Catch-up sweep: anything that filled/cancelled while the
            # socket was down gets reconciled now rather than waiting out
            # the poll interval. Fired as a task so consuming starts
            # immediately and no live events are missed meanwhile.
            from app.services.alpaca_fill_sync import sweep_once
            asyncio.create_task(sweep_once(source="stream-reconnect sweep"))
            await stream._consume()
            log.warning("alpaca_trade_stream: stream ended — reconnecting in %ss", backoff)
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await stream.close()
            raise
        except Exception as e:  # noqa: BLE001
            if "failed to authenticate" in str(e):
                # Bad credentials don't heal with retries (e.g. a broker row
                # created with placeholder keys) — go straight to max backoff
                backoff = _BACKOFF_MAX
            log.warning("alpaca_trade_stream: %s — reconnecting in %ss", e, backoff)
        with contextlib.suppress(Exception):
            await stream.close()
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _BACKOFF_MAX)


async def _active_credential_sets() -> dict[tuple[str, bool], tuple[str, str, bool]]:
    """(key, paper) → (key, secret, paper) for every active ALPACA broker."""
    creds: dict[tuple[str, bool], tuple[str, str, bool]] = {}
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Broker).where(Broker.broker_type == "ALPACA", Broker.is_active == True)  # noqa: E712
        )
        for b in result.scalars().all():
            try:
                c = json.loads(decrypt_credentials(b.credentials_enc))
            except Exception:  # noqa: BLE001
                continue
            key, secret = c.get("api_key") or "", c.get("api_secret") or ""
            if key and secret:
                creds[(key, bool(b.is_paper))] = (key, secret, bool(b.is_paper))
    return creds


async def run_alpaca_trade_streams() -> None:
    """Manager entrypoint — maintains one stream task per credential set."""
    if not settings.ALPACA_TRADE_STREAM_ENABLED:
        log.info("alpaca_trade_stream: disabled via ALPACA_TRADE_STREAM_ENABLED")
        return
    tasks: dict[tuple[str, bool], asyncio.Task] = {}
    log.info("Alpaca trade-update stream manager started")
    try:
        while True:
            try:
                wanted = await _active_credential_sets()
                for ident, (key, secret, paper) in wanted.items():
                    task = tasks.get(ident)
                    if task is None or task.done():
                        tasks[ident] = asyncio.create_task(_run_one_stream(key, secret, paper))
                for ident in [i for i in tasks if i not in wanted]:
                    tasks.pop(ident).cancel()
            except Exception as e:  # noqa: BLE001
                log.warning("alpaca_trade_stream manager: %s", e)
            await asyncio.sleep(_REFRESH_SECS)
    except asyncio.CancelledError:
        for task in tasks.values():
            task.cancel()
        raise
