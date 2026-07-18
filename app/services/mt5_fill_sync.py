# app/services/mt5_fill_sync.py
"""
Background broker↔app order reconciliation for MT5 (EA bridge).

MT5Adapter.submit_order reports MARKET fills inline (the EA's ORDER_RESULT
carries the deal price), but a LIMIT/STOP that rests as a pending order in
the terminal comes back SUBMITTED — and previously stayed SUBMITTED in the
app forever after MT5 triggered it, because the adapter had no fill-sync
path. Two paths close that gap, mirroring the Alpaca model (trade-update
stream + poll safety net):

  push — the EA's OnTradeTransaction pushes an unsolicited ORDER_UPDATE
         frame the moment a deal executes against one of its tickets (or a
         pending order dies broker-side: cancelled/expired/rejected). It
         carries the actual print price/volume, so the Fill row records the
         real execution price. MT5Connection._dispatch routes those frames
         to handle_order_update() below.
  poll — every MT5_FILL_SYNC_INTERVAL_SECS this loop scans open
         (SUBMITTED/PARTIAL) non-algo orders on MT5 brokers whose EA is
         currently connected and asks the terminal for each ticket's state
         (GET_ORDER → ORDER_STATUS). This catches whatever the push missed:
         fills that landed while the EA was reconnecting, or an ORDER_UPDATE
         that raced the submit path before broker_order_id was committed.

The poll fallback has no per-execution feed, but MT5's ORDER_STATUS reply
carries the cumulative filled qty and average price, so the newly-filled
delta's true average is backed out from the two cumulative notionals rather
than blended at the broker's running average.

Same discipline as alpaca_fill_sync: per-iteration sessions, each order
re-read with_for_update so a user cancel racing the sync can't corrupt the
row, and the trader's open screens nudged over WS only after commit. Algo
orders (TWAP/VWAP/ICEBERG) are excluded — their slices are marketable and
their parent status belongs to execution_algo.py.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.all_models import Broker, Fill, Order, OrderStatus
from app.services.broker_service import get_adapter
from app.services.brokers.mt5.adapter import MT5Adapter, mt5_registry
from app.services.positions_service import apply_fill_to_position, write_pnl_snapshot
from app.services.trade_events import publish_order_event, publish_position_event

log = logging.getLogger(__name__)

_ALGO_TYPES = ("TWAP", "VWAP", "ICEBERG")

# EA statuses that end an order without (further) fills
_TERMINAL_UNFILLED = {"CANCELLED", "EXPIRED", "REJECTED"}


async def _find_open_order_id(broker_id: str, ticket: str):
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order.id).where(
                Order.broker_id == uuid.UUID(broker_id),
                Order.broker_order_id == str(ticket),
                Order.status.in_((OrderStatus.SUBMITTED, OrderStatus.PARTIAL)),
            )
        )
        return result.scalars().first()


async def handle_order_update(broker_id: str, msg: dict) -> None:
    """One unsolicited EA ORDER_UPDATE frame → the reconciliation core.

    Called from MT5Connection._dispatch (as a task — dispatch is sync).
    Unknown tickets are dropped: manual terminal trades and other-EA orders
    legitimately produce events for orders this app never placed.
    """
    ticket = str(msg.get("ticket") or "")
    if not ticket:
        return

    order_id = await _find_open_order_id(broker_id, ticket)
    if order_id is None:
        # The EA fires OnTradeTransaction the instant a marketable LIMIT
        # executes — sometimes before order_service has committed
        # broker_order_id. One short retry covers that race; anything still
        # unmatched is a foreign ticket (or already terminal) and the poll
        # loop remains the safety net regardless.
        await asyncio.sleep(2.0)
        order_id = await _find_open_order_id(broker_id, ticket)
        if order_id is None:
            log.debug("[MT5:%s] ORDER_UPDATE for unknown ticket %s ignored", broker_id, ticket)
            return

    broker_state = {
        "status":     str(msg.get("status") or "").upper(),
        "filled_qty": msg.get("filled_qty"),
        "avg_price":  msg.get("avg_fill_price"),
    }
    execution = None
    if float(msg.get("fill_qty") or 0) > 0 and float(msg.get("fill_price") or 0) > 0:
        execution = {"qty": float(msg["fill_qty"]), "price": float(msg["fill_price"])}
    await _sync_order(order_id, broker_state=broker_state, source="EA push", execution=execution)


async def _sync_order(
    order_id,
    broker_state: dict | None = None,
    source: str = "fill-sync",
    execution: dict | None = None,
) -> None:
    """
    Reconcile one order against the MT5 terminal. `broker_state` is
    injectable — the EA push passes the event's order state; when None it's
    fetched from the EA inside the row lock so the status acted on is
    current at write time. `execution` is the push event's per-deal print
    ({price, qty}) — when its qty accounts for the whole newly-filled delta,
    the Fill row records the actual print price.
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order)
            .options(
                selectinload(Order.symbol),
                selectinload(Order.broker),
                selectinload(Order.events),
            )
            .where(Order.id == order_id)
            .with_for_update(of=Order)
        )
        order = result.scalar_one_or_none()
        # Re-check under the lock — a user cancel may have won the race
        if order is None or order.status not in (OrderStatus.SUBMITTED, OrderStatus.PARTIAL):
            return

        if broker_state is None:
            adapter = get_adapter(order.broker)
            if not isinstance(adapter, MT5Adapter):
                return
            broker_state = await adapter.get_order(order.broker_order_id)
            if broker_state is None:
                return

        status = str(broker_state.get("status") or "").upper()
        if status in ("", "UNKNOWN"):
            # The terminal couldn't place the ticket (history window, wrong
            # account logged in, …) — don't guess; retry next pass.
            return

        broker_filled = float(broker_state.get("filled_qty") or 0)
        avg_price = float(broker_state.get("avg_price") or 0)

        prior_filled = float(order.filled_qty or 0)
        newly_filled = broker_filled - prior_filled
        changed = False

        if newly_filled > 1e-12:
            if (
                execution
                and abs(execution["qty"] - newly_filled) <= max(newly_filled * 1e-6, 1e-12)
            ):
                # The push event's deal print covers the whole delta — one
                # real Fill row at its own price.
                fill_price = execution["price"]
            else:
                # Poll-driven (or several deals batched): back the delta's
                # true average out of the two cumulative notionals instead
                # of blending at the broker's running average.
                delta_notional = avg_price * broker_filled - (order.avg_fill_price or 0) * prior_filled
                fill_price = delta_notional / newly_filled
                if not (fill_price > 0):
                    fill_price = avg_price
            if fill_price > 0:
                commission = newly_filled * fill_price * 0.001
                db.add(Fill(
                    order_id=order.id,
                    symbol_id=order.symbol_id,
                    side=order.side,
                    qty=newly_filled,
                    price=fill_price,
                    commission=commission,
                    total_cost=commission,
                ))
                await apply_fill_to_position(
                    db,
                    user_id=order.user_id,
                    broker_id=order.broker_id,
                    strategy_id=order.strategy_id,
                    symbol_id=order.symbol_id,
                    side=order.side,
                    qty=newly_filled,
                    price=fill_price,
                    commission=commission,
                )
                prior_notional = prior_filled * (order.avg_fill_price or 0)
                order.avg_fill_price = (prior_notional + newly_filled * fill_price) / broker_filled
                order.filled_qty = broker_filled
                await write_pnl_snapshot(db, order.user_id)
                changed = True

        if status == "FILLED":
            order.transition("FILLED", f"Broker {source}")
            order.filled_at = datetime.now(timezone.utc)
            changed = True
        elif status == "PARTIAL" and changed:
            order.transition("PARTIAL", f"Broker {source}")
        elif status in _TERMINAL_UNFILLED:
            order.transition(status, f"Broker-side terminal state ({source})")
            if status == "REJECTED":
                order.reject_reason = order.reject_reason or f"Rejected at broker ({source})"
            elif status == "CANCELLED":
                order.cancelled_at = datetime.now(timezone.utc)
            changed = True

        if not changed:
            return

        user_id = order.user_id
        new_status = order.status
        symbol_name = getattr(order.symbol, "symbol", None)
        filled_qty = float(order.filled_qty or 0)
        await db.commit()

    # After (never before) the commit, nudge the trader's open screens
    await publish_order_event(
        user_id, order_id=order_id, status=new_status,
        symbol_name=symbol_name, filled_qty=filled_qty,
    )
    if filled_qty > 0:
        await publish_position_event(user_id, symbol_name=symbol_name)
    log.info("mt5_fill_sync: order %s → %s (filled %s)", order_id, new_status, filled_qty)


async def sweep_once(source: str = "fill-sync") -> int:
    """One reconciliation pass over every open MT5 order whose EA is
    currently connected. Disconnected terminals are skipped silently —
    an offline EA is a normal state, not an error, and the orders are
    picked up as soon as it reconnects."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order.id, Order.broker_id)
            .join(Broker, Broker.id == Order.broker_id)
            .where(
                Order.status.in_((OrderStatus.SUBMITTED, OrderStatus.PARTIAL)),
                Order.broker_order_id.isnot(None),
                Order.order_type.notin_(_ALGO_TYPES),
                Broker.broker_type == "MT5",
            )
        )
        candidates = result.all()

    synced = 0
    for order_id, broker_id in candidates:
        # Deliberately the LOCAL registry, not ea_connected_anywhere(): every
        # worker runs this loop, and gating on the local socket makes exactly
        # one worker (the socket holder) poll each EA — relaying from all of
        # them would just multiply GET_ORDER traffic at the terminal.
        conn = mt5_registry.get(str(broker_id))
        if conn is None or not conn.is_connected:
            continue
        try:
            await _sync_order(order_id, source=source)
            synced += 1
        except Exception as e:  # noqa: BLE001
            log.warning("mt5_fill_sync: order %s failed: %s", order_id, e)
    return synced


async def run_mt5_fill_sync() -> None:
    """Loop entrypoint — never raises; per-order errors are logged and the
    order is retried on the next pass."""
    interval = max(5, int(settings.MT5_FILL_SYNC_INTERVAL_SECS))
    log.info("MT5 fill-sync loop started (every %ss)", interval)
    while True:
        try:
            await sweep_once()
        except asyncio.CancelledError:
            log.info("MT5 fill-sync loop stopped")
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("mt5_fill_sync: pass failed: %s", e)
        await asyncio.sleep(interval)
