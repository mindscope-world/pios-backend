# app/services/broker_fill_sync.py
"""
Shared reconciliation core for broker fill-sync loops.

sync_order_against_broker() reconciles ONE app order against the broker's
current view of it — row-locked, delta-priced, WS-nudged after commit. It
was born in mt5_fill_sync (pass 25) and is shape-generic: any adapter that
exposes `get_order(broker_order_id) -> {status, filled_qty, avg_price}`
in app vocabulary (SUBMITTED/PARTIAL/FILLED/CANCELLED/EXPIRED/REJECTED,
UNKNOWN when the broker can't place the ticket, None on timeout) can use
it. mt5_fill_sync and oanda_fill_sync both drive it; their sweep loops
differ only in how they pick candidates (MT5 gates on the local EA socket,
OANDA polls REST unconditionally).

Discipline (same as alpaca_fill_sync): per-call sessions, the order re-read
with_for_update so a user cancel racing the sync can't corrupt the row, and
the trader's open screens nudged over WS only after commit.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.session import AsyncSessionLocal
from app.models.all_models import Fill, Order, OrderStatus
from app.services.broker_service import get_adapter
from app.services.positions_service import apply_fill_to_position, write_pnl_snapshot
from app.services.trade_events import publish_order_event, publish_position_event

log = logging.getLogger(__name__)

ALGO_TYPES = ("TWAP", "VWAP", "ICEBERG")

# Broker statuses that end an order without (further) fills
TERMINAL_UNFILLED = {"CANCELLED", "EXPIRED", "REJECTED"}


async def sync_order_against_broker(
    order_id,
    broker_state: dict | None = None,
    source: str = "fill-sync",
    execution: dict | None = None,
) -> None:
    """
    Reconcile one order against its broker. `broker_state` is injectable —
    push-style callers (the MT5 EA's ORDER_UPDATE) pass the event's order
    state; when None it's fetched from the adapter inside the row lock so
    the status acted on is current at write time. `execution` is a push
    event's per-deal print ({price, qty}) — when its qty accounts for the
    whole newly-filled delta, the Fill row records the actual print price.
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
            if not hasattr(adapter, "get_order"):
                return
            broker_state = await adapter.get_order(order.broker_order_id)
            if broker_state is None:
                return

        status = str(broker_state.get("status") or "").upper()
        if status in ("", "UNKNOWN"):
            # The broker couldn't place/find the ticket — don't guess; retry
            # next pass.
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
        elif status in TERMINAL_UNFILLED:
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
    log.info("%s: order %s → %s (filled %s)", source, order_id, new_status, filled_qty)
