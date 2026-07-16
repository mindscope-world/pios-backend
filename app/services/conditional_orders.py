# app/services/conditional_orders.py
"""
Conditional-order engine — STOP_LIMIT trigger monitoring and OCO linked legs.

order_service.submit_order holds these order types app-side (SUBMITTED, no
broker_order_id, "armed" note in state history) instead of firing them at the
broker immediately — on the paper adapter everything instant-fills, so real
stop/OCO semantics have to live here, broker-agnostically. Every
CONDITIONAL_POLL_SECS this loop:

1. Evaluates each armed order's trigger against the live mark
   (market_data_service.get_live_ticker — same domain routing as everything
   else: crypto→ccxt/Alpaca, fiat/metals→OANDA, equities→Alpaca):
     - STOP_LIMIT     BUY: mark >= stop_price   SELL: mark <= stop_price
                      → submitted to the broker as a LIMIT at order.price
     - OCO limit leg  BUY: mark <= price        SELL: mark >= price
                      → submitted as a LIMIT at order.price
     - OCO stop leg   BUY: mark >= stop_price   SELL: mark <= stop_price
                      → submitted at the market (order.price set to the
                        trigger mark so the paper adapter fills at it)
2. Enforces one-cancels-other: the moment any OCO leg reaches FILLED —
   whether filled here at trigger time or later at the broker (a triggered
   LIMIT resting at Alpaca is completed by alpaca_fill_sync/the trade
   stream) — every open sibling in its group is cancelled.

OCO legs are two ordinary Order rows created together by submit_order,
linked via algo_config = {"oco_group": <parent id>, "oco_leg": "limit"|
"stop"} — the JSON column already exists, so no migration. The limit leg is
the row the submit call returns; the stop leg carries "-STP" on its
client_order_id.

Same discipline as execution_algo/alpaca_fill_sync: per-iteration sessions,
each order re-read with_for_update before acting so a user cancel racing the
trigger can't double-execute, WS pushes only after commit.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.all_models import Fill, Order, OrderStatus
from app.services.broker_service import get_adapter
from app.services.market_data_service import get_live_ticker
from app.services.positions_service import apply_fill_to_position, write_pnl_snapshot
from app.services.trade_events import publish_order_event, publish_position_event

log = logging.getLogger(__name__)

_CONDITIONAL_TYPES = ("STOP_LIMIT", "OCO")


def _oco_group(order: Order) -> str | None:
    cfg = order.algo_config or {}
    return cfg.get("oco_group")


def _oco_leg(order: Order) -> str | None:
    cfg = order.algo_config or {}
    return cfg.get("oco_leg")


def _is_triggered(order: Order, mark: float) -> bool:
    buy = order.side == "BUY"
    if order.order_type == "STOP_LIMIT" or (order.order_type == "OCO" and _oco_leg(order) == "stop"):
        stop = float(order.stop_price or 0)
        if stop <= 0:
            return False
        return mark >= stop if buy else mark <= stop
    if order.order_type == "OCO":  # limit leg
        limit = float(order.price or 0)
        if limit <= 0:
            return False
        return mark <= limit if buy else mark >= limit
    return False


async def _mark_price(symbol: str) -> float | None:
    try:
        t = await get_live_ticker(symbol)
    except Exception as e:  # noqa: BLE001
        log.debug("conditional_orders: ticker %s failed: %s", symbol, e)
        return None
    if t.get("error"):
        return None
    last = t.get("last")
    if last:
        return float(last)
    bid, ask = float(t.get("bid") or 0), float(t.get("ask") or 0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2
    return None


async def _execute_triggered(order_id, mark: float) -> None:
    """Fire one triggered order at the broker with the same bookkeeping as
    the instant-fill path; a broker result that rests (e.g. a real LIMIT at
    Alpaca) stays SUBMITTED with broker_order_id set, and alpaca_fill_sync /
    the trade stream complete it."""
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
        # Re-check under the lock — a user cancel may have won the race, or
        # a previous pass may already have sent it to the broker.
        if (
            order is None
            or order.status != OrderStatus.SUBMITTED
            or order.broker_order_id is not None
        ):
            return

        is_stop_leg = order.order_type == "OCO" and _oco_leg(order) == "stop"
        if is_stop_leg:
            # Stop legs execute at the market — give the paper adapter (which
            # fills at order.price) the trigger mark as its execution price.
            order.price = mark

        adapter = get_adapter(order.broker)
        try:
            broker_result = await adapter.submit_order(order)
        except Exception as e:  # noqa: BLE001
            order.transition("REJECTED", f"Trigger execution failed: {e}")
            order.reject_reason = str(e)
            await db.commit()
            log.warning("conditional_orders: order %s trigger submit failed: %s", order_id, e)
            return

        order.broker_order_id = broker_result.get("broker_order_id")
        trigger_note = f"Trigger hit at {mark}"

        if broker_result.get("status") == "FILLED":
            fill_price = float(broker_result.get("avg_price") or order.price or 0)
            fill_qty = float(broker_result.get("filled_qty") or order.qty)
            commission = fill_qty * fill_price * 0.001
            db.add(Fill(
                order_id=order.id,
                symbol_id=order.symbol_id,
                side=order.side,
                qty=fill_qty,
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
                qty=fill_qty,
                price=fill_price,
                commission=commission,
            )
            await write_pnl_snapshot(db, order.user_id)
            order.filled_qty = fill_qty
            order.avg_fill_price = fill_price
            order.transition("FILLED", trigger_note)
            order.filled_at = datetime.now(timezone.utc)
        else:
            order.transition("SUBMITTED", f"{trigger_note} — sent to broker")

        user_id = order.user_id
        new_status = order.status
        symbol_name = getattr(order.symbol, "symbol", None)
        filled_qty = float(order.filled_qty or 0)
        await db.commit()

    await publish_order_event(
        user_id, order_id=order_id, status=new_status,
        symbol_name=symbol_name, filled_qty=filled_qty,
    )
    if filled_qty > 0:
        await publish_position_event(user_id, symbol_name=symbol_name)
    log.info("conditional_orders: order %s triggered at %s → %s", order_id, mark, new_status)


async def _cancel_oco_siblings(group: str, filled_order_id) -> None:
    """One-cancels-other: cancel every still-open leg in `group` other than
    the one that filled."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order)
            .options(selectinload(Order.symbol), selectinload(Order.broker), selectinload(Order.events))
            .where(
                Order.order_type == "OCO",
                Order.status.in_((OrderStatus.SUBMITTED, OrderStatus.PARTIAL)),
                Order.algo_config["oco_group"].as_string() == group,
                Order.id != filled_order_id,
            )
            .with_for_update(of=Order)
        )
        siblings = result.scalars().all()
        published = []
        for sib in siblings:
            if sib.broker_order_id:
                try:
                    await get_adapter(sib.broker).cancel_order(sib.broker_order_id)
                except Exception:  # noqa: BLE001
                    pass  # still cancel locally — same policy as user cancel
            sib.transition("CANCELLED", "OCO sibling filled")
            sib.cancelled_at = datetime.now(timezone.utc)
            published.append((sib.user_id, sib.id, getattr(sib.symbol, "symbol", None)))
        if not siblings:
            return
        await db.commit()
    for user_id, sib_id, symbol_name in published:
        await publish_order_event(
            user_id, order_id=sib_id, status="CANCELLED",
            symbol_name=symbol_name, filled_qty=0.0,
        )
        log.info("conditional_orders: OCO sibling %s cancelled (group %s)", sib_id, group)


async def sweep_conditionals() -> None:
    # 1. OCO housekeeping: any group with a FILLED leg cancels its open
    #    siblings — covers legs that filled at the broker after triggering,
    #    not just instant fills.
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order.id, Order.algo_config).where(
                Order.order_type == "OCO",
                Order.status == OrderStatus.FILLED,
                Order.algo_config.isnot(None),
            )
        )
        filled_groups = {
            (cfg or {}).get("oco_group"): oid for oid, cfg in result.all() if (cfg or {}).get("oco_group")
        }
    for group, filled_id in filled_groups.items():
        await _cancel_oco_siblings(group, filled_id)

    # 2. Evaluate armed triggers against live marks, one price fetch per symbol.
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order)
            .options(selectinload(Order.symbol))
            .where(
                Order.status == OrderStatus.SUBMITTED,
                Order.broker_order_id.is_(None),
                Order.order_type.in_(_CONDITIONAL_TYPES),
            )
        )
        armed = [
            (o.id, getattr(o.symbol, "symbol", None), o)
            for o in result.scalars().all()
        ]

    marks: dict[str, float | None] = {}
    for order_id, symbol_name, order in armed:
        if not symbol_name:
            continue
        if symbol_name not in marks:
            marks[symbol_name] = await _mark_price(symbol_name)
        mark = marks[symbol_name]
        if mark is None:
            continue
        if _is_triggered(order, mark):
            try:
                await _execute_triggered(order_id, mark)
            except Exception as e:  # noqa: BLE001
                log.warning("conditional_orders: order %s trigger failed: %s", order_id, e)


async def run_conditional_orders() -> None:
    """Loop entrypoint — never raises; per-pass errors are logged and retried."""
    interval = max(1, int(settings.CONDITIONAL_POLL_SECS))
    log.info("Conditional-order engine started (every %ss)", interval)
    while True:
        try:
            await sweep_conditionals()
        except asyncio.CancelledError:
            log.info("Conditional-order engine stopped")
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("conditional_orders: pass failed: %s", e)
        await asyncio.sleep(interval)
