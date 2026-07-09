# app/services/execution_algo.py
"""
Algorithmic execution engine -- TWAP / ICEBERG / VWAP(-approx) order slicing.

order_service.submit_order allocates an algo order once (status SUBMITTED,
zero fills) and hands it off to run_algo_order() as a background asyncio
task rather than waiting on it inline -- a real slice schedule can span
minutes, far longer than an HTTP request should block. Each slice is sent
to the broker adapter independently and recorded against the same parent
Order row: Fill rows accumulate, Order.filled_qty/avg_fill_price update
incrementally, and status walks SUBMITTED -> PARTIAL -> ... -> FILLED,
exactly the states the Order state machine already has for this purpose.

Every slice iteration opens its own DB session -- the request-scoped
session that created the order is long closed by the time later slices
run -- and re-checks the order's live status before submitting, so a
user cancelling mid-schedule (DELETE /orders/{id}) stops future slices
instead of racing the background task.
"""
from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.session import AsyncSessionLocal
from app.models.all_models import Order, Fill, OrderStatus
from app.services.broker_service import get_adapter
from app.services.audit_service import write_audit
from app.services.positions_service import apply_fill_to_position, write_pnl_snapshot
from app.services.trade_events import publish_order_event, publish_position_event

log = logging.getLogger(__name__)


@dataclass
class _SliceOrder:
    """
    Duck-typed stand-in for Order, carrying only what BrokerAdapter.submit_order
    reads -- scoped to one slice's quantity rather than the parent order's
    total, and always MARKET (child slices execute immediately; the parent
    order_type just selects the slicing strategy, not each slice's type).
    """
    id: UUID
    client_order_id: str
    symbol: object          # Symbol row -- adapters read .symbol.symbol
    side: str
    order_type: str
    qty: float
    price: float | None
    stop_price: float | None
    algo_config: dict | None


def _slice_plan(order_type: str, total_qty: float, algo_config: dict | None) -> tuple[list[float], float]:
    """Returns (slice quantities, interval_seconds between slices)."""
    cfg = algo_config or {}
    n = max(1, int(cfg.get("slices", 5)))
    interval = float(cfg.get("interval_seconds", 5.0))

    if order_type == "ICEBERG":
        display_qty = min(float(cfg.get("display_qty", total_qty / n)), total_qty)
        sizes: list[float] = []
        remaining = total_qty
        while remaining > 1e-9:
            take = min(display_qty, remaining)
            sizes.append(take)
            remaining -= take
        return sizes, interval

    if order_type == "VWAP":
        # No live intraday volume profile is wired in yet -- approximate with
        # a U-shaped participation curve (heavier at the first/last slices,
        # like real volume tends to be) rather than pretending equal slicing
        # is volume-weighted. Swap for a real volume-profile lookup once
        # market_data exposes one.
        weights = [1.2 - 0.4 * math.sin(math.pi * i / max(1, n - 1)) for i in range(n)]
        total_w = sum(weights)
        sizes = [total_qty * w / total_w for w in weights]
        sizes[-1] += total_qty - sum(sizes)  # last slice absorbs rounding remainder
        return sizes, interval

    # TWAP (default): equal slices at a fixed interval
    base = total_qty / n
    sizes = [base] * (n - 1) + [total_qty - base * (n - 1)]
    return sizes, interval


async def run_algo_order(order_id: UUID) -> None:
    """Background task entrypoint. Never raises -- errors are logged and
    leave the order in whatever partially-filled state it reached."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Order).options(selectinload(Order.symbol), selectinload(Order.broker))
            .where(Order.id == order_id)
        )
        order = result.scalar_one_or_none()
        if order is None:
            log.error("run_algo_order: order %s not found", order_id)
            return
        sizes, interval = _slice_plan(order.order_type, order.qty, order.algo_config)
        adapter = get_adapter(order.broker)
        symbol = order.symbol
        parent_client_order_id = order.client_order_id

    for i, slice_qty in enumerate(sizes):
        is_last = i == len(sizes) - 1

        async with AsyncSessionLocal() as db:
            # with_for_update() serializes against order_service.cancel_order(),
            # which takes the same lock -- see its comment for why. Without
            # it, a cancel racing this slice can lose the update: whichever
            # of us commits last wins the columns it touched, silently
            # corrupting the order (e.g. CANCELLED status with FILLED qty).
            result = await db.execute(
                select(Order).options(selectinload(Order.events))
                .where(Order.id == order_id)
                .with_for_update()
            )
            order = result.scalar_one_or_none()
            if order is None or order.status not in (OrderStatus.SUBMITTED, OrderStatus.PARTIAL):
                log.info("run_algo_order %s: stopping at slice %d (status=%s)",
                          order_id, i + 1, order.status if order else "MISSING")
                return

            slice_order = _SliceOrder(
                id=order.id,
                client_order_id=f"{parent_client_order_id}-S{i + 1}",
                symbol=symbol,
                side=order.side,
                order_type="MARKET",
                qty=slice_qty,
                price=order.price,
                stop_price=None,
                algo_config=order.algo_config,
            )

            try:
                broker_result = await adapter.submit_order(slice_order)
            except Exception as e:
                log.error("run_algo_order %s slice %d/%d failed: %s", order_id, i + 1, len(sizes), e)
                await write_audit(
                    db, action="ALGO_SLICE_FAILED", resource_type="order",
                    resource_id=str(order_id), actor_id=order.user_id, actor_email="system",
                    after_state={"slice": i + 1, "of": len(sizes), "error": str(e)},
                )
                await db.commit()
                return

            fill_price = float(broker_result.get("avg_price") or order.price or 0)
            commission = slice_qty * fill_price * 0.001
            db.add(Fill(
                order_id=order.id,
                symbol_id=symbol.id,
                side=order.side,
                qty=slice_qty,
                price=fill_price,
                commission=commission,
                total_cost=commission,
            ))
            # Same per-trader position bookkeeping as the instant-fill path
            # in order_service.submit_order -- every Fill must be netted.
            await apply_fill_to_position(
                db,
                user_id=order.user_id,
                broker_id=order.broker_id,
                strategy_id=order.strategy_id,
                symbol_id=symbol.id,
                side=order.side,
                qty=slice_qty,
                price=fill_price,
                commission=commission,
            )
            await write_pnl_snapshot(db, order.user_id)

            prior_notional = order.filled_qty * (order.avg_fill_price or 0)
            new_filled = order.filled_qty + slice_qty
            order.avg_fill_price = (prior_notional + slice_qty * fill_price) / new_filled
            order.filled_qty = new_filled
            order.broker_order_id = broker_result.get("broker_order_id") or order.broker_order_id

            if new_filled >= order.qty - 1e-9:
                order.transition("FILLED", f"Algo slice {i + 1}/{len(sizes)}")
                order.filled_at = datetime.now(timezone.utc)
            else:
                order.transition("PARTIAL", f"Algo slice {i + 1}/{len(sizes)}")

            user_id = order.user_id
            new_status = order.status
            await db.commit()

            # After (never before) the slice commits, nudge this trader's
            # open screens over the WS bridge -- see trade_events.py.
            await publish_order_event(
                user_id, order_id=order_id, status=new_status,
                symbol_name=getattr(symbol, "symbol", None), filled_qty=new_filled,
            )
            await publish_position_event(user_id, symbol_name=getattr(symbol, "symbol", None))

        if not is_last:
            await asyncio.sleep(interval)
