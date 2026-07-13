# app/services/alpaca_fill_sync.py
"""
Background broker↔app order reconciliation for Alpaca.

AlpacaAdapter.submit_order only polls ~5s for a fill; a LIMIT that rests
past that window is handed back as SUBMITTED and — since Alpaca has no
get_fills() implementation — previously stayed SUBMITTED in the app forever
even after filling at the broker. This loop closes that gap: every
ALPACA_FILL_SYNC_INTERVAL_SECS it scans open (SUBMITTED/PARTIAL) non-algo
orders on ALPACA brokers, asks Alpaca for their current state, and applies
exactly the bookkeeping the instant-fill path does — Fill row(s) for the
newly executed delta, position netting, PnL snapshot, state transition, WS
push. When several fills land between two passes (stream disconnected, or
just unlucky timing), each is replayed as its own Fill at its own execution
price via Alpaca's Account Activities API (AlpacaAdapter.get_order_fills)
rather than collapsed into one delta at the broker's running average.

Runs inside the API process (started from main.py's lifespan), same as the
algo slice executor: per-iteration sessions, and each order is re-read
with_for_update so a user cancel racing the sync can't corrupt the row.
Broker-side cancels/expiries are mirrored too, so state converges in both
directions. Algo orders (TWAP/VWAP/ICEBERG) are excluded — their slices are
marketable and their parent status belongs to execution_algo.py.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.all_models import Broker, Fill, Order, OrderStatus
from app.services.broker_service import AlpacaAdapter, get_adapter
from app.services.positions_service import apply_fill_to_position, write_pnl_snapshot
from app.services.trade_events import publish_order_event, publish_position_event

log = logging.getLogger(__name__)

_ALGO_TYPES = ("TWAP", "VWAP", "ICEBERG")

# Broker statuses that end an order without (further) fills
_TERMINAL_UNFILLED = {"CANCELED": "CANCELLED", "EXPIRED": "EXPIRED", "REJECTED": "REJECTED"}


async def _candidate_order_ids(db) -> list:
    result = await db.execute(
        select(Order.id)
        .join(Broker, Broker.id == Order.broker_id)
        .where(
            Order.status.in_((OrderStatus.SUBMITTED, OrderStatus.PARTIAL)),
            Order.broker_order_id.isnot(None),
            Order.order_type.notin_(_ALGO_TYPES),
            Broker.broker_type == "ALPACA",
        )
    )
    return [row[0] for row in result.all()]


async def _sync_order(
    order_id,
    broker_state: dict | None = None,
    source: str = "fill-sync",
    execution: dict | None = None,
) -> None:
    """
    Reconcile one order against Alpaca. `broker_state` is injectable — the
    trade-update stream passes the event's order state (and its `source`
    label ends up in the state history); when None it's fetched from the
    broker inside the row lock so the status acted on is current at write
    time. `execution` is the stream event's per-execution print
    ({price, qty, id}) — when its qty accounts for the whole newly-filled
    delta, the Fill row records the actual print price instead of the
    broker's running average.
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
            if not isinstance(adapter, AlpacaAdapter):
                return
            broker_state = await adapter.get_order(order.broker_order_id)

        status = broker_state["status"]
        broker_filled = float(broker_state.get("filled_qty") or 0)
        avg_price = float(broker_state.get("avg_price") or 0)

        newly_filled = broker_filled - float(order.filled_qty or 0)
        changed = False

        if newly_filled > 1e-12 and avg_price > 0:
            # Stream events carry the actual execution print — when its qty
            # covers the whole delta unambiguously, that's one real Fill row
            # at its own price, no API call needed. Otherwise (poller-driven,
            # or several fills batched between two passes) replay Alpaca's
            # own per-execution activity records so each print gets its own
            # Fill row instead of collapsing them into one delta at the
            # broker's running average.
            if (
                execution
                and float(execution.get("price") or 0) > 0
                and abs(float(execution.get("qty") or 0) - newly_filled)
                <= max(newly_filled * 1e-6, 1e-12)
            ):
                executions = [{"qty": newly_filled, "price": float(execution["price"])}]
            else:
                adapter_for_fills = get_adapter(order.broker)
                executions = []
                if isinstance(adapter_for_fills, AlpacaAdapter):
                    prior_filled = float(order.filled_qty or 0)
                    raw = await adapter_for_fills.get_order_fills(
                        order.broker_order_id, since=order.created_at
                    )
                    running = 0.0
                    for f in raw:
                        prev_running = running
                        running += f["qty"]
                        if running <= prior_filled + 1e-9:
                            continue  # fully accounted for by an earlier sync
                        contributed = running - max(prev_running, prior_filled)
                        if contributed > 1e-12:
                            executions.append({"qty": contributed, "price": f["price"]})
                covered = sum(e["qty"] for e in executions)
                remainder = newly_filled - covered
                if remainder > 1e-9:
                    # Activities API came back short of the order's own
                    # cumulative filled_qty (a fetch failure, or Alpaca's
                    # eventual-consistency lag) — don't lose the fill,
                    # record what's left at the broker's running average.
                    executions.append({"qty": remainder, "price": avg_price})

            for ex in executions:
                ex_qty, ex_price = ex["qty"], ex["price"]
                commission = ex_qty * ex_price * 0.001
                db.add(Fill(
                    order_id=order.id,
                    symbol_id=order.symbol_id,
                    side=order.side,
                    qty=ex_qty,
                    price=ex_price,
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
                    qty=ex_qty,
                    price=ex_price,
                    commission=commission,
                )
                prior_notional = order.filled_qty * (order.avg_fill_price or 0)
                new_filled = order.filled_qty + ex_qty
                order.avg_fill_price = (prior_notional + ex_qty * ex_price) / new_filled
                order.filled_qty = new_filled

            await write_pnl_snapshot(db, order.user_id)
            changed = True

        if status == "FILLED":
            order.transition("FILLED", f"Broker {source}")
            order.filled_at = datetime.now(timezone.utc)
            changed = True
        elif status == "PARTIALLY_FILLED" and changed:
            order.transition("PARTIAL", f"Broker {source}")
        elif status in _TERMINAL_UNFILLED:
            app_status = _TERMINAL_UNFILLED[status]
            order.transition(app_status, f"Broker-side terminal state ({source})")
            if app_status == "REJECTED":
                order.reject_reason = order.reject_reason or f"Rejected at broker ({source})"
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
    log.info("alpaca_fill_sync: order %s → %s (filled %s)", order_id, new_status, filled_qty)


async def sweep_once(source: str = "fill-sync") -> int:
    """One reconciliation pass over every open Alpaca order. Also called by
    the trade-update stream on (re)connect, so anything that filled or
    cancelled while the socket was down is caught immediately instead of
    waiting out the poll interval."""
    async with AsyncSessionLocal() as db:
        order_ids = await _candidate_order_ids(db)
    for oid in order_ids:
        try:
            await _sync_order(oid, source=source)
        except Exception as e:  # noqa: BLE001
            log.warning("alpaca_fill_sync: order %s failed: %s", oid, e)
    return len(order_ids)


async def run_alpaca_fill_sync() -> None:
    """Loop entrypoint — never raises; per-order errors are logged and the
    order is retried on the next pass."""
    interval = max(5, int(settings.ALPACA_FILL_SYNC_INTERVAL_SECS))
    log.info("Alpaca fill-sync loop started (every %ss)", interval)
    while True:
        try:
            await sweep_once()
        except asyncio.CancelledError:
            log.info("Alpaca fill-sync loop stopped")
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("alpaca_fill_sync: pass failed: %s", e)
        await asyncio.sleep(interval)
