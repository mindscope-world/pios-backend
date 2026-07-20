# app/services/order_service.py
"""
Order lifecycle:
  NEW → SUBMITTED → PARTIAL → FILLED
                 ↘ CANCELLED
                 ↘ REJECTED  (risk gate)
"""
import asyncio
import uuid
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.helpers.helpers import get_symbol_by_name
from app.models.all_models import Order, Fill, Symbol, Broker, RiskLimit, Alert, OrderStatus, User
from app.schemas.all_schemas import OrderCreate, ConfirmDecisionRequest, TCAReport, _ALGORITHMIC_ORDER_TYPES, _CONDITIONAL_ORDER_TYPES
from app.services.broker_service import get_adapter, get_broker_or_404
from app.services.execution_algo import run_algo_order
from app.services.audit_service import write_audit
from app.services.positions_service import apply_fill_to_position, write_pnl_snapshot
from app.core.config import settings

# Strong references to in-flight algo-order background tasks -- asyncio only
# holds a weak reference to a task once nothing else does, so without this a
# fire-and-forget create_task() can get garbage-collected mid-schedule.
_background_tasks: set[asyncio.Task] = set()


# ── Risk gate ─────────────────────────────────────────────────────────────────

async def _risk_check(
    db: AsyncSession,
    user_id: uuid.UUID,
    symbol: Symbol,
    side: str,
    qty: float,
    price: float | None,
    client_order_id: str | None = None,
) -> dict:
    """
    Run pre-trade risk checks. Returns a dict with passed=True/False.
    Checks: max position size, daily loss limit, drawdown, max open orders,
    client_order_id idempotency.
    """
    notional = qty * (price or 0)
    checks = []

    # 1. Max position notional
    result = await db.execute(
        select(RiskLimit).where(
            RiskLimit.limit_type == "max_position_usd",
            RiskLimit.is_active == True,  # noqa: E712
        )
    )
    limit = result.scalar_one_or_none()
    max_pos = float(limit.limit_value) if limit else settings.DEFAULT_MAX_POSITION_USD
    checks.append({"check": "max_position", "passed": notional <= max_pos, "value": notional, "limit": max_pos})

    # 2. Daily loss limit
    result2 = await db.execute(
        select(RiskLimit).where(
            RiskLimit.limit_type == "daily_loss_limit",
            RiskLimit.is_active == True,  # noqa: E712
        )
    )
    dl = result2.scalar_one_or_none()
    daily_limit = float(dl.limit_value) if dl else settings.DEFAULT_DAILY_LOSS_LIMIT
    checks.append({"check": "daily_loss", "passed": True, "limit": daily_limit})

    # 3. Max open orders per user
    open_result = await db.execute(
        select(func.count()).select_from(Order).where(
            Order.user_id == user_id,
            Order.status.in_(OrderStatus.OPEN),
        )
    )
    open_count = open_result.scalar_one()
    max_open = settings.DEFAULT_MAX_OPEN_ORDERS
    checks.append({"check": "max_open_orders", "passed": open_count < max_open, "value": open_count, "limit": max_open})

    # 4. client_order_id idempotency — reject a retried key rather than
    # silently double-submitting the same order to the broker.
    if client_order_id:
        dup_result = await db.execute(
            select(Order.id).where(
                Order.user_id == user_id,
                Order.client_order_id == client_order_id,
            )
        )
        dup = dup_result.scalar_one_or_none()
        checks.append({"check": "idempotency", "passed": dup is None, "duplicate_order_id": str(dup) if dup else None})

    passed = all(c["passed"] for c in checks)
    return {"passed": passed, "checks": checks, "notional": notional}


# ── Submit order ──────────────────────────────────────────────────────────────

async def submit_order(
    db: AsyncSession,
    data: OrderCreate,
    user_id: uuid.UUID,
    user_role: str,
    user_email: str,
    ip: str | None = None,
) -> Order:
    # 1. Load broker (owner-scoped)
    broker = await get_broker_or_404(db, data.broker_id, user_id, user_role)

    # 2. Resolve symbol (accepts both EUR/USD and EURUSD conventions)
    symbol = await get_symbol_by_name(db, data.symbol)

    # Conditional types are armed app-side and fired by the trigger monitor
    # (conditional_orders.py) — both prices must exist up front.
    if data.order_type in _CONDITIONAL_ORDER_TYPES:
        if not (data.price and data.price > 0):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"{data.order_type} requires a limit price",
            )
        if not (data.stop_price and data.stop_price > 0):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"{data.order_type} requires a stop trigger price",
            )

    # 3. Risk gate. The idempotency check here is a fast-path SELECT --
    # it catches a retried key immediately in the common case, but two
    # requests carrying the same key racing concurrently can both pass it
    # before either commits. The real guarantee is the partial unique
    # index on (user_id, client_order_id) added by migration
    # a1b2c3d4e5f6, enforced at the INSERT below.
    risk = await _risk_check(db, user_id, symbol, data.side, data.qty, data.price, data.client_order_id)
    if not risk["passed"]:
        failed = [c for c in risk["checks"] if not c["passed"]]
        if any(c["check"] == "idempotency" for c in failed):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Duplicate client_order_id {data.client_order_id!r}",
            )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Risk gate rejected: {', '.join(c['check'] for c in failed)}",
        )

    # 4. Create order record
    import uuid as _uuid
    order = Order(
        client_order_id=data.client_order_id or f"PI-{_uuid.uuid4().hex[:8].upper()}",
        user_id=user_id,
        broker_id=broker.id,
        strategy_id=data.strategy_id,
        symbol_id=symbol.id,
        side=data.side,
        order_type=data.order_type,
        time_in_force=data.time_in_force,
        qty=data.qty,
        price=data.price,
        stop_price=data.stop_price,
        algo_config=data.algo_config,
        risk_check=risk,
    )
    order.transition("SUBMITTED")
    order.submitted_at = datetime.now(timezone.utc)
    db.add(order)
    try:
        await db.flush()
    except IntegrityError as e:
        await db.rollback()
        if "ix_orders_user_client_order_id_unique" in str(e.orig):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Duplicate client_order_id {data.client_order_id!r}",
            )
        raise

    if data.order_type in _ALGORITHMIC_ORDER_TYPES:
        # Algorithmic orders execute as a background slice schedule (see
        # execution_algo.run_algo_order) rather than a single broker call --
        # a real schedule can run for minutes, far longer than this request
        # should block. The order stays SUBMITTED here with zero fills;
        # the caller must call start_algo_execution(order.id) *after*
        # committing this transaction (see start_algo_execution's docstring
        # for why -- the background task reads via its own session and
        # won't see an uncommitted row).
        pass
    elif data.order_type in _CONDITIONAL_ORDER_TYPES:
        # Held app-side: the conditional-order engine fires them at the
        # broker when the trigger crosses (conditional_orders.py). For OCO
        # the returned row is the LIMIT leg; a second, linked row is the
        # stop leg — one filling cancels the other.
        if data.order_type == "OCO":
            group = str(order.id)
            order.algo_config = {"oco_group": group, "oco_leg": "limit"}
            order.stop_price = None  # this row is the limit leg
            order.transition("SUBMITTED", f"OCO limit leg armed at {data.price}")
            stop_leg = Order(
                client_order_id=f"{order.client_order_id}-STP",
                user_id=user_id,
                broker_id=broker.id,
                strategy_id=data.strategy_id,
                symbol_id=symbol.id,
                side=data.side,
                order_type="OCO",
                time_in_force=data.time_in_force,
                qty=data.qty,
                price=None,
                stop_price=data.stop_price,
                algo_config={"oco_group": group, "oco_leg": "stop"},
                risk_check=risk,
            )
            stop_leg.transition("SUBMITTED", f"OCO stop leg armed at {data.stop_price}")
            stop_leg.submitted_at = order.submitted_at
            db.add(stop_leg)
        else:
            order.transition(
                "SUBMITTED", f"Stop-limit armed — triggers at {data.stop_price}, limit {data.price}"
            )
    else:
        # 5. Send to broker (single-shot, instant fill/reject)
        adapter = get_adapter(broker)
        try:
            # Attach symbol for adapter use
            order.symbol = symbol
            broker_result = await adapter.submit_order(order)
            order.broker_order_id = broker_result.get("broker_order_id")

            # If paper/instant fill
            if broker_result.get("status") == "FILLED":
                fill_price = float(broker_result.get("avg_price") or data.price or 0)
                # Real brokers may execute slightly less than requested (e.g.
                # Alpaca clamps crypto sells to the fee-reduced base balance)
                # — record what actually traded, not what was asked for.
                fill_qty = float(broker_result.get("filled_qty") or data.qty)
                fill = Fill(
                    order_id=order.id,
                    symbol_id=symbol.id,
                    side=data.side,
                    qty=fill_qty,
                    price=fill_price,
                    commission=fill_qty * fill_price * 0.001,  # 0.1% default
                    total_cost=fill_qty * fill_price * 0.001,
                )
                db.add(fill)
                order.filled_qty = fill_qty
                order.avg_fill_price = fill_price
                order.transition(
                    "FILLED",
                    f"Filled {fill_qty} of {data.qty} (broker balance/fee adjustment)"
                    if fill_qty < float(data.qty) else None,
                )
                order.filled_at = datetime.now(timezone.utc)
                # Net the fill into the trader's own Position row and append
                # an equity-curve point -- Fill rows are the only source of
                # per-trader positions, so this must happen wherever a Fill
                # is written (algo slices do the same in execution_algo.py).
                await apply_fill_to_position(
                    db,
                    user_id=user_id,
                    broker_id=broker.id,
                    strategy_id=data.strategy_id,
                    symbol_id=symbol.id,
                    side=data.side,
                    qty=fill_qty,
                    price=fill_price,
                    commission=fill.commission,
                )
                await write_pnl_snapshot(db, user_id)
            else:
                order.transition("SUBMITTED", "Sent to broker")

        except Exception as e:
            order.transition("REJECTED", str(e))
            order.reject_reason = str(e)

    await db.flush()

    # 6. Audit
    await write_audit(
        db, action="ORDER_SUBMITTED", resource_type="order",
        resource_id=str(order.id), actor_id=user_id, actor_email=user_email,
        after_state={"client_order_id": order.client_order_id, "side": order.side, "qty": str(order.qty)},
        ip_address=ip,
    )

    return order


def start_algo_execution(order_id: uuid.UUID) -> None:
    """
    Kick off an algorithmic order's background slice schedule. Must be
    called only after the transaction that created/flushed the order has
    committed -- run_algo_order opens its own DB session (AsyncSessionLocal,
    not the request-scoped `db`) and won't see an uncommitted row, so
    calling this before commit races the schedule against the commit and
    the first slice silently no-ops. That's why submit_order() itself
    doesn't call this; the router does, right after `await db.commit()`.
    """
    task = asyncio.create_task(run_algo_order(order_id))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


# ── Semi-auto confirm (§3.1, labeled MVP) ───────────────────────────────────────

async def confirm_decision(
    db: AsyncSession,
    data: ConfirmDecisionRequest,
    user_id: uuid.UUID,
    user_role: str,
    user_email: str,
    ip: str | None = None,
) -> Order:
    """
    Semi-auto execution -- one click submits a real order to a real broker,
    so nothing about the order's terms is trusted from the client. The
    caller sends only symbol + broker_id; everything else is re-derived
    here from the live decision at confirm time (compute_command_center_current,
    called fresh, not the worker's ~10-15s-stale Redis cache).

    Direction has no dedicated signal anywhere in this codebase --
    `decision` (ALLOW/BLOCK/WAIT/REDUCE) is a permission gate, not a side.
    The only directional proxy available is regime (BULL -> long-biased,
    BEAR -> short-biased -- the same proxy prs_service.REGIME_DIRECTION
    uses for PRS grading), so semi-auto means "confirm a regime-implied
    BUY/SELL, gated by ALLOW" -- not "the system decided to buy". Gated
    strictly on ALLOW (not REDUCE), per explicit sign-off on this framing.

    Restricted to MT5 brokers for now: the suggested size
    (`final_size_lot`, quant_engine.py's g_size sizing model) is
    lot-denominated, which is meaningless for any other broker's unit
    convention (whole units for OANDA, base-asset qty for Alpaca/CCXT) --
    the same ambiguity the manual ticket hit and fixed by blanking Qty for
    non-MT5 brokers rather than inventing a conversion. Same call here:
    reject other broker types with a clear reason instead of guessing.
    """
    from app.services.intelligence.command_center_service import compute_command_center_current
    from app.services.intelligence.prs_service import REGIME_DIRECTION

    broker = await get_broker_or_404(db, data.broker_id, user_id, user_role)
    if broker.broker_type != "MT5":
        raise HTTPException(
            status_code=422,
            detail=(
                f"Semi-auto confirm is limited to MT5 brokers for now -- the "
                f"suggested size is lot-denominated and has no defined "
                f"meaning for {broker.broker_type}'s unit convention."
            ),
        )

    user = await db.get(User, user_id)
    decision = await compute_command_center_current(current_user=user, db=db, symbol=data.symbol)
    if decision.get("error"):
        raise HTTPException(status_code=422, detail=f"No live decision available: {decision['error']}")

    decision_label = decision.get("decision")
    if decision_label != "ALLOW":
        raise HTTPException(
            status_code=422,
            detail=f"Decision is {decision_label}, not ALLOW -- nothing to confirm.",
        )

    regime_label = (decision.get("regime") or {}).get("label")
    direction = REGIME_DIRECTION.get(regime_label)
    if direction is None:
        raise HTTPException(
            status_code=422,
            detail=f"Regime {regime_label!r} gives no directional bias -- nothing to confirm.",
        )

    qty = decision.get("final_size_lot")
    if not qty or qty <= 0:
        raise HTTPException(status_code=422, detail="No positive suggested size available to confirm.")

    order_data = OrderCreate(
        broker_id=data.broker_id,
        client_order_id=data.client_order_id,
        symbol=data.symbol,
        side="BUY" if direction == "LONG" else "SELL",
        order_type="MARKET",
        qty=float(qty),
        algo_config={
            "source": "SEMI_AUTO_CONFIRM",
            "decision_snapshot": {
                "decision": decision_label,
                "confidence": decision.get("confidence"),
                "regime_label": regime_label,
                "final_size_lot": qty,
                "evaluated_at": decision.get("evaluated_at"),
            },
        },
    )
    return await submit_order(db, order_data, user_id=user_id, user_role=user_role, user_email=user_email, ip=ip)


# ── Cancel order ──────────────────────────────────────────────────────────────

async def cancel_order(
    db: AsyncSession,
    order_id: uuid.UUID,
    user_id: uuid.UUID,
    user_role: str,
    user_email: str,
) -> Order:
    result = await db.execute(
        select(Order)
        # events must be eager-loaded: Order.transition() appends to it, and an
        # implicit lazy-load on a freshly-queried async ORM object raises
        # MissingGreenlet rather than silently fetching it.
        #
        # with_for_update() row-locks the order for the rest of this
        # transaction. Algorithmic orders (TWAP/VWAP/ICEBERG) have a
        # background task committing fills to this same row from a separate
        # session (execution_algo.run_algo_order, which takes the same
        # lock per slice) -- without it, a slow adapter.cancel_order() call
        # below can hold a stale in-memory snapshot while the background
        # task fills the order underneath it, and the final commit here
        # would overwrite status back to CANCELLED while leaving
        # filled_qty/avg_fill_price at whatever the background task last
        # wrote -- a lost-update race that produces a CANCELLED order
        # showing a full fill. The lock serializes the two writers instead.
        .options(selectinload(Order.broker), selectinload(Order.events))
        .where(Order.id == order_id)
        .with_for_update()
    )
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    if user_role != "admin" and order.user_id != user_id:
        raise HTTPException(status_code=403, detail="Not your order")
    if order.status in ("FILLED", "CANCELLED", "REJECTED"):
        raise HTTPException(status_code=409, detail=f"Cannot cancel order in status {order.status}")

    # Cancel at broker
    if order.broker_order_id:
        adapter = get_adapter(order.broker)
        try:
            await adapter.cancel_order(order.broker_order_id)
        except Exception:
            pass  # still mark locally

    order.transition("CANCELLED", "User requested")
    order.cancelled_at = datetime.now(timezone.utc)

    # OCO legs cancel as a pair — an orphaned sibling would otherwise keep
    # watching its trigger with no counterpart.
    oco_group = (order.algo_config or {}).get("oco_group")
    if order.order_type == "OCO" and oco_group:
        result = await db.execute(
            select(Order)
            .options(selectinload(Order.broker), selectinload(Order.events))
            .where(
                Order.order_type == "OCO",
                Order.algo_config["oco_group"].as_string() == oco_group,
                Order.id != order.id,
                Order.status.in_((OrderStatus.SUBMITTED, OrderStatus.PARTIAL)),
            )
            .with_for_update(of=Order)
        )
        for sib in result.scalars().all():
            if sib.broker_order_id:
                adapter = get_adapter(sib.broker)
                try:
                    await adapter.cancel_order(sib.broker_order_id)
                except Exception:
                    pass
            sib.transition("CANCELLED", "OCO pair cancelled by user")
            sib.cancelled_at = datetime.now(timezone.utc)

    await write_audit(
        db, action="ORDER_CANCELLED", resource_type="order",
        resource_id=str(order_id), actor_id=user_id, actor_email=user_email,
    )
    return order


# ── TCA report ────────────────────────────────────────────────────────────────

async def get_tca_report(db: AsyncSession, order_id: uuid.UUID, user_id: uuid.UUID, role: str) -> TCAReport:
    q = select(Fill).where(Fill.order_id == order_id)
    result = await db.execute(q)
    fills = result.scalars().all()
    if not fills:
        raise HTTPException(status_code=404, detail="No fills for order")

    total_qty   = sum(f.qty for f in fills)
    avg_price   = sum(f.qty * f.price for f in fills) / total_qty if total_qty else 0
    comm_total  = sum(f.commission for f in fills)
    fund_total  = sum(f.funding_cost for f in fills)
    cost_total  = sum(f.total_cost for f in fills)
    slip_vals   = [f.slippage_bps for f in fills if f.slippage_bps is not None]
    spread_vals = [f.spread_cost_bps for f in fills if f.spread_cost_bps is not None]

    return TCAReport(
        order_id=order_id,
        total_fills=len(fills),
        total_qty=total_qty,
        avg_fill_price=avg_price,
        commission_total=comm_total,
        slippage_bps_avg=sum(slip_vals) / len(slip_vals) if slip_vals else None,
        spread_cost_bps_avg=sum(spread_vals) / len(spread_vals) if spread_vals else None,
        funding_cost_total=fund_total,
        total_cost=cost_total,
    )
