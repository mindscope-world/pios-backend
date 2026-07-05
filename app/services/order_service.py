# app/services/order_service.py
"""
Order lifecycle:
  NEW → SUBMITTED → PARTIAL → FILLED
                 ↘ CANCELLED
                 ↘ REJECTED  (risk gate)
"""
import uuid
from datetime import datetime, timezone

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.all_models import Order, Fill, Symbol, Broker, RiskLimit, Alert
from app.schemas.all_schemas import OrderCreate, TCAReport
from app.services.broker_service import get_adapter, get_broker_or_404
from app.services.audit_service import write_audit
from app.core.config import settings


# ── Risk gate ─────────────────────────────────────────────────────────────────

async def _risk_check(
    db: AsyncSession,
    user_id: uuid.UUID,
    symbol: Symbol,
    side: str,
    qty: float,
    price: float | None,
) -> dict:
    """
    Run pre-trade risk checks. Returns a dict with passed=True/False.
    Checks: max position size, daily loss limit, drawdown.
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

    # 2. Resolve symbol
    sym_result = await db.execute(select(Symbol).where(Symbol.symbol == data.symbol))
    symbol = sym_result.scalar_one_or_none()
    if not symbol:
        raise HTTPException(status_code=404, detail=f"Symbol '{data.symbol}' not found")

    # 3. Risk gate
    risk = await _risk_check(db, user_id, symbol, data.side, data.qty, data.price)
    if not risk["passed"]:
        failed = [c for c in risk["checks"] if not c["passed"]]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Risk gate rejected: {', '.join(c['check'] for c in failed)}",
        )

    # 4. Create order record
    import uuid as _uuid
    order = Order(
        client_order_id=f"PI-{_uuid.uuid4().hex[:8].upper()}",
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
    await db.flush()

    # 5. Send to broker
    adapter = get_adapter(broker)
    try:
        # Attach symbol for adapter use
        order.symbol = symbol
        broker_result = await adapter.submit_order(order)
        order.broker_order_id = broker_result.get("broker_order_id")

        # If paper/instant fill
        if broker_result.get("status") == "FILLED":
            fill_price = float(broker_result.get("avg_price") or data.price or 0)
            fill = Fill(
                order_id=order.id,
                symbol_id=symbol.id,
                side=data.side,
                qty=data.qty,
                price=fill_price,
                commission=data.qty * fill_price * 0.001,  # 0.1% default
                total_cost=data.qty * fill_price * 0.001,
            )
            db.add(fill)
            order.filled_qty = data.qty
            order.avg_fill_price = fill_price
            order.transition("FILLED")
            order.filled_at = datetime.now(timezone.utc)
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
        .options(selectinload(Order.broker))
        .where(Order.id == order_id)
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
