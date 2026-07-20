# app/api/v1/endpoints/orders.py
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, require_trade_exec
from app.db.session import get_db
from app.models.all_models import Order, Fill, User
from app.schemas.all_schemas import (
    OrderCreate, ConfirmDecisionRequest, OrderOut, FillOut, TCAReport,
    CancelOrderResponse, PaginatedResponse,
)
from app.schemas.all_schemas import _ALGORITHMIC_ORDER_TYPES
from app.services.order_service import submit_order, confirm_decision, cancel_order, get_tca_report, start_algo_execution
from app.services.trade_events import publish_order_event, publish_position_event

router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("", response_model=OrderOut, status_code=201)
async def create_order(
    data: OrderCreate,
    request: Request,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    """
    Submit a new order. Runs risk gate before sending to broker.
    Supports: MARKET, LIMIT, STOP, STOP_LIMIT, OCO, TWAP, VWAP, ICEBERG.

    TWAP/VWAP/ICEBERG execute algorithmically via a background slice
    schedule (app/services/execution_algo.py) -- the response comes back
    SUBMITTED with zero fills and fills accumulate over the schedule.
    Everything else (incl. OCO/STOP_LIMIT) executes as a single broker
    call. Check the response's `execution_style` field ("INSTANT" vs
    "ALGORITHMIC") to know which path an order took.
    """
    ip = request.client.host if request.client else None
    order = await submit_order(
        db, data,
        user_id=current_user.id,
        user_role=current_user.role,
        user_email=current_user.email,
        ip=ip,
    )
    await db.commit()
    if data.order_type in _ALGORITHMIC_ORDER_TYPES and order.status == "SUBMITTED":
        start_algo_execution(order.id)
    # Post-commit WS nudges (user-scoped; see trade_events.py). Instant fills
    # changed positions too; algo orders publish per slice from execution_algo.
    await publish_order_event(
        current_user.id, order_id=order.id, status=order.status,
        symbol_name=data.symbol, filled_qty=float(order.filled_qty or 0),
    )
    if order.status == "FILLED":
        await publish_position_event(current_user.id, symbol_name=data.symbol)
    return await _load_order(db, order.id)


@router.post("/confirm-decision", response_model=OrderOut, status_code=201)
async def confirm_decision_order(
    data: ConfirmDecisionRequest,
    request: Request,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    """
    Semi-auto execution (§3.1) -- confirms the *current live* decision for
    `symbol`, not whatever the frontend last rendered. Side/qty/whether
    it's even allowed to fire are re-derived server-side; see
    order_service.confirm_decision for exactly what that means and why.
    """
    ip = request.client.host if request.client else None
    order = await confirm_decision(
        db, data,
        user_id=current_user.id,
        user_role=current_user.role,
        user_email=current_user.email,
        ip=ip,
    )
    await db.commit()
    await publish_order_event(
        current_user.id, order_id=order.id, status=order.status,
        symbol_name=data.symbol, filled_qty=float(order.filled_qty or 0),
    )
    if order.status == "FILLED":
        await publish_position_event(current_user.id, symbol_name=data.symbol)
    return await _load_order(db, order.id)


@router.get("", response_model=PaginatedResponse)
async def list_orders(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: str | None = None,
    symbol: str | None = None,
    strategy_id: uuid.UUID | None = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    q = (
        select(Order)
        .options(selectinload(Order.symbol))
        .where(Order.user_id == current_user.id)
    )
    if current_user.role == "admin":
        q = select(Order).options(selectinload(Order.symbol))

    if status:
        q = q.where(Order.status == status.upper())
    if strategy_id:
        q = q.where(Order.strategy_id == strategy_id)

    count_result = await db.execute(select(func.count()).select_from(q.subquery()))
    total = count_result.scalar_one()

    q = q.order_by(Order.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    orders = result.scalars().all()

    return PaginatedResponse(
        items=[OrderOut.model_validate(o) for o in orders],
        total=total, page=page, page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.get("/{order_id}", response_model=OrderOut)
async def get_order(
    order_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    order = await _load_order(db, order_id)
    if current_user.role != "admin" and order.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden")
    return order


@router.delete("/{order_id}", response_model=CancelOrderResponse)
async def cancel(
    order_id: uuid.UUID,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    order = await cancel_order(
        db, order_id,
        user_id=current_user.id,
        user_role=current_user.role,
        user_email=current_user.email,
    )
    await db.commit()
    await publish_order_event(current_user.id, order_id=order.id, status=order.status)
    return CancelOrderResponse(order_id=order.id, status=order.status, message="Order cancelled")


@router.get("/{order_id}/fills", response_model=list[FillOut])
async def get_fills(
    order_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Fill)
        .where(Fill.order_id == order_id)
        .order_by(Fill.filled_at)
    )
    return result.scalars().all()


@router.get("/{order_id}/tca", response_model=TCAReport)
async def tca_report(
    order_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Transaction cost analysis for a completed order."""
    return await get_tca_report(db, order_id, current_user.id, current_user.role)


# ── fills (global list) ───────────────────────────────────────────────────────

@router.get("/fills/all", response_model=PaginatedResponse)
async def list_all_fills(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List all fills for the current user across all orders."""
    q = (
        select(Fill)
        .join(Order, Fill.order_id == Order.id)
        .where(Order.user_id == current_user.id)
        .order_by(Fill.filled_at.desc())
    )
    count_result = await db.execute(select(func.count()).select_from(q.subquery()))
    total = count_result.scalar_one()
    q = q.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    fills = result.scalars().all()

    return PaginatedResponse(
        items=[FillOut.model_validate(f) for f in fills],
        total=total, page=page, page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


async def _load_order(db: AsyncSession, order_id: uuid.UUID) -> Order:
    result = await db.execute(
        select(Order)
        .options(selectinload(Order.symbol), selectinload(Order.fills))
        .where(Order.id == order_id)
    )
    order = result.scalar_one_or_none()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order
