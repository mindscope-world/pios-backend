# app/api/v1/endpoints/brokers.py
import uuid
import json
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, require_trade_exec
from app.core.security import encrypt_credentials
from app.db.session import get_db
from app.models.all_models import Broker, User
from app.schemas.all_schemas import (
    BrokerCreate, BrokerUpdate, BrokerOut,
    BrokerTestResult, PaginatedResponse, MessageResponse,
)
from app.services.broker_service import create_broker, get_adapter, get_broker_or_404
from app.services.audit_service import write_audit

router = APIRouter(prefix="/brokers", tags=["brokers"])


@router.get("", response_model=PaginatedResponse)
async def list_brokers(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Traders see only their own brokers.
    Admins see all brokers across all traders.
    """
    q = select(Broker).where(Broker.is_active == True)  # noqa: E712
    if current_user.role != "admin":
        q = q.where(Broker.owner_id == current_user.id)

    count_result = await db.execute(select(func.count()).select_from(q.subquery()))
    total = count_result.scalar_one()

    q = q.order_by(Broker.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    brokers = result.scalars().all()

    return PaginatedResponse(
        items=[BrokerOut.model_validate(b) for b in brokers],
        total=total, page=page, page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.post("", response_model=BrokerOut, status_code=201)
async def add_broker(
    data: BrokerCreate,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    """
    Traders register their own broker connection.
    Credentials are AES-256 encrypted before storage.
    """
    print(f"Data: {data}")
    
    broker = await create_broker(db, data, current_user.id)
    print(f"Created broker2 {broker.name} for user {current_user.email}")
    print(f"Broker config2: {broker.config}")
    print(f"Data2: {data}")
    await write_audit(
        db, "BROKER_ADDED", "broker", str(broker.id),
        actor_id=current_user.id, actor_email=current_user.email,
        after_state={"name": broker.name, "type": broker.broker_type, "is_paper": broker.is_paper},
    )
    await db.commit()
    return broker


@router.get("/{broker_id}", response_model=BrokerOut)
async def get_broker(
    broker_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)
    return broker


@router.patch("/{broker_id}", response_model=BrokerOut)
async def update_broker(
    broker_id: uuid.UUID,
    data: BrokerUpdate,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)

    if data.name is not None:
        broker.name = data.name
    if data.is_active is not None:
        broker.is_active = data.is_active
    if data.config is not None:
        broker.config = {**broker.config, **data.config}
    if data.credentials is not None:
        broker.credentials_enc = encrypt_credentials(json.dumps(data.credentials.model_dump()))

    await write_audit(db, "BROKER_UPDATED", "broker", str(broker_id),
                      actor_id=current_user.id, actor_email=current_user.email)
    await db.commit()
    return broker


@router.delete("/{broker_id}", response_model=MessageResponse)
async def remove_broker(
    broker_id: uuid.UUID,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)
    broker.is_active = False
    await write_audit(db, "BROKER_REMOVED", "broker", str(broker_id),
                      actor_id=current_user.id, actor_email=current_user.email)
    await db.commit()
    return MessageResponse(message="Broker removed")


@router.post("/{broker_id}/test", response_model=BrokerTestResult)
async def test_broker(
    broker_id: uuid.UUID,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    """Test connectivity and measure latency for this broker."""
    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)
    adapter = get_adapter(broker)
    result = await adapter.test_connection()

    from datetime import datetime, timezone
    broker.status = "CONNECTED" if result.success else "ERROR"
    broker.last_heartbeat = datetime.now(timezone.utc)
    broker.latency_p99_ms = result.latency_ms
    broker.error_message = None if result.success else result.message
    await db.commit()

    return result


@router.get("/{broker_id}/account", response_model=dict)
async def broker_account(
    broker_id: uuid.UUID,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    """Fetch live account balance / buying power from broker."""
    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)
    adapter = get_adapter(broker)
    try:
        return await adapter.get_account()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Broker error: {e}")
