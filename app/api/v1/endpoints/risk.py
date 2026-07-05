# app/api/v1/endpoints/risk.py
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, require_admin
from app.db.session import get_db
from app.models.all_models import User, RiskLimit, KillSwitchEvent
from app.schemas.all_schemas import (
    RiskMetricsOut, RiskLimitCreate, RiskLimitUpdate, RiskLimitOut,
    KillSwitchRequest, KillSwitchEventOut, MessageResponse,
)
from app.services.risk_service import compute_risk_metrics, trigger_kill_switch
from app.services.audit_service import write_audit

router = APIRouter(prefix="/risk", tags=["risk"])


@router.get("/metrics", response_model=RiskMetricsOut)
async def risk_metrics(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await compute_risk_metrics(db, current_user.id)


@router.get("/limits", response_model=list[RiskLimitOut])
async def list_limits(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(RiskLimit).where(RiskLimit.is_active == True).order_by(RiskLimit.id)  # noqa: E712
    )
    return result.scalars().all()


@router.post("/limits", response_model=RiskLimitOut, status_code=201)
async def create_limit(
    data: RiskLimitCreate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    limit = RiskLimit(
        name=data.name,
        scope=data.scope,
        scope_id=data.scope_id,
        limit_type=data.limit_type,
        limit_value=data.limit_value,
        breach_action=data.breach_action,
        created_by=admin.id,
    )
    db.add(limit)
    await db.flush()
    await write_audit(db, "RISK_LIMIT_CREATED", "risk_limit", str(limit.id),
                      actor_id=admin.id, actor_email=admin.email,
                      after_state={"name": limit.name, "value": limit.limit_value})
    await db.commit()
    return limit


@router.patch("/limits/{limit_id}", response_model=RiskLimitOut)
async def update_limit(
    limit_id: int,
    data: RiskLimitUpdate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(RiskLimit).where(RiskLimit.id == limit_id))
    limit = result.scalar_one_or_none()
    if not limit:
        raise HTTPException(status_code=404, detail="Risk limit not found")

    before = {"value": float(limit.limit_value), "action": limit.breach_action}
    if data.limit_value is not None:  limit.limit_value = data.limit_value
    if data.breach_action is not None: limit.breach_action = data.breach_action
    if data.is_active is not None:    limit.is_active = data.is_active

    await write_audit(db, "RISK_LIMIT_UPDATED", "risk_limit", str(limit_id),
                      actor_id=admin.id, actor_email=admin.email,
                      before_state=before,
                      after_state={"value": float(limit.limit_value), "action": limit.breach_action})
    await db.commit()
    return limit


@router.delete("/limits/{limit_id}", response_model=MessageResponse)
async def delete_limit(
    limit_id: int,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(RiskLimit).where(RiskLimit.id == limit_id))
    limit = result.scalar_one_or_none()
    if not limit:
        raise HTTPException(status_code=404, detail="Risk limit not found")
    limit.is_active = False
    await write_audit(db, "RISK_LIMIT_DELETED", "risk_limit", str(limit_id),
                      actor_id=admin.id, actor_email=admin.email)
    await db.commit()
    return MessageResponse(message="Risk limit deactivated")


@router.post("/killswitch", response_model=KillSwitchEventOut, status_code=202)
async def kill_switch(
    data: KillSwitchRequest,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """
    Cancel ALL open orders + close ALL positions for the admin's account.
    Requires MFA if enabled. Writes immutable audit entry.
    """
    from app.core.security import decrypt_credentials
    import pyotp
    if admin.mfa_enabled and data.mfa_code:
        secret = decrypt_credentials(admin.mfa_secret_enc)
        totp = pyotp.TOTP(secret)
        if not totp.verify(data.mfa_code, valid_window=1):
            raise HTTPException(status_code=400, detail="Invalid MFA code")
    elif admin.mfa_enabled and not data.mfa_code:
        raise HTTPException(status_code=400, detail="MFA code required for kill switch")

    event = await trigger_kill_switch(db, data, admin.id, admin.email)
    await db.commit()
    return event


@router.get("/killswitch/history", response_model=list[KillSwitchEventOut])
async def kill_switch_history(
    limit: int = Query(20, ge=1, le=100),
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(KillSwitchEvent)
        .where(KillSwitchEvent.triggered_by == admin.id)
        .order_by(KillSwitchEvent.created_at.desc())
        .limit(limit)
    )
    return result.scalars().all()
