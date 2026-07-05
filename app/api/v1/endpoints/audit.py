# app/api/v1/endpoints/audit.py
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import require_audit
from app.db.session import get_db
from app.models.all_models import AuditLog, User
from app.schemas.all_schemas import AuditEntryOut, PaginatedResponse

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("", response_model=PaginatedResponse)
async def list_audit(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    action: str | None = None,
    resource_type: str | None = None,
    actor_email: str | None = None,
    _: User = Depends(require_audit),
    db: AsyncSession = Depends(get_db),
):
    q = select(AuditLog)
    if action:
        q = q.where(AuditLog.action.ilike(f"%{action}%"))
    if resource_type:
        q = q.where(AuditLog.resource_type == resource_type)
    if actor_email:
        q = q.where(AuditLog.actor_email.ilike(f"%{actor_email}%"))

    count_result = await db.execute(select(func.count()).select_from(q.subquery()))
    total = count_result.scalar_one()

    q = q.order_by(AuditLog.event_time.desc()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    entries = result.scalars().all()

    return PaginatedResponse(
        items=[AuditEntryOut.model_validate(e) for e in entries],
        total=total, page=page, page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.get("/verify", response_model=dict)
async def verify_chain(
    _: User = Depends(require_audit),
    db: AsyncSession = Depends(get_db),
):
    """Verify SHA-256 chain integrity. Returns first broken link if found."""
    from app.core.security import compute_record_hash
    import json

    result = await db.execute(select(AuditLog).order_by(AuditLog.id))
    entries = result.scalars().all()

    broken_at = None
    prev_hash = None
    for entry in entries:
        data_str = json.dumps({
            "action": entry.action,
            "resource_type": entry.resource_type,
            "resource_id": entry.resource_id,
            "actor_id": str(entry.actor_id) if entry.actor_id else None,
            "ts": entry.event_time.isoformat(),
        }, sort_keys=True)
        expected = compute_record_hash(data_str, prev_hash)
        if expected != entry.record_hash:
            broken_at = entry.id
            break
        prev_hash = entry.record_hash

    return {
        "chain_intact": broken_at is None,
        "total_entries": len(entries),
        "broken_at_id": broken_at,
    }
