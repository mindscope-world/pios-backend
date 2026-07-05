# app/services/audit_service.py
import json
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.all_models import AuditLog
from app.core.security import compute_record_hash


async def write_audit(
    db: AsyncSession,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
    actor_id: uuid.UUID | None = None,
    actor_email: str | None = None,
    before_state: dict | None = None,
    after_state: dict | None = None,
    ip_address: str | None = None,
    request_id: uuid.UUID | None = None,
) -> AuditLog:
    """Append an immutable audit entry with SHA-256 chain."""
    # Get last hash for chain
    result = await db.execute(
        select(AuditLog.record_hash)
        .order_by(AuditLog.id.desc())
        .limit(1)
    )
    prev_hash = result.scalar_one_or_none()

    # Build hash from key fields
    data_str = json.dumps({
        "action": action,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "actor_id": str(actor_id) if actor_id else None,
        "ts": datetime.now(timezone.utc).isoformat(),
    }, sort_keys=True)

    record_hash = compute_record_hash(data_str, prev_hash)

    entry = AuditLog(
        actor_id=actor_id,
        actor_email=actor_email,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        before_state=before_state,
        after_state=after_state,
        ip_address=ip_address,
        request_id=request_id,
        record_hash=record_hash,
        prev_hash=prev_hash,
    )
    db.add(entry)
    await db.flush()
    return entry
