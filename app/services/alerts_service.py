import uuid
from datetime import datetime, timezone
from fastapi import HTTPException, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.all_models import Alert, User
from app.schemas.all_schemas import AlertOut, AlertAckRequest, PaginatedResponse, MessageResponse


async def compute_list_alerts(
    current_user: User,
    db: AsyncSession,
    page: int = 1,
    page_size: int = 50,
    severity: str | None = None,
    acked: bool | None = None,
    source: str | None = None,
):
    q = select(Alert)
    if severity:
        q = q.where(Alert.severity == severity.upper())
    if acked is not None:
        q = q.where(Alert.is_acknowledged == acked)
    if source:
        q = q.where(Alert.source == source.upper())

    count_result = await db.execute(select(func.count()).select_from(q.subquery()))
    total = count_result.scalar_one()

    q = (
        q.order_by(
            Alert.severity.asc(),           # P1 first
            Alert.created_at.desc(),
        )
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(q)
    alerts = result.scalars().all()

    return PaginatedResponse(
        items=[AlertOut.model_validate(a) for a in alerts],
        total=total, page=page, page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


async def compute_get_alert(
    current_user: User,
    db: AsyncSession,
    alert_id: uuid.UUID,

):
    from fastapi import HTTPException
    result = await db.execute(select(Alert).where(Alert.id == alert_id))
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


async def compute_acknowledge_alert(
    current_user: User,
    db: AsyncSession,
    alert_id: uuid.UUID,
    data: AlertAckRequest
):
    result = await db.execute(select(Alert).where(Alert.id == alert_id))
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")

    alert.is_acknowledged = True
    alert.acknowledged_by = current_user.id
    alert.acknowledged_at = datetime.now(timezone.utc)
    alert.ack_note = data.note
    await db.commit()
    return alert


async def compute_acknowledge_all(
    current_user: User,
    db: AsyncSession
):
    result = await db.execute(
        select(Alert).where(Alert.is_acknowledged == False)  # noqa: E712
    )
    alerts = result.scalars().all()
    now = datetime.now(timezone.utc)
    for alert in alerts:
        alert.is_acknowledged = True
        alert.acknowledged_by = current_user.id
        alert.acknowledged_at = now
    await db.commit()
    return MessageResponse(message=f"Acknowledged {len(alerts)} alerts")
