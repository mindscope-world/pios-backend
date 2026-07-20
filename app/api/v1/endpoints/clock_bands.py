# app/api/v1/endpoints/clock_bands.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, require_admin
from app.db.session import get_db
from app.models.all_models import User, ClockWeightBand
from app.schemas.all_schemas import (
    ClockWeightBandCreate, ClockWeightBandUpdate, ClockWeightBandOut, MessageResponse,
)
from app.services.audit_service import write_audit

router = APIRouter(prefix="/clock-bands", tags=["clock-bands"])


@router.get("", response_model=list[ClockWeightBandOut])
async def list_bands(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(ClockWeightBand).where(ClockWeightBand.is_active == True).order_by(ClockWeightBand.id)  # noqa: E712
    )
    return result.scalars().all()


@router.post("", response_model=ClockWeightBandOut, status_code=201)
async def create_band(
    data: ClockWeightBandCreate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    if data.min_pct > data.max_pct:
        raise HTTPException(status_code=422, detail="min_pct cannot exceed max_pct")
    band = ClockWeightBand(
        clock=data.clock,
        regime=data.regime,
        min_pct=data.min_pct,
        max_pct=data.max_pct,
        created_by=admin.id,
    )
    db.add(band)
    await db.flush()
    await write_audit(db, "CLOCK_BAND_CREATED", "clock_weight_band", str(band.id),
                      actor_id=admin.id, actor_email=admin.email,
                      after_state={"clock": band.clock, "regime": band.regime,
                                   "min_pct": float(band.min_pct), "max_pct": float(band.max_pct)})
    await db.commit()
    return band


@router.patch("/{band_id}", response_model=ClockWeightBandOut)
async def update_band(
    band_id: int,
    data: ClockWeightBandUpdate,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(ClockWeightBand).where(ClockWeightBand.id == band_id))
    band = result.scalar_one_or_none()
    if not band:
        raise HTTPException(status_code=404, detail="Clock weight band not found")

    before = {"min_pct": float(band.min_pct), "max_pct": float(band.max_pct), "is_active": band.is_active}
    new_min = data.min_pct if data.min_pct is not None else float(band.min_pct)
    new_max = data.max_pct if data.max_pct is not None else float(band.max_pct)
    if new_min > new_max:
        raise HTTPException(status_code=422, detail="min_pct cannot exceed max_pct")
    band.min_pct = new_min
    band.max_pct = new_max
    if data.is_active is not None:
        band.is_active = data.is_active

    await write_audit(db, "CLOCK_BAND_UPDATED", "clock_weight_band", str(band_id),
                      actor_id=admin.id, actor_email=admin.email,
                      before_state=before,
                      after_state={"min_pct": float(band.min_pct), "max_pct": float(band.max_pct), "is_active": band.is_active})
    await db.commit()
    # Same MissingGreenlet trap as RiskLimit's PATCH -- updated_at is
    # DB-computed (onupdate=func.now()), commit() expires it, refresh before
    # serializing the response.
    await db.refresh(band)
    return band


@router.delete("/{band_id}", response_model=MessageResponse)
async def delete_band(
    band_id: int,
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(ClockWeightBand).where(ClockWeightBand.id == band_id))
    band = result.scalar_one_or_none()
    if not band:
        raise HTTPException(status_code=404, detail="Clock weight band not found")
    band.is_active = False
    await write_audit(db, "CLOCK_BAND_DELETED", "clock_weight_band", str(band_id),
                      actor_id=admin.id, actor_email=admin.email)
    await db.commit()
    return MessageResponse(message="Clock weight band deactivated")
