# app/api/v1/endpoints/capital.py
"""
Capital Allocation endpoints.

Thin wrappers around app.services.intelligence.capital_service, which is
also the intelligence worker's precompute path -- single source of truth
for the allocation math (asset-level HRP slices + V10.4 D.2 clock-band
exposure). Previously this endpoint had its own fully independent,
drifted copy of the same logic (its gmig_modifier was hardcoded 1.0 while
capital_service.py did a real GMIG lookup); collapsed here so clock bands
(and any future change) only need to land in one place.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, require_admin
from app.db.session import get_db
from app.models.all_models import User
from app.services.intelligence.capital_service import compute_capital_allocation, compute_rebalance

router = APIRouter(prefix="/capital", tags=["capital"])


@router.get("/allocation")
async def capital_allocation(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    return await compute_capital_allocation(current_user, db)


@router.post("/rebalance")
async def trigger_rebalance(
    admin: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    return await compute_rebalance(admin, db)
