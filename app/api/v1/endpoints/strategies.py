# app/api/v1/endpoints/strategies.py
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user, require_quant
from app.db.session import get_db
from app.models.all_models import Strategy, BacktestJob, User
from app.schemas.all_schemas import (
    StrategyCreate, StrategyUpdate, StrategyOut,
    StrategyAdvanceRequest, BacktestJobCreate, BacktestJobOut,
    PaginatedResponse, MessageResponse,
)
from app.services.strategy_service import (
    compute_create_strategy, compute_advance_stage, retire_strategy,
)
from app.services.audit_service import write_audit

router = APIRouter(prefix="/strategies", tags=["strategies"])


@router.get("", response_model=PaginatedResponse)
async def list_strategies(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    stage: str | None = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    q = select(Strategy)
    # Non-admin users see only their own strategies
    if current_user.role not in ("admin",):
        q = q.where(Strategy.created_by == current_user.id)
    if stage:
        q = q.where(Strategy.lifecycle_stage == stage.upper())

    count_result = await db.execute(select(func.count()).select_from(q.subquery()))
    total = count_result.scalar_one()

    q = q.order_by(Strategy.updated_at.desc()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    strategies = result.scalars().all()

    return PaginatedResponse(
        items=[StrategyOut.model_validate(s) for s in strategies],
        total=total, page=page, page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.post("", response_model=StrategyOut, status_code=201)
async def create(
    data: StrategyCreate,
    current_user: User = Depends(require_quant),
    db: AsyncSession = Depends(get_db),
):
    strategy = await compute_create_strategy(db, data, current_user.id, current_user.email)
    await db.commit()
    return strategy


@router.get("/{strategy_id}", response_model=StrategyOut)
async def get_strategy(
    strategy_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Strategy)
        .options(selectinload(Strategy.backtest_jobs))
        .where(Strategy.id == strategy_id)
    )
    s = result.scalar_one_or_none()
    if not s:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if current_user.role != "admin" and s.created_by != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden")
    return s


@router.patch("/{strategy_id}", response_model=StrategyOut)
async def update_strategy(
    strategy_id: uuid.UUID,
    data: StrategyUpdate,
    current_user: User = Depends(require_quant),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Strategy).where(Strategy.id == strategy_id))
    s = result.scalar_one_or_none()
    if not s:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if current_user.role != "admin" and s.created_by != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden")

    if data.name:         s.name = data.name
    if data.hypothesis:   s.hypothesis = data.hypothesis
    if data.description:  s.description = data.description
    if data.config:       s.config = {**s.config, **data.config}
    if data.alpha_clock is not None: s.config = {**s.config, "alpha_clock": data.alpha_clock}
    if data.optimiser_mode is not None: s.config = {**s.config, "optimiser_mode": data.optimiser_mode}
    if data.risk_profile: s.risk_profile = {**s.risk_profile, **data.risk_profile}
    if data.tags:         s.tags = data.tags

    await write_audit(db, "STRATEGY_UPDATED", "strategy", str(strategy_id),
                      actor_id=current_user.id, actor_email=current_user.email)
    await db.commit()
    return s


@router.post("/{strategy_id}/advance", response_model=StrategyOut)
async def advance(
    strategy_id: uuid.UUID,
    data: StrategyAdvanceRequest,
    current_user: User = Depends(require_quant),
    db: AsyncSession = Depends(get_db),
):
    """Advance strategy to the next lifecycle stage (gate-checked)."""
    s = await compute_advance_stage(db, strategy_id, data, current_user.id, current_user.email)
    await db.commit()
    return s


@router.post("/{strategy_id}/retire", response_model=StrategyOut)
async def retire(
    strategy_id: uuid.UUID,
    data: StrategyAdvanceRequest,
    current_user: User = Depends(require_quant),
    db: AsyncSession = Depends(get_db),
):
    s = await retire_strategy(
        db, strategy_id, data.notes or "Manual retirement",
        current_user.id, current_user.email,
    )
    await db.commit()
    return s


@router.delete("/{strategy_id}", response_model=MessageResponse)
async def delete_strategy(
    strategy_id: uuid.UUID,
    current_user: User = Depends(require_quant),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Strategy).where(Strategy.id == strategy_id))
    s = result.scalar_one_or_none()
    if not s:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if current_user.role != "admin" and s.created_by != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden")
    if s.lifecycle_stage in ("LIVE_SMALL", "SCALED", "MONITOR"):
        raise HTTPException(status_code=409, detail="Cannot delete a live strategy — retire it first")
    await db.delete(s)
    await db.commit()
    return MessageResponse(message="Strategy deleted")


# ── Backtest ──────────────────────────────────────────────────────────────────

@router.post("/{strategy_id}/backtest", response_model=BacktestJobOut, status_code=202)
async def submit_backtest(
    strategy_id: uuid.UUID,
    data: BacktestJobCreate,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(require_quant),
    db: AsyncSession = Depends(get_db),
):
    """Queue a backtest job. Runs async via Celery worker."""
    result = await db.execute(select(Strategy).where(Strategy.id == strategy_id))
    s = result.scalar_one_or_none()
    if not s:
        raise HTTPException(status_code=404, detail="Strategy not found")

    job = BacktestJob(
        strategy_id=strategy_id,
        submitted_by=current_user.id,
        start_date=data.start_date,
        end_date=data.end_date,
        symbols=data.symbols,
        cost_model=data.cost_model,
        config=data.config,
    )
    db.add(job)
    await db.flush()
    job_id = str(job.id)

    # Dispatch to Celery
    try:
        from app.workers.backtest_worker import run_backtest_task
        task = run_backtest_task.delay(job_id)
        job.celery_task_id = task.id
    except Exception:
        # If Celery not running, mark for manual run
        job.celery_task_id = "no-celery"

    await write_audit(db, "BACKTEST_SUBMITTED", "backtest_job", job_id,
                      actor_id=current_user.id, actor_email=current_user.email)
    await db.commit()
    return job


@router.get("/{strategy_id}/backtest", response_model=list[BacktestJobOut])
async def list_backtest_jobs(
    strategy_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # capital_service.compute_rebalance() also writes rows into this same
    # table (config={"type": "REBALANCE"}) -- "the only job-tracking table
    # available", per that function's own docstring -- since BacktestJob.
    # strategy_id is NOT NULL, it attaches to whichever strategy the
    # triggering admin happens to own. Found live (2026-07-21): a strategy
    # with exactly 3 real backtests had 32,000+ rebalance rows attached the
    # same way, drowning the real results and picking a rebalance row as
    # "most recent" for _check_gate's PAPER-gate check too (strategy_service.
    # py) -- exclude them here with the identical predicate used there.
    result = await db.execute(
        select(BacktestJob)
        .where(
            BacktestJob.strategy_id == strategy_id,
            BacktestJob.config["type"].as_string().is_distinct_from("REBALANCE"),
        )
        .order_by(BacktestJob.created_at.desc())
    )
    return result.scalars().all()


@router.get("/backtest/{job_id}", response_model=BacktestJobOut)
async def get_backtest_job(
    job_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(BacktestJob).where(BacktestJob.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Backtest job not found")
    return job
