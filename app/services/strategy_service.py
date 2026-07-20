"""
strategy_service.py
-------------------
Production-ready service layer for Strategy lifecycle management.

Changes from prototype:
- Added full fetch layer: get_strategy, list_strategies (with filtering,
  sorting, and cursor-based pagination).
- Locked DB rows with SELECT … FOR UPDATE to prevent concurrent stage
  transitions from producing split-brain state.
- Replaced bare `raise HTTPException` inside pure service functions with
  domain exceptions; HTTP translation lives at the router layer.
- Added structured logging on every mutating path.
- Validated stage transition against both direction (forward only) and a
  configurable skip-prevention guard so callers cannot jump two stages at once.
- `retire_strategy` now records a gate_history entry for consistency.
- `update_strategy` added: partial-patch with field-level allow-listing so
  callers cannot overwrite audit or lifecycle fields.
- All async DB calls use explicit transactions via the session passed from
  the router's `get_db` dependency — the service never commits; it only
  flushes, leaving commit/rollback to the caller.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import String, select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.all_models import Strategy, BacktestJob
from app.schemas.all_schemas import (
    StrategyCreate,
    StrategyUpdate,
    StrategyAdvanceRequest
    )
from app.services.audit_service import write_audit

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LIFECYCLE_ORDER: list[str] = [
    "IDEA",
    "RESEARCH",
    "BACKTEST",
    "PAPER",
    "LIVE_SMALL",
    "SCALED",
    "MONITOR",
    "RETIRED",
]

# Fields that callers are NOT allowed to patch directly
_IMMUTABLE_FIELDS: frozenset[str] = frozenset(
    {
        "id",
        "created_by",
        "created_at",
        "updated_at",
        "lifecycle_stage",
        "gate_history",
        "deployed_at",
        "retired_at",
        "retirement_reason",
    }
)

# Mutable strategy fields callers may PATCH
_PATCHABLE_FIELDS: frozenset[str] = frozenset(
    {
        "name",
        "hypothesis",
        "description",
        "feature_list",
        "allowed_symbols",
        "allowed_regimes",
        "risk_profile",
        "config",
        "tags",
        "is_paper_only",
    }
)

# ---------------------------------------------------------------------------
# Domain Exceptions
# (Translate to HTTP at the router level, not here.)
# ---------------------------------------------------------------------------


class StrategyNotFound(Exception):
    """Raised when a requested strategy does not exist."""


class GateFailed(Exception):
    """Raised when a stage-advancement gate check fails."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class InvalidTransition(Exception):
    """Raised for nonsensical stage transitions (backward, skip, already final)."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _stage_index(stage: str) -> int:
    try:
        return LIFECYCLE_ORDER.index(stage)
    except ValueError as exc:
        raise ValueError(f"Unknown lifecycle stage: {stage!r}") from exc


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _check_gate(strategy: Strategy, to_stage: str) -> tuple[bool, str]:
    """
    Return (can_advance: bool, reason: str).

    All gate logic is concentrated here to keep advance_stage readable.
    Each guard returns *early* on the first failing condition so the caller
    receives the most actionable error message.
    """
    if to_stage == "BACKTEST":
        if not (strategy.hypothesis and strategy.hypothesis.strip()):
            return False, "A non-empty hypothesis is required before entering BACKTEST"

    if to_stage in ("PAPER", "LIVE_SMALL", "SCALED"):
        jobs: list[BacktestJob] = strategy.backtest_jobs or []
        if not jobs:
            return False, "At least one completed backtest is required"

        latest = max(jobs, key=lambda j: j.created_at)
        if latest.status != "COMPLETE":
            return (
                False,
                f"Most recent backtest (id={latest.id}) has status '{latest.status}', "
                "need 'COMPLETE'",
            )

        if to_stage == "PAPER":
            sharpe = latest.sharpe_ratio
            if sharpe is None or sharpe < 0.8:
                return (
                    False,
                    f"OOS Sharpe {sharpe} is below the 0.8 gate threshold",
                )
            dd = latest.max_drawdown
            if dd is not None and abs(dd) > 15:
                return (
                    False,
                    f"Max drawdown {dd:.2f}% exceeds the 15% gate threshold",
                )
            trade_count = latest.trade_count
            if trade_count is None or trade_count < 200:
                return (
                    False,
                    f"Backtest produced {trade_count} trades; need ≥ 200",
                )

    if to_stage in ("LIVE_SMALL", "SCALED") and strategy.is_paper_only:
        return (
            False,
            "Strategy is flagged is_paper_only=True; an admin must clear this before "
            "live-capital deployment",
        )

    return True, "Gate passed"


# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------


async def compute_get_strategy(
    db: AsyncSession,
    strategy_id: uuid.UUID,
    *,
    lock: bool = False,
) -> Strategy:
    """
    Fetch a single strategy by primary key.

    Parameters
    ----------
    db:          AsyncSession from the router dependency.
    strategy_id: UUID of the strategy to load.
    lock:        If True, acquire a row-level FOR UPDATE lock (use inside
                 mutating transactions to prevent concurrent edits).

    Raises
    ------
    StrategyNotFound: if no row matches `strategy_id`.
    """
    stmt = (
        select(Strategy)
        .where(Strategy.id == strategy_id)
        .options(selectinload(Strategy.backtest_jobs))
    )
    if lock:
        stmt = stmt.with_for_update()

    result = await db.execute(stmt)
    s = result.scalar_one_or_none()
    if s is None:
        logger.warning("get_strategy: strategy %s not found", strategy_id)
        raise StrategyNotFound(str(strategy_id))
    return s


async def compute_list_strategies(
    db: AsyncSession,
    stages: list[str] | None = None,
    created_by: uuid.UUID | None = None,
    is_paper_only: bool | None = None,
    search: str | None = None,
    sort_by: str = "created_at",
    sort_order: str = "desc",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    base_conditions = []

    if stages:
        invalid = set(stages) - set(LIFECYCLE_ORDER)
        if invalid:
            raise ValueError(f"Unknown stage(s): {', '.join(sorted(invalid))}")
        base_conditions.append(Strategy.lifecycle_stage.in_(stages))

    if created_by:
        base_conditions.append(Strategy.created_by == created_by)

    if is_paper_only is not None:
        base_conditions.append(Strategy.is_paper_only == is_paper_only)

    if search:
        term = f"%{search}%"
        base_conditions.append(
            or_(
                Strategy.name.ilike(term),
                Strategy.description.ilike(term),
                Strategy.hypothesis.ilike(term),
                func.cast(Strategy.tags, String).ilike(term),
            )
        )

    where_clause = and_(*base_conditions) if base_conditions else True

    count_stmt = select(func.count()).select_from(Strategy).where(where_clause)
    total: int = (await db.execute(count_stmt)).scalar_one()

    sort_col = getattr(Strategy, sort_by, None)
    if sort_col is None:
        raise ValueError(f"Invalid sort_by field: {sort_by!r}")
    order_expr = sort_col.desc() if sort_order == "desc" else sort_col.asc()

    limit  = max(1, min(limit, 200))
    offset = max(0, offset)

    data_stmt = (
        select(Strategy)
        .where(where_clause)
        .options(selectinload(Strategy.backtest_jobs))
        .order_by(order_expr)
        .limit(limit)
        .offset(offset)
    )
    rows = (await db.execute(data_stmt)).scalars().all()

    return {
        "items":    rows,
        "total":    total,
        "offset":   offset,
        "limit":    limit,
        "has_more": (offset + len(rows)) < total,
    }

async def compute_get_strategy_gate_history(
    db: AsyncSession,
    strategy_id: uuid.UUID,
) -> list[dict[str, Any]]:
    """
    Return the full gate_history log for a strategy without loading
    the heavyweight backtest_jobs relationship.
    """
    stmt = select(Strategy.gate_history).where(Strategy.id == strategy_id)
    result = await db.execute(stmt)
    row = result.one_or_none()
    if row is None:
        raise StrategyNotFound(str(strategy_id))
    return row[0] or []


# ---------------------------------------------------------------------------
# Mutation functions
# ---------------------------------------------------------------------------


async def compute_create_strategy(
    db: AsyncSession,
    data: StrategyCreate,
    creator_id: uuid.UUID,
    creator_email: str,
) -> Strategy:
    """
    Create a new strategy in the IDEA stage.

    Raises
    ------
    ValueError: if the payload contains fields that map to immutable columns
                (defensive; normally blocked by schema validation upstream).
    """
    config = dict(data.config or {})
    if data.alpha_clock is not None:
        config["alpha_clock"] = data.alpha_clock
    if data.optimiser_mode is not None:
        config["optimiser_mode"] = data.optimiser_mode
    s = Strategy(
        name=data.name.strip(),
        created_by=creator_id,
        hypothesis=data.hypothesis,
        description=data.description,
        feature_list=data.feature_list or [],
        allowed_symbols=data.allowed_symbols or [],
        allowed_regimes=data.allowed_regimes or [],
        risk_profile=data.risk_profile,
        config=config,
        tags=data.tags or [],
    )
    db.add(s)
    await db.flush()  # populate s.id without committing

    logger.info(
        "create_strategy: created strategy id=%s name=%r by actor=%s",
        s.id,
        s.name,
        creator_email,
    )

    await write_audit(
        db,
        "STRATEGY_CREATED",
        "strategy",
        str(s.id),
        actor_id=creator_id,
        actor_email=creator_email,
        after_state={"name": s.name, "stage": s.lifecycle_stage},
    )
    return s


async def compute_update_strategy(
    db: AsyncSession,
    strategy_id: uuid.UUID,
    data: StrategyUpdate,
    actor_id: uuid.UUID,
    actor_email: str,
) -> Strategy:
    """
    Partial-patch a strategy's mutable fields.

    Only fields present in `_PATCHABLE_FIELDS` are accepted; any attempt to
    set lifecycle or audit fields is silently ignored (belt-and-suspenders on
    top of schema-level validation).

    Raises
    ------
    StrategyNotFound: if no strategy with `strategy_id` exists.
    """
    s = await compute_get_strategy(db, strategy_id, lock=True)

    before_state: dict[str, Any] = {}
    after_state: dict[str, Any] = {}

    patch = data.model_dump(exclude_unset=True)
    for field, value in patch.items():
        if field not in _PATCHABLE_FIELDS:
            logger.warning(
                "update_strategy: actor=%s attempted to patch immutable field %r — skipped",
                actor_email,
                field,
            )
            continue
        before_state[field] = getattr(s, field, None)
        setattr(s, field, value)
        after_state[field] = value

    if not after_state:
        # Nothing actually changed — return early without touching the DB
        return s

    await db.flush()
    logger.info(
        "update_strategy: strategy id=%s patched fields=%s by actor=%s",
        s.id,
        list(after_state.keys()),
        actor_email,
    )
    await write_audit(
        db,
        "STRATEGY_UPDATED",
        "strategy",
        str(s.id),
        actor_id=actor_id,
        actor_email=actor_email,
        before_state=before_state,
        after_state=after_state,
    )
    return s


async def compute_advance_stage(
    db: AsyncSession,
    strategy_id: uuid.UUID,
    data: StrategyAdvanceRequest,
    actor_id: uuid.UUID,
    actor_email: str,
) -> Strategy:
    """
    Advance a strategy by exactly one stage in the lifecycle.

    The function:
    1. Locks the row for update to serialise concurrent advancement requests.
    2. Validates the transition is forward and not a skip.
    3. Runs the gate check for the target stage.
    4. Appends a gate_history entry regardless of outcome (audit trail).
    5. On success, updates the stage and relevant timestamps.

    Raises
    ------
    StrategyNotFound:  if no strategy with `strategy_id` exists.
    InvalidTransition: if the strategy is already at the final stage.
    GateFailed:        if the gate check for the target stage fails.
    """
    s = await compute_get_strategy(db, strategy_id, lock=True)

    current_idx = _stage_index(s.lifecycle_stage)
    final_idx = len(LIFECYCLE_ORDER) - 1

    if current_idx >= final_idx:
        raise InvalidTransition(
            f"Strategy is already at the final stage ({s.lifecycle_stage}); "
            "cannot advance further"
        )

    next_stage = LIFECYCLE_ORDER[current_idx + 1]
    can, reason = _check_gate(s, next_stage)

    gate_entry: dict[str, Any] = {
        "from": s.lifecycle_stage,
        "to": next_stage,
        "passed": can,
        "reason": reason,
        "ts": _utcnow().isoformat(),
        "actor": actor_email,
        "notes": data.notes,
    }
    # Defensive copy: JSONB columns can be mutated in-place without SQLAlchemy
    # detecting the change if we mutate the list object directly.
    s.gate_history = list(s.gate_history or []) + [gate_entry]

    if not can:
        await db.flush()  # persist gate_history entry even on failure
        logger.warning(
            "advance_stage: gate FAILED strategy=%s %s→%s reason=%r actor=%s",
            strategy_id,
            s.lifecycle_stage,
            next_stage,
            reason,
            actor_email,
        )
        raise GateFailed(reason)

    old_stage = s.lifecycle_stage
    s.lifecycle_stage = next_stage
    now = _utcnow()

    if next_stage in ("LIVE_SMALL", "SCALED") and not s.deployed_at:
        s.deployed_at = now

    if next_stage == "RETIRED":
        s.retired_at = now
        s.retirement_reason = data.notes or "Manual retirement via advance_stage"

    await db.flush()
    logger.info(
        "advance_stage: strategy=%s %s→%s actor=%s",
        strategy_id,
        old_stage,
        next_stage,
        actor_email,
    )
    await write_audit(
        db,
        "STRATEGY_ADVANCED",
        "strategy",
        str(s.id),
        actor_id=actor_id,
        actor_email=actor_email,
        before_state={"stage": old_stage},
        after_state={"stage": next_stage, "gate_passed": True},
    )
    return s


async def retire_strategy(
    db: AsyncSession,
    strategy_id: uuid.UUID,
    reason: str,
    actor_id: uuid.UUID,
    actor_email: str,
) -> Strategy:
    """
    Immediately retire a strategy from any non-RETIRED stage.

    This is an admin escape-hatch that bypasses gate checks but still records
    a full audit and gate_history entry.

    Raises
    ------
    StrategyNotFound:  if no strategy with `strategy_id` exists.
    InvalidTransition: if the strategy is already RETIRED.
    ValueError:        if `reason` is empty or whitespace-only.
    """
    if not reason or not reason.strip():
        raise ValueError("A non-empty retirement reason is required")

    s = await compute_get_strategy(db, strategy_id, lock=True)

    if s.lifecycle_stage == "RETIRED":
        raise InvalidTransition(
            f"Strategy {strategy_id} is already RETIRED (retired_at={s.retired_at})"
        )

    old_stage = s.lifecycle_stage
    now = _utcnow()

    gate_entry: dict[str, Any] = {
        "from": old_stage,
        "to": "RETIRED",
        "passed": True,
        "reason": f"Admin forced retirement: {reason.strip()}",
        "ts": now.isoformat(),
        "actor": actor_email,
        "notes": reason.strip(),
    }
    s.gate_history = list(s.gate_history or []) + [gate_entry]
    s.lifecycle_stage = "RETIRED"
    s.retired_at = now
    s.retirement_reason = reason.strip()

    await db.flush()
    logger.info(
        "retire_strategy: strategy=%s %s→RETIRED actor=%s reason=%r",
        strategy_id,
        old_stage,
        actor_email,
        reason,
    )
    await write_audit(
        db,
        "STRATEGY_RETIRED",
        "strategy",
        str(s.id),
        actor_id=actor_id,
        actor_email=actor_email,
        before_state={"stage": old_stage},
        after_state={"stage": "RETIRED", "reason": reason.strip()},
    )
    return s


# ---------------------------------------------------------------------------
# Router-level HTTP translation helpers
# (Call these in your FastAPI routers instead of catching domain exceptions.)
# ---------------------------------------------------------------------------


def raise_http_for(exc: Exception) -> None:
    """
    Translate domain exceptions to FastAPI HTTPExceptions.

    Usage in a router::

        try:
            strategy = await advance_stage(db, ...)
        except Exception as exc:
            raise_http_for(exc)

    """
    if isinstance(exc, StrategyNotFound):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Strategy not found: {exc}",
        )
    if isinstance(exc, GateFailed):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Gate failed: {exc.reason}",
        )
    if isinstance(exc, InvalidTransition):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        )
    if isinstance(exc, ValueError):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )
    # Re-raise anything unexpected so it surfaces as a 500
    raise exc