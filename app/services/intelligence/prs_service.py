"""
V10.1 -- Predictor Reliability Score (PRS), labeled MVP.

No V10.1 spec text exists anywhere in this codebase or filesystem -- only
the one-line description in the V10.4 addendum audit ("short-horizon hit
rate, relative_drop thresholds"). There is no formula, horizon, or
threshold definition to build against, and the audit's substrate check
found that "decision outcomes are already recorded for the rejection-stats
card" doesn't hold up: /intelligence/rejection-stats is reject_reason
counts, not decision-outcome hit-rate tracking. This module and
QuantDecision (app/models/all_models.py) are net-new infrastructure and a
clearly-labeled placeholder methodology, not a verbatim implementation of
an unseen spec:

  - direction is a regime-only proxy (BULL -> LONG, BEAR -> SHORT, anything
    else -> no directional bias, never graded)
  - a decision is graded HIT/MISS once horizon_minutes has elapsed, by
    checking whether price moved in the predicted direction by more than
    NOISE_FLOOR_PCT (moves inside the noise floor are left ungraded rather
    than forced into HIT or MISS)
  - RELATIVE_DROP_ALERT_PCT and the HIGH/MEDIUM/LOW tier cutoffs are
    invented constants, not backend-validated numbers

Replace the grading logic and thresholds wholesale if the real V10.1 spec
ever surfaces -- nothing here should be treated as authoritative
prediction-quality tracking in the meantime.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.all_models import QuantDecision, MarketTick

RECORD_MIN_INTERVAL_MINUTES = 5    # throttle: at most one decision row per symbol per this window
DEFAULT_HORIZON_MINUTES = 30       # how far ahead to check whether the regime-implied direction held
NOISE_FLOOR_PCT = 0.05             # moves smaller than this count as "no move" -> ungraded, not a MISS
RECENT_WINDOW = 20                 # sample size for the "recent" hit rate
BASELINE_WINDOW = 100              # sample size for the "baseline" hit rate
RELATIVE_DROP_ALERT_PCT = 25.0     # placeholder degradation threshold (%) -- see module docstring

REGIME_DIRECTION = {"BULL": "LONG", "BEAR": "SHORT"}  # RANGE/CRISIS/RECOVERY -> no directional bias


async def record_decision(
    db: AsyncSession,
    symbol_id: int,
    decision: str,
    confidence: float,
    regime_label: str,
    price: float | None,
) -> None:
    """Throttled: no-op if this symbol already has a decision recorded
    within RECORD_MIN_INTERVAL_MINUTES, and if no live price is available
    (nothing to grade a direction against later)."""
    if price is None:
        return

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=RECORD_MIN_INTERVAL_MINUTES)
    recent = (await db.execute(
        select(QuantDecision.id)
        .where(QuantDecision.symbol_id == symbol_id, QuantDecision.time >= cutoff)
        .limit(1)
    )).scalar_one_or_none()
    if recent is not None:
        return

    db.add(QuantDecision(
        symbol_id=symbol_id,
        decision=decision,
        direction=REGIME_DIRECTION.get(regime_label),
        confidence=confidence,
        regime_label=regime_label,
        price_at_decision=price,
        horizon_minutes=DEFAULT_HORIZON_MINUTES,
    ))
    await db.commit()


async def _latest_price(db: AsyncSession, symbol_id: int) -> float | None:
    row = (await db.execute(
        select(MarketTick.price)
        .where(MarketTick.symbol_id == symbol_id)
        .order_by(MarketTick.time.desc())
        .limit(1)
    )).scalar_one_or_none()
    return float(row) if row is not None else None


async def grade_pending_decisions(db: AsyncSession) -> int:
    """Grade every decision whose horizon has elapsed and isn't graded yet.
    Returns the number graded. Safe to call repeatedly (idempotent per row --
    graded_at is set exactly once)."""
    now = datetime.now(timezone.utc)
    pending = (await db.execute(
        select(QuantDecision).where(QuantDecision.graded_at.is_(None))
    )).scalars().all()

    graded = 0
    for dec in pending:
        due_at = dec.time + timedelta(minutes=dec.horizon_minutes)
        if now < due_at:
            continue

        if dec.direction is None:
            # No directional bias at decision time (RANGE/CRISIS/RECOVERY) --
            # nothing to grade, but mark graded_at so it stops being re-scanned.
            dec.graded_at = now
            graded += 1
            continue

        latest_price = await _latest_price(db, dec.symbol_id)
        if latest_price is None:
            continue  # no fresh tick yet -- try again next pass

        move_pct = (latest_price - float(dec.price_at_decision)) / float(dec.price_at_decision) * 100
        if abs(move_pct) < NOISE_FLOOR_PCT:
            outcome = None
        elif dec.direction == "LONG":
            outcome = "HIT" if move_pct > 0 else "MISS"
        else:
            outcome = "HIT" if move_pct < 0 else "MISS"

        dec.outcome = outcome
        dec.price_at_grading = latest_price
        dec.graded_at = now
        graded += 1

    if graded:
        await db.commit()
    return graded


async def compute_prs(db: AsyncSession, symbol_id: int) -> dict:
    """
    hit_rate_recent (last RECENT_WINDOW graded decisions) vs hit_rate_baseline
    (last BASELINE_WINDOW). relative_drop_pct is how far recent has fallen
    below baseline; reliability_tier buckets that drop via the placeholder
    RELATIVE_DROP_ALERT_PCT threshold. See module docstring -- not a real
    V10.1 threshold, there isn't one to copy.
    """
    outcomes = (await db.execute(
        select(QuantDecision.outcome)
        .where(QuantDecision.symbol_id == symbol_id, QuantDecision.outcome.isnot(None))
        .order_by(QuantDecision.graded_at.desc())
        .limit(BASELINE_WINDOW)
    )).scalars().all()

    if not outcomes:
        return {
            "symbol_id": symbol_id,
            "sample_size": 0,
            "hit_rate_recent_pct": None,
            "hit_rate_baseline_pct": None,
            "relative_drop_pct": None,
            "reliability_tier": "UNKNOWN",
            "note": "no graded decisions yet",
        }

    def _hit_rate(rows: list[str]) -> float | None:
        return round(100 * sum(1 for o in rows if o == "HIT") / len(rows), 2) if rows else None

    hit_recent = _hit_rate(outcomes[:RECENT_WINDOW])
    hit_baseline = _hit_rate(outcomes)

    relative_drop = None
    tier = "UNKNOWN"
    if hit_baseline and hit_baseline > 0 and hit_recent is not None:
        relative_drop = round((hit_baseline - hit_recent) / hit_baseline * 100, 2)
        if relative_drop >= RELATIVE_DROP_ALERT_PCT:
            tier = "LOW"
        elif relative_drop >= RELATIVE_DROP_ALERT_PCT / 2:
            tier = "MEDIUM"
        else:
            tier = "HIGH"

    return {
        "symbol_id": symbol_id,
        "sample_size": len(outcomes),
        "hit_rate_recent_pct": hit_recent,
        "hit_rate_baseline_pct": hit_baseline,
        "relative_drop_pct": relative_drop,
        "reliability_tier": tier,
    }
