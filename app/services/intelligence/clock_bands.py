"""
V10.4 addendum, D.2 -- ClockWeightBands.

Clamps per-AlphaClock capital exposure (computed in capital_service.py from
open positions tagged with a strategy carrying a clock) against
admin-configured min/max bands (ClockWeightBand rows) for the current
regime.

Also holds detect_clock_conflict(), a labeled MVP of the V10.3
clock-conflict reconciler -- see that function's docstring for what's
invented vs derived from D.2's own output. D.3 (PRS-gated dynamic
reallocation) lives in reallocation_service.py, on top of both.
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.all_models import ClockWeightBand

ALPHA_CLOCKS = ["SHORT_FLOW", "MEDIUM_TREND", "LONG_MACRO"]

# Maps the regime labels detect_regime_hmm/_regime_fallback actually
# produce (quant_engine.py REGIME_SIZE_MULT) onto V10.4's five target
# labels. RECOVERY is included only for forward-compat -- neither
# detect_regime_hmm nor _regime_fallback ever emit it today (confirmed by
# source audit). MACRO_EVENT is deliberately absent from this map: no
# event-window detection state exists anywhere in the codebase, so nothing
# should ever resolve to it -- wiring it to a fake source would be
# dishonest (same reasoning the V10.4 audit applied to the band table).
V104_REGIME_MAP: dict[str, str] = {
    "BULL": "LOW_VOL_TREND",
    "RANGE": "RANGE_BOUND",
    "BEAR": "HIGH_VOL_TREND",
    "CRISIS": "CRISIS_LIQUIDITY",
    "RECOVERY": "LOW_VOL_TREND",
}


async def get_active_bands(db: AsyncSession) -> dict[tuple[str, str], tuple[float, float]]:
    """(clock, regime) -> (min_pct, max_pct) for every active band row."""
    result = await db.execute(select(ClockWeightBand).where(ClockWeightBand.is_active == True))  # noqa: E712
    bands: dict[tuple[str, str], tuple[float, float]] = {}
    for row in result.scalars().all():
        bands[(row.clock, row.regime)] = (float(row.min_pct), float(row.max_pct))
    return bands


def constrain(
    clock_exposure_pct: dict[str, float],
    regime_label_v10: str | None,
    bands: dict[tuple[str, str], tuple[float, float]],
) -> list[dict]:
    """
    Independently clamps each of the 3 clocks' raw exposure (% of total
    equity) into its configured band for the current regime. Always reports
    all 3 clocks, even ones with zero exposure or no configured band --
    hiding an untagged/unconfigured clock would be a silent gap, not an
    honest empty state.

    Deliberately does NOT renormalize the clamped set back to 100%: with
    only one clock tagged, renormalizing would push an over-limit clock
    straight back to its raw value, defeating the clamp. Each clock's
    raw/band/clamped triple is reported independently, same as how the
    existing asset-level slices in compute_capital_allocation don't force a
    sum to exactly 100% either.
    """
    regime_v104 = V104_REGIME_MAP.get(regime_label_v10) if regime_label_v10 else None

    out = []
    for clock in ALPHA_CLOCKS:
        raw_pct = round(clock_exposure_pct.get(clock, 0.0), 2)
        band = bands.get((clock, regime_v104)) if regime_v104 else None
        if band is None:
            out.append({
                "clock": clock,
                "raw_pct": raw_pct,
                "band_min_pct": None,
                "band_max_pct": None,
                "clamped_pct": raw_pct,
                "clamped": False,
            })
            continue
        min_pct, max_pct = band
        clamped_pct = min(max(raw_pct, min_pct), max_pct)
        out.append({
            "clock": clock,
            "raw_pct": raw_pct,
            "band_min_pct": min_pct,
            "band_max_pct": max_pct,
            "clamped_pct": round(clamped_pct, 2),
            "clamped": clamped_pct != raw_pct,
        })
    return out


def _band_pressure(clock_row: dict) -> str:
    """BELOW (wants more capital) / ABOVE (wants less) / IN_BAND / UNCONFIGURED."""
    if clock_row["band_min_pct"] is None or clock_row["band_max_pct"] is None:
        return "UNCONFIGURED"
    if clock_row["raw_pct"] < clock_row["band_min_pct"]:
        return "BELOW"
    if clock_row["raw_pct"] > clock_row["band_max_pct"]:
        return "ABOVE"
    return "IN_BAND"


def detect_clock_conflict(clocks: list[dict]) -> dict:
    """
    V10.3 clock-conflict reconciler -- labeled MVP.

    The addendum names this control "LONG_MEDIUM_CLOCK_CONFLICT
    block-and-escalate" with nothing else: no formula, no escalation
    semantics, no definition of what "conflict" means numerically (same
    spec vacuum as PRS -- see prs_service.py's docstring). Rather than
    invent a new signal, this derives conflict from constrain()'s own D.2
    output: LONG_MACRO and MEDIUM_TREND are in conflict when one clock's
    exposure sits below its band floor (wants more capital) while the
    other sits above its band ceiling (wants less) in the same regime --
    two clocks pulling in opposite directions under the same market
    conditions.

    "Block-and-escalate" is implemented as: the caller (capital_service)
    sets clock_bands.conflict.conflict = True in the API response, and
    writes a deduped Alert for escalation. There's no D.3 executor to
    literally block yet (see reallocation_service.py for what D.3's MVP
    actually does with this flag: FREEZE the reallocation-speed
    recommendation).
    """
    by_clock = {c["clock"]: c for c in clocks}
    long_c = by_clock.get("LONG_MACRO")
    med_c = by_clock.get("MEDIUM_TREND")
    if not long_c or not med_c:
        return {"conflict": False, "type": None, "detail": None}

    long_pressure = _band_pressure(long_c)
    med_pressure = _band_pressure(med_c)

    if {long_pressure, med_pressure} != {"BELOW", "ABOVE"}:
        return {"conflict": False, "type": None, "detail": None}

    return {
        "conflict": True,
        "type": "LONG_MEDIUM_CLOCK_CONFLICT",
        "detail": (
            f"LONG_MACRO {long_pressure.lower()} its band "
            f"({long_c['raw_pct']}% vs [{long_c['band_min_pct']}, {long_c['band_max_pct']}]) while "
            f"MEDIUM_TREND {med_pressure.lower()} its band "
            f"({med_c['raw_pct']}% vs [{med_c['band_min_pct']}, {med_c['band_max_pct']}]) -- "
            f"opposing pressure in the same regime."
        ),
    }
