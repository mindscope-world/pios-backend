"""
V10.4 D.3 -- Dynamic Reallocation Trigger, labeled MVP.

The addendum describes a reallocation-speed planner gated on PRS (all
three speed tiers) and the V10.3 reconciler (a freeze row), but gives no
formula for either gate beyond names -- same spec vacuum as PRS itself
(prs_service.py) and the reconciler (clock_bands.detect_clock_conflict).
This is a clearly-labeled placeholder mapping, not a verbatim
implementation of an unseen spec:

  - FREEZE whenever the V10.3 reconciler flags a clock conflict --
    unconditionally, regardless of PRS -- since the addendum's own stated
    reasoning for D.3 is "don't move faster than the system's confidence
    in its own signals", and an active clock conflict is a stronger
    "don't move" signal than any reliability score.
  - Otherwise, speed follows the PRS reliability tier for the primary
    symbol: HIGH -> FAST, MEDIUM -> NORMAL, LOW/UNKNOWN -> SLOW (unknown
    reliability defaults to the conservative tier, not the fast one).

PRS is computed per-symbol/globally in this codebase (see prs_service.py),
not per-clock -- there's no per-clock PRS substrate to gate D.3's three
speed tiers against individually, so this MVP uses one symbol-level PRS
reading for the whole reallocation decision. Replace this mapping wholesale
if the real V10.4 D.3 spec text ever surfaces.
"""
from __future__ import annotations

SPEED_BY_PRS_TIER = {
    "HIGH": "FAST",
    "MEDIUM": "NORMAL",
    "LOW": "SLOW",
    "UNKNOWN": "SLOW",
}


def plan_reallocation_speed(prs_tier: str, conflict: bool) -> dict:
    if conflict:
        return {
            "speed": "FREEZE",
            "reason": "LONG_MEDIUM_CLOCK_CONFLICT active -- reconciler holds reallocation until clocks agree",
        }
    speed = SPEED_BY_PRS_TIER.get(prs_tier, "SLOW")
    return {
        "speed": speed,
        "reason": f"PRS reliability tier {prs_tier} -> {speed}",
    }
