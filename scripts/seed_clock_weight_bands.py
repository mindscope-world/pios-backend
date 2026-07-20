"""
Seeds the initial V10.4 D.2 ClockWeightBand rows (3 AlphaClocks x 4
reachable V10.4 regimes -- MACRO_EVENT deliberately excluded, since no
event-window detection state exists anywhere in the codebase to ever
resolve a regime to it; seeding a band for it would clamp against a label
that can never occur).

Bounds are % of total equity, taken verbatim from the V10.4 addendum's D.2
table -- admin-editable afterward via the /clock-bands endpoints (same as
RiskLimit rows).

Idempotent: if any ClockWeightBand row already exists, this is a no-op.
Run after the DB is up:

    python scripts/seed_clock_weight_bands.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select, func  # noqa: E402

from app.db.session import AsyncSessionLocal  # noqa: E402
from app.models.all_models import ClockWeightBand  # noqa: E402

# (clock, regime, min_pct, max_pct) -- V10.4 addendum D.2 table, verbatim.
DEFAULT_BANDS = [
    ("SHORT_FLOW",   "LOW_VOL_TREND",    10.0, 20.0),
    ("SHORT_FLOW",   "HIGH_VOL_TREND",   15.0, 25.0),
    ("SHORT_FLOW",   "RANGE_BOUND",      30.0, 45.0),
    ("SHORT_FLOW",   "CRISIS_LIQUIDITY", 35.0, 50.0),
    ("MEDIUM_TREND", "LOW_VOL_TREND",    50.0, 70.0),
    ("MEDIUM_TREND", "HIGH_VOL_TREND",   40.0, 55.0),
    ("MEDIUM_TREND", "RANGE_BOUND",      30.0, 45.0),
    ("MEDIUM_TREND", "CRISIS_LIQUIDITY", 10.0, 20.0),
    ("LONG_MACRO",   "LOW_VOL_TREND",    15.0, 30.0),
    ("LONG_MACRO",   "HIGH_VOL_TREND",   25.0, 40.0),
    ("LONG_MACRO",   "RANGE_BOUND",      15.0, 25.0),
    ("LONG_MACRO",   "CRISIS_LIQUIDITY", 35.0, 50.0),
]


async def main() -> None:
    async with AsyncSessionLocal() as db:
        count = (await db.execute(select(func.count()).select_from(ClockWeightBand))).scalar_one()
        if count > 0:
            print(f"clock_weight_bands already has {count} row(s) — nothing to seed.")
            return

        for clock, regime, min_pct, max_pct in DEFAULT_BANDS:
            db.add(ClockWeightBand(clock=clock, regime=regime, min_pct=min_pct, max_pct=max_pct))

        await db.commit()
        print(f"Seeded {len(DEFAULT_BANDS)} clock_weight_bands rows.")


if __name__ == "__main__":
    asyncio.run(main())
