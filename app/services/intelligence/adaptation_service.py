import asyncio
import math
from datetime import datetime, timedelta, timezone

import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.helpers.helpers import latest_regime, primary_symbol, recent_ticks, sharpe
from app.models.all_models import BacktestJob, RegimeState


async def compute_adaptation_feed(db: AsyncSession, limit: int = 50) -> dict:
    """
    Returns ALL adaptation events from completed backtest jobs and regime
    switches. No user filtering — caller/channel layer handles that.
    """
    result = await db.execute(
        select(BacktestJob)
        .where(BacktestJob.status == "COMPLETE")
        .order_by(BacktestJob.completed_at.desc())
        .limit(limit)
    )
    jobs = result.scalars().all()

    items = []
    for job in jobs:
        sharpe_val = float(job.sharpe_ratio or 0)
        prev       = float(job.profit_factor or 1)
        delta_v    = round(sharpe_val - 1.0, 2)
        pos        = delta_v >= 0

        if sharpe_val > 1.5:
            etype  = "ALPHA_PROMOTE"
            icon   = "🚀"
            title  = "Alpha Promoted"
            detail = f"Strategy passed promotion gate: Sharpe {sharpe_val:.2f}"
        elif sharpe_val < 0.5:
            etype  = "ALPHA_PRUNE"
            icon   = "✂️"
            title  = "Alpha Pruned"
            detail = f"Strategy below threshold: Sharpe {sharpe_val:.2f}"
        elif job.max_drawdown and abs(float(job.max_drawdown)) > 15:
            etype  = "PARAM_CHANGE"
            icon   = "⚙️"
            title  = "Risk Params Tightened"
            detail = f"Max drawdown {round(abs(float(job.max_drawdown)), 1)}% exceeded threshold"
        else:
            etype  = "WEIGHT_RECALIBRATION"
            icon   = "⚖️"
            title  = "Weight Recalibration"
            detail = f"Walk-forward Sharpe {sharpe_val:.2f}, WF profit factor {prev:.2f}"

        items.append({
            "id":             str(job.id),
            "user_id":        str(job.submitted_by),   # kept for channel-level filtering
            "type":           etype,
            "icon":           icon,
            "title":          title,
            "detail":         detail,
            "delta":          f"+{delta_v:.2f}" if pos else f"{delta_v:.2f}",
            "delta_positive": pos,
            "occurred_at":    (job.completed_at or job.created_at).isoformat(),
        })

    # Regime switches for the primary symbol (last 7 days)
    primary = await primary_symbol(db)
    if primary:
        since = datetime.now(timezone.utc) - timedelta(days=7)
        reg_result = await db.execute(
            select(RegimeState)
            .where(RegimeState.symbol_id == primary.id, RegimeState.time >= since)
            .order_by(RegimeState.time.desc())
            .limit(10)
        )
        regimes = reg_result.scalars().all()
        prev_regime = None
        for r in reversed(regimes):
            if prev_regime and r.regime_label != prev_regime:
                items.append({
                    "id":             f"regime-{r.id}",
                    "user_id":        None,            # regime events are global
                    "type":           "REGIME_SWITCH",
                    "icon":           "🔀",
                    "title":          f"Regime Switch: {prev_regime} → {r.regime_label}",
                    "detail":         (
                        f"HMM detected regime transition with "
                        f"{round(float(r.confidence) * 100, 1)}% confidence"
                    ),
                    "delta":          r.regime_label,
                    "delta_positive": r.regime_label in ("BULL", "RECOVERY"),
                    "occurred_at":    r.time.isoformat(),
                })
            prev_regime = r.regime_label

    items.sort(key=lambda x: x["occurred_at"], reverse=True)
    return {"items": items}


async def compute_adaptation_active(db: AsyncSession) -> list[dict]:
    """
    Returns active parameter adaptations for ALL regime states.
    No user filtering — caller/channel layer handles that.
    """
    primary = await primary_symbol(db)
    regime  = await latest_regime(db, primary.id) if primary else None
    regime_label = regime.regime_label if regime else "RANGE"

    mult_map     = {"BULL": 1.0, "BEAR": 0.6, "RANGE": 0.8, "CRISIS": 0.3, "RECOVERY": 0.7}
    regime_mult  = mult_map.get(regime_label, 0.8)
    regime_change_pct = round((regime_mult - 1) * 100, 1)

    return [
        {
            "parameter": "Position Size Multiplier",
            "change":    f"{'Increased' if regime_change_pct >= 0 else 'Reduced'} {abs(regime_change_pct):.0f}%",
            "direction": "UP" if regime_change_pct >= 0 else "DOWN",
        },
        {
            "parameter": "Stop-Loss Buffer",
            "change":    "Widened +0.5σ",
            "direction": "UP" if regime_label in ("BEAR", "CRISIS") else "NEUTRAL",
        },
        {
            "parameter": "Rebalance Frequency",
            "change":    "Weekly → Daily" if regime_label in ("BEAR", "CRISIS") else "Daily",
            "direction": "UP" if regime_label in ("BEAR", "CRISIS") else "NEUTRAL",
        },
    ]


async def compute_adaptation_drift(db: AsyncSession, periods: int = 8) -> list[dict]:
    """
    Returns model-drift data (live vs backtest Sharpe) across ALL jobs.
    periods must be between 4 and 20 — validated by the caller, not FastAPI.
    No user filtering — caller/channel layer handles that.
    """
    periods = max(4, min(20, periods))   # clamp defensively

    result = await db.execute(
        select(BacktestJob)
        .where(
            BacktestJob.status == "COMPLETE",
            BacktestJob.sharpe_ratio.isnot(None),
        )
        .order_by(BacktestJob.completed_at.desc())
        .limit(periods)
    )
    jobs    = list(reversed(result.scalars().all()))
    primary = await primary_symbol(db)

    output = []
    for job in jobs:
        bt_sharpe = float(job.sharpe_ratio or 0)

        # Real live Sharpe from the primary symbol's recent tick returns (the
        # best real signal available -- there's no per-strategy PnLSnapshot
        # history yet to compare against instead). No synthetic fallback: when
        # there isn't enough real data, report None rather than a plausible-
        # looking number derived from the backtest Sharpe itself.
        live_sharpe = None
        if primary and job.completed_at:
            window_ticks = await recent_ticks(db, primary.id, 50)
            rets = [
                (float(window_ticks[k].price) - float(window_ticks[k - 1].price))
                / float(window_ticks[k - 1].price)
                for k in range(1, min(len(window_ticks), 20))
                if float(window_ticks[k - 1].price) > 0
            ]
            if len(rets) >= 2:
                live_sharpe = sharpe(rets)

        conf = (
            round(min(100, max(0, 100 - abs(live_sharpe - bt_sharpe) * 30)), 1)
            if live_sharpe is not None else None
        )
        output.append({
            "user_id":          str(job.submitted_by),  # kept for channel-level filtering
            "ts":               (job.completed_at or job.created_at).isoformat(),
            "live_sharpe":      round(live_sharpe, 4) if live_sharpe is not None else None,
            "backtest_sharpe":  round(bt_sharpe, 4),
            "confidence_pct":   conf,
        })

    return output