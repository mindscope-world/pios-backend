from datetime import datetime, timedelta, timezone
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.all_models import BacktestJob, Strategy, User

async def compute_alpha_factory_state(
    current_user: User,
    db: AsyncSession,
):
    result = await db.execute(
        select(BacktestJob)
        .where(BacktestJob.submitted_by == current_user.id)
        .order_by(BacktestJob.created_at.desc())
        .limit(10)
    )
    jobs = result.scalars().all()

    # Count active strategies
    strat_result = await db.execute(
        select(func.count(Strategy.id))
        .where(Strategy.created_by == current_user.id,
               Strategy.lifecycle_stage.in_(["LIVE_SMALL", "SCALED", "PAPER"]))
    )
    active_count = strat_result.scalar_one() or 0

    # Determine next prune at: end of current week
    now = datetime.now(timezone.utc)
    days_to_sunday = (6 - now.weekday()) % 7 or 7
    next_prune = now + timedelta(days=days_to_sunday)

    engines = []
    status_map = {"QUEUED": "QUEUED", "RUNNING": "TRAINING", "COMPLETE": "IDLE", "FAILED": "ERROR"}
    algo_names = ["HMM-Regime-v3", "Darwin-GA-v2", "LSTM-Alpha-v1"]
    for i, job in enumerate(jobs[:3]):
        engines.append({
            "id": str(job.id),
            "name": algo_names[i % len(algo_names)],
            "status": status_map.get(job.status, "IDLE"),
            "progress_pct": job.progress_pct,
            "progress_label": f"Epoch {job.progress_pct}/{100}",
            "sharpe": float(job.sharpe_ratio) if job.sharpe_ratio else None,
        })

    # Best performing strategy
    best_strat = (await db.execute(
        select(Strategy)
        .where(Strategy.created_by == current_user.id, Strategy.sharpe_last.isnot(None))
        .order_by(Strategy.sharpe_last.desc())
        .limit(1)
    )).scalar_one_or_none()

    # Recent backtest success rate
    recent_jobs = (await db.execute(
        select(BacktestJob)
        .where(BacktestJob.submitted_by == current_user.id,
               BacktestJob.completed_at >= datetime.now(timezone.utc) - timedelta(days=30))
    )).scalars().all()
    passed_jobs = [j for j in recent_jobs if (j.sharpe_ratio or 0) >= 0.8]

    return {
        "next_prune_at":        next_prune.isoformat(),
        "active_alphas":        active_count,
        "prune_threshold_sharpe": 0.8,
        "engines":              engines,
        "champion": {
            "name":      best_strat.name         if best_strat else None,
            "sharpe":    float(best_strat.sharpe_last) if best_strat and best_strat.sharpe_last else None,
            "fitness":   float(best_strat.fitness_score) if best_strat and best_strat.fitness_score else None,
            "stage":     best_strat.lifecycle_stage if best_strat else None,
        },
        "backtest_stats_30d": {
            "total_runs":    len(recent_jobs),
            "passed":        len(passed_jobs),
            "pass_rate_pct": round(len(passed_jobs) / max(len(recent_jobs), 1) * 100, 1),
            "avg_sharpe":    round(sum(float(j.sharpe_ratio or 0) for j in passed_jobs) / max(len(passed_jobs), 1), 4),
        },
    }
    

async def compute_alpha_darwin(
    current_user: User,
    db: AsyncSession,
):
    """Darwin leaderboard — rank strategies by latest backtest Sharpe."""
    result = await db.execute(
        select(Strategy)
        .where(Strategy.created_by == current_user.id)
        .order_by(Strategy.sharpe_last.desc().nullslast())
        .limit(20)
    )
    strategies = result.scalars().all()

    candidates = []
    for i, s in enumerate(strategies):
        sharpe = float(s.sharpe_last or 0)
        fitness = float(s.fitness_score or 0)
        if sharpe >= 1.5:
            status = "CHAMPION"
        elif sharpe >= 0.8:
            status = "CHALLENGER"
        elif sharpe >= 0.3:
            status = "PROBATION"
        else:
            status = "RETIRED"

        # Approximate win rate and max_dd from backtest jobs
        bt_result = await db.execute(
            select(BacktestJob)
            .where(BacktestJob.strategy_id == s.id, BacktestJob.status == "COMPLETE")
            .order_by(BacktestJob.completed_at.desc())
            .limit(1)
        )
        bt = bt_result.scalar_one_or_none()
        win_rate = float(bt.win_rate or 0.5) if bt else 0.5
        max_dd   = float(bt.max_drawdown or 0) if bt else 0.0

        # Recent regime performance from backtest report
        regime_breakdown = {}
        if bt and bt.full_report:
            regime_breakdown = bt.full_report.get("regime_breakdown", {})

        candidates.append({
            "rank":             i + 1,
            "id":               str(s.id),
            "name":             s.name,
            "version":          s.version,
            "generation":       s.generation or 0,
            "lifecycle_stage":  s.lifecycle_stage,
            "sharpe":           round(sharpe, 4),
            "fitness_score":    round(float(s.fitness_score or 0), 4),
            "max_dd":           round(max_dd, 4),
            "win_rate":         round(win_rate, 4),
            "profit_factor":    round(float(bt.profit_factor or 0), 4) if bt else None,
            "total_return_pct": round(float(bt.total_return or 0), 4) if bt else None,
            "trade_count":      bt.trade_count if bt else None,
            "status":           status,
            "regime_breakdown": regime_breakdown,
            "deployed_at":      s.deployed_at.isoformat() if s.deployed_at else None,
            "last_backtest":    bt.completed_at.isoformat() if bt and bt.completed_at else None,
        })

    return candidates