# app/workers/backtest_worker.py
"""
PiOS Backtest Engine — Production Grade
Run workers:
  celery -A app.workers.celery_app worker -Q backtest,default --loglevel=info
  celery -A app.workers.celery_app beat  --loglevel=info
"""
from __future__ import annotations

import uuid
import math
import logging
from datetime import datetime, timezone, timedelta

import numpy as np
import mlflow
from app.db.sync_session import SyncSession
from app.services.quant_engine import (detect_regime_hmm, estimate_volatility_garch, compute_ofi_signals, run_monte_carlo)
from sqlalchemy import select
from app.models.all_models import Strategy, BacktestJob, Position, PnLSnapshot, User, MarketTick, Symbol, RegimeState, Alert
from app.services.quant_engine import detect_regime_hmm, REGIME_SIZE_MULT, COST_MODEL_FEE_BPS
from app.services.strategy_signals import (
    simulate_fold as v10_simulate_fold,
    STRATEGY_KEYS as V10_STRATEGY_KEYS,
    DEFAULT_STRATEGY_PARAMS as V10_DEFAULT_PARAMS,
)
from app.workers.celery_app import celery_app

log = logging.getLogger(__name__)

# v10 D2.2 Walk-Forward Validator gate: "Out-of-sample Sharpe > 0.5 across
# all folds" (verbatim from PiOSQ_Complete_v10_Specification D2.2).
WALK_FORWARD_SHARPE_GATE = 0.5


@celery_app.task(bind=True, name="run_backtest", queue="backtest", max_retries=2)
def run_backtest_task(self, job_id: str):

    mlflow_active = False
    try:
        mlflow.set_experiment("pios_backtests")
        mlflow.start_run(run_name=f"backtest_{job_id[:8]}")
        mlflow_active = True
    except Exception:
        pass

    with SyncSession() as db:
        job = db.get(BacktestJob, uuid.UUID(job_id))
        if not job:
            return {"error": "job_not_found"}

        job.status = "RUNNING"
        job.started_at = datetime.now(timezone.utc)
        db.commit()

        try:
            # Which of the five fixed proprietary strategies (if any) this job
            # is attached to. An absent/unrecognized key means this Strategy
            # row predates the v10 registry (or is a legacy user-created row
            # from the generic CRUD system) -- it gets the old generic
            # placeholder rule, clearly labeled as such below, never silently
            # mistaken for real strategy logic.
            strategy: Strategy | None = db.get(Strategy, job.strategy_id) if job.strategy_id else None
            strategy_key = (strategy.config or {}).get("strategy_key") if strategy else None
            if strategy_key not in V10_STRATEGY_KEYS:
                strategy_key = None
            strategy_param_overrides = (
                (strategy.config or {}).get("params") if (strategy and strategy_key) else None
            )

            sym_str = (job.symbols or ["BTC/USDT"])[0]
            sym_row = db.execute(select(Symbol).where(Symbol.symbol == sym_str)).scalar_one_or_none()

            ticks = []
            if sym_row:
                result = db.execute(
                    select(MarketTick)
                    .where(
                        MarketTick.symbol_id == sym_row.id,
                        MarketTick.time >= _parse_date(job.start_date),
                        MarketTick.time <= _parse_date(job.end_date, end=True),
                    )
                    .order_by(MarketTick.time)
                )
                ticks = result.scalars().all()

            prices  = [float(t.price)  for t in ticks] if ticks else _synthetic_prices(500)
            volumes = [float(t.volume) for t in ticks] if ticks else [1.0] * len(prices)
            sides   = [str(t.side or "") for t in ticks] if ticks else [""] * len(prices)
            times   = [t.time for t in ticks] if ticks else _synthetic_times(len(prices))

            # BTC-Neutral Residual Mean Reversion (v10 D2.2 Strategy 4) trades
            # a spread against a hedge asset -- fetch its ticks over the same
            # date range and asof-align them onto the primary symbol's own
            # tick timestamps once, up front, so every fold can slice the
            # result exactly like `prices`.
            hedge_prices_aligned = None
            if strategy_key == "BTC_NEUTRAL_MEAN_REVERSION":
                hedge_symbol = (
                    (strategy_param_overrides or {}).get("hedge_symbol")
                    or V10_DEFAULT_PARAMS["BTC_NEUTRAL_MEAN_REVERSION"]["hedge_symbol"]
                )
                hedge_sym_row = db.execute(select(Symbol).where(Symbol.symbol == hedge_symbol)).scalar_one_or_none()
                hedge_ticks = []
                if hedge_sym_row and ticks:
                    hedge_result = db.execute(
                        select(MarketTick)
                        .where(
                            MarketTick.symbol_id == hedge_sym_row.id,
                            MarketTick.time >= _parse_date(job.start_date),
                            MarketTick.time <= _parse_date(job.end_date, end=True),
                        )
                        .order_by(MarketTick.time)
                    )
                    hedge_ticks = hedge_result.scalars().all()
                hedge_prices_aligned = _align_hedge_prices(times, hedge_ticks)

            if mlflow_active:
                import mlflow as _mlf
                _mlf.log_params({
                    "symbol": sym_str, "tick_count": len(prices), "cost_model": job.cost_model,
                    "strategy_key": strategy_key or "GENERIC_PLACEHOLDER",
                })

            n_folds   = max(4, min(12, len(prices) // 50))
            fold_size = max(5, len(prices) // n_folds)
            folds_results = []
            all_trade_returns: list[float] = []

            for fold_i in range(n_folds):
                train_end  = fold_size * (fold_i + 1)
                test_start = train_end
                test_end   = min(test_start + fold_size, len(prices))
                if test_end <= test_start:
                    break

                train_prices = prices[:train_end]
                train_vols   = volumes[:train_end]
                train_sides  = sides[:train_end]
                train_times  = times[:train_end]
                train_hedge  = hedge_prices_aligned[:train_end] if hedge_prices_aligned else None
                test_prices  = prices[test_start:test_end]
                test_vols    = volumes[test_start:test_end]
                test_sides   = sides[test_start:test_end]
                test_times   = times[test_start:test_end]
                test_hedge   = hedge_prices_aligned[test_start:test_end] if hedge_prices_aligned else None

                regime_data = detect_regime_hmm(train_prices)
                vol_data    = estimate_volatility_garch(train_prices)
                daily_vol   = vol_data["daily_vol"] / 100

                # v10 D2.4 Hyperparameter Optimization (opt-in via
                # job.config.hpo): TPE-sampled, median-pruned search over
                # this strategy's numeric params -- scored ONLY against
                # train_prices/train_* (see hyperparameter_search.py's
                # module docstring for why the fold's test slice must never
                # be touched by the search itself). The winning params are
                # then used, once, to score test_prices below -- identical
                # to how strategy_param_overrides was always used, just
                # fold-tuned instead of fixed for the whole job.
                hpo_result = None
                fold_params = strategy_param_overrides
                if strategy_key and job.config.get("hpo"):
                    from app.services.hyperparameter_search import search_fold_hyperparameters
                    hpo_result = search_fold_hyperparameters(
                        strategy_key, train_prices, train_times, train_vols, train_sides,
                        regime_data["regime"], regime_data["confidence"] / 100, job.cost_model,
                        hedge_prices=train_hedge,
                        n_trials=int(job.config.get("hpo_trials", 20)),
                    )
                    fold_params = hpo_result["best_params"]

                if strategy_key:
                    fold_pnl = v10_simulate_fold(
                        strategy_key, test_prices, test_times, test_vols, test_sides,
                        regime_data["regime"], regime_data["confidence"] / 100, job.cost_model,
                        params=fold_params, hedge_prices=test_hedge,
                    )
                else:
                    tick_dicts = [{"price": p, "volume": v, "side": s}
                                  for p, v, s in zip(test_prices, test_vols, test_sides)]
                    ofi = compute_ofi_signals(tick_dicts)
                    fold_pnl = _simulate_fold(test_prices, regime_data["regime"], daily_vol, ofi, job.cost_model)
                    fold_pnl["diagnostics"] = {"engine": "GENERIC_PLACEHOLDER"}

                all_trade_returns.extend(fold_pnl["returns"])
                fold_sharpe = _sharpe(fold_pnl["returns"])
                folds_results.append({
                    "fold": fold_i + 1,
                    "sharpe": round(fold_sharpe, 4),
                    "regime": regime_data["regime"],
                    "trades": fold_pnl["n_trades"],
                    "win_rate": fold_pnl["win_rate"],
                    "total_return_pct": fold_pnl["total_return_pct"],
                    "max_dd": fold_pnl["max_dd"],
                    "engine": fold_pnl.get("diagnostics", {}).get("engine", "GENERIC_PLACEHOLDER"),
                    # bool(...): fold_sharpe can be a numpy scalar (test_prices is a
                    # numpy array upstream in _simulate_fold), and numpy's bool_ --
                    # unlike numpy.float64, which transparently subclasses float --
                    # does not subclass Python's bool, so it fails full_report's JSON
                    # serialization on save (numpy 2.x renamed its __name__ to "bool",
                    # which is why that failure reads "Object of type bool is not
                    # JSON serializable" and not "bool_").
                    # Threshold is v10 D2.2's own Walk-Forward Validator gate --
                    # "Out-of-sample Sharpe > 0.5 across all folds" -- not the
                    # unrelated 0.8 the generic BACKTEST->PAPER CRUD gate uses.
                    "passed": bool(fold_sharpe > WALK_FORWARD_SHARPE_GATE),
                    "hyperparameter_search": hpo_result,
                })

                pct = int((fold_i + 1) / n_folds * 90)
                self.update_state(state="PROGRESS", meta={"progress": pct, "fold": fold_i + 1})
                job.progress_pct = pct
                db.commit()

            all_sharpes = [f["sharpe"] for f in folds_results]
            all_trades  = sum(f["trades"] for f in folds_results)
            all_wr      = [f["win_rate"] for f in folds_results if f["trades"] > 0]

            sharpe   = round(float(np.mean(all_sharpes)), 4)
            max_dd   = round(float(np.min([f["max_dd"] for f in folds_results])), 4)
            total_r  = round(float(np.sum([f["total_return_pct"] for f in folds_results])), 4)
            win_rate = round(float(np.mean(all_wr)), 4) if all_wr else 0.5
            profit_f = _profit_factor(all_trade_returns)

            mc = run_monte_carlo(prices, n_sims=1000, horizon_days=30)
            equity_curve = _build_equity_curve(prices, folds_results)
            full_regime  = detect_regime_hmm(prices)
            vol_engine   = estimate_volatility_garch(prices[-100:]).get("engine", "ROLLING_STD")

            if mlflow_active:
                import mlflow as _mlf
                _mlf.log_metrics({"sharpe": sharpe, "max_dd": abs(max_dd), "win_rate": win_rate})
                _mlf.end_run()

            job.status        = "COMPLETE"
            job.sharpe_ratio  = sharpe
            job.max_drawdown  = max_dd
            job.total_return  = total_r
            job.trade_count   = all_trades
            job.win_rate      = win_rate
            job.profit_factor = profit_f
            job.equity_curve  = equity_curve
            job.full_report   = {
                "folds": [f["sharpe"] for f in folds_results],
                "walk_forward": folds_results,
                "strategy_key": strategy_key,
                "monte_carlo": {
                    "p5":  mc.get("p5_return_pct"),
                    "p50": mc.get("p50_return_pct"),
                    "p95": mc.get("p95_return_pct"),
                },
                "regime_breakdown": _compute_regime_breakdown(prices, n_folds),
                "regime_engine":    full_regime.get("engine"),
                "vol_engine":       vol_engine,
                "cost_analysis":    _cost_analysis(all_trades, prices, job.cost_model),
                "tick_count":       len(prices),
                "data_source":      "live_ticks" if ticks else "synthetic_fallback",
            }
            job.progress_pct = 100
            job.completed_at = datetime.now(timezone.utc)

            if strategy:
                span_days = max(1, (_parse_date(job.end_date, end=True) - _parse_date(job.start_date)).days)
                annualized_return = total_r * (365 / span_days)
                calmar = annualized_return / abs(max_dd) if max_dd else 0.0
                strategy.sharpe_last   = sharpe
                # v10 D5.2 Darwin Fitness Gate (PiOSQ_Complete_v10_Specification):
                # fitness = Sharpe*0.4 + Calmar*0.3 + WinRate*0.2 + PF*0.1 - MaxDD*0.5 > 0.6
                strategy.fitness_score = round(
                    sharpe * 0.4 + calmar * 0.3 + win_rate * 0.2 + profit_f * 0.1 - (abs(max_dd) / 100) * 0.5,
                    4,
                )

            db.commit()
            return {"job_id": job_id, "status": "COMPLETE", "sharpe": sharpe}

        except Exception as exc:
            # A failure during the try block's own commit (e.g. a bad JSON
            # value) leaves this session mid-transaction -- committing again
            # without rolling back first raises PendingRollbackError, which
            # would propagate out of this handler and leave the job stuck at
            # RUNNING forever instead of ever reaching FAILED.
            db.rollback()
            job.status        = "FAILED"
            job.error_message = str(exc)[:500]
            job.completed_at  = datetime.now(timezone.utc)
            db.commit()
            if mlflow_active:
                try:
                    import mlflow as _mlf
                    _mlf.end_run(status="FAILED")
                except Exception:
                    pass
            self.retry(exc=exc, countdown=10)



# Population-convergence threshold for diversity injection -- mean
# coefficient of variation (std/|mean|) across every numeric config key
# shared by >=2 active strategies. Below this, Darwin has converged its
# numeric params to near-identical values (pure exploitation, no
# exploration left) even if fitness scores still differ.
DIVERSITY_CV_THRESHOLD = 0.05
DIVERSITY_INJECT_COUNT = 2


def _population_diversity(strategies: list) -> float:
    """Mean CV across shared numeric config keys. Returns 1.0 (treated as
    "diverse enough, don't force-inject") when there are no shared numeric
    keys to compare at all -- an empty/singleton population isn't evidence
    of convergence."""
    by_key: dict[str, list[float]] = {}
    for s in strategies:
        for k, v in (s.config or {}).items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                by_key.setdefault(k, []).append(float(v))
    cvs = []
    for vals in by_key.values():
        if len(vals) < 2:
            continue
        arr = np.array(vals)
        mean = abs(arr.mean())
        if mean > 1e-9:
            cvs.append(float(arr.std() / mean))
    return float(np.mean(cvs)) if cvs else 1.0


def _wide_mutate_config(config: dict) -> dict:
    """Diversity-injection mutation: ~3.5x the standard deviation
    _mutate_config uses, so an injected variant actually moves the
    population away from a converged cluster instead of landing back
    inside it (a second copy of the standard-strength mutation)."""
    import copy
    rng = np.random.default_rng()
    m = copy.deepcopy(config)
    for k, v in m.items():
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v != 0:
            m[k] = round(v + rng.normal(0, abs(v) * 0.35), 6)
    m["_mutated_at"] = datetime.now(timezone.utc).isoformat()
    m["_diversity_injection"] = True
    return m


@celery_app.task(name="darwin_evolution_cycle", queue="backtest")
def darwin_evolution_cycle():
    with SyncSession() as db:
        strategies = db.execute(
            select(Strategy).where(
                Strategy.lifecycle_stage.in_(["LIVE_SMALL", "SCALED", "PAPER", "BACKTEST"])
            )
        ).scalars().all()

        prune_threshold = 0.8
        scored = sorted([(s, float(s.fitness_score or 0)) for s in strategies], key=lambda x: -x[1])
        bottom_n = max(1, len(scored) // 5)
        top_n    = max(1, len(scored) // 5)

        for strat, _ in scored[-bottom_n:]:
            if float(strat.sharpe_last or 0) < prune_threshold:
                strat.lifecycle_stage   = "RETIRED"
                strat.retired_at        = datetime.now(timezone.utc)
                strat.retirement_reason = "Darwin: below fitness threshold"

        new_children = []
        for parent, _ in scored[:top_n]:
            child = Strategy(
                name            = f"DARWIN-{parent.name[:10]}-G{(parent.generation or 0)+1}",
                created_by      = parent.created_by,
                parent_id       = parent.id,
                generation      = (parent.generation or 0) + 1,
                lifecycle_stage = "BACKTEST",
                hypothesis      = parent.hypothesis,
                description     = f"Darwin mutation of {parent.name}",
                feature_list    = parent.feature_list,
                allowed_symbols = parent.allowed_symbols,
                allowed_regimes = parent.allowed_regimes,
                is_paper_only   = True,
                config          = _mutate_config(parent.config or {}),
                risk_profile    = parent.risk_profile or {},
            )
            db.add(child)
            db.flush()
            new_children.append(child)

        # Diversity injection: clone+mutate above only ever perturbs the top
        # 20% by a small amount -- left alone, repeated cycles converge the
        # whole population toward one numeric neighborhood and Darwin stops
        # being able to discover anything the current cluster doesn't
        # already resemble. When the surviving (non-retired) population's
        # config diversity drops below DIVERSITY_CV_THRESHOLD, inject a
        # couple of widely-mutated variants from randomly-picked survivors
        # (not just the top scorers) to push exploration back in.
        surviving = [s for s in strategies if s.lifecycle_stage != "RETIRED"]
        diversity = _population_diversity(surviving)
        diversity_injected = 0
        if diversity < DIVERSITY_CV_THRESHOLD and surviving:
            rng = np.random.default_rng()
            n_pick = min(DIVERSITY_INJECT_COUNT, len(surviving))
            picks = rng.choice(len(surviving), size=n_pick, replace=False)
            for idx in picks:
                parent = surviving[int(idx)]
                child = Strategy(
                    name            = f"DARWIN-DIV-{parent.name[:8]}-G{(parent.generation or 0)+1}",
                    created_by      = parent.created_by,
                    parent_id       = parent.id,
                    generation      = (parent.generation or 0) + 1,
                    lifecycle_stage = "BACKTEST",
                    hypothesis      = parent.hypothesis,
                    description     = f"Darwin diversity injection from {parent.name} (population CV={diversity:.4f} < {DIVERSITY_CV_THRESHOLD})",
                    feature_list    = parent.feature_list,
                    allowed_symbols = parent.allowed_symbols,
                    allowed_regimes = parent.allowed_regimes,
                    is_paper_only   = True,
                    config          = _wide_mutate_config(parent.config or {}),
                    risk_profile    = parent.risk_profile or {},
                )
                db.add(child)
                db.flush()
                new_children.append(child)
                diversity_injected += 1

        db.commit()

        for child in new_children:
            job = BacktestJob(
                strategy_id  = child.id,
                submitted_by = child.created_by,
                status       = "QUEUED",
                start_date   = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d"),
                end_date     = datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                symbols      = child.allowed_symbols or ["BTC/USDT"],
                cost_model   = "FULL",
            )
            db.add(job)
        db.commit()

        for child in new_children:
            j = db.execute(
                select(BacktestJob)
                .where(BacktestJob.strategy_id == child.id, BacktestJob.status == "QUEUED")
                .limit(1)
            ).scalar_one_or_none()
            if j:
                run_backtest_task.delay(str(j.id))

    log.info(
        f"Darwin cycle: {bottom_n} pruned, {len(new_children) - diversity_injected} cloned, "
        f"{diversity_injected} diversity-injected (population CV={diversity:.4f})"
    )


@celery_app.task(name="snapshot_pnl", queue="default")
def snapshot_pnl():
    with SyncSession() as db:
        users = db.execute(select(User).where(User.is_active.is_(True))).scalars().all()
        for user in users:
            positions = db.execute(
                select(Position).where(Position.user_id == user.id, Position.is_open.is_(True))
            ).scalars().all()
            if not positions:
                continue

            total_unreal = 0.0
            total_real   = 0.0
            for pos in positions:
                tick = db.execute(
                    select(MarketTick)
                    .where(MarketTick.symbol_id == pos.symbol_id)
                    .order_by(MarketTick.time.desc())
                    .limit(1)
                ).scalar_one_or_none()
                if tick:
                    cur = float(tick.price)
                    avg = float(pos.avg_cost)
                    qty = float(pos.qty)
                    unreal = (cur - avg) * qty if pos.side == "LONG" else (avg - cur) * qty
                    pos.unrealized_pnl = unreal
                total_unreal += float(pos.unrealized_pnl)
                total_real   += float(pos.realized_pnl)

            last = db.execute(
                select(PnLSnapshot)
                .where(PnLSnapshot.user_id == user.id)
                .order_by(PnLSnapshot.snapshot_at.desc())
                .limit(1)
            ).scalar_one_or_none()

            cash      = float(last.cash_balance) if last else 100_000.0
            equity    = cash + total_unreal + total_real
            peak      = float(last.total_equity) if last else equity
            drawdown  = min(0.0, (equity - peak) / peak * 100) if peak > 0 else 0.0

            db.add(PnLSnapshot(
                user_id        = user.id,
                total_equity   = round(equity, 4),
                realized_pnl   = round(total_real, 4),
                unrealized_pnl = round(total_unreal, 4),
                cash_balance   = round(cash, 4),
                drawdown_pct   = round(drawdown, 4),
            ))
        db.commit()
    log.info("PnL snapshot done")


@celery_app.task(name="regime_scan", queue="default")
def regime_scan_task():
    with SyncSession() as db:
        syms = db.execute(select(Symbol).where(Symbol.is_active.is_(True))).scalars().all()
        for sym in syms:
            ticks = db.execute(
                select(MarketTick)
                .where(MarketTick.symbol_id == sym.id)
                .order_by(MarketTick.time.desc())
                .limit(500)
            ).scalars().all()
            if len(ticks) < 30:
                continue
            prices = list(reversed([float(t.price) for t in ticks]))
            regime = detect_regime_hmm(prices)
            db.add(RegimeState(
                time         = datetime.now(timezone.utc),
                symbol_id    = sym.id,
                regime_label = regime["regime"],
                confidence   = regime["confidence"] / 100,
                hmm_probs    = {"conf_low": regime["confidence_low"] / 100,
                                "conf_high": regime["confidence_high"] / 100,
                                "engine": regime.get("engine")},
                detected_by  = "HMM",
            ))
        db.commit()
    log.info("Regime scan done")


@celery_app.task(name="drift_monitor", queue="default")
def drift_monitor_task():
    with SyncSession() as db:
        syms = db.execute(select(Symbol).where(Symbol.is_active.is_(True)).limit(5)).scalars().all()
        for sym in syms:
            ticks = db.execute(
                select(MarketTick)
                .where(MarketTick.symbol_id == sym.id)
                .order_by(MarketTick.time.desc())
                .limit(1000)
            ).scalars().all()
            if len(ticks) < 200:
                continue
            prices = list(reversed([float(t.price) for t in ticks]))
            rets   = np.diff(np.log(np.array(prices) + 1e-10))
            half   = len(rets) // 2
            psi    = _compute_psi(rets[:half], rets[half:])
            if psi > 0.2:
                db.add(Alert(
                    severity  = "P2",
                    source    = "DRIFT_MONITOR",
                    category  = "MODEL_DRIFT",
                    title     = f"Return distribution drift: {sym.symbol} (PSI={psi:.3f})",
                    message   = f"PSI={psi:.3f} exceeds 0.2 threshold. Model recalibration recommended.",
                    symbol_id = sym.id,
                    meta      = {"psi": round(psi, 4)},
                ))
        db.commit()


@celery_app.task(name="alpha_factory_search", queue="backtest")
def alpha_factory_search(strategy_id: str | None = None):
    """
    Real idea generation (Guide Ch.13): PySR symbolic regression and a
    stable-baselines3 PPO agent each independently search the same rolling
    feature set over each scoped symbol's recent tick history, and each
    successful search becomes a new, quarantined Strategy row (see
    alpha_factory_engine.py and strategy_service.ALPHA_FACTORY_ORIGINS for
    why quarantined). Previously this task only extracted tsfresh features
    and logged a count -- no candidate was ever generated; extraction is
    kept below (still a real, useful diagnostic) but no longer the whole of
    what this task does.
    """
    from app.db.sync_session import SyncSession
    from app.models.all_models import MarketTick, Symbol, Strategy, User
    from app.services.quant_engine import extract_ts_features
    from app.services.alpha_factory_engine import run_pysr_search, run_rl_search
    from sqlalchemy import select

    with SyncSession() as db:
        owner = db.execute(
            select(User).where(User.role.in_(["admin", "quant"]), User.is_active.is_(True)).limit(1)
        ).scalar_one_or_none()

        syms = db.execute(select(Symbol).where(Symbol.is_active.is_(True)).limit(3)).scalars().all()
        generated = 0
        for sym in syms:
            ticks = db.execute(
                select(MarketTick)
                .where(MarketTick.symbol_id == sym.id)
                .order_by(MarketTick.time.desc())
                .limit(300)
            ).scalars().all()
            if len(ticks) < 50:
                continue
            prices  = list(reversed([float(t.price)  for t in ticks]))
            volumes = list(reversed([float(t.volume) for t in ticks]))
            features = extract_ts_features(prices, volumes)
            log.info(f"Alpha factory: {sym.symbol} — {len(features)} features extracted")

            if not owner:
                log.warning("Alpha factory: no admin/quant user found to own generated candidates -- skipping")
                continue

            ts = datetime.now(timezone.utc)
            sym_tag = sym.symbol.replace("/", "")

            pysr_result = run_pysr_search(prices, volumes)
            if "error" not in pysr_result:
                db.add(Strategy(
                    name=f"ALPHAFACTORY-PYSR-{sym_tag}-{ts:%Y%m%d%H%M%S}",
                    created_by=owner.id,
                    lifecycle_stage="IDEA",
                    hypothesis=f"PySR-discovered formula on {sym.symbol}: {pysr_result['formula']}",
                    description="Auto-generated by Alpha Factory symbolic regression (PySR) -- "
                                 "quarantined pending human review, see config.origin.",
                    feature_list=pysr_result["features_used"],
                    allowed_symbols=[sym.symbol],
                    is_paper_only=True,
                    config={"origin": "alpha_factory_pysr", "alpha_factory_reviewed": False, **pysr_result},
                ))
                generated += 1
            else:
                log.info(f"Alpha factory PySR search skipped for {sym.symbol}: {pysr_result['error']}")

            rl_result = run_rl_search(prices, volumes)
            if "error" not in rl_result:
                db.add(Strategy(
                    name=f"ALPHAFACTORY-RL-{sym_tag}-{ts:%Y%m%d%H%M%S}",
                    created_by=owner.id,
                    lifecycle_stage="IDEA",
                    hypothesis=(
                        f"PPO-trained policy on {sym.symbol} revealed a {rl_result['action_bias']} bias "
                        f"(long {rl_result['long_frac']:.0%} / short {rl_result['short_frac']:.0%} / "
                        f"flat {rl_result['flat_frac']:.0%})"
                    ),
                    description="Auto-generated by Alpha Factory RL (stable-baselines3 PPO) -- "
                                 "quarantined pending human review, see config.origin.",
                    feature_list=rl_result["features_used"],
                    allowed_symbols=[sym.symbol],
                    is_paper_only=True,
                    config={"origin": "alpha_factory_rl", "alpha_factory_reviewed": False, **rl_result},
                ))
                generated += 1
            else:
                log.info(f"Alpha factory RL search skipped for {sym.symbol}: {rl_result['error']}")

        db.commit()
        log.info(f"Alpha factory search: {generated} quarantined candidate(s) generated")
        return {"generated": generated}


@celery_app.task(bind=True, name="rebalance_portfolio", queue="default", max_retries=1)
def rebalance_portfolio_task(self, job_id: str):
    """
    Recompute HRP target allocation weights for a capital rebalance request.
    Dispatched by capital_service.compute_rebalance() -- see that function for
    why this exists (previously created a BacktestJob row and did nothing).
    """
    from app.services.quant_engine import compute_hrp_allocation

    with SyncSession() as db:
        job = db.get(BacktestJob, uuid.UUID(job_id))
        if not job:
            return {"error": "job_not_found"}

        job.status = "RUNNING"
        job.started_at = datetime.now(timezone.utc)
        db.commit()

        try:
            positions = db.execute(
                select(Position).where(
                    Position.user_id == job.submitted_by,
                    Position.is_open.is_(True),
                )
            ).scalars().all()

            sym_ids = list({p.symbol_id for p in positions})
            sym_map = {}
            if sym_ids:
                for s in db.execute(select(Symbol).where(Symbol.id.in_(sym_ids))).scalars().all():
                    sym_map[s.id] = s

            asset_exposure: dict[str, float] = {}
            for p in positions:
                sym = sym_map.get(p.symbol_id)
                if not sym:
                    continue
                base = sym.base_asset.upper()
                asset_exposure[base] = asset_exposure.get(base, 0) + float(p.qty) * float(p.avg_cost)

            returns_matrix: dict[str, list[float]] = {}
            for sym_obj in db.execute(select(Symbol).where(Symbol.is_active.is_(True))).scalars().all():
                base = sym_obj.base_asset.upper()
                if base not in asset_exposure:
                    continue
                ticks = db.execute(
                    select(MarketTick)
                    .where(MarketTick.symbol_id == sym_obj.id)
                    .order_by(MarketTick.time.desc())
                    .limit(100)
                ).scalars().all()
                t_list = list(reversed(ticks))
                if len(t_list) >= 10:
                    prices = [float(t.price) for t in t_list]
                    returns_matrix[base] = list(np.diff(np.log(np.array(prices) + 1e-10)))

            if len(returns_matrix) >= 2:
                target_weights = compute_hrp_allocation(returns_matrix)
            else:
                n = max(len(asset_exposure), 1)
                target_weights = {k: 1.0 / n for k in asset_exposure}

            job.full_report = {
                "type": "REBALANCE",
                "target_weights": target_weights,
                "asset_exposure_usd": asset_exposure,
            }
            job.status = "COMPLETE"
            job.progress_pct = 100
            job.completed_at = datetime.now(timezone.utc)
            db.commit()
            return {"job_id": job_id, "target_weights": target_weights}

        except Exception as e:
            job.status = "FAILED"
            job.error_message = str(e)
            db.commit()
            log.error(f"rebalance_portfolio_task error: {e}", exc_info=True)
            return {"error": str(e)}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _simulate_fold(prices, regime, daily_vol, ofi, cost_model):
    if len(prices) < 5:
        return {"returns": [], "n_trades": 0, "win_rate": 0.5, "total_return_pct": 0.0, "max_dd": 0.0}

    regime_mult = REGIME_SIZE_MULT.get(regime, 0.8)
    ofi_mult    = max(0.4, 1.0 + ofi.get("net_modifier", 0))
    size        = round(regime_mult * ofi_mult * 0.1, 4)
    fee_bps     = COST_MODEL_FEE_BPS.get(cost_model or "FULL", 8)
    fee         = fee_bps / 10_000

    arr = np.array(prices, dtype=float)
    returns, equity, peak, max_dd, wins = [], 1.0, 1.0, 0.0, 0
    n_trades = 0

    for i in range(10, len(arr) - 1):
        window = arr[i-10:i]
        mu_w   = float(window.mean())
        sd_w   = float(window.std()) + 1e-8
        z      = (arr[i] - mu_w) / sd_w
        if abs(z) > 1.8:
            direction = -1 if z > 0 else 1
            ret_raw   = direction * (arr[i+1] - arr[i]) / arr[i] * size
            ret_net   = ret_raw - abs(ret_raw) * fee
            equity   *= (1 + ret_net)
            peak      = max(peak, equity)
            max_dd    = min(max_dd, (equity - peak) / peak)
            returns.append(ret_net)
            n_trades += 1
            if ret_net > 0:
                wins += 1

    return {
        "returns": returns, "n_trades": n_trades,
        "win_rate": round(wins / n_trades, 4) if n_trades else 0.5,
        "total_return_pct": round((equity - 1) * 100, 4),
        "max_dd": round(max_dd * 100, 4),
    }


def _build_equity_curve(prices, folds):
    equity = 100_000.0
    curve  = []
    n_days = min(90, len(prices))
    rng    = np.random.default_rng(42)
    for day in range(n_days):
        fi    = min(day * len(folds) // n_days, len(folds) - 1)
        r     = folds[fi]["total_return_pct"] / 100 / max(len(prices) // len(folds), 1)
        equity *= (1 + r + rng.normal(0, 0.002))
        curve.append({"day": day, "value": round(max(equity, 10_000), 2)})
    return curve


def _compute_regime_breakdown(prices, n_folds):
    from app.services.quant_engine import detect_regime_hmm
    counts = {"BULL": 0, "BEAR": 0, "RANGE": 0, "CRISIS": 0}
    fold_size = max(1, len(prices) // n_folds)
    for i in range(n_folds):
        chunk = prices[i*fold_size:(i+1)*fold_size]
        if len(chunk) >= 10:
            r = detect_regime_hmm(chunk)["regime"]
            counts[r] = counts.get(r, 0) + 1
    total = sum(counts.values()) or 1
    return {k: round(v / total, 4) for k, v in counts.items()}


def _cost_analysis(n_trades, prices, cost_model):
    avg_p = float(np.mean(prices)) if prices else 50_000.0
    fee   = COST_MODEL_FEE_BPS.get(cost_model or "FULL", 8)
    return {
        "total_slippage_bps": round(fee * 0.4, 2),
        "total_commission":   round(n_trades * avg_p * fee / 10_000, 2),
        "total_cost_bps":     fee, "cost_model": cost_model,
    }


def _synthetic_prices(n):
    rng    = np.random.default_rng(42)
    shocks = rng.normal(0.0002, 0.012, n)
    prices = [50_000.0]
    for s in shocks:
        prices.append(prices[-1] * math.exp(s))
    return prices


def _synthetic_times(n):
    """Evenly-spaced (1-minute) timestamps for the synthetic-price fallback
    path, so the OU half-life filter (which needs wall-clock time, not just
    bar count) still has something real to convert against."""
    now = datetime.now(timezone.utc)
    return [now - timedelta(minutes=(n - i)) for i in range(n)]


def _align_hedge_prices(times, hedge_ticks):
    """Asof-align a hedge symbol's own tick series onto `times` (the primary
    symbol's tick timestamps): each output point is the hedge price at the
    most recent hedge tick at-or-before that timestamp. Tick timelines
    between two different symbols never line up exactly, so this is a
    documented approximation, not tick-for-tick synchronization."""
    if not hedge_ticks:
        return None
    hedge_epoch  = np.array([t.time.timestamp() for t in hedge_ticks])
    hedge_prices = np.array([float(t.price) for t in hedge_ticks])
    out = []
    for t in times:
        idx = int(np.searchsorted(hedge_epoch, t.timestamp(), side="right")) - 1
        idx = max(0, min(idx, len(hedge_prices) - 1))
        out.append(float(hedge_prices[idx]))
    return out


def _profit_factor(returns):
    """Real profit factor: gross profit / gross loss across every closed
    trade (replaces the old `sharpe*0.8+0.6` placeholder, which wasn't a
    profit factor at all -- it never looked at the trades themselves)."""
    if not returns:
        return 0.0
    gains  = sum(r for r in returns if r > 0)
    losses = abs(sum(r for r in returns if r < 0))
    if losses < 1e-12:
        return round(gains, 4) if gains > 0 else 0.0
    return round(gains / losses, 4)


def _parse_date(date_str, end=False):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if end:
            dt = dt + timedelta(days=1) - timedelta(seconds=1)
        return dt
    except Exception:
        return datetime.now(timezone.utc) if end else datetime.now(timezone.utc) - timedelta(days=90)


def _sharpe(returns):
    if len(returns) < 2: return 0.0
    import statistics
    mu = statistics.mean(returns)
    sd = statistics.stdev(returns)
    return round((mu / sd) * math.sqrt(252 * 390), 4) if sd else 0.0


def _compute_psi(reference, current, n_bins=10):
    try:
        min_v = min(reference.min(), current.min())
        max_v = max(reference.max(), current.max())
        bins  = np.linspace(min_v, max_v, n_bins + 1)
        rh, _ = np.histogram(reference, bins=bins)
        ch, _ = np.histogram(current, bins=bins)
        rp = (rh / len(reference)).clip(1e-6)
        cp = (ch / len(current)).clip(1e-6)
        return float(np.sum((cp - rp) * np.log(cp / rp)))
    except Exception:
        return 0.0


def _mutate_config(config):
    import copy
    rng = np.random.default_rng()
    m = copy.deepcopy(config)
    for k, v in m.items():
        # bool is a subclass of int in Python -- without excluding it, a
        # reviewed Alpha Factory candidate's config.alpha_factory_reviewed
        # (True) would get numerically perturbed by a clone (still truthy,
        # but no longer a clean boolean, and the child never went through
        # its own review despite inheriting the flag as if it had).
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v != 0:
            m[k] = round(v + rng.normal(0, abs(v) * 0.1), 6)
    m["_mutated_at"] = datetime.now(timezone.utc).isoformat()
    return m


# ─── Scheduled Sharpe re-computation ──────────────────────────────────────────
# Two tasks, run in this order every night (see beat schedule below): reap
# stale RUNNING jobs first, then dispatch fresh backtests for the 5 real v10
# strategies. Split apart rather than combined into one per-strategy check
# because a stale RUNNING row is not guaranteed to be the strategy's *latest*
# BacktestJob (a newer QUEUED row can sit on top of it) -- a per-strategy
# "check latest, reap if stale" loop would miss exactly that case. Sweeping
# every RUNNING row table-wide, independent of scope, is what catches it.

STALE_BACKTEST_AFTER = timedelta(hours=3)


@celery_app.task(name="reap_stale_backtest_jobs", queue="backtest")
def reap_stale_backtest_jobs():
    """Marks FAILED any BacktestJob stuck at RUNNING for longer than
    STALE_BACKTEST_AFTER -- nothing else in this codebase ever reaps a job
    whose worker process died mid-run (a job only leaves RUNNING today if
    the task that's actually processing it completes or raises)."""
    with SyncSession() as db:
        cutoff = datetime.now(timezone.utc) - STALE_BACKTEST_AFTER
        stale = db.execute(
            select(BacktestJob).where(
                BacktestJob.status == "RUNNING",
                BacktestJob.started_at < cutoff,
            )
        ).scalars().all()
        for job in stale:
            job.status = "FAILED"
            job.error_message = "Reaped: stale RUNNING job, no worker activity detected"
            job.completed_at = datetime.now(timezone.utc)
            log.warning(
                f"reap_stale_backtest_jobs: reaped job={job.id} strategy={job.strategy_id} "
                f"started_at={job.started_at}"
            )
        db.commit()
        return {"reaped": len(stale)}


@celery_app.task(name="refresh_v10_strategy_backtests", queue="backtest")
def refresh_v10_strategy_backtests():
    """Nightly Sharpe refresh for the 5 real v10 strategies (identified by
    config.strategy_key, same convention run_backtest_task itself uses) --
    without this, sharpe_last only ever updates when someone manually
    re-submits a backtest. Skips a strategy whose latest non-rebalance job is
    still QUEUED/RUNNING rather than piling a second one on top."""
    with SyncSession() as db:
        strategies = db.execute(select(Strategy)).scalars().all()
        in_scope = [s for s in strategies if (s.config or {}).get("strategy_key") in V10_STRATEGY_KEYS]

        queued_count, skipped_count = 0, 0
        for s in in_scope:
            latest = db.execute(
                select(BacktestJob)
                .where(
                    BacktestJob.strategy_id == s.id,
                    BacktestJob.config["type"].as_string().is_distinct_from("REBALANCE"),
                )
                .order_by(BacktestJob.created_at.desc())
                .limit(1)
            ).scalar_one_or_none()

            if latest is not None and latest.status in ("QUEUED", "RUNNING"):
                skipped_count += 1
                continue

            job = BacktestJob(
                strategy_id=s.id,
                submitted_by=s.created_by,
                status="QUEUED",
                start_date=(datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d"),
                end_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                symbols=s.allowed_symbols or ["BTC/USDT"],
                cost_model="FULL",
            )
            db.add(job)
            db.flush()
            try:
                task = run_backtest_task.delay(str(job.id))
                job.celery_task_id = task.id
            except Exception:
                job.celery_task_id = "no-celery"
            queued_count += 1
        db.commit()
        log.info(
            f"refresh_v10_strategy_backtests: queued={queued_count} "
            f"skipped={skipped_count} scope={len(in_scope)}"
        )
        return {"queued": queued_count, "skipped": skipped_count}


@celery_app.task(name="historical_backfill", queue="backtest", max_retries=1)
def historical_backfill_task(symbol: str, days: int = 30, timeframe: str = "5m", exchange_id: str | None = None):
    """
    Guide Part III / Ch.12 "historical data depth for research" -- on-demand
    backfill for one symbol (see historical_backfill_service.py's module
    docstring for how candle-derived synthetic ticks are built and why).
    Triggered via POST /market/backfill; not on a beat schedule -- there's
    no vendor-agnostic default watchlist/date-range worth running nightly
    for every symbol, and a bulk import is exactly the kind of one-off,
    operator-initiated action that shouldn't silently re-run.
    """
    from app.services.historical_backfill_service import backfill_symbol
    result = backfill_symbol(symbol, days=days, timeframe=timeframe, exchange_id=exchange_id)
    log.info(f"historical_backfill: {result}")
    return result


# ─── Beat schedule ────────────────────────────────────────────────────────────
# Merge into the schedule celery_app.py already owns (which includes
# snapshot-pnl-5min) rather than overwriting it — a prior version of this
# module replaced celery_app.conf.beat_schedule wholesale, which silently
# dropped celery_app.py's entries depending on import order.
from celery.schedules import crontab

celery_app.conf.beat_schedule.update({
    "regime-scan-hourly":    {"task": "regime_scan",             "schedule": crontab(minute=0)},
    "darwin-nightly":        {"task": "darwin_evolution_cycle",  "schedule": crontab(hour=2, minute=0)},
    # Reaper must run before the refresh task so its skip-logic sees
    # post-reap state -- 5 minutes apart since beat doesn't guarantee
    # cross-entry ordering otherwise.
    "reap-stale-backtests":       {"task": "reap_stale_backtest_jobs",       "schedule": crontab(hour=4, minute=0)},
    "v10-sharpe-refresh-nightly": {"task": "refresh_v10_strategy_backtests", "schedule": crontab(hour=4, minute=5)},
    "drift-monitor-daily":   {"task": "drift_monitor",           "schedule": crontab(hour=6, minute=0)},
    "alpha-factory-nightly": {"task": "alpha_factory_search",    "schedule": crontab(hour=3, minute=0)},
})
celery_app.conf.timezone = "UTC"
