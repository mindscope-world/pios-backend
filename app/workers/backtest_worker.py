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
from app.services.quant_engine import detect_regime_hmm, REGIME_SIZE_MULT
from app.workers.celery_app import celery_app

log = logging.getLogger(__name__)


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

            if mlflow_active:
                import mlflow as _mlf
                _mlf.log_params({"symbol": sym_str, "tick_count": len(prices), "cost_model": job.cost_model})

            n_folds   = max(4, min(12, len(prices) // 50))
            fold_size = max(5, len(prices) // n_folds)
            folds_results = []

            for fold_i in range(n_folds):
                train_end  = fold_size * (fold_i + 1)
                test_start = train_end
                test_end   = min(test_start + fold_size, len(prices))
                if test_end <= test_start:
                    break

                train_prices = prices[:train_end]
                test_prices  = prices[test_start:test_end]
                test_vols    = volumes[test_start:test_end]
                test_sides   = sides[test_start:test_end]

                regime_data = detect_regime_hmm(train_prices)
                vol_data    = estimate_volatility_garch(train_prices)
                daily_vol   = vol_data["daily_vol"] / 100

                tick_dicts = [{"price": p, "volume": v, "side": s}
                              for p, v, s in zip(test_prices, test_vols, test_sides)]
                ofi = compute_ofi_signals(tick_dicts)

                fold_pnl = _simulate_fold(test_prices, regime_data["regime"], daily_vol, ofi, job.cost_model)
                fold_sharpe = _sharpe(fold_pnl["returns"])
                folds_results.append({
                    "fold": fold_i + 1,
                    "sharpe": round(fold_sharpe, 4),
                    "regime": regime_data["regime"],
                    "trades": fold_pnl["n_trades"],
                    "win_rate": fold_pnl["win_rate"],
                    "total_return_pct": fold_pnl["total_return_pct"],
                    "max_dd": fold_pnl["max_dd"],
                    "passed": fold_sharpe >= 0.8,
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
            profit_f = round(max(0.0, sharpe * 0.8 + 0.6), 4)

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

            if job.strategy_id:
                strat = db.get(Strategy, job.strategy_id)
                if strat:
                    strat.sharpe_last   = sharpe
                    strat.fitness_score = round(
                        sharpe * 0.4 + win_rate * 30 + (1 - abs(max_dd) / 30) * 30, 2
                    )

            db.commit()
            return {"job_id": job_id, "status": "COMPLETE", "sharpe": sharpe}

        except Exception as exc:
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

    log.info(f"Darwin cycle: {bottom_n} pruned, {len(new_children)} spawned")


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
    from app.db.sync_session import SyncSession
    from app.models.all_models import MarketTick, Symbol
    from app.services.quant_engine import extract_ts_features
    from sqlalchemy import select

    with SyncSession() as db:
        syms = db.execute(select(Symbol).where(Symbol.is_active.is_(True)).limit(3)).scalars().all()
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
    fee_bps     = {"FULL": 8, "FEES_ONLY": 4, "SLIPPAGE_ONLY": 4, "ZERO_COST": 0}.get(cost_model or "FULL", 8)
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
    fee   = {"FULL": 8, "FEES_ONLY": 4, "SLIPPAGE_ONLY": 4, "ZERO_COST": 0}.get(cost_model or "FULL", 8)
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
        if isinstance(v, (int, float)) and v != 0:
            m[k] = round(v + rng.normal(0, abs(v) * 0.1), 6)
    m["_mutated_at"] = datetime.now(timezone.utc).isoformat()
    return m


# ─── Beat schedule ────────────────────────────────────────────────────────────
from celery.schedules import crontab

celery_app.conf.beat_schedule = {
    "snapshot-pnl-5min":     {"task": "snapshot_pnl",           "schedule": 300.0},
    "regime-scan-hourly":    {"task": "regime_scan",             "schedule": crontab(minute=0)},
    "darwin-nightly":        {"task": "darwin_evolution_cycle",  "schedule": crontab(hour=2, minute=0)},
    "drift-monitor-daily":   {"task": "drift_monitor",           "schedule": crontab(hour=6, minute=0)},
    "alpha-factory-nightly": {"task": "alpha_factory_search",    "schedule": crontab(hour=3, minute=0)},
}
celery_app.conf.timezone = "UTC"
