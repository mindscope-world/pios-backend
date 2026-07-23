# app/services/hyperparameter_search.py
"""
Hyperparameter Optimization (Guide Part III / Ch.14 — "Optuna, Tree-Parzen
Estimator, median pruner")
=============================================================================
Runs entirely inside a single walk-forward fold's TRAINING slice — it never
touches that fold's held-out test slice. The search further splits the
training slice into nested, expanding sub-windows so the median pruner has
intermediate values to prune against (Optuna's pruners need >1 reported
value per trial; a single train-Sharpe-per-trial objective would have none
to prune on). The winning trial's params are then handed to the caller to
score the fold's real test slice exactly once — if the search ever touched
the test slice, every Sharpe reported downstream would be inflated by
lookahead bias, which is precisely the failure mode the guide's own
"an optimistic backtest is more dangerous than none" line warns about.

Opt-in, not automatic: a BacktestJob only runs this when its own
config.hpo is true (see backtest_worker.run_backtest_task) — real Optuna
trials cost real compute per fold, and defaulting every backtest to a
20-trial nested search would silently multiply every job's runtime.
"""
from __future__ import annotations

import logging

import numpy as np
import optuna
from optuna.pruners import MedianPruner
from optuna.samplers import TPESampler

from app.services.strategy_signals import DEFAULT_STRATEGY_PARAMS

log = logging.getLogger(__name__)

optuna.logging.set_verbosity(optuna.logging.WARNING)

# Multiplicative search bounds applied to each strategy's own
# DEFAULT_STRATEGY_PARAMS value, so the search space scales to whatever that
# param's default already is rather than one global range that would make no
# sense across e.g. a window-size param and a z-score threshold.
_SEARCH_BOUNDS: dict[str, tuple[float, float]] = {
    "entry_z": (0.5, 2.2),
    "exit_z": (0.3, 2.2),
    "sl_atr_mult": (0.5, 2.5),
    "trailing_atr_mult": (0.5, 2.5),
    "protective_atr_mult": (0.5, 2.0),
    "entry_vol_ratio": (0.6, 1.6),
    "entry_price_signal": (0.6, 1.8),
    "min_confidence": (0.7, 1.3),
    "trend_h": (0.9, 1.1),
    "range_h": (0.9, 1.1),
    "mid_band_size_mult": (0.5, 1.8),
    "base_size": (0.4, 2.0),
    "coint_pvalue_max": (0.5, 2.0),
}
_DEFAULT_MULT_BOUNDS = (0.6, 1.6)  # window/period/hours-style params
_INT_PARAM_HINTS = ("window", "period", "chunks", "hours")


def _suggest_params(trial: "optuna.Trial", strategy_key: str) -> dict:
    base = DEFAULT_STRATEGY_PARAMS[strategy_key]
    params = dict(base)
    for name, default in base.items():
        if not isinstance(default, (int, float)) or isinstance(default, bool):
            continue
        lo_mult, hi_mult = _SEARCH_BOUNDS.get(name, _DEFAULT_MULT_BOUNDS)
        lo, hi = default * lo_mult, default * hi_mult
        if lo > hi:
            lo, hi = hi, lo
        if lo == hi:
            continue
        if isinstance(default, int) and any(h in name for h in _INT_PARAM_HINTS):
            lo_i, hi_i = max(2, int(round(lo))), max(int(round(hi)), int(round(lo)) + 1)
            params[name] = trial.suggest_int(name, lo_i, hi_i)
        else:
            params[name] = round(trial.suggest_float(name, lo, hi), 6)
    return params


def search_fold_hyperparameters(
    strategy_key: str,
    train_prices: list[float],
    train_times: list,
    train_vols: list[float],
    train_sides: list[str],
    regime: str,
    confidence: float,
    cost_model: str | None,
    hedge_prices: list[float] | None = None,
    n_trials: int = 20,
    n_sub_folds: int = 4,
) -> dict:
    """
    TPE-sampled, median-pruned search over strategy_key's numeric params,
    scored ONLY against train_prices. Returns:
      {"best_params", "best_train_sharpe", "n_trials_completed",
       "n_trials_pruned", "skipped_reason" (only when search didn't run)}
    Falls back to the strategy's own fixed defaults when the training slice
    is too short to sub-fold meaningfully, or on any search-internal error —
    callers always get back a usable, real params dict.
    """
    from app.services.strategy_signals import simulate_fold as v10_simulate_fold

    n = len(train_prices)
    if strategy_key not in DEFAULT_STRATEGY_PARAMS or n < n_sub_folds * 20:
        return {
            "best_params": DEFAULT_STRATEGY_PARAMS.get(strategy_key, {}),
            "best_train_sharpe": None,
            "n_trials_completed": 0,
            "n_trials_pruned": 0,
            "skipped_reason": "training_slice_too_short",
        }

    sub_size = n // n_sub_folds

    def _sub_sharpe(params: dict, k: int) -> float:
        end = sub_size * (k + 1)
        result = v10_simulate_fold(
            strategy_key,
            train_prices[:end], train_times[:end], train_vols[:end], train_sides[:end],
            regime, confidence, cost_model, params=params,
            hedge_prices=hedge_prices[:end] if hedge_prices else None,
        )
        rets = result.get("returns") or []
        if len(rets) < 2:
            return 0.0
        arr = np.array(rets, dtype=float)
        return float(arr.mean() / (arr.std() + 1e-9) * np.sqrt(252))

    def _objective(trial: "optuna.Trial") -> float:
        params = _suggest_params(trial, strategy_key)
        running: list[float] = []
        for k in range(n_sub_folds):
            running.append(_sub_sharpe(params, k))
            trial.report(float(np.mean(running)), step=k)
            if trial.should_prune():
                raise optuna.TrialPruned()
        return float(np.mean(running))

    study = optuna.create_study(
        direction="maximize",
        sampler=TPESampler(seed=42),
        pruner=MedianPruner(n_warmup_steps=1),
    )
    try:
        study.optimize(_objective, n_trials=n_trials, catch=(Exception,))
    except Exception as e:  # noqa: BLE001
        log.warning(f"Optuna search failed for {strategy_key}: {e}")
        return {
            "best_params": DEFAULT_STRATEGY_PARAMS[strategy_key],
            "best_train_sharpe": None,
            "n_trials_completed": 0,
            "n_trials_pruned": 0,
            "skipped_reason": "search_error",
        }

    completed = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    pruned = [t for t in study.trials if t.state == optuna.trial.TrialState.PRUNED]
    if not completed:
        return {
            "best_params": DEFAULT_STRATEGY_PARAMS[strategy_key],
            "best_train_sharpe": None,
            "n_trials_completed": 0,
            "n_trials_pruned": len(pruned),
            "skipped_reason": "no_completed_trials",
        }

    best_params = dict(DEFAULT_STRATEGY_PARAMS[strategy_key])
    best_params.update(study.best_params)
    return {
        "best_params": best_params,
        "best_train_sharpe": round(study.best_value, 4),
        "n_trials_completed": len(completed),
        "n_trials_pruned": len(pruned),
    }
