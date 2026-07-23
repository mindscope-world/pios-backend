# app/services/alpha_factory_engine.py
"""
Alpha Factory Idea Generation (Guide Part III / Ch.13 — "symbolic
regression via PySR, RL via FinRL")
=============================================================================
Real PySR symbolic regression and a real RL policy (stable-baselines3 PPO,
trained against a custom minimal single-asset environment rather than
FinRL's own env class — see requirements.txt's FinRL comment for why: its
package __init__.py forces an unrelated, conflicting legacy dependency just
to expose an environment class this module reimplements in ~20 lines
anyway). Both produce a *candidate idea* — a Strategy row — not a
deployable model; nothing here writes to decision_service.py's live
inference path.

Every candidate this module produces is quarantined: see
strategy_service.ALPHA_FACTORY_ORIGINS / _check_gate. With ~hours of real
tick history in this dev environment, there is no way to tell whether a
generated candidate has genuine edge versus noise — "it ran and produced a
row" is the only available check here, not "it works" — so nothing
generated here can advance past its starting lifecycle stage without an
admin/quant explicitly setting config.alpha_factory_reviewed = true.

Search budgets (PySR niterations, PPO total_timesteps) are deliberately
small — this is a demo-scale search against limited data, not a production
symbolic-regression or RL training run.
"""
from __future__ import annotations

import logging

import numpy as np

log = logging.getLogger(__name__)

FEATURE_NAMES = ["momentum", "vol_zscore", "realized_vol", "vol_ratio"]


def _build_feature_matrix(
    prices: list[float], volumes: list[float], lookback: int = 10, horizon: int = 1,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Real, causal rolling features (each computed only from data at/before
    index i — no lookahead) and a forward log-return target. Small,
    honestly-scoped feature set — not tsfresh's full rolling-window feature
    space, which would need real additional engineering (tsfresh's own
    rolling API) to apply per-timestep rather than once over a whole series.
    """
    arr = np.array(prices, dtype=float)
    vol = np.array(volumes, dtype=float)
    n = len(arr)
    rows, targets = [], []
    for i in range(lookback, n - horizon):
        window = arr[i - lookback:i]
        w0 = window[0] if window[0] else 1e-9
        momentum = (arr[i] - window[0]) / w0
        vol_z = (arr[i] - window.mean()) / (window.std() + 1e-9)
        realized_vol = float(np.std(np.diff(np.log(window + 1e-9))))
        vwin = vol[i - lookback:i]
        vol_ratio = float(vol[i] / (vwin.mean() + 1e-9)) if len(vwin) else 1.0
        rows.append([momentum, vol_z, realized_vol, vol_ratio])
        targets.append(float(np.log((arr[i + horizon] + 1e-9) / (arr[i] + 1e-9))))
    return np.array(rows), np.array(targets)


def run_pysr_search(
    prices: list[float], volumes: list[float], n_iterations: int = 15, timeout_seconds: int = 180,
) -> dict:
    """Real PySR symbolic regression discovering a closed-form formula that
    predicts forward return from the rolling feature set above."""
    X, y = _build_feature_matrix(prices, volumes)
    if len(X) < 30:
        return {"error": "insufficient_data_for_search", "n_samples": int(len(X))}

    try:
        from pysr import PySRRegressor
    except ImportError:
        return {"error": "pysr_not_installed", "n_samples": int(len(X))}

    try:
        model = PySRRegressor(
            niterations=n_iterations,
            binary_operators=["+", "-", "*", "/"],
            unary_operators=["sin", "square"],
            model_selection="best",
            verbosity=0,
            progress=False,
            temp_equation_file=True,
            timeout_in_seconds=timeout_seconds,
        )
        model.fit(X, y, variable_names=FEATURE_NAMES)
        best = model.get_best()
        return {
            "formula": str(best["equation"]),
            "features_used": FEATURE_NAMES,
            "n_samples": int(len(X)),
            "loss": float(best["loss"]),
            "complexity": int(best["complexity"]),
        }
    except Exception as e:  # noqa: BLE001
        log.warning(f"PySR search failed: {e}")
        return {"error": f"pysr_search_failed: {e}", "n_samples": int(len(X))}


class _SingleAssetTradingEnv:
    """Minimal long/flat/short single-asset Gym environment — deliberately
    small rather than FinRL's own StockTradingEnv (see module docstring)."""

    def __init__(self, features: np.ndarray, returns: np.ndarray):
        import gymnasium as gym
        from gymnasium import spaces

        self._gym = gym
        self.features = features.astype(np.float32)
        self.returns = returns.astype(np.float32)
        self.n = len(features)
        self.i = 0
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(features.shape[1],), dtype=np.float32,
        )
        self.action_space = spaces.Discrete(3)  # 0=short, 1=flat, 2=long

    def reset(self, seed=None, options=None):
        self.i = 0
        return self.features[self.i], {}

    def step(self, action: int):
        position = int(action) - 1
        reward = float(position * self.returns[self.i])
        self.i += 1
        terminated = self.i >= self.n - 1
        obs = self.features[self.i] if not terminated else self.features[-1]
        return obs, reward, terminated, False, {}


def run_rl_search(prices: list[float], volumes: list[float], total_timesteps: int = 3000) -> dict:
    """
    Trains a real PPO agent (stable-baselines3) against the minimal env
    above for `total_timesteps` — a smoke-scale training budget; production
    RL training runs millions of steps. The trained policy's own revealed
    action bias (replayed deterministically over the same data) becomes the
    candidate strategy's starting hypothesis — this generates an idea, it
    does not deploy a live inference policy.
    """
    X, y = _build_feature_matrix(prices, volumes)
    if len(X) < 30:
        return {"error": "insufficient_data_for_search", "n_samples": int(len(X))}

    try:
        import gymnasium as gym
        from stable_baselines3 import PPO
    except ImportError as e:
        return {"error": f"rl_deps_not_installed: {e}", "n_samples": int(len(X))}

    class _Env(gym.Env):
        metadata: dict = {}

        def __init__(self):
            super().__init__()
            self._inner = _SingleAssetTradingEnv(X, y)
            self.observation_space = self._inner.observation_space
            self.action_space = self._inner.action_space

        def reset(self, seed=None, options=None):
            super().reset(seed=seed)
            return self._inner.reset(seed=seed, options=options)

        def step(self, action):
            return self._inner.step(action)

    try:
        env = _Env()
        n_steps = min(256, max(32, len(X) // 2))
        model = PPO(
            "MlpPolicy", env, verbose=0,
            n_steps=n_steps, batch_size=min(32, n_steps),
        )
        model.learn(total_timesteps=total_timesteps)

        obs, _ = env.reset()
        actions = []
        for _ in range(env._inner.n - 1):
            action, _ = model.predict(obs, deterministic=True)
            actions.append(int(action))
            obs, _, terminated, _, _ = env.step(action)
            if terminated:
                break

        actions_arr = np.array(actions)
        long_frac = float((actions_arr == 2).mean()) if len(actions_arr) else 0.0
        short_frac = float((actions_arr == 0).mean()) if len(actions_arr) else 0.0
        flat_frac = float((actions_arr == 1).mean()) if len(actions_arr) else 0.0
        if long_frac > short_frac + 0.1:
            bias = "LONG_BIASED"
        elif short_frac > long_frac + 0.1:
            bias = "SHORT_BIASED"
        else:
            bias = "NEUTRAL"

        return {
            "algorithm": "PPO",
            "total_timesteps": total_timesteps,
            "n_samples": int(len(X)),
            "features_used": FEATURE_NAMES,
            "action_bias": bias,
            "long_frac": round(long_frac, 4),
            "short_frac": round(short_frac, 4),
            "flat_frac": round(flat_frac, 4),
        }
    except Exception as e:  # noqa: BLE001
        log.warning(f"RL search failed: {e}")
        return {"error": f"rl_search_failed: {e}", "n_samples": int(len(X))}
