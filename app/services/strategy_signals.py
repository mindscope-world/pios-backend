# app/services/strategy_signals.py
"""
PiOS Proprietary Strategy Signals — v10 D2.2 "The Five Strategies"

Real, fixed, hardcoded signal logic for the five proprietary strategies
defined in PiOSQ_Complete_v10_Specification D2.2. There is no user-strategy-
authoring concept here or anywhere upstream of this module: `simulate_fold`
dispatches purely on a `strategy_key` string (one of `STRATEGY_KEYS`) to one
of five fixed formulas below. An unrecognized/absent key gets no real
trading logic at all (see `backtest_worker.run_backtest_task`, which falls
back to the pre-existing generic placeholder rule and labels it as such in
`full_report` -- it is never silently mistaken for one of these five).

Math is sourced verbatim from the v10 spec (each docstring quotes it).
Where the spec gives a formula shape but not a specific window/threshold
constant, a labeled default is used, exposed via `params` so it can be
tuned (e.g. by Darwin mutation) without touching the formula itself.
"""
from __future__ import annotations

from typing import Any

import numpy as np

from app.services.quant_engine import (
    COST_MODEL_FEE_BPS,
    estimate_ou_process,
    compute_hurst_exponent,
    estimate_gjr_garch_path,
    compute_rolling_ols_residual,
)

STRATEGY_KEYS = [
    "OFI_MOMENTUM",
    "OU_MEAN_REVERSION",
    "GARCH_BREAKOUT",
    "BTC_NEUTRAL_MEAN_REVERSION",
    "HURST_ADAPTIVE_META",
]

# Defaults for windows/thresholds the spec doesn't pin to a specific number
# (the formula shape itself is always taken verbatim from the spec text —
# see each strategy's docstring). Stored on the seeded Strategy.config so
# they're visible/tunable per strategy row, not buried in code.
DEFAULT_STRATEGY_PARAMS: dict[str, dict[str, Any]] = {
    "OFI_MOMENTUM": {
        "ofi_agg_window": 20,   # ticks summed into OFI_t
        "ofi_z_window": 50,     # spec: mean(OFI,50) / std(OFI,50)
        "entry_z": 1.5,         # spec: signal_z > +1.5 / < -1.5
        "atr_period": 14,
        "sl_atr_mult": 1.5,     # spec: SL = 1.5 x ATR_14
        "base_size": 0.1,
    },
    "OU_MEAN_REVERSION": {
        "ou_window": 60,        # rolling OU refit + z-score window N
        "entry_z": 2.0,         # spec: z < -2.0 or z > +2.0
        "exit_z": 0.5,          # convergence exit (spec gives this exact
                                 # number for the sibling BTC-Neutral
                                 # strategy's mean-reversion exit; reused
                                 # here since Strategy 2 gives no exit of
                                 # its own beyond the half-life filter)
        "min_half_life_hours": 2.0,
        "max_half_life_hours": 120.0,
        "protective_atr_mult": 4.0,  # non-spec safety net (see docstring)
        "atr_period": 14,
        "base_size": 0.1,
    },
    "GARCH_BREAKOUT": {
        "vol_ratio_window": 20,   # spec: vol_ratio = sigma_t / rolling_mean(sigma,20)
        "price_signal_window": 20,
        "entry_vol_ratio": 1.8,   # spec: vol_ratio > 1.8
        "entry_price_signal": 1.0,  # spec: |price_signal| > 1.0
        "atr_period": 14,
        "trailing_atr_mult": 2.0,  # spec: Trailing stop 2 x ATR_14
        "base_size": 0.1,
    },
    "BTC_NEUTRAL_MEAN_REVERSION": {
        "ols_window": 60,       # spec: rolling OLS, window=60
        "entry_z": 2.0,         # spec: z < -2.0 / z > +2.0
        "exit_z": 0.5,          # spec: Exit |z| < 0.5
        "coint_pvalue_max": 0.05,  # spec: coint p-value < 0.05
        "base_size": 0.1,
        "hedge_symbol": "BTC/USDT",
    },
    "HURST_ADAPTIVE_META": {
        "hurst_window": 100,     # trailing return count fed into R/S
        "hurst_n_chunks": 4,
        "trend_h": 0.55,         # spec: H > 0.55 -> Strategy 1
        "range_h": 0.45,         # spec: H < 0.45 -> Strategy 2
        "min_confidence": 0.65,  # spec: confidence > 0.65 required
        "mid_band_size_mult": 0.5,  # spec: 0.45-0.55 -> size x0.5
        "base_size": 0.1,
    },
}


# ─── Shared helpers ────────────────────────────────────────────────────────

def _fee(cost_model: str | None) -> float:
    return COST_MODEL_FEE_BPS.get(cost_model or "FULL", 8) / 10_000


def _rolling_zscore(arr: np.ndarray, window: int) -> np.ndarray:
    out = np.zeros(len(arr))
    for i in range(window, len(arr) + 1):
        w = arr[i - window:i]
        sd = w.std()
        out[i - 1] = (arr[i - 1] - w.mean()) / sd if sd > 1e-12 else 0.0
    return out


def _atr_series(prices: np.ndarray, period: int = 14) -> np.ndarray:
    """Causal Wilder-smoothed ATR (close-to-close true range — no OHLC in
    tick data, matching quant_engine._calc_atr's own definition)."""
    n = len(prices)
    atr = np.zeros(n)
    if n < 2:
        return atr
    tr = np.abs(np.diff(prices))
    running = float(tr[0])
    atr[1] = running
    for i in range(2, n):
        idx = i - 1
        if i <= period:
            running = float(tr[:idx + 1].mean())
        else:
            running = (running * (period - 1) + float(tr[idx])) / period
        atr[i] = running
    if n > 1:
        atr[0] = atr[1]
    return atr


def _ofi_z_series(volumes: np.ndarray, sides: list[str], agg_window: int, z_window: int) -> np.ndarray:
    """OFI_t = sum(signed volume) over N ticks; signal_z = rolling z-score
    of OFI_t (v10 D2.2 Strategy 1)."""
    signed = np.array([
        v if str(s).upper() in ("BUY", "BID", "B")
        else (-v if str(s).upper() in ("SELL", "ASK", "S") else 0.0)
        for v, s in zip(volumes, sides)
    ])
    ofi = np.zeros(len(signed))
    for i in range(len(signed)):
        lo = max(0, i - agg_window + 1)
        ofi[i] = signed[lo:i + 1].sum()
    return _rolling_zscore(ofi, z_window)


def _avg_seconds_per_bar(times) -> float:
    if not times or len(times) < 2:
        return 60.0
    deltas = [(times[i] - times[i - 1]).total_seconds() for i in range(1, len(times))]
    deltas = [d for d in deltas if d > 0]
    return float(np.mean(deltas)) if deltas else 60.0


def _empty_result(reason: str, strategy_key: str | None = None) -> dict:
    # Tagged with the real strategy_key (never "GENERIC_PLACEHOLDER") so a
    # fold that skipped for lack of data is never mistaken, in full_report,
    # for one that ran the old legacy simulator.
    engine = f"{strategy_key}_SKIPPED" if strategy_key else "V10_SKIPPED"
    return {
        "returns": [], "n_trades": 0, "win_rate": 0.5,
        "total_return_pct": 0.0, "max_dd": 0.0,
        "diagnostics": {"skipped_reason": reason, "engine": engine},
    }


def _close_trade(direction: int, entry_price: float, exit_price: float, size: float, fee: float) -> float:
    ret_raw = direction * (exit_price - entry_price) / entry_price * size
    return ret_raw - abs(ret_raw) * fee


def _run_price_engine(prices: np.ndarray, fee: float, warmup: int, entry_fn, update_stop_fn, should_exit_fn) -> dict:
    """
    Generic multi-bar position engine shared by the four directional,
    price-based strategies (OFI, OU, GARCH, and Hurst's delegated signal).
    `entry_fn(i) -> {"direction", "stop", "size"} | None`
    `update_stop_fn(position, i, price) -> new_stop | None`
    `should_exit_fn(position, i, price) -> bool`
    """
    position = None
    returns: list[float] = []
    equity, peak, max_dd, wins, n_trades = 1.0, 1.0, 0.0, 0, 0

    def _close(exit_price: float):
        nonlocal equity, peak, max_dd, wins, n_trades, position
        ret_net = _close_trade(position["direction"], position["entry_price"], exit_price, position["size"], fee)
        equity *= (1 + ret_net)
        peak = max(peak, equity)
        max_dd = min(max_dd, (equity - peak) / peak)
        returns.append(ret_net)
        n_trades += 1
        if ret_net > 0:
            wins += 1
        position = None

    for i in range(warmup, len(prices)):
        price = float(prices[i])
        if position is None:
            sig = entry_fn(i)
            if sig:
                position = {
                    "direction": sig["direction"],
                    "entry_price": price,
                    "entry_i": i,
                    "stop": sig.get("stop"),
                    "size": sig.get("size", 0.1),
                }
            continue

        new_stop = update_stop_fn(position, i, price)
        if new_stop is not None:
            position["stop"] = new_stop

        stop_hit = position["stop"] is not None and (
            (position["direction"] == 1 and price <= position["stop"])
            or (position["direction"] == -1 and price >= position["stop"])
        )
        if stop_hit or should_exit_fn(position, i, price):
            _close(price)

    if position is not None:
        _close(float(prices[-1]))

    return {
        "returns": returns, "n_trades": n_trades,
        "win_rate": round(wins / n_trades, 4) if n_trades else 0.5,
        "total_return_pct": round((equity - 1) * 100, 4),
        "max_dd": round(max_dd * 100, 4),
    }


# ─── Strategy 1 — OFI Momentum ─────────────────────────────────────────────

def _sim_ofi_momentum(prices, volumes, sides, regime, cost_model, params) -> dict:
    """
    v10 D2.2 Strategy 1 (D3 Engine 1 primary):
        OFI_t = Σ(ΔBidVol - ΔAskVol) over N ticks
        signal_z = (OFI_t - mean(OFI,50)) / std(OFI,50)
        Entry: signal_z > +1.5 (long) / < -1.5 (short) AND regime = trending
        SL = 1.5 x ATR_14
    Exit condition isn't specified beyond the stop -- momentum-exhaustion
    exit (z crossing back through 0 against the held direction) is an
    engineering addition to avoid indefinite unstopped holds, not spec math.
    """
    p = np.asarray(prices, dtype=float)
    fee = _fee(cost_model)
    z = _ofi_z_series(np.asarray(volumes, dtype=float), sides, params["ofi_agg_window"], params["ofi_z_window"])
    atr = _atr_series(p, params["atr_period"])
    trending = regime in ("BULL", "BEAR")
    warmup = max(params["ofi_z_window"], params["atr_period"]) + 1

    def entry_fn(i):
        if not trending:
            return None
        zi = z[i]
        if zi > params["entry_z"]:
            direction = 1
        elif zi < -params["entry_z"]:
            direction = -1
        else:
            return None
        a = atr[i] or (p[i] * 0.01)
        stop = p[i] - direction * params["sl_atr_mult"] * a
        return {"direction": direction, "stop": stop, "size": params["base_size"]}

    def update_stop_fn(position, i, price):
        return None  # fixed SL, no trailing

    def should_exit_fn(position, i, price):
        return (position["direction"] == 1 and z[i] < 0) or (position["direction"] == -1 and z[i] > 0)

    result = _run_price_engine(p, fee, warmup, entry_fn, update_stop_fn, should_exit_fn)
    result["diagnostics"] = {"engine": "OFI_MOMENTUM", "regime_ok": trending, "last_z": round(float(z[-1]), 4)}
    return result


# ─── Strategy 2 — Ornstein-Uhlenbeck Mean Reversion ────────────────────────

def _ou_z_and_halflife_series(p: np.ndarray, window: int, avg_sec_per_bar: float):
    z = np.zeros(len(p))
    half_life_hours = np.full(len(p), np.nan)
    for i in range(window, len(p)):
        w = p[i - window:i + 1]
        ou = estimate_ou_process(w.tolist(), dt=1.0)
        if ou["half_life"] is not None:
            half_life_hours[i] = ou["half_life"] * avg_sec_per_bar / 3600.0
        mu_w, sd_w = w.mean(), w.std()
        z[i] = (p[i] - mu_w) / sd_w if sd_w > 1e-12 else 0.0
    return z, half_life_hours


def _sim_ou_mean_reversion(prices, times, regime, cost_model, params) -> dict:
    """
    v10 D2.2 Strategy 2 (D3 Engine 3 gate):
        dX = kappa(mu - X)dt + sigma dW
        kappa = -log(1+b)/dt   mu = -a/b   half_life = log(2)/kappa
        Only trade when 2h < half_life < 120h
        z_score = (X_t - rolling_mean(X,N)) / rolling_std(X,N)
        Entry: z < -2.0 or z > +2.0 AND regime = ranging
    No stop/exit given beyond the half-life filter itself; exit uses the
    same |z|<0.5 convergence threshold the spec gives explicitly for the
    sibling BTC-Neutral mean-reversion strategy. `protective_atr_mult` is a
    non-spec safety-net stop (a fully unstopped strategy isn't realistic to
    run in a backtest engine that has to report a max drawdown).
    """
    p = np.asarray(prices, dtype=float)
    fee = _fee(cost_model)
    window = params["ou_window"]
    avg_sec = _avg_seconds_per_bar(times)
    z, half_life_hours = _ou_z_and_halflife_series(p, window, avg_sec)
    atr = _atr_series(p, params["atr_period"])
    ranging = regime == "RANGE"
    warmup = window + 1

    def entry_fn(i):
        if not ranging:
            return None
        hl = half_life_hours[i]
        if np.isnan(hl) or not (params["min_half_life_hours"] < hl < params["max_half_life_hours"]):
            return None
        zi = z[i]
        if zi < -params["entry_z"]:
            direction = 1
        elif zi > params["entry_z"]:
            direction = -1
        else:
            return None
        a = atr[i] or (p[i] * 0.01)
        stop = p[i] - direction * params["protective_atr_mult"] * a
        return {"direction": direction, "stop": stop, "size": params["base_size"]}

    def update_stop_fn(position, i, price):
        return None

    def should_exit_fn(position, i, price):
        return abs(z[i]) < params["exit_z"]

    result = _run_price_engine(p, fee, warmup, entry_fn, update_stop_fn, should_exit_fn)
    valid_hl = half_life_hours[~np.isnan(half_life_hours)]
    result["diagnostics"] = {
        "engine": "OU_MEAN_REVERSION", "regime_ok": ranging,
        "last_z": round(float(z[-1]), 4),
        "last_half_life_hours": round(float(valid_hl[-1]), 4) if len(valid_hl) else None,
    }
    return result


# ─── Strategy 3 — GARCH Breakout ────────────────────────────────────────────

def _sim_garch_breakout(prices, regime, cost_model, params) -> dict:
    """
    v10 D2.2 Strategy 3 (D3 Engine 2 primary):
        sigma^2(t+1) = omega + alpha*eps^2(t) + gamma*I-(t)*eps^2(t) + beta*sigma^2(t)   [GJR-GARCH(1,1)]
        vol_ratio = sigma_t / rolling_mean(sigma,20)
        Entry: vol_ratio > 1.8 AND |price_signal| > 1.0 AND regime = volatile
        Trailing stop 2 x ATR_14
    "price_signal" isn't defined elsewhere in the spec; used here as the
    rolling z-score of price itself (the same z-score construction the spec
    uses everywhere else), and its sign gives the breakout direction.
    "regime = volatile" maps to this codebase's CRISIS regime label (the
    only volatility-flavored state in the existing 4-state HMM regime enum).
    """
    p = np.asarray(prices, dtype=float)
    fee = _fee(cost_model)
    atr = _atr_series(p, params["atr_period"])
    volatile = regime == "CRISIS"

    garch = estimate_gjr_garch_path(p.tolist())
    vol_path = np.asarray(garch["vol_path"], dtype=float)
    # vol_path is aligned to log-returns (len = len(p)-1); pad front to align to p
    vol_path = np.concatenate([[vol_path[0] if len(vol_path) else 0.0], vol_path])
    vol_ratio_window = params["vol_ratio_window"]
    vol_mean = np.zeros(len(vol_path))
    for i in range(vol_ratio_window, len(vol_path)):
        vol_mean[i] = vol_path[i - vol_ratio_window:i].mean()
    vol_ratio = np.divide(vol_path, vol_mean, out=np.ones_like(vol_path), where=vol_mean > 1e-12)

    price_signal = _rolling_zscore(p, params["price_signal_window"])
    warmup = max(vol_ratio_window, params["price_signal_window"], params["atr_period"]) + 1

    def entry_fn(i):
        if not volatile:
            return None
        if vol_ratio[i] <= params["entry_vol_ratio"]:
            return None
        ps = price_signal[i]
        if abs(ps) <= params["entry_price_signal"]:
            return None
        direction = 1 if ps > 0 else -1
        a = atr[i] or (p[i] * 0.01)
        stop = p[i] - direction * params["trailing_atr_mult"] * a
        return {"direction": direction, "stop": stop, "size": params["base_size"]}

    def update_stop_fn(position, i, price):
        a = atr[i] or (price * 0.01)
        trail = price - position["direction"] * params["trailing_atr_mult"] * a
        if position["direction"] == 1:
            return max(position["stop"], trail) if position["stop"] is not None else trail
        return min(position["stop"], trail) if position["stop"] is not None else trail

    def should_exit_fn(position, i, price):
        return False  # trailing stop is the only exit, per spec

    result = _run_price_engine(p, fee, warmup, entry_fn, update_stop_fn, should_exit_fn)
    result["diagnostics"] = {
        "engine": garch["engine"], "regime_ok": volatile,
        "last_vol_ratio": round(float(vol_ratio[-1]), 4),
    }
    return result


# ─── Strategy 4 — BTC-Neutral Residual Mean Reversion ──────────────────────

def _sim_btc_neutral(prices, hedge_prices, cost_model, params) -> dict:
    """
    v10 D2.2 Strategy 4 (multi-symbol D2.2 Component 1):
        R_y = alpha + beta_t * R_x + eps_t     (rolling OLS, window=60)
        z = (eps_t - mean(eps,60)) / std(eps,60)
        Pre-filter: coint(y,x) p-value < 0.05
        Long y + Short beta*x when z < -2.0. Short y + Long beta*x when z > +2.0.
        Exit: |z| < 0.5
    The spread position's per-bar return is exactly the residual itself
    (R_y - beta*R_x = eps), so this strategy is simulated as a compounding
    return series rather than the price-delta engine the other three use.
    """
    fee = _fee(cost_model)
    if not hedge_prices or len(hedge_prices) < params["ols_window"] + 10:
        return _empty_result("no_hedge_symbol_data", "BTC_NEUTRAL_MEAN_REVERSION")

    ols = compute_rolling_ols_residual(prices, hedge_prices, window=params["ols_window"])
    if ols["coint_pvalue"] is None or ols["coint_pvalue"] >= params["coint_pvalue_max"]:
        r = _empty_result("cointegration_filter_failed", "BTC_NEUTRAL_MEAN_REVERSION")
        r["diagnostics"]["coint_pvalue"] = ols["coint_pvalue"]
        return r

    residual = np.asarray(ols["residual"])
    z = np.asarray(ols["z"])
    size = params["base_size"]

    position = None  # {"direction", "trade_equity"} while a spread trade is open
    returns: list[float] = []
    equity, peak, max_dd, wins, n_trades = 1.0, 1.0, 0.0, 0, 0
    warmup = params["ols_window"]

    for t in range(warmup, len(z)):
        if position is None:
            if z[t] < -params["entry_z"]:
                position = {"direction": 1, "trade_equity": 1.0}
            elif z[t] > params["entry_z"]:
                position = {"direction": -1, "trade_equity": 1.0}
            continue

        ret_raw = position["direction"] * residual[t] * size
        ret_net = ret_raw - abs(ret_raw) * fee
        position["trade_equity"] *= (1 + ret_net)
        equity *= (1 + ret_net)
        peak = max(peak, equity)
        max_dd = min(max_dd, (equity - peak) / peak)

        if abs(z[t]) < params["exit_z"]:
            trade_return = position["trade_equity"] - 1.0
            returns.append(trade_return)
            n_trades += 1
            if trade_return > 0:
                wins += 1
            position = None

    return {
        "returns": returns, "n_trades": n_trades,
        "win_rate": round(wins / n_trades, 4) if n_trades else 0.5,
        "total_return_pct": round((equity - 1) * 100, 4),
        "max_dd": round(max_dd * 100, 4),
        "diagnostics": {
            "engine": "BTC_NEUTRAL_MEAN_REVERSION",
            "coint_pvalue": ols["coint_pvalue"],
            "last_z": round(float(z[-1]), 4) if len(z) else None,
            "last_beta": round(float(ols["beta"][-1]), 6) if ols["beta"] else None,
        },
    }


# ─── Strategy 5 — Hurst Adaptive Meta-Strategy ─────────────────────────────

def _sim_hurst_adaptive(prices, times, volumes, sides, regime, regime_confidence, cost_model, params) -> dict:
    """
    v10 D2.2 Strategy 5 (D3 Engine 3 secondary gate):
        H = log(E[R/S]) / log(n)
        confidence = HMM_confidence x |H - 0.5| x 2
        H > 0.55 -> activate Strategy 1. H < 0.45 -> activate Strategy 2.
        confidence > 0.65 required. 0.45-0.55 -> size x0.5
    Delegates to the OFI-momentum and OU-mean-reversion bar-signals
    (reusing their exact entry/stop rules) rather than re-deriving them.
    In the 0.45-0.55 band the spec says "size x0.5" without saying which
    sub-signal supplies direction; both are evaluated and whichever fires
    is taken (OFI preferred on a tie) — an explicit, documented choice
    where the spec is silent on tie-breaking, not invented math.
    """
    p = np.asarray(prices, dtype=float)
    fee = _fee(cost_model)

    ofi_params = DEFAULT_STRATEGY_PARAMS["OFI_MOMENTUM"]
    ou_params = DEFAULT_STRATEGY_PARAMS["OU_MEAN_REVERSION"]

    ofi_z = _ofi_z_series(np.asarray(volumes, dtype=float), sides, ofi_params["ofi_agg_window"], ofi_params["ofi_z_window"])
    atr = _atr_series(p, ofi_params["atr_period"])
    avg_sec = _avg_seconds_per_bar(times)
    ou_z, half_life_hours = _ou_z_and_halflife_series(p, ou_params["ou_window"], avg_sec)

    log_rets = np.diff(np.log(p + 1e-10))
    hurst_window = params["hurst_window"]
    n_chunks = params["hurst_n_chunks"]
    H = np.full(len(p), 0.5)
    for i in range(hurst_window + 1, len(p)):
        chunk = log_rets[i - 1 - hurst_window:i - 1]
        H[i] = compute_hurst_exponent(chunk.tolist(), n_chunks=n_chunks)["H"]

    conf = regime_confidence * np.abs(H - 0.5) * 2
    trending = regime in ("BULL", "BEAR")
    ranging = regime == "RANGE"
    warmup = max(ofi_params["ofi_z_window"], ou_params["ou_window"], hurst_window) + 1

    def _ofi_signal(i):
        if not trending:
            return None
        if ofi_z[i] > ofi_params["entry_z"]:
            direction = 1
        elif ofi_z[i] < -ofi_params["entry_z"]:
            direction = -1
        else:
            return None
        a = atr[i] or (p[i] * 0.01)
        return {"direction": direction, "stop": p[i] - direction * ofi_params["sl_atr_mult"] * a}

    def _ou_signal(i):
        if not ranging:
            return None
        hl = half_life_hours[i]
        if np.isnan(hl) or not (ou_params["min_half_life_hours"] < hl < ou_params["max_half_life_hours"]):
            return None
        if ou_z[i] < -ou_params["entry_z"]:
            direction = 1
        elif ou_z[i] > ou_params["entry_z"]:
            direction = -1
        else:
            return None
        a = atr[i] or (p[i] * 0.01)
        return {"direction": direction, "stop": p[i] - direction * ou_params["protective_atr_mult"] * a}

    def entry_fn(i):
        if conf[i] <= params["min_confidence"]:
            return None
        h = H[i]
        if h > params["trend_h"]:
            sig = _ofi_signal(i)
            size_mult = 1.0
        elif h < params["range_h"]:
            sig = _ou_signal(i)
            size_mult = 1.0
        else:
            sig = _ofi_signal(i) or _ou_signal(i)
            size_mult = params["mid_band_size_mult"]
        if not sig:
            return None
        sig["size"] = params["base_size"] * size_mult
        return sig

    def update_stop_fn(position, i, price):
        return None

    def should_exit_fn(position, i, price):
        return (position["direction"] == 1 and ofi_z[i] < 0 and ou_z[i] > -0.5) or \
               (position["direction"] == -1 and ofi_z[i] > 0 and ou_z[i] < 0.5)

    result = _run_price_engine(p, fee, warmup, entry_fn, update_stop_fn, should_exit_fn)
    result["diagnostics"] = {
        "engine": "HURST_ADAPTIVE_META",
        "last_H": round(float(H[-1]), 4),
        "last_confidence": round(float(conf[-1]), 4),
    }
    return result


# ─── Public dispatch ────────────────────────────────────────────────────────

def simulate_fold(
    strategy_key: str,
    prices: list[float],
    times,
    volumes: list[float],
    sides: list[str],
    regime: str,
    regime_confidence: float,
    cost_model: str,
    params: dict | None = None,
    hedge_prices: list[float] | None = None,
) -> dict:
    """
    Dispatch to one of the five fixed proprietary strategy simulators.
    Returns {"returns", "n_trades", "win_rate", "total_return_pct",
    "max_dd", "diagnostics"} — same shape regardless of which strategy ran.
    """
    if strategy_key not in STRATEGY_KEYS:
        raise ValueError(f"Unknown strategy_key: {strategy_key!r}")

    p = DEFAULT_STRATEGY_PARAMS[strategy_key].copy()
    p.update(params or {})

    if len(prices) < 15:
        return _empty_result("insufficient_data", strategy_key)

    if strategy_key == "OFI_MOMENTUM":
        return _sim_ofi_momentum(prices, volumes, sides, regime, cost_model, p)
    if strategy_key == "OU_MEAN_REVERSION":
        return _sim_ou_mean_reversion(prices, times, regime, cost_model, p)
    if strategy_key == "GARCH_BREAKOUT":
        return _sim_garch_breakout(prices, regime, cost_model, p)
    if strategy_key == "BTC_NEUTRAL_MEAN_REVERSION":
        return _sim_btc_neutral(prices, hedge_prices, cost_model, p)
    if strategy_key == "HURST_ADAPTIVE_META":
        return _sim_hurst_adaptive(prices, times, volumes, sides, regime, regime_confidence, cost_model, p)

    raise AssertionError("unreachable")
