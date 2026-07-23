"""
ML feature compute service — pure computation layer.

Contract:
  • Signature: (current_user, db, *, category=None) — no FastAPI Depends/Query.
  • category filtering is an explicit keyword param applied after feature
    computation (all features are always computed first so top_features and
    categories reflect the full unfiltered set).
  • Never raises; always returns a serialisable dict.
  • All DB calls are async.
  • Live ticker fetch is non-fatal; degraded result is returned on failure.
"""

from __future__ import annotations

import statistics
from datetime import datetime, timezone
from typing import Any

from app.db.session import AsyncSession
from app.models.all_models import User
from app.services.market_data_service import get_live_ticker
from app.helpers.helpers import (
    latest_regime,
    open_positions,
    primary_symbol,
    recent_ticks,
)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        f = float(value)
        return default if (f != f) else f   # NaN guard
    except (TypeError, ValueError):
        return default


# ── Feature versioning (Guide Ch.9) ───────────────────────────────────────────
# Every feature this service computes is tagged with its own version string,
# bumped only when that specific feature's calculation actually changes
# (formula, window length, normalization). A consumer that cached or trained
# against an older version can compare tags and know it's looking at a
# differently-computed value, rather than silently trusting a number that
# looks the same shape but isn't the same calculation — the guide's stated
# purpose for feature versioning.
#
# Honest scope note: this is the versioning *mechanism*, not full online/
# offline parity. There is still no separate offline/training pipeline
# anywhere in this codebase that reads features by version for model
# training — every consumer today is this same live path (the intelligence
# worker's cache, and this endpoint). Versioning something with only one
# real consumer has a real purpose (a *future* consumer knows immediately
# when a feature it depends on has changed shape), but it does not by
# itself close the "online/offline parity" gap the guide describes.
FEATURE_SET_VERSION = "v1"
FEATURE_VERSIONS: dict[str, str] = {
    "Order Flow Imbalance":   "v1",
    "Buy/Sell Volume Ratio":  "v1",
    "Volume-Weighted Price":  "v1",
    "Tick Momentum 5":        "v1",
    "Tick Momentum 20":       "v1",
    "Rolling Volatility 20":  "v1",
    "Rolling Volatility 60":  "v1",
    "Regime Confidence":      "v1",
    "Regime Label Encoding":  "v1",
    "Open Positions Count":   "v1",
    "Total Exposure USD":     "v1",
    "Net Unrealized PnL":     "v1",
}


# ─────────────────────────────────────────────────────────────────────────────
# compute_features
# ─────────────────────────────────────────────────────────────────────────────

async def compute_features(
    current_user: User,
    db: AsyncSession,
    *,
    category: str | None = None,
) -> dict:
    """
    Derives ML features from live market data.

    Feature groups
    --------------
    OFI        — order-flow imbalance, volume ratio, VWAP, tick momentum
    Volatility — rolling coefficient-of-variation at 20 and 60 ticks
    Regime     — HMM confidence and label encoding
    Portfolio  — open position count, exposure, unrealised P&L

    Parameters
    ----------
    category:
        When supplied, the returned ``features`` list is filtered to that
        category (case-insensitive).  ``top_features`` and ``categories``
        always reflect the *full* unfiltered set so callers see the complete
        picture regardless of the filter.
    """
    try:
        now_str = _now_iso()
        fid     = 0          # mutable counter — incremented inside _feat()

        # ── Feature factory ───────────────────────────────────────────────────
        def _feat(
            name: str,
            cat: str,
            value: float,
            unit: str,
            importance: float,
            drift_pct: float = 0.0,
        ) -> dict:
            nonlocal fid
            fid += 1
            return {
                "id":              f"F{fid:03d}",
                "name":            name,
                "category":        cat,
                "value":           round(_safe_float(value), 6),
                "unit":            unit,
                "importance":      round(importance, 4),
                "drift_pct":       round(drift_pct, 2),
                "updated_at":      now_str,
                "feature_version": FEATURE_VERSIONS.get(name, "v1"),
            }

        # ── DB data — non-fatal individually ─────────────────────────────────
        primary   = await primary_symbol(db)
        ticks     = await recent_ticks(db, primary.id, 200) if primary else []
        regime    = await latest_regime(db, primary.id)     if primary else None
        positions = await open_positions(db, current_user.id)

        all_features: list[dict] = []

        # ── OFI + volatility features (require ticks) ─────────────────────────
        if ticks:
            prices  = [_safe_float(t.price)  for t in ticks]
            volumes = [_safe_float(t.volume) for t in ticks]

            buy_vol  = sum(
                _safe_float(t.volume) for t in ticks
                if (t.side or "").upper() in ("BUY", "BID", "B")
            )
            sell_vol = sum(
                _safe_float(t.volume) for t in ticks
                if (t.side or "").upper() in ("SELL", "ASK", "S")
            )
            total_vol = buy_vol + sell_vol or 1.0

            # Volume-weighted price (VWAP over window)
            vol_sum   = sum(volumes) or 1.0
            vwap      = sum(p * v for p, v in zip(prices, volumes)) / vol_sum

            # OFI drift: deviation of buy-fraction from the neutral 0.5 baseline
            buy_frac  = buy_vol / total_vol
            ofi_drift = (buy_frac - 0.5) * 20   # ±10 range at extremes

            all_features.extend([
                _feat(
                    "Order Flow Imbalance", "OFI",
                    (buy_vol - sell_vol) / total_vol,
                    "ratio", 0.82, ofi_drift,
                ),
                _feat(
                    "Buy/Sell Volume Ratio", "OFI",
                    buy_vol / max(sell_vol, 1e-9),
                    "ratio", 0.74,
                ),
                _feat(
                    "Volume-Weighted Price", "OFI",
                    vwap,
                    "USD", 0.61,
                ),
                _feat(
                    "Tick Momentum 5", "OFI",
                    (prices[-1] - prices[-6]) / prices[-6] * 100
                    if len(prices) >= 6 and prices[-6] != 0 else 0.0,
                    "%", 0.55,
                ),
                _feat(
                    "Tick Momentum 20", "OFI",
                    (prices[-1] - prices[-21]) / prices[-21] * 100
                    if len(prices) >= 21 and prices[-21] != 0 else 0.0,
                    "%", 0.49,
                ),
            ])

            # Volatility — coefficient of variation (std / mean × 100)
            if len(prices) >= 20:
                window20 = prices[-20:]
                mean20   = statistics.mean(window20)
                if mean20 != 0:
                    all_features.append(_feat(
                        "Rolling Volatility 20", "Volatility",
                        statistics.stdev(window20) / mean20 * 100,
                        "%", 0.78,
                    ))

            if len(prices) >= 60:
                window60 = prices[-60:]
                mean60   = statistics.mean(window60)
                if mean60 != 0:
                    all_features.append(_feat(
                        "Rolling Volatility 60", "Volatility",
                        statistics.stdev(window60) / mean60 * 100,
                        "%", 0.65,
                    ))

        # ── Regime features ───────────────────────────────────────────────────
        if regime:
            conf = _safe_float(regime.confidence)
            _LABEL_ENC = {
                "BULL": 1.0, "RECOVERY": 0.5, "RANGE": 0.0,
                "BEAR": -1.0, "CRISIS": -2.0,
            }
            all_features.extend([
                _feat("Regime Confidence",    "Regime", conf * 100, "%",   0.91),
                _feat("Regime Label Encoding","Regime",
                      _LABEL_ENC.get(regime.regime_label, 0.0), "enum", 0.88),
            ])

        # ── Portfolio features ────────────────────────────────────────────────
        if positions:
            total_exposure = sum(
                _safe_float(p.qty) * _safe_float(p.avg_cost) for p in positions
            )
            unrealised = sum(_safe_float(p.unrealized_pnl) for p in positions)
            all_features.extend([
                _feat("Open Positions Count", "Portfolio", float(len(positions)), "count", 0.42),
                _feat("Total Exposure USD",   "Portfolio", total_exposure,         "USD",   0.58),
                _feat("Net Unrealized PnL",   "Portfolio", unrealised,             "USD",   0.63),
            ])

        # ── Precompute full-set derived values before filtering ───────────────
        top_features = sorted(all_features, key=lambda x: -x["importance"])[:5]
        all_categories = sorted({f["category"] for f in all_features})

        # ── Category filter (applied last, to returned features only) ─────────
        filtered = (
            [f for f in all_features if f["category"].lower() == category.lower()]
            if category
            else all_features
        )

        # ── Live ticker enrichment (non-fatal) ────────────────────────────────
        live_price: float | None = None
        live_bias:  str   | None = None
        if primary:
            try:
                ticker    = await get_live_ticker(primary.symbol)
                live_price = ticker.get("last")
                chg        = _safe_float(ticker.get("change_pct_24h"), 0.0)
                live_bias  = "BULLISH" if chg > 1 else "BEARISH" if chg < -1 else "NEUTRAL"
            except Exception:  # noqa: BLE001
                pass

        return {
            "features":            filtered,
            "count":               len(filtered),
            "primary_symbol":      primary.symbol if primary else None,
            "live_price":          live_price,
            "market_bias":         live_bias,
            "categories":          all_categories,           # always full, unfiltered
            "top_features":        top_features,             # always full, unfiltered
            "feature_set_version": FEATURE_SET_VERSION,
            "category_filter": category,
            "fetched_at":     _now_iso(),
        }

    except Exception as exc:  # noqa: BLE001
        return {
            "error":               str(exc),
            "features":            [],
            "count":               0,
            "primary_symbol":      None,
            "live_price":          None,
            "market_bias":         None,
            "categories":          [],
            "top_features":        [],
            "feature_set_version": FEATURE_SET_VERSION,
            "category_filter": category,
            "fetched_at":     _now_iso(),
        }