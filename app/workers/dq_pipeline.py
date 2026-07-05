# app/workers/dq_pipeline.py
"""
Lightweight tick-by-tick Data Quality pipeline.

Design:
  - One shared singleton (_dq) used by both Redis and Kafka consumers.
  - No DB calls — purely in-memory rolling windows.
  - Fast: O(1) per tick after window warm-up.
  - Full LOF scan runs separately in regime_scan_task (hourly).

Returns (dq_result, flags):
  PASS   — tick is clean, write to candle aggregator
  FLAG   — tick is suspicious, write to candle + emit DQEvent
  REJECT — tick is bad, emit DQEvent only, skip candle
"""
from __future__ import annotations

import numpy as np

from app.core.config import settings


class DQPipeline:

    def __init__(self):
        self._price_window: dict[int, list[float]] = {}
        self._vol_window:   dict[int, list[float]] = {}
        self._seen:         dict[int, set]         = {}

    def check(self, tick: dict) -> tuple[str, list[str]]:
        sym_id = tick.get("symbol_id")
        price  = float(tick.get("price", 0))
        volume = float(tick.get("volume", 0))
        ts     = tick.get("time", "")

        # ── Hard rejects ─────────────────────────────────────
        if price <= 0:
            return "REJECT", ["ZERO_PRICE"]
        if volume < 0:
            return "REJECT", ["NEGATIVE_VOLUME"]

        # ── Duplicate detection ───────────────────────────────
        seen = self._seen.setdefault(sym_id, set())
        key  = (round(price, 8), round(volume, 8), ts)
        if key in seen:
            return "REJECT", ["DUPLICATE"]
        seen.add(key)
        if len(seen) > 500:                           # bound memory
            for old in list(seen)[:100]:
                seen.discard(old)

        # ── Price spike detection ─────────────────────────────
        flags: list[str] = []
        pw = self._price_window.setdefault(sym_id, [])
        if len(pw) >= 5:
            window      = pw[-settings.DQ_PRICE_WINDOW:]
            mean        = float(np.mean(window))
            spike_pct   = abs(price - mean) / mean if mean else 0.0
            if spike_pct > settings.DQ_SPIKE_THRESHOLD:
                flags.append(f"SPIKE_{spike_pct:.2%}")
                if spike_pct > settings.DQ_SPIKE_THRESHOLD * 3:
                    pw.append(price)
                    return "REJECT", flags

        pw.append(price)
        if len(pw) > 200:
            self._price_window[sym_id] = pw[-200:]

        # ── Volume outlier detection ──────────────────────────
        vw = self._vol_window.setdefault(sym_id, [])
        if len(vw) >= 10:
            avg = float(np.mean(vw[-50:]))
            if avg > 0 and volume > avg * settings.DQ_VOLUME_MAX_FACTOR:
                flags.append(f"VOL_OUTLIER_{volume / avg:.0f}x")

        vw.append(volume)
        if len(vw) > 200:
            self._vol_window[sym_id] = vw[-200:]

        return ("FLAG" if flags else "PASS"), flags


# Module-level singleton — imported directly by consumers
dq = DQPipeline()