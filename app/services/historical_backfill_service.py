# app/services/historical_backfill_service.py
"""
Historical Data Backfill (Guide Part III / Ch.12 — "historical data depth
for research")
=============================================================================
This dev environment only ever accumulates whatever the live ingestion
worker (market_ingestion_worker.py) has captured this session — a few
hours of real ticks. None of OANDA, Alpaca, or ccxt expose *historical
ticks* — every vendor's backfill API returns OHLC candles, never a raw
trade tape further back than a live subscription — so closing this gap
forces a choice: teach the backtest engine to replay candles, or
synthesize ticks from candles and let the existing tick-level engine run
unchanged.

We do the latter. Each candle becomes 4 ticks (open → high/low in printed
order → close, per bar direction) at evenly-spaced sub-timestamps inside
the bar, written to market_ticks with candle_ref set to the bar's own
timestamp — an existing column on MarketTick that was defined but never
populated anywhere in the codebase before this. Real streamed ticks never
set candle_ref, so `candle_ref IS NOT NULL` cleanly separates the two
populations going forward without a schema change.

This is real, honestly-labeled OHLC data, not fabricated prices — but it
is NOT the guide's tick-level order-book replay. A backtest run over a
backfilled range is directional/statistical validation over real historical
price action, not an execution-cost-accurate simulation of that period
(no real bid/ask spread or intra-bar order-book state survives the
candle → tick synthesis). Callers should treat it accordingly.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import aiohttp
from sqlalchemy import select

from app.core.config import settings
from app.db.sync_session import SyncSession
from app.models.all_models import MarketTick, Symbol
from app.services.market_data_service import (
    CCXT_AVAILABLE,
    DEFAULT_CRYPTO_EXCHANGE,
    _alpaca_crypto_candidates,
    _alpaca_headers,
    _get_exchange,
    _has_alpaca_credentials,
    _has_oanda_credentials,
    _oanda_base_url,
    _oanda_headers,
    _oanda_instrument,
    _is_equity_symbol,
    _is_forex_symbol,
    _yf_download,
)

log = logging.getLogger(__name__)

# Hard ceiling per backfill call — a runaway range/timeframe combination
# (e.g. 1m over 180 days) stops here instead of hanging a worker for hours.
MAX_CANDLES_PER_BACKFILL = 50_000

_OANDA_GRANULARITY = {"1m": "M1", "5m": "M5", "15m": "M15", "1h": "H1", "4h": "H4", "1d": "D"}
_ALPACA_TIMEFRAMES = {"1m": "1Min", "5m": "5Min", "15m": "15Min", "1h": "1Hour", "4h": "4Hour", "1d": "1Day"}
_YF_INTERVALS = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "60m", "4h": "1h", "1d": "1d"}
TIMEFRAME_SECS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}

_ALPACA_DATA_BASE = "https://data.alpaca.markets"
_ALPACA_CRYPTO_BASE = f"{_ALPACA_DATA_BASE}/v1beta3/crypto/us"


# ── Provider-specific paginated candle fetchers ──────────────────────────────
# Every fetcher returns the same shape: [{"time": tz-aware datetime, "open",
# "high", "low", "close", "volume": float}, ...] ascending by time.

async def _oanda_range(symbol: str, timeframe: str, start: datetime, end: datetime) -> list[dict]:
    if not _has_oanda_credentials():
        return []
    granularity = _OANDA_GRANULARITY.get(timeframe, "M1")
    step = timedelta(seconds=TIMEFRAME_SECS.get(timeframe, 60))
    instrument = _oanda_instrument(symbol)
    url = f"{_oanda_base_url()}/instruments/{instrument}/candles"
    out: list[dict] = []
    cursor = start
    async with aiohttp.ClientSession() as session:
        while cursor < end and len(out) < MAX_CANDLES_PER_BACKFILL:
            params = {
                "granularity": granularity,
                "price": "M",
                "from": cursor.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "count": 5000,
            }
            try:
                async with session.get(url, headers=_oanda_headers(), params=params, timeout=20) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            except Exception as e:  # noqa: BLE001
                log.warning(f"OANDA backfill {symbol}: {e}")
                break
            candles = [c for c in data.get("candles", []) if c.get("complete") is True]
            if not candles:
                break
            advanced = False
            for c in candles:
                ts = datetime.fromisoformat(c["time"].replace("Z", "+00:00"))
                if ts < cursor or ts >= end:
                    continue
                out.append({
                    "time": ts, "open": float(c["mid"]["o"]), "high": float(c["mid"]["h"]),
                    "low": float(c["mid"]["l"]), "close": float(c["mid"]["c"]),
                    "volume": float(c.get("volume", 0)),
                })
                advanced = True
            last_ts = datetime.fromisoformat(candles[-1]["time"].replace("Z", "+00:00"))
            if last_ts <= cursor:
                break
            cursor = last_ts + step
            if len(candles) < 5000 or not advanced:
                break
    return out


async def _alpaca_stock_range(symbol: str, timeframe: str, start: datetime, end: datetime) -> list[dict]:
    if not _has_alpaca_credentials():
        return []
    tf = _ALPACA_TIMEFRAMES.get(timeframe, "5Min")
    url = f"{_ALPACA_DATA_BASE}/v2/stocks/{symbol.upper()}/bars"
    out: list[dict] = []
    page_token = None
    async with aiohttp.ClientSession() as session:
        while len(out) < MAX_CANDLES_PER_BACKFILL:
            params = {
                "timeframe": tf, "start": start.isoformat(), "end": end.isoformat(),
                "limit": 10000, "feed": settings.ALPACA_DATA_FEED, "adjustment": "raw",
            }
            if page_token:
                params["page_token"] = page_token
            try:
                async with session.get(url, headers=_alpaca_headers(), params=params, timeout=20) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            except Exception as e:  # noqa: BLE001
                log.warning(f"Alpaca backfill {symbol}: {e}")
                break
            for b in data.get("bars") or []:
                ts = datetime.fromisoformat(b["t"].replace("Z", "+00:00"))
                out.append({
                    "time": ts, "open": float(b["o"]), "high": float(b["h"]),
                    "low": float(b["l"]), "close": float(b["c"]), "volume": float(b.get("v", 0)),
                })
            page_token = data.get("next_page_token")
            if not page_token:
                break
    return out


async def _alpaca_crypto_range(symbol: str, timeframe: str, start: datetime, end: datetime) -> list[dict]:
    if not _has_alpaca_credentials():
        return []
    tf = _ALPACA_TIMEFRAMES.get(timeframe, "5Min")
    for cand in _alpaca_crypto_candidates(symbol):
        url = f"{_ALPACA_CRYPTO_BASE}/bars"
        out: list[dict] = []
        page_token = None
        async with aiohttp.ClientSession() as session:
            while len(out) < MAX_CANDLES_PER_BACKFILL:
                params = {
                    "symbols": cand, "timeframe": tf,
                    "start": start.isoformat(), "end": end.isoformat(), "limit": 10000,
                }
                if page_token:
                    params["page_token"] = page_token
                try:
                    async with session.get(url, headers=_alpaca_headers(), params=params, timeout=20) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                except Exception as e:  # noqa: BLE001
                    log.debug(f"Alpaca crypto backfill {cand}: {e}")
                    break
                bars = (data.get("bars") or {}).get(cand) or []
                for b in bars:
                    ts = datetime.fromisoformat(b["t"].replace("Z", "+00:00"))
                    out.append({
                        "time": ts, "open": float(b["o"]), "high": float(b["h"]),
                        "low": float(b["l"]), "close": float(b["c"]), "volume": float(b.get("v", 0)),
                    })
                page_token = data.get("next_page_token")
                if not page_token:
                    break
        if out:
            return out
    return []


async def _ccxt_range(symbol: str, timeframe: str, start: datetime, end: datetime, exchange_id: str) -> list[dict]:
    if not CCXT_AVAILABLE:
        return []
    ex = _get_exchange(exchange_id)
    if not ex:
        return []
    since_ms = int(start.timestamp() * 1000)
    until_ms = int(end.timestamp() * 1000)
    out: list[dict] = []
    try:
        while since_ms < until_ms and len(out) < MAX_CANDLES_PER_BACKFILL:
            raw = await asyncio.wait_for(
                ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=1000), timeout=15.0,
            )
            if not raw:
                break
            for c in raw:
                if c[4] is None or c[0] >= until_ms:
                    continue
                out.append({
                    "time": datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc),
                    "open": float(c[1]), "high": float(c[2]), "low": float(c[3]),
                    "close": float(c[4]), "volume": float(c[5]),
                })
            last_ms = raw[-1][0]
            if last_ms <= since_ms:
                break
            since_ms = last_ms + 1
            if len(raw) < 1000:
                break
    except Exception as e:  # noqa: BLE001
        log.warning(f"ccxt backfill {exchange_id}/{symbol}: {e}")
    return out


async def _yf_range(symbol: str, timeframe: str, start: datetime, end: datetime) -> list[dict]:
    df = await _yf_download(
        symbol, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"),
        interval=_YF_INTERVALS.get(timeframe, "1d"), progress=False, auto_adjust=True,
    )
    if df is None or df.empty:
        return []
    out = []
    for idx, row in df.iterrows():
        ts = idx.to_pydatetime()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        out.append({
            "time": ts, "open": float(row["Open"]), "high": float(row["High"]),
            "low": float(row["Low"]), "close": float(row["Close"]), "volume": float(row["Volume"]),
        })
    return out


async def _fetch_range(
    symbol: str, timeframe: str, start: datetime, end: datetime, exchange_id: str | None,
) -> tuple[str, list[dict]]:
    if _is_forex_symbol(symbol):
        return "oanda", await _oanda_range(symbol, timeframe, start, end)

    if _is_equity_symbol(symbol):
        candles = await _alpaca_stock_range(symbol, timeframe, start, end)
        if candles:
            return "alpaca", candles
        return "yfinance", await _yf_range(symbol, timeframe, start, end)

    if "/" in symbol and CCXT_AVAILABLE:
        ex_id = exchange_id or DEFAULT_CRYPTO_EXCHANGE
        candles = await _ccxt_range(symbol, timeframe, start, end, ex_id)
        if candles:
            return ex_id, candles
        for fallback in ("kraken", "okx", "bybit"):
            if fallback == ex_id:
                continue
            candles = await _ccxt_range(symbol, timeframe, start, end, fallback)
            if candles:
                return fallback, candles

    if "/" in symbol and _has_alpaca_credentials():
        candles = await _alpaca_crypto_range(symbol, timeframe, start, end)
        if candles:
            return "alpaca_crypto", candles

    return "none", []


# ── Candle → tick synthesis ──────────────────────────────────────────────────

def _synthesize_ticks(candle: dict, symbol_id: int, provider: str, step_secs: int) -> list[MarketTick]:
    t0 = candle["time"]
    o, h, l, c = candle["open"], candle["high"], candle["low"], candle["close"]
    v = candle.get("volume") or 0.0
    # Convention, not reconstruction: an up-bar (close >= open) is assumed to
    # have printed its high before its low, and vice versa for a down-bar —
    # a candle alone cannot reveal true intra-bar order, and this is a
    # standard OHLC-to-tick approximation, never claimed to be the real
    # printed sequence.
    seq = [o, h, l, c] if c >= o else [o, l, h, c]
    n = len(seq)
    per_tick_vol = v / n if n else 0.0
    return [
        MarketTick(
            time=t0 + timedelta(seconds=step_secs * i / n),
            symbol_id=symbol_id,
            price=price,
            volume=per_tick_vol,
            side=None,
            exchange=f"backfill:{provider}",
            quality_score=100,
            flags=None,
            dq_result="PASS",
            meta=["historical_backfill"],
            candle_ref=t0,
        )
        for i, price in enumerate(seq)
    ]


def backfill_symbol(
    symbol: str, days: int = 30, timeframe: str = "5m", exchange_id: str | None = None,
) -> dict:
    """
    Sync entrypoint — safe to call from a Celery task or an API request
    handler. Fetches `days` of history at `timeframe` granularity from
    whichever real vendor serves this symbol (OANDA/Alpaca/ccxt/yfinance,
    same routing `get_ohlcv` uses), synthesizes ticks, and writes them to
    market_ticks — skipping any candle already backfilled for this symbol
    so re-running is idempotent.
    """
    with SyncSession() as db:
        sym_row = db.execute(select(Symbol).where(Symbol.symbol == symbol)).scalar_one_or_none()
        if not sym_row:
            return {"symbol": symbol, "error": "symbol_not_found", "ticks_inserted": 0}

        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        step_secs = TIMEFRAME_SECS.get(timeframe, 60)

        provider, candles = asyncio.run(_fetch_range(symbol, timeframe, start, end, exchange_id))
        if not candles:
            return {
                "symbol": symbol, "provider": provider, "timeframe": timeframe,
                "candles_fetched": 0, "ticks_inserted": 0,
            }

        already = set(
            db.execute(
                select(MarketTick.candle_ref).where(
                    MarketTick.symbol_id == sym_row.id,
                    MarketTick.candle_ref.isnot(None),
                    MarketTick.candle_ref >= start,
                    MarketTick.candle_ref <= end,
                )
            ).scalars().all()
        )

        rows: list[MarketTick] = []
        for candle in candles:
            if candle["time"] in already:
                continue
            rows.extend(_synthesize_ticks(candle, sym_row.id, provider, step_secs))

        for i in range(0, len(rows), 2000):
            db.bulk_save_objects(rows[i:i + 2000])
            db.commit()

        return {
            "symbol": symbol, "provider": provider, "timeframe": timeframe,
            "range_start": start.isoformat(), "range_end": end.isoformat(),
            "candles_fetched": len(candles), "candles_new": len(candles) - len(already),
            "ticks_inserted": len(rows),
        }
