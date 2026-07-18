"""
PiOS Live Market Data Service
==============================
Real-time market data from globally-available exchanges (no Binance):
  - KuCoin    — crypto, global, high volume
  - OKX       — crypto, global
  - Kraken    — crypto, EU/US/global
  - Bybit     — crypto, global
  - Alpaca    — US stocks (primary) + crypto (fallback venue)
  - OANDA     — forex + metals (60+ pairs)
  - yfinance  — indices; stocks/ETFs fallback when Alpaca unavailable

Domain routing: forex/metals pairs → OANDA · plain US tickers → Alpaca
(yfinance fallback) · crypto pairs → ccxt venues, Alpaca last-resort ·
^indices → yfinance.

Provides:
  • get_live_ticker(symbol, exchanges)  — best-bid/ask + price across venues
  • get_ohlcv(symbol, timeframe, limit) — candlestick data
  • get_orderbook(symbol, exchange)     — L2 depth
  • get_live_trades(symbol, exchange)   — recent trade tape
  • get_multi_asset_snapshot()          — cross-asset dashboard snapshot
  • get_technical_indicators(prices)   — RSI, MACD, ATR, BB, Stoch, EMA stack
  • get_market_breadth()               — advancing/declining, new highs/lows
  • WebSocket feed manager for SSE streaming
"""
from __future__ import annotations

import aiohttp
import asyncio
import logging
import math
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import numpy as np

from app.core.config import settings

log = logging.getLogger(__name__)

# ── Optional heavy imports ────────────────────────────────────────────────────
try:
    import ccxt.async_support as ccxt_async
    import ccxt
    CCXT_AVAILABLE = True
except ImportError:
    CCXT_AVAILABLE = False
    log.warning("ccxt not installed")

try:
    import yfinance as yf
    YF_AVAILABLE = True
    # yfinance logs every per-ticker failure at ERROR ("Failed to get ticker
    # 'AAPL' ... JSONDecodeError", "1 Failed download:") — when Yahoo is
    # rate-limiting (429) that's 7+ lines per Markets-tab load. The circuit
    # breaker below is the real signal; the per-ticker spam is noise.
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)
except ImportError:
    YF_AVAILABLE = False

# ── yfinance circuit breaker + thread offload ────────────────────────────────
# Yahoo aggressively 429s unauthenticated clients; yf.download() then returns
# an empty frame after burning seconds of retries — and it's a *blocking* call,
# so invoking it inline from these async endpoints stalls the event loop for
# every concurrent request. _yf_download() runs it in a worker thread and,
# after _YF_TRIP_AFTER consecutive failures, stops calling Yahoo entirely for
# _YF_COOLDOWN_S so a rate-limited environment degrades to fast, quiet
# "equities unavailable" responses instead of slow, log-spamming ones.
_YF_TRIP_AFTER = 3
_YF_COOLDOWN_S = 600.0
_yf_consecutive_failures = 0
_yf_blocked_until = 0.0


def _yf_ready() -> bool:
    return YF_AVAILABLE and time.time() >= _yf_blocked_until


async def _yf_download(*args, **kwargs):
    """yf.download in a thread, feeding the circuit breaker. Returns the
    DataFrame (possibly empty) or None when yfinance is tripped/unavailable."""
    global _yf_consecutive_failures, _yf_blocked_until
    if not _yf_ready():
        return None
    try:
        df = await asyncio.to_thread(yf.download, *args, **kwargs)
        failed = df is None or df.empty
    except Exception as e:  # noqa: BLE001
        log.debug(f"yfinance download {args}: {e}")
        failed = True
        df = None
    if failed:
        _yf_consecutive_failures += 1
        # Only trip+log once per outage window — downloads already in flight
        # when the breaker trips also land here and shouldn't re-log.
        if _yf_consecutive_failures >= _YF_TRIP_AFTER and time.time() >= _yf_blocked_until:
            _yf_blocked_until = time.time() + _YF_COOLDOWN_S
            log.warning(
                "yfinance: %d consecutive failed downloads (Yahoo rate limit?) — "
                "pausing equity data for %.0f min",
                _yf_consecutive_failures, _YF_COOLDOWN_S / 60,
            )
        return None
    _yf_consecutive_failures = 0
    return df


# ── Tiny in-process TTL cache ────────────────────────────────────────────────
# get_market_breadth / get_multi_asset_snapshot fan out to several external
# providers per call; every Markets-tab visitor re-triggering that is wasted
# quota (and what got this environment 429'd by Yahoo in the first place).
_ttl_cache: dict[str, tuple[float, Any]] = {}


def _ttl_get(key: str, ttl_s: float):
    hit = _ttl_cache.get(key)
    if hit and time.time() - hit[0] < ttl_s:
        return hit[1]
    return None


def _ttl_set(key: str, value: Any) -> None:
    _ttl_cache[key] = (time.time(), value)

try:
    import ta
    TA_AVAILABLE = True
except ImportError:
    TA_AVAILABLE = False
    log.warning("ta not installed — install with: pip install ta")

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# Exchange configuration — globally accessible (no Binance)
# ─────────────────────────────────────────────────────────────────────────────

CRYPTO_EXCHANGES = {
    "kucoin":   {"class": "kucoin",   "name": "KuCoin",   "region": "global"},
    "okx":      {"class": "okx",      "name": "OKX",      "region": "global"},
    "kraken":   {"class": "kraken",   "name": "Kraken",   "region": "global"},
    "bybit":    {"class": "bybit",    "name": "Bybit",    "region": "global"},
    "gate":     {"class": "gate",     "name": "Gate.io",  "region": "global"},
    "coinbase": {"class": "coinbase", "name": "Coinbase", "region": "us_global"},
}

# Default symbol routing per asset class
DEFAULT_CRYPTO_EXCHANGE  = os.getenv("DEFAULT_CRYPTO_EXCHANGE",  "kraken")
DEFAULT_FOREX_EXCHANGE   = os.getenv("DEFAULT_FOREX_EXCHANGE",   "kraken")
DEFAULT_STOCKS_EXCHANGE  = os.getenv("DEFAULT_STOCKS_EXCHANGE",  "alpaca")

# Core watchlist — globally liquid instruments
CORE_CRYPTO_SYMBOLS  = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT", "BNB/USDT", "AVAX/USDT"]
CORE_FOREX_PAIRS     = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF"]
CORE_STOCK_TICKERS   = ["AAPL", "MSFT", "NVDA", "TSLA", "SPY", "QQQ", "GLD", "USO"]
CORE_INDICES         = ["^GSPC", "^DJI", "^IXIC", "^VIX", "^TNX"]


# ─────────────────────────────────────────────────────────────────────────────
# Exchange pool — singleton async exchange instances
# ─────────────────────────────────────────────────────────────────────────────

_exchange_pool: dict[str, Any] = {}


def _get_exchange(exchange_id: str) -> Any:
    """Get or create a CCXT async exchange instance."""
    if not CCXT_AVAILABLE:
        return None
    if exchange_id not in _exchange_pool:
        exchange_class = getattr(ccxt_async, exchange_id, None)
        if not exchange_class:
            return None
        _exchange_pool[exchange_id] = exchange_class({
            "enableRateLimit": True,
            "rateLimit": 100,
            "options": {"defaultType": "spot"},
        })
    return _exchange_pool[exchange_id]


_OANDA_PRACTICE_BASE = "https://api-fxpractice.oanda.com/v3"
_OANDA_LIVE_BASE = "https://api-fxtrade.oanda.com/v3"


def _oanda_base_url() -> str:
    return _OANDA_LIVE_BASE if settings.OANDA_ENVIRONMENT == "live" else _OANDA_PRACTICE_BASE


def _has_oanda_credentials() -> bool:
    return bool(settings.OANDA_API_KEY and settings.OANDA_ACCOUNT_ID)


def _oanda_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.OANDA_API_KEY}",
        "Accept-Datetime-Format": "RFC3339",
    }


# OANDA serves fiat pairs and metals-vs-fiat; everything else (any side being
# a crypto asset — BTC/USDT, ETH/USDC, …) must go to the ccxt exchanges. The
# old "any X/Y is forex" version of this routed ALL crypto pairs to OANDA,
# which (without credentials) made every ticker/orderbook/trades/OHLCV call
# for crypto return oanda_credentials_missing — no live price anywhere.
_FIAT_CCYS = {"USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD", "SGD", "HKD", "SEK", "NOK", "DKK", "PLN", "ZAR", "MXN", "TRY", "CNH"}
_METALS = {"XAU", "XAG", "XPT", "XPD"}


def _split_pair(symbol: str) -> tuple[str, str] | None:
    """
    (base, quote) for both symbol conventions in play: the frontend's
    slashed form ("EUR/USD", "XAU/USD") and the DB's slash-less form
    ("EURUSD", "XAUUSD" — see the symbols table). Returns None for
    anything that doesn't look like a two-sided pair.
    """
    if not isinstance(symbol, str):
        return None
    s = symbol.strip().upper()
    if "/" in s:
        parts = s.split("/")
        return (parts[0], parts[1]) if len(parts) == 2 and all(parts) else None
    if len(s) in (6, 7) and s[-3:] in _FIAT_CCYS:
        return s[:-3], s[-3:]
    return None


def _is_forex_symbol(symbol: str) -> bool:
    pair = _split_pair(symbol)
    if pair is None:
        return False
    base, quote = pair
    return quote in _FIAT_CCYS and (base in _FIAT_CCYS or base in _METALS)


def _oanda_instrument(symbol: str) -> str:
    # OANDA wants EUR_USD — handle both "EUR/USD" and the DB's "EURUSD".
    pair = _split_pair(symbol)
    return f"{pair[0]}_{pair[1]}" if pair else symbol.replace("/", "_")


async def _fetch_oanda_pricing(symbol: str) -> dict:
    instrument = _oanda_instrument(symbol)
    url = f"{_oanda_base_url()}/accounts/{settings.OANDA_ACCOUNT_ID}/pricing"
    params = {"instruments": instrument}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_oanda_headers(), params=params, timeout=15) as response:
            response.raise_for_status()
            return await response.json()


async def _fetch_oanda_orderbook(symbol: str) -> dict:
    instrument = _oanda_instrument(symbol)
    url = f"{_oanda_base_url()}/instruments/{instrument}/orderBook"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_oanda_headers(), timeout=15) as response:
            response.raise_for_status()
            return await response.json()


async def _fetch_oanda_transactions(symbol: str, limit: int = 50) -> list[dict]:
    instrument = _oanda_instrument(symbol)
    url = f"{_oanda_base_url()}/accounts/{settings.OANDA_ACCOUNT_ID}/transactions"
    params = {"type": "ORDER_FILL", "pageSize": limit}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_oanda_headers(), params=params, timeout=15) as response:
            response.raise_for_status()
            data = await response.json()

    trades = []
    for tx in data.get("transactions", []):
        if tx.get("instrument") != instrument:
            continue
        units = float(tx.get("units", 0))
        if units == 0:
            continue
        price = float(tx.get("price") or 0)
        trades.append({
            "id": str(tx.get("id", "")),
            "time": tx.get("time") or datetime.now(timezone.utc).isoformat(),
            "price": price,
            "amount": abs(units),
            "side": "BUY" if units > 0 else "SELL",
            "cost": abs(units) * price,
            "exchange": "oanda",
        })
    return trades


# ─────────────────────────────────────────────────────────────────────────────
# Alpaca market data — US equities (primary) + crypto (last-resort venue)
# ─────────────────────────────────────────────────────────────────────────────
# Domain routing mirrors OANDA's: forex/metals → OANDA, plain US tickers →
# Alpaca (yfinance demoted to fallback), crypto pairs → ccxt venues with
# Alpaca's crypto feed as the fallback when every ccxt venue fails. Talks to
# the data REST API directly (no SDK), like app/providers/stocks.py; paper
# keys get the free IEX feed (SIP 403s without a paid data subscription).

_ALPACA_DATA_BASE   = "https://data.alpaca.markets"
_ALPACA_CRYPTO_BASE = f"{_ALPACA_DATA_BASE}/v1beta3/crypto/us"

_ALPACA_TIMEFRAMES     = {"1m": "1Min", "5m": "5Min", "15m": "15Min", "1h": "1Hour", "4h": "4Hour", "1d": "1Day"}
_ALPACA_TIMEFRAME_MINS = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}


def _has_alpaca_credentials() -> bool:
    return bool(settings.ALPACA_API_KEY and settings.ALPACA_API_SECRET)


def _alpaca_headers() -> dict[str, str]:
    return {
        "APCA-API-KEY-ID":     settings.ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": settings.ALPACA_API_SECRET,
        "Accept":              "application/json",
    }


async def list_alpaca_crypto_symbols() -> list[dict]:
    """
    Every crypto currency Alpaca currently lists as tradable, one entry per
    *base* asset — Alpaca lists the same base against several quotes (USD,
    USDC, USDT: e.g. BTC/USD, BTC/USDC, BTC/USDT all exist), which would
    look like duplicate "currencies" if surfaced per-pair rather than
    deduped to the underlying asset.

    Symbols are returned in the app's established /USDT quote convention
    (matching the existing seeded BTC/USDT symbol) regardless of which
    quote Alpaca itself lists first — order submission and market-data
    fetches already translate that back to whatever Alpaca/ccxt actually
    wants (AlpacaAdapter._alpaca_symbol, _alpaca_crypto_candidates), so the
    quote shown here doesn't need to match Alpaca's own listing.

    Uses the Trading API's asset list (alpaca-py's TradingClient, same SDK
    broker_service.py already depends on) rather than the data REST calls
    the rest of this module uses — this is asset metadata, not price data.
    Cached for an hour: Alpaca's crypto listing changes rarely and this is
    a full-account API call, not a per-symbol one.
    """
    cached = _ttl_get("alpaca_crypto_symbols", 3600)
    if cached is not None:
        return cached
    if not _has_alpaca_credentials():
        return []
    try:
        from alpaca.trading.client import TradingClient
        from alpaca.trading.enums import AssetClass
        from alpaca.trading.requests import GetAssetsRequest

        client = TradingClient(settings.ALPACA_API_KEY, settings.ALPACA_API_SECRET, paper=settings.ALPACA_PAPER)
        req = GetAssetsRequest(asset_class=AssetClass.CRYPTO)
        assets = await asyncio.to_thread(client.get_all_assets, req)
    except Exception as e:  # noqa: BLE001
        log.debug(f"Alpaca crypto asset list: {e}")
        return []

    bases = sorted({
        a.symbol.split("/")[0] for a in assets
        if a.tradable and str(getattr(a.status, "value", a.status)).lower() == "active"
    })
    # USDT itself is listed as a base (USDT/USD at Alpaca) but the /USDT
    # quote convention would surface it as the degenerate USDT/USDT pair.
    result = [{"symbol": f"{b}/USDT", "base": b} for b in bases if b != "USDT"]
    _ttl_set("alpaca_crypto_symbols", result)
    return result


def _is_equity_symbol(symbol: str) -> bool:
    """
    Plain US tickers (AAPL, SPY, BRK.B). Excludes pairs (anything with a
    slash), the slash-less forex/metal forms (EURUSD, XAUUSD) and index
    symbols (^GSPC — those stay on yfinance; Alpaca has no indices).
    """
    if not isinstance(symbol, str):
        return False
    s = symbol.strip().upper()
    if not s or "/" in s or s.startswith("^") or _is_forex_symbol(s):
        return False
    return len(s) <= 6 and s.replace(".", "").replace("-", "").isalpha()


def _alpaca_crypto_candidates(symbol: str) -> list[str]:
    """Alpaca quotes crypto against USD; the app's convention is mostly
    /USDT. Try the symbol as-is first, then the /USD equivalent."""
    s = symbol.strip().upper()
    if "/" not in s:
        return []
    cands = [s]
    for alt in ("USDT", "USDC"):
        if s.endswith("/" + alt):
            cands.append(s[: -len(alt)] + "USD")
    return cands


async def _alpaca_get(url: str, params: dict | None = None) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_alpaca_headers(), params=params or {}, timeout=15) as response:
            response.raise_for_status()
            return await response.json()


def _alpaca_bar_params(timeframe: str, limit: int) -> dict:
    # Explicit start: without it Alpaca defaults to "today", which is empty
    # on weekends/holidays for stocks. 3× the nominal window (min 4 days)
    # covers closed market hours; sort=desc keeps the most recent `limit`.
    mins = _ALPACA_TIMEFRAME_MINS.get(timeframe, 60)
    lookback = max(timedelta(minutes=mins * limit * 3), timedelta(days=4))
    return {
        "timeframe": _ALPACA_TIMEFRAMES.get(timeframe, "1Hour"),
        "limit": limit,
        "start": (datetime.now(timezone.utc) - lookback).isoformat(),
        "sort": "desc",
    }


def _alpaca_bar_to_candle(b: dict) -> dict:
    ts = datetime.fromisoformat(b["t"].replace("Z", "+00:00"))
    return {
        "ts":     int(ts.timestamp() * 1000),
        "time":   b["t"],
        "open":   float(b["o"]),
        "high":   float(b["h"]),
        "low":    float(b["l"]),
        "close":  float(b["c"]),
        "volume": float(b.get("v", 0)),
    }


def _alpaca_snapshot_to_ticker(symbol: str, snap: dict) -> dict | None:
    """Stock and crypto snapshots share this shape (latestTrade/latestQuote/
    dailyBar/prevDailyBar) → the get_live_ticker payload."""
    quote = snap.get("latestQuote") or {}
    trade = snap.get("latestTrade") or {}
    day   = snap.get("dailyBar") or {}
    prev  = snap.get("prevDailyBar") or {}
    last = trade.get("p") or day.get("c")
    bid = float(quote.get("bp") or 0) or None
    ask = float(quote.get("ap") or 0) or None
    # On thin pairs (Alpaca's crypto USDT books) latestTrade can be hours
    # stale while the quote is live — trust the mid when they disagree.
    mid = (bid + ask) / 2 if bid and ask else None
    if mid and (not last or abs(float(last) - mid) / mid > 0.01):
        last = mid
    if not last:
        return None
    last = round(float(last), 8)
    spread_pct = round((ask - bid) / bid * 100, 4) if bid and ask else None
    prev_close = prev.get("c")
    change_pct = (
        round((last - float(prev_close)) / float(prev_close) * 100, 4)
        if prev_close else None
    )
    return {
        "symbol":         symbol,
        "last":           last,
        "bid":            bid,
        "ask":            ask,
        "spread_pct":     spread_pct,
        "vwap":           day.get("vw"),
        "open_24h":       day.get("o"),
        "high_24h":       day.get("h"),
        "low_24h":        day.get("l"),
        "volume_24h":     day.get("v"),
        "change_pct_24h": change_pct,
        "sources":        ["alpaca"],
        "fetched_at":     trade.get("t") or datetime.now(timezone.utc).isoformat(),
    }


async def _alpaca_stock_ticker(symbol: str) -> dict | None:
    snap = await _alpaca_get(
        f"{_ALPACA_DATA_BASE}/v2/stocks/{symbol.upper()}/snapshot",
        {"feed": settings.ALPACA_DATA_FEED},
    )
    return _alpaca_snapshot_to_ticker(symbol, snap)


async def _alpaca_crypto_ticker(symbol: str) -> dict | None:
    for cand in _alpaca_crypto_candidates(symbol):
        try:
            data = await _alpaca_get(f"{_ALPACA_CRYPTO_BASE}/snapshots", {"symbols": cand})
            snap = (data.get("snapshots") or {}).get(cand)
            if snap:
                ticker = _alpaca_snapshot_to_ticker(symbol, snap)
                if ticker:
                    return ticker
        except Exception as e:
            log.debug(f"Alpaca crypto snapshot {cand}: {e}")
    return None


async def _alpaca_stock_ohlcv(symbol: str, timeframe: str, limit: int) -> list[dict]:
    params = _alpaca_bar_params(timeframe, limit)
    params |= {"feed": settings.ALPACA_DATA_FEED, "adjustment": "raw"}
    data = await _alpaca_get(f"{_ALPACA_DATA_BASE}/v2/stocks/{symbol.upper()}/bars", params)
    bars = data.get("bars") or []
    return [_alpaca_bar_to_candle(b) for b in reversed(bars)]


async def _alpaca_crypto_ohlcv(symbol: str, timeframe: str, limit: int) -> list[dict]:
    for cand in _alpaca_crypto_candidates(symbol):
        try:
            params = _alpaca_bar_params(timeframe, limit) | {"symbols": cand}
            data = await _alpaca_get(f"{_ALPACA_CRYPTO_BASE}/bars", params)
            bars = (data.get("bars") or {}).get(cand) or []
            if bars:
                return [_alpaca_bar_to_candle(b) for b in reversed(bars)]
        except Exception as e:
            log.debug(f"Alpaca crypto bars {cand}: {e}")
    return []


async def _alpaca_market_open() -> bool | None:
    """US market open right now? (Alpaca trading-API clock, 60s cached.)
    None when the clock can't be fetched — callers should not assume."""
    cached = _ttl_get("alpaca_clock", 60)
    if cached is not None:
        return cached
    base = "https://paper-api.alpaca.markets" if settings.ALPACA_PAPER else "https://api.alpaca.markets"
    try:
        data = await _alpaca_get(f"{base}/v2/clock")
        is_open = bool(data.get("is_open"))
        _ttl_set("alpaca_clock", is_open)
        return is_open
    except Exception as e:  # noqa: BLE001
        log.debug(f"Alpaca clock: {e}")
        return None


async def _alpaca_stock_orderbook(symbol: str) -> dict | None:
    """Alpaca has no L2 book for stocks — synthesize a one-level book from
    the NBBO quote, same trick as the OANDA forex branch."""
    data = await _alpaca_get(
        f"{_ALPACA_DATA_BASE}/v2/stocks/{symbol.upper()}/quotes/latest",
        {"feed": settings.ALPACA_DATA_FEED},
    )
    q = data.get("quote") or {}
    bid = float(q.get("bp") or 0)
    ask = float(q.get("ap") or 0)
    if bid <= 0 or ask <= 0:
        return None
    # Quote sizes are in round lots (100 shares)
    bid_qty = float(q.get("bs") or 0) * 100
    ask_qty = float(q.get("as") or 0) * 100
    payload = _orderbook_payload(symbol, "alpaca", [(bid, bid_qty)], [(ask, ask_qty)])
    if payload:
        payload["fetched_at"] = q.get("t") or payload["fetched_at"]
    return payload


async def _alpaca_crypto_orderbook(symbol: str, depth: int = 20) -> dict | None:
    for cand in _alpaca_crypto_candidates(symbol):
        try:
            data = await _alpaca_get(f"{_ALPACA_CRYPTO_BASE}/latest/orderbooks", {"symbols": cand})
            ob = (data.get("orderbooks") or {}).get(cand)
            if not ob:
                continue
            bids = [(float(l["p"]), float(l["s"])) for l in (ob.get("b") or [])[:depth]]
            asks = [(float(l["p"]), float(l["s"])) for l in (ob.get("a") or [])[:depth]]
            payload = _orderbook_payload(symbol, "alpaca", bids, asks)
            if payload:
                payload["fetched_at"] = ob.get("t") or payload["fetched_at"]
                return payload
        except Exception as e:
            log.debug(f"Alpaca crypto orderbook {cand}: {e}")
    return None


async def _alpaca_stock_trades(symbol: str, limit: int = 50) -> list[dict]:
    params = {
        "limit": limit,
        "feed": settings.ALPACA_DATA_FEED,
        "sort": "desc",
        # Same weekend problem as bars: default start is "today"
        "start": (datetime.now(timezone.utc) - timedelta(days=4)).isoformat(),
    }
    data = await _alpaca_get(f"{_ALPACA_DATA_BASE}/v2/stocks/{symbol.upper()}/trades", params)
    return [
        {
            "id":       str(t.get("i", "")),
            "time":     t.get("t") or datetime.now(timezone.utc).isoformat(),
            "price":    float(t["p"]),
            "amount":   float(t.get("s") or 0),
            # Stock trade condition codes don't encode the aggressor side
            "side":     "NEUTRAL",
            "cost":     float(t["p"]) * float(t.get("s") or 0),
            "exchange": "alpaca",
        }
        for t in data.get("trades") or [] if t.get("p")
    ]


async def _alpaca_crypto_trades(symbol: str, limit: int = 50) -> list[dict]:
    for cand in _alpaca_crypto_candidates(symbol):
        try:
            data = await _alpaca_get(
                f"{_ALPACA_CRYPTO_BASE}/trades",
                {"symbols": cand, "limit": limit, "sort": "desc"},
            )
            trades = (data.get("trades") or {}).get(cand) or []
            if trades:
                return [
                    {
                        "id":       str(t.get("i", "")),
                        "time":     t.get("t") or datetime.now(timezone.utc).isoformat(),
                        "price":    float(t["p"]),
                        "amount":   float(t.get("s") or 0),
                        "side":     {"B": "BUY", "S": "SELL"}.get(t.get("tks"), "NEUTRAL"),
                        "cost":     float(t["p"]) * float(t.get("s") or 0),
                        "exchange": "alpaca",
                    }
                    for t in trades if t.get("p")
                ]
        except Exception as e:
            log.debug(f"Alpaca crypto trades {cand}: {e}")
    return []


async def close_all_exchanges():
    """Call on shutdown to close all WS connections."""
    for ex in _exchange_pool.values():
        try:
            await ex.close()
        except Exception:
            pass
    _exchange_pool.clear()


# ─────────────────────────────────────────────────────────────────────────────
# § 1  LIVE TICKER — best bid/ask across venues
# ─────────────────────────────────────────────────────────────────────────────

async def get_live_ticker(
    symbol: str,
    exchanges: list[str] | None = None,
    timeout: float = 5.0,
) -> dict:
    """
    Fetch live ticker from multiple exchanges simultaneously.
    For forex symbols, prefer OANDA REST v20 pricing.
    """
    if _is_forex_symbol(symbol):
        if _has_oanda_credentials():
            try:
                payload = await _fetch_oanda_pricing(symbol)
                prices = payload.get("prices", [])
                if prices:
                    price = prices[0]
                    bids = price.get("bids", [])
                    asks = price.get("asks", [])
                    bid = float(bids[0].get("price", 0)) if bids else 0.0
                    ask = float(asks[0].get("price", 0)) if asks else 0.0
                    if bid > 0 and ask > 0:
                        mid = round((bid + ask) / 2, 8)
                        spread_pct = round((ask - bid) / bid * 100, 4)
                    else:
                        mid = None
                        spread_pct = None

                    return {
                        "symbol": symbol,
                        "last": mid,
                        "bid": bid,
                        "ask": ask,
                        "spread_pct": spread_pct,
                        "vwap": None,
                        "open_24h": None,
                        "high_24h": None,
                        "low_24h": None,
                        "volume_24h": None,
                        "change_pct_24h": None,
                        "sources": ["oanda"],
                        "fetched_at": price.get("time") or datetime.now(timezone.utc).isoformat(),
                    }
            except Exception as e:
                log.debug(f"OANDA ticker {symbol}: {e}")
                return {"symbol": symbol, "error": "oanda_unavailable", "fetched_at": datetime.now(timezone.utc).isoformat()}
        return {"symbol": symbol, "error": "oanda_credentials_missing", "fetched_at": datetime.now(timezone.utc).isoformat()}

    if _is_equity_symbol(symbol):
        if _has_alpaca_credentials():
            try:
                ticker = await _alpaca_stock_ticker(symbol)
                if ticker:
                    return ticker
            except Exception as e:
                log.debug(f"Alpaca ticker {symbol}: {e}")
        return await _yf_equity_ticker(symbol)

    if not CCXT_AVAILABLE:
        return await _crypto_ticker_fallback(symbol)

    target_exchanges = exchanges or [DEFAULT_CRYPTO_EXCHANGE, "kraken", "okx"]

    async def fetch_one(exchange_id: str) -> dict | None:
        try:
            ex = _get_exchange(exchange_id)
            if not ex:
                return None
            ticker = await asyncio.wait_for(ex.fetch_ticker(symbol), timeout=timeout)
            return {
                "exchange": exchange_id,
                "symbol":   symbol,
                "bid":      ticker.get("bid"),
                "ask":      ticker.get("ask"),
                "last":     ticker.get("last"),
                "open":     ticker.get("open"),
                "high":     ticker.get("high"),
                "low":      ticker.get("low"),
                "volume":   ticker.get("baseVolume"),
                "quote_volume": ticker.get("quoteVolume"),
                "change":   ticker.get("change"),
                "change_pct": ticker.get("percentage"),
                "vwap":     ticker.get("vwap"),
                "ts":       datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            log.debug(f"Ticker fetch failed {exchange_id}/{symbol}: {e}")
            return None

    results = await asyncio.gather(*[fetch_one(ex) for ex in target_exchanges])
    valid = [r for r in results if r and r.get("last")]

    if not valid:
        return await _crypto_ticker_fallback(symbol)

    # Best bid/ask across venues
    best_bid = max((r["bid"] for r in valid if r.get("bid")), default=None)
    best_ask = min((r["ask"] for r in valid if r.get("ask")), default=None)
    prices   = [r["last"] for r in valid if r.get("last")]
    vwap_vals= [r["vwap"] for r in valid if r.get("vwap")]

    primary = valid[0]
    spread_pct = ((best_ask - best_bid) / best_bid * 100) if best_bid and best_ask else None

    return {
        "symbol":        symbol,
        "last":          prices[0] if prices else None,
        "bid":           best_bid,
        "ask":           best_ask,
        "spread_pct":    round(spread_pct, 4) if spread_pct else None,
        "vwap":          round(sum(vwap_vals) / len(vwap_vals), 8) if vwap_vals else None,
        "open_24h":      primary.get("open"),
        "high_24h":      primary.get("high"),
        "low_24h":       primary.get("low"),
        "volume_24h":    primary.get("volume"),
        "quote_vol_24h": primary.get("quote_volume"),
        "change_pct_24h":primary.get("change_pct"),
        "sources":       [r["exchange"] for r in valid],
        "cross_venue_prices": {r["exchange"]: r["last"] for r in valid},
        "fetched_at":    datetime.now(timezone.utc).isoformat(),
    }


def _ticker_fallback(symbol: str) -> dict:
    return {"symbol": symbol, "error": "market_data_unavailable",
            "fetched_at": datetime.now(timezone.utc).isoformat()}


async def _crypto_ticker_fallback(symbol: str) -> dict:
    """Every ccxt venue failed (or ccxt missing) — try Alpaca's crypto feed
    before giving up."""
    if _has_alpaca_credentials():
        ticker = await _alpaca_crypto_ticker(symbol)
        if ticker:
            return ticker
    return _ticker_fallback(symbol)


async def _yf_equity_ticker(symbol: str) -> dict:
    """Equity ticker from yfinance daily bars — the fallback when Alpaca is
    unconfigured or erroring. No intraday quote, so bid/ask stay None."""
    df = await _yf_download(symbol, period="2d", interval="1d", progress=False, auto_adjust=True)
    if df is not None and not df.empty:
        try:
            last = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2]) if len(df) > 1 else None
            change_pct = round((last - prev) / prev * 100, 4) if prev else None
            return {
                "symbol":         symbol,
                "last":           round(last, 4),
                "bid":            None,
                "ask":            None,
                "spread_pct":     None,
                "vwap":           None,
                "open_24h":       float(df["Open"].iloc[-1]),
                "high_24h":       float(df["High"].iloc[-1]),
                "low_24h":        float(df["Low"].iloc[-1]),
                "volume_24h":     float(df["Volume"].iloc[-1]),
                "change_pct_24h": change_pct,
                "sources":        ["yfinance"],
                "fetched_at":     datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            log.debug(f"yfinance equity ticker {symbol}: {e}")
    if not _has_alpaca_credentials():
        return {"symbol": symbol, "error": "alpaca_credentials_missing",
                "fetched_at": datetime.now(timezone.utc).isoformat()}
    return _ticker_fallback(symbol)


# ─────────────────────────────────────────────────────────────────────────────
# § 2  OHLCV — Candlestick data
# ─────────────────────────────────────────────────────────────────────────────

async def get_ohlcv(
    symbol: str,
    timeframe: str = "1m",
    limit: int = 200,
    exchange_id: str | None = None,
) -> list[dict]:
    """
    Fetch OHLCV candles. Timeframes: 1m, 5m, 15m, 1h, 4h, 1d.
    Uses OANDA for forex symbols. Fallbacks are crypto/exchange-specific or yfinance for equities.
    """
    if _is_forex_symbol(symbol):
        if _has_oanda_credentials():
            granularity_map = {
                "1m": "M1",
                "5m": "M5",
                "15m": "M15",
                "1h": "H1",
                "4h": "H4",
                "1d": "D",
            }
            granularity = granularity_map.get(timeframe, "M1")
            instrument = _oanda_instrument(symbol)
            url = f"{_oanda_base_url()}/instruments/{instrument}/candles"
            params = {
                "granularity": granularity,
                "count": limit,
                "price": "MBA",
                "alignmentTimezone": "UTC",
            }
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=_oanda_headers(), params=params, timeout=15) as response:
                        response.raise_for_status()
                        data = await response.json()
                candles = data.get("candles", [])
                return [
                    {
                        "ts": int(datetime.fromisoformat(c["time"].replace("Z", "+00:00")).timestamp() * 1000),
                        "time": c["time"],
                        "open": float(c["mid"]["o"]),
                        "high": float(c["mid"]["h"]),
                        "low": float(c["mid"]["l"]),
                        "close": float(c["mid"]["c"]),
                        "volume": float(c.get("volume", 0)),
                    }
                    for c in candles if c.get("complete") is True
                ]
            except Exception as e:
                log.debug(f"OANDA OHLCV {symbol}: {e}")
                return []
        return []

    if _is_equity_symbol(symbol):
        if _has_alpaca_credentials():
            try:
                candles = await _alpaca_stock_ohlcv(symbol, timeframe, limit)
                if candles:
                    return candles
            except Exception as e:
                log.debug(f"Alpaca OHLCV {symbol}: {e}")
        # fall through to the yfinance fallback below (skip the ccxt loop —
        # no crypto venue lists plain US tickers)
    elif CCXT_AVAILABLE:
        ex_id = exchange_id or DEFAULT_CRYPTO_EXCHANGE
        for ex_name in [ex_id, "kraken", "okx", "bybit"]:
            try:
                ex = _get_exchange(ex_name)
                if not ex:
                    continue
                raw = await asyncio.wait_for(
                    ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit),
                    timeout=8.0,
                )
                if raw:
                    return [
                        {
                            "ts":     int(c[0]),
                            "time":   datetime.fromtimestamp(c[0] / 1000, tz=timezone.utc).isoformat(),
                            "open":   float(c[1]),
                            "high":   float(c[2]),
                            "low":    float(c[3]),
                            "close":  float(c[4]),
                            "volume": float(c[5]),
                        }
                        for c in raw if c[4] is not None
                    ]
            except Exception as e:
                log.debug(f"OHLCV {ex_name}: {e}")

    # Alpaca crypto — last-resort venue when every ccxt exchange failed
    if "/" in symbol and _has_alpaca_credentials():
        candles = await _alpaca_crypto_ohlcv(symbol, timeframe, limit)
        if candles:
            return candles

    # yfinance fallback for stocks / ETFs
    if _yf_ready() and not symbol.endswith("/USDT"):
        try:
            interval_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "60m", "4h": "1h", "1d": "1d"}
            period_map   = {"1m": "1d", "5m": "5d", "15m": "5d", "1h": "1mo", "4h": "3mo", "1d": "1y"}
            df = await _yf_download(
                symbol, period=period_map.get(timeframe, "1mo"),
                interval=interval_map.get(timeframe, "1h"), progress=False, auto_adjust=True,
            )
            if df is not None and not df.empty:
                return [
                    {"ts": int(idx.timestamp() * 1000), "time": idx.isoformat(),
                     "open": float(row["Open"]), "high": float(row["High"]),
                     "low": float(row["Low"]), "close": float(row["Close"]),
                     "volume": float(row["Volume"])}
                    for idx, row in df.tail(limit).iterrows()
                ]
        except Exception as e:
            log.debug(f"yfinance OHLCV {symbol}: {e}")

    return []


# ─────────────────────────────────────────────────────────────────────────────
# § 3  ORDER BOOK — L2 depth
# ─────────────────────────────────────────────────────────────────────────────

def _orderbook_payload(
    symbol: str,
    exchange: str,
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
) -> dict | None:
    """Depth metrics from normalized (price, qty) levels — shared by the
    ccxt venues and the Alpaca books (which may be a single NBBO level)."""
    if not bids or not asks:
        return None

    best_bid  = float(bids[0][0])
    best_ask  = float(asks[0][0])
    mid_price = (best_bid + best_ask) / 2
    spread    = best_ask - best_bid
    spread_bps= spread / mid_price * 10_000

    # Volume-weighted mid
    bid_vols  = sum(float(b[1]) for b in bids[:5])
    ask_vols  = sum(float(a[1]) for a in asks[:5])
    wm_mid    = (best_bid * ask_vols + best_ask * bid_vols) / (bid_vols + ask_vols + 1e-9)

    # Bid-ask imbalance
    total_bid_depth = sum(float(b[0]) * float(b[1]) for b in bids[:10])
    total_ask_depth = sum(float(a[0]) * float(a[1]) for a in asks[:10])
    total_depth     = total_bid_depth + total_ask_depth
    imbalance       = (total_bid_depth - total_ask_depth) / (total_depth + 1e-9)

    # Slippage estimate for $50k notional
    def _slippage(side_levels, notional=50_000):
        filled = 0.0
        cost   = 0.0
        for price, qty in side_levels:
            value  = float(price) * float(qty)
            take   = min(value, notional - cost)
            filled += take / float(price)
            cost   += take
            if cost >= notional:
                break
        if filled == 0:
            return 0.0
        avg_fill = cost / filled
        ref_price = float(side_levels[0][0]) if side_levels else avg_fill
        return abs(avg_fill - ref_price) / ref_price * 100 if ref_price else 0.0

    slip_buy  = _slippage(asks)
    slip_sell = _slippage([(b[0], b[1]) for b in bids])

    return {
        "symbol":          symbol,
        "exchange":        exchange,
        "best_bid":        best_bid,
        "best_ask":        best_ask,
        "mid_price":       round(mid_price, 8),
        "weighted_mid":    round(wm_mid, 8),
        "spread":          round(spread, 8),
        "spread_bps":      round(spread_bps, 4),
        "imbalance":       round(imbalance, 4),
        "bid_depth_usd":   round(total_bid_depth, 2),
        "ask_depth_usd":   round(total_ask_depth, 2),
        "slippage_buy_pct": round(slip_buy, 6),
        "slippage_sell_pct":round(slip_sell, 6),
        "liquidity_score": round(min(100, total_depth / 1_000_000 * 100), 1),
        "bids": [[float(b[0]), float(b[1])] for b in bids[:15]],
        "asks": [[float(a[0]), float(a[1])] for a in asks[:15]],
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


async def get_orderbook(
    symbol: str,
    exchange_id: str | None = None,
    depth: int = 20,
) -> dict:
    """
    Fetch L2 order book. Returns bid/ask walls, imbalance ratio,
    mid-price, weighted mid, liquidity score.
    """
    if _is_forex_symbol(symbol):
        if _has_oanda_credentials():
            try:
                pricing = await _fetch_oanda_pricing(symbol)
                orderbook = await _fetch_oanda_orderbook(symbol)
                prices = pricing.get("prices", [])
                if prices:
                    price = prices[0]
                    bids = price.get("bids", [])
                    asks = price.get("asks", [])
                    bid = float(bids[0].get("price", 0)) if bids else 0.0
                    ask = float(asks[0].get("price", 0)) if asks else 0.0
                    if bid <= 0 or ask <= 0:
                        raise ValueError("invalid oanda pricing")

                    best_bid = bid
                    best_ask = ask
                    mid_price = round((best_bid + best_ask) / 2, 8)
                    spread = best_ask - best_bid
                    spread_bps = round(spread / mid_price * 10_000, 4) if mid_price else 0.0

                    buckets = orderbook.get("orderBook", {}).get("buckets", [])
                    total_long = sum(float(b.get("longCountPercent", 0)) for b in buckets)
                    total_short = sum(float(b.get("shortCountPercent", 0)) for b in buckets)
                    imbalance = (total_long - total_short) / max(total_long + total_short, 1)
                    liquidity_score = round(min(100, max(total_long, total_short)), 1)

                    return {
                        "symbol": symbol,
                        "exchange": "oanda",
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "mid_price": mid_price,
                        "weighted_mid": mid_price,
                        "spread": round(spread, 8),
                        "spread_bps": spread_bps,
                        "imbalance": round(imbalance, 4),
                        "bid_depth_usd": 0.0,
                        "ask_depth_usd": 0.0,
                        "slippage_buy_pct": round(spread / best_ask * 100, 6) if best_ask else 0.0,
                        "slippage_sell_pct": round(spread / best_bid * 100, 6) if best_bid else 0.0,
                        "liquidity_score": liquidity_score,
                        "bids": [[best_bid, 1.0]],
                        "asks": [[best_ask, 1.0]],
                        "fetched_at": orderbook.get("orderBook", {}).get("time") or datetime.now(timezone.utc).isoformat(),
                    }
            except Exception as e:
                log.debug(f"OANDA orderbook {symbol}: {e}")
                return {"symbol": symbol, "error": "oanda_unavailable"}
        return {"symbol": symbol, "error": "oanda_credentials_missing"}

    if _is_equity_symbol(symbol):
        if _has_alpaca_credentials():
            try:
                payload = await _alpaca_stock_orderbook(symbol)
                if payload:
                    return payload
            except Exception as e:
                log.debug(f"Alpaca orderbook {symbol}: {e}")
            # IEX quotes zero out when the US market is closed — tell the
            # caller *why* the book is empty instead of a generic error
            if await _alpaca_market_open() is False:
                return {"symbol": symbol, "error": "equity_market_closed"}
            return {"symbol": symbol, "error": "orderbook_unavailable"}
        return {"symbol": symbol, "error": "alpaca_credentials_missing"}

    ex_id = exchange_id or DEFAULT_CRYPTO_EXCHANGE

    for ex_name in [ex_id, "kraken", "okx"]:
        try:
            if not CCXT_AVAILABLE:
                break
            ex = _get_exchange(ex_name)
            if not ex:
                continue
            ob = await asyncio.wait_for(ex.fetch_order_book(symbol, limit=depth), timeout=5.0)
            # Some venues (kraken, okx) return [price, qty, timestamp] levels —
            # normalize to (price, qty) or the tuple unpacking below blows up.
            bids = [(float(b[0]), float(b[1])) for b in ob.get("bids", [])[:depth]]
            asks = [(float(a[0]), float(a[1])) for a in ob.get("asks", [])[:depth]]

            payload = _orderbook_payload(symbol, ex_name, bids, asks)
            if payload:
                return payload
        except Exception as e:
            log.debug(f"Orderbook {ex_name}: {e}")

    # Alpaca crypto — last-resort venue when every ccxt exchange failed
    if "/" in symbol and _has_alpaca_credentials():
        payload = await _alpaca_crypto_orderbook(symbol, depth)
        if payload:
            return payload

    return {"symbol": symbol, "error": "orderbook_unavailable"}


# ─────────────────────────────────────────────────────────────────────────────
# § 4  RECENT TRADES — trade tape
# ─────────────────────────────────────────────────────────────────────────────

async def get_recent_trades(
    symbol: str,
    exchange_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Fetch recent trade tape from exchange."""
    if _is_forex_symbol(symbol):
        if _has_oanda_credentials():
            try:
                return await _fetch_oanda_transactions(symbol, limit)
            except Exception as e:
                log.debug(f"OANDA trades {symbol}: {e}")
                return []
        return []

    if _is_equity_symbol(symbol):
        if _has_alpaca_credentials():
            try:
                return await _alpaca_stock_trades(symbol, limit)
            except Exception as e:
                log.debug(f"Alpaca trades {symbol}: {e}")
        return []

    ex_id = exchange_id or DEFAULT_CRYPTO_EXCHANGE

    for ex_name in [ex_id, "kraken", "okx"]:
        try:
            if not CCXT_AVAILABLE:
                break
            ex = _get_exchange(ex_name)
            if not ex:
                continue
            trades = await asyncio.wait_for(
                ex.fetch_trades(symbol, limit=limit), timeout=5.0
            )
            return [
                {
                    "id":        str(t.get("id", "")),
                    "time":      datetime.fromtimestamp(
                        t["timestamp"] / 1000, tz=timezone.utc
                    ).isoformat() if t.get("timestamp") else datetime.now(timezone.utc).isoformat(),
                    "price":     float(t["price"]),
                    "amount":    float(t["amount"]),
                    "side":      t.get("side", "neutral").upper(),
                    "cost":      float(t.get("cost") or float(t["price"]) * float(t["amount"])),
                    "exchange":  ex_name,
                }
                for t in trades if t.get("price")
            ]
        except Exception as e:
            log.debug(f"Trades {ex_name}: {e}")

    # Alpaca crypto — last-resort venue when every ccxt exchange failed
    if "/" in symbol and _has_alpaca_credentials():
        return await _alpaca_crypto_trades(symbol, limit)

    return []


# ─────────────────────────────────────────────────────────────────────────────
# § 5  TECHNICAL INDICATORS — full stack
# ─────────────────────────────────────────────────────────────────────────────

def compute_technical_indicators(
    prices: list[float],
    volumes: list[float] | None = None,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
) -> dict:
    """
    Compute comprehensive technical indicators using `ta` library.
    Requires at least 50 price points for meaningful output.

    Indicators:
      Trend:      EMA(9), EMA(21), EMA(50), EMA(200), MACD
      Momentum:   RSI(14), Stochastic %K/%D, CCI, Williams %R, MFI
      Volatility: ATR(14), Bollinger Bands, Keltner Channels
      Volume:     OBV, VWAP, CMF, Volume SMA
      Support/Resistance: Pivot Points, Key S/R levels
    """
    if len(prices) < 14:
        return {"error": "insufficient_data", "min_required": 14}

    arr = np.array(prices, dtype=float)
    result: dict[str, Any] = {}

    if TA_AVAILABLE and PANDAS_AVAILABLE:
        import pandas as pd
        close = pd.Series(arr)
        high  = pd.Series(highs or arr)
        low   = pd.Series(lows  or arr)
        vol   = pd.Series(volumes or [1.0] * len(arr))

        try:
            # ── Trend ─────────────────────────────────────────────────
            result["ema_9"]   = round(float(ta.trend.ema_indicator(close, window=9).iloc[-1]), 8)
            result["ema_21"]  = round(float(ta.trend.ema_indicator(close, window=21).iloc[-1]), 8)
            if len(arr) >= 50:
                result["ema_50"]  = round(float(ta.trend.ema_indicator(close, window=50).iloc[-1]), 8)
            if len(arr) >= 200:
                result["ema_200"] = round(float(ta.trend.ema_indicator(close, window=200).iloc[-1]), 8)
            result["sma_20"]  = round(float(ta.trend.sma_indicator(close, window=20).iloc[-1]), 8) if len(arr) >= 20 else None

            # MACD
            macd_obj = ta.trend.MACD(close)
            result["macd"]        = round(float(macd_obj.macd().iloc[-1]), 8)
            result["macd_signal"] = round(float(macd_obj.macd_signal().iloc[-1]), 8)
            result["macd_hist"]   = round(float(macd_obj.macd_diff().iloc[-1]), 8)
            result["macd_cross"]  = (
                "BULL" if result["macd"] > result["macd_signal"]
                else "BEAR"
            )

            # ── Momentum ──────────────────────────────────────────────
            rsi = ta.momentum.RSIIndicator(close, window=14)
            result["rsi_14"] = round(float(rsi.rsi().iloc[-1]), 2)
            result["rsi_signal"] = (
                "OVERBOUGHT" if result["rsi_14"] > 70
                else "OVERSOLD" if result["rsi_14"] < 30
                else "NEUTRAL"
            )

            stoch = ta.momentum.StochasticOscillator(high, low, close)
            result["stoch_k"] = round(float(stoch.stoch().iloc[-1]), 2)
            result["stoch_d"] = round(float(stoch.stoch_signal().iloc[-1]), 2)
            result["stoch_signal"] = (
                "OVERBOUGHT" if result["stoch_k"] > 80
                else "OVERSOLD" if result["stoch_k"] < 20
                else "NEUTRAL"
            )

            cci = ta.trend.CCIIndicator(high, low, close)
            result["cci_20"] = round(float(cci.cci().iloc[-1]), 2)

            wr = ta.momentum.WilliamsRIndicator(high, low, close)
            result["williams_r"] = round(float(wr.williams_r().iloc[-1]), 2)

            # ── Volatility ────────────────────────────────────────────
            atr = ta.volatility.AverageTrueRange(high, low, close)
            result["atr_14"]     = round(float(atr.average_true_range().iloc[-1]), 8)
            result["atr_pct"]    = round(result["atr_14"] / float(close.iloc[-1]) * 100, 4)

            bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
            result["bb_upper"]   = round(float(bb.bollinger_hband().iloc[-1]), 8)
            result["bb_mid"]     = round(float(bb.bollinger_mavg().iloc[-1]), 8)
            result["bb_lower"]   = round(float(bb.bollinger_lband().iloc[-1]), 8)
            result["bb_width"]   = round(float(bb.bollinger_wband().iloc[-1]), 4)
            result["bb_pct"]     = round(float(bb.bollinger_pband().iloc[-1]), 4)
            result["bb_signal"]  = (
                "UPPER_BAND" if float(close.iloc[-1]) >= result["bb_upper"]
                else "LOWER_BAND" if float(close.iloc[-1]) <= result["bb_lower"]
                else "INSIDE"
            )

            # ── Volume indicators ─────────────────────────────────────
            if vol.sum() > 0:
                obv = ta.volume.OnBalanceVolumeIndicator(close, vol)
                result["obv"] = round(float(obv.on_balance_volume().iloc[-1]), 2)

                cmf = ta.volume.ChaikinMoneyFlowIndicator(high, low, close, vol)
                result["cmf"] = round(float(cmf.chaikin_money_flow().iloc[-1]), 4)
                result["cmf_signal"] = "BULLISH" if result["cmf"] > 0 else "BEARISH"

                mfi = ta.volume.MFIIndicator(high, low, close, vol)
                result["mfi"] = round(float(mfi.money_flow_index().iloc[-1]), 2)
                result["mfi_signal"] = (
                    "OVERBOUGHT" if result["mfi"] > 80
                    else "OVERSOLD" if result["mfi"] < 20
                    else "NEUTRAL"
                )

                # VWAP (rolling, using available data)
                vwap_tp = (high + low + close) / 3
                result["vwap"] = round(float((vwap_tp * vol).sum() / vol.sum()), 8)

        except Exception as e:
            log.warning(f"TA calculation error: {e}")

    # ── Manual fallback indicators ────────────────────────────────────────
    # RSI manual if TA not available
    if "rsi_14" not in result and len(arr) >= 15:
        deltas = np.diff(arr[-15:])
        gains  = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_g  = np.mean(gains) + 1e-10
        avg_l  = np.mean(losses) + 1e-10
        rs     = avg_g / avg_l
        result["rsi_14"] = round(100 - 100 / (1 + rs), 2)
        result["rsi_signal"] = (
            "OVERBOUGHT" if result["rsi_14"] > 70 else
            "OVERSOLD"   if result["rsi_14"] < 30 else "NEUTRAL"
        )

    # Pivot points (daily)
    if highs and lows:
        pivot = (highs[-1] + lows[-1] + prices[-1]) / 3
        result["pivot_point"] = round(pivot, 8)
        result["pivot_r1"]    = round(2 * pivot - lows[-1], 8)
        result["pivot_r2"]    = round(pivot + (highs[-1] - lows[-1]), 8)
        result["pivot_s1"]    = round(2 * pivot - highs[-1], 8)
        result["pivot_s2"]    = round(pivot - (highs[-1] - lows[-1]), 8)

    # Current price position
    current = float(arr[-1])
    result["price"]      = current
    result["price_vs_ema9"] = round((current - result.get("ema_9", current)) / current * 100, 4) if result.get("ema_9") else None
    result["price_vs_sma20"]= round((current - result.get("sma_20", current)) / current * 100, 4) if result.get("sma_20") else None

    # Composite signal strength (-100 to +100)
    signal_score = 0.0
    signals_used = 0
    if result.get("rsi_14"):
        signal_score += (50 - result["rsi_14"]) * -1  # inverted: high RSI = bearish signal
        signals_used += 1
    if result.get("macd_hist"):
        signal_score += 30 if result["macd_hist"] > 0 else -30
        signals_used += 1
    if result.get("cmf"):
        signal_score += result["cmf"] * 100
        signals_used += 1
    if result.get("bb_pct") is not None:
        signal_score += (0.5 - result["bb_pct"]) * 40
        signals_used += 1

    result["composite_signal"] = round(signal_score / max(signals_used, 1), 2)
    result["composite_bias"]   = (
        "STRONG_BULL" if result["composite_signal"] > 40 else
        "BULL"        if result["composite_signal"] > 15 else
        "STRONG_BEAR" if result["composite_signal"] < -40 else
        "BEAR"        if result["composite_signal"] < -15 else
        "NEUTRAL"
    )

    return result


# ─────────────────────────────────────────────────────────────────────────────
# § 6  MULTI-ASSET SNAPSHOT — cross-asset dashboard
# ─────────────────────────────────────────────────────────────────────────────

async def get_multi_asset_snapshot(
    crypto_symbols: list[str] | None = None,
    forex_pairs: list[str] | None = None,
    stock_tickers: list[str] | None = None,
) -> dict:
    """
    Parallel snapshot across crypto, forex, stocks.
    Returns price, 24h change, volume, technical bias for each.
    """
    crypto  = crypto_symbols  or CORE_CRYPTO_SYMBOLS[:4]
    forex   = forex_pairs     or CORE_FOREX_PAIRS[:4]
    stocks  = stock_tickers   or CORE_STOCK_TICKERS[:4]

    cache_key = f"snapshot:{','.join(crypto)}|{','.join(forex)}|{','.join(stocks)}"
    cached = _ttl_get(cache_key, 60)
    if cached is not None:
        return cached

    async def crypto_snap(sym: str) -> dict | None:
        try:
            ex = _get_exchange(DEFAULT_CRYPTO_EXCHANGE)
            if not ex or not CCXT_AVAILABLE:
                return None
            t = await asyncio.wait_for(ex.fetch_ticker(sym), timeout=4.0)
            return {
                "symbol": sym, "asset_class": "crypto",
                "price": t.get("last"), "change_pct": t.get("percentage"),
                "volume_24h": t.get("baseVolume"), "high_24h": t.get("high"),
                "low_24h": t.get("low"), "exchange": DEFAULT_CRYPTO_EXCHANGE,
            }
        except Exception:
            return None

    async def forex_snap(pair: str) -> dict | None:
        try:
            # Kraken has forex-like pairs
            ex = _get_exchange("kraken")
            if not ex or not CCXT_AVAILABLE:
                return None
            kraken_sym = pair.replace("/", "")
            t = await asyncio.wait_for(ex.fetch_ticker(pair), timeout=4.0)
            return {
                "symbol": pair, "asset_class": "forex",
                "price": t.get("last"), "change_pct": t.get("percentage"),
                "high_24h": t.get("high"), "low_24h": t.get("low"),
                "exchange": "kraken",
            }
        except Exception:
            return None

    async def stock_snap(ticker: str) -> dict | None:
        if _has_alpaca_credentials():
            try:
                t = await _alpaca_stock_ticker(ticker)
                if t and t.get("last"):
                    return {
                        "symbol": ticker, "asset_class": "equity",
                        "price": t["last"], "change_pct": t.get("change_pct_24h"),
                        "volume_24h": t.get("volume_24h"),
                        "high_24h": t.get("high_24h"), "low_24h": t.get("low_24h"),
                        "exchange": "alpaca",
                    }
            except Exception as e:
                log.debug(f"Alpaca snapshot {ticker}: {e}")
        if not _yf_ready():
            return None
        try:
            data = await _yf_download(ticker, period="2d", interval="1d", progress=False, auto_adjust=True)
            if data is None or data.empty or len(data) < 2:
                return None
            prev_close = float(data["Close"].iloc[-2])
            last_close = float(data["Close"].iloc[-1])
            change_pct = (last_close - prev_close) / prev_close * 100
            return {
                "symbol": ticker, "asset_class": "equity",
                "price": round(last_close, 4),
                "change_pct": round(change_pct, 4),
                "volume_24h": float(data["Volume"].iloc[-1]),
                "high_24h": float(data["High"].iloc[-1]),
                "low_24h": float(data["Low"].iloc[-1]),
                "exchange": "NYSE/NASDAQ",
            }
        except Exception:
            return None

    # Run all in parallel
    crypto_tasks = [crypto_snap(s) for s in crypto]
    forex_tasks  = [forex_snap(p)  for p in forex]
    stock_tasks  = [stock_snap(t)  for t in stocks]

    all_results = await asyncio.gather(
        *crypto_tasks, *forex_tasks, *stock_tasks,
        return_exceptions=True,
    )

    items = []
    for r in all_results:
        if r and not isinstance(r, Exception) and r.get("price"):
            r["up"] = (r.get("change_pct") or 0) >= 0
            items.append(r)

    snapshot = {
        "assets": items,
        "count":  len(items),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    _ttl_set(cache_key, snapshot)
    return snapshot


# ─────────────────────────────────────────────────────────────────────────────
# § 7  MARKET BREADTH — macro market health
# ─────────────────────────────────────────────────────────────────────────────

async def get_market_breadth() -> dict:
    """
    Compute market breadth indicators:
      - Fear & Greed proxy (RSI-based)
      - Crypto dominance (BTC vs ETH vs ALTs)
      - Sector strength
      - VIX level
      - Yield curve (10Y-2Y spread)
    """
    cached = _ttl_get("breadth", 60)
    if cached is not None:
        return cached

    result = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }

    # Crypto relative strength
    if CCXT_AVAILABLE:
        async def fetch_change(sym: str) -> tuple[str, float]:
            try:
                ex = _get_exchange(DEFAULT_CRYPTO_EXCHANGE)
                t  = await asyncio.wait_for(ex.fetch_ticker(sym), timeout=4.0)
                return sym, float(t.get("percentage") or 0)
            except Exception:
                return sym, 0.0

        changes = dict(await asyncio.gather(
            *[fetch_change(s) for s in ["BTC/USDT", "ETH/USDT", "SOL/USDT"]]
        ))

        btc_ch = changes.get("BTC/USDT", 0)
        eth_ch = changes.get("ETH/USDT", 0)
        sol_ch = changes.get("SOL/USDT", 0)
        avg_alt_ch = (eth_ch + sol_ch) / 2

        result["crypto"] = {
            "btc_24h_pct":    round(btc_ch, 4),
            "eth_24h_pct":    round(eth_ch, 4),
            "sol_24h_pct":    round(sol_ch, 4),
            "btc_dominance_signal": "RISK_ON" if btc_ch > avg_alt_ch else "ALT_SEASON",
            "market_momentum": "BULL" if btc_ch > 2 else "BEAR" if btc_ch < -2 else "NEUTRAL",
        }

    # Equity indices via yfinance
    if _yf_ready():
        try:
            indices = await _yf_download("^VIX ^GSPC ^TNX", period="2d", interval="1d",
                                         progress=False, auto_adjust=True)
            if indices is not None and not indices.empty:
                spx = float(indices["Close"]["^GSPC"].iloc[-1])
                vix = float(indices["Close"]["^VIX"].iloc[-1])
                tny = float(indices["Close"]["^TNX"].iloc[-1])  # 10Y yield

                result["equities"] = {
                    "spx_price":    round(spx, 2),
                    "vix":          round(vix, 2),
                    "vix_regime":   "FEAR" if vix > 30 else "ELEVATED" if vix > 20 else "CALM",
                    "ten_yr_yield": round(tny, 3),
                    "risk_tone":    "RISK_OFF" if vix > 25 else "RISK_ON",
                }
        except Exception as e:
            log.debug(f"Breadth yfinance: {e}")

    # Fear & Greed proxy (simple RSI-based on BTC)
    result["fear_greed_proxy"] = _fear_greed_proxy(result)

    _ttl_set("breadth", result)
    return result


def _fear_greed_proxy(breadth_data: dict) -> dict:
    """Simple fear/greed index proxy from available data."""
    score = 50  # neutral baseline
    label = "NEUTRAL"

    crypto = breadth_data.get("crypto", {})
    eq     = breadth_data.get("equities", {})

    btc_ch = crypto.get("btc_24h_pct", 0)
    vix    = eq.get("vix", 20)

    # BTC momentum component
    score += min(25, max(-25, btc_ch * 3))

    # VIX component (inverse)
    if vix < 15:   score += 15
    elif vix < 20: score += 5
    elif vix > 30: score -= 20
    elif vix > 25: score -= 10

    score = min(100, max(0, score))
    if score >= 75:   label = "EXTREME_GREED"
    elif score >= 55: label = "GREED"
    elif score >= 45: label = "NEUTRAL"
    elif score >= 25: label = "FEAR"
    else:             label = "EXTREME_FEAR"

    return {"score": round(score, 1), "label": label}


# ─────────────────────────────────────────────────────────────────────────────
# § 8  FUNDING RATES — perpetual futures
# ─────────────────────────────────────────────────────────────────────────────

async def get_funding_rates(symbols: list[str] | None = None) -> list[dict]:
    """
    Fetch perpetual futures funding rates from Bybit / OKX.
    Funding rate reveals market sentiment (positive = long bias, negative = short bias).
    """
    syms = symbols or ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]
    results = []

    for ex_id in ["bybit", "okx"]:
        try:
            if not CCXT_AVAILABLE:
                break
            ex = _get_exchange(ex_id)
            if not ex:
                continue
            ex.options["defaultType"] = "swap"
            for sym in syms:
                try:
                    fr = await asyncio.wait_for(ex.fetch_funding_rate(sym), timeout=4.0)
                    if fr:
                        rate = float(fr.get("fundingRate") or 0)
                        results.append({
                            "symbol":       sym.split(":")[0],
                            "exchange":     ex_id,
                            "funding_rate": round(rate * 100, 6),
                            "funding_rate_8h_pct": round(rate * 100, 6),
                            "annualized_pct": round(rate * 3 * 365 * 100, 2),
                            "sentiment":    "LONG_BIAS" if rate > 0 else "SHORT_BIAS",
                            "next_funding": fr.get("fundingDatetime"),
                        })
                except Exception:
                    pass
            if results:
                break  # got data from this exchange
        except Exception as e:
            log.debug(f"Funding rates {ex_id}: {e}")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# § 9  LIVE PRICE STREAM — async generator for SSE
# ─────────────────────────────────────────────────────────────────────────────

async def live_price_stream(
    symbols: list[str],
    exchange_id: str | None = None,
    interval_seconds: float = 2.0,
):
    """
    Async generator: yields live price updates every `interval_seconds`.
    Used by the SSE /market/stream endpoint.
    Each yield: dict with symbol, price, bid, ask, volume, change_pct, ts
    """
    ex_id = exchange_id or DEFAULT_CRYPTO_EXCHANGE

    while True:
        for sym in symbols:
            try:
                if CCXT_AVAILABLE:
                    ex = _get_exchange(ex_id)
                    t  = await asyncio.wait_for(ex.fetch_ticker(sym), timeout=3.0)
                    yield {
                        "type":        "price",
                        "symbol":      sym,
                        "price":       t.get("last"),
                        "bid":         t.get("bid"),
                        "ask":         t.get("ask"),
                        "change_pct":  t.get("percentage"),
                        "volume":      t.get("baseVolume"),
                        "exchange":    ex_id,
                        "ts":          datetime.now(timezone.utc).isoformat(),
                    }
            except Exception as e:
                log.debug(f"Stream price error {sym}: {e}")
                yield {
                    "type": "error", "symbol": sym,
                    "message": str(e), "ts": datetime.now(timezone.utc).isoformat()
                }
        await asyncio.sleep(interval_seconds)
