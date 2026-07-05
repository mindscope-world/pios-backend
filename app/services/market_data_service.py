"""
PiOS Live Market Data Service
==============================
Real-time market data from globally-available exchanges (no Binance):
  - KuCoin    — crypto, global, high volume
  - OKX       — crypto, global
  - Kraken    — crypto, EU/US/global
  - Bybit     — crypto, global
  - Alpaca    — US stocks + forex
  - OANDA     — forex (60+ pairs)
  - yfinance  — stocks, ETFs, indices (fallback REST)

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
except ImportError:
    YF_AVAILABLE = False

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


def _is_forex_symbol(symbol: str) -> bool:
    return isinstance(symbol, str) and "/" in symbol and len(symbol.split("/")) == 2


def _oanda_instrument(symbol: str) -> str:
    return symbol.replace("/", "_")


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

    if not CCXT_AVAILABLE:
        return _ticker_fallback(symbol)

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
        return _ticker_fallback(symbol)

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

    ex_id = exchange_id or DEFAULT_CRYPTO_EXCHANGE

    if CCXT_AVAILABLE:
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

    # yfinance fallback for stocks / ETFs
    if YF_AVAILABLE and not symbol.endswith("/USDT"):
        try:
            interval_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "60m", "4h": "1h", "1d": "1d"}
            period_map   = {"1m": "1d", "5m": "5d", "15m": "5d", "1h": "1mo", "4h": "3mo", "1d": "1y"}
            df = yf.download(
                symbol, period=period_map.get(timeframe, "1mo"),
                interval=interval_map.get(timeframe, "1h"), progress=False, auto_adjust=True,
            )
            if not df.empty:
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

    ex_id = exchange_id or DEFAULT_CRYPTO_EXCHANGE

    for ex_name in [ex_id, "kraken", "okx"]:
        try:
            if not CCXT_AVAILABLE:
                break
            ex = _get_exchange(ex_name)
            if not ex:
                continue
            ob = await asyncio.wait_for(ex.fetch_order_book(symbol, limit=depth), timeout=5.0)
            bids = ob.get("bids", [])[:depth]
            asks = ob.get("asks", [])[:depth]

            if not bids or not asks:
                continue

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

            # Slippage estimate for 1 BTC / $50k notional
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
                "exchange":        ex_name,
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
        except Exception as e:
            log.debug(f"Orderbook {ex_name}: {e}")

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
        if not YF_AVAILABLE:
            return None
        try:
            data = yf.download(ticker, period="2d", interval="1d", progress=False, auto_adjust=True)
            if data.empty or len(data) < 2:
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

    return {
        "assets": items,
        "count":  len(items),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


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
    if YF_AVAILABLE:
        try:
            indices = yf.download("^VIX ^GSPC ^TNX", period="2d", interval="1d",
                                   progress=False, auto_adjust=True)
            if not indices.empty:
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
