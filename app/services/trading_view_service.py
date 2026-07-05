# app/services/trading_view_service.py

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
import aiohttp

from app.core.config import settings

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.all_models import (
    Symbol,
    Candle1m,
    Candle1h,
)

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _resolution_to_model(resolution: str):
    """
    TradingView resolution mapper.
    """

    resolution = str(resolution)

    if resolution in ["60", "1H", "1h"]:
        return Candle1h

    return Candle1m

def _normalize_symbol(symbol: str) -> str:
    if not symbol:
        return ""

    return (
        symbol
        .strip()
        .upper()
        .replace("/", "")
        .replace("-", "")
    )


def _normalize_resolution(resolution: str) -> str:
    """
    Normalize TradingView resolutions.
    """

    mapping = {
        "1": "1",
        "5": "1",
        "15": "1",
        "30": "1",
        "60": "60",
        "1H": "60",
        "1h": "60",
    }

    return mapping.get(str(resolution), "1")


# ---------------------------------------------------------
# Core service
# ---------------------------------------------------------


async def compute_tradingview_payload(
    *,
    symbol: str,
    resolution: str,
    db: AsyncSession,
    limit: int = 500,
) -> dict:
    """
    Creates a TradingView-ready payload for worker publishing.

    This is designed for Redis pub/sub or websocket fanout.

    Includes:
    - latest price
    - latest candle
    - candle history
    - symbol metadata

    Never raises.
    """

    try:
        normalized_resolution = _normalize_resolution(resolution)

        # Support broker-prefixed symbols like "OANDA:EUR_USD" or "OANDA:EUR/USD"
        broker = None
        lookup_symbol = symbol
        if ":" in symbol:
            parts = symbol.split(":", 1)
            broker = parts[0].upper()
            lookup_symbol = parts[1]

        normalized = _normalize_symbol(lookup_symbol)

        symbol_result = await db.execute(
            select(Symbol).where(
                func.upper(func.replace(func.replace(Symbol.symbol, "/", ""), "-", "")) == normalized
            )
        )

        sym = symbol_result.scalar_one_or_none()

        candle_model = _resolution_to_model(normalized_resolution)

        candles_result = await db.execute(
            select(candle_model)
            .where(candle_model.symbol_id == sym.id)
            .order_by(candle_model.time.desc())
            .limit(limit)
        )

        candles = list(reversed(candles_result.scalars().all()))

        if not candles:
            # Fallback: attempt to fetch a single latest price from OANDA (or public rates)
            price = None
            timestamp = int(datetime.now(timezone.utc).timestamp())
            # try OANDA only for forex-like symbols
            try:
                async with aiohttp.ClientSession() as session:
                    use_oanda = bool(settings.OANDA_API_KEY and settings.OANDA_ACCOUNT_ID)
                    if use_oanda:
                        base_url = (
                            "https://api-fxtrade.oanda.com/v3"
                            if settings.OANDA_ENVIRONMENT == "live"
                            else "https://api-fxpractice.oanda.com/v3"
                        )
                        pricing_url = f"{base_url}/accounts/{settings.OANDA_ACCOUNT_ID}/pricing"
                        headers = {"Authorization": f"Bearer {settings.OANDA_API_KEY}", "Accept-Datetime-Format": "RFC3339"}
                        instrument = lookup_symbol.replace("/", "_")
                        params = {"instruments": instrument}
                        async with session.get(pricing_url, headers=headers, params=params, timeout=10) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                prices = data.get("prices", [])
                                if prices:
                                    p = prices[0]
                                    bids = p.get("bids", [])
                                    asks = p.get("asks", [])
                                    if bids and asks:
                                        bid = float(bids[0].get("price", 0))
                                        ask = float(asks[0].get("price", 0))
                                        if bid > 0 and ask > 0:
                                            price = (bid + ask) / 2
                    # if OANDA not available or failed, try public rates for simple base pairs
                    if price is None:
                        # public API supports base currencies like USD,EUR,GBP,JPY
                        url = "https://open.er-api.com/v6/latest/"
                        base = lookup_symbol.split("/")[0] if "/" in lookup_symbol else lookup_symbol[:3]
                        async with session.get(url + base, timeout=10) as r:
                            if r.status == 200:
                                data = await r.json()
                                rates = data.get("rates", {})
                                if "/" in lookup_symbol:
                                    quote = lookup_symbol.split("/")[1]
                                else:
                                    quote = lookup_symbol[3:6]
                                if quote in rates:
                                    price = float(rates[quote])
            except Exception:
                price = None

            if price is None:
                # final fallback: zeroed minimal payload
                price = 0.0
            # build a minimal single-candle response
            history = {
                "s": "ok",
                "t": [timestamp],
                "o": [price],
                "h": [price],
                "l": [price],
                "c": [price],
                "v": [0],
            }

            return {
                "symbol": sym.symbol if sym else lookup_symbol,
                "symbol_id": sym.id if sym else None,
                "resolution": normalized_resolution,
                "price": safe_float(price),
                "latest_candle": {
                    "time": timestamp,
                    "open": safe_float(price),
                    "high": safe_float(price),
                    "low": safe_float(price),
                    "close": safe_float(price),
                    "volume": 0,
                },
                "history": history,
                "bars_count": 1,
                "from": history["t"][0],
                "to": history["t"][-1],
                "evaluated_at": now_iso(),
            }

        latest = candles[-1]

        history = {
            "s": "ok",
            "t": [int(c.time.timestamp()) for c in candles],
            "o": [safe_float(c.open) for c in candles],
            "h": [safe_float(c.high) for c in candles],
            "l": [safe_float(c.low) for c in candles],
            "c": [safe_float(c.close) for c in candles],
            "v": [safe_float(c.volume) for c in candles],
        }

        return {
            "symbol": sym.symbol,
            "symbol_id": sym.id,
            "resolution": normalized_resolution,
            "price": safe_float(latest.close),
            "latest_candle": {
                "time": int(latest.time.timestamp()),
                "open": safe_float(latest.open),
                "high": safe_float(latest.high),
                "low": safe_float(latest.low),
                "close": safe_float(latest.close),
                "volume": safe_float(latest.volume),
            },
            "history": history,
            "bars_count": len(candles),
            "from": history["t"][0] if history["t"] else None,
            "to": history["t"][-1] if history["t"] else None,
            "evaluated_at": now_iso(),
        }

    except Exception as exc:
        return {
            "error": str(exc),
            "symbol": symbol,
            "resolution": resolution,
            "evaluated_at": now_iso(),
        }


# ---------------------------------------------------------
# Optional live tick payload
# ---------------------------------------------------------


async def compute_live_tick_payload(
    symbol: str,
    db: AsyncSession,
) -> dict:
    """
    Lightweight live payload for fast UI updates.
    """

    try:
        # support broker-prefixed symbols
        broker = None
        lookup_symbol = symbol
        if ":" in symbol:
            parts = symbol.split(":", 1)
            broker = parts[0].upper()
            lookup_symbol = parts[1]

        normalized = _normalize_symbol(lookup_symbol)

        symbol_result = await db.execute(
            select(Symbol).where(
                func.upper(func.replace(func.replace(Symbol.symbol, "/", ""), "-", "")) == normalized
            )
        )

        sym = symbol_result.scalar_one_or_none()

        candle_result = None
        latest = None
        if sym is not None:
            candle_result = await db.execute(
                select(Candle1m)
                .where(Candle1m.symbol_id == sym.id)
                .order_by(Candle1m.time.desc())
                .limit(1)
            )

            latest = candle_result.scalar_one_or_none()

        if latest is None:
            # Try OANDA / public fallback similar to compute_tradingview_payload
            price = None
            timestamp = int(datetime.now(timezone.utc).timestamp())
            try:
                async with aiohttp.ClientSession() as session:
                    use_oanda = bool(settings.OANDA_API_KEY and settings.OANDA_ACCOUNT_ID)
                    if use_oanda:
                        base_url = (
                            "https://api-fxtrade.oanda.com/v3"
                            if settings.OANDA_ENVIRONMENT == "live"
                            else "https://api-fxpractice.oanda.com/v3"
                        )
                        pricing_url = f"{base_url}/accounts/{settings.OANDA_ACCOUNT_ID}/pricing"
                        headers = {"Authorization": f"Bearer {settings.OANDA_API_KEY}", "Accept-Datetime-Format": "RFC3339"}
                        instrument = lookup_symbol.replace("/", "_")
                        params = {"instruments": instrument}
                        async with session.get(pricing_url, headers=headers, params=params, timeout=10) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                prices = data.get("prices", [])
                                if prices:
                                    p = prices[0]
                                    bids = p.get("bids", [])
                                    asks = p.get("asks", [])
                                    if bids and asks:
                                        bid = float(bids[0].get("price", 0))
                                        ask = float(asks[0].get("price", 0))
                                        if bid > 0 and ask > 0:
                                            price = (bid + ask) / 2
                    if price is None:
                        url = "https://open.er-api.com/v6/latest/"
                        base = lookup_symbol.split("/")[0] if "/" in lookup_symbol else lookup_symbol[:3]
                        async with session.get(url + base, timeout=10) as r:
                            if r.status == 200:
                                data = await r.json()
                                rates = data.get("rates", {})
                                if "/" in lookup_symbol:
                                    quote = lookup_symbol.split("/")[1]
                                else:
                                    quote = lookup_symbol[3:6]
                                if quote in rates:
                                    price = float(rates[quote])
            except Exception:
                price = None

            if price is None:
                price = 0.0

            return {
                "symbol": sym.symbol if sym else lookup_symbol,
                "symbol_id": sym.id if sym else None,
                "price": safe_float(price),
                "change": 0.0,
                "change_pct": 0.0,
                "time": timestamp,
                "volume": 0,
                "evaluated_at": now_iso(),
            }
        # If we have a latest candle from DB, return its data
        if latest is not None:
            return {
                "symbol": sym.symbol,
                "symbol_id": sym.id,
                "price": safe_float(latest.close),
                "change": (safe_float(latest.close) - safe_float(latest.open)),
                "change_pct": (( (safe_float(latest.close) - safe_float(latest.open)) / safe_float(latest.open, 1) ) * 100),
                "time": int(latest.time.timestamp()),
                "volume": safe_float(latest.volume),
                "evaluated_at": now_iso(),
            }

    except Exception as exc:
        return {
            "error": str(exc),
            "symbol": symbol,
            "evaluated_at": now_iso(),
        }