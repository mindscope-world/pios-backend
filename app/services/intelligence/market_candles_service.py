from __future__ import annotations

from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.trading_view_service import compute_tradingview_payload


async def compute_market_candles(*, symbol: str, db: AsyncSession, resolution: str = "1", limit: int = 200) -> dict[str, Any]:
    """Return TradingView-style OHLCV payload for the given symbol.

    Wraps the existing trading_view_service and normalises the result.
    Never raises; returns an error dict on failure.
    """
    try:
        payload = await compute_tradingview_payload(symbol=symbol, resolution=resolution, db=db, limit=limit)
        return payload
    except Exception as exc:  # defensive, compute_tradingview_payload already catches, but keep safe
        return {"error": str(exc), "symbol": symbol}
