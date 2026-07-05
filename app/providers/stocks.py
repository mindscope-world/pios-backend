# app/providers/alpaca.py
"""
Alpaca real-time stock trade feed — REST polling only.

Polls GET /v2/stocks/trades/latest on a fixed interval.
No SDK, no WebSocket, no extra dependencies beyond aiohttp.

Auth headers on every request:
  APCA-API-KEY-ID:     <api_key>
  APCA-API-SECRET-KEY: <secret>

Environments:
  paper → IEX feed (free, works on paper accounts)
  live  → SIP feed (requires paid Unlimited data plan)

Rate limits (free tier): 200 req/min.
At _REST_POLL_INTERVAL = 1.0s we consume 60 req/min — safely within limits.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Awaitable

import aiohttp

from app.providers.base import BaseProvider

log = logging.getLogger(__name__)

_REST_DATA          = "https://data.alpaca.markets"
_LATEST_TRADES_PATH = "/v2/stocks/trades/latest"   # ?symbols=AAPL,TSLA

_REST_POLL_INTERVAL = 1.0   # seconds between polls


class AlpacaProvider(BaseProvider):
    """
    Stock trade feed via Alpaca REST polling.

    Args:
        symbols:  list of ticker strings, e.g. ["AAPL", "TSLA"]
        api_key:  Alpaca API Key ID        (APCA-API-KEY-ID)
        secret:   Alpaca Secret Key        (APCA-API-SECRET-KEY)
        paper:    True  → paper account, IEX feed (default)
                  False → live account, SIP feed (paid plan required)
    """

    def __init__(
        self,
        symbols:  list[str],
        api_key:  str,
        secret:   str,
        paper:    bool = True,
    ):
        super().__init__(symbols)
        self.api_key     = api_key
        self.secret      = secret
        self.paper       = paper
        self._feed_label = "IEX (paper)" if paper else "SIP (live)"
        self._headers    = {
            "APCA-API-KEY-ID":     self.api_key,
            "APCA-API-SECRET-KEY": self.secret,
            "Accept":              "application/json",
        }

    async def start(self, publish: Callable[[dict], Awaitable[None]]) -> None:
        if not self.api_key or not self.secret:
            log.warning("Alpaca credentials not set — provider disabled")
            return

        await self._poll(publish)

    # ─────────────────────────────────────────────────────────────────────────
    # REST poll loop
    # ─────────────────────────────────────────────────────────────────────────

    async def _poll(self, publish) -> None:
        url           = _REST_DATA + _LATEST_TRADES_PATH
        symbols_param = ",".join(self.symbols)
        backoff       = 5
        seen: dict[str, str] = {}   # symbol → last trade id/timestamp (dedup)

        log.info(
            f"Alpaca REST: polling {len(self.symbols)} symbol(s) "
            f"every {_REST_POLL_INTERVAL}s via {self._feed_label}"
        )

        async with aiohttp.ClientSession(headers=self._headers) as session:
            while True:
                try:
                    async with session.get(
                        url,
                        params  = {"symbols": symbols_param},
                        timeout = aiohttp.ClientTimeout(total=10),
                    ) as resp:

                        if resp.status == 401:
                            log.error("Alpaca 401 — invalid API key/secret. Not retrying.")
                            return
                        if resp.status == 403:
                            log.error(
                                "Alpaca 403 — feed not available on this plan. "
                                "Use paper=True for IEX (free) data."
                            )
                            return
                        if resp.status == 429:
                            log.warning(f"Alpaca 429 rate limited — backing off {backoff}s")
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 60)
                            continue
                        if resp.status != 200:
                            log.error(f"Alpaca HTTP {resp.status} — retrying in {backoff}s")
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 60)
                            continue

                        backoff = 5
                        data    = await resp.json()

                        # {"trades": {"AAPL": {trade}, "TSLA": {trade}}}
                        for symbol, trade in data.get("trades", {}).items():
                            trade_id = trade.get("i") or trade.get("t")
                            if seen.get(symbol) == trade_id:
                                continue   # no new trade since last poll
                            seen[symbol] = trade_id

                            await publish(_trade_payload(
                                symbol     = symbol,
                                price      = float(trade["p"]),
                                volume     = float(trade["s"]),
                                conditions = trade.get("c"),
                                timestamp  = trade.get("t"),
                                source     = "alpaca_rest",
                            ))

                except asyncio.CancelledError:
                    log.info("Alpaca REST poll: cancelled — shutting down")
                    raise
                except aiohttp.ClientConnectorError as e:
                    log.error(f"Alpaca connector error: {e} — retrying in {backoff}s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue
                except Exception as e:
                    log.error(f"Alpaca unexpected error: {e} — retrying in {backoff}s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue

                await asyncio.sleep(_REST_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _trade_payload(
    symbol:     str,
    price:      float,
    volume:     float,
    conditions: list | str | None,
    timestamp:  object,
    source:     str,
) -> dict:
    if isinstance(timestamp, datetime):
        ts = timestamp.isoformat()
    elif isinstance(timestamp, str):
        ts = timestamp
    else:
        ts = datetime.now(timezone.utc).isoformat()

    return {
        "symbol":      symbol,
        "price":       price,
        "volume":      volume,
        "side":        "neutral",   # Alpaca condition codes don't encode aggressor side
        "asset_class": "stock",
        "source":      source,
        "time":        ts,
    }