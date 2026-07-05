import asyncio
import logging
from datetime import datetime, timezone
from typing import Iterable

import ccxt.pro as ccxt          # ← ccxt.pro, not ccxt.async_support
                                  #   async_support has watch* stubs that all return None;
                                  #   ccxt.pro has the actual WebSocket implementations.

from app.providers.base import BaseProvider

log = logging.getLogger(__name__)


class CryptoProvider(BaseProvider):
    """
    Real-time crypto trade feed via ccxt.pro WebSocket.

    Strategy chosen per exchange at connect time:
      watchTradesForSymbols  — preferred (one subscription for all symbols)
      watchTrades per symbol — fallback (one task per symbol, shared WS connection)
    """

    def __init__(self, exchange_ids: Iterable[str], symbols):
        super().__init__(symbols)
        if isinstance(exchange_ids, str):
            exchange_ids = [exchange_ids]
        self.exchange_ids = [e.lower() for e in exchange_ids]

    async def start(self, publish):
        while True:
            for exchange_id in self.exchange_ids:
                exchange = None
                try:
                    exchange_class = getattr(ccxt, exchange_id, None)
                    if not exchange_class:
                        log.error(f"Unknown exchange: {exchange_id}")
                        continue

                    exchange = exchange_class({
                        "enableRateLimit": True,
                        "options": {"defaultType": "spot"},
                    })
                    await exchange.load_markets()

                    valid = {
                        s.symbol: s
                        for s in self.symbols
                        if s.symbol in exchange.markets
                    }
                    if not valid:
                        log.warning(
                            f"{exchange_id}: no valid symbols from "
                            f"{[s.symbol for s in self.symbols]} — trying next exchange"
                        )
                        continue

                    tickers = list(valid.keys())
                    log.info(f"{exchange_id}: streaming {len(tickers)} symbol(s): {tickers}")

                    if exchange.has.get("watchTradesForSymbols"):
                        await self._watch_multi(exchange, tickers, valid, publish, exchange_id)
                    elif exchange.has.get("watchTrades"):
                        log.info(
                            f"{exchange_id}: watchTradesForSymbols not supported — "
                            "using per-symbol watchTrades tasks"
                        )
                        await self._watch_per_symbol(exchange, tickers, valid, publish, exchange_id)
                    else:
                        log.error(
                            f"{exchange_id}: neither watchTradesForSymbols nor watchTrades "
                            "supported — trying next exchange"
                        )
                        continue

                except asyncio.CancelledError:
                    log.info(f"{exchange_id}: cancelled — shutting down")
                    raise
                except ccxt.NetworkError as e:
                    log.warning(f"{exchange_id} network error: {e} — trying next exchange")
                    await asyncio.sleep(3)
                except ccxt.ExchangeError as e:
                    log.error(f"{exchange_id} exchange error: {e} — trying next exchange")
                    await asyncio.sleep(10)
                except Exception as e:
                    log.error(f"{exchange_id} provider crashed: {e} — trying next exchange")
                    await asyncio.sleep(5)
                finally:
                    if exchange:
                        try:
                            await exchange.close()
                            exchange = None
                        except Exception as e:
                            log.warning(f"{exchange_id}: error closing exchange: {e}")

            log.warning("All configured crypto exchanges failed. Retrying in 10s.")
            await asyncio.sleep(10)

    # ─────────────────────────────────────────────────────────────────────────
    # Watch paths
    # ─────────────────────────────────────────────────────────────────────────

    async def _watch_multi(self, exchange, tickers, valid, publish, exchange_id):
        """Single subscription — exchange supports watchTradesForSymbols."""
        while True:
            trades = await exchange.watch_trades_for_symbols(tickers)
            for t in trades:
                await _emit(t, valid, publish, exchange_id)

    async def _watch_per_symbol(self, exchange, tickers, valid, publish, exchange_id):
        """
        One task per symbol, all sharing the same ccxt.pro exchange instance.
        ccxt.pro multiplexes watch_* calls over a single WS connection internally.
        If any task raises, the whole group is cancelled and the outer loop retries.
        """
        async def _watch_one(symbol: str):
            while True:
                trades = await exchange.watch_trades(symbol)
                for t in trades:
                    await _emit(t, valid, publish, exchange_id)

        tasks = [
            asyncio.create_task(_watch_one(s), name=f"{exchange_id}:{s}")
            for s in tickers
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            raise
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


# ─────────────────────────────────────────────────────────────────────────────
# Shared emit helper
# ─────────────────────────────────────────────────────────────────────────────

async def _emit(t: dict, valid: dict, publish, exchange_id: str) -> None:
    sym = valid.get(t["symbol"])
    if not sym:
        return

    raw_side = t.get("takerSide") or t.get("side") or ""
    side = (
        "BUY"  if raw_side in ("buy",  "BUY",  "BID") else
        "SELL" if raw_side in ("sell", "SELL", "ASK") else
        "neutral"
    )

    await publish({
        "symbol_id":   sym.id,
        "symbol":      t["symbol"],
        "price":       float(t["price"]),
        "volume":      float(t["amount"]),
        "side":        side,
        "asset_class": "crypto",
        "source":      exchange_id,
        "time":        t.get("datetime") or datetime.now(timezone.utc).isoformat(),
    })