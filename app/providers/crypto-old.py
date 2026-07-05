import ccxt.async_support as ccxt
from datetime import datetime, timezone
from app.providers.base import BaseProvider


class CryptoProvider(BaseProvider):
    def __init__(self, exchange_id, symbols):
        super().__init__(symbols)
        self.exchange_id = exchange_id

    async def start(self, publish):
        exchange_class = getattr(ccxt, self.exchange_id)
        exchange = exchange_class({"enableRateLimit": True})

        await exchange.load_markets()

        valid = {
            s.symbol: s
            for s in self.symbols
            if s.symbol in exchange.markets
        }

        tickers = list(valid.keys())

        while True:
            trades = await exchange.watch_trades_for_symbols(tickers)

            for t in trades:
                sym = valid.get(t["symbol"])
                if not sym:
                    continue

                await publish({
                    "symbol_id": sym.id,
                    "symbol": t["symbol"],
                    "price": float(t["price"]),
                    "volume": float(t["amount"]),
                    "asset_class": "crypto",
                    "source": self.exchange_id,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })