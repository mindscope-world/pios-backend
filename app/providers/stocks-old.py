import json
import websockets
from datetime import datetime, timezone
from app.providers.base import BaseProvider


class AlpacaProvider(BaseProvider):
    def __init__(self, symbols, api_key, secret):
        super().__init__(symbols)
        self.api_key = api_key
        self.secret = secret

    async def start(self, publish):
        ws_url = "wss://stream.data.alpaca.markets/v2/sip"

        async with websockets.connect(ws_url) as ws:

            await ws.send(json.dumps({
                "action": "auth",
                "key": self.api_key,
                "secret": self.secret
            }))

            await ws.send(json.dumps({
                "action": "subscribe",
                "trades": self.symbols
            }))

            while True:
                msg = json.loads(await ws.recv())

                for item in msg:
                    if item.get("T") != "t":
                        continue

                    await publish({
                        "symbol": item["S"],
                        "price": float(item["p"]),
                        "volume": float(item["s"]),
                        "asset_class": "stock",
                        "source": "alpaca",
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })