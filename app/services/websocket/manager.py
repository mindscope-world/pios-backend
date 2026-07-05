from fastapi import WebSocket
from collections import defaultdict
import asyncio

class WSManager:
    def __init__(self):
        # channel -> symbol -> user_id -> set of websockets
        self.connections = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))

    def _normalize(self, symbol: str) -> str:
        return (symbol or "").replace("/", "").upper()

    async def connect(self, ws: WebSocket, user_id: str, channel: str, symbol: str):
        symbol = self._normalize(symbol)
        self.connections[channel][symbol][user_id].add(ws)
        print(f"✅ Manager: user={user_id} subscribed to {channel}:{symbol}")
        print(f"   Active subs: { {ch: list(syms.keys()) for ch, syms in self.connections.items()} }")

    def disconnect(self, ws: WebSocket):
        for channel in self.connections.values():
            for symbol in channel.values():
                for users in symbol.values():
                    users.discard(ws)

    async def send_to_user(self, user_id: str, channel: str, symbol: str, data: dict):
        symbol = self._normalize(symbol)
        sockets = self.connections[channel][symbol].get(user_id, set())
        dead = set()
        for ws in sockets:
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)
        sockets -= dead

    async def broadcast_symbol(self, channel: str, symbol: str, data: dict):
        symbol = self._normalize(symbol)
        targets = self.connections[channel][symbol]

        if not targets:
            return

        dead = set()
        for user_id, sockets in targets.items():
            for ws in sockets:
                try:
                    await ws.send_json(data)
                except Exception:
                    dead.add(ws)

        # Cleanup dead connections
        for user_id, sockets in targets.items():
            sockets -= dead


manager = WSManager()