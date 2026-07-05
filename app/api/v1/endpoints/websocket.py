from fastapi import APIRouter, WebSocket
from app.services.websocket.manager import manager
from app.core.ws_auth import get_ws_user

router = APIRouter()

@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    user_id = None
    try:
        user_id = await get_ws_user(ws)  # ✅ Only once
        if not user_id:
            await ws.close(code=1008, reason="Unauthorized")
            return

        while True:
            msg = await ws.receive_json()

            if msg["type"] == "subscribe":
                await manager.connect(
                    ws,
                    user_id,
                    msg["channel"],
                    msg["symbol"],
                )
                # ✅ Confirm subscription to client
                await ws.send_json({
                    "type": "subscribed",
                    "channel": msg["channel"],
                    "symbol": msg["symbol"],
                })

            elif msg["type"] == "ping":
                await ws.send_json({"type": "pong"})

    except Exception as e:
        print(f"🔌 WS disconnected user={user_id}: {e}")
    finally:
        manager.disconnect(ws)