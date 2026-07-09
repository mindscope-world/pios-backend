# app/api/v1/endpoints/mt5_bridge.py
"""
MT5 EA bridge WebSocket endpoint.

The MT5 Expert Advisor connects here -- one connection per broker_id -- and
stays connected for the life of the terminal session. An EA can't hold a
user JWT, so it authenticates with the shared bridge secret set as the
`passphrase` field when the MT5 broker connection was created (encrypted at
rest in Broker.credentials_enc, same as every other broker's credentials).
"""
import logging
import uuid

from fastapi import APIRouter, WebSocket
from sqlalchemy import select

from app.core.security import decrypt_credentials
from app.db.session import AsyncSessionLocal
from app.models.all_models import Broker
from app.services.brokers.mt5.adapter import mt5_registry

log = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws/mt5/{broker_id}")
async def mt5_bridge_endpoint(ws: WebSocket, broker_id: uuid.UUID):
    await ws.accept()

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Broker).where(
                Broker.id == broker_id,
                Broker.broker_type == "MT5",
                Broker.is_active == True,  # noqa: E712
            )
        )
        broker = result.scalar_one_or_none()

    if broker is None:
        await ws.close(code=4404, reason="Unknown MT5 broker connection")
        return

    try:
        import json
        expected_token = json.loads(decrypt_credentials(broker.credentials_enc)).get("passphrase")
    except Exception:
        expected_token = None

    try:
        handshake = await ws.receive_json()
    except Exception:
        await ws.close(code=1008, reason="Expected HANDSHAKE frame")
        return

    if not expected_token or handshake.get("type") != "HANDSHAKE" or handshake.get("token") != expected_token:
        await ws.close(code=1008, reason="Invalid or missing bridge token")
        return

    conn = await mt5_registry.register(str(broker_id), ws)
    await ws.send_json({"type": "HANDSHAKE_ACK"})

    try:
        await conn.receive_loop()
    finally:
        mt5_registry.unregister(str(broker_id))
