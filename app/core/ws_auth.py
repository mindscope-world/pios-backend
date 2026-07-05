import time
from jose import jwt, JWTError, ExpiredSignatureError
from fastapi import WebSocket, WebSocketException, status
from typing import Optional

from app.core.config import settings


def _extract_token(ws: WebSocket) -> Optional[str]:
    token = ws.query_params.get("token")
    if token:
        return token

    auth_header = ws.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        return auth_header.split(" ")[1]

    return None


def _decode_token(token: str) -> dict:
    try:
        payload = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
        )

        exp = payload.get("exp")
        if exp and exp < time.time():
            raise ExpiredSignatureError

        return payload

    except ExpiredSignatureError:
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Token expired",
        )
    except JWTError:
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Invalid token",
        )


async def get_ws_user(ws: WebSocket) -> str:
    token = _extract_token(ws)
    print(f"🔍 Extracted token: {token[:20] if token else 'MISSING'}...")
    if not token:
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Missing authentication token",
        )

    payload = _decode_token(token)
    user_id = payload.get("sub")

    if not user_id:
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason="Invalid token payload",
        )

    return str(user_id)