# app/core/deps.py
from uuid import UUID
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.security import decode_token
from app.db.session import get_db
from app.models.all_models import User

# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")
security = HTTPBearer()

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
) -> User:
    token = credentials.credentials

    credentials_exc = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = decode_token(token)
        user_id = payload.get("sub")
        if user_id is None:
            raise credentials_exc
    except JWTError:
        raise credentials_exc

    result = await db.execute(select(User).where(User.id == UUID(user_id)))
    user = result.scalar_one_or_none()

    if user is None or not user.is_active:
        raise credentials_exc

    return user

async def get_user_by_id(
    user_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> User:  
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user

# async def get_current_user(
#     token: str = Depends(oauth2_scheme),
#     db: AsyncSession = Depends(get_db),
# ) -> User:
#     credentials_exc = HTTPException(
#         status_code=status.HTTP_401_UNAUTHORIZED,
#         detail="Could not validate credentials",
#         headers={"WWW-Authenticate": "Bearer"},
#     )
#     try:
#         payload = decode_token(token)
#         user_id: str | None = payload.get("sub")
#         if user_id is None:
#             raise credentials_exc
#     except JWTError:
#         raise credentials_exc

#     result = await db.execute(select(User).where(User.id == UUID(user_id)))
#     user = result.scalar_one_or_none()
#     if user is None or not user.is_active:
#         raise credentials_exc
#     return user

async def get_system_user(db: AsyncSession) -> User | None:
    result = await db.execute(
        select(User).where(User.email == "admin@pios.com")
    )
    return result.scalar_one_or_none()


def require_roles(*roles: str):
    async def checker(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{current_user.role}' cannot access this resource",
            )
        return current_user
    return checker


# Role shortcuts used across endpoints
require_admin      = require_roles("admin")
require_trader     = require_roles("admin", "trader")
require_quant      = require_roles("admin", "quant")
require_audit      = require_roles("admin", "compliance", "quant")
require_trade_exec = require_roles("admin", "trader")
