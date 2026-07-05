# app/api/v1/endpoints/auth.py
import hashlib
import uuid
from datetime import datetime, timedelta, timezone

import pyotp
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.deps import get_current_user
from app.core.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    encrypt_credentials,
    decrypt_credentials,
    hash_password,
    verify_password,
)
from app.db.session import get_db
from app.models.all_models import User, UserSession
from app.schemas.all_schemas import (
    LoginRequest,
    MFASetupResponse,
    MFAVerifyRequest,
    MessageResponse,
    RefreshRequest,
    RegisterRequest,
    TokenPair,
    UserOut,
)
from app.services.audit_service import write_audit

router = APIRouter(prefix="/auth", tags=["auth"])

MAX_FAILED = 5
LOCK_MINUTES = 30


@router.post("/login", response_model=TokenPair)
async def login(
    data: LoginRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(User).where(User.email == data.email))
    user = result.scalar_one_or_none()

    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    # Account lock check
    if user.locked_until and datetime.now(timezone.utc) < user.locked_until:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Account locked — too many failed attempts")

    if not verify_password(data.password, user.password_hash):
        user.failed_logins += 1
        if user.failed_logins >= MAX_FAILED:
            user.locked_until = datetime.now(timezone.utc) + timedelta(minutes=LOCK_MINUTES)
        await db.commit()
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    # MFA check
    if user.mfa_enabled:
        if not data.mfa_code:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="MFA code required")
        secret = decrypt_credentials(user.mfa_secret_enc)
        totp = pyotp.TOTP(secret)
        if not totp.verify(data.mfa_code, valid_window=1):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid MFA code")

    # Reset failed logins
    user.failed_logins = 0
    user.locked_until = None
    user.last_login_at = datetime.now(timezone.utc)
    user.last_login_ip = request.client.host if request.client else None

    # Create tokens
    extra = {"role": user.role, "email": user.email}
    access_token  = create_access_token(str(user.id), extra=extra)
    refresh_token = create_refresh_token(str(user.id))

    # Store hashed refresh token
    session = UserSession(
        user_id=user.id,
        refresh_token_hash=hashlib.sha256(refresh_token.encode()).hexdigest(),
        ip_address=user.last_login_ip,
        device_info={"user_agent": request.headers.get("user-agent", "")},
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db.add(session)

    await write_audit(
        db, "USER_LOGIN", "user", str(user.id),
        actor_id=user.id, actor_email=user.email,
        ip_address=user.last_login_ip,
    )
    await db.commit()

    return TokenPair(
        access_token=access_token,
        refresh_token=refresh_token,
        user=UserOut.model_validate(user),
    )

@router.post("/register", response_model=TokenPair, status_code=status.HTTP_201_CREATED)
async def register(
    data: RegisterRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    # 1. Check if user already exists
    result = await db.execute(select(User).where(User.email == data.email))
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="Email already registered"
        )

    # 2. Create the new user object
    new_user = User(
        full_name=data.full_name,
        email=data.email,
        password_hash=hash_password(data.password), # Hash the password!
        is_active=True,
        role="trader", # Default role
        created_at=datetime.now(timezone.utc),
        last_login_at=datetime.now(timezone.utc),
        last_login_ip=request.client.host if request.client else None
    )
    
    db.add(new_user)
    await db.flush() # Flush to get the new_user.id

    # 3. Generate initial tokens (Auto-login after registration)
    extra = {"role": new_user.role, "email": new_user.email}
    access_token = create_access_token(str(new_user.id), extra=extra)
    refresh_token = create_refresh_token(str(new_user.id))

    # 4. Create initial session
    session = UserSession(
        user_id=new_user.id,
        refresh_token_hash=hashlib.sha256(refresh_token.encode()).hexdigest(),
        ip_address=new_user.last_login_ip,
        device_info={"user_agent": request.headers.get("user-agent", "")},
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db.add(session)

    # 5. Audit Log
    await write_audit(
        db, "USER_REGISTER", "user", str(new_user.id),
        actor_id=new_user.id, actor_email=new_user.email,
        ip_address=new_user.last_login_ip,
    )
    
    await db.commit()

    return TokenPair(
        access_token=access_token,
        refresh_token=refresh_token,
        user=UserOut.model_validate(new_user),
    )


@router.post("/refresh", response_model=TokenPair)
async def refresh(data: RefreshRequest, db: AsyncSession = Depends(get_db)):
    from jose import JWTError
    try:
        payload = decode_token(data.refresh_token)
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Invalid token type")
        user_id = payload["sub"]
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    token_hash = hashlib.sha256(data.refresh_token.encode()).hexdigest()
    sess_result = await db.execute(
        select(UserSession).where(
            UserSession.refresh_token_hash == token_hash,
            UserSession.revoked == False,  # noqa: E712
        )
    )
    session = sess_result.scalar_one_or_none()
    if not session:
        raise HTTPException(status_code=401, detail="Session revoked or not found")

    user_result = await db.execute(select(User).where(User.id == uuid.UUID(user_id)))
    user = user_result.scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found")

    # Rotate: revoke old, issue new
    session.revoked = True
    new_access  = create_access_token(str(user.id), extra={"role": user.role, "email": user.email})
    new_refresh = create_refresh_token(str(user.id))

    new_session = UserSession(
        user_id=user.id,
        refresh_token_hash=hashlib.sha256(new_refresh.encode()).hexdigest(),
        ip_address=session.ip_address,
        expires_at=datetime.now(timezone.utc) + timedelta(days=7),
    )
    db.add(new_session)
    await db.commit()

    return TokenPair(access_token=new_access, refresh_token=new_refresh, user=UserOut.model_validate(user))


@router.post("/logout", response_model=MessageResponse)
async def logout(
    data: RefreshRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    token_hash = hashlib.sha256(data.refresh_token.encode()).hexdigest()
    result = await db.execute(select(UserSession).where(UserSession.refresh_token_hash == token_hash))
    session = result.scalar_one_or_none()
    if session:
        session.revoked = True
        await db.commit()
    return MessageResponse(message="Logged out")


@router.post("/mfa/setup", response_model=MFASetupResponse)
async def mfa_setup(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if current_user.mfa_enabled:
        raise HTTPException(status_code=409, detail="MFA already enabled")
    secret = pyotp.random_base32()
    totp = pyotp.TOTP(secret)
    qr_uri = totp.provisioning_uri(name=current_user.email, issuer_name="PiOS")
    current_user.mfa_secret_enc = encrypt_credentials(secret)
    await db.commit()
    return MFASetupResponse(secret=secret, qr_uri=qr_uri)


@router.post("/mfa/verify", response_model=MessageResponse)
async def mfa_verify(
    data: MFAVerifyRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not current_user.mfa_secret_enc:
        raise HTTPException(status_code=400, detail="Call /mfa/setup first")
    secret = decrypt_credentials(current_user.mfa_secret_enc)
    totp = pyotp.TOTP(secret)
    if not totp.verify(data.code, valid_window=1):
        raise HTTPException(status_code=400, detail="Invalid code")
    current_user.mfa_enabled = True
    await write_audit(db, "MFA_ENABLED", "user", str(current_user.id),
                      actor_id=current_user.id, actor_email=current_user.email)
    await db.commit()
    return MessageResponse(message="MFA enabled")


@router.get("/me", response_model=UserOut)
async def me(current_user: User = Depends(get_current_user)):
    return current_user
