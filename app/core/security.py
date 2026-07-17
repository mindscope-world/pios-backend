# app/core/security.py
import hashlib
import base64
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from jose import jwt
from passlib.context import CryptContext
from cryptography.fernet import Fernet

from app.core.config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ── Passwords ─────────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


# ── JWT ───────────────────────────────────────────────────────────────────────

def create_access_token(subject: str | Any, extra: dict | None = None) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    data = {"sub": str(subject), "exp": expire, "type": "access"}
    if extra:
        data.update(extra)
    return jwt.encode(data, settings.SECRET_KEY, algorithm=settings.ALGORITHM)

def create_refresh_token(subject: str | Any) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    # jti: exp has second resolution, so without it two logins for the same
    # user inside one second mint byte-identical tokens — whose sha256 then
    # collides with user_sessions' unique refresh_token_hash (login 500)
    return jwt.encode(
        {"sub": str(subject), "exp": expire, "type": "refresh", "jti": uuid.uuid4().hex},
        settings.SECRET_KEY, algorithm=settings.ALGORITHM,
    )

def decode_token(token: str) -> dict:
    return jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])


# ── Broker credential encryption (Fernet / AES-128-CBC) ──────────────────────

def _fernet() -> Fernet:
    raw = settings.ENCRYPTION_KEY.encode()[:32].ljust(32, b"0")
    return Fernet(base64.urlsafe_b64encode(raw))

def encrypt_credentials(plaintext: str) -> str:
    return _fernet().encrypt(plaintext.encode()).decode()

def decrypt_credentials(ciphertext: str) -> str:
    return _fernet().decrypt(ciphertext.encode()).decode()


# ── Audit chain hashing ───────────────────────────────────────────────────────

def compute_record_hash(data: str, prev_hash: str | None) -> str:
    combined = f"{data}:{prev_hash or ''}"
    return hashlib.sha256(combined.encode()).hexdigest()[:16]
