import os
from datetime import datetime, timedelta
from jose import JWTError, jwt
import bcrypt
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

import database  # needed for authenticate_user

# CONFIG
SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "super-secret-key-for-dev")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 43200  # 30 Days

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# ==========================================
# DIRECT NATIVE BCRYPT IMPLEMENTATION
# ==========================================
def _safe_bytes(password: str) -> bytes:
    """Safely convert to bytes and strictly cut at 72 to prevent crashes."""
    if not password:
        return b""
    return password.encode("utf-8")[:72]


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(
            _safe_bytes(plain_password), hashed_password.encode("utf-8")
        )
    except Exception:
        return False


def get_password_hash(password: str) -> str:
    salt = bcrypt.gensalt()
    hashed_bytes = bcrypt.hashpw(_safe_bytes(password), salt)
    return hashed_bytes.decode("utf-8")
# ==========================================


def authenticate_user(username: str, password: str):
    """
    Looks up the user in the database and verifies their password.
    Returns the user dict on success, or None on failure.
    This function was missing — it is the root cause of the 422 error
    because app.py called it but it did not exist in this module.
    """
    user = database.get_user(username)
    if not user:
        return None
    if not verify_password(password, user["hashed_password"]):
        return None
    return user


def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_user_from_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except Exception:
        return None


async def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    user = get_user_from_token(token)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )
    return user