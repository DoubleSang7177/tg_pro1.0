from __future__ import annotations

import hashlib
import os
from datetime import datetime, timedelta, timezone

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from database import get_db
from models import User

security = HTTPBearer(auto_error=False)

JWT_SECRET = os.environ.get(
    "JWT_SECRET",
    "tg-pro-jwt-secret-minimum-32-characters-long-for-hs256",
)
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_DAYS = int(os.environ.get("JWT_EXPIRE_DAYS", "7"))


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_password(password: str, hashed: str) -> bool:
    return hash_password(password) == hashed


def create_access_token(user: User) -> str:
    now = datetime.now(timezone.utc)
    exp = now + timedelta(days=max(1, JWT_EXPIRE_DAYS))
    payload = {
        "sub": str(user.id),
        "typ": "access",
        "iat": int(now.timestamp()),
        "exp": exp,
    }
    raw = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return raw if isinstance(raw, str) else raw.decode("utf-8")


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
    db: Session = Depends(get_db),
) -> User:
    if credentials is None or not credentials.credentials:
        raise HTTPException(status_code=401, detail="未登录，请携带 Bearer Token")

    token = credentials.credentials.strip()

    try:
        data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if data.get("typ") != "access":
            raise ValueError("invalid token type")
        uid = int(data.get("sub", 0))
        user = db.query(User).filter(User.id == uid).first()
        if user is None:
            raise HTTPException(status_code=401, detail="用户不存在")
        return user
    except HTTPException:
        raise
    except Exception:
        user = db.query(User).filter(User.token == token).first()
        if user is None:
            raise HTTPException(status_code=401, detail="无效或过期的令牌")
        return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可执行此操作")
    return user


def require_user_or_admin(user: User = Depends(get_current_user)) -> User:
    if user.role not in {"user", "admin"}:
        raise HTTPException(status_code=403, detail="无权限")
    return user


def get_current_user_optional(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
    db: Session = Depends(get_db),
) -> User | None:
    """无 Token 或 Token 无效时返回 None，供游客只读浏览。"""
    if credentials is None or not credentials.credentials:
        return None
    token = credentials.credentials.strip()
    try:
        data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if data.get("typ") != "access":
            return None
        uid = int(data.get("sub", 0))
        user = db.query(User).filter(User.id == uid).first()
        return user
    except Exception:
        user = db.query(User).filter(User.token == token).first()
        return user


def user_public_dict(user: User) -> dict:
    return {
        "id": user.id,
        "username": user.username,
        "role": user.role,
        "avatar_url": getattr(user, "avatar_url", None) or None,
    }


def complete_login(db: Session, username: str, password: str) -> dict:
    name = (username or "").strip()
    user = db.query(User).filter(User.username == name).first()
    if user is None or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    token = create_access_token(user)
    return {
        "ok": True,
        "token": token,
        "user": user_public_dict(user),
    }
