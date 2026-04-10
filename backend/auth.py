from __future__ import annotations

import hashlib
import secrets

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.orm import Session

from database import get_db
from models import User


security = HTTPBearer(auto_error=False)


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_password(password: str, hashed: str) -> bool:
    return hash_password(password) == hashed


def issue_token() -> str:
    return secrets.token_hex(24)


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
    db: Session = Depends(get_db),
) -> User:
    if credentials is None or not credentials.credentials:
        raise HTTPException(status_code=401, detail="未登录，请携带 Bearer Token")

    token = credentials.credentials.strip()
    user = db.query(User).filter(User.token == token).first()
    if user is None:
        raise HTTPException(status_code=401, detail="无效令牌")
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可执行此操作")
    return user


def require_user_or_admin(user: User = Depends(get_current_user)) -> User:
    if user.role not in {"user", "admin"}:
        raise HTTPException(status_code=403, detail="无权限")
    return user
