"""认证：登录、注册、当前用户、资料与头像"""
from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.orm import Session

from auth import (
    complete_login,
    create_access_token,
    get_current_user,
    hash_password,
    user_public_dict,
    verify_password,
)
from database import get_db
from models import User

router = APIRouter(prefix="/auth", tags=["auth"])

# 与 main 挂载的静态目录一致：backend/uploads/avatars
_UPLOADS_ROOT = Path(__file__).resolve().parent.parent / "uploads"
_AVATAR_DIR = _UPLOADS_ROOT / "avatars"
_ALLOWED_IMAGE_CT = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp", "image/gif": ".gif"}
_MAX_AVATAR_BYTES = 2 * 1024 * 1024

_USERNAME_RE = re.compile(r"^[a-zA-Z0-9_\u4e00-\u9fff]{2,50}$")


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


class RegisterRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    username: str = Field(..., min_length=2, max_length=50)
    password: str = Field(..., min_length=6, max_length=128)


class UpdateProfileRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    username: str = Field(..., min_length=2, max_length=50)


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=6, max_length=128)


@router.post("/login")
def login(payload: LoginRequest, db: Session = Depends(get_db)) -> dict:
    return complete_login(db, payload.username, payload.password)


@router.post("/register")
def register(payload: RegisterRequest, db: Session = Depends(get_db)) -> dict:
    name = (payload.username or "").strip()
    if not _USERNAME_RE.match(name):
        raise HTTPException(
            status_code=400,
            detail="用户名 2～50 位，仅字母、数字、下划线或中文",
        )
    if db.query(User).filter(User.username == name).first():
        raise HTTPException(status_code=400, detail="用户名已存在")

    pwd = (payload.password or "").strip()
    if len(pwd) < 6:
        raise HTTPException(status_code=400, detail="密码至少 6 位")

    user = User(
        username=name,
        password_hash=hash_password(pwd),
        role="user",
        avatar_url=None,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token(user)
    return {"ok": True, "token": token, "user": user_public_dict(user)}


@router.post("/logout")
def logout(_user: User = Depends(get_current_user)) -> dict:
    return {"ok": True, "message": "已登出"}


@router.get("/me")
def me(user: User = Depends(get_current_user)) -> dict:
    u = user_public_dict(user)
    return {"ok": True, **u, "user": u}


@router.patch("/profile")
def update_profile(
    payload: UpdateProfileRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    name = (payload.username or "").strip()
    if not _USERNAME_RE.match(name):
        raise HTTPException(
            status_code=400,
            detail="用户名 2～50 位，仅字母、数字、下划线或中文",
        )
    other = db.query(User).filter(User.username == name, User.id != user.id).first()
    if other:
        raise HTTPException(status_code=400, detail="该用户名已被占用")

    user.username = name
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"ok": True, "user": user_public_dict(user)}


@router.post("/password")
def change_password(
    payload: ChangePasswordRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    if not verify_password(payload.current_password, user.password_hash):
        raise HTTPException(status_code=400, detail="当前密码错误")
    np = (payload.new_password or "").strip()
    if len(np) < 6:
        raise HTTPException(status_code=400, detail="新密码至少 6 位")

    user.password_hash = hash_password(np)
    db.add(user)
    db.commit()
    return {"ok": True, "message": "密码已更新"}


@router.post("/avatar")
async def upload_avatar(
    file: UploadFile = File(...),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> dict:
    ct = (file.content_type or "").split(";")[0].strip().lower()
    ext = _ALLOWED_IMAGE_CT.get(ct)
    if not ext:
        raise HTTPException(status_code=400, detail="仅支持 PNG、JPEG、WebP、GIF")

    raw = await file.read()
    if len(raw) > _MAX_AVATAR_BYTES:
        raise HTTPException(status_code=400, detail="图片不超过 2MB")

    _AVATAR_DIR.mkdir(parents=True, exist_ok=True)
    # 删除该用户旧头像（任意后缀）
    for p in _AVATAR_DIR.glob(f"{user.id}.*"):
        try:
            p.unlink()
        except OSError:
            pass

    rel = f"/uploads/avatars/{user.id}{ext}"
    dest = _AVATAR_DIR / f"{user.id}{ext}"
    dest.write_bytes(raw)

    user.avatar_url = rel
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"ok": True, "avatar_url": rel, "user": user_public_dict(user)}
