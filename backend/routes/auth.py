from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import complete_login, get_current_user
from database import get_db
from models import User


router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


@router.post("/login")
def login(payload: LoginRequest, db: Session = Depends(get_db)) -> dict:
    return complete_login(db, payload.username, payload.password)


@router.post("/logout")
def logout(_user: User = Depends(get_current_user)) -> dict:
    return {"ok": True, "message": "已登出"}


@router.get("/me")
def me(user: User = Depends(get_current_user)) -> dict:
    return {
        "ok": True,
        "username": user.username,
        "role": user.role,
        "user": {"username": user.username, "role": user.role},
    }
