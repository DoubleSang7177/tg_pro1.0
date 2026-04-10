from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import get_current_user, issue_token, verify_password
from database import get_db
from models import User


router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


@router.post("/login")
def login(payload: LoginRequest, db: Session = Depends(get_db)) -> dict:
    user = db.query(User).filter(User.username == payload.username).first()
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    token = issue_token()
    user.token = token
    db.add(user)
    db.commit()
    return {"ok": True, "token": token, "role": user.role, "username": user.username}


@router.get("/me")
def me(user: User = Depends(get_current_user)) -> dict:
    return {"ok": True, "username": user.username, "role": user.role}
