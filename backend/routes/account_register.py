from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_user_or_admin
from database import get_db
from models import User
from services.account_register_service import complete_register_login, send_register_code

router = APIRouter(prefix="/accounts/register", tags=["account-register"])


class RegisterSendCodeBody(BaseModel):
    phone: str = Field(..., min_length=5, description="手机号，含国家区号")


class RegisterCompleteBody(BaseModel):
    account_id: int = Field(..., ge=1)
    phone: str = Field(..., min_length=5)
    code: str = Field("", max_length=32)
    phone_code_hash: str = Field("")
    password: str | None = Field(None, max_length=256)


@router.post("/send_code")
async def register_send_code(
    body: RegisterSendCodeBody,
    db: Session = Depends(get_db),
    user: User = Depends(require_user_or_admin),
) -> dict:
    try:
        out = await send_register_code(db, user.id, body.phone.strip())
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    return out


@router.post("/complete")
async def register_complete(
    body: RegisterCompleteBody,
    db: Session = Depends(get_db),
    user: User = Depends(require_user_or_admin),
) -> dict:
    try:
        out = await complete_register_login(
            db,
            owner_id=user.id,
            account_id=body.account_id,
            phone=body.phone.strip(),
            code=body.code,
            phone_code_hash=(body.phone_code_hash or "").strip(),
            password=body.password,
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    return out
