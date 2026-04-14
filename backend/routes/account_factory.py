from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_user_or_admin
from database import get_db
from models import User
from services.account_factory.factory_runner import get_runtime_snapshot, start_factory_task, stop_factory_task
from services.account_factory.register_service import complete_factory_login, list_factory_accounts, send_factory_code
from services.account_factory.sms_provider import normalize_strategy

router = APIRouter(prefix="/factory", tags=["account-factory"])


class FactoryStartBody(BaseModel):
    countries: list[str] = Field(default_factory=lambda: ["ID", "PH", "BR", "IN"])
    strategy: str = Field("balanced")
    max_retries: int = Field(3, ge=1, le=10)


class FactorySendCodeBody(BaseModel):
    phone: str = Field(..., min_length=5)
    country: str = Field("ID", min_length=2, max_length=8)
    strategy: str = Field("balanced")


class FactoryCompleteBody(BaseModel):
    account_id: int = Field(..., ge=1)
    phone: str = Field(..., min_length=5)
    code: str = Field("", max_length=32)
    phone_code_hash: str = Field("")
    password: str | None = Field(None, max_length=256)


@router.get("/accounts")
def factory_accounts(
    db: Session = Depends(get_db),
    user: User = Depends(require_user_or_admin),
) -> dict:
    owner_filter = None if user.role == "admin" else user.id
    rows = list_factory_accounts(db, owner_filter)
    return {
        "items": [
            {
                "id": r.id,
                "phone": r.phone,
                "country": r.country,
                "status": r.status,
                "session_path": r.session_path,
                "warmup_until": r.warmup_until.isoformat() if r.warmup_until else None,
                "fail_reason": r.fail_reason,
                "source": r.source,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]
    }


@router.get("/runtime")
def factory_runtime(user: User = Depends(require_user_or_admin)) -> dict:
    _ = user
    snap = get_runtime_snapshot()
    return {"ok": True, **snap}


@router.post("/runtime/start")
def factory_runtime_start(
    body: FactoryStartBody,
    user: User = Depends(require_user_or_admin),
) -> dict:
    _ = user
    countries = [str(x or "").strip().upper() for x in body.countries if str(x or "").strip()]
    if not countries:
        countries = ["ID", "PH", "BR", "IN"]
    snap = start_factory_task(countries=countries, strategy=normalize_strategy(body.strategy), max_retries=body.max_retries)
    return {"ok": True, **snap}


@router.post("/runtime/stop")
def factory_runtime_stop(user: User = Depends(require_user_or_admin)) -> dict:
    _ = user
    snap = stop_factory_task()
    return {"ok": True, **snap}


@router.post("/register/send_code")
async def factory_send_code(
    body: FactorySendCodeBody,
    db: Session = Depends(get_db),
    user: User = Depends(require_user_or_admin),
) -> dict:
    try:
        out = await send_factory_code(
            db,
            owner_id=user.id,
            phone=body.phone.strip(),
            country=body.country.strip().upper(),
            strategy=body.strategy,
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    return out


@router.post("/register/complete")
async def factory_complete(
    body: FactoryCompleteBody,
    db: Session = Depends(get_db),
    user: User = Depends(require_user_or_admin),
) -> dict:
    try:
        out = await complete_factory_login(
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

