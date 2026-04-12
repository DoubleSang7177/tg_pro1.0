from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import get_current_user_optional, require_admin, require_user_or_admin
from database import get_db
from models import AccountPath, Setting, User


router = APIRouter(prefix="/settings", tags=["settings"])


class UpdateSettingRequest(BaseModel):
    key: str = Field(..., min_length=1)
    value: str = Field(..., min_length=1)


class AccountPathRequest(BaseModel):
    path: str = Field(..., min_length=1)


class DeleteAccountPathRequest(BaseModel):
    id: int


@router.post("")
def update_setting(
    payload: UpdateSettingRequest,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    row = db.query(Setting).filter(Setting.key == payload.key).first()
    if row is None:
        row = Setting(key=payload.key, value=payload.value)
    else:
        row.value = payload.value
    db.add(row)
    db.commit()
    return {"ok": True, "key": payload.key, "value": payload.value}


@router.get("")
def list_settings(_admin: User = Depends(require_admin), db: Session = Depends(get_db)) -> dict:
    rows = db.query(Setting).order_by(Setting.id.desc()).all()
    return {"ok": True, "settings": [{"key": r.key, "value": r.value} for r in rows]}


@router.get("/account-paths")
def list_account_paths(_user: User | None = Depends(get_current_user_optional), db: Session = Depends(get_db)) -> dict:
    rows = db.query(AccountPath).order_by(AccountPath.id.desc()).all()
    return {
        "ok": True,
        "items": [{"id": r.id, "path": r.path, "created_at": r.created_at.isoformat() if r.created_at else None} for r in rows],
    }


@router.post("/account-paths")
def add_or_update_account_path(
    payload: AccountPathRequest,
    _user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    path_value = payload.path.strip()
    if not path_value:
        return {"ok": False, "message": "path 不能为空"}
    existed = db.query(AccountPath).filter(AccountPath.path == path_value).first()
    if existed is None:
        db.add(AccountPath(path=path_value))
        db.commit()
    return {"ok": True}


@router.delete("/account-paths")
def delete_account_path(
    payload: DeleteAccountPathRequest,
    _user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    row = db.query(AccountPath).filter(AccountPath.id == payload.id).first()
    if row is None:
        return {"ok": False, "message": "path 不存在"}
    db.delete(row)
    db.commit()
    return {"ok": True}
