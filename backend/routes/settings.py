from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_admin
from database import get_db
from models import Setting, User


router = APIRouter(prefix="/settings", tags=["settings"])


class UpdateSettingRequest(BaseModel):
    key: str = Field(..., min_length=1)
    value: str = Field(..., min_length=1)


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
