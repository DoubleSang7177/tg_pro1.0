from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_admin
from database import get_db
from models import User


router = APIRouter(prefix="/users", tags=["users"])


class UpdateUserRoleRequest(BaseModel):
    role: str = Field(..., pattern="^(admin|user)$")


@router.get("")
def list_users(_admin: User = Depends(require_admin), db: Session = Depends(get_db)) -> dict:
    users = db.query(User).order_by(User.id.asc()).all()
    return {
        "ok": True,
        "users": [
            {
                "id": u.id,
                "username": u.username,
                "role": u.role,
                "created_at": u.created_at.isoformat() if u.created_at else None,
            }
            for u in users
        ],
    }


@router.put("/{user_id}/role")
def update_user_role(
    user_id: int,
    payload: UpdateUserRoleRequest,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    target = db.query(User).filter(User.id == user_id).first()
    if target is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    if target.id == admin.id and payload.role != "admin":
        raise HTTPException(status_code=400, detail="不能取消自己的管理员权限")

    target.role = payload.role
    db.add(target)
    db.commit()
    return {"ok": True, "id": target.id, "role": target.role}
