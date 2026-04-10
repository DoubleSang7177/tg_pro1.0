from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_user_or_admin
from database import get_db
from models import Group, User
from services.daily_reset import perform_daily_reset_if_needed


router = APIRouter(tags=["groups"])


class UpdateGroupLimitRequest(BaseModel):
    daily_limit: int = Field(..., ge=1, le=10000)


@router.get("/groups")
def list_groups(_user: User = Depends(require_user_or_admin), db: Session = Depends(get_db)) -> dict:
    perform_daily_reset_if_needed(db)
    now_utc = datetime.now(timezone.utc)
    rows = db.query(Group).order_by(Group.id.asc()).all()
    for g in rows:
        if g.disabled_until and now_utc >= g.disabled_until:
            g.disabled_until = None
            if g.status == "limited":
                g.status = "normal"
            db.add(g)
    db.commit()
    return {
        "ok": True,
        "groups": [
            {
                "id": g.id,
                "username": g.username,
                "title": g.title,
                "members_count": g.members_count,
                "total_added": g.total_added,
                "today_added": g.today_added,
                "yesterday_added": g.yesterday_added,
                "yesterday_left": g.yesterday_left,
                "status": g.status,
                "daily_limit": g.daily_limit,
                "disabled_until": g.disabled_until.isoformat() if g.disabled_until else None,
                "available": not (g.disabled_until and now_utc < g.disabled_until) and g.today_added < g.daily_limit,
            }
            for g in rows
        ],
    }


@router.put("/groups/{group_id}/limit")
def update_group_limit(
    group_id: int,
    payload: UpdateGroupLimitRequest,
    _user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    row = db.query(Group).filter(Group.id == group_id).first()
    if row is None:
        return {"ok": False, "message": "group not found"}
    row.daily_limit = payload.daily_limit
    db.add(row)
    db.commit()
    return {"ok": True, "id": row.id, "daily_limit": row.daily_limit}
