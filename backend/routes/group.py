from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_user_or_admin
from database import get_db
from models import Group, Setting, User
from services.daily_reset import perform_daily_reset_if_needed
from services.telegram_service import GROUP_METADATA_SYNC_KEY, sync_groups_metadata


router = APIRouter(tags=["groups"])


class UpdateGroupLimitRequest(BaseModel):
    daily_limit: int = Field(..., ge=1, le=10000)


class SyncGroupMetadataRequest(BaseModel):
    force: bool = False


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _display_handle(g: Group) -> str:
    if getattr(g, "public_username", None):
        return f"@{g.public_username}"
    return g.username


@router.post("/groups/sync-metadata")
async def sync_group_metadata(
    payload: SyncGroupMetadataRequest,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    owner_id = None if user.role == "admin" else user.id
    result = await sync_groups_metadata(owner_id, payload.force, db)
    return {"ok": result.get("ok", False), **{k: v for k, v in result.items() if k != "ok"}}


@router.get("/groups")
def list_groups(_user: User = Depends(require_user_or_admin), db: Session = Depends(get_db)) -> dict:
    perform_daily_reset_if_needed(db)
    now_utc = datetime.now(timezone.utc)
    sync_row = db.query(Setting).filter(Setting.key == GROUP_METADATA_SYNC_KEY).first()
    last_metadata_sync = sync_row.value if sync_row else None
    rows = db.query(Group).order_by(Group.id.asc()).all()
    for g in rows:
        disabled_until_utc = _as_utc(g.disabled_until)
        if disabled_until_utc and now_utc >= disabled_until_utc:
            g.disabled_until = None
            if g.status == "limited":
                g.status = "normal"
            db.add(g)
    db.commit()
    groups = []
    for g in rows:
        disabled_until_utc = _as_utc(g.disabled_until)
        y_add = int(g.yesterday_added or 0)
        y_left = int(g.yesterday_left or 0)
        groups.append(
            {
                "id": g.id,
                "username": g.username,
                "title": g.title,
                "public_username": getattr(g, "public_username", None),
                "display_handle": _display_handle(g),
                "members_count": g.members_count,
                "total_added": g.total_added,
                "today_added": g.today_added,
                "yesterday_added": g.yesterday_added,
                "yesterday_left": g.yesterday_left,
                "yesterday_leave_count": y_left,
                "net_growth": y_add - y_left,
                "status": g.status,
                "daily_limit": g.daily_limit,
                "disabled_until": g.disabled_until.isoformat() if g.disabled_until else None,
                "available": not (disabled_until_utc and now_utc < disabled_until_utc) and g.today_added < g.daily_limit,
            }
        )
    return {
        "ok": True,
        "groups": groups,
        "last_metadata_sync": last_metadata_sync,
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
