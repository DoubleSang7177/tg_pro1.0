import threading
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_user_or_admin
from database import get_db
from logger import get_logger
from models import AccountFile, InteractionTargetGroup, InteractionTask, User
from services.account_status import ST_DAILY_LIMITED, ST_NORMAL, recover_and_normalize
from services.daily_reset import perform_daily_reset_if_needed
from services.interaction_live_log import get_snapshot as interaction_live_snapshot
from services.interaction_live_log import init_session as interaction_live_init
from services.interaction_service import run_interaction_task_sync
from services.task_run_control import register_interaction_job, task_run_start
from services.telegram_service import _normalize_chat_identifier

router = APIRouter(tags=["interaction"])
log = get_logger("interaction")


class CreateInteractionTaskBody(BaseModel):
    groups: list[str] = Field(..., min_length=1, description="目标群组 username 列表")
    scan_limit: int = Field(300, ge=10, le=5000)
    valid_only: bool = Field(
        False,
        description="为 True 时仅执行数据库中存在的群组，忽略未知项",
    )
    force_reset_memory: bool = Field(
        False,
        description="为 True 时强制清空互动游标记忆并从头开始",
    )


class RegisterTargetGroupsBody(BaseModel):
    usernames: list[str] = Field(default_factory=list, description="写入互动目标群组库的群组 username")
    raw_input: str = Field(default="", description="换行/逗号分隔的群组标识")
    title: str | None = Field(default=None, description="群组名称（可选）")
    titles: list[str] = Field(default_factory=list, description="与群组 username 一一对应的群组名称（可选）")
    remark: str | None = Field(default=None, description="备注")


class DeleteTargetGroupsBody(BaseModel):
    usernames: list[str] = Field(default_factory=list, description="待删除的互动目标群组 username 列表")


class UpdateTargetGroupRemarkBody(BaseModel):
    remark: str | None = Field(default=None, description="备注，空字符串表示清空")


class UpdateTargetGroupBody(BaseModel):
    username: str = Field(..., min_length=1, description="群组ID（如 @xxx 或 -100...）")
    title: str | None = Field(default=None, description="群组名称（可选）")
    remark: str | None = Field(default=None, description="备注（可选）")


def _extract_identifiers(body: RegisterTargetGroupsBody) -> list[str]:
    def _canonical_username(raw: str) -> str:
        base = _normalize_chat_identifier(raw)
        base = str(base or "").strip().lstrip("@")
        return f"@{base}" if base else ""

    raw_parts = [x.strip() for x in str(body.raw_input or "").replace(",", "\n").splitlines() if x.strip()]
    merged = [*list(body.usernames or []), *raw_parts]
    normalized = [_canonical_username(str(u).strip()) for u in merged if str(u).strip()]
    return list(dict.fromkeys(normalized))


def _extract_title_map(body: RegisterTargetGroupsBody, normalized: list[str]) -> dict[str, str | None]:
    # 新增批量 titles 优先：ID 多于名称 -> 余下 ID 用 @xxx；名称多于 ID -> 直接忽略
    titles = [str(x).strip()[:255] for x in (body.titles or [])]
    if titles:
        out: dict[str, str | None] = {}
        for idx, username in enumerate(normalized):
            title = titles[idx] if idx < len(titles) else ""
            out[username] = title or None
        return out
    single = (body.title or "").strip()[:255] or None
    return {u: single for u in normalized}


def _register_interaction_targets(
    db: Session,
    normalized: list[str],
    title_map: dict[str, str | None],
    remark: str | None,
) -> tuple[list[str], list[str], list[str]]:
    def _canon(raw: str) -> str:
        base = _normalize_chat_identifier(raw)
        base = str(base or "").strip().lstrip("@")
        return f"@{base}" if base else ""

    existing = db.query(InteractionTargetGroup).all()
    existing_map: dict[str, InteractionTargetGroup] = {}
    for g in existing:
        key = _canon(str(g.username or ""))
        if key:
            existing_map[key] = g
    already = set(existing_map.keys())
    have = set(already)
    added: list[str] = []
    updated: list[str] = []
    for un in normalized:
        row_title = (title_map.get(un) or "").strip()[:255] or un
        if un in have:
            row = existing_map.get(un)
            if row is not None:
                changed = False
                # 统一 username 存储形态为 @xxx
                if row.username != un:
                    row.username = un
                    db.add(row)
                    changed = True
                if title_map.get(un) and row_title != row.title:
                    row.title = row_title
                    db.add(row)
                    changed = True
                if remark and remark != (row.remark or ""):
                    row.remark = remark
                    db.add(row)
                    changed = True
                if changed:
                    updated.append(un)
            continue
        db.add(InteractionTargetGroup(username=un, title=row_title, remark=remark))
        added.append(un)
        have.add(un)
    db.commit()
    skipped = [u for u in normalized if (u in already and u not in set(updated))]
    return added, updated, skipped


def _pick_engagement_accounts(db: Session, owner_id: int | None) -> list[AccountFile]:
    """非风控：可用 + 当日受限（与账号列表 active + limited 一致，不含 risk_suspected）。"""
    from datetime import datetime, timezone

    now_utc = datetime.now(timezone.utc)
    q = db.query(AccountFile).order_by(AccountFile.id.desc())
    if owner_id is not None:
        q = q.filter(AccountFile.owner_id == owner_id)
    out: list[AccountFile] = []
    for row in q.all():
        recover_and_normalize(row, now_utc)
        if row.status not in (ST_NORMAL, ST_DAILY_LIMITED):
            continue
        out.append(row)
    return out


def _partition_groups(db: Session, normalized: list[str]) -> tuple[list[str], list[str]]:
    if not normalized:
        return [], []
    rows = (
        db.query(InteractionTargetGroup)
        .filter(InteractionTargetGroup.username.in_(normalized))
        .all()
    )
    found = {g.username for g in rows}
    valid = [x for x in normalized if x in found]
    invalid = [x for x in normalized if x not in found]
    return valid, invalid


def _task_to_dict(row: InteractionTask) -> dict[str, Any]:
    groups = list(row.target_groups or [])
    acc_ids = list(row.account_ids or [])
    return {
        "id": row.id,
        "owner_id": row.owner_id,
        "groups": groups,
        "group_count": len(groups),
        "account_ids": acc_ids,
        "account_count": len(acc_ids),
        "status": row.status,
        "success_count": row.success_count or 0,
        "fail_count": row.fail_count or 0,
        "round_idx": int(row.round_idx or 0),
        "memory_size": len(dict(row.cursor_map or {})),
        "scan_limit": row.scan_limit or 300,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


@router.post("/interaction/target-groups/register")
async def register_target_groups(
    body: RegisterTargetGroupsBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    """将群组 username 写入「互动目标群组库」(interaction_target_groups)。"""
    perform_daily_reset_if_needed(db)
    normalized = _extract_identifiers(body)
    if not normalized:
        raise HTTPException(status_code=400, detail="请提供至少一个群组")
    title_map = _extract_title_map(body, normalized)
    remark = (body.remark or "").strip()[:255] or None
    added, updated, skipped = _register_interaction_targets(db, normalized, title_map, remark)
    log.info(
        "interaction register_target_groups user=%s added=%s updated=%s skipped=%s",
        user.id,
        added,
        updated,
        skipped,
    )
    return {"ok": True, "added": added, "updated": updated, "skipped": skipped}


@router.post("/interaction/target-groups")
async def create_interaction_target_groups(
    body: RegisterTargetGroupsBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    """互动目标群组录入接口（标准入口，等价于 /register）。"""
    perform_daily_reset_if_needed(db)
    normalized = _extract_identifiers(body)
    if not normalized:
        raise HTTPException(status_code=400, detail="请提供至少一个群组")
    title_map = _extract_title_map(body, normalized)
    remark = (body.remark or "").strip()[:255] or None
    added, updated, skipped = _register_interaction_targets(db, normalized, title_map, remark)
    log.info(
        "interaction create_target_groups user=%s added=%s updated=%s skipped=%s",
        user.id,
        added,
        updated,
        skipped,
    )
    return {"ok": True, "added": added, "updated": updated, "skipped": skipped}


@router.get("/interaction/target-groups")
def list_interaction_target_groups(
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    _ = user
    rows = db.query(InteractionTargetGroup).order_by(InteractionTargetGroup.id.desc()).all()
    groups = [
        {
            "id": g.id,
            "username": g.username,
            "title": g.title,
            "remark": g.remark,
            "display_handle": g.username,
            "created_at": g.created_at.isoformat() if g.created_at else None,
        }
        for g in rows
    ]
    return {"ok": True, "groups": groups}


@router.delete("/interaction/target-groups")
def delete_interaction_target_groups(
    body: DeleteTargetGroupsBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    _ = user
    raw_inputs = [str(u).strip() for u in (body.usernames or []) if str(u).strip()]
    if not raw_inputs:
        raise HTTPException(status_code=400, detail="请至少选择一个群组")

    normalized_inputs = [_normalize_chat_identifier(x) for x in raw_inputs]
    keyset = {
        str(x).strip().lower()
        for x in [*raw_inputs, *normalized_inputs]
        if str(x).strip()
    }

    all_rows = db.query(InteractionTargetGroup).all()
    rows = []
    for r in all_rows:
        raw_key = str(r.username or "").strip().lower()
        norm_key = _normalize_chat_identifier(str(r.username or "")).strip().lower()
        if raw_key in keyset or norm_key in keyset:
            rows.append(r)

    existing = {str(r.username or "").strip().lower() for r in rows}
    deleted = []
    for row in rows:
        deleted.append(row.username)
        db.delete(row)
    db.commit()
    skipped = []
    for raw in raw_inputs:
        raw_key = raw.strip().lower()
        norm_key = _normalize_chat_identifier(raw).strip().lower()
        if raw_key not in existing and norm_key not in existing:
            skipped.append(raw)
    return {"ok": True, "deleted": deleted, "skipped": skipped}


@router.patch("/interaction/target-groups/{group_id}/remark")
def update_interaction_target_group_remark(
    group_id: int,
    body: UpdateTargetGroupRemarkBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    _ = user
    row = db.query(InteractionTargetGroup).filter(InteractionTargetGroup.id == group_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail="群组不存在")
    remark = (body.remark or "").strip()[:255] or None
    row.remark = remark
    db.add(row)
    db.commit()
    return {"ok": True, "group": {"id": row.id, "remark": row.remark}}


@router.patch("/interaction/target-groups/{group_id}")
def update_interaction_target_group(
    group_id: int,
    body: UpdateTargetGroupBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    _ = user
    row = db.query(InteractionTargetGroup).filter(InteractionTargetGroup.id == group_id).first()
    if row is None:
        raise HTTPException(status_code=404, detail="群组不存在")

    normalized_username = _normalize_chat_identifier(str(body.username or "").strip())
    normalized_username = str(normalized_username or "").strip()
    if not normalized_username:
        raise HTTPException(status_code=400, detail="群组ID不能为空")

    title = (body.title or "").strip()[:255] or normalized_username
    remark = (body.remark or "").strip()[:255] or None

    duplicate = (
        db.query(InteractionTargetGroup)
        .filter(InteractionTargetGroup.username == normalized_username, InteractionTargetGroup.id != group_id)
        .first()
    )
    if duplicate is not None:
        raise HTTPException(status_code=409, detail="群组ID已存在，请使用其他值")

    row.username = normalized_username
    row.title = title
    row.remark = remark
    db.add(row)
    db.commit()
    db.refresh(row)
    return {
        "ok": True,
        "group": {
            "id": row.id,
            "username": row.username,
            "title": row.title,
            "remark": row.remark,
            "display_handle": row.username,
        },
    }


@router.post("/interaction/tasks")
def create_interaction_task(
    body: CreateInteractionTaskBody,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    perform_daily_reset_if_needed(db)
    db.commit()

    normalized = [_normalize_chat_identifier(g) for g in body.groups if str(g).strip()]
    if not normalized:
        raise HTTPException(status_code=400, detail="请至少选择一个群组")
    normalized = list(dict.fromkeys(normalized))

    # 群组互动页已限定从互动目标群组库中选择，这里不再做重复拦截校验，
    # 避免出现“已可选却提示不在库中”的误判。

    # 任务归属始终写当前用户；仅账号筛选口径对 admin 放开为全量
    account_owner_filter = None if user.role == "admin" else user.id
    accounts = _pick_engagement_accounts(db, account_owner_filter)
    if not accounts:
        raise HTTPException(status_code=400, detail="没有符合条件的账号（需要可用或当日受限，不含风控列）")

    resume_cursor_map: dict[str, int] = {}
    resume_round_idx = 0
    if not bool(body.force_reset_memory):
        history = (
            db.query(InteractionTask)
            .filter(InteractionTask.owner_id == user.id)
            .order_by(InteractionTask.id.desc())
            .limit(100)
            .all()
        )
        for old in history:
            if list(old.target_groups or []) == normalized:
                resume_cursor_map = {
                    str(k): int(v)
                    for k, v in dict(old.cursor_map or {}).items()
                    if str(k).strip()
                }
                resume_round_idx = max(0, int(old.round_idx or 0))
                break

    task = InteractionTask(
        owner_id=user.id,
        target_groups=normalized,
        account_ids=[a.id for a in accounts],
        status="pending",
        success_count=0,
        fail_count=0,
        cursor_map=resume_cursor_map,
        round_idx=resume_round_idx,
        scan_limit=int(body.scan_limit),
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    tid = task.id
    job_id = uuid.uuid4().hex
    interaction_live_init(job_id, owner_id=user.id, task_id=tid)
    log.info(
        "interaction task created id=%s job=%s user=%s groups=%s accounts=%s",
        tid,
        job_id,
        user.id,
        len(normalized),
        len(accounts),
    )

    task_run_start()
    register_interaction_job(job_id)

    def _runner() -> None:
        run_interaction_task_sync(tid, job_id)

    threading.Thread(target=_runner, name=f"interaction-{tid}", daemon=True).start()
    return {"ok": True, "job_id": job_id, "task": _task_to_dict(task)}


@router.get("/interaction/live/{job_id}")
def interaction_live_status(
    job_id: str,
    user: User = Depends(require_user_or_admin),
) -> dict:
    snap = interaction_live_snapshot(job_id)
    if snap is None:
        raise HTTPException(status_code=404, detail="会话不存在或已过期")
    if user.role != "admin" and snap["owner_id"] != user.id:
        raise HTTPException(status_code=403, detail="无权查看该会话")
    return {
        "ok": True,
        "job_id": job_id,
        "task_id": snap["task_id"],
        "status": snap["status"],
        "logs": snap["logs"],
    }


@router.get("/interaction/tasks")
def list_interaction_tasks(
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    q = db.query(InteractionTask).order_by(InteractionTask.id.desc())
    if user.role != "admin":
        q = q.filter(InteractionTask.owner_id == user.id)
    rows = q.limit(200).all()
    return {"ok": True, "tasks": [_task_to_dict(r) for r in rows]}
