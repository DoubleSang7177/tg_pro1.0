from __future__ import annotations

import shutil
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.orm import Session

from auth import require_user_or_admin
from database import get_db
from logger import get_logger
from models import AccountFile, AccountPath, Proxy, User
from services.account_status import (
    RECENT_SIDEBAR_SECONDS,
    ST_COOLDOWN,
    ST_DAILY_LIMITED,
    ST_NORMAL,
    ST_RISK_SUSPECTED,
    lifecycle_ui_labels,
    recover_and_normalize,
)
from services.daily_reset import perform_daily_reset_if_needed
from services.account_activity_log import list_account_activity_for_user
from services.proxy_service import assign_proxy_to_account


router = APIRouter(tags=["accounts"])
BASE_TDATA_DIR = Path(__file__).resolve().parent.parent / "data" / "tdata"
log = get_logger("account")


def _safe_extract_zip(zip_path: Path, extract_to: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            member_path = (extract_to / member.filename).resolve()
            if not str(member_path).startswith(str(extract_to.resolve())):
                raise ValueError("zip 包含非法路径")
        zf.extractall(extract_to)


def _parse_phone_from_name(name: str) -> str | None:
    digits = "".join(ch for ch in name if ch.isdigit())
    if not digits:
        return None
    return f"+{digits}"


def _account_payload(row: AccountFile, proxy_ip: str | None = None) -> dict:
    digits = "".join(ch for ch in str(row.phone or "") if ch.isdigit())
    formatted_phone = f"#{digits}" if digits else None
    return {
        "id": row.id,
        "owner_id": row.owner_id,
        "phone": row.phone,
        "formatted_phone": formatted_phone,
        "proxy_type": row.proxy_type,
        "proxy_ip": proxy_ip,
        "path": row.saved_path,
        "status": row.status,
        "today_count": row.today_count,
        "error_count": row.error_count,
        "today_used_count": row.today_used_count,
        "last_used_time": row.last_used_time.isoformat() if row.last_used_time else None,
        "limited_until": row.limited_until.isoformat() if row.limited_until else None,
        "login_fail_count": getattr(row, "login_fail_count", 0) or 0,
        "last_login_fail_at": row.last_login_fail_at.isoformat() if getattr(row, "last_login_fail_at", None) else None,
        "status_changed_at": row.status_changed_at.isoformat() if getattr(row, "status_changed_at", None) else None,
        "status_note": getattr(row, "status_note", None),
        "lifecycle_primary": lifecycle_ui_labels(row.status, getattr(row, "status_note", None))[0],
        "lifecycle_sub": lifecycle_ui_labels(row.status, getattr(row, "status_note", None))[1],
        "filename": row.filename,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _scan_accounts(user: User, db: Session) -> None:
    path_rows = db.query(AccountPath).all()
    for path_row in path_rows:
        base_dir = Path(path_row.path)
        if not base_dir.exists() or not base_dir.is_dir():
            continue
        for subdir in base_dir.iterdir():
            if not subdir.is_dir():
                continue
            phone = _parse_phone_from_name(subdir.name)
            if not phone:
                continue
            existing = db.query(AccountFile).filter(AccountFile.phone == phone).first()
            if existing is None:
                created = AccountFile(
                    owner_id=user.id,
                    phone=phone,
                    filename=subdir.name,
                    saved_path=str(subdir.resolve()),
                    status=ST_NORMAL,
                    today_count=0,
                    error_count=0,
                )
                db.add(created)
                db.flush()
                assign_proxy_to_account(created)
            else:
                existing.saved_path = str(subdir.resolve())
                if not existing.status:
                    existing.status = ST_NORMAL
                db.add(existing)
    db.commit()


async def _upload_and_create_account(file: UploadFile, user: User, db: Session) -> dict:
    if not file.filename.lower().endswith(".zip"):
        log.warning("upload reject user_id=%s filename=%s reason=not_zip", user.id, file.filename)
        raise HTTPException(status_code=400, detail="仅支持 zip 格式")

    user_dir = BASE_TDATA_DIR / str(user.id)
    user_dir.mkdir(parents=True, exist_ok=True)
    zip_path = user_dir / file.filename

    with zip_path.open("wb") as f:
        content = await file.read()
        f.write(content)

    extracted_dir = user_dir / Path(file.filename).stem
    if extracted_dir.exists():
        shutil.rmtree(extracted_dir)
    extracted_dir.mkdir(parents=True, exist_ok=True)

    try:
        _safe_extract_zip(zip_path, extracted_dir)
    except (zipfile.BadZipFile, ValueError) as exc:
        log.exception("upload failed user_id=%s zip_path=%s", user.id, zip_path)
        raise HTTPException(status_code=400, detail=f"zip 文件无效: {exc}") from exc

    warning = None
    existing = (
        db.query(AccountFile)
        .filter(AccountFile.saved_path == str(extracted_dir), AccountFile.owner_id == user.id)
        .first()
    )
    if existing is None:
        created = AccountFile(
            owner_id=user.id,
            phone=_parse_phone_from_name(Path(file.filename).stem) or Path(file.filename).stem,
            filename=file.filename,
            saved_path=str(extracted_dir),
            status=ST_NORMAL,
            today_count=0,
            error_count=0,
        )
        db.add(created)
        db.flush()
        assigned = assign_proxy_to_account(created)
        if assigned.get("warning") == "无代理库存":
            warning = "当前无代理库存，账号将使用本地IP，存在风控风险"
        db.commit()
        log.info("upload db_saved user_id=%s path=%s", user.id, extracted_dir)

    log.info("upload success user_id=%s extracted=%s", user.id, extracted_dir)
    return {
        "ok": True,
        "user_id": user.id,
        "filename": file.filename,
        "saved_path": str(extracted_dir),
        "warning": warning,
    }


@router.post("/upload_account")
async def upload_account(
    file: UploadFile = File(...),
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    log.info("upload start user_id=%s filename=%s", user.id, file.filename)
    return await _upload_and_create_account(file, user, db)


@router.post("/accounts/upload")
async def upload_account_v2(
    file: UploadFile = File(...),
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    log.info("accounts/upload start user_id=%s filename=%s", user.id, file.filename)
    return await _upload_and_create_account(file, user, db)


@router.get("/accounts")
def list_accounts(
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    perform_daily_reset_if_needed(db)
    _scan_accounts(user, db)
    query = db.query(AccountFile).order_by(AccountFile.id.desc())
    if user.role != "admin":
        query = query.filter(AccountFile.owner_id == user.id)
    rows = query.all()
    proxy_ids = [r.proxy_id for r in rows if r.proxy_id]
    proxy_map: dict[int, str] = {}
    if proxy_ids:
        proxy_rows = db.query(Proxy).filter(Proxy.id.in_(proxy_ids)).all()
        proxy_map = {p.id: f"{p.host}:{p.port}" for p in proxy_rows}
    log.info("accounts list user_id=%s role=%s count=%s", user.id, user.role, len(rows))
    grouped = {"active": [], "limited": [], "banned": []}
    recent_sidebar_echo: list[dict] = []
    now_utc = datetime.now(timezone.utc)
    for row in rows:
        recover_and_normalize(row, now_utc)
        st = (row.status or ST_NORMAL).lower()
        if st == ST_NORMAL:
            bucket = "active"
        elif st in (ST_DAILY_LIMITED, ST_COOLDOWN):
            bucket = "limited"
        elif st == ST_RISK_SUSPECTED:
            bucket = "banned"
        else:
            bucket = "active"
        payload = _account_payload(row, proxy_map.get(row.proxy_id))
        grouped[bucket].append(payload)
        if st != ST_NORMAL:
            changed = _as_utc(getattr(row, "status_changed_at", None))
            if changed and (now_utc - changed).total_seconds() <= RECENT_SIDEBAR_SECONDS:
                _, sub = lifecycle_ui_labels(row.status, getattr(row, "status_note", None))
                recent_sidebar_echo.append({**payload, "sidebar_echo": True, "echo_label": sub})
    db.commit()
    flat_accounts = []
    for item in grouped["active"] + grouped["limited"] + grouped["banned"]:
        if item.get("formatted_phone"):
            flat_accounts.append(item["formatted_phone"])
    activity_feed = list_account_activity_for_user(
        viewer_id=user.id,
        is_admin=user.role == "admin",
        limit=15,
    )
    return {
        "ok": True,
        "accounts": flat_accounts,
        "active": grouped["active"],
        "limited": grouped["limited"],
        "banned": grouped["banned"],
        "recent_sidebar_echo": recent_sidebar_echo,
        "recent_limited_sidebar": recent_sidebar_echo,
        "activity_feed": activity_feed,
    }


@router.delete("/accounts/{phone}")
def delete_account(
    phone: str,
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    row = db.query(AccountFile).filter(AccountFile.phone == phone).first()
    if row is None:
        raise HTTPException(status_code=404, detail="账号不存在")
    if user.role != "admin" and row.owner_id != user.id:
        raise HTTPException(status_code=403, detail="无权限删除该账号")

    # 安全约束：删除账号仅移除数据库记录，绝不删除本地账号文件目录。
    db.delete(row)
    db.commit()
    log.info("account unbound_from_db phone=%s by_user=%s path=%s", phone, user.id, row.saved_path)
    return {"ok": True, "phone": phone}
