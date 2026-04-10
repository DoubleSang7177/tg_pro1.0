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
from models import AccountFile, User
from services.daily_reset import perform_daily_reset_if_needed


router = APIRouter(tags=["accounts"])
BASE_TDATA_DIR = Path(__file__).resolve().parent.parent / "data" / "tdata"
SCAN_BASE_DIR = Path("C:/Users/葛萨桑桑/Desktop/TGTDATAaccount")
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


def _account_payload(row: AccountFile) -> dict:
    digits = "".join(ch for ch in str(row.phone or "") if ch.isdigit())
    formatted_phone = f"#{digits}" if digits else None
    return {
        "id": row.id,
        "owner_id": row.owner_id,
        "phone": row.phone,
        "formatted_phone": formatted_phone,
        "path": row.saved_path,
        "status": row.status,
        "today_count": row.today_count,
        "error_count": row.error_count,
        "today_used_count": row.today_used_count,
        "last_used_time": row.last_used_time.isoformat() if row.last_used_time else None,
        "limited_until": row.limited_until.isoformat() if row.limited_until else None,
        "filename": row.filename,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _scan_accounts(user: User, db: Session) -> None:
    if not SCAN_BASE_DIR.exists():
        return
    for subdir in SCAN_BASE_DIR.iterdir():
        if not subdir.is_dir():
            continue
        phone = _parse_phone_from_name(subdir.name)
        if not phone:
            continue
        existing = db.query(AccountFile).filter(AccountFile.phone == phone).first()
        if existing is None:
            db.add(
                AccountFile(
                    owner_id=user.id,
                    phone=phone,
                    filename=subdir.name,
                    saved_path=str(subdir.resolve()),
                    status="active",
                    today_count=0,
                    error_count=0,
                )
            )
        else:
            existing.saved_path = str(subdir.resolve())
            if not existing.status:
                existing.status = "active"
            db.add(existing)
    db.commit()


@router.post("/upload_account")
async def upload_account(
    file: UploadFile = File(...),
    user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    log.info("upload start user_id=%s filename=%s", user.id, file.filename)
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

    existing = (
        db.query(AccountFile)
        .filter(AccountFile.saved_path == str(extracted_dir), AccountFile.owner_id == user.id)
        .first()
    )
    if existing is None:
        db.add(
            AccountFile(
                owner_id=user.id,
                phone=_parse_phone_from_name(Path(file.filename).stem) or Path(file.filename).stem,
                filename=file.filename,
                saved_path=str(extracted_dir),
                status="active",
                today_count=0,
                error_count=0,
            )
        )
        db.commit()
        log.info("upload db_saved user_id=%s path=%s", user.id, extracted_dir)

    log.info("upload success user_id=%s extracted=%s", user.id, extracted_dir)
    return {
        "ok": True,
        "user_id": user.id,
        "filename": file.filename,
        "saved_path": str(extracted_dir),
    }


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
    log.info("accounts list user_id=%s role=%s count=%s", user.id, user.role, len(rows))
    grouped = {"active": [], "limited": [], "banned": []}
    now_utc = datetime.now(timezone.utc)
    for row in rows:
        status = (row.status or "active").lower()
        if status == "limited_today":
            if row.limited_until and now_utc >= row.limited_until:
                row.status = "active"
                row.limited_until = None
                status = "active"
            elif row.limited_until and now_utc - row.limited_until > timedelta(days=3):
                row.status = "banned"
                status = "banned"
            else:
                status = "limited"
        elif status not in grouped:
            status = "active"
        grouped[status].append(_account_payload(row))
    db.commit()
    flat_accounts = []
    for item in grouped["active"] + grouped["limited"] + grouped["banned"]:
        if item.get("formatted_phone"):
            flat_accounts.append(item["formatted_phone"])
    return {
        "ok": True,
        "accounts": flat_accounts,
        "active": grouped["active"],
        "limited": grouped["limited"],
        "banned": grouped["banned"],
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

    target = Path(row.saved_path)
    if target.exists() and target.is_dir():
        shutil.rmtree(target)
    db.delete(row)
    db.commit()
    log.info("account deleted phone=%s by_user=%s", phone, user.id)
    return {"ok": True, "phone": phone}
