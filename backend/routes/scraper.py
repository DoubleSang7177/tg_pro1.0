from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from auth import require_user_or_admin
from database import get_db
from logger import get_logger
from models import ScraperTask, User
from services.scraper_account_service import complete_login, get_account_status, send_code_request
from services.scraper_service import RESULTS_DIR, scrape_group_users

router = APIRouter(prefix="/scraper", tags=["scraper"])
log = get_logger("scraper")

SCRAPED_LEGACY_DIR = Path(__file__).resolve().parent.parent / "data" / "scraped"

_SAFE_TXT = re.compile(r"^[A-Za-z0-9._\-]+\.txt$")


class ScraperSendCodeBody(BaseModel):
    phone: str = Field(..., min_length=5, description="手机号，含国家区号")


class ScraperLoginBody(BaseModel):
    phone: str = Field(..., min_length=5)
    code: str = Field("", max_length=32)
    phone_code_hash: str = Field("")
    password: str | None = Field(None, max_length=256)


class ScraperRunBody(BaseModel):
    group_id: str = Field(..., min_length=1, description="群组 username、邀请链接或 id")
    days: int = Field(7, ge=1, le=365, description="向前追溯天数")
    max_messages: int = Field(5000, ge=1, le=200_000, description="最多扫描消息条数上限")


@router.post("/send_code")
async def scraper_send_code(body: ScraperSendCodeBody, _user: User = Depends(require_user_or_admin)) -> dict:
    try:
        result = await send_code_request(body.phone.strip())
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    if not result.get("ok"):
        return {"ok": False, "error": result.get("error", "发送失败")}
    return {
        "ok": True,
        "phone": result["phone"],
        "phone_code_hash": result["phone_code_hash"],
    }


@router.post("/login")
async def scraper_login(
    body: ScraperLoginBody,
    db: Session = Depends(get_db),
    _user: User = Depends(require_user_or_admin),
) -> dict:
    try:
        result = await complete_login(
            db,
            body.phone.strip(),
            body.code,
            (body.phone_code_hash or "").strip(),
            body.password,
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    if result.get("need_password"):
        return {"need_password": True}
    if result.get("ok"):
        return {"ok": True, "phone": result.get("phone")}
    return {"ok": False, "error": result.get("error", "登录失败")}


@router.get("/account")
async def scraper_account(db: Session = Depends(get_db), _user: User = Depends(require_user_or_admin)) -> dict:
    return await get_account_status(db)


@router.post("/run")
async def scraper_run(body: ScraperRunBody, _user: User = Depends(require_user_or_admin)) -> dict:
    result = await scrape_group_users(body.group_id.strip(), body.days, body.max_messages)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "采集失败"))
    task_id = result.get("task_id")
    if not task_id:
        raise HTTPException(status_code=500, detail="内部错误：缺少 task_id")
    return {
        "file_url": f"/scraper/download/{task_id}",
        "task_id": task_id,
        "group_id": result.get("group"),
        "count": result.get("count"),
    }


@router.get("/tasks")
def scraper_list_tasks(db: Session = Depends(get_db), _user: User = Depends(require_user_or_admin)) -> list[dict]:
    rows = (
        db.query(ScraperTask)
        .filter(ScraperTask.user_count > 0)
        .order_by(ScraperTask.created_at.desc())
        .limit(200)
        .all()
    )
    out = []
    for r in rows:
        out.append(
            {
                "id": r.id,
                "group_link": r.group_link,
                "group_name": r.group_name,
                "days": getattr(r, "days", None),
                "max_messages": getattr(r, "max_messages", None),
                "user_count": r.user_count,
                "download_count": r.download_count,
                "status": r.status,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
        )
    return out


@router.get("/download/{task_id:int}")
def scraper_download_by_task(
    task_id: int,
    db: Session = Depends(get_db),
    _user: User = Depends(require_user_or_admin),
):
    row = db.query(ScraperTask).filter(ScraperTask.id == task_id).first()
    if row is None or row.status != "done" or not row.result_file:
        raise HTTPException(status_code=404, detail="任务不存在或未完成")
    path = Path(row.result_file).resolve()
    root = RESULTS_DIR.resolve()
    try:
        path.relative_to(root)
    except ValueError:
        log.error("scraper download path escape task_id=%s path=%s", task_id, path)
        raise HTTPException(status_code=400, detail="非法文件路径")
    if not path.is_file():
        raise HTTPException(status_code=404, detail="结果文件不存在")
    row.download_count = (row.download_count or 0) + 1
    db.commit()
    fname = f"scraper_{task_id}.txt"
    return FileResponse(
        path,
        media_type="text/plain; charset=utf-8",
        filename=fname,
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.get("/download/{filename}")
def scraper_download(filename: str, _user: User = Depends(require_user_or_admin)):
    if not _SAFE_TXT.match(filename):
        raise HTTPException(status_code=400, detail="非法文件名")
    path = None
    for base in (RESULTS_DIR, SCRAPED_LEGACY_DIR):
        p = (base / filename).resolve()
        try:
            p.relative_to(base.resolve())
        except ValueError:
            continue
        if p.is_file():
            path = p
            break
    if path is None:
        raise HTTPException(status_code=404, detail="文件不存在")
    return FileResponse(
        path,
        media_type="text/plain; charset=utf-8",
        filename=filename,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
