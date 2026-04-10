from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query

from auth import require_admin
from logger import LOG_DIR
from models import User


router = APIRouter(prefix="/logs", tags=["logs"])


@router.get("")
def list_logs(_admin: User = Depends(require_admin)) -> dict:
    files = sorted(LOG_DIR.glob("*.log*"), key=lambda p: p.stat().st_mtime, reverse=True)
    return {
        "ok": True,
        "files": [{"name": f.name, "size": f.stat().st_size} for f in files],
    }


@router.get("/history")
def read_log_history(
    filename: str = Query(default="app.log"),
    tail: int = Query(default=200, ge=1, le=2000),
    _admin: User = Depends(require_admin),
) -> dict:
    log_path = (LOG_DIR / filename).resolve()
    if not str(log_path).startswith(str(LOG_DIR.resolve())):
        raise HTTPException(status_code=400, detail="非法日志路径")
    if not log_path.exists() or not log_path.is_file():
        raise HTTPException(status_code=404, detail="日志文件不存在")

    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    return {
        "ok": True,
        "filename": Path(filename).name,
        "lines": lines[-tail:],
    }
