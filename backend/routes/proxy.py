from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.orm import Session

from auth import require_admin, require_user_or_admin
from database import get_db
from models import AccountFile, Proxy, User
from services.proxy_service import import_proxies_from_file


router = APIRouter(tags=["proxy"])


@router.post("/proxy/upload")
async def upload_proxy_file(
    file: UploadFile = File(...),
    _admin: User = Depends(require_admin),
) -> dict:
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(status_code=400, detail="仅支持 json 文件")

    target = Path(__file__).resolve().parents[2] / "proxy_config_plus.json"
    content = await file.read()
    target.write_bytes(content)

    imported_count = import_proxies_from_file()
    return {"ok": True, "imported_count": imported_count}


@router.get("/proxy")
def list_proxies(
    _user: User = Depends(require_user_or_admin),
    db: Session = Depends(get_db),
) -> dict:
    account_rows = db.query(AccountFile).order_by(AccountFile.id.asc()).all()
    proxy_rows = db.query(Proxy).order_by(Proxy.id.asc()).all()
    proxy_map = {p.id: p for p in proxy_rows}

    total = len(account_rows)
    used = 0
    idle = 0
    dead = 0
    items = []
    for a in account_rows:
        proxy_obj = proxy_map.get(a.proxy_id) if a.proxy_id else None
        if a.proxy_type == "direct":
            idle += 1
        else:
            used += 1
        if a.status in {"banned", "limited_long", "risk_suspected"}:
            dead += 1

        proxy_value = "-"
        status = "idle" if a.proxy_type == "direct" else "used"
        if proxy_obj is not None:
            proxy_value = f"{proxy_obj.host}:{proxy_obj.port}@{proxy_obj.username}:{proxy_obj.password}"
            status = proxy_obj.status

        items.append(
            {
                "id": a.id,
                "phone": a.phone,
                "proxy_type": a.proxy_type,
                "proxy_value": proxy_value,
                "status": status,
                "proxy_id": a.proxy_id,
            }
        )

    return {
        "ok": True,
        "summary": {
            "total": total,
            "idle": idle,
            "used": used,
            "dead": dead,
        },
        "items": items,
    }


@router.post("/proxy/{proxy_id}/mark_dead")
def mark_proxy_dead(
    proxy_id: int,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if proxy is None:
        raise HTTPException(status_code=404, detail="代理不存在")
    proxy.status = "dead"
    db.add(proxy)
    db.commit()
    return {"ok": True, "id": proxy.id, "status": proxy.status}


@router.post("/proxy/{proxy_id}/unbind")
def unbind_proxy(
    proxy_id: int,
    _admin: User = Depends(require_admin),
    db: Session = Depends(get_db),
) -> dict:
    proxy = db.query(Proxy).filter(Proxy.id == proxy_id).first()
    if proxy is None:
        raise HTTPException(status_code=404, detail="代理不存在")

    if proxy.assigned_account_id:
        account = db.query(AccountFile).filter(AccountFile.id == proxy.assigned_account_id).first()
        if account is not None:
            account.proxy_id = None
            account.proxy_type = "direct"
            db.add(account)

    proxy.assigned_account_id = None
    proxy.status = "idle"
    db.add(proxy)
    db.commit()
    return {"ok": True, "id": proxy.id, "status": proxy.status}
