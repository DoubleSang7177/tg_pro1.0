from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from models import AccountFactory
from services.account_factory.factory_runner import append_factory_log


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def run_factory_warmup_cycle_once(db: Session) -> dict[str, int]:
    now = _utcnow()
    rows = db.query(AccountFactory).filter(AccountFactory.source == "factory", AccountFactory.status == "WARMING").all()
    stats = {"warming": 0, "ready": 0}
    for row in rows:
        deadline = row.warmup_until
        if deadline and now >= deadline:
            row.status = "READY"
            row.fail_reason = None
            db.add(row)
            stats["ready"] += 1
            append_factory_log("SUCCESS", "WARMUP", f"账号 {row.phone} 养号完成，状态 READY")
        else:
            stats["warming"] += 1
    db.commit()
    return stats

