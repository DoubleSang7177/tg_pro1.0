"""进程内账户活动流（最近 N 条），供用户增长页左侧监控面板；按 owner 过滤。"""
from __future__ import annotations

import itertools
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any

_MAX_EVENTS = 200
_lock = threading.Lock()
_buffer: deque[dict[str, Any]] = deque(maxlen=_MAX_EVENTS)
_id_seq = itertools.count(1)


def mask_phone(phone: str | None) -> str:
    d = "".join(c for c in str(phone or "") if c.isdigit())
    if not d:
        return "—"
    if len(d) <= 6:
        return f"+{d}"
    return f"+{d[:4]}***{d[-3:]}"


def record_account_activity(
    owner_id: int,
    phone: str | None,
    *,
    action: str,
    status: str,
    level: str,
) -> None:
    """
    level: success | warn | error | info
    owner_id: 任务归属用户；无归属时用 0（仅管理员可见聚合时可过滤）
    """
    oid = int(owner_id) if owner_id is not None else 0
    evt = {
        "id": next(_id_seq),
        "ts": datetime.now(timezone.utc).isoformat(),
        "owner_id": oid,
        "phone": mask_phone(phone),
        "action": action,
        "status": status,
        "level": (level or "info").lower(),
    }
    with _lock:
        _buffer.appendleft(evt)


def list_account_activity_for_user(*, viewer_id: int, is_admin: bool, limit: int = 15) -> list[dict[str, Any]]:
    lim = max(1, min(25, int(limit)))
    with _lock:
        items = list(_buffer)
    if is_admin:
        out = items[:lim]
    else:
        out = [e for e in items if int(e.get("owner_id") or 0) == int(viewer_id)][:lim]
    # 返回时去掉 owner_id（前端不需要）
    return [{k: v for k, v in e.items() if k != "owner_id"} for e in out]
