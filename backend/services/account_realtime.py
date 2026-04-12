"""
账号状态 WebSocket 广播：与 account_files 状态变更同步，供前端 State Driven UI。
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import WebSocket

from services.account_status import ST_BANNED, ST_COOLDOWN, ST_DAILY_LIMITED, ST_NORMAL, ST_RISK_SUSPECTED

JWT_SECRET = os.environ.get(
    "JWT_SECRET",
    "tg-pro-jwt-secret-minimum-32-characters-long-for-hs256",
)
JWT_ALGORITHM = "HS256"

_ws_clients: list[tuple[WebSocket, int, bool]] = []


def _status_to_ui_status(raw: str | None) -> str:
    st = (raw or ST_NORMAL).lower()
    if st == ST_BANNED:
        return "banned"
    if st == ST_RISK_SUSPECTED:
        return "risk"
    if st in (ST_DAILY_LIMITED, ST_COOLDOWN):
        return "limited"
    return "active"


def _event_type_for_ui(ui: str) -> str:
    return {
        "active": "ACCOUNT_ACTIVE",
        "limited": "ACCOUNT_LIMITED",
        "risk": "ACCOUNT_RISK",
        "banned": "ACCOUNT_BANNED",
    }[ui]


async def broadcast_account_event(event: dict[str, Any]) -> None:
    """向有权查看该 owner 账号的连接推送（管理员收全部）。"""
    owner_id = event.get("owner_id")
    to_remove: list[int] = []
    for i, (ws, uid, is_admin) in enumerate(_ws_clients):
        if not is_admin and uid != owner_id:
            continue
        try:
            await ws.send_json(event)
        except Exception:
            to_remove.append(i)
    for i in reversed(to_remove):
        _ws_clients.pop(i)


def schedule_account_broadcast(account: Any) -> None:
    """
    在内存中已更新 account.status 后调用（建议在 db.commit 前，以便 last_update 一并持久化）。
    """
    now = datetime.now(timezone.utc)
    try:
        if hasattr(account, "last_update"):
            account.last_update = now
    except Exception:
        pass
    ui = _status_to_ui_status(getattr(account, "status", None))
    event: dict[str, Any] = {
        "type": _event_type_for_ui(ui),
        "account_id": int(getattr(account, "id", 0) or 0),
        "owner_id": int(getattr(account, "owner_id", 0) or 0),
        "phone": getattr(account, "phone", None),
        "status": getattr(account, "status", None),
        "ui_status": ui,
        "timestamp": now.isoformat(),
        "last_update": now.isoformat(),
    }
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(broadcast_account_event(event))


def register_ws_client(ws: WebSocket, user_id: int, is_admin: bool) -> None:
    _ws_clients.append((ws, user_id, is_admin))


def unregister_ws_client(ws: WebSocket) -> None:
    global _ws_clients
    _ws_clients = [c for c in _ws_clients if c[0] is not ws]
