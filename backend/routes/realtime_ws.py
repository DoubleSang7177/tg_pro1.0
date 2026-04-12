"""WebSocket：账号状态实时推送。"""
from __future__ import annotations

import jwt
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy.orm import Session

from auth import JWT_ALGORITHM, JWT_SECRET
from database import SessionLocal
from models import User
from services.account_realtime import register_ws_client, unregister_ws_client

router = APIRouter(tags=["realtime"])


def _user_from_token(token: str | None) -> tuple[User | None, str | None]:
    if not token or not str(token).strip():
        return None, "missing_token"
    raw = str(token).strip()
    db: Session = SessionLocal()
    try:
        try:
            data = jwt.decode(raw, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            if data.get("typ") != "access":
                return None, "invalid_token"
            uid = int(data.get("sub", 0))
            user = db.query(User).filter(User.id == uid).first()
            if user is None:
                return None, "user_not_found"
            return user, None
        except Exception:
            user = db.query(User).filter(User.token == raw).first()
            if user is None:
                return None, "invalid_token"
            return user, None
    finally:
        db.close()


@router.websocket("/ws")
async def websocket_account_events(websocket: WebSocket):
    token = websocket.query_params.get("token")
    user, _err = _user_from_token(token)
    if user is None:
        await websocket.close(code=1008)
        return
    await websocket.accept()
    is_admin = user.role == "admin"
    register_ws_client(websocket, user.id, is_admin)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        unregister_ws_client(websocket)
