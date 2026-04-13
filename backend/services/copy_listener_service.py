from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session
from telethon import TelegramClient
from telethon.errors import (
    ApiIdInvalidError,
    FloodWaitError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    RPCError,
    SessionPasswordNeededError,
    PasswordHashInvalidError,
)

from models import CopyListenerAccount
from settings import TELEGRAM_API_HASH, TELEGRAM_API_ID

LISTENER_SESSIONS_DIR = Path(__file__).resolve().parent.parent / "sessions" / "listeners"
LISTENER_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

_pending: dict[str, dict[str, Any]] = {}
_lock = asyncio.Lock()


def _norm_phone(phone: str) -> str:
    digits = re.sub(r"\D+", "", str(phone or ""))
    if len(digits) < 8:
        raise ValueError("手机号格式无效")
    return f"+{digits}"


def _session_name(phone: str) -> str:
    d = re.sub(r"\D+", "", phone or "")
    return f"listener_{d}"


def _session_base(name: str) -> str:
    return str((LISTENER_SESSIONS_DIR / name).resolve())


def session_ready(session_name: str | None) -> bool:
    if not session_name:
        return False
    return (LISTENER_SESSIONS_DIR / f"{session_name}.session").is_file()


async def send_code_request(phone: str) -> dict[str, Any]:
    p = _norm_phone(phone)
    sn = _session_name(p)
    client = TelegramClient(_session_base(sn), TELEGRAM_API_ID, TELEGRAM_API_HASH)
    async with _lock:
        try:
            await client.connect()
            try:
                sent = await client.send_code_request(p)
            except PhoneNumberInvalidError:
                return {"ok": False, "error": "手机号无效或未开通 Telegram"}
            except ApiIdInvalidError:
                return {"ok": False, "error": "系统全局 Telegram API 配置无效"}
            except FloodWaitError as exc:
                return {"ok": False, "error": f"请求过频，请 {exc.seconds}s 后再试"}
            except RPCError as exc:
                return {"ok": False, "error": f"发送验证码失败: {exc.__class__.__name__}"}
            code_hash = getattr(sent, "phone_code_hash", None)
            if not code_hash:
                return {"ok": False, "error": "未获取到 phone_code_hash"}
            sent_type = type(getattr(sent, "type", None)).__name__ if getattr(sent, "type", None) else None
            next_type = type(getattr(sent, "next_type", None)).__name__ if getattr(sent, "next_type", None) else None
            timeout = getattr(sent, "timeout", None)
            _pending[p] = {
                "session_name": sn,
                "phone_code_hash": str(code_hash),
            }
            return {
                "ok": True,
                "phone": p,
                "session_name": sn,
                "phone_code_hash": str(code_hash),
                "sent_type": sent_type,
                "next_type": next_type,
                "timeout": timeout,
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}
        finally:
            await client.disconnect()


async def complete_login(
    db: Session,
    owner_id: int,
    phone: str,
    code: str | None = None,
    phone_code_hash: str | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    p = _norm_phone(phone)
    row = _pending.get(p)
    pch = str(phone_code_hash or "").strip() or (row or {}).get("phone_code_hash")
    pwd = str(password or "").strip()
    cod = str(code or "").strip()
    if not pwd and not pch:
        return {"ok": False, "error": "缺少 phone_code_hash，请先发送验证码"}
    session_name = (row or {}).get("session_name") or _session_name(p)
    client = TelegramClient(_session_base(session_name), TELEGRAM_API_ID, TELEGRAM_API_HASH)
    async with _lock:
        try:
            await client.connect()
            if pwd and not cod:
                try:
                    await client.sign_in(password=pwd)
                except PasswordHashInvalidError:
                    return {"ok": False, "error": "二步验证密码错误"}
                except FloodWaitError as exc:
                    return {"ok": False, "error": f"请求过频，请 {exc.seconds}s 后再试"}
                except RPCError as exc:
                    return {"ok": False, "error": f"登录失败: {exc.__class__.__name__}: {exc}"}
            else:
                if not cod:
                    return {"ok": False, "error": "请填写验证码"}
                try:
                    await client.sign_in(p, cod, phone_code_hash=pch)
                except PhoneCodeInvalidError:
                    return {"ok": False, "error": "验证码错误"}
                except PhoneCodeExpiredError:
                    return {"ok": False, "error": "验证码已过期，请重新发送"}
                except SessionPasswordNeededError:
                    return {"need_password": True}
                except FloodWaitError as exc:
                    return {"ok": False, "error": f"请求过频，请 {exc.seconds}s 后再试"}
                except RPCError as exc:
                    return {"ok": False, "error": f"登录失败: {exc.__class__.__name__}: {exc}"}
            if not await client.is_user_authorized():
                return {"ok": False, "error": "登录未授权"}
            cur = db.query(CopyListenerAccount).filter(CopyListenerAccount.phone == p).first()
            now = datetime.now(timezone.utc)
            if cur is None:
                cur = CopyListenerAccount(
                    owner_id=int(owner_id),
                    api_id=TELEGRAM_API_ID,
                    api_hash=TELEGRAM_API_HASH,
                    phone=p,
                    session_name=session_name,
                    status="active",
                    enabled=1,
                    last_error=None,
                    last_seen_at=now,
                )
            else:
                cur.api_id = TELEGRAM_API_ID
                cur.api_hash = TELEGRAM_API_HASH
                cur.session_name = session_name
                cur.status = "active"
                cur.enabled = 1
                cur.last_error = None
                cur.last_seen_at = now
            db.add(cur)
            db.commit()
            db.refresh(cur)
            _pending.pop(p, None)
            return {"ok": True, "id": cur.id, "phone": cur.phone, "session_name": cur.session_name}
        except Exception as e:
            return {"ok": False, "error": f"登录异常: {e}"}
        finally:
            await client.disconnect()
