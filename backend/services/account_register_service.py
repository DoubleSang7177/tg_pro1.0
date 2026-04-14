from __future__ import annotations

import asyncio
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session
from telethon import TelegramClient
from telethon.errors import (
    ApiIdInvalidError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    RPCError,
    SessionPasswordNeededError,
)

from logger import get_logger
from models import AccountFile
from services.account_status import ST_COOLDOWN, ST_NORMAL
from settings import TELEGRAM_API_HASH, TELEGRAM_API_ID

log = get_logger("account_register")

BACKEND_ROOT = Path(__file__).resolve().parent.parent
SESSIONS_DIR = BACKEND_ROOT / "sessions"
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
WARMUP_READY_DAYS = 3
_register_lock = asyncio.Lock()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_phone_e164(phone: str) -> str:
    raw = (phone or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) < 8:
        raise ValueError("手机号格式无效")
    return f"+{digits}"


def _session_base_for_account(account_id: int) -> Path:
    return SESSIONS_DIR / f"account_{int(account_id)}"


def _session_file_for_account(account_id: int) -> Path:
    return _session_base_for_account(account_id).with_suffix(".session")


async def _disconnect_safely(client: TelegramClient | None) -> None:
    if client is None:
        return
    try:
        await client.disconnect()
    except Exception:
        pass


def _prepare_pending_register_row(db: Session, owner_id: int, phone_e164: str) -> AccountFile:
    row = (
        db.query(AccountFile)
        .filter(AccountFile.owner_id == owner_id, AccountFile.phone == phone_e164)
        .order_by(AccountFile.id.desc())
        .first()
    )
    if row is None:
        row = AccountFile(
            owner_id=owner_id,
            phone=phone_e164,
            filename="pending.session",
            saved_path=str(SESSIONS_DIR.resolve()),
            status=ST_COOLDOWN,
            source_type="register",
            register_status="pending",
            warmup_status="none",
        )
        db.add(row)
        db.flush()
    row.source_type = "register"
    row.register_status = "pending"
    row.warmup_status = "none"
    row.warmup_start_at = None
    row.ready_at = None
    session_file = _session_file_for_account(row.id).resolve()
    row.session_path = str(session_file)
    row.saved_path = str(session_file.parent)
    row.filename = session_file.name
    row.status = ST_COOLDOWN
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


async def send_register_code(db: Session, owner_id: int, phone: str) -> dict[str, Any]:
    phone_e164 = normalize_phone_e164(phone)
    row = _prepare_pending_register_row(db, owner_id, phone_e164)
    session_base = _session_base_for_account(row.id)
    async with _register_lock:
        client = TelegramClient(str(session_base), TELEGRAM_API_ID, TELEGRAM_API_HASH)
        try:
            await client.connect()
            try:
                sent = await client.send_code_request(phone_e164)
            except PhoneNumberInvalidError:
                return {"ok": False, "error": "手机号无效或未开通 Telegram"}
            except ApiIdInvalidError:
                return {"ok": False, "error": "API_ID / API_HASH 配置无效"}
            except FloodWaitError as exc:
                return {"ok": False, "error": f"请求过频，请 {exc.seconds}s 后再试"}
            except RPCError as exc:
                return {"ok": False, "error": f"发送验证码失败: {exc.__class__.__name__}"}
            hash_ = getattr(sent, "phone_code_hash", None)
            if not hash_:
                return {"ok": False, "error": "未获取到 phone_code_hash"}
            return {
                "ok": True,
                "account_id": row.id,
                "phone": phone_e164,
                "phone_code_hash": hash_,
            }
        finally:
            await client.disconnect()


async def complete_register_login(
    db: Session,
    *,
    owner_id: int,
    account_id: int,
    phone: str,
    code: str | None = None,
    phone_code_hash: str | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    row = db.query(AccountFile).filter(AccountFile.id == int(account_id)).first()
    if row is None:
        return {"ok": False, "error": "账号记录不存在"}
    if int(row.owner_id) != int(owner_id):
        return {"ok": False, "error": "无权操作该账号"}
    phone_e164 = normalize_phone_e164(phone)
    if row.phone and row.phone != phone_e164:
        return {"ok": False, "error": "手机号与待注册记录不一致"}
    cod = (code or "").strip()
    pch = (phone_code_hash or "").strip()
    pwd = (password or "").strip()
    session_base = _session_base_for_account(row.id)
    async with _register_lock:
        client = TelegramClient(str(session_base), TELEGRAM_API_ID, TELEGRAM_API_HASH)
        try:
            await client.connect()
            if pwd and not cod:
                try:
                    await client.sign_in(password=pwd)
                except PasswordHashInvalidError:
                    return {"ok": False, "error": "密码错误"}
                except FloodWaitError as exc:
                    return {"ok": False, "error": f"请求过频，请 {exc.seconds}s 后再试"}
                except RPCError as exc:
                    return {"ok": False, "error": f"登录失败: {exc.__class__.__name__}: {exc}"}
            else:
                if not cod or not pch:
                    return {"ok": False, "error": "请先发送验证码并填写验证码"}
                try:
                    await client.sign_in(phone_e164, cod, phone_code_hash=pch)
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
                return {"ok": False, "error": "登录未生效，请重试"}
        finally:
            await client.disconnect()

    now = _utcnow()
    row.phone = phone_e164
    row.source_type = "register"
    row.register_status = "success"
    row.status = ST_COOLDOWN
    row.warmup_status = "warming"
    row.warmup_start_at = now
    row.ready_at = None
    sess_file = _session_file_for_account(row.id).resolve()
    row.session_path = str(sess_file)
    row.saved_path = str(sess_file.parent)
    row.filename = sess_file.name
    row.last_update = now
    db.add(row)
    db.commit()
    return {"ok": True, "account_id": row.id, "phone": phone_e164, "session_path": row.session_path}


def _to_base_from_session_path(session_path: str | None) -> str | None:
    if not session_path:
        return None
    p = Path(session_path)
    if p.suffix == ".session":
        p = p.with_suffix("")
    return str(p)


async def _run_light_warmup(session_base: str) -> bool:
    client = TelegramClient(session_base, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return False
        dialogs = await client.get_dialogs(limit=10)
        _ = len(dialogs)
        await asyncio.sleep(random.uniform(1.0, 2.5))
        return True
    finally:
        await _disconnect_safely(client)


def run_warmup_cycle_once(db: Session) -> dict[str, int]:
    now = _utcnow()
    rows = (
        db.query(AccountFile)
        .filter(AccountFile.source_type == "register", AccountFile.register_status == "success")
        .all()
    )
    stats = {"warming": 0, "ready": 0, "failed": 0}
    for row in rows:
        warm = (row.warmup_status or "none").lower()
        if warm == "ready":
            continue
        if warm not in {"warming", "none"}:
            continue
        start_at = row.warmup_start_at or row.created_at or now
        ready_deadline = start_at + timedelta(days=WARMUP_READY_DAYS)
        if now >= ready_deadline:
            row.status = ST_NORMAL
            row.warmup_status = "ready"
            row.ready_at = now
            row.last_update = now
            db.add(row)
            stats["ready"] += 1
            continue

        session_base = _to_base_from_session_path(row.session_path)
        if not session_base:
            row.register_status = "failed"
            row.last_update = now
            db.add(row)
            stats["failed"] += 1
            continue
        try:
            ok = asyncio.run(_run_light_warmup(session_base))
            if not ok:
                row.register_status = "failed"
                row.last_update = now
                db.add(row)
                stats["failed"] += 1
                continue
            row.warmup_status = "warming"
            row.last_update = now
            db.add(row)
            stats["warming"] += 1
        except Exception:
            log.exception("warmup action failed account_id=%s phone=%s", row.id, row.phone)
            row.register_status = "failed"
            row.last_update = now
            db.add(row)
            stats["failed"] += 1
    db.commit()
    return stats
