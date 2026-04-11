from __future__ import annotations

import asyncio
import os
import re
from datetime import datetime, timezone
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
from models import ScraperAccount

log = get_logger("scraper_account")

API_ID = int(os.getenv("TELEGRAM_API_ID", "20954937"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "d5a748cfdb420593307b5265c1864ba3")

BACKEND_ROOT = Path(__file__).resolve().parent.parent
SCRAPER_DATA_DIR = BACKEND_ROOT / "data" / "scraper"

_auth_lock = asyncio.Lock()


def normalize_phone_e164(phone: str) -> str:
    raw = (phone or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits or len(digits) < 8:
        raise ValueError("手机号格式无效")
    return f"+{digits}"


def session_base_name(phone_e164: str) -> str:
    d = "".join(ch for ch in phone_e164 if ch.isdigit())
    return f"scraper_{d}"


def session_path_base(phone_e164: str) -> Path:
    """Telethon 使用的路径（不含 .session 后缀）。"""
    SCRAPER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return SCRAPER_DATA_DIR / session_base_name(phone_e164)


def telethon_session_arg(path_base: Path) -> str:
    """TelegramClient 传入的 name：无 .session 后缀。"""
    s = str(path_base)
    if s.endswith(".session"):
        return str(Path(s).with_suffix(""))
    return s


async def _disconnect_safely(client: TelegramClient | None) -> None:
    if client is None:
        return
    try:
        await client.disconnect()
    except Exception:
        log.debug("disconnect failed", exc_info=True)


def _unlink_session_files(path_base: Path) -> None:
    """删除 path_base（无 .session 后缀）对应的 .session 及 SQLite 辅助文件。"""
    base = path_base
    if base.suffix == ".session":
        base = base.with_suffix("")
    sess = base.with_suffix(".session")
    for f in (
        sess,
        Path(str(sess) + "-journal"),
        Path(str(sess) + "-wal"),
        Path(str(sess) + "-shm"),
    ):
        try:
            if f.is_file():
                f.unlink()
        except OSError:
            pass


def get_stored_account(db: Session) -> ScraperAccount | None:
    return db.query(ScraperAccount).order_by(ScraperAccount.id.asc()).first()


def resolve_session_path_for_scrape(db: Session) -> Path | None:
    """
    供采集任务使用：返回已登录账号的 session 路径基址（无 .session 后缀的 Path）。
    """
    row = get_stored_account(db)
    if not row or row.status != "active":
        return None
    raw = Path(row.session_file)
    if raw.suffix == ".session":
        raw = raw.with_suffix("")
    if raw.is_file():
        return raw
    p = raw.with_suffix(".session")
    if p.is_file():
        return raw
    return None


async def verify_session_authorized(path_base: Path) -> bool:
    client = TelegramClient(telethon_session_arg(path_base), API_ID, API_HASH)
    try:
        await client.connect()
        return await client.is_user_authorized()
    except Exception as exc:
        log.warning("verify_session_authorized failed: %s", exc)
        return False
    finally:
        await _disconnect_safely(client)


async def send_code_request(phone: str) -> dict[str, Any]:
    phone_e164 = normalize_phone_e164(phone)
    path_base = session_path_base(phone_e164)
    async with _auth_lock:
        client = TelegramClient(telethon_session_arg(path_base), API_ID, API_HASH)
        try:
            await client.connect()
            try:
                sent = await client.send_code_request(phone_e164)
            except PhoneNumberInvalidError as exc:
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
            return {"ok": True, "phone": phone_e164, "phone_code_hash": hash_}
        finally:
            await _disconnect_safely(client)


def _persist_scraper_account(db: Session, phone_e164: str, path_base: Path) -> dict[str, Any]:
    session_str = str(path_base.resolve())
    old = get_stored_account(db)
    if old and Path(old.session_file).resolve() != path_base.resolve():
        old_base = Path(old.session_file)
        if old_base.suffix == ".session":
            old_base = old_base.with_suffix("")
        _unlink_session_files(old_base)
    try:
        db.query(ScraperAccount).delete()
        now = datetime.now(timezone.utc)
        db.add(
            ScraperAccount(
                phone=phone_e164,
                session_file=session_str,
                status="active",
                created_at=now,
                updated_at=now,
            )
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    log.info("scraper account login ok phone=%s", phone_e164)
    return {"ok": True, "phone": phone_e164, "session_file": session_str}


async def complete_login(
    db: Session,
    phone: str,
    code: str | None = None,
    phone_code_hash: str | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    """
    验证码登录：code + phone_code_hash；
    二步验证：仅 password（与验证码同一 session 文件，须先完成验证码步并收到 need_password）。
    """
    phone_e164 = normalize_phone_e164(phone)
    path_base = session_path_base(phone_e164)
    pwd = (password or "").strip()
    cod = (code or "").strip()
    pch = (phone_code_hash or "").strip()

    async with _auth_lock:
        client = TelegramClient(telethon_session_arg(path_base), API_ID, API_HASH)
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
                if not await client.is_user_authorized():
                    return {"ok": False, "error": "二步验证未通过，请重试"}
                return _persist_scraper_account(db, phone_e164, path_base)

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
            return _persist_scraper_account(db, phone_e164, path_base)
        finally:
            await _disconnect_safely(client)


async def get_account_status(db: Session) -> dict[str, Any]:
    row = get_stored_account(db)
    if not row:
        return {"status": "not_logged"}
    path_base = Path(row.session_file)
    if path_base.suffix == ".session":
        path_base = path_base.with_suffix("")
    if not path_base.with_suffix(".session").is_file():
        row.status = "invalid"
        row.updated_at = datetime.now(timezone.utc)
        db.commit()
        return {"phone": row.phone, "status": "invalid"}
    ok = await verify_session_authorized(path_base)
    if not ok:
        row.status = "invalid"
        row.updated_at = datetime.now(timezone.utc)
        db.commit()
        return {"phone": row.phone, "status": "invalid"}
    if row.status != "active":
        row.status = "active"
        row.updated_at = datetime.now(timezone.utc)
        db.commit()
    return {"phone": row.phone, "status": "active"}
