from __future__ import annotations

import asyncio
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

from models import AccountFactory
from settings import TELEGRAM_API_HASH, TELEGRAM_API_ID
from services.account_factory.factory_runner import append_factory_log
from services.account_factory.sms_provider import pick_provider_name

BACKEND_ROOT = Path(__file__).resolve().parents[2]
SESSIONS_DIR = BACKEND_ROOT / "sessions"
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
WARMUP_DAYS = 3
_register_lock = asyncio.Lock()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_phone_e164(phone: str) -> str:
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if len(digits) < 8:
        raise ValueError("手机号格式无效")
    return f"+{digits}"


def _session_base_for_factory(account_id: int) -> Path:
    return SESSIONS_DIR / f"factory_{int(account_id)}"


def _upsert_pending_row(db: Session, owner_id: int, phone_e164: str, country: str) -> AccountFactory:
    row = (
        db.query(AccountFactory)
        .filter(AccountFactory.owner_id == owner_id, AccountFactory.phone == phone_e164, AccountFactory.source == "factory")
        .order_by(AccountFactory.id.desc())
        .first()
    )
    if row is None:
        row = AccountFactory(owner_id=owner_id, phone=phone_e164, country=str(country or "ID").upper(), status="NEW", source="factory")
        db.add(row)
        db.flush()
    row.phone = phone_e164
    row.country = str(country or "ID").upper()
    row.status = "NEW"
    row.fail_reason = None
    row.warmup_until = None
    row.source = "factory"
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


async def send_factory_code(
    db: Session, *, owner_id: int, phone: str, country: str, strategy: str | None
) -> dict[str, Any]:
    try:
        append_factory_log("INFO", "REGISTER", "准备获取手机号...")
        phone_e164 = normalize_phone_e164(phone)
        row = _upsert_pending_row(db, owner_id, phone_e164, country)
        provider = pick_provider_name(strategy, [country])
        append_factory_log("SUCCESS", "REGISTER", f"获取号码成功: {phone_e164}（{provider}）")
        session_base = _session_base_for_factory(row.id)
        async with _register_lock:
            client = TelegramClient(str(session_base), TELEGRAM_API_ID, TELEGRAM_API_HASH)
            try:
                await client.connect()
                append_factory_log("INFO", "SMS", "请求发送验证码...")
                try:
                    sent = await client.send_code_request(phone_e164)
                except PhoneNumberInvalidError:
                    row.status = "FAILED"
                    row.fail_reason = "手机号无效或未开通 Telegram"
                    db.add(row)
                    db.commit()
                    append_factory_log("ERROR", "REGISTER", row.fail_reason)
                    return {"ok": False, "error": row.fail_reason}
                except ApiIdInvalidError:
                    err = "API_ID / API_HASH 配置无效"
                    append_factory_log("ERROR", "REGISTER", err)
                    return {"ok": False, "error": err}
                except FloodWaitError as exc:
                    err = f"请求过频，请 {exc.seconds}s 后再试"
                    append_factory_log("ERROR", "REGISTER", err)
                    return {"ok": False, "error": err}
                except RPCError as exc:
                    err = f"发送验证码失败: {exc.__class__.__name__}"
                    append_factory_log("ERROR", "REGISTER", err)
                    return {"ok": False, "error": err}
                hash_ = getattr(sent, "phone_code_hash", None)
                if not hash_:
                    err = "未获取到 phone_code_hash"
                    append_factory_log("ERROR", "REGISTER", err)
                    return {"ok": False, "error": err}
                append_factory_log("SUCCESS", "SMS", "验证码已发送")
                append_factory_log("INFO", "SMS", "等待验证码中...")
                return {"ok": True, "account_id": row.id, "phone": phone_e164, "phone_code_hash": hash_}
            except Exception as exc:
                err = f"发送验证码流程异常: {exc}"
                append_factory_log("ERROR", "REGISTER", err)
                return {"ok": False, "error": err}
            finally:
                await client.disconnect()
    except ValueError as exc:
        append_factory_log("ERROR", "REGISTER", str(exc))
        raise
    except Exception as exc:
        err = f"注册准备阶段异常: {exc}"
        append_factory_log("ERROR", "REGISTER", err)
        return {"ok": False, "error": err}


async def complete_factory_login(
    db: Session,
    *,
    owner_id: int,
    account_id: int,
    phone: str,
    code: str | None,
    phone_code_hash: str | None,
    password: str | None,
) -> dict[str, Any]:
    try:
        row = db.query(AccountFactory).filter(AccountFactory.id == int(account_id), AccountFactory.source == "factory").first()
        if row is None:
            err = "账号记录不存在"
            append_factory_log("ERROR", "REGISTER", err)
            return {"ok": False, "error": err}
        if int(row.owner_id) != int(owner_id):
            err = "无权操作该账号"
            append_factory_log("ERROR", "REGISTER", err)
            return {"ok": False, "error": err}
        phone_e164 = normalize_phone_e164(phone)
        if row.phone != phone_e164:
            err = "手机号与待注册记录不一致"
            append_factory_log("ERROR", "REGISTER", err)
            return {"ok": False, "error": err}

        cod = (code or "").strip()
        pch = (phone_code_hash or "").strip()
        pwd = (password or "").strip()
        if cod:
            append_factory_log("SUCCESS", "SMS", f"收到验证码: {cod}")
        session_base = _session_base_for_factory(row.id)
        append_factory_log("INFO", "LOGIN", "正在登录...")
        async with _register_lock:
            client = TelegramClient(str(session_base), TELEGRAM_API_ID, TELEGRAM_API_HASH)
            try:
                await client.connect()
                if pwd and not cod:
                    try:
                        await client.sign_in(password=pwd)
                    except PasswordHashInvalidError:
                        err = "密码错误"
                        append_factory_log("ERROR", "LOGIN", err)
                        return {"ok": False, "error": err}
                    except RPCError as exc:
                        err = f"登录失败: {exc.__class__.__name__}: {exc}"
                        append_factory_log("ERROR", "LOGIN", err)
                        return {"ok": False, "error": err}
                else:
                    if not cod or not pch:
                        err = "请先发送验证码并填写验证码"
                        append_factory_log("ERROR", "LOGIN", err)
                        return {"ok": False, "error": err}
                    try:
                        await client.sign_in(phone_e164, cod, phone_code_hash=pch)
                    except PhoneCodeInvalidError:
                        err = "验证码错误"
                        append_factory_log("ERROR", "LOGIN", err)
                        return {"ok": False, "error": err}
                    except PhoneCodeExpiredError:
                        err = "验证码已过期，请重新发送"
                        append_factory_log("ERROR", "LOGIN", err)
                        return {"ok": False, "error": err}
                    except SessionPasswordNeededError:
                        append_factory_log("ERROR", "LOGIN", "该账号需要二步验证密码")
                        return {"need_password": True}
                    except FloodWaitError as exc:
                        err = f"请求过频，请 {exc.seconds}s 后再试"
                        append_factory_log("ERROR", "LOGIN", err)
                        return {"ok": False, "error": err}
                    except RPCError as exc:
                        err = f"登录失败: {exc.__class__.__name__}: {exc}"
                        append_factory_log("ERROR", "LOGIN", err)
                        return {"ok": False, "error": err}
                if not await client.is_user_authorized():
                    err = "登录未生效，请重试"
                    append_factory_log("ERROR", "LOGIN", err)
                    return {"ok": False, "error": err}
            except Exception as exc:
                err = f"登录流程异常: {exc}"
                append_factory_log("ERROR", "LOGIN", err)
                return {"ok": False, "error": err}
            finally:
                await client.disconnect()

        now = _utcnow()
        sess_file = str(_session_base_for_factory(row.id).with_suffix(".session").resolve())
        row.session_path = sess_file
        row.status = "WARMING"
        row.warmup_until = now + timedelta(days=WARMUP_DAYS)
        row.fail_reason = None
        db.add(row)
        db.commit()
        append_factory_log("SUCCESS", "LOGIN", "session生成成功")
        append_factory_log("INFO", "WARMUP", "进入养号阶段（3天）")
        return {"ok": True, "account_id": row.id, "phone": row.phone, "session_path": row.session_path, "status": row.status}
    except ValueError as exc:
        append_factory_log("ERROR", "REGISTER", str(exc))
        raise
    except Exception as exc:
        err = f"登录收尾异常: {exc}"
        append_factory_log("ERROR", "REGISTER", err)
        return {"ok": False, "error": err}


def list_factory_accounts(db: Session, owner_id: int | None) -> list[AccountFactory]:
    q = db.query(AccountFactory).filter(AccountFactory.source == "factory").order_by(AccountFactory.id.desc())
    if owner_id is not None:
        q = q.filter(AccountFactory.owner_id == owner_id)
    return q.all()

