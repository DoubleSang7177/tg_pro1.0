"""
Copy 转发：Telethon（Bot）session 监听源频道/群，同 session 以 MTProto 转发到目标（drop_author）。
支持多任务并行、同 Bot 复用、暂停/恢复、启动时自动恢复 running 任务。
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import threading
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from telethon import TelegramClient, events
from telethon.errors import AuthKeyDuplicatedError, RPCError
from telethon.tl.functions.channels import GetParticipantRequest, JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from database import SessionLocal
from models import CopyBot, CopyListenerAccount, CopyTask, ForwardRecord
from services import copy_listener_service as cls
from settings import TELEGRAM_API_HASH, TELEGRAM_API_ID

log = logging.getLogger("copy_forward")

# Telethon 会话目录：backend/sessions/{session_name}.session
SESSIONS_DIR = Path(__file__).resolve().parent.parent / "sessions"
COPY_CONNECT_TIMEOUT_SEC = float(os.getenv("COPY_CONNECT_TIMEOUT_SEC", "20"))


def normalize_session_name(session_name: str | None) -> str | None:
    """库中可能误存为 xxx.session，统一为不含后缀的逻辑名。"""
    if not session_name:
        return None
    s = str(session_name).strip()
    if s.lower().endswith(".session"):
        s = s[: -len(".session")].strip()
    return s or None


def session_file_path(session_name: str) -> Path:
    sn = normalize_session_name(session_name) or ""
    return SESSIONS_DIR / f"{sn}.session"


def session_file_exists(session_name: str | None) -> bool:
    sn = normalize_session_name(session_name)
    if not sn:
        return False
    return session_file_path(sn).is_file()


def bot_session_ready(bot: CopyBot) -> bool:
    return session_file_exists(getattr(bot, "session_name", None))


def reconcile_copy_bot_session_name(bot: CopyBot, db: Session) -> None:
    """补全 session_name：支持 bot_{id}.session、{api_id}.session（手动放入目录）。"""
    fixed = normalize_session_name(getattr(bot, "session_name", None))
    if fixed and fixed != (bot.session_name or "").strip():
        bot.session_name = fixed
        db.add(bot)
        db.commit()
    if bot_session_ready(bot):
        return
    cand = f"bot_{bot.id}"
    if session_file_exists(cand):
        bot.session_name = cand
        db.add(bot)
        db.commit()
        return
    if getattr(bot, "api_id", None):
        api_cand = str(int(bot.api_id))
        if session_file_exists(api_cand):
            bot.session_name = api_cand
            db.add(bot)
            db.commit()


def bootstrap_new_bot_session(bot_id: int, bot_token: str) -> str:
    """用 BOT_TOKEN 首次登录 Telegram，写入 backend/sessions/bot_{id}.session，返回 session_name。"""
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    name = f"bot_{bot_id}"
    base = SESSIONS_DIR / name
    _append_log("info", f"录入 Bot | 清理旧文件并准备登录 Telegram，目标 session：{name}", bot_id=bot_id)
    for p in SESSIONS_DIR.glob(f"{name}.session*"):
        try:
            p.unlink()
        except OSError:
            pass

    async def _go() -> None:
        _append_log("info", f"录入 Bot | 正在连接 Telegram（bot_token 登录）…", bot_id=bot_id)
        c = TelegramClient(str(base), TELEGRAM_API_ID, TELEGRAM_API_HASH)
        try:
            await c.start(bot_token=bot_token.strip())
            if not await c.is_user_authorized():
                raise RuntimeError("登录后仍未授权，请检查 bot_token 是否有效")
            me = await c.get_me()
            who = f"id={me.id}" + (f" @{me.username}" if getattr(me, "username", None) else "")
            _append_log("info", f"录入 Bot | 登录成功，账号 {who} bot={bool(getattr(me, 'bot', False))}", bot_id=bot_id)
        finally:
            try:
                await c.disconnect()
            except Exception:
                log.debug("bootstrap disconnect", exc_info=True)

    asyncio.run(_go())
    if not session_file_exists(name):
        raise RuntimeError("session 文件未生成")
    _append_log("info", f"录入 Bot | 已写入 {name}.session", bot_id=bot_id)
    return name


def verify_session_connect(session_name: str, *, bot_id: int | None = None) -> dict[str, Any]:
    """connect → is_user_authorized → get_me；失败抛错，成功返回账号摘要。"""
    sn = normalize_session_name(session_name) or ""
    path = session_file_path(sn)
    bid = bot_id
    _append_log("info", f"校验 session | 逻辑名={sn!r} 路径存在={path.is_file()}", bot_id=bid)

    if not session_file_exists(session_name):
        raise RuntimeError("session 文件不存在")

    base = str(SESSIONS_DIR / sn)

    async def _go() -> dict[str, Any]:
        _append_log("info", "校验 session | Telethon.connect() …", bot_id=bid)
        c = TelegramClient(base, TELEGRAM_API_ID, TELEGRAM_API_HASH)
        try:
            await c.connect()
            ok = await c.is_user_authorized()
            _append_log("info", f"校验 session | 连接结果 authorized={ok}", bot_id=bid)
            if not ok:
                raise RuntimeError("session 无效或未授权")
            me = await c.get_me()
            who = f"id={me.id}" + (f" @{me.username}" if getattr(me, "username", None) else "")
            _append_log("info", f"校验 session | get_me() → {who} bot={bool(getattr(me, 'bot', False))}", bot_id=bid)
            return {
                "id": me.id,
                "username": getattr(me, "username", None),
                "is_bot": bool(getattr(me, "bot", False)),
            }
        finally:
            try:
                await c.disconnect()
            except Exception:
                log.debug("verify_session disconnect", exc_info=True)

    return asyncio.run(_go())

LOG_CAP = 500
_log_deque: deque[dict[str, Any]] = deque(maxlen=LOG_CAP)

_registry_lock = asyncio.Lock()
_loop: asyncio.AbstractEventLoop | None = None
_loop_ready = threading.Event()

# bot_id -> TelegramClient
_bot_clients: dict[int, TelegramClient] = {}
# bot_id -> set(task_id) 已挂到 source 索引
_bot_task_ids: dict[int, set[int]] = {}
# task_id -> 运行时字段
_runtime: dict[int, dict[str, Any]] = {}
# bot_id -> source_chat_id -> list task_id
_source_index: dict[int, dict[int, list[int]]] = {}
_active_task_ids: set[int] = set()
# bot_id -> 保活任务（get_me 防止长时间无流量被服务端回收）
_keepalive_tasks: dict[int, asyncio.Task] = {}
# bot_id -> session_name（用于会话独占与释放）
_bot_session_name: dict[int, str] = {}
# session_name -> bot_id（进程内独占）
_session_owner: dict[str, int] = {}
# bot_id -> lock 文件描述符（跨进程独占）
_session_lock_fd: dict[int, int] = {}
_listener_clients: dict[int, TelegramClient] = {}
_listener_task_ids: dict[int, set[int]] = {}
_listener_source_index: dict[int, dict[int, list[int]]] = {}


def _session_lock_path(session_name: str) -> Path:
    sn = normalize_session_name(session_name) or ""
    return SESSIONS_DIR / f"{sn}.lock"


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _acquire_session_lock(session_name: str, bot_id: int) -> bool:
    sn = normalize_session_name(session_name) or ""
    if not sn:
        return False
    owner = _session_owner.get(sn)
    if owner is not None and owner != bot_id:
        return False
    lock_path = _session_lock_path(sn)
    if lock_path.exists():
        try:
            raw = lock_path.read_text(encoding="utf-8").strip()
            lock_pid = int(raw.splitlines()[0]) if raw else 0
        except Exception:
            lock_pid = 0
        if lock_pid > 0 and _pid_alive(lock_pid):
            return False
        try:
            lock_path.unlink()
        except OSError:
            return False
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
    except FileExistsError:
        return False
    except OSError:
        return False
    try:
        os.write(fd, f"{os.getpid()}\n{datetime.now(timezone.utc).isoformat()}".encode("utf-8"))
    except OSError:
        pass
    _session_owner[sn] = bot_id
    _session_lock_fd[bot_id] = fd
    _bot_session_name[bot_id] = sn
    return True


def _release_session_lock(bot_id: int) -> None:
    fd = _session_lock_fd.pop(bot_id, None)
    sn = _bot_session_name.pop(bot_id, None)
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass
    if sn:
        _session_owner.pop(sn, None)
        p = _session_lock_path(sn)
        try:
            if p.exists():
                p.unlink()
        except OSError:
            pass


def _append_log(level: str, message: str, task_id: int | None = None, bot_id: int | None = None) -> None:
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
        "task_id": task_id,
        "bot_id": bot_id,
    }
    _log_deque.append(entry)
    if level == "error":
        log.error("copy | task=%s bot=%s | %s", task_id, bot_id, message)
    elif level in ("warn", "warning"):
        log.warning("copy | task=%s bot=%s | %s", task_id, bot_id, message)
    else:
        log.info("copy | task=%s bot=%s | %s", task_id, bot_id, message)


def _event_text_preview(event: Any, limit: int = 50) -> str:
    """NewMessage 事件文本预览（无正文时返回空串）。"""
    msg = getattr(event, "message", None)
    if msg is not None:
        body = getattr(msg, "message", None)
        if body is not None and str(body).strip():
            s = str(body).replace("\r\n", " ").replace("\n", " ").strip()
            return s[:limit]
        raw_text = getattr(msg, "raw_text", None)
        if raw_text and str(raw_text).strip():
            s = str(raw_text).replace("\r\n", " ").replace("\n", " ").strip()
            return s[:limit]
    for attr in ("raw_text", "text"):
        v = getattr(event, attr, None)
        if v and str(v).strip():
            s = str(v).replace("\r\n", " ").replace("\n", " ").strip()
            return s[:limit]
    return ""


def log_snapshot(limit: int = 200) -> list[dict[str, Any]]:
    return list(_log_deque)[-limit:]


def append_log(level: str, message: str, task_id: int | None = None, bot_id: int | None = None) -> None:
    _append_log(level, message, task_id=task_id, bot_id=bot_id)


def _bot_has_active_tasks_in_memory(bot_id: int) -> bool:
    """内存中该 Bot 是否仍有已注册的 copy 任务（监听索引用）。"""
    s = _bot_task_ids.get(bot_id)
    return bool(s)


def bot_has_active_copy_tasks_sync(bot_id: int) -> bool:
    """
    同步判断：该 Bot 是否存在 status=running 的 copy 任务（以数据库为准，供路由/其它模块查询）。
    与内存索引双保险，避免进程内状态与 DB 不一致时误断开。
    """
    db = SessionLocal()
    try:
        n = (
            db.query(CopyTask)
            .filter(CopyTask.bot_id == bot_id, CopyTask.status == "running")
            .count()
        )
        return n > 0
    finally:
        db.close()


def _stop_keepalive(bot_id: int) -> None:
    t = _keepalive_tasks.pop(bot_id, None)
    if t is not None and not t.done():
        t.cancel()


async def _keepalive_loop(bot_id: int) -> None:
    """约每 30s 调用 get_me，降低长时间无交互被对端断开导致监听器失效的概率。"""
    tick = 0
    try:
        while True:
            await asyncio.sleep(30)
            tick += 1
            if bot_id not in _bot_clients:
                break
            c = _bot_clients.get(bot_id)
            if not c:
                break
            try:
                if c.is_connected():
                    await c.get_me()
                    _append_log("info", "[HEARTBEAT] 监听中...", bot_id=bot_id)
                    if tick % 2 == 0:
                        task_ids = list(_bot_task_ids.get(bot_id, set()))
                        for tid in task_ids:
                            rt = _runtime.get(tid) or {}
                            recv = int(rt.get("recv_count", 0))
                            succ = int(rt.get("success_count", 0))
                            fail = int(rt.get("fail_count", 0))
                            _append_log(
                                "info",
                                f"[STATS] 收到消息: {recv} 转发成功: {succ} 转发失败: {fail}",
                                task_id=tid,
                                bot_id=bot_id,
                            )
            except Exception as exc:
                log.debug("copy keepalive get_me failed bot_id=%s: %s", bot_id, exc)
    except asyncio.CancelledError:
        raise


def _ensure_keepalive_started(bot_id: int) -> None:
    existing = _keepalive_tasks.get(bot_id)
    if existing is not None and not existing.done():
        return
    _keepalive_tasks[bot_id] = asyncio.create_task(_keepalive_loop(bot_id))


def _message_hash(task_id: int, chat_id: int, message_id: int) -> str:
    raw = f"{task_id}:{chat_id}:{message_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _classify_entity_error(err: BaseException, *, side: str) -> str:
    s = str(err or "")
    low = s.lower()
    if "chat not found" in low or "peer_id_invalid" in low or "usernameinvaliderror" in low:
        return f"[ERROR] {side} 无法访问"
    if "channel_private" in low or "private" in low:
        return f"[ERROR] 未加入 {side} 群"
    if "chatadminrequired" in low or "forbidden" in low or "not enough rights" in low:
        return "[ERROR] 无读取权限" if side == "source" else "[ERROR] target 无法发送"
    return f"[ERROR] {side} 解析失败"


def _mark_bot_error(db: Session, bot: CopyBot, msg: str) -> None:
    bot.status = "error"
    bot.last_error = msg[:2000]
    db.add(bot)
    db.commit()


def _mark_task_error(db: Session, task: CopyTask, msg: str) -> None:
    task.status = "error"
    task.last_error = msg[:2000]
    db.add(task)
    db.commit()


def _fail_start_task(db: Session, task: CopyTask, msg: str, *, bot_id: int | None = None) -> dict[str, Any]:
    task.status = "error"
    task.last_error = msg[:2000]
    db.add(task)
    db.commit()
    _append_log("error", msg, task_id=task.id, bot_id=bot_id)
    return {"ok": False, "message": msg}


def recover_stale_starting_tasks() -> None:
    """进程启动时清理上次未完成的 starting，避免 UI 永久卡在启动中。"""
    db = SessionLocal()
    try:
        rows = db.query(CopyTask).filter(CopyTask.status == "starting").all()
        for t in rows:
            t.status = "idle"
            if not (t.last_error or "").strip():
                t.last_error = "上次启动未完成（如服务重启），请重新启动"
            db.add(t)
        if rows:
            db.commit()
    finally:
        db.close()


async def _process_new_message(bot_id: int, event: Any, *, listener_id: int | None = None) -> None:
    chat_id = int(event.chat_id)
    msg_id = int(event.id)
    _append_log("info", f"[FILTER] 开始过滤 msg_id={msg_id}", bot_id=bot_id)
    async with _registry_lock:
        if listener_id is not None:
            tids = list(_listener_source_index.get(listener_id, {}).get(chat_id, []))
        else:
            tids = list(_source_index.get(bot_id, {}).get(chat_id, []))
    if not tids:
        _append_log(
            "warn",
            f"[FILTER] 消息被过滤（无任务监听此 chat，或源频道 id 与配置不一致）| chat_id={chat_id}",
            bot_id=bot_id,
        )
        return
    _append_log(
        "info",
        f"[FILTER] 命中监听索引 | chat_id={chat_id} | 关联任务数={len(tids)}",
        bot_id=bot_id,
    )

    db = SessionLocal()
    try:
        bot = db.query(CopyBot).filter(CopyBot.id == bot_id).first()
        if not bot or bot.status != "active":
            return

        for task_id in tids:
            rt = _runtime.get(task_id)
            if not rt:
                continue
            rt["recv_count"] = int(rt.get("recv_count", 0)) + 1
            source_id = int(rt["source_id"])
            _append_log(
                "info",
                f"[FILTER] 当前消息来自: {chat_id} | 任务 source_id: {source_id} | task_id={task_id}",
                task_id=task_id,
                bot_id=bot_id,
            )
            task = db.query(CopyTask).filter(CopyTask.id == task_id).first()
            if not task or task.status != "running":
                st = getattr(task, "status", None) if task else None
                _append_log(
                    "warn",
                    f"[FILTER] 消息被过滤（任务未运行）| task_id={task_id} status={st!r}",
                    task_id=task_id,
                    bot_id=bot_id,
                )
                continue
            if source_id != chat_id:
                _append_log(
                    "warn",
                    "[FILTER] 消息被过滤（非目标源）",
                    task_id=task_id,
                    bot_id=bot_id,
                )
                continue

            mh = _message_hash(task_id, chat_id, msg_id)

            rec = ForwardRecord(task_id=task_id, message_hash=mh)
            db.add(rec)
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                _append_log(
                    "info",
                    f"[FILTER] 重复消息已处理，跳过转发 | msg_id={msg_id}",
                    task_id=task_id,
                    bot_id=bot_id,
                )
                continue

            tgt = int(rt["target_id"])
            _append_log(
                "info",
                f"[FILTER] 判断通过 | task_id={task_id} source={chat_id} target={tgt}",
                task_id=task_id,
                bot_id=bot_id,
            )
            sender_bot_id = int(rt.get("bot_id", 0))
            sender_client = _bot_clients.get(sender_bot_id)
            if not sender_client:
                sender_client = await _ensure_client(sender_bot_id)
            if not sender_client:
                _append_log("error", "[ERROR] 发送Bot未就绪，跳过本条", task_id=task_id, bot_id=sender_bot_id)
                continue
            try:
                if not sender_client.is_connected():
                    _append_log("warn", f"转发前重连 Telethon | task={task_id}", task_id=task_id, bot_id=bot_id)
                    await sender_client.connect()
                _append_log(
                    "info",
                    f"[FORWARD] 准备转发消息 id={msg_id} | target_id={tgt}",
                    task_id=task_id,
                    bot_id=bot_id,
                )
                await sender_client.forward_messages(
                    tgt,
                    msg_id,
                    from_peer=chat_id,
                    drop_author=True,
                    silent=True,
                )
            except AuthKeyDuplicatedError as exc:
                _append_log(
                    "error",
                    f"[ERROR] AuthKeyDuplicatedError msg_id={msg_id}: {exc}",
                    task_id=task_id,
                    bot_id=bot_id,
                )
                _mark_bot_error(db, bot, "AuthKeyDuplicatedError: session 被并发使用")
                _mark_task_error(db, task, "session 被并发占用，请确保只在单处运行")
                await pause_task(task_id, reason="auth_key_duplicated")
                return
            except Exception as exc:
                err = str(exc)
                rt["fail_count"] = int(rt.get("fail_count", 0)) + 1
                _append_log("error", f"[ERROR] 转发失败 msg_id={msg_id}: {err}", task_id=task_id, bot_id=bot_id)
                _append_log("error", f"[BOT] 转发失败 target={tgt} error={err}", task_id=task_id, bot_id=bot_id)
                if (
                    "AUTH_KEY" in err
                    or "SESSION" in err.upper()
                    or "401" in err
                    or "not authorized" in err.lower()
                    or "USER_DEACTIVATED" in err
                ):
                    _mark_bot_error(db, bot, f"Session 失效或账号异常: {err[:200]}")
                    _mark_task_error(db, task, "Session 失效，请重新登录或导入 session")
                    await pause_task(task_id, reason="session_invalid")
                    return
                if "chat not found" in err.lower() or "PEER_ID_INVALID" in err or "CHANNEL_PRIVATE" in err:
                    _mark_task_error(db, task, "目标或来源频道不可用/无权限")
                task.last_error = err[:500]
                db.add(task)
                db.commit()
                continue

            now = datetime.now(timezone.utc)
            day = _utc_day()
            if task.stats_utc_date != day:
                task.today_forwarded = 0
                task.stats_utc_date = day
            task.today_forwarded = (task.today_forwarded or 0) + 1
            task.total_forwarded = (task.total_forwarded or 0) + 1
            task.last_run_at = now
            task.last_error = None
            db.add(task)
            db.commit()
            rt["success_count"] = int(rt.get("success_count", 0)) + 1
            _append_log(
                "info",
                f"[SUCCESS] 转发成功 msg_id={msg_id} | target_id={tgt}",
                task_id=task_id,
                bot_id=bot_id,
            )
            _append_log("info", f"[BOT] 转发成功 target={tgt}", task_id=task_id, bot_id=bot_id)
    finally:
        db.close()


def _make_handler(bot_id: int):
    async def handler(event: Any) -> None:
        try:
            _append_log("info", "[DEBUG] 命中监听事件", bot_id=bot_id)
            chat_id = int(event.chat_id)
            msg_id = int(event.id)
            preview = _event_text_preview(event, 50)
            chat_title = None
            chat_type = None
            try:
                chat = await event.get_chat()
                chat_title = getattr(chat, "title", None) or getattr(chat, "username", None)
                chat_type = type(chat).__name__
            except Exception:
                pass
            _append_log(
                "info",
                f"[EVENT] 收到消息 | chat_id={chat_id} | msg_id={msg_id}",
                bot_id=bot_id,
            )
            _append_log(
                "info",
                (
                    "[EVENT] 来源群信息 | "
                    f"chat_id={chat_id} | chat_title={chat_title} | chat_type={chat_type} | "
                    f"sender={getattr(event, 'sender_id', None)} | message_id={msg_id}"
                ),
                bot_id=bot_id,
            )
            _append_log("info", f"[EVENT] 消息内容: {preview!r}", bot_id=bot_id)
            await _process_new_message(bot_id, event)
        except Exception:
            log.exception("copy handler bot_id=%s", bot_id)

    return handler


def _make_listener_handler(listener_id: int):
    async def handler(event: Any) -> None:
        try:
            _append_log("info", f"[LISTENER] 命中监听事件 listener_id={listener_id}")
            await _process_new_message(0, event, listener_id=listener_id)
        except Exception:
            log.exception("copy listener handler listener_id=%s", listener_id)

    return handler


async def _ensure_client(bot_id: int) -> TelegramClient | None:
    if bot_id in _bot_clients:
        _ensure_keepalive_started(bot_id)
        return _bot_clients[bot_id]

    db = SessionLocal()
    try:
        row = db.query(CopyBot).filter(CopyBot.id == bot_id).first()
        if not row or row.status != "active":
            return None
        reconcile_copy_bot_session_name(row, db)
        sn = normalize_session_name(row.session_name) or ""
    finally:
        db.close()

    if not sn or not session_file_exists(sn):
        msg = "未生成 session 或 session 文件缺失，请重新创建 Bot 或导入 session"
        _append_log("error", f"加载 session | {msg} session_name={sn!r}", bot_id=bot_id)
        db2 = SessionLocal()
        try:
            row2 = db2.query(CopyBot).filter(CopyBot.id == bot_id).first()
            if row2:
                _mark_bot_error(db2, row2, msg)
        finally:
            db2.close()
        return None

    base = str(SESSIONS_DIR / sn)
    if not _acquire_session_lock(sn, bot_id):
        _append_log("error", "session 正在被其他进程使用", bot_id=bot_id)
        return None
    _append_log("info", f"加载 session | 使用文件 {sn}.session，正在 connect() …", bot_id=bot_id)
    try:
        client = TelegramClient(base, TELEGRAM_API_ID, TELEGRAM_API_HASH)
        _append_log("info", f"[SYSTEM] connect timeout={COPY_CONNECT_TIMEOUT_SEC:.0f}s", bot_id=bot_id)
        await asyncio.wait_for(client.connect(), timeout=COPY_CONNECT_TIMEOUT_SEC)
        _append_log("info", "[SYSTEM] connect() 成功，开始校验授权…", bot_id=bot_id)
        auth = await asyncio.wait_for(client.is_user_authorized(), timeout=8.0)
        _append_log("info", f"加载 session | connect 完成 authorized={auth}", bot_id=bot_id)
        if not auth:
            await client.disconnect()
            raise RuntimeError("session 未授权或已失效")
        me = await asyncio.wait_for(client.get_me(), timeout=8.0)
        who = f"id={me.id}" + (f" @{me.username}" if getattr(me, "username", None) else "")
        _append_log("info", f"加载 session | get_me() → {who} bot={bool(getattr(me, 'bot', False))}", bot_id=bot_id)
        _bot_clients[bot_id] = client

        h = _make_handler(bot_id)
        client.add_event_handler(h, events.NewMessage(incoming=True))
        _append_log("info", f"[SYSTEM] 已注册 NewMessage 监听器 | bot_id={bot_id}", bot_id=bot_id)
        _ensure_keepalive_started(bot_id)
        return client
    except asyncio.TimeoutError:
        _append_log(
            "error",
            (
                "[ERROR] connect 超时：Telegram 连接未在限定时间内完成。"
                "常见原因：网络不通、被防火墙拦截、代理不可用、DNS 异常。"
            ),
            bot_id=bot_id,
        )
        db2 = SessionLocal()
        try:
            row2 = db2.query(CopyBot).filter(CopyBot.id == bot_id).first()
            if row2:
                _mark_bot_error(db2, row2, "connect timeout to Telegram")
        finally:
            db2.close()
        try:
            await client.disconnect()
        except Exception:
            pass
        _release_session_lock(bot_id)
        return None
    except AuthKeyDuplicatedError as exc:
        _append_log("error", f"AuthKeyDuplicatedError：session 已在其他进程/IP 使用 | {exc}", bot_id=bot_id)
        db2 = SessionLocal()
        try:
            row2 = db2.query(CopyBot).filter(CopyBot.id == bot_id).first()
            if row2:
                _mark_bot_error(db2, row2, "AuthKeyDuplicatedError: session 被并发使用")
        finally:
            db2.close()
        _release_session_lock(bot_id)
        return None
    except Exception as exc:
        _append_log("error", f"连接 Bot(session) 失败: {exc}", bot_id=bot_id)
        db2 = SessionLocal()
        try:
            row2 = db2.query(CopyBot).filter(CopyBot.id == bot_id).first()
            if row2:
                _mark_bot_error(db2, row2, str(exc))
        finally:
            db2.close()
        _release_session_lock(bot_id)
        return None


async def _disconnect_bot_if_idle(bot_id: int) -> None:
    async with _registry_lock:
        mem_busy = _bot_has_active_tasks_in_memory(bot_id)
    if mem_busy:
        return

    if bot_has_active_copy_tasks_sync(bot_id):
        msg = "[SYSTEM] 检测到空闲（内存无任务索引），但当前任务运行中，跳过断开"
        _append_log("warn", msg, bot_id=bot_id)
        log.warning("%s bot_id=%s", msg, bot_id)
        return

    async with _registry_lock:
        if _bot_has_active_tasks_in_memory(bot_id):
            return

    c = _bot_clients.pop(bot_id, None)
    if not c:
        return
    _stop_keepalive(bot_id)
    try:
        await c.disconnect()
    except Exception:
        log.debug("disconnect bot %s", bot_id, exc_info=True)
    _release_session_lock(bot_id)
    _append_log("info", f"已断开空闲 bot_id={bot_id}", bot_id=bot_id)


async def _ensure_listener_client(listener_id: int) -> TelegramClient | None:
    if listener_id in _listener_clients:
        return _listener_clients[listener_id]
    db = SessionLocal()
    try:
        row = db.query(CopyListenerAccount).filter(CopyListenerAccount.id == listener_id).first()
        if not row or not bool(row.enabled):
            return None
        if not cls.session_ready(row.session_name):
            return None
        base = str((cls.LISTENER_SESSIONS_DIR / row.session_name).resolve())
        client = TelegramClient(base, TELEGRAM_API_ID, TELEGRAM_API_HASH)
        await asyncio.wait_for(client.connect(), timeout=COPY_CONNECT_TIMEOUT_SEC)
        auth = await asyncio.wait_for(client.is_user_authorized(), timeout=8.0)
        if not auth:
            await client.disconnect()
            return None
        _listener_clients[listener_id] = client
        client.add_event_handler(_make_listener_handler(listener_id), events.NewMessage(incoming=True))
        row.last_seen_at = datetime.now(timezone.utc)
        row.last_error = None
        db.add(row)
        db.commit()
        _append_log("info", f"[LISTENER] 监听客户端就绪 listener_id={listener_id}")
        return client
    except Exception as exc:
        row = db.query(CopyListenerAccount).filter(CopyListenerAccount.id == listener_id).first()
        if row:
            row.status = "error"
            row.last_error = str(exc)[:500]
            db.add(row)
            db.commit()
        _append_log("error", f"[LISTENER] 连接失败 listener_id={listener_id}: {exc}")
        return None
    finally:
        db.close()


async def stop_listener(listener_id: int) -> None:
    async with _registry_lock:
        tids = list(_listener_task_ids.get(listener_id, set()))
    for tid in tids:
        await pause_task(tid, reason="listener_disabled")
    c = _listener_clients.pop(listener_id, None)
    if c:
        try:
            await c.disconnect()
        except Exception:
            log.debug("disconnect listener %s", listener_id, exc_info=True)
    async with _registry_lock:
        _listener_task_ids.pop(listener_id, None)
        _listener_source_index.pop(listener_id, None)


def _unregister_task_from_index(task_id: int, bot_id: int, source_id: int) -> None:
    mp = _source_index.setdefault(bot_id, {})
    lst = mp.get(source_id)
    if not lst:
        return
    mp[source_id] = [x for x in lst if x != task_id]
    if not mp[source_id]:
        del mp[source_id]


async def pause_task(task_id: int, *, reason: str | None = None) -> bool:
    bot_id: int | None = None
    listener_id: int | None = None
    async with _registry_lock:
        rt = _runtime.pop(task_id, None)
        _active_task_ids.discard(task_id)
        if rt:
            bid = int(rt["bot_id"])
            lid = rt.get("listener_id")
            if lid is not None:
                lid = int(lid)
                mp = _listener_source_index.setdefault(lid, {})
                lst = mp.get(int(rt["source_id"])) or []
                mp[int(rt["source_id"])] = [x for x in lst if x != task_id]
                if not mp[int(rt["source_id"])]:
                    mp.pop(int(rt["source_id"]), None)
                lset = _listener_task_ids.setdefault(lid, set())
                lset.discard(task_id)
                if not lset:
                    _listener_task_ids.pop(lid, None)
                listener_id = lid
            else:
                _unregister_task_from_index(task_id, bid, int(rt["source_id"]))
                bset = _bot_task_ids.setdefault(bid, set())
                bset.discard(task_id)
                if not bset:
                    _bot_task_ids.pop(bid, None)
            bot_id = bid

    db = SessionLocal()
    try:
        task = db.query(CopyTask).filter(CopyTask.id == task_id).first()
        if task:
            task.status = "paused"
            if reason:
                task.last_error = reason[:500]
            db.add(task)
            db.commit()
    finally:
        db.close()

    if reason:
        _append_log("warn", f"任务已暂停: {reason}", task_id=task_id)
    if bot_id is not None:
        await _disconnect_bot_if_idle(bot_id)
    if listener_id is not None:
        async with _registry_lock:
            has_tasks = bool(_listener_task_ids.get(listener_id))
        if not has_tasks:
            c = _listener_clients.pop(listener_id, None)
            if c:
                try:
                    await c.disconnect()
                except Exception:
                    log.debug("disconnect listener %s", listener_id, exc_info=True)
    return True


async def start_task(task_id: int) -> dict[str, Any]:
    async with _registry_lock:
        if task_id in _active_task_ids:
            return {"ok": True, "message": "已在运行"}

    db = SessionLocal()
    try:
        task = db.query(CopyTask).filter(CopyTask.id == task_id).first()
        if not task:
            return {"ok": False, "message": "任务不存在"}

        resume = task.status == "running"
        if not resume:
            if task.status not in ("idle", "paused", "error", "starting"):
                return {"ok": False, "message": f"当前状态不可启动: {task.status}"}

        bot = db.query(CopyBot).filter(CopyBot.id == task.bot_id).first()
        if not bot:
            return _fail_start_task(db, task, "机器人不存在", bot_id=None)
        reconcile_copy_bot_session_name(bot, db)
        if bot.status != "active":
            return _fail_start_task(db, task, "机器人处于 ERROR 状态，请先修复或重建", bot_id=bot.id)

        if not bot_session_ready(bot):
            msg = "未生成 session 或 session 文件缺失，请导入 session 或重新创建 Bot"
            _mark_bot_error(db, bot, msg)
            return _fail_start_task(db, task, msg, bot_id=bot.id)

        sender_client = await _ensure_client(bot.id)
        if not sender_client:
            return _fail_start_task(db, task, "无法连接 Telegram（请检查 session 与网络）", bot_id=bot.id)
        listener_id: int | None = int(task.listener_id) if task.listener_id else None
        if listener_id is None:
            auto_listener = (
                db.query(CopyListenerAccount)
                .filter(CopyListenerAccount.enabled == 1, CopyListenerAccount.status == "active")
                .order_by(CopyListenerAccount.last_seen_at.asc().nullsfirst(), CopyListenerAccount.id.asc())
                .first()
            )
            if auto_listener:
                listener_id = int(auto_listener.id)
                task.listener_id = listener_id
                db.add(task)
                db.commit()
                _append_log("info", f"[LISTENER] 自动分配 listener_id={listener_id}", task_id=task_id, bot_id=bot.id)
            else:
                return _fail_start_task(db, task, "未配置可用监听账号，Bot 不负责监听", bot_id=bot.id)
        listener_client = await _ensure_listener_client(listener_id)
        if not listener_client:
            return _fail_start_task(db, task, "监听账号不可用或连接失败", bot_id=bot.id)

        source = task.source_channel.strip()
        try:
            src_ent = await listener_client.get_entity(source)
        except Exception as e:
            msg = f"[MONITOR ERROR] 无法获取频道信息: {e}"
            _mark_task_error(db, task, msg)
            _append_log("error", msg, task_id=task_id, bot_id=bot.id)
            return {"ok": False, "message": msg}

        try:
            await listener_client(JoinChannelRequest(src_ent))
            _append_log("info", f"[SYSTEM] 已自动加入频道 {source}", task_id=task_id, bot_id=bot.id)
        except Exception as e:
            # 私有邀请链接：t.me/+xxxx 或 joinchat/xxxx
            src_low = source.lower()
            invite_hash = ""
            if "t.me/+" in src_low:
                invite_hash = source.split("t.me/+", 1)[1].split("?", 1)[0].strip().lstrip("+")
            elif "joinchat/" in src_low:
                invite_hash = source.split("joinchat/", 1)[1].split("?", 1)[0].strip()
            if invite_hash:
                try:
                    await listener_client(ImportChatInviteRequest(invite_hash))
                    _append_log("info", f"[SYSTEM] 已通过邀请链接加入频道 {source}", task_id=task_id, bot_id=bot.id)
                except Exception as e2:
                    _append_log("warn", f"[SYSTEM] 加入频道失败: {e2}", task_id=task_id, bot_id=bot.id)
            else:
                _append_log("warn", f"[SYSTEM] 加入频道失败: {e}", task_id=task_id, bot_id=bot.id)

        src_id = int(src_ent.id)
        try:
            await listener_client(GetParticipantRequest(channel=src_ent, participant="me"))
        except Exception as e:
            msg = f"[MONITOR ERROR] 监听账号不在该频道，无法监听: {e}"
            _mark_task_error(db, task, msg)
            _append_log("error", msg, task_id=task_id, bot_id=bot.id)
            return {"ok": False, "message": msg}

        try:
            tgt_ent = await sender_client.get_entity(task.target_channel.strip())
        except RPCError as e:
            msg = f"{_classify_entity_error(e, side='source')} / {_classify_entity_error(e, side='target')} | {e}"
            _mark_task_error(db, task, msg)
            _append_log("error", msg, task_id=task_id, bot_id=bot.id)
            return {"ok": False, "message": msg}
        except Exception as e:
            msg = f"{_classify_entity_error(e, side='source')} / {_classify_entity_error(e, side='target')} | {e}"
            _mark_task_error(db, task, msg)
            _append_log("error", msg, task_id=task_id, bot_id=bot.id)
            return {"ok": False, "message": msg}

        tgt_id = int(tgt_ent.id)
        _append_log(
            "info",
            f"[MONITOR] 监听初始化 | source_id={src_id} | target_id={tgt_id}",
            task_id=task_id,
            bot_id=bot.id,
        )
        try:
            entity = await listener_client.get_entity(src_id)
            _append_log(
                "info",
                (
                    "[MONITOR] 监听群信息 | "
                    f"id={getattr(entity, 'id', None)} | "
                    f"title={getattr(entity, 'title', None)} | "
                    f"username={getattr(entity, 'username', None)} | "
                    f"type={type(entity).__name__}"
                ),
                task_id=task_id,
                bot_id=bot.id,
            )
        except Exception as e:
            msg = f"[MONITOR ERROR] 无法获取source群: {e}"
            _mark_task_error(db, task, msg)
            _append_log("error", msg, task_id=task_id, bot_id=bot.id)
            return {"ok": False, "message": msg}

        try:
            dialogs = await listener_client.get_dialogs()
            for d in dialogs:
                _append_log(
                    "info",
                    f"[DIALOG] {getattr(d, 'id', None)} | {getattr(d, 'name', None)}",
                    task_id=task_id,
                    bot_id=bot.id,
                )
        except Exception as e:
            _append_log("warn", f"[DIALOG] 拉取失败: {e}", task_id=task_id, bot_id=bot.id)

        db.refresh(task)
        if task.status not in ("starting", "running"):
            hint = "启动已中止" if task.status in ("paused", "idle") else f"状态已变为 {task.status}"
            _append_log("warn", hint, task_id=task_id, bot_id=bot.id)
            return {"ok": False, "message": hint}

        task.status = "running"
        task.last_error = None
        db.add(task)
        db.commit()

        async with _registry_lock:
            _runtime[task_id] = {
                "bot_id": bot.id,
                "listener_id": listener_id,
                "source_id": src_id,
                "target_id": tgt_id,
                "recv_count": 0,
                "success_count": 0,
                "fail_count": 0,
            }
            if listener_id is not None:
                mp = _listener_source_index.setdefault(listener_id, {})
                mp.setdefault(src_id, []).append(task_id)
                _listener_task_ids.setdefault(listener_id, set()).add(task_id)
            else:
                mp = _source_index.setdefault(bot.id, {})
                mp.setdefault(src_id, []).append(task_id)
                _bot_task_ids.setdefault(bot.id, set()).add(task_id)
            _active_task_ids.add(task_id)

        _append_log(
            "info",
            f"[TASK] copy任务启动 | source={src_id} target={tgt_id}",
            task_id=task_id,
            bot_id=bot.id,
        )
        _append_log(
            "info",
            (
                "[TASK DEBUG]\n"
                f"source: {src_id}\n"
                f"target: {tgt_id}\n"
                f"bot_id: {bot.id}\n"
                f"listener_id: {listener_id or '-'}\n"
                "监听状态: 已注册"
            ),
            task_id=task_id,
            bot_id=bot.id,
        )
        _append_log("info", f"[SYSTEM] 监听已启动 source={src_id}", task_id=task_id, bot_id=bot.id)
        _append_log("info", f"[LISTENER] 开始监听 source={src_id}", task_id=task_id, bot_id=bot.id)
        return {"ok": True, "message": "已启动"}
    finally:
        db.close()


async def delete_task_runtime(task_id: int) -> None:
    await pause_task(task_id, reason=None)
    async with _registry_lock:
        _runtime.pop(task_id, None)
        _active_task_ids.discard(task_id)


def _wait_loop_ready(timeout: float = 20.0) -> None:
    if _loop_ready.is_set():
        return
    _loop_ready.wait(timeout=timeout)


async def _start_task_safe(task_id: int) -> None:
    try:
        await start_task(task_id)
    except Exception:
        log.exception("start_task failed task_id=%s", task_id)
        db = SessionLocal()
        try:
            t = db.query(CopyTask).filter(CopyTask.id == task_id).first()
            if t and t.status == "starting":
                t.status = "error"
                t.last_error = "启动过程中发生未预期错误"
                db.add(t)
                db.commit()
        finally:
            db.close()


def schedule_start_task(task_id: int) -> None:
    global _loop
    _wait_loop_ready()
    if _loop and _loop.is_running():

        async def _go():
            await _start_task_safe(task_id)

        asyncio.run_coroutine_threadsafe(_go(), _loop)
    else:

        async def _run():
            await _start_task_safe(task_id)

        asyncio.run(_run())


def schedule_pause_task(task_id: int) -> None:
    global _loop
    _wait_loop_ready()
    if _loop and _loop.is_running():

        async def _go():
            await pause_task(task_id)

        asyncio.run_coroutine_threadsafe(_go(), _loop)
    else:

        async def _run():
            await pause_task(task_id)

        asyncio.run(_run())


def schedule_stop_listener(listener_id: int) -> None:
    global _loop
    _wait_loop_ready()
    if _loop and _loop.is_running():

        async def _go():
            await stop_listener(listener_id)

        asyncio.run_coroutine_threadsafe(_go(), _loop)
    else:

        async def _run():
            await stop_listener(listener_id)

        asyncio.run(_run())


async def force_disconnect_bot(bot_id: int) -> None:
    _stop_keepalive(bot_id)
    c = _bot_clients.pop(bot_id, None)
    if c:
        try:
            await c.disconnect()
        except Exception:
            log.debug("force disconnect bot %s", bot_id, exc_info=True)
    _release_session_lock(bot_id)


def wait_force_disconnect_bot(bot_id: int, *, timeout: float = 20.0) -> None:
    _wait_loop_ready()
    if _loop and _loop.is_running():
        fut = asyncio.run_coroutine_threadsafe(force_disconnect_bot(bot_id), _loop)
        try:
            fut.result(timeout=timeout)
        except Exception:
            log.exception("wait_force_disconnect_bot bot_id=%s", bot_id)
    else:
        asyncio.run(force_disconnect_bot(bot_id))


def wait_pause_task(task_id: int, *, timeout: float = 45.0) -> None:
    """同步等待任务从运行时注销（删除任务 / 删 Bot 前调用）。"""
    global _loop
    _wait_loop_ready()
    if _loop and _loop.is_running():

        async def _go():
            await pause_task(task_id)

        fut = asyncio.run_coroutine_threadsafe(_go(), _loop)
        fut.result(timeout=timeout)
        return

    async def _run():
        await pause_task(task_id)

    asyncio.run(_run())


async def resume_all_running_tasks() -> None:
    db = SessionLocal()
    try:
        rows = db.query(CopyTask).filter(CopyTask.status == "running").order_by(CopyTask.id.asc()).all()
        ids = [r.id for r in rows]
    finally:
        db.close()
    for tid in ids:
        try:
            async with _registry_lock:
                if tid in _active_task_ids:
                    continue
            await start_task(tid)
        except Exception:
            log.exception("resume task %s failed", tid)
            db = SessionLocal()
            try:
                t = db.query(CopyTask).filter(CopyTask.id == tid).first()
                if t:
                    t.status = "error"
                    t.last_error = "自动恢复失败"
                    db.add(t)
                    db.commit()
            finally:
                db.close()


def run_background_loop() -> None:
    """在独立线程中运行 asyncio 循环，供启动恢复与路由调度。"""
    global _loop

    async def _main():
        global _loop
        _loop = asyncio.get_running_loop()
        _loop_ready.set()
        await asyncio.to_thread(recover_stale_starting_tasks)
        await resume_all_running_tasks()
        while True:
            await asyncio.sleep(3600)

    asyncio.run(_main())


def spawn_copy_forward_thread() -> None:
    t = threading.Thread(target=run_background_loop, name="copy-forward-loop", daemon=True)
    t.start()
