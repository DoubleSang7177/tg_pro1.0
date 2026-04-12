"""
Copy 转发：Telethon（Bot）监听源频道/群，HTTP Bot API copyMessage 发到目标。
支持多任务并行、同 Bot 复用、暂停/恢复、启动时自动恢复 running 任务。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import threading
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from telethon import TelegramClient, events
from telethon.errors import RPCError

from database import SessionLocal
from models import CopyBot, CopyTask, ForwardRecord

log = logging.getLogger("copy_forward")

# Telethon 会话目录：backend/sessions/{session_name}.session
SESSIONS_DIR = Path(__file__).resolve().parent.parent / "sessions"


def session_file_path(session_name: str) -> Path:
    sn = (session_name or "").strip()
    return SESSIONS_DIR / f"{sn}.session"


def session_file_exists(session_name: str | None) -> bool:
    if not session_name or not str(session_name).strip():
        return False
    return session_file_path(str(session_name).strip()).is_file()


def bot_session_ready(bot: CopyBot) -> bool:
    return session_file_exists(getattr(bot, "session_name", None))


def reconcile_copy_bot_session_name(bot: CopyBot, db: Session) -> None:
    """若磁盘上已有 bot_{id}.session 但库中未记录，自动补全 session_name（兼容旧数据）。"""
    if bot_session_ready(bot):
        return
    cand = f"bot_{bot.id}"
    if session_file_exists(cand):
        bot.session_name = cand
        db.add(bot)
        db.commit()


def bootstrap_new_bot_session(bot_id: int, api_id: int, api_hash: str, bot_token: str) -> str:
    """首次用 BOT_TOKEN 登录并写入本地 session 文件，返回 session_name（如 bot_12）。"""
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    name = f"bot_{bot_id}"
    base = SESSIONS_DIR / name
    for p in SESSIONS_DIR.glob(f"{name}.session*"):
        try:
            p.unlink()
        except OSError:
            pass

    async def _go() -> None:
        c = TelegramClient(str(base), int(api_id), api_hash.strip())
        await c.start(bot_token=bot_token.strip())
        await c.disconnect()

    asyncio.run(_go())
    if not session_file_exists(name):
        raise RuntimeError("session 文件未生成")
    return name


def verify_session_connect(api_id: int, api_hash: str, session_name: str) -> None:
    if not session_file_exists(session_name):
        raise RuntimeError("session 文件不存在")
    base = str(SESSIONS_DIR / session_name.strip())

    async def _go() -> None:
        c = TelegramClient(base, int(api_id), api_hash.strip())
        try:
            await c.connect()
            if not await c.is_user_authorized():
                raise RuntimeError("session 无效或未授权")
        finally:
            try:
                await c.disconnect()
            except Exception:
                log.debug("verify_session disconnect", exc_info=True)

    asyncio.run(_go())

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
    else:
        log.info("copy | task=%s bot=%s | %s", task_id, bot_id, message)


def log_snapshot(limit: int = 200) -> list[dict[str, Any]]:
    return list(_log_deque)[-limit:]


def append_log(level: str, message: str, task_id: int | None = None, bot_id: int | None = None) -> None:
    _append_log(level, message, task_id=task_id, bot_id=bot_id)


def _message_hash(task_id: int, chat_id: int, message_id: int) -> str:
    raw = f"{task_id}:{chat_id}:{message_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _bot_api_copy_message(
    token: str,
    chat_id: int,
    from_chat_id: int,
    message_id: int,
    *,
    retries: int = 3,
) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/copyMessage"
    payload = json.dumps(
        {
            "chat_id": chat_id,
            "from_chat_id": from_chat_id,
            "message_id": message_id,
        }
    ).encode("utf-8")
    last_err: Exception | None = None
    delay = 1.0
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=90) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            if not body.get("ok"):
                desc = body.get("description") or str(body)
                raise RuntimeError(desc)
            return body
        except (urllib.error.URLError, TimeoutError, RuntimeError) as e:
            last_err = e
            if attempt < retries - 1:
                import time

                time.sleep(delay)
                delay = min(delay * 2, 8.0)
    raise last_err or RuntimeError("copyMessage failed")


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


async def _process_new_message(bot_id: int, event: Any) -> None:
    chat_id = int(event.chat_id)
    async with _registry_lock:
        tids = list(_source_index.get(bot_id, {}).get(chat_id, []))
    if not tids:
        return

    db = SessionLocal()
    try:
        bot = db.query(CopyBot).filter(CopyBot.id == bot_id).first()
        if not bot or bot.status != "active":
            return
        token = bot.bot_token.strip()

        for task_id in tids:
            rt = _runtime.get(task_id)
            if not rt:
                continue
            task = db.query(CopyTask).filter(CopyTask.id == task_id).first()
            if not task or task.status != "running":
                continue
            if int(rt["source_id"]) != chat_id:
                continue

            msg_id = int(event.id)
            mh = _message_hash(task_id, chat_id, msg_id)

            rec = ForwardRecord(task_id=task_id, message_hash=mh)
            db.add(rec)
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                continue

            tgt = int(rt["target_id"])
            try:
                await asyncio.to_thread(
                    _bot_api_copy_message,
                    token,
                    tgt,
                    chat_id,
                    msg_id,
                )
            except Exception as exc:
                err = str(exc)
                _append_log("error", f"copyMessage 失败: {err}", task_id=task_id, bot_id=bot_id)
                if "Unauthorized" in err or "401" in err or "not valid" in err.lower():
                    _mark_bot_error(db, bot, "Bot Token 无效或已失效")
                    _mark_task_error(db, task, "Bot Token 失效")
                    await pause_task(task_id, reason="bot_token_invalid")
                    return
                if "chat not found" in err.lower() or "PEER_ID_INVALID" in err:
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
            _append_log(
                "info",
                f"已转发 msg_id={msg_id} → target {tgt}",
                task_id=task_id,
                bot_id=bot_id,
            )
    finally:
        db.close()


def _make_handler(bot_id: int):
    async def handler(event: Any) -> None:
        try:
            await _process_new_message(bot_id, event)
        except Exception:
            log.exception("copy handler bot_id=%s", bot_id)

    return handler


async def _ensure_client(bot_id: int) -> TelegramClient | None:
    if bot_id in _bot_clients:
        return _bot_clients[bot_id]

    db = SessionLocal()
    try:
        row = db.query(CopyBot).filter(CopyBot.id == bot_id).first()
        if not row or row.status != "active":
            return None
        reconcile_copy_bot_session_name(row, db)
        api_id = int(row.api_id)
        api_hash = row.api_hash.strip()
        sn = (row.session_name or "").strip()
    finally:
        db.close()

    if not sn or not session_file_exists(sn):
        msg = "未生成 session 或 session 文件缺失，请重新创建 Bot 或导入 session"
        _append_log("error", msg, bot_id=bot_id)
        db2 = SessionLocal()
        try:
            row2 = db2.query(CopyBot).filter(CopyBot.id == bot_id).first()
            if row2:
                _mark_bot_error(db2, row2, msg)
        finally:
            db2.close()
        return None

    base = str(SESSIONS_DIR / sn)
    try:
        client = TelegramClient(base, api_id, api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            raise RuntimeError("session 未授权或已失效")
        _bot_clients[bot_id] = client

        h = _make_handler(bot_id)
        client.add_event_handler(h, events.NewMessage(incoming=True))
        _append_log("info", f"Telethon 已连接（session）bot_id={bot_id}", bot_id=bot_id)
        return client
    except Exception as exc:
        _append_log("error", f"连接 Bot(session) 失败: {exc}", bot_id=bot_id)
        db2 = SessionLocal()
        try:
            row2 = db2.query(CopyBot).filter(CopyBot.id == bot_id).first()
            if row2:
                _mark_bot_error(db2, row2, str(exc))
        finally:
            db2.close()
        return None


async def _disconnect_bot_if_idle(bot_id: int) -> None:
    async with _registry_lock:
        if _bot_task_ids.get(bot_id):
            return
    c = _bot_clients.pop(bot_id, None)
    if c:
        try:
            await c.disconnect()
        except Exception:
            log.debug("disconnect bot %s", bot_id, exc_info=True)
        _append_log("info", f"已断开空闲 bot_id={bot_id}", bot_id=bot_id)


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
    async with _registry_lock:
        rt = _runtime.pop(task_id, None)
        _active_task_ids.discard(task_id)
        if rt:
            _unregister_task_from_index(task_id, int(rt["bot_id"]), int(rt["source_id"]))
            bset = _bot_task_ids.setdefault(int(rt["bot_id"]), set())
            bset.discard(task_id)
            bot_id = int(rt["bot_id"])

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

        client = await _ensure_client(bot.id)
        if not client:
            return _fail_start_task(db, task, "无法连接 Telegram（请检查 session 与网络）", bot_id=bot.id)

        try:
            src_ent = await client.get_entity(task.source_channel.strip())
            tgt_ent = await client.get_entity(task.target_channel.strip())
        except RPCError as e:
            msg = f"解析频道失败: {e}"
            _mark_task_error(db, task, msg)
            _append_log("error", msg, task_id=task_id, bot_id=bot.id)
            return {"ok": False, "message": msg}
        except Exception as e:
            msg = f"频道无权限或不存在: {e}"
            _mark_task_error(db, task, msg)
            _append_log("error", msg, task_id=task_id, bot_id=bot.id)
            return {"ok": False, "message": msg}

        src_id = int(src_ent.id)
        tgt_id = int(tgt_ent.id)

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
                "source_id": src_id,
                "target_id": tgt_id,
            }
            mp = _source_index.setdefault(bot.id, {})
            mp.setdefault(src_id, []).append(task_id)
            _bot_task_ids.setdefault(bot.id, set()).add(task_id)
            _active_task_ids.add(task_id)

        _append_log("info", f"任务启动成功 source={src_id} target={tgt_id}", task_id=task_id, bot_id=bot.id)
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


async def force_disconnect_bot(bot_id: int) -> None:
    c = _bot_clients.pop(bot_id, None)
    if c:
        try:
            await c.disconnect()
        except Exception:
            log.debug("force disconnect bot %s", bot_id, exc_info=True)


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
