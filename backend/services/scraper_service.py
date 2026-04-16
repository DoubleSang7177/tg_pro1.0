from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    FloodWaitError,
    InviteHashExpiredError,
    RPCError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
    UserNotParticipantError,
)
from telethon.tl.types import PeerUser, User

from database import SessionLocal
from logger import get_logger
from models import ScraperTask
from services.scraper_account_service import resolve_session_path_for_scrape, telethon_session_arg

log = get_logger("scraper_service")

API_ID = int(os.getenv("TELEGRAM_API_ID", "20954937"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "d5a748cfdb420593307b5265c1864ba3")
RATE_SLEEP_SEC = float(os.getenv("SCRAPER_RATE_SLEEP", "0.05"))

BACKEND_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = BACKEND_ROOT / "data" / "scraper" / "results"

_scraper_lock = asyncio.Lock()


def _entity_display_name(entity: Any) -> str:
    title = getattr(entity, "title", None)
    if title:
        return str(title).strip()[:500]
    un = getattr(entity, "username", None)
    if un:
        return f"@{un}"[:500]
    return ""


def _fail_task(db, task_row: ScraperTask | None) -> None:
    if task_row is None:
        return
    try:
        task_row.status = "failed"
        db.commit()
    except Exception:
        db.rollback()


async def _cached_username(
    client: TelegramClient,
    sender_id: int,
    cache: dict[int, str | None],
) -> str | None:
    if sender_id in cache:
        return cache[sender_id]
    try:
        ent = await client.get_entity(sender_id)
        if isinstance(ent, User) and getattr(ent, "username", None):
            cache[sender_id] = f"@{ent.username}"
        else:
            cache[sender_id] = None
    except Exception as exc:
        log.debug("get_entity sender_id=%s failed: %s", sender_id, exc)
        cache[sender_id] = None
    return cache[sender_id]


async def scrape_group_users(group_id: str, days: int, max_messages: int) -> dict[str, Any]:
    """
    使用独立 Telethon session 采集群内近期发言用户的 username（去重后写入 txt）。
    结果持久化到 data/scraper/results/{task_id}.txt 并写入 ScraperTask。
    """
    if days < 1:
        return {"ok": False, "error": "days 须 >= 1"}
    if max_messages < 1:
        return {"ok": False, "error": "max_messages 须 >= 1"}

    async with _scraper_lock:
        db = SessionLocal()
        task_row: ScraperTask | None = None
        client: TelegramClient | None = None
        try:
            path_base = resolve_session_path_for_scrape(db)
            if path_base is None:
                return {
                    "ok": False,
                    "error": "未配置采集账号或 session 无效：请在侧栏「用户采集」中登录采集账号（手机号+验证码）",
                }

            client = TelegramClient(telethon_session_arg(path_base), API_ID, API_HASH)
            await client.connect()
            if not await client.is_user_authorized():
                return {
                    "ok": False,
                    "error": "采集账号 session 已失效：请重新在「用户采集」中登录采集账号",
                }

            try:
                entity = await client.get_entity(group_id)
            except UsernameNotOccupiedError:
                return {"ok": False, "error": "群组/频道不存在：用户名或链接无效"}
            except UsernameInvalidError:
                return {"ok": False, "error": "群组 ID 格式无效"}
            except ChannelPrivateError:
                return {"ok": False, "error": "私有群或频道：未加入或无权限查看"}
            except ChannelInvalidError:
                return {"ok": False, "error": "无效的群组/频道"}
            except InviteHashExpiredError:
                return {"ok": False, "error": "邀请链接已失效"}
            except UserNotParticipantError:
                return {"ok": False, "error": "未加入该群，无法读取消息历史"}
            except ValueError as exc:
                return {"ok": False, "error": f"无法解析群组标识: {exc}"}
            except RPCError as exc:
                return {"ok": False, "error": f"获取群组失败: {exc.__class__.__name__}: {exc}"}

            gname = _entity_display_name(entity) or str(group_id).strip()[:500]
            glink = str(group_id).strip()[:512]
            new_days = int(days)
            new_max_messages = int(max_messages)

            # 同一群组只保留一条采集记录：按“覆盖逻辑”决定是否更新历史结果。
            existing = (
                db.query(ScraperTask)
                .filter(ScraperTask.group_link == glink)
                .order_by(ScraperTask.id.desc())
                .first()
            )

            should_overwrite = True
            if existing and existing.status == "done":
                old_days = getattr(existing, "days", None)
                old_max_messages = getattr(existing, "max_messages", None)
                if old_days is not None:
                    old_days = int(old_days)
                if old_max_messages is not None:
                    old_max_messages = int(old_max_messages)

                # 新范围更大才覆盖；范围相同时 max_messages 更大才覆盖
                if old_days is not None:
                    if new_days < old_days:
                        should_overwrite = False
                    elif new_days == old_days:
                        if old_max_messages is not None and new_max_messages <= old_max_messages:
                            should_overwrite = False
                # old_days 为空时，无法判断“旧比新更大”，这里保守选择覆盖为新参数（以便结果可控）

            if existing and existing.status == "done" and not should_overwrite:
                # 不覆盖：直接复用旧结果（避免用更短范围抹掉历史）
                return {
                    "ok": True,
                    "task_id": existing.id,
                    "group": group_id,
                    "count": existing.user_count,
                    "file": existing.result_file,
                }

            # 覆盖/写入：更新 existing 或创建新记录
            if existing:
                task_row = existing
                task_row.group_name = gname
                task_row.days = new_days
                task_row.max_messages = new_max_messages
                task_row.result_file = ""
                task_row.user_count = 0
                task_row.status = "running"
                db.add(task_row)
                db.commit()
                db.refresh(task_row)
            else:
                task_row = ScraperTask(
                    group_link=glink,
                    group_name=gname,
                    days=new_days,
                    max_messages=new_max_messages,
                    result_file="",
                    user_count=0,
                    download_count=0,
                    status="running",
                )
                db.add(task_row)
                db.commit()
                db.refresh(task_row)

            task_id = task_row.id

            username_cache: dict[int, str | None] = {}
            usernames: set[str] = set()
            cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
            scanned = 0

            try:
                async for message in client.iter_messages(entity):
                    if message.date is None:
                        await asyncio.sleep(RATE_SLEEP_SEC)
                        continue
                    msg_dt = message.date
                    if msg_dt.tzinfo is None:
                        msg_dt = msg_dt.replace(tzinfo=timezone.utc)
                    else:
                        msg_dt = msg_dt.astimezone(timezone.utc)
                    if msg_dt < cutoff:
                        break
                    if scanned >= max_messages:
                        break
                    scanned += 1

                    sender_id = getattr(message, "sender_id", None)
                    if sender_id is None and message.from_id and isinstance(message.from_id, PeerUser):
                        sender_id = message.from_id.user_id
                    if sender_id is None:
                        await asyncio.sleep(RATE_SLEEP_SEC)
                        continue

                    uname = await _cached_username(client, int(sender_id), username_cache)
                    if uname:
                        usernames.add(uname)

                    await asyncio.sleep(RATE_SLEEP_SEC)

            except ChatAdminRequiredError:
                _fail_task(db, task_row)
                return {"ok": False, "error": "无权限读取该群消息历史（可能需要管理员权限）"}
            except ChannelPrivateError:
                _fail_task(db, task_row)
                return {"ok": False, "error": "无权限访问消息（私有或限制）"}
            except FloodWaitError as exc:
                _fail_task(db, task_row)
                return {"ok": False, "error": f"触发 Telegram 限速，请稍后重试（FloodWait {exc.seconds}s）"}

            RESULTS_DIR.mkdir(parents=True, exist_ok=True)
            out_path = RESULTS_DIR / f"{task_id}.txt"
            lines = sorted(usernames)
            out_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

            task_row.result_file = str(out_path.resolve())
            task_row.user_count = len(usernames)
            task_row.status = "done"
            db.commit()

            log.info(
                "scrape done task_id=%s group=%s days=%s max_messages=%s scanned=%s unique_usernames=%s file=%s",
                task_id,
                group_id,
                days,
                max_messages,
                scanned,
                len(usernames),
                out_path,
            )

            return {
                "ok": True,
                "task_id": task_id,
                "group": group_id,
                "count": len(usernames),
                "file": str(out_path.resolve()),
            }
        except Exception as exc:
            log.exception("scrape_group_users failed")
            _fail_task(db, task_row)
            return {"ok": False, "error": str(exc)[:500]}
        finally:
            try:
                if client is not None:
                    await client.disconnect()
            except Exception:
                log.debug("client.disconnect failed", exc_info=True)
            db.close()
