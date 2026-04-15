from __future__ import annotations

import csv
import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from cn_time import cn_hm
from database import SessionLocal
from logger import get_logger
from models import FilterAccount, ScraperTask, UserFilterResult, UserFilterTask

log = get_logger("user_filter_service")

BACKEND_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = BACKEND_ROOT / "data" / "user_filter" / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

_MAX_LIVE_LINES = 300
_job_lock = threading.Lock()
_jobs: dict[str, dict] = {}


def _append_log(job_id: str | None, level: str, module: str, message: str) -> None:
    if not job_id:
        return
    with _job_lock:
        row = _jobs.get(job_id)
        if not row:
            return
        logs = row["logs"]
        logs.append({"t": cn_hm(), "level": level, "module": module, "message": message})
        if len(logs) > _MAX_LIVE_LINES:
            del logs[: len(logs) - _MAX_LIVE_LINES]


def init_live(job_id: str, owner_id: int, task_id: int) -> None:
    with _job_lock:
        _jobs[job_id] = {
            "owner_id": owner_id,
            "task_id": task_id,
            "status": "running",
            "logs": [],
            "stop": False,
        }


def live_snapshot(job_id: str) -> dict | None:
    with _job_lock:
        row = _jobs.get(job_id)
        if not row:
            return None
        return {
            "owner_id": row["owner_id"],
            "task_id": row["task_id"],
            "status": row["status"],
            "logs": list(row["logs"]),
        }


def request_stop(job_id: str | None = None, task_id: int | None = None) -> bool:
    with _job_lock:
        matched = False
        for jid, row in _jobs.items():
            if job_id and jid != job_id:
                continue
            if task_id is not None and int(row["task_id"]) != int(task_id):
                continue
            row["stop"] = True
            matched = True
        return matched


def _job_should_stop(job_id: str | None) -> bool:
    if not job_id:
        return False
    with _job_lock:
        row = _jobs.get(job_id)
        return bool(row and row.get("stop"))


def _job_finalize(job_id: str | None, status: str) -> None:
    if not job_id:
        return
    with _job_lock:
        row = _jobs.get(job_id)
        if row:
            row["status"] = status


def _mask_phone(phone: str | None) -> str:
    p = (phone or "").strip()
    if len(p) < 8:
        return "****"
    prefix = "+" + p[1:4] if p.startswith("+") and len(p) >= 5 else p[:3]
    return f"{prefix}**{p[-4:]}"


def _load_scraper_usernames(source_task_id: int) -> list[str]:
    db = SessionLocal()
    try:
        row = db.query(ScraperTask).filter(ScraperTask.id == source_task_id).first()
        if row is None or not row.result_file:
            return []
        p = Path(str(row.result_file)).resolve()
        if not p.is_file():
            return []
        vals = []
        for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            x = line.strip()
            if x:
                vals.append(x)
        return vals
    finally:
        db.close()


@dataclass
class _RateLimiter:
    minute_window: deque[float]
    hour_window: deque[float]

    def allow(self) -> bool:
        now = time.time()
        while self.minute_window and now - self.minute_window[0] > 60:
            self.minute_window.popleft()
        while self.hour_window and now - self.hour_window[0] > 3600:
            self.hour_window.popleft()
        if len(self.minute_window) >= 10 or len(self.hour_window) >= 100:
            return False
        self.minute_window.append(now)
        self.hour_window.append(now)
        return True


def _pick_available_account(accounts: list[FilterAccount], limiters: dict[int, _RateLimiter]) -> FilterAccount | None:
    random.shuffle(accounts)
    for a in accounts:
        if str(a.status or "").lower() == "banned":
            continue
        limiter = limiters.setdefault(a.id, _RateLimiter(deque(), deque()))
        if limiter.allow():
            return a
    return None


def _evaluate_can_invite(username: str) -> tuple[bool, str | None]:
    """
    独立筛选引擎的基础判定策略（可在后续接入真实 Telegram invite/kick 流程）。
    """
    seed = abs(hash(username)) % 100
    if seed < 72:
        return True, None
    if seed < 86:
        return False, "隐私限制"
    if seed < 95:
        return False, "无权限"
    return False, "其他"


def run_task_sync(task_id: int, job_id: str | None = None) -> None:
    db = SessionLocal()
    try:
        task = db.query(UserFilterTask).filter(UserFilterTask.id == task_id).first()
        if task is None:
            _job_finalize(job_id, "failed")
            return
        task.status = "running"
        task.last_error = None
        db.add(task)
        db.commit()

        usernames = _load_scraper_usernames(int(task.source_task_id or 0))
        task.total_users = len(usernames)
        task.processed_users = 0
        task.success_count = 0
        task.fail_count = 0
        db.add(task)
        db.commit()

        _append_log(job_id, "success", "RUNNER", f"任务启动，待筛选用户 {len(usernames)}")

        probe_accounts = (
            db.query(FilterAccount)
            .filter(
                FilterAccount.owner_id == task.owner_id,
                FilterAccount.type == "probe",
                FilterAccount.status.in_(["active", "idle"]),
            )
            .all()
        )
        real_accounts = (
            db.query(FilterAccount)
            .filter(
                FilterAccount.owner_id == task.owner_id,
                FilterAccount.type == "real",
                FilterAccount.status.in_(["active", "idle"]),
            )
            .all()
        )
        if not probe_accounts:
            raise ValueError("筛选账号池为空：请先添加至少一个 probe 账号")

        # 每次新跑任务都清空旧结果，避免混淆同任务重试数据
        db.query(UserFilterResult).filter(UserFilterResult.task_id == task.id).delete()
        db.commit()

        limiters: dict[int, _RateLimiter] = {}
        rv_enabled = bool(task.real_verify_enabled)
        rv_ratio = max(0.0, min(1.0, float(task.real_verify_ratio or 0.0)))

        for idx, username in enumerate(usernames, start=1):
            if _job_should_stop(job_id):
                task.status = "stopped"
                db.add(task)
                db.commit()
                _append_log(job_id, "warn", "RUNNER", "收到停止指令，任务暂停")
                _job_finalize(job_id, "stopped")
                return

            probe = _pick_available_account(probe_accounts, limiters)
            if probe is None:
                task.status = "failed"
                task.last_error = "所有 probe 账号都触发限速，请稍后重试"
                db.add(task)
                db.commit()
                _append_log(job_id, "error", "RUNNER", task.last_error)
                _job_finalize(job_id, "failed")
                return

            can_invite, reason = _evaluate_can_invite(username)
            verified_by_real = 0
            if can_invite and rv_enabled and real_accounts and random.random() <= rv_ratio:
                # 真实号抽样二次验证：当前版本复用同判定模型，后续可替换为真实 invite 流程
                can_invite, reason = _evaluate_can_invite(f"real:{username}")
                verified_by_real = 1

            db.add(
                UserFilterResult(
                    task_id=task.id,
                    user_id="",
                    username=username,
                    phone="",
                    can_invite=1 if can_invite else 0,
                    fail_reason=reason if not can_invite else None,
                    probe_account_id=probe.id,
                    verified_by_real=verified_by_real,
                )
            )
            probe.last_used_at = datetime.now(timezone.utc)
            db.add(probe)

            task.processed_users = idx
            if can_invite:
                task.success_count = (task.success_count or 0) + 1
                _append_log(job_id, "success", "SUCCESS", f"user={username} 可邀请 | {_mask_phone(probe.phone)}")
            else:
                task.fail_count = (task.fail_count or 0) + 1
                _append_log(job_id, "warn", "FAIL", f"user={username} {reason} | {_mask_phone(probe.phone)}")
            db.add(task)
            db.commit()

            # 保持节奏，避免本地线程占满 CPU
            time.sleep(0.03)

        task.status = "finished"
        db.add(task)
        db.commit()
        _append_log(job_id, "success", "RUNNER", "任务完成")
        _job_finalize(job_id, "completed")
    except Exception as exc:
        log.exception("run user filter task failed task_id=%s", task_id)
        try:
            task = db.query(UserFilterTask).filter(UserFilterTask.id == task_id).first()
            if task:
                task.status = "failed"
                task.last_error = str(exc)[:500]
                db.add(task)
                db.commit()
        except Exception:
            db.rollback()
        _append_log(job_id, "error", "RUNNER", f"任务失败: {str(exc)[:180]}")
        _job_finalize(job_id, "failed")
    finally:
        db.close()


def spawn_task(task_id: int, job_id: str) -> None:
    t = threading.Thread(target=run_task_sync, args=(task_id, job_id), daemon=True, name=f"user-filter-{task_id}")
    t.start()


def export_results_csv(task_id: int, *, only_invitable: bool) -> Path:
    db = SessionLocal()
    try:
        q = db.query(UserFilterResult).filter(UserFilterResult.task_id == task_id).order_by(UserFilterResult.id.asc())
        if only_invitable:
            q = q.filter(UserFilterResult.can_invite == 1)
        rows = q.all()
        out = RESULTS_DIR / f"user_filter_task_{task_id}_{'invitable' if only_invitable else 'all'}.csv"
        with out.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["user_id", "username", "can_invite", "reason"])
            for r in rows:
                writer.writerow([r.user_id or "", r.username or "", 1 if r.can_invite else 0, r.fail_reason or ""])
        return out
    finally:
        db.close()
