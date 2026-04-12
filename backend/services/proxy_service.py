import json
import re
import sys
from pathlib import Path

# 自动加入项目根目录
sys.path.append(str(Path(__file__).resolve().parents[2]))

from sqlalchemy.orm import object_session

try:
    # 供 uvicorn 在 backend 目录运行时使用，避免重复加载 models
    from database import SessionLocal
    from logger import get_logger
    from models import AccountFile, Proxy
except ModuleNotFoundError:
    # 供直接执行脚本时使用：python backend/services/proxy_service.py
    from backend.database import SessionLocal
    from backend.logger import get_logger
    from backend.models import AccountFile, Proxy


log = get_logger("proxy_service")
PROXY_PATTERN = re.compile(r"^(?P<host>[^:@\s]+):(?P<port>\d+)@(?P<username>[^:@\s]+):(?P<password>.+)$")


def _add_proxy_if_new(db, host: str, port: int, username: str, password: str) -> int | None:
    """与库中 host+port+username+password 完全一致则跳过；新插入返回代理 id。"""
    existed = (
        db.query(Proxy)
        .filter(
            Proxy.host == host,
            Proxy.port == port,
            Proxy.username == username,
            Proxy.password == password,
        )
        .first()
    )
    if existed is not None:
        return None
    p = Proxy(
        host=host,
        port=port,
        username=username,
        password=password,
        status="idle",
        proxy_status="unknown",
    )
    db.add(p)
    db.flush()
    return p.id


def import_proxies_from_text(text: str) -> tuple[int, list[int]]:
    """
    从纯文本导入：每行一条，格式 host:port@username:password
    空行、# 开头注释行跳过；去重按完整凭证（含密码），与 JSON 导入一致。
    返回 (新增条数, 新代理 id 列表)。
    """
    db = SessionLocal()
    imported_count = 0
    new_ids: list[int] = []
    try:
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            match = PROXY_PATTERN.match(line)
            if not match:
                log.debug("proxy txt skip (format): %s", line[:80])
                continue
            host = match.group("host")
            port = int(match.group("port"))
            username = match.group("username")
            password = match.group("password").strip()
            nid = _add_proxy_if_new(db, host, port, username, password)
            if nid is not None:
                imported_count += 1
                new_ids.append(nid)
        db.commit()
    finally:
        db.close()
    return imported_count, new_ids


def import_proxies_from_file() -> tuple[int, list[int]]:
    root = Path(__file__).resolve().parents[2]
    file_path = root / "proxy_config_plus.json"

    if not file_path.exists():
        print(f"未找到文件: {file_path}")
        return 0, []

    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"JSON 解析失败: {exc}")
        return 0, []

    if not isinstance(data, dict):
        print("JSON 根结构必须是对象")
        return 0, []

    db = SessionLocal()
    imported_count = 0
    new_ids: list[int] = []
    try:
        for key, payload in data.items():
            try:
                # 只要 payload 中包含 proxy 字段就处理
                if not isinstance(payload, dict) or "proxy" not in payload:
                    print("SKIP:", key)
                    continue

                proxy_text = str(payload.get("proxy", "")).strip()
                if not proxy_text or proxy_text == "默认无代理":
                    print("SKIP:", key)
                    continue

                print("IMPORT:", proxy_text)
                match = PROXY_PATTERN.match(proxy_text)
                if not match:
                    print("SKIP:", key)
                    continue

                host = match.group("host")
                port = int(match.group("port"))
                username = match.group("username")
                password = match.group("password").strip()

                nid = _add_proxy_if_new(db, host, port, username, password)
                if nid is None:
                    print("SKIP:", key)
                    continue
                imported_count += 1
                new_ids.append(nid)
            except Exception:
                print("SKIP:", key)
                continue

        db.commit()
    finally:
        db.close()

    return imported_count, new_ids


def assign_proxy_to_account(account: AccountFile) -> dict:
    db = object_session(account)
    if db is None:
        return {"ok": False, "warning": "account 未绑定到数据库会话"}

    existed = db.query(Proxy).filter(Proxy.assigned_account_id == account.id).first()
    if existed is not None:
        account.proxy_id = existed.id
        account.proxy_type = "proxy"
        db.add(account)
        return {"ok": True, "proxy_id": existed.id}

    proxy = db.query(Proxy).filter(Proxy.status == "idle").order_by(Proxy.id.asc()).first()
    if proxy is None:
        account.proxy_type = "direct"
        account.proxy_id = None
        db.add(account)
        log.warning("assign proxy skipped: no idle proxy for account_id=%s", account.id)
        return {"ok": False, "warning": "无代理库存"}

    proxy.status = "used"
    proxy.assigned_account_id = account.id
    account.proxy_id = proxy.id
    account.proxy_type = "proxy"
    db.add(proxy)
    db.add(account)
    log.info("assign proxy success account_id=%s proxy_id=%s", account.id, proxy.id)
    return {"ok": True, "proxy_id": proxy.id}


if __name__ == "__main__":
    count, _ = import_proxies_from_file()
    print(f"导入完成: {count}")
