import json
import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from database import SessionLocal
from models import AccountFile, AccountPath, Proxy, User


PATHS = [
    r"C:\Users\葛萨桑桑\Desktop\TGTDATAaccount\niya",
    r"C:\Users\葛萨桑桑\Desktop\TGTDATAaccount\ss",
    r"C:\Users\葛萨桑桑\Desktop\TGTDATAaccount",
    r"C:\Users\葛萨桑桑\Desktop\TGTDATAaccount\tg 20 old",
    r"C:\Users\葛萨桑桑\Desktop\TGTDATAaccount\Thailand new 10",
    r"C:\Users\葛萨桑桑\Desktop\TGTDATAaccount\Tailand new 10 niya",
    r"C:\Users\葛萨桑桑\Desktop\TGTDATAaccount\teq50",
    r"C:\Users\葛萨桑桑\Desktop\TGTDATAaccount\Thaland old local session",
    r"C:\Users\葛萨桑桑\Desktop\TGTDATAaccount\Tailand old 20250915",
]

PROXY_PATTERN = re.compile(r"^(?P<host>[^:@\s]+):(?P<port>\d+)@(?P<username>[^:@\s]+):(?P<password>.+)$")


def parse_phone(name: str) -> str | None:
    digits = "".join(ch for ch in name if ch.isdigit())
    return f"+{digits}" if digits else None


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    proxy_json_path = root / "proxy_config_plus.json"

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.username == "admin").first()
        owner_id = user.id if user else 1

        # 1) 写入路径
        added_paths = 0
        for p in PATHS:
            existed = db.query(AccountPath).filter(AccountPath.path == p).first()
            if existed is None:
                db.add(AccountPath(path=p))
                added_paths += 1
        db.commit()

        # 2) 扫描入库账号
        added_accounts = 0
        for p in PATHS:
            base = Path(p)
            if not base.exists() or not base.is_dir():
                continue
            for sub in base.iterdir():
                if not sub.is_dir():
                    continue
                phone = parse_phone(sub.name)
                if not phone:
                    continue
                existed = db.query(AccountFile).filter(AccountFile.phone == phone).first()
                if existed is None:
                    db.add(
                        AccountFile(
                            owner_id=owner_id,
                            phone=phone,
                            filename=sub.name,
                            saved_path=str(sub.resolve()),
                            status="active",
                            proxy_type="direct",
                        )
                    )
                    added_accounts += 1
                else:
                    existed.saved_path = str(sub.resolve())
                    db.add(existed)
        db.commit()

        # 3) 读取旧代理并绑定（重置旧代理数据后按完整代理值重新导入）
        bound_count = 0
        created_proxy_count = 0
        reset_proxy_count = 0
        if proxy_json_path.exists():
            # 先清空旧绑定，避免历史错误数据影响重跑
            account_rows = db.query(AccountFile).all()
            for account in account_rows:
                account.proxy_id = None
                account.proxy_type = "direct"
                db.add(account)
            db.commit()

            reset_proxy_count = db.query(Proxy).count()
            db.query(Proxy).delete()
            db.commit()

            payload = json.loads(proxy_json_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                for key, data in payload.items():
                    if not isinstance(data, dict):
                        continue
                    proxy_text = str(data.get("proxy", "")).strip()
                    if not proxy_text or proxy_text == "默认无代理":
                        continue
                    m = PROXY_PATTERN.match(proxy_text)
                    if not m:
                        continue
                    phone = key.removeprefix("account_")
                    if not phone.startswith("+"):
                        continue
                    account = db.query(AccountFile).filter(AccountFile.phone == phone).first()
                    if account is None:
                        continue

                    # 去重维度改为 host+port+username+password（完整代理值）
                    proxy = (
                        db.query(Proxy)
                        .filter(
                            Proxy.host == m.group("host"),
                            Proxy.port == int(m.group("port")),
                            Proxy.username == m.group("username"),
                            Proxy.password == m.group("password"),
                        )
                        .first()
                    )
                    if proxy is None:
                        proxy = Proxy(
                            host=m.group("host"),
                            port=int(m.group("port")),
                            username=m.group("username"),
                            password=m.group("password"),
                            status="idle",
                        )
                        db.add(proxy)
                        db.flush()
                        created_proxy_count += 1

                    # 绑定（初始化阶段覆盖直连）
                    proxy.status = "used"
                    proxy.assigned_account_id = account.id
                    account.proxy_id = proxy.id
                    account.proxy_type = "socks5"
                    db.add(proxy)
                    db.add(account)
                    bound_count += 1
            db.commit()

        total_accounts = db.query(AccountFile).count()
        total_paths = db.query(AccountPath).count()
        total_proxies = db.query(Proxy).count()
        print(
            f"完成: 新增路径={added_paths}, 新增账号={added_accounts}, 重置旧代理={reset_proxy_count}, 新增代理={created_proxy_count}, "
            f"绑定账号代理={bound_count}, 当前总路径={total_paths}, 当前总账号={total_accounts}, 当前总代理={total_proxies}"
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
