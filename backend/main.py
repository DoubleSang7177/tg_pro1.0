import asyncio
import threading
import time
from uuid import uuid4

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from auth import complete_login, hash_password, require_admin
from database import SessionLocal, get_db, init_db
from logger import get_logger, setup_logging
from routes.account import router as account_router
from routes.auth import router as auth_router
from routes.logs import router as logs_router
from routes.settings import router as settings_router
from routes.task import router as task_router
from routes.user import router as user_router
from routes.group import router as group_router
from routes.proxy import router as proxy_router
from routes.scraper import router as scraper_router
from routes.interaction import router as interaction_router
from routes.copy_forward import router as copy_forward_router
from routes.auth import LoginRequest
from models import Group, User
from services.copy_forward_service import spawn_copy_forward_thread
from config.groups import GROUPS


app = FastAPI(title="Telegram System API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(task_router)
app.include_router(account_router)
app.include_router(auth_router)
app.include_router(settings_router)
app.include_router(logs_router)
app.include_router(user_router)
app.include_router(group_router)
app.include_router(proxy_router)
app.include_router(scraper_router)
app.include_router(interaction_router)
app.include_router(copy_forward_router)
api_logger = get_logger("api")


@app.middleware("http")
async def request_response_logger(request: Request, call_next):
    request_id = uuid4().hex[:8]
    start = time.perf_counter()
    api_logger.info(
        "REQ id=%s method=%s path=%s client=%s",
        request_id,
        request.method,
        request.url.path,
        request.client.host if request.client else "unknown",
    )
    try:
        response = await call_next(request)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        api_logger.info(
            "RES id=%s status=%s elapsed_ms=%s path=%s",
            request_id,
            response.status_code,
            elapsed_ms,
            request.url.path,
        )
        return response
    except Exception:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        api_logger.exception(
            "ERR id=%s elapsed_ms=%s path=%s",
            request_id,
            elapsed_ms,
            request.url.path,
        )
        raise


@app.on_event("startup")
def startup() -> None:
    # 触发模型注册并创建表
    import models  # noqa: F401

    setup_logging()
    init_db()
    db: Session = SessionLocal()
    try:
        admin = db.query(User).filter(User.username == "admin").first()
        if admin is None:
            db.add(User(username="admin", password_hash=hash_password("admin123"), role="admin"))
        user = db.query(User).filter(User.username == "user").first()
        if user is None:
            db.add(User(username="user", password_hash=hash_password("user123"), role="user"))
        for username in GROUPS:
            existed_group = db.query(Group).filter(Group.username == username).first()
            if existed_group is None:
                db.add(Group(username=username, title=username, status="normal", daily_limit=30))
        db.commit()
    finally:
        db.close()

    def _background_group_metadata_sync() -> None:
        from database import SessionLocal
        from services.telegram_service import sync_groups_metadata

        db = SessionLocal()
        try:
            asyncio.run(sync_groups_metadata(None, False, db))
        except Exception:
            api_logger.exception("startup group metadata sync failed")
        finally:
            db.close()

    threading.Thread(target=_background_group_metadata_sync, name="group-metadata-sync", daemon=True).start()
    spawn_copy_forward_thread()


@app.get("/")
def root() -> dict:
    return {"status": "success", "message": "backend is running"}


@app.post("/login")
def login_root(payload: LoginRequest, db: Session = Depends(get_db)) -> dict:
    """与 POST /auth/login 等价，返回 JWT token 与用户信息。"""
    return complete_login(db, payload.username, payload.password)


@app.get("/web", response_class=HTMLResponse)
def web_page() -> str:
    return """
<!doctype html>
<html>
  <head><meta charset="utf-8"><title>Task Starter</title></head>
  <body style="font-family: Arial; max-width: 760px; margin: 30px auto;">
    <h2>拉人任务控制台</h2>
    <p>先登录，再提交任务。默认账号：admin/admin123，user/user123</p>
    <div>
      <input id="u" placeholder="username" value="user"/>
      <input id="p" placeholder="password" type="password" value="user123"/>
      <button onclick="login()">登录</button>
    </div>
    <p id="loginResult"></p>
    <hr/>
    <div>
      <input id="groups" placeholder="groups，逗号分隔"/>
      <input id="accounts" placeholder="accounts_path" value="C:\\\\tg_pro1.0\\\\backend\\\\data\\\\tdata"/>
      <textarea id="users" rows="8" style="width:100%;" placeholder="每行一个用户名"></textarea>
      <button onclick="startTask()">启动任务</button>
    </div>
    <pre id="result"></pre>
    <script>
      let token = "";
      async function login() {
        const resp = await fetch("/login", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({username: u.value, password: p.value})
        });
        const data = await resp.json();
        if (data.ok && data.token) {
          token = data.token;
          const role = (data.user && data.user.role) || data.role || "";
          loginResult.textContent = "登录成功，角色: " + role;
        } else {
          loginResult.textContent = "登录失败";
        }
      }
      async function startTask() {
        const usersList = users.value.split("\\n").map(x => x.trim()).filter(Boolean);
        const resp = await fetch("/start_task", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token
          },
          body: JSON.stringify({
            groups: groups.value.split(",").map(x => x.trim()).filter(Boolean),
            users: usersList,
            accounts_path: accounts.value
          })
        });
        result.textContent = JSON.stringify(await resp.json(), null, 2);
      }
    </script>
  </body>
</html>
"""


@app.post("/admin/reload_settings")
def admin_reload_settings(_admin: User = Depends(require_admin)) -> dict:
    return {"ok": True, "message": "settings reloaded"}
