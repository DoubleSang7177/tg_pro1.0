# 🚀 Telegram 拉人系统（Web版）

## 📌 项目简介

这是一个基于 Python + FastAPI + React 的 Telegram 自动化管理系统。

目标：

* 将原有本地脚本升级为 Web 系统
* 支持多人使用（权限控制）
* 支持 TDATA 账号管理
* 支持用户列表管理（替代 users.txt）
* 支持任务化执行（拉人任务）
* 支持日志可视化

---

## 🧱 技术架构

### 后端

* Python 3.10+
* FastAPI
* SQLite（前期）
* Pyrogram（Telegram客户端）

### 前端（后续）

* React
* TailwindCSS（UI美化）

---

## 📁 项目结构

```
project/
│
├── backend/
│   ├── main.py                # FastAPI入口
│   ├── database.py            # 数据库连接
│   │
│   ├── models/                # 数据模型
│   │   ├── user.py
│   │   ├── task.py
│   │   ├── account.py
│   │
│   ├── routes/                # 路由
│   │   ├── auth.py
│   │   ├── task.py
│   │   ├── account.py
│   │   ├── userlist.py
│   │
│   ├── services/              # 核心逻辑
│   │   ├── telegram_service.py   # ⭐核心：你的拉人代码
│
│   ├── utils/
│   │   ├── auth.py
│
├── frontend/                  # 前端（后期）
│
├── data/
│   ├── tdata/                 # 存储上传的账号
│   ├── users/                 # 用户列表
│
├── requirements.txt
├── README.md
```

---

## ⚙️ 功能模块

### 1️⃣ 用户系统

* 注册 / 登录
* JWT认证
* 权限控制（admin / user）

---

### 2️⃣ 账号管理（TDATA）

* 支持填写本地路径
* 支持上传 TDATA（zip）
* 自动识别 session
* 构建账号池

---

### 3️⃣ 用户列表管理

* 上传 users.txt
* 手动输入用户名
* 存入数据库

---

### 4️⃣ 拉人任务系统（核心）

* 选择群组
* 选择账号池
* 选择用户列表
* 启动任务（异步执行）

---

### 5️⃣ 代理管理

* 每个账号绑定代理
* 可视化配置

---

### 6️⃣ 日志系统

* 实时日志
* 成功 / 失败统计
* 账号状态监控

---

## 🚀 开发步骤（必须按顺序）

### ✅ 第1步：基础后端

* FastAPI启动
* 健康检查接口

---

### ✅ 第2步：任务接口

* 创建任务接口
* 能调用 telegram_service

---

### ✅ 第3步：账号系统

* 扫描 TDATA
* 存储账号

---

### ✅ 第4步：用户列表

* 替代 users.txt

---

### ✅ 第5步：前端页面

* 简单操作UI

---

## 🧠 核心改造（非常重要）

你原有代码必须改造为：

```python
async def run_adder(config):
    ...
```

不能再使用：

```python
if __name__ == "__main__":
```

---

## 📦 安装依赖

```bash
pip install -r requirements.txt
```

---

## ▶️ 启动项目

```bash
cd backend
uvicorn main:app --reload
```

访问：

```
http://localhost:8000
```

---

## ⚠️ 注意事项

* Telegram 有风控限制（PEER_FLOOD）
* 不要高频拉人
* 建议多账号 + 低频策略

---

## 🧩 下一步

使用 Cursor：

👉 输入提示词：

“根据 README 创建 backend 基础结构，并实现 FastAPI 启动”

---

## 👨‍💻 作者说明

本项目用于自动化 Telegram 运营系统构建，不建议用于违规行为。
