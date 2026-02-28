# team-manage（GPT Team 管理和兑换码自动邀请系统）

本仓库是一个基于 **FastAPI + SQLite（SQLAlchemy Async）** 的 ChatGPT Team 账号管理系统：管理员维护 Team 账号与兑换码库存，用户通过兑换码自动加入 Team 并发送邀请邮件。

## 快速启动

### Docker（推荐）
```bash
cp .env.example .env
docker compose up -d --build
```

- 端口由 `APP_PORT` 控制（默认 `8008`）
- Docker Compose 会把数据库放在容器内的 `/app/data/team_manage.db`（使用 `team-manage-data` volume 持久化）

### 本地开发
> 说明：部分环境没有 `python` 命令，请统一使用 `python3`。

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt

cp .env.example .env

# 可选：初始化默认 settings（首次启动也会自动建表/迁移并初始化管理员密码）
python3 init_db.py

python3 -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8008
```

常用地址：
- 用户兑换页：`/`
- 管理员登录：`/login`
- 管理后台：`/admin`
- 健康检查：`/health`

## 目录与入口（先看这些）
- `app/main.py`：FastAPI 入口 + lifespan（建表、自动迁移、初始化管理员）
- `app/config.py`：Pydantic Settings（读取 `.env`）
- `app/database.py`：SQLAlchemy Async engine / session 工厂
- `app/models.py`：核心表（`Team` / `RedemptionCode` / `RedemptionRecord` / `Setting`）
- `app/routes/*`：路由层（页面/API）
- `app/services/*`：业务层（Team/兑换/通知/质保/ChatGPT 调用）
- `app/templates/*` + `app/static/*`：Jinja2 + 原生 JS/CSS（无前端构建步骤）

## 数据库与迁移
- 默认数据库位置：`./data/team_manage.db`（见 `.env.example` / `app/config.py`）
- 启动时（`app/main.py` lifespan）会做三件事：
  1) 确保数据库目录存在
  2) `Base.metadata.create_all()` 建表
  3) `app/db_migrations.py:run_auto_migration()` 自动补列（仅 SQLite，`ALTER TABLE ADD COLUMN`）

新增/修改表字段时建议同步更新：
- `app/models.py`（新字段定义）
- `app/db_migrations.py`（为已有数据库补列，避免用户升级后报错）

## 安全与配置（非常重要）
- `SECRET_KEY` 同时用于：
  - `SessionMiddleware` 的 Session 签名
  - `app/services/encryption.py` 派生 Fernet 密钥，用于加密数据库里的 AT/RT/ST
- **不要随意更换 `SECRET_KEY`**：更换后将无法解密数据库中已存的 token（需要重新导入 Team 账号）。

管理员密码机制：
- 首次启动会把 `.env` 的 `ADMIN_PASSWORD` 哈希后写入数据库 `settings` 表（key=`admin_password_hash`）
- 之后修改 `.env` 的 `ADMIN_PASSWORD` 不会影响已有数据库；请使用后台的“修改密码”功能（`POST /auth/change-password`）或直接改表。

`.env` 与数据库 settings 的分工：
- `.env`：运行时配置（端口、`DATABASE_URL`、`SECRET_KEY`、默认 `ADMIN_PASSWORD`、日志等）
- `Setting` 表：动态配置（代理、Webhook、阈值、API Key、日志级别等，`app/services/settings.py` 带缓存）

## 鉴权与自动化对接
后台权限依赖 `app/dependencies/auth.py:require_admin`：
- 优先 Session（`POST /auth/login` 成功后写入 `request.session["user"]`）
- 无 Session 时可使用 `X-API-Key`（值来自数据库 `Setting(key="api_key")`），用于第三方程序调用导入接口

对接说明：
- 库存预警 Webhook 与自动导入：`integration_docs.md`
- 自动导入接口：`POST /admin/teams/import`（支持 `single` / `batch`）

## 修改业务逻辑时的关键注意点
- 兑换流程在 `app/services/redeem_flow.py`
  - 事务尽量短：**不要把外部网络请求（发送邀请）放在持锁写事务里**，否则 SQLite 容易锁表/超时
  - 代码里对 `rollback()` / `expire_all()` 的处理是为了解决 `A transaction is already begun` 与 identity map 缓存问题，修改时不要随意移除
- ChatGPT 调用在 `app/services/chatgpt.py`
  - 使用 `curl-cffi` 且 `impersonate="chrome110"`，并按 identifier（account_id / email）隔离持久会话，避免 Cloudflare / 身份污染问题
  - 代理配置从数据库 settings 读取（`proxy_enabled` / `proxy`）

## 辅助脚本
- `init_db.py`：建表 + 插入默认 settings（适合首次初始化）
- `test_webhook.py`：手动测试库存预警 Webhook（注意脚本里 `DATABASE_URL` 可能需要与你当前使用的 DB 路径对齐，例如 `./data/team_manage.db`）

## CI / 镜像发布
- `.github/workflows/docker-image.yml`：在 push/tag 时构建并推送镜像到 GHCR

