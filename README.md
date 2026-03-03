# GPT Team 管理和兑换码自动邀请系统

一个基于 FastAPI 的 ChatGPT Team 账号管理系统，支持管理员批量管理 Team 账号，用户通过兑换码自动加入 Team。

## 🚀 Docker 一键部署 & 更新

### 一键部署
```bash
git clone https://github.com/flydrm/team-manage.git
cd team-manage
cp .env.example .env
docker compose up -d
```

### 一键更新
```bash
git pull && docker compose down && docker compose up -d --build
```

> 提示：系统已对静态资源（JS/CSS）自动加版本号，更新后一般无需手动清浏览器缓存/清 Cookie。

## ✨ 功能特性

### 管理员功能
- **Team 账号管理**
  - 单个/批量导入 Team 账号（支持任意格式的 AT Token）
  - 智能识别和提取 AT Token、邮箱、Account ID
  - 自动同步 Team 信息（名称、订阅计划、到期时间、成员数）
  - Team 成员管理（查看、添加、删除成员）
  - Team 状态监控（可用/已满/已过期/错误）

- **兑换码管理**
  - 单个/批量生成兑换码
  - 自定义兑换码和有效期
  - 兑换码状态筛选（未使用/已使用/已过期）
  - 导出兑换码为文本文件
  - 删除未使用的兑换码

- **TEAM 兑换（后台免兑换码）**
  - 在管理端“TEAM兑换”页面输入邮箱即可自动分配 Team 并发送邀请
  - 页面顶部会展示当前总可用车位，便于快速判断库存是否充足
  - 自动选择可用兑换码；若无可用兑换码则自动生成 10 个**无过期质保**兑换码后完成兑换
  - 支持使用 `X-API-Key` 调用 `POST /admin/redeem/auto` 实现自动化上车

- **使用记录查询**
  - 多维度筛选（邮箱、兑换码、Team ID、日期范围）
  - 支持按来源筛选（用户端/管理端/Telegram），Telegram 记录支持按 `chat_id` 精准查询
  - 分页展示（每页20条记录）
  - 统计数据（总数、今日、本周、本月）
  - 支持按当前筛选条件导出 CSV / NDJSON

- **系统设置**
  - 代理配置（HTTP/SOCKS5）
  - 管理员密码修改
  - 日志级别动态调整
  - **库存预警 Webhook** (支持库存不足时自动通知第三方系统补货)
  - **Telegram Bot**（仅白名单 chat_id 可用；`/redeem 邮箱` 仅私聊，且可通过 `tg_redeem_chat_ids` 单独授权；`/records`/`/withdraw` 仅私聊；`/status`/`/importteam` 仅超管私聊；库存预警仅推送超管（`tg_notify_chat_ids`））

### 自动化与集成
- **库存预警与自动导入**
  - 当系统总可用车位低于或等于阈值时，自动触发库存预警通知（Webhook / Telegram）
  - 支持第三方程序通过 API 自动导入新 Team 账号
  - 详细对接说明见 [integration_docs.md](integration_docs.md)
- **Telegram Bot 自动兑换**
  - 私聊发送 `/redeem user@example.com` 自动兑换并分配 Team（可通过 `tg_redeem_chat_ids` 限制可用人员；留空则默认=允许白名单；无可用兑换码时自动生成 10 个无过期质保码后继续）
  - 仅超管的兑换成功回执会返回“当前总可用车位”，便于判断库存（普通用户不披露库存信息）
  - 仅超管私聊可用：`/status`（支持 `/status full`）、`/importteam <Access Token>`
  - 私聊可用：`/records user@example.com` 查询有效期内使用记录（超管可 `all` 查询全量）；`/withdraw user@example.com` 撤销上车（按钮二次确认；普通用户仅可撤销自己通过 TG 拉上车的记录；超管可撤销所有来源）
  - 配置与对接说明见 [integration_docs.md](integration_docs.md)

### 用户功能
- **兑换流程**
  - 输入邮箱和兑换码
  - 自动验证兑换码有效性
  - 展示可用 Team 列表
  - 手动选择或自动分配 Team
  - 自动发送 Team 邀请到用户邮箱

## 🛠️ 技术栈

- **后端框架**: FastAPI 0.109+
- **Web 服务器**: Uvicorn
- **数据库**: SQLite + SQLAlchemy 2.0 + aiosqlite
- **模板引擎**: Jinja2
- **HTTP 客户端**: curl-cffi（模拟浏览器指纹，绕过 Cloudflare 防护）
- **认证**: Session-based（bcrypt 密码哈希）
- **加密**: cryptography（AES-256-GCM）
- **JWT 解析**: PyJWT
- **前端**: HTML + CSS + 原生 JavaScript

## 📋 系统要求

- Python 3.10+
- pip（Python 包管理器）
- 操作系统：Windows / Linux / macOS

## 🚀 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/flydrm/team-manage.git
cd team-manage
```

### 2. 创建虚拟环境

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/macOS
python3 -m venv venv
source venv/bin/activate
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 配置环境变量

复制 `.env.example` 为 `.env` 并修改配置：

```bash
cp .env.example .env
```

编辑 `.env` 文件：

```env
# 应用配置
APP_NAME=GPT Team 管理系统
APP_VERSION=0.1.0
APP_HOST=0.0.0.0
APP_PORT=8008
DEBUG=True

# 数据库配置（默认使用 SQLite）
DATABASE_URL=sqlite+aiosqlite:///./data/team_manage.db

# 安全配置（生产环境请修改）
SECRET_KEY=your-secret-key-here-change-in-production
ADMIN_PASSWORD=admin123

# 日志配置
LOG_LEVEL=INFO

# 代理配置（可选）
PROXY_ENABLED=False
PROXY=

# JWT 配置
JWT_VERIFY_SIGNATURE=False

# Team 配置
TEAM_MAX_MEMBERS_DEFAULT=5  # 新导入 Team 的默认最大成员数（已存在 Team 以数据库记录为准）
```

### 5. 初始化数据库

```bash
python init_db.py
```

### 6. 启动应用

```bash
# 开发模式（自动重载）
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8008

# 或者直接运行
python app/main.py
```

### 7. 访问应用

- **用户兑换页面**: http://localhost:8008/
- **管理员登录页面**: http://localhost:8008/login
- **管理员控制台**: http://localhost:8008/admin

**默认管理员账号**:
- 用户名: `admin`
- 密码: `admin123`（请在首次登录后修改）

---

## 🐳 Docker 部署 (推荐)

项目支持使用 Docker 快速部署，确保环境一致性并简化配置。

### 1. 准备工作

确保你的系统已安装：
- Docker
- Docker Compose

### 2. 快速启动

1.  克隆项目并进入目录。
2.  配置 `.env` 文件（参考上述"配置环境变量"章节）。
3.  运行 Docker Compose 命令：

```bash
# 构建并启动容器
docker compose up -d
```

### 3. 数据持久化

Docker Compose 使用名为 `team-manage-data` 的 volume 挂载到容器内的 `/app/data`，数据库文件为 `/app/data/team_manage.db`，即使删除容器数据也会保留（除非显式删除 volume）。

### 4. 常用命令

```bash
# 查看日志
docker compose logs -f

# 停止并移除容器
docker compose down

# 重新构建镜像
docker compose build --no-cache
```

## 📁 项目结构

```
team-manage/
├── AGENTS.md
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── init_db.py
├── integration_docs.md
├── test_webhook.py
├── README.md
└── app/                        # 应用主目录
    ├── main.py                 # FastAPI 入口文件
    ├── config.py               # 配置管理
    ├── database.py             # 数据库连接
    ├── models.py               # SQLAlchemy 模型
    ├── routes/                 # 路由模块
    │   ├── admin.py            # 管理后台
    │   ├── redeem.py           # 用户兑换页面
    │   ├── tg.py               # Telegram Webhook
    │   └── ...
    ├── services/               # 业务逻辑服务
    │   ├── auto_redeem.py      # 自动兑换（仅邮箱）
    │   ├── telegram.py         # Telegram API 封装
    │   └── ...
    ├── templates/              # Jinja2 模板
    └── static/                 # 静态文件
```

## 🔧 配置说明

### 数据库配置

默认使用 SQLite 数据库，数据库文件为 `team_manage.db`。如需使用其他数据库，请修改 `DATABASE_URL`。

### 代理配置

如果需要通过代理访问 ChatGPT API，可以在管理员面板的"系统设置"中配置代理：

- 支持 HTTP 代理：`http://proxy.example.com:8080`
- 支持 SOCKS5 代理：`socks5://proxy.example.com:1080`

### 安全配置

**生产环境部署前，请务必修改以下配置**：

1. `SECRET_KEY`: 用于 Session 签名，请使用随机字符串
2. `ADMIN_PASSWORD`: 管理员初始密码，首次登录后请立即修改
3. `DEBUG`: 生产环境请设置为 `False`

### Team 席位配置

可通过环境变量 `TEAM_MAX_MEMBERS_DEFAULT` 设置**新导入 Team** 的默认最大成员数（`max_members`）。已存在 Team 以数据库记录为准，可在后台编辑 Team 时单独修改。

## 📖 使用指南

### 管理员操作流程

1. **登录管理员面板**
   - 访问 http://localhost:8008/login
   - 使用默认账号登录（admin/admin123）
   - 首次登录后建议修改密码

2. **导入 Team 账号**
   - 进入"Team 管理" → "导入 Team"
   - 单个导入：填写 AT Token、邮箱（可选）、Account ID（可选）
   - 批量导入：粘贴包含 AT Token 的文本（支持任意格式）
   - 系统会自动识别和提取信息

3. **生成兑换码**
   - 进入"兑换码管理" → "生成兑换码"
   - 单个生成：可自定义兑换码和有效期
   - 批量生成：设置数量和有效期
   - 生成后可复制或下载

4. **TEAM 兑换（后台免兑换码上车）**
   - 进入左侧菜单“TEAM兑换”
   - 页面顶部可查看当前总可用车位
   - 输入用户邮箱，点击“自动兑换上车”
   - 系统会自动选择可用兑换码并自动分配 Team；若无可用兑换码则自动生成 10 个**无过期质保**兑换码后完成兑换

5. **查看使用记录**
   - 进入"使用记录"
   - 可按邮箱、兑换码、Team ID、日期范围筛选
   - 查看统计数据（总数、今日、本周、本月）

6. **系统设置**
   - 进入"系统设置"
   - 配置代理（如需）
   - 修改管理员密码
   - 调整日志级别
   - 配置库存预警 Webhook 与 API Key（用于 `X-API-Key` 认证管理员接口）
   - 配置 Telegram Bot（保存后点击“同步 Webhook”；私聊可用 `/redeem`（可按 `tg_redeem_chat_ids` 授权）、`/records`、`/withdraw`；仅超管私聊可用 `/importteam`、`/status`）

### 用户兑换流程

1. **访问兑换页面**
   - 访问 http://localhost:8008/

2. **输入信息**
   - 填写邮箱地址
   - 输入兑换码

3. **选择 Team**
   - 系统展示可用 Team 列表
   - 手动选择 Team 或点击"自动选择"

4. **完成兑换**
   - 系统自动发送邀请到邮箱
   - 查看兑换结果（Team 名称、到期时间）

5. **接受邀请**
   - 检查邮箱收到的 ChatGPT Team 邀请邮件
   - 点击邮件中的链接接受邀请

## 🔌 API 接口

接口与参数以 FastAPI 文档为准：访问 `/docs` 查看（Swagger UI）。

主要接口：

- `POST /auth/login` - 管理员登录
- `POST /auth/logout` - 管理员登出
- `POST /redeem/verify` - 验证兑换码
- `POST /redeem/confirm` - 确认兑换
- `GET /admin` - 管理员控制台
- `POST /admin/teams/import` - Team 导入
- `GET /admin/codes` - 兑换码列表
- `GET /admin/records` - 使用记录
- `GET /admin/records/export.csv` - 导出使用记录（CSV，支持筛选参数）
- `GET /admin/records/export.json` - 导出使用记录（NDJSON，支持筛选参数）
- `POST /admin/redeem/auto` - 管理端自动兑换（仅需邮箱，自动分配 Team；支持 Session 或 `X-API-Key`）

## 🐛 故障排除

### 数据库初始化失败

```bash
# 本地开发默认数据库路径（见 .env.example）
rm -f ./data/team_manage.db

# 重新初始化
python3 init_db.py
```

如果你使用 Docker 部署并希望清空数据（会删除所有数据，请谨慎操作）：
```bash
docker compose down -v
```

### 无法访问 ChatGPT API

1. 检查网络连接
2. 配置代理（如需）
3. 检查 AT Token 是否有效
4. 查看日志文件排查错误

### 导入 Team 失败

1. 确保 AT Token 格式正确
2. 检查 Token 是否过期
3. 验证 Token 是否有 Team 管理权限

### Docker Permission Denied
如果遇到 `permission denied while trying to connect to the Docker daemon socket`：
- 使用 `sudo docker compose up -d` 运行
- 或将当前用户加入 `docker` 用户组后重新登录

## 📄 许可证

本项目仅供学习和研究使用。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

**注意**: 本系统仅用于合法的 ChatGPT Team 账号管理，请遵守 OpenAI 的服务条款。
