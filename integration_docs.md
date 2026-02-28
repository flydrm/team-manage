# 库存预警 Webhook 与自动导入对接文档

本文档用于指导开发者编写对接程序，实现在收到库存预警通知后自动导入新账号的功能。

## 1. 库存预警 Webhook 通知

当系统内所有活跃 Team 的总剩余车位（`max_members - current_members`）数量低于或等于管理员设置的阈值时，系统会向配置的 Webhook URL 发送 POST 请求。

同时，如果启用了 **Telegram Bot** 并配置了 `tg_allowed_chat_ids`，系统也会向白名单 chat_id 推送一条库存预警消息（默认 10 分钟内去抖，避免重复刷屏）。
如果你只希望使用 Telegram 通知，也可以不配置 Webhook URL。

### 请求信息
- **方法**: `POST`
- **Content-Type**: `application/json`

### 请求 Payload 示例
```json
{
    "event": "low_stock",
    "current_seats": 5,
    "threshold": 10,
    "message": "库存不足预警：系统总可用车位仅剩 5，已低于预警阈值 10，请及时补货导入新账号。"
}
```

---

## 2. 账号自动导入接口

对接程序在收到通知并准备好新账号数据后，可以调用以下接口进行导入。

### 接口信息
- **接口地址**: `/admin/teams/import`
- **方法**: `POST`
- **认证方式**:
  1. **Session 认证**: 浏览器访问时自动使用。
  2. **API Key 认证**: 对接程序建议使用此方式。在 `Header` 中添加 `X-API-Key`。
- **配置位置**: 管理员后台 -> 系统设置 -> 库存预警 Webhook -> API Key。（该值存储在数据库 `settings` 表中，不依赖环境变量）

### 导入模式 A：单账号导入 (Single)
适用于逐个导入账号。

**重要说明（请按实际接口实现调用）**:
- 当前 `POST /admin/teams/import` 在 `import_type=single` 时 **API 层要求 `access_token` 必填**（即使你也提供了 ST/RT）。
- `session_token` / `refresh_token + client_id` 用于 **当 AT 过期时自动刷新**，因此你也可以传入“已过期的 AT”配合 ST/RT 让系统刷新后完成导入。

**Payload 结构**:
| 字段 | 类型 | 必填 | 说明 |
| :--- | :--- | :--- | :--- |
| `import_type` | string | **是** | 固定为 `"single"` |
| `access_token` | string | **是** | ChatGPT 的 Access Token (AT)，可为过期 AT（会尝试用 ST/RT 刷新） |
| `session_token` | string | 否 | 用于自动刷新 AT 的 Session Token (ST) |
| `email` | string | 否 | 账号邮箱。若不填，系统将尝试从 AT 中解析。 |
| `account_id` | string | 否 | Team 的 Account ID。若不填，系统将自动获取该账号下所有活跃的 Team。 |
| `refresh_token`| string | 否 | 用于刷新的 Refresh Token (RT) |
| `client_id` | string | 否 | 配合 RT 使用的 Client ID |

---

### 导入模式 B：批量导入 (Batch)
适用于一次性导入多个账号，系统会自动解析文本中的信息。

**Payload 结构**:
| 字段 | 类型 | 必填 | 说明 |
| :--- | :--- | :--- | :--- |
| `import_type` | string | **是** | 固定为 `"batch"` |
| `content` | string | **是** | 包含账号信息的文本内容 |

**批量导入格式说明**:
支持多种分隔符（如 `,` 或 `----`）。通常每一行代表一个账号，格式建议为：
`邮箱,Access_Token,Refresh_Token,Session_Token,Client_ID`
*(注：如果某列缺失可以用空占位，如 `email,at,,,`)*

---

## 3. 管理员自动兑换接口（免兑换码）

当你希望通过程序“只提供邮箱即可完成上车”（无需自己管理/分配兑换码）时，可调用该接口。

### 接口信息
- **接口地址**: `/admin/redeem/auto`
- **方法**: `POST`
- **认证方式**:
  1. **Session 认证**: 浏览器访问时自动使用。
  2. **API Key 认证**: 对接程序建议使用此方式。在 `Header` 中添加 `X-API-Key`（配置位置同上）。

### 逻辑说明
- 系统会自动选择一个可用的 `unused` 兑换码进行兑换，并自动分配可用 Team。
- 如果系统内没有可用兑换码，会自动批量生成 10 个**无过期质保**兑换码后继续兑换。

### 请求 Payload 示例
```json
{
  "email": "user@example.com"
}
```

---

## 4. 实现建议 (Python 示例)

```python
import httpx
from fastapi import FastAPI, Request

app = FastAPI()

# 这里的 API Key 需要与管理系统“系统设置”中配置的一致
API_KEY = "YOUR_CONFIGURED_API_KEY"
ADMIN_API_URL = "http://your-manager-domain.com/admin/teams/import"
ADMIN_REDEEM_URL = "http://your-manager-domain.com/admin/redeem/auto"

@app.post("/webhook/low-stock")
async def handle_low_stock(request: Request):
    data = await request.json()
    print(f"收到预警: {data['message']}")
    
    # 逻辑：从其它来源获取新账号数据
    # ...获取逻辑...
    
    new_account = {
        "import_type": "single",
        "email": "new_team@example.com",
        "access_token": "NEW_ACCESS_TOKEN"
    }
    
    # 调用管理系统导入接口
    async with httpx.AsyncClient() as client:
        # 使用 X-API-Key 进行身份验证
        response = await client.post(
            ADMIN_API_URL,
            json=new_account,
            headers={"X-API-Key": API_KEY}
        )
        print(f"导入结果: {response.json()}")

        # 可选：直接触发自动兑换（只需邮箱即可上车）
        redeem_resp = await client.post(
            ADMIN_REDEEM_URL,
            json={"email": "user@example.com"},
            headers={"X-API-Key": API_KEY}
        )
        print(f"自动兑换结果: {redeem_resp.json()}")
    
    return {"status": "ok"}
```

---

## 5. Telegram Bot 自动兑换（Webhook）

该功能用于在 Telegram 中通过命令触发“后台免兑换码上车”流程（与 `POST /admin/redeem/auto` 逻辑一致）。

另外，Telegram Bot 也支持“补账号导入”（复用 `TeamService.import_team_single` 的导入逻辑），用于在库存不足时快速导入新的 Team 账号。

### 配置位置
管理员后台 -> 系统设置 -> **库存预警 Webhook** 下方 -> **Telegram Bot**

需要配置：
- **启用 Telegram TEAM 兑换**
- **PUBLIC_BASE_URL**：你的系统外网可访问地址（用于拼接 Webhook：`{PUBLIC_BASE_URL}/tg/webhook`）
- **Bot Token**：从 BotFather 获取
- **允许的 Chat ID 白名单**：仅白名单中的 chat_id 可使用（支持逗号/空格/换行分隔；群组/频道可能是负数）
- **Webhook Secret Token**：为空保存时系统会自动生成（用于校验 Telegram 回调 Header）

#### 如何获取 Chat ID（参考）
建议在“同步 Webhook”之前获取 chat_id（Webhook 启用后 `getUpdates` 会冲突）。

说明：
- **同步 Webhook 后仍然可以随时新增/修改白名单 chat_id**：只需要在后台“Telegram Bot”配置里更新白名单并保存即可（通常不需要重新同步 Webhook，除非你更换了 `PUBLIC_BASE_URL` / `Bot Token` / `Secret Token`）。
- “冲突”的意思是：当 Webhook 启用后，Telegram 会将消息更新推送到 Webhook，此时再调用 `getUpdates` 会返回 Conflict 错误（这是 Telegram 的机制限制）。

- 私聊：对 Bot 发送任意消息后，调用 `getUpdates` 查看 `message.chat.id`
- 群组：把 Bot 拉进群并发送任意消息，同样通过 `getUpdates` 查看 `message.chat.id`（通常为负数，超级群一般以 `-100` 开头）

示例：
```bash
curl -s "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates"
```

如果你已经同步了 Webhook，但后续需要再获取新的 chat_id，可以临时删除 Webhook 后再用 `getUpdates` 拉取（拿到 chat_id 后再回到后台点击一次“同步 Webhook”即可）：
```bash
curl -s "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/deleteWebhook?drop_pending_updates=true"
curl -s "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates"
```

### 一键同步 Webhook
点击“同步 Webhook”后，系统会调用 Telegram `setWebhook`，并同步命令列表（`setMyCommands`，用于输入 `/` 时联想出 `/help`、`/redeem` 等命令）：
- Webhook URL：`{PUBLIC_BASE_URL}/tg/webhook`
- Secret Token：`tg_secret_token`

系统收到回调后会校验：
- Header `X-Telegram-Bot-Api-Secret-Token` 必须匹配 `tg_secret_token`
- `chat_id` 必须在白名单中

命令联想说明：
- 同步完成后，在 Telegram 输入 `/` 会出现命令列表：
  - **群聊/频道/默认**：`/help`、`/redeem`、`/start`
  - **私聊**：除以上命令外，还会出现 `/importteam`（补账号导入）
- 如果未立刻出现，可能是 Telegram 客户端缓存，建议等待一会儿或重新打开聊天窗口再试。

### 使用方法
在 Telegram 对 Bot 发送命令：
```
/redeem user@example.com
```

补账号导入（**仅私聊**，避免在群聊泄漏 Token）：
```
/importteam <Access Token>
```
建议导入完成后手动删除包含 AT 的消息，以降低泄漏风险。

也支持“回复导入”：回复一条包含 AT 的消息，然后发送：
```
/importteam
```

返回信息包含：
- 兑换结果
- 使用的兑换码 `used_code`（注意：会暴露兑换码）
- 分配的 Team 信息（若兑换成功）

说明：
- 若系统无可用兑换码，会自动生成 10 个**无过期质保**兑换码后继续兑换
- 为避免群聊噪音，默认不响应非命令消息
