"""
Telegram Bot Webhook 路由
用于在 Telegram 中输入 /redeem 邮箱 自动触发兑换
"""
import asyncio
import logging
import re
import time
from typing import Any, Dict, Optional, Set, Tuple

from fastapi import APIRouter, HTTPException, Request, status

from app.database import AsyncSessionLocal
from app.services.auto_redeem import auto_redeem_by_email
from app.services.settings import settings_service
from app.services.telegram import send_message

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tg", tags=["telegram"])

# 频率限制：每个 chat_id 5 秒内只处理一次
_rate_limit: Dict[int, float] = {}
_RATE_LIMIT_SECONDS = 5.0

# 邮箱提取
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# /redeem 命令（支持 /redeem@botname）
_REDEEM_CMD_RE = re.compile(r"^/redeem(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)


def _parse_chat_ids(raw: str) -> Set[int]:
    """
    解析 chat_id 白名单，支持逗号/空格/换行分隔，允许负数。
    """
    raw = (raw or "").strip()
    if not raw:
        return set()
    parts = re.split(r"[, \t\r\n]+", raw)
    ids: Set[int] = set()
    for p in parts:
        p = p.strip()
        if not p:
            continue
        ids.add(int(p))
    return ids


def _rate_limited(chat_id: int) -> bool:
    now = time.monotonic()
    last = _rate_limit.get(chat_id)
    if last is not None and now - last < _RATE_LIMIT_SECONDS:
        return True
    _rate_limit[chat_id] = now
    return False


def _extract_message(update: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], str]:
    """
    从 Telegram update 中提取 (chat_id, message_id, text)。
    """
    msg = update.get("message") or update.get("edited_message") or update.get("channel_post") or update.get("edited_channel_post")
    if not isinstance(msg, dict):
        return None, None, ""
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    message_id = msg.get("message_id")
    text = msg.get("text") or ""
    return chat_id, message_id, text


def _build_help_text() -> str:
    return (
        "使用方法：\n"
        "发送命令：/redeem user@example.com\n\n"
        "说明：系统会自动选择可用兑换码并自动分配 Team 完成上车；若无可用兑换码会自动生成 10 个无过期质保码后继续。"
    )


def _format_redeem_result(result: Dict[str, Any]) -> str:
    if result.get("success"):
        team_info = result.get("team_info") or {}
        lines = [
            f"兑换成功：{result.get('message') or ''}".strip(),
            f"使用兑换码: {result.get('used_code')}",
        ]
        if result.get("generated_codes"):
            lines.append(f"本次自动生成兑换码: {result.get('generated_codes')}")
        team_id = team_info.get("team_id")
        team_name = team_info.get("team_name")
        if team_name or team_id is not None:
            lines.append(f"Team: {team_name or '-'} (ID: {team_id if team_id is not None else '-'})")
        if team_info.get("expires_at"):
            lines.append(f"到期时间: {team_info.get('expires_at')}")
        return "\n".join(lines).strip()

    return f"兑换失败：{result.get('error') or '未知错误'}"


async def _process_redeem(chat_id: int, reply_to_message_id: Optional[int], email: str) -> None:
    """
    后台任务：执行兑换并回发结果到 Telegram
    """
    async with AsyncSessionLocal() as db:
        try:
            tg_enabled = (await settings_service.get_setting(db, "tg_enabled", "false") or "false").lower() == "true"
            if not tg_enabled:
                return

            bot_token = await settings_service.get_setting(db, "tg_bot_token", "")
            if not bot_token:
                logger.error("TG Bot Token 未配置，无法发送消息")
                return

            result = await auto_redeem_by_email(email, db)
            text = _format_redeem_result(result)
            await send_message(bot_token, chat_id, text, reply_to_message_id=reply_to_message_id)
        except Exception as e:
            logger.error(f"处理 TG 兑换任务失败: {e}")
            try:
                bot_token = await settings_service.get_setting(db, "tg_bot_token", "")
                if bot_token:
                    await send_message(bot_token, chat_id, f"兑换失败：{str(e)}", reply_to_message_id=reply_to_message_id)
            except Exception:
                pass


@router.post("/webhook")
async def telegram_webhook(request: Request):
    """
    Telegram Webhook 回调入口
    """
    try:
        update = await request.json()
    except Exception as e:
        # Telegram 回调应为 JSON；对于探测/异常请求，直接返回 200 避免重试与噪音
        logger.warning(f"TG webhook 收到非 JSON 请求: {e}")
        return {"ok": True}
    if not isinstance(update, dict):
        return {"ok": True}

    # 读取配置（独立 DB 会话）
    async with AsyncSessionLocal() as db:
        tg_enabled = (await settings_service.get_setting(db, "tg_enabled", "false") or "false").lower() == "true"
        if not tg_enabled:
            return {"ok": True}

        secret_token = await settings_service.get_setting(db, "tg_secret_token", "")
        allowed_raw = await settings_service.get_setting(db, "tg_allowed_chat_ids", "")
        bot_token = await settings_service.get_setting(db, "tg_bot_token", "")

    # 校验 secret token（防止伪造回调）
    header_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not secret_token or header_secret != secret_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="invalid telegram secret token")

    chat_id, message_id, text = _extract_message(update)
    if chat_id is None:
        return {"ok": True}

    # 校验白名单 chat_id
    try:
        allowed_chat_ids = _parse_chat_ids(allowed_raw)
    except Exception:
        allowed_chat_ids = set()
    if not allowed_chat_ids or chat_id not in allowed_chat_ids:
        return {"ok": True}

    # 频率限制
    if _rate_limited(chat_id):
        if bot_token:
            asyncio.create_task(
                send_message(bot_token, chat_id, "操作太频繁，请稍后再试。", reply_to_message_id=message_id)
            )
        return {"ok": True}

    text = (text or "").strip()
    if not text:
        return {"ok": True}

    # /start /help
    if text.lower().startswith("/start") or text.lower().startswith("/help"):
        if bot_token:
            asyncio.create_task(send_message(bot_token, chat_id, _build_help_text(), reply_to_message_id=message_id))
        return {"ok": True}

    # /redeem 邮箱
    m = _REDEEM_CMD_RE.match(text)
    if not m:
        # 默认不响应非命令，避免群里噪音
        return {"ok": True}

    rest = (m.group(1) or "").strip()
    email_match = _EMAIL_RE.search(rest)
    if not email_match:
        if bot_token:
            asyncio.create_task(
                send_message(bot_token, chat_id, "请输入有效邮箱。\n\n" + _build_help_text(), reply_to_message_id=message_id)
            )
        return {"ok": True}

    email = email_match.group(0)

    # 先回一条“处理中”，再异步执行兑换并回结果
    if bot_token:
        asyncio.create_task(
            send_message(bot_token, chat_id, f"已收到兑换请求：{email}\n正在处理中，请稍候…", reply_to_message_id=message_id)
        )
    asyncio.create_task(_process_redeem(chat_id, message_id, email))
    return {"ok": True}
