"""
Telegram Bot Webhook 路由
用于在 Telegram 中输入命令触发自动兑换与导入补账号
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
from app.services.team import team_service
from app.services.telegram import send_message
from app.utils.token_parser import TokenParser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tg", tags=["telegram"])

# 频率限制：按命令维度区分，避免互相影响
_rate_limit_redeem: Dict[int, float] = {}
_rate_limit_import: Dict[int, float] = {}
_REDEEM_RATE_LIMIT_SECONDS = 5.0
_IMPORT_RATE_LIMIT_SECONDS = 10.0

# 邮箱提取
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# /redeem 命令（支持 /redeem@botname）
_REDEEM_CMD_RE = re.compile(r"^/redeem(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)

# /importteam 命令（支持 /importteam@botname）
_IMPORTTEAM_CMD_RE = re.compile(r"^/importteam(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)

# Access Token 提取（JWT）
_JWT_RE = re.compile(TokenParser.JWT_PATTERN)


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
        try:
            ids.add(int(p))
        except Exception:
            continue
    return ids


def _rate_limited(chat_id: int, bucket: Dict[int, float], seconds: float) -> bool:
    now = time.monotonic()
    last = bucket.get(chat_id)
    if last is not None and now - last < seconds:
        return True
    bucket[chat_id] = now
    return False


def _extract_message(update: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], str, Optional[str], str, list]:
    """
    从 Telegram update 中提取 (chat_id, message_id, text, chat_type, reply_text, entities)。
    """
    msg = update.get("message") or update.get("edited_message") or update.get("channel_post") or update.get("edited_channel_post")
    if not isinstance(msg, dict):
        return None, None, "", None, "", []
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    message_id = msg.get("message_id")
    text = msg.get("text") or ""
    reply_text = ""
    reply = msg.get("reply_to_message")
    if isinstance(reply, dict):
        reply_text = reply.get("text") or ""
    entities = msg.get("entities") or []
    if not isinstance(entities, list):
        entities = []
    return chat_id, message_id, text, chat_type, reply_text, entities


def _extract_command_from_entities(text: str, entities: list) -> Tuple[Optional[str], str]:
    """
    优先通过 Telegram entities 提取命令与参数，避免换行/空格导致解析不稳定。

    Returns:
        (command, rest)
        - command: 原始命令字符串（可能包含 @botname），如 "/redeem@xxx"
        - rest: 命令后剩余文本（trim 后）
    """
    if not text or not entities:
        return None, ""
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        if ent.get("type") != "bot_command":
            continue
        if ent.get("offset") != 0:
            continue
        length = ent.get("length")
        if not isinstance(length, int) or length <= 0:
            continue
        cmd = text[:length]
        rest = (text[length:] or "").strip()
        return cmd, rest
    return None, ""


def _extract_access_token(text: str) -> Optional[str]:
    """
    从文本中提取第一个 Access Token (JWT)。
    """
    m = _JWT_RE.search(text or "")
    return m.group(0) if m else None


def _mask_secrets(text: str) -> str:
    """
    避免在错误信息中泄漏 Token（兜底）。
    """
    if not text:
        return ""
    try:
        text = re.sub(TokenParser.JWT_PATTERN, "[TOKEN]", text)
        text = re.sub(TokenParser.REFRESH_TOKEN_PATTERN, "[RT]", text)
    except Exception:
        pass
    return text


def _build_help_text() -> str:
    return (
        "使用方法：\n"
        "1) 自动上车：/redeem user@example.com\n"
        "2) 补账号导入(仅私聊)：/importteam <Access Token>\n\n"
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


def _format_import_result(result: Dict[str, Any]) -> str:
    if result.get("success"):
        lines = [
            f"导入成功：{(result.get('message') or '').strip()}".strip("：").strip(),
        ]
        email = result.get("email")
        if email:
            lines.append(f"邮箱: {email}")
        team_id = result.get("team_id")
        if team_id is not None:
            lines.append(f"首个 Team ID: {team_id}")
        return "\n".join([l for l in lines if l]).strip()

    err = result.get("error") or "未知错误"
    msg = f"导入失败：{err}"
    if "已在系统" in err or "已存在" in err:
        msg += "\n说明：账号已存在，无需重复导入。"
    return msg

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
                    await send_message(
                        bot_token,
                        chat_id,
                        f"兑换失败：{_mask_secrets(str(e)) or '系统异常'}",
                        reply_to_message_id=reply_to_message_id,
                    )
            except Exception:
                pass


async def _process_import(chat_id: int, reply_to_message_id: Optional[int], access_token: str) -> None:
    """
    后台任务：执行 Team 导入并回发结果到 Telegram
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

            result = await team_service.import_team_single(access_token=access_token, db_session=db)
            text = _format_import_result(result)
            await send_message(bot_token, chat_id, text, reply_to_message_id=reply_to_message_id)
        except Exception as e:
            logger.error(f"处理 TG 导入任务失败: {e}")
            try:
                bot_token = await settings_service.get_setting(db, "tg_bot_token", "")
                if bot_token:
                    await send_message(
                        bot_token,
                        chat_id,
                        f"导入失败：{_mask_secrets(str(e)) or '系统异常'}",
                        reply_to_message_id=reply_to_message_id,
                    )
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

    chat_id, message_id, text, chat_type, reply_text, entities = _extract_message(update)
    if chat_id is None:
        return {"ok": True}

    # 校验白名单 chat_id
    try:
        allowed_chat_ids = _parse_chat_ids(allowed_raw)
    except Exception:
        allowed_chat_ids = set()
    if not allowed_chat_ids or chat_id not in allowed_chat_ids:
        return {"ok": True}

    text = (text or "").strip()
    if not text:
        return {"ok": True}

    # 优先用 entities 提取命令，兜底再用 regex
    cmd_raw, rest_from_entities = _extract_command_from_entities(text, entities)
    cmd = (cmd_raw.split("@", 1)[0].lower() if cmd_raw else "")

    # /start /help
    if cmd in ("/start", "/help") or text.lower().startswith("/start") or text.lower().startswith("/help"):
        if bot_token:
            asyncio.create_task(send_message(bot_token, chat_id, _build_help_text(), reply_to_message_id=message_id))
        return {"ok": True}

    # /redeem 邮箱（支持 entities + regex 兜底）
    if cmd == "/redeem" or _REDEEM_CMD_RE.match(text):
        # 频率限制（redeem）
        if _rate_limited(chat_id, _rate_limit_redeem, _REDEEM_RATE_LIMIT_SECONDS):
            if bot_token:
                asyncio.create_task(
                    send_message(bot_token, chat_id, "操作太频繁，请稍后再试。", reply_to_message_id=message_id)
                )
            return {"ok": True}

        rest = rest_from_entities
        if not rest:
            m = _REDEEM_CMD_RE.match(text)
            rest = (m.group(1) or "").strip() if m else ""

        email_match = _EMAIL_RE.search(rest or "")
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

    # /importteam 补账号导入（仅私聊）
    if cmd == "/importteam" or _IMPORTTEAM_CMD_RE.match(text):
        # 频率限制（import）
        if _rate_limited(chat_id, _rate_limit_import, _IMPORT_RATE_LIMIT_SECONDS):
            if bot_token:
                asyncio.create_task(
                    send_message(bot_token, chat_id, "操作太频繁，请稍后再试。", reply_to_message_id=message_id)
                )
            return {"ok": True}

        if chat_type != "private":
            if bot_token:
                asyncio.create_task(
                    send_message(bot_token, chat_id, "为安全起见，补账号导入仅支持私聊本 Bot 使用。", reply_to_message_id=message_id)
                )
            return {"ok": True}

        rest = rest_from_entities
        if not rest:
            m = _IMPORTTEAM_CMD_RE.match(text)
            rest = (m.group(1) or "").strip() if m else ""

        # 支持“回复导入”：无参数时从被回复消息提取 AT
        token_source = rest or (reply_text or "")
        access_token = _extract_access_token(token_source)
        if not access_token:
            if bot_token:
                asyncio.create_task(
                    send_message(
                        bot_token,
                        chat_id,
                        "请粘贴有效的 Access Token(AT)。\n示例：/importteam <AT>\n（也支持回复一条包含 AT 的消息后发送 /importteam）",
                        reply_to_message_id=message_id,
                    )
                )
            return {"ok": True}

        if bot_token:
            asyncio.create_task(
                send_message(bot_token, chat_id, "已收到导入请求：正在处理中，请稍候…", reply_to_message_id=message_id)
            )
        asyncio.create_task(_process_import(chat_id, message_id, access_token))
        return {"ok": True}

    # 默认不响应非命令，避免群里噪音
    return {"ok": True}
