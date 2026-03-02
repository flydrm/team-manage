"""
Telegram Bot API 服务
用于发送消息、同步 Webhook 等
"""
import logging
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"


def _mask_token(token: str) -> str:
    if not token:
        return ""
    if len(token) <= 10:
        return "***"
    return f"{token[:3]}***{token[-3:]}"


async def send_message(
    bot_token: str,
    chat_id: int,
    text: str,
    *,
    reply_to_message_id: Optional[int] = None,
    disable_web_page_preview: bool = True,
    reply_markup: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    发送 Telegram 消息
    """
    if not bot_token:
        return {"success": False, "data": None, "error": "Bot Token 未配置"}

    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = reply_to_message_id
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
        data = {}
        try:
            data = resp.json()
        except Exception:
            pass

        if resp.status_code < 200 or resp.status_code >= 300:
            err = data.get("description") if isinstance(data, dict) else resp.text
            logger.warning(f"Telegram sendMessage 失败: status={resp.status_code}, token={_mask_token(bot_token)}, err={err}")
            return {"success": False, "data": data, "error": err or f"HTTP {resp.status_code}"}

        if isinstance(data, dict) and not data.get("ok", False):
            err = data.get("description") or "Telegram 返回 ok=false"
            logger.warning(f"Telegram sendMessage 返回 ok=false: token={_mask_token(bot_token)}, err={err}")
            return {"success": False, "data": data, "error": err}

        return {"success": True, "data": data, "error": None}

    except Exception as e:
        logger.error(f"Telegram sendMessage 异常: {e} (token={_mask_token(bot_token)})")
        return {"success": False, "data": None, "error": str(e)}


async def answer_callback_query(
    bot_token: str,
    callback_query_id: str,
    *,
    text: Optional[str] = None,
    show_alert: bool = False,
) -> Dict[str, Any]:
    """
    回应 Telegram callback_query（用于 InlineKeyboard 按钮点击后的提示与停止 loading）
    """
    if not bot_token:
        return {"success": False, "data": None, "error": "Bot Token 未配置"}
    if not callback_query_id:
        return {"success": False, "data": None, "error": "callback_query_id 不能为空"}

    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/answerCallbackQuery"
    payload: Dict[str, Any] = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    if show_alert:
        payload["show_alert"] = True

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
        data = {}
        try:
            data = resp.json()
        except Exception:
            pass

        if resp.status_code < 200 or resp.status_code >= 300:
            err = data.get("description") if isinstance(data, dict) else resp.text
            logger.warning(
                f"Telegram answerCallbackQuery 失败: status={resp.status_code}, token={_mask_token(bot_token)}, err={err}"
            )
            return {"success": False, "data": data, "error": err or f"HTTP {resp.status_code}"}

        if isinstance(data, dict) and not data.get("ok", False):
            err = data.get("description") or "Telegram 返回 ok=false"
            logger.warning(f"Telegram answerCallbackQuery 返回 ok=false: token={_mask_token(bot_token)}, err={err}")
            return {"success": False, "data": data, "error": err}

        return {"success": True, "data": data, "error": None}

    except Exception as e:
        logger.error(f"Telegram answerCallbackQuery 异常: {e} (token={_mask_token(bot_token)})")
        return {"success": False, "data": None, "error": str(e)}


async def set_webhook(
    bot_token: str,
    webhook_url: str,
    *,
    secret_token: str,
) -> Dict[str, Any]:
    """
    设置 Telegram Webhook（建议在后台配置页一键同步）
    """
    if not bot_token:
        return {"success": False, "data": None, "error": "Bot Token 未配置"}
    if not webhook_url:
        return {"success": False, "data": None, "error": "Webhook URL 不能为空"}
    if not secret_token:
        return {"success": False, "data": None, "error": "secret_token 不能为空"}

    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/setWebhook"
    payload = {"url": webhook_url, "secret_token": secret_token}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=payload)
        data = {}
        try:
            data = resp.json()
        except Exception:
            pass

        if resp.status_code < 200 or resp.status_code >= 300:
            err = data.get("description") if isinstance(data, dict) else resp.text
            logger.warning(f"Telegram setWebhook 失败: status={resp.status_code}, token={_mask_token(bot_token)}, err={err}")
            return {"success": False, "data": data, "error": err or f"HTTP {resp.status_code}"}

        if isinstance(data, dict) and not data.get("ok", False):
            err = data.get("description") or "Telegram 返回 ok=false"
            logger.warning(f"Telegram setWebhook 返回 ok=false: token={_mask_token(bot_token)}, err={err}")
            return {"success": False, "data": data, "error": err}

        return {"success": True, "data": data, "error": None}

    except Exception as e:
        logger.error(f"Telegram setWebhook 异常: {e} (token={_mask_token(bot_token)})")
        return {"success": False, "data": None, "error": str(e)}


async def set_my_commands(
    bot_token: str,
    commands: list[dict],
    *,
    scope: Optional[Dict[str, Any]] = None,
    language_code: Optional[str] = None,
) -> Dict[str, Any]:
    """
    设置 Telegram 命令列表（用于输入 / 时的命令联想）
    """
    if not bot_token:
        return {"success": False, "data": None, "error": "Bot Token 未配置"}
    if not commands:
        return {"success": False, "data": None, "error": "commands 不能为空"}

    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/setMyCommands"
    payload: Dict[str, Any] = {"commands": commands}
    if scope is not None:
        payload["scope"] = scope
    if language_code:
        payload["language_code"] = language_code

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=payload)
        data = {}
        try:
            data = resp.json()
        except Exception:
            pass

        if resp.status_code < 200 or resp.status_code >= 300:
            err = data.get("description") if isinstance(data, dict) else resp.text
            logger.warning(
                f"Telegram setMyCommands 失败: status={resp.status_code}, token={_mask_token(bot_token)}, err={err}"
            )
            return {"success": False, "data": data, "error": err or f"HTTP {resp.status_code}"}

        if isinstance(data, dict) and not data.get("ok", False):
            err = data.get("description") or "Telegram 返回 ok=false"
            logger.warning(f"Telegram setMyCommands 返回 ok=false: token={_mask_token(bot_token)}, err={err}")
            return {"success": False, "data": data, "error": err}

        return {"success": True, "data": data, "error": None}

    except Exception as e:
        logger.error(f"Telegram setMyCommands 异常: {e} (token={_mask_token(bot_token)})")
        return {"success": False, "data": None, "error": str(e)}
