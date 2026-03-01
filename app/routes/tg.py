"""
Telegram Bot Webhook 路由
用于在 Telegram 中输入命令触发自动兑换与导入补账号
"""
import asyncio
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set, Tuple

from fastapi import APIRouter, HTTPException, Request, status
from sqlalchemy import and_, func, or_, select

from app.database import AsyncSessionLocal
from app.models import RedemptionCode, RedemptionRecord, Team
from app.services.auto_redeem import auto_redeem_by_email
from app.services.settings import settings_service
from app.services.team import team_service
from app.services.telegram import send_message
from app.utils.token_parser import TokenParser
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tg", tags=["telegram"])

# 频率限制：按命令维度区分，避免互相影响
_rate_limit_redeem: Dict[int, float] = {}
_rate_limit_import: Dict[int, float] = {}
_rate_limit_status: Dict[int, float] = {}
_REDEEM_RATE_LIMIT_SECONDS = 5.0
_IMPORT_RATE_LIMIT_SECONDS = 10.0
_STATUS_RATE_LIMIT_SECONDS = 5.0

# 邮箱提取
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# /redeem 命令（支持 /redeem@botname）
_REDEEM_CMD_RE = re.compile(r"^/redeem(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)

# /importteam 命令（支持 /importteam@botname）
_IMPORTTEAM_CMD_RE = re.compile(r"^/importteam(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)

# /status 命令（支持 /status@botname）
_STATUS_CMD_RE = re.compile(r"^/status(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)

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


def _truncate(text: str, limit: int = 260) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "…"


def _format_iso_dt(iso_text: Optional[str]) -> str:
    """
    将 ISO 文本转成更适合阅读的格式（尽量保持简短）。
    """
    s = (iso_text or "").strip()
    if not s:
        return "-"
    s = s.replace("T", " ")
    # 去掉微秒
    if "." in s:
        s = s.split(".", 1)[0]
    return s


def _parse_expires_at(value: Any) -> Optional[datetime]:
    """
    将 expires_at（可能是 datetime / str）解析为 datetime 以用于排序。
    注意：历史数据可能存在格式不一致，解析失败则返回 None（排序时会放到最后）。
    """
    if value is None:
        return None

    dt: Optional[datetime] = None
    try:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            s = s.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(s)
            except ValueError:
                # 兼容部分客户端使用空格而非 "T"
                dt = datetime.fromisoformat(s.replace(" ", "T"))
        else:
            return None

        if dt is None:
            return None

        # 统一到 naive datetime，避免 tz-aware 与 naive 混用导致比较异常
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


_TG_TEMPLATES: Dict[str, str] = {
    "rate_limited": "⏱️ 操作太频繁，请稍后再试。",
    "invalid_email": "📧 请输入有效邮箱。\n\n{help}",
    "redeem_received": "🧾 已收到兑换请求：{email}\n⏳ 正在处理中，请稍候…",
    "import_received": "📥 已收到导入请求\n⏳ 正在处理中，请稍候…",
    "import_private_only": "🛡️ 为安全起见，补账号导入仅支持私聊本 Bot 使用。",
    "need_access_token": (
        "🔑 请粘贴有效的 Access Token(AT)。\n"
        "示例：/importteam <AT>\n"
        "💡 也支持：回复一条包含 AT 的消息后发送 /importteam"
    ),
}


def _tg_text(key: str, **kwargs: Any) -> str:
    tpl = _TG_TEMPLATES.get(key, "")
    try:
        return tpl.format(**kwargs)
    except Exception:
        return tpl


def _build_help_text() -> str:
    return (
        "🤖 使用方法\n"
        "\n"
        "✅ 自动上车：\n"
        "/redeem user@example.com\n"
        "\n"
        "📊 查看业务状态：\n"
        "/status\n"
        "/status full\n"
        "\n"
        "🔑 补账号导入（仅私聊）：\n"
        "/importteam <AT>\n"
        "💡 也支持：回复一条包含 AT 的消息后发送 /importteam\n"
        "\n"
        "📝 说明：系统会自动选择可用兑换码并分配 Team 完成上车；若无可用兑换码会自动生成 10 个无过期质保码后继续。"
    )


def _format_unknown_error(prefix: str, error: str) -> str:
    err = _truncate(_mask_secrets(error), 260)
    if not err:
        return f"❌ {prefix}失败：未知错误"
    return f"❌ {prefix}失败：{err}"


def _friendly_redeem_error(error: str) -> str:
    err = _truncate(_mask_secrets(error), 260)
    err_lower = err.lower()

    # 无可用 Team / 车位相关
    if any(k in err for k in ["没有可用的 Team", "您已加入所有可用 Team", "当前无可用 Team"]):
        return "🚫 当前没有可用车位，请先补账号导入（私聊发送 /importteam <AT>）。"

    # Team 满员
    if any(k in err for k in ["Team 已满", "Team 席位已满"]) or any(
        k in err_lower for k in ["maximum number of seats", "reached maximum number of seats"]
    ):
        return "🚫 当前 Team 席位已满，请稍后重试或先补账号导入（/importteam，仅私聊）。"

    # Team 异常/封禁/Token 失效
    if any(k in err for k in ["Team 账号被封禁", "Team 账号连续出错", "Team 账号 Token 已失效"]):
        return f"⛔ {err}。建议补账号或重新导入（/importteam，仅私聊）。".strip()

    if any(k in err for k in ["所选 Team 已失效", "Team 状态异常"]):
        return f"⚠️ {err}。建议稍后重试或补账号导入。".strip()

    # 自动生成兑换码失败
    if any(k in err for k in ["自动生成兑换码失败", "批量生成兑换码失败"]):
        return "⚠️ 自动生成兑换码失败，请稍后重试。"

    # 兑换码问题（理论上会自动重试，这里给用户更清晰提示）
    if "兑换码" in err and any(k in err for k in ["不存在", "已被使用", "已过期", "已被占用", "记录丢失"]):
        return "🎟️ 兑换码不可用，系统已自动重试；如仍失败请稍后再试。"

    # 其他：尽量展示原错误，便于自用排查
    return _format_unknown_error("兑换", err)


def _friendly_import_error(error: str) -> str:
    err = _truncate(_mask_secrets(error), 260)
    err_lower = err.lower()

    if any(k in err for k in ["已在系统", "已存在", "unique constraint"]):
        return "ℹ️ 账号已存在，无需重复导入。"

    if any(k in err for k in ["Token 对应的账号身份", "邮箱不符", "Session 污染"]):
        return "⚠️ Token 身份与邮箱不一致，可能是 Session 污染。建议清理后重新获取 Token 再导入。"

    if "未发现可导入的 Team 账号" in err:
        return "📭 未发现可导入的 Team 账号（AT 可能无效/无 Team/权限不足）。"

    if any(k in err_lower for k in ["token", "jwt", "unauthorized", "forbidden", "invalid", "expired"]):
        return "🔑 AT 无效或已过期，请重新获取后再试。"

    return _format_unknown_error("导入", err)


def _format_business_status(
    *,
    available_seats: int,
    threshold: int,
    team_total: int,
    team_available: int,
    team_status_counts: Dict[str, int],
    code_total: int,
    code_status_counts: Dict[str, int],
    unused_warranty: int,
    unused_normal: int,
    full: bool = False,
    redeem_24h: Optional[int] = None,
    redeem_7d: Optional[int] = None,
    expiring_teams: Optional[list[dict]] = None,
) -> str:
    known_team_statuses = {"active", "full", "expired", "banned", "error"}
    other_team = sum(v for k, v in team_status_counts.items() if (k or "other") not in known_team_statuses)

    team_active = int(team_status_counts.get("active") or 0)
    team_full = int(team_status_counts.get("full") or 0)
    team_expired = int(team_status_counts.get("expired") or 0)
    team_banned = int(team_status_counts.get("banned") or 0)
    team_error = int(team_status_counts.get("error") or 0)

    known_code_statuses = {"unused", "used", "expired", "warranty_active"}
    other_code = sum(v for k, v in code_status_counts.items() if (k or "other") not in known_code_statuses)

    code_unused = int(code_status_counts.get("unused") or 0)
    code_used = int(code_status_counts.get("used") or 0)
    code_expired = int(code_status_counts.get("expired") or 0)
    code_warranty_active = int(code_status_counts.get("warranty_active") or 0)

    lines = [
        "📊 业务状态",
        f"📦 车位：总可用 {int(available_seats)} ｜🎯 预警阈值 {int(threshold)}",
        f"👥 Team：总 {int(team_total)} ｜可用 {int(team_available)}",
        f"🧩 状态：active {team_active} / full {team_full} / expired {team_expired} / banned {team_banned} / error {team_error} / other {int(other_team)}",
        f"🎟️ 兑换码：总 {int(code_total)}",
        f"📌 状态：unused {code_unused}(质保 {int(unused_warranty)} / 普通 {int(unused_normal)}) / used {code_used} / expired {code_expired} / warranty_active {code_warranty_active} / other {int(other_code)}",
    ]

    if not full:
        return "\n".join(lines).strip()

    lines.extend(["", "📈 兑换趋势"])
    if redeem_24h is not None:
        lines.append(f"- 24h 兑换次数：{int(redeem_24h)}")
    if redeem_7d is not None:
        lines.append(f"- 7d 兑换次数：{int(redeem_7d)}")

    lines.extend(["", "⏳ 即将到期 Team（Top 5）"])
    if expiring_teams:
        for t in expiring_teams[:5]:
            team_id = t.get("team_id")
            team_name = t.get("team_name") or "-"
            expires_at = _format_iso_dt(t.get("expires_at"))
            remaining = t.get("remaining_seats")
            status_text = t.get("status") or "-"
            lines.append(f"- ID {team_id}｜{team_name}｜{expires_at}｜剩余 {remaining}｜{status_text}")
    else:
        lines.append("- （暂无）")

    return "\n".join(lines).strip()


def _format_redeem_result(result: Dict[str, Any]) -> str:
    if result.get("success"):
        team_info = result.get("team_info") or {}
        msg = (result.get("message") or "").strip()
        lines = [f"✅ 兑换成功{('：' + msg) if msg else ''}".strip("：")]
        lines.append(f"🎟️ 兑换码: {result.get('used_code') or '-'}")
        generated = int(result.get("generated_codes") or 0)
        if generated:
            lines.append(f"🆕 自动生成兑换码: {generated} 个")
        team_id = team_info.get("team_id")
        team_name = team_info.get("team_name")
        if team_name or team_id is not None:
            lines.append(f"👥 Team: {team_name or '-'} (ID: {team_id if team_id is not None else '-'})")
        if team_info.get("expires_at"):
            lines.append(f"📅 到期时间: {_format_iso_dt(team_info.get('expires_at'))}")
        if result.get("available_seats") is not None:
            try:
                lines.append(f"📦 当前总可用车位: {int(result.get('available_seats'))}")
            except Exception:
                pass
        return "\n".join(lines).strip()

    return _friendly_redeem_error(result.get("error") or "")


def _format_import_result(result: Dict[str, Any]) -> str:
    if result.get("success"):
        lines = [
            f"✅ 导入成功{('：' + (result.get('message') or '').strip()) if (result.get('message') or '').strip() else ''}".strip("："),
        ]
        email = result.get("email")
        if email:
            lines.append(f"📧 邮箱: {email}")
        team_id = result.get("team_id")
        if team_id is not None:
            lines.append(f"👥 首个 Team ID: {team_id}")
        return "\n".join([l for l in lines if l]).strip()

    return _friendly_import_error(result.get("error") or "")

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

            result = await auto_redeem_by_email(email, db, source="tg", tg_chat_id=chat_id)
            if result.get("success"):
                try:
                    result["available_seats"] = int(await team_service.get_total_available_seats(db))
                except Exception:
                    pass
            text = _format_redeem_result(result)
            await send_message(bot_token, chat_id, text, reply_to_message_id=reply_to_message_id)
        except Exception as e:
            logger.error(f"处理 TG 兑换任务失败: {e}")
            try:
                bot_token = await settings_service.get_setting(db, "tg_bot_token", "")
                if bot_token:
                    err_text = _truncate(_mask_secrets(str(e)), 260) or "系统异常"
                    await send_message(
                        bot_token,
                        chat_id,
                        f"⚠️ 兑换失败：系统异常\n原因：{err_text}",
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
                    err_text = _truncate(_mask_secrets(str(e)), 260) or "系统异常"
                    await send_message(
                        bot_token,
                        chat_id,
                        f"⚠️ 导入失败：系统异常\n原因：{err_text}",
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

    # /status 查看业务状态（只读）
    if cmd == "/status" or _STATUS_CMD_RE.match(text):
        if _rate_limited(chat_id, _rate_limit_status, _STATUS_RATE_LIMIT_SECONDS):
            if bot_token:
                asyncio.create_task(
                    send_message(bot_token, chat_id, _tg_text("rate_limited"), reply_to_message_id=message_id)
                )
            return {"ok": True}

        rest = rest_from_entities
        if not rest:
            m = _STATUS_CMD_RE.match(text)
            rest = (m.group(1) or "").strip() if m else ""
        rest_norm = (rest or "").strip().lower()
        is_full = rest_norm in {"full", "detail", "details", "all"} or (rest or "").strip() in {"全部", "详细"}

        async with AsyncSessionLocal() as db:
            threshold_str = await settings_service.get_setting(db, "low_stock_threshold", "10")
            try:
                threshold = int(threshold_str)
            except Exception:
                threshold = 10

            available_seats = await team_service.get_total_available_seats(db)

            team_total = int((await db.execute(select(func.count()).select_from(Team))).scalar() or 0)
            team_available = int(
                (
                    await db.execute(
                        select(func.count())
                        .select_from(Team)
                        .where(and_(Team.status == "active", Team.current_members < Team.max_members))
                    )
                ).scalar()
                or 0
            )
            team_rows = (await db.execute(select(Team.status, func.count()).group_by(Team.status))).all()
            team_status_counts = {(k or "other"): int(v or 0) for k, v in team_rows}

            code_total = int((await db.execute(select(func.count()).select_from(RedemptionCode))).scalar() or 0)
            code_rows = (await db.execute(select(RedemptionCode.status, func.count()).group_by(RedemptionCode.status))).all()
            code_status_counts = {(k or "other"): int(v or 0) for k, v in code_rows}

            unused_warranty = int(
                (
                    await db.execute(
                        select(func.count())
                        .select_from(RedemptionCode)
                        .where(and_(RedemptionCode.status == "unused", RedemptionCode.has_warranty.is_(True)))
                    )
                ).scalar()
                or 0
            )
            unused_normal = int(
                (
                    await db.execute(
                        select(func.count())
                        .select_from(RedemptionCode)
                        .where(
                            and_(
                                RedemptionCode.status == "unused",
                                or_(RedemptionCode.has_warranty.is_(False), RedemptionCode.has_warranty.is_(None)),
                            )
                        )
                    )
                ).scalar()
                or 0
            )

            redeem_24h = None
            redeem_7d = None
            expiring_teams = None
            if is_full:
                now = get_now()
                since_24h = now - timedelta(hours=24)
                since_7d = now - timedelta(days=7)

                redeem_24h = int(
                    (
                        await db.execute(
                            select(func.count())
                            .select_from(RedemptionRecord)
                            .where(RedemptionRecord.redeemed_at >= since_24h)
                        )
                    ).scalar()
                    or 0
                )
                redeem_7d = int(
                    (
                        await db.execute(
                            select(func.count())
                            .select_from(RedemptionRecord)
                            .where(RedemptionRecord.redeemed_at >= since_7d)
                        )
                    ).scalar()
                    or 0
                )

                exp_rows = (
                    await db.execute(
                        select(
                            Team.id,
                            Team.team_name,
                            Team.expires_at,
                            Team.current_members,
                            Team.max_members,
                            Team.status,
                        )
                        .where(and_(Team.status == "active", Team.expires_at.is_not(None)))
                    )
                ).all()
                candidates = []
                for team_id, team_name, expires_at, current_members, max_members, t_status in exp_rows:
                    parsed_expires_at = _parse_expires_at(expires_at)
                    remaining = int((max_members or 0) - (current_members or 0))
                    expires_text: Optional[str] = None
                    if isinstance(expires_at, datetime):
                        expires_text = expires_at.isoformat()
                    elif isinstance(expires_at, str):
                        expires_text = expires_at

                    candidates.append(
                        {
                            "team_id": team_id,
                            "team_name": team_name,
                            "expires_at": expires_text,
                            "remaining_seats": remaining,
                            "status": t_status,
                            "_expires_at_dt": parsed_expires_at,
                        }
                    )
                candidates.sort(
                    key=lambda t: (
                        t.get("_expires_at_dt") is None,
                        t.get("_expires_at_dt") or datetime.max,
                        int(t.get("team_id") or 0),
                    )
                )
                expiring_teams = [{k: v for k, v in t.items() if k != "_expires_at_dt"} for t in candidates[:5]]

        status_text = _format_business_status(
            available_seats=int(available_seats),
            threshold=int(threshold),
            team_total=team_total,
            team_available=team_available,
            team_status_counts=team_status_counts,
            code_total=code_total,
            code_status_counts=code_status_counts,
            unused_warranty=unused_warranty,
            unused_normal=unused_normal,
            full=is_full,
            redeem_24h=redeem_24h,
            redeem_7d=redeem_7d,
            expiring_teams=expiring_teams,
        )

        if bot_token:
            asyncio.create_task(send_message(bot_token, chat_id, status_text, reply_to_message_id=message_id))
        return {"ok": True}

    # /redeem 邮箱（支持 entities + regex 兜底）
    if cmd == "/redeem" or _REDEEM_CMD_RE.match(text):
        # 频率限制（redeem）
        if _rate_limited(chat_id, _rate_limit_redeem, _REDEEM_RATE_LIMIT_SECONDS):
            if bot_token:
                asyncio.create_task(
                    send_message(bot_token, chat_id, _tg_text("rate_limited"), reply_to_message_id=message_id)
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
                    send_message(
                        bot_token,
                        chat_id,
                        _tg_text("invalid_email", help=_build_help_text()),
                        reply_to_message_id=message_id,
                    )
                )
            return {"ok": True}

        email = email_match.group(0)

        # 先回一条“处理中”，再异步执行兑换并回结果
        if bot_token:
            asyncio.create_task(
                send_message(
                    bot_token,
                    chat_id,
                    _tg_text("redeem_received", email=email),
                    reply_to_message_id=message_id,
                )
            )
        asyncio.create_task(_process_redeem(chat_id, message_id, email))
        return {"ok": True}

    # /importteam 补账号导入（仅私聊）
    if cmd == "/importteam" or _IMPORTTEAM_CMD_RE.match(text):
        # 频率限制（import）
        if _rate_limited(chat_id, _rate_limit_import, _IMPORT_RATE_LIMIT_SECONDS):
            if bot_token:
                asyncio.create_task(
                    send_message(bot_token, chat_id, _tg_text("rate_limited"), reply_to_message_id=message_id)
                )
            return {"ok": True}

        if chat_type != "private":
            if bot_token:
                asyncio.create_task(
                    send_message(bot_token, chat_id, _tg_text("import_private_only"), reply_to_message_id=message_id)
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
                        _tg_text("need_access_token"),
                        reply_to_message_id=message_id,
                    )
                )
            return {"ok": True}

        if bot_token:
            asyncio.create_task(
                send_message(bot_token, chat_id, _tg_text("import_received"), reply_to_message_id=message_id)
            )
        asyncio.create_task(_process_import(chat_id, message_id, access_token))
        return {"ok": True}

    # 默认不响应非命令，避免群里噪音
    return {"ok": True}
