"""
Telegram Bot Webhook 路由
用于在 Telegram 中输入命令触发自动兑换与导入补账号
"""
import asyncio
import logging
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set, Tuple

from fastapi import APIRouter, HTTPException, Request, status
from sqlalchemy import and_, func, or_, select, case

from app.database import AsyncSessionLocal
from app.models import RedemptionCode, RedemptionRecord, Team
from app.services.auto_redeem import auto_redeem_by_email
from app.services.redemption import RedemptionService
from app.services.settings import settings_service
from app.services.team import team_service
from app.services.telegram import answer_callback_query, send_message
from app.utils.token_parser import TokenParser
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tg", tags=["telegram"])

# 服务实例
redemption_service = RedemptionService()

# 频率限制：按命令维度区分，避免互相影响
_rate_limit_redeem: Dict[int, float] = {}
_rate_limit_import: Dict[int, float] = {}
_rate_limit_status: Dict[int, float] = {}
_rate_limit_records: Dict[int, float] = {}
_rate_limit_withdraw: Dict[int, float] = {}
_REDEEM_RATE_LIMIT_SECONDS = 5.0
_IMPORT_RATE_LIMIT_SECONDS = 10.0
_STATUS_RATE_LIMIT_SECONDS = 5.0
_RECORDS_RATE_LIMIT_SECONDS = 3.0
_WITHDRAW_RATE_LIMIT_SECONDS = 10.0

# 每分钟上限（额外兜底）
_records_minute_bucket: Dict[int, Tuple[float, int]] = {}
_withdraw_minute_bucket: Dict[int, Tuple[float, int]] = {}
_RECORDS_PER_MINUTE_LIMIT = 30
_WITHDRAW_PER_MINUTE_LIMIT = 10

# Bot 统计（仅内存，单实例使用足够）
_bot_metrics: Dict[str, Dict[str, Any]] = {
    "records": {"calls": 0, "success": 0, "fail": 0, "rate_limited": 0, "last_at": None},
    "withdraw": {"calls": 0, "success": 0, "fail": 0, "rate_limited": 0, "last_at": None},
}
_rate_limit_alert_counts: Dict[str, int] = {"records": 0, "withdraw": 0}

# 限流告警：同一命令 60s 内命中 >=3 次则提示一次，冷却 10min
_rate_limit_alert_state: Dict[Tuple[int, str], Dict[str, Any]] = {}
_RATE_LIMIT_ALERT_WINDOW_SECONDS = 60.0
_RATE_LIMIT_ALERT_THRESHOLD = 3
_RATE_LIMIT_ALERT_COOLDOWN_SECONDS = 600.0

# 撤销流程的 pending（候选选择 / 二次确认），仅内存（单实例足够）
_pending_withdraw_select: Dict[str, Dict[str, Any]] = {}
_pending_withdraw_confirm: Dict[str, Dict[str, Any]] = {}
_PENDING_TTL_SECONDS = 120.0

# 邮箱提取
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# /redeem 命令（支持 /redeem@botname）
_REDEEM_CMD_RE = re.compile(r"^/redeem(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)

# /importteam 命令（支持 /importteam@botname）
_IMPORTTEAM_CMD_RE = re.compile(r"^/importteam(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)

# /status 命令（支持 /status@botname）
_STATUS_CMD_RE = re.compile(r"^/status(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)

# /records 命令（支持 /records@botname）
_RECORDS_CMD_RE = re.compile(r"^/records(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)

# /withdraw 命令（支持 /withdraw@botname）
_WITHDRAW_CMD_RE = re.compile(r"^/withdraw(?:@[\w_]+)?(?:\s+(.+))?$", re.IGNORECASE)

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


def _minute_limited(chat_id: int, bucket: Dict[int, Tuple[float, int]], limit: int) -> bool:
    """
    简单的每分钟限流（固定窗口，使用 monotonic 时间）。
    """
    now = time.monotonic()
    window_start, count = bucket.get(chat_id, (now, 0))
    if now - window_start >= 60.0:
        window_start, count = now, 0
    count += 1
    bucket[chat_id] = (window_start, count)
    return count > int(limit)


def _metric_inc(command: str, key: str) -> None:
    m = _bot_metrics.get(command)
    if not m:
        return
    try:
        m[key] = int(m.get(key) or 0) + 1
    except Exception:
        m[key] = 1


def _metric_touch(command: str) -> None:
    m = _bot_metrics.get(command)
    if not m:
        return
    m["last_at"] = get_now()


def _track_rate_limit_hit(chat_id: int, command: str) -> bool:
    """
    返回 True 表示需要发送一次“频繁触发限流”的提醒。
    """
    now = time.monotonic()
    key = (int(chat_id), str(command))
    st = _rate_limit_alert_state.get(key)
    if not st or now - float(st.get("window_start") or 0) >= _RATE_LIMIT_ALERT_WINDOW_SECONDS:
        # 新窗口不重置 last_warn，保证 10min 冷却在跨窗口场景下仍然生效
        st = {"window_start": now, "hits": 0, "last_warn": float(st.get("last_warn") or 0.0) if st else 0.0}

    st["hits"] = int(st.get("hits") or 0) + 1
    should_warn = False
    if st["hits"] >= int(_RATE_LIMIT_ALERT_THRESHOLD):
        last_warn = float(st.get("last_warn") or 0.0)
        if now - last_warn >= float(_RATE_LIMIT_ALERT_COOLDOWN_SECONDS):
            st["last_warn"] = now
            should_warn = True
            _rate_limit_alert_counts[command] = int(_rate_limit_alert_counts.get(command) or 0) + 1
            logger.warning(f"TG rate-limit alert: chat_id={chat_id}, cmd={command}, hits_in_60s={st['hits']}")

    _rate_limit_alert_state[key] = st
    return should_warn


def _cleanup_pending() -> None:
    """
    清理过期 pending，避免长时间运行后字典膨胀（单实例）。
    """
    now = time.monotonic()

    def _gc(d: Dict[str, Dict[str, Any]]) -> None:
        if not d:
            return
        expired = []
        for k, v in d.items():
            created = float(v.get("created_at") or 0.0)
            if created <= 0 or now - created >= float(_PENDING_TTL_SECONDS):
                expired.append(k)
        for k in expired:
            d.pop(k, None)

    _gc(_pending_withdraw_select)
    _gc(_pending_withdraw_confirm)


def _format_dt(value: Any) -> str:
    """
    将 datetime/str/None 格式化为统一展示格式。
    """
    if value is None:
        return "-"
    try:
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, str):
            return _format_iso_dt(value)
    except Exception:
        pass
    return "-"


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


def _extract_callback_query(update: Dict[str, Any]) -> Tuple[Optional[str], Optional[int], Optional[int], Optional[str], Optional[str]]:
    """
    从 Telegram callback_query update 中提取 (callback_query_id, chat_id, message_id, data, chat_type)。
    """
    cb = update.get("callback_query")
    if not isinstance(cb, dict):
        return None, None, None, None, None
    cb_id = cb.get("id")
    data = cb.get("data")
    msg = cb.get("message") or {}
    if not isinstance(msg, dict):
        msg = {}
    chat = msg.get("chat") or {}
    if not isinstance(chat, dict):
        chat = {}
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    message_id = msg.get("message_id")
    if not isinstance(cb_id, str) or not cb_id:
        cb_id = None
    if not isinstance(data, str) or not data:
        data = None
    return cb_id, chat_id, message_id, data, chat_type


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


def _build_help_text(*, is_superadmin: bool, can_redeem: bool) -> str:
    lines = ["🤖 使用方法", ""]

    if can_redeem:
        lines.extend(["✅ 自动上车（仅私聊）：", "/redeem user@example.com", ""])

    lines.extend(
        [
            "🧾 查询使用记录（仅私聊）：",
            "/records user@example.com",
            "/records user@example.com 10",
            "",
            "🧹 撤销上车（仅私聊，需确认）：",
            "/withdraw user@example.com",
            "/withdraw 123",
            "",
        ]
    )

    if is_superadmin:
        lines.extend(
            [
                "📊 查看业务状态（仅超管私聊）：",
                "/status",
                "/status full",
                "",
                "🔑 补账号导入（仅超管私聊）：",
                "/importteam <AT>",
                "💡 也支持：回复一条包含 AT 的消息后发送 /importteam",
                "",
            ]
        )

    lines.append("📝 说明：系统会自动选择可用兑换码并分配 Team 完成上车；若无可用兑换码会自动生成 10 个无过期质保码后继续。")
    if not is_superadmin:
        lines.append("ℹ️ 如需查看系统状态/补账号导入，请联系管理员开通超管权限。")

    return "\n".join([l for l in lines if l is not None]).strip()


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
        return "🚫 当前没有可用车位，请联系管理员补货导入新账号（超管私聊 /importteam）。"

    # Team 满员
    if any(k in err for k in ["Team 已满", "Team 席位已满"]) or any(
        k in err_lower for k in ["maximum number of seats", "reached maximum number of seats"]
    ):
        return "🚫 当前 Team 席位已满，请稍后重试或联系管理员补货导入新账号（超管私聊 /importteam）。"

    # Team 异常/封禁/Token 失效
    if any(k in err for k in ["Team 账号被封禁", "Team 账号连续出错", "Team 账号 Token 已失效"]):
        return f"⛔ {err}。请联系管理员补账号或重新导入（超管私聊 /importteam）。".strip()

    if any(k in err for k in ["所选 Team 已失效", "Team 状态异常"]):
        return f"⚠️ {err}。建议稍后重试；如持续失败请联系管理员补账号或重新导入（超管私聊 /importteam）。".strip()

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
    records_total: Optional[int] = None,
    records_today: Optional[int] = None,
    records_this_week: Optional[int] = None,
    records_this_month: Optional[int] = None,
    full: bool = False,
    redeem_24h: Optional[int] = None,
    redeem_7d: Optional[int] = None,
    expiring_teams: Optional[list[dict]] = None,
    bot_metrics: Optional[Dict[str, Dict[str, Any]]] = None,
    rate_limit_alert_counts: Optional[Dict[str, int]] = None,
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
    if (
        records_total is not None
        and records_today is not None
        and records_this_week is not None
        and records_this_month is not None
    ):
        lines.append(
            "🧾 使用记录：总 {total} ｜今日 {today} ｜本周 {week} ｜本月 {month}".format(
                total=int(records_total),
                today=int(records_today),
                week=int(records_this_week),
                month=int(records_this_month),
            )
        )

    if not full:
        return "\n".join(lines).strip()

    lines.extend(["", "📈 兑换趋势"])
    if redeem_24h is not None:
        lines.append(f"- 24h 兑换次数：{int(redeem_24h)}")
    if redeem_7d is not None:
        lines.append(f"- 7d 兑换次数：{int(redeem_7d)}")

    if (
        records_total is not None
        and records_today is not None
        and records_this_week is not None
        and records_this_month is not None
    ):
        lines.extend(["", "🧾 使用记录"])
        lines.append(f"- 总记录数：{int(records_total)}")
        lines.append(f"- 今日使用：{int(records_today)}")
        lines.append(f"- 本周使用：{int(records_this_week)}")
        lines.append(f"- 本月使用：{int(records_this_month)}")

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

    if bot_metrics:
        lines.extend(["", "🤖 Bot 统计"])
        for cmd in ("records", "withdraw"):
            m = bot_metrics.get(cmd) or {}
            calls = int(m.get("calls") or 0)
            succ = int(m.get("success") or 0)
            fail = int(m.get("fail") or 0)
            rl = int(m.get("rate_limited") or 0)
            last_at = m.get("last_at")
            last_text = _format_dt(last_at) if last_at else "-"
            lines.append(f"- /{cmd}：调用 {calls}｜成功 {succ}｜失败 {fail}｜限流 {rl}｜最后 {last_text}")

        if rate_limit_alert_counts:
            rec = int(rate_limit_alert_counts.get("records") or 0)
            wd = int(rate_limit_alert_counts.get("withdraw") or 0)
            total_alert = rec + wd
            lines.append(f"- ⚠️ 限流告警：{total_alert}（records {rec} / withdraw {wd}）")

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
                team_info = result.get("team_info") or {}
                logger.info(
                    "TG audit: action=redeem_success, actor_chat_id=%s, email=%s, team_id=%s, used_code=%s, generated_codes=%s",
                    chat_id,
                    email,
                    team_info.get("team_id"),
                    result.get("used_code"),
                    result.get("generated_codes"),
                )
            else:
                err = _truncate(_mask_secrets(str(result.get("error") or "")), 200)
                logger.info(
                    "TG audit: action=redeem_fail, actor_chat_id=%s, email=%s, error=%s",
                    chat_id,
                    email,
                    err or "-",
                )
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
            if result.get("success"):
                logger.info(
                    "TG audit: action=importteam_success, actor_chat_id=%s, email=%s, team_id=%s",
                    chat_id,
                    result.get("email"),
                    result.get("team_id"),
                )
            else:
                err = _truncate(_mask_secrets(str(result.get("error") or "")), 200)
                logger.info(
                    "TG audit: action=importteam_fail, actor_chat_id=%s, error=%s",
                    chat_id,
                    err or "-",
                )
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


async def _process_records(
    chat_id: int,
    reply_to_message_id: Optional[int],
    email: str,
    *,
    limit: int = 5,
    is_all: bool = False,
    is_superadmin: bool = False,
) -> None:
    """
    后台任务：按邮箱查询使用记录并回发到 Telegram
    """
    async with AsyncSessionLocal() as db:
        bot_token = ""
        try:
            tg_enabled = (await settings_service.get_setting(db, "tg_enabled", "false") or "false").lower() == "true"
            if not tg_enabled:
                return

            bot_token = await settings_service.get_setting(db, "tg_bot_token", "")
            if not bot_token:
                logger.error("TG Bot Token 未配置，无法发送消息")
                return

            email = (email or "").strip()
            if not email:
                return

            try:
                limit = int(limit)
            except Exception:
                limit = 5
            if limit < 1:
                limit = 1
            if limit > 20:
                limit = 20

            now = get_now()
            stmt = (
                select(
                    RedemptionRecord,
                    Team.team_name,
                    Team.status,
                    Team.expires_at,
                )
                .join(Team, Team.id == RedemptionRecord.team_id, isouter=bool(is_all and is_superadmin))
                .where(RedemptionRecord.email == email)
                .order_by(RedemptionRecord.redeemed_at.desc(), RedemptionRecord.id.desc())
                .limit(limit)
            )

            if not (is_all and is_superadmin):
                stmt = stmt.where(
                    and_(
                        or_(Team.status.is_(None), Team.status.notin_(["expired", "banned", "error"])),
                        or_(Team.expires_at.is_(None), Team.expires_at > now),
                    )
                )

            if not is_superadmin:
                stmt = stmt.where(and_(RedemptionRecord.source == "tg", RedemptionRecord.tg_chat_id == int(chat_id)))

            rows = (await db.execute(stmt)).all()
            found = len(rows)

            title = (
                "🧾 使用记录（全量历史｜最近 {} 条）".format(found)
                if (is_all and is_superadmin)
                else "🧾 使用记录（有效期内｜最近 {} 条）".format(found)
            )
            lines = [title, f"📧 邮箱: {email}", ""]

            if not rows:
                lines.append("📭 未找到该邮箱的使用记录。")
            else:
                for record, team_name, _team_status, team_expires_at in rows:
                    src = (record.source or "user").strip().lower() or "user"
                    redeemed_at_text = _format_dt(record.redeemed_at)
                    expires_text = _format_dt(team_expires_at) if team_expires_at is not None else "未知"
                    team_name_text = (team_name or "-").strip()
                    lines.append(
                        f"- #{record.id}｜📅 {redeemed_at_text}｜👥 Team {record.team_id}({team_name_text})｜🎟️ {record.code}"
                    )
                    detail_parts = [f"⏳ 到期: {expires_text}", f"📍 {src}"]
                    if src == "tg" and record.tg_chat_id is not None:
                        detail_parts.append(f"💬 {record.tg_chat_id}")
                    lines.append("  " + "｜".join(detail_parts))

                if found == limit and limit < 20:
                    lines.append("…（可能还有更多记录，可提高 n，最多 20）")

            await send_message(bot_token, chat_id, "\n".join(lines).strip(), reply_to_message_id=reply_to_message_id)
            _metric_inc("records", "success")
            _metric_touch("records")

        except Exception as e:
            _metric_inc("records", "fail")
            _metric_touch("records")
            logger.error(f"处理 TG /records 失败: {e}")
            try:
                if bot_token:
                    err_text = _truncate(_mask_secrets(str(e)), 260) or "系统异常"
                    await send_message(
                        bot_token,
                        chat_id,
                        f"⚠️ 查询失败：系统异常\n原因：{err_text}",
                        reply_to_message_id=reply_to_message_id,
                    )
            except Exception:
                pass


async def _process_withdraw_send_candidates(
    chat_id: int,
    reply_to_message_id: Optional[int],
    email: str,
    *,
    is_superadmin: bool,
) -> None:
    """
    按邮箱撤销：先返回最近候选记录供点击选择（避免手输 record_id）。
    """
    async with AsyncSessionLocal() as db:
        bot_token = ""
        try:
            tg_enabled = (await settings_service.get_setting(db, "tg_enabled", "false") or "false").lower() == "true"
            if not tg_enabled:
                return

            bot_token = await settings_service.get_setting(db, "tg_bot_token", "")
            if not bot_token:
                logger.error("TG Bot Token 未配置，无法发送消息")
                return

            withdraw_enabled = (await settings_service.get_setting(db, "tg_withdraw_enabled", "true") or "true").lower() == "true"
            if not withdraw_enabled:
                await send_message(
                    bot_token,
                    chat_id,
                    "🚫 撤销功能已禁用（后台可开启 tg_withdraw_enabled）。",
                    reply_to_message_id=reply_to_message_id,
                )
                return

            email = (email or "").strip()
            if not email:
                return

            now = get_now()
            since_24h = now - timedelta(hours=24)

            stmt = (
                select(
                    RedemptionRecord.id,
                    RedemptionRecord.redeemed_at,
                    RedemptionRecord.team_id,
                    RedemptionRecord.code,
                    RedemptionRecord.source,
                    RedemptionRecord.tg_chat_id,
                    Team.team_name,
                    Team.status,
                    Team.expires_at,
                )
                .join(Team, Team.id == RedemptionRecord.team_id)
                .where(
                    and_(
                        RedemptionRecord.email == email,
                        RedemptionRecord.redeemed_at.is_not(None),
                        RedemptionRecord.redeemed_at >= since_24h,
                        or_(Team.status.is_(None), Team.status.notin_(["expired", "banned", "error"])),
                        or_(Team.expires_at.is_(None), Team.expires_at > now),
                    )
                )
                .order_by(RedemptionRecord.redeemed_at.desc(), RedemptionRecord.id.desc())
                .limit(3)
            )
            if not is_superadmin:
                stmt = stmt.where(and_(RedemptionRecord.source == "tg", RedemptionRecord.tg_chat_id == int(chat_id)))

            rows = (await db.execute(stmt)).all()
            if not rows:
                await send_message(
                    bot_token,
                    chat_id,
                    "📭 未找到最近 24h 内可撤销的记录。\n"
                    "💡 你可以先用 /records 查询 record_id，再用 /withdraw <记录ID> 撤销。",
                    reply_to_message_id=reply_to_message_id,
                )
                return

            _cleanup_pending()
            token = secrets.token_urlsafe(8)
            _pending_withdraw_select[token] = {
                "chat_id": int(chat_id),
                "email": email,
                "record_ids": [int(r[0]) for r in rows],
                "created_at": time.monotonic(),
            }

            lines = [f"🧹 请选择要撤销的记录（最近 {len(rows)} 条）", f"📧 邮箱: {email}", ""]
            keyboard = []
            for record_id, redeemed_at, team_id, code, source, tg_chat_id, team_name, team_status, team_expires_at in rows:
                lines.append(
                    f"- #{record_id}｜📅 {_format_dt(redeemed_at)}｜👥 Team {team_id}({(team_name or '-').strip()})｜🎟️ {code}"
                )
                keyboard.append(
                    [{"text": f"🧹 撤销 #{record_id}", "callback_data": f"v1:ws:pick:{token}:{int(record_id)}"}]
                )
            keyboard.append([{"text": "❎ 取消", "callback_data": f"v1:ws:cancel:{token}"}])

            await send_message(
                bot_token,
                chat_id,
                "\n".join(lines).strip(),
                reply_to_message_id=reply_to_message_id,
                reply_markup={"inline_keyboard": keyboard},
            )
        except Exception as e:
            _metric_inc("withdraw", "fail")
            _metric_touch("withdraw")
            logger.error(f"处理 TG /withdraw 候选失败: {e}")
            try:
                if bot_token:
                    err_text = _truncate(_mask_secrets(str(e)), 260) or "系统异常"
                    await send_message(
                        bot_token,
                        chat_id,
                        f"⚠️ 撤销失败：系统异常\n原因：{err_text}",
                        reply_to_message_id=reply_to_message_id,
                    )
            except Exception:
                pass


async def _process_withdraw_prepare_confirm(
    chat_id: int,
    reply_to_message_id: Optional[int],
    record_id: int,
    *,
    is_superadmin: bool,
) -> None:
    """
    根据 record_id 发送撤销确认卡片（按钮二次确认），不执行撤销。
    """
    async with AsyncSessionLocal() as db:
        bot_token = ""
        try:
            tg_enabled = (await settings_service.get_setting(db, "tg_enabled", "false") or "false").lower() == "true"
            if not tg_enabled:
                return

            bot_token = await settings_service.get_setting(db, "tg_bot_token", "")
            if not bot_token:
                logger.error("TG Bot Token 未配置，无法发送消息")
                return

            withdraw_enabled = (await settings_service.get_setting(db, "tg_withdraw_enabled", "true") or "true").lower() == "true"
            if not withdraw_enabled:
                await send_message(
                    bot_token,
                    chat_id,
                    "🚫 撤销功能已禁用（后台可开启 tg_withdraw_enabled）。",
                    reply_to_message_id=reply_to_message_id,
                )
                return

            try:
                record_id_int = int(record_id)
            except Exception:
                record_id_int = 0
            if record_id_int <= 0:
                await send_message(bot_token, chat_id, "📭 无效的记录ID。", reply_to_message_id=reply_to_message_id)
                return

            row = (
                await db.execute(
                    select(
                        RedemptionRecord,
                        Team.team_name,
                        Team.status,
                        Team.expires_at,
                    )
                    .join(Team, Team.id == RedemptionRecord.team_id, isouter=True)
                    .where(RedemptionRecord.id == record_id_int)
                )
            ).first()
            if not row:
                await send_message(bot_token, chat_id, "📭 记录不存在或已撤销。", reply_to_message_id=reply_to_message_id)
                return

            record, team_name, team_status, team_expires_at = row
            if not is_superadmin:
                if (record.source or "").strip().lower() != "tg" or record.tg_chat_id != int(chat_id):
                    await send_message(bot_token, chat_id, "⛔ 无权限撤销该记录。", reply_to_message_id=reply_to_message_id)
                    return

            _cleanup_pending()
            token = secrets.token_urlsafe(8)
            _pending_withdraw_confirm[token] = {
                "chat_id": int(chat_id),
                "record_id": int(record.id),
                "created_at": time.monotonic(),
                "is_superadmin": bool(is_superadmin),
            }

            now = get_now()
            warning = ""
            try:
                if record.redeemed_at and (now - record.redeemed_at).total_seconds() < 60:
                    warning = "🚧 该记录刚上车（<60s），确认后仍会撤销，请谨慎。\n"
            except Exception:
                pass

            expires_text = _format_dt(team_expires_at) if team_expires_at is not None else "未知"
            status_text = (team_status or "-").strip() if isinstance(team_status, str) else (team_status or "-")
            team_name_text = (team_name or "-").strip()

            lines = [
                "⚠️ 即将撤销上车记录，请确认",
                f"🆔 记录ID: {record.id}",
                f"📧 邮箱: {record.email}",
                f"👥 Team: {record.team_id} ({team_name_text})",
                f"🎟️ 兑换码: {record.code}",
                f"📅 兑换时间: {_format_dt(record.redeemed_at)}",
                f"📍 来源: {(record.source or 'user').strip().lower() or 'user'}",
                f"📌 Team 状态: {status_text}｜⏳ 到期: {expires_text}",
                (f"💬 TG Chat ID: {record.tg_chat_id}" if record.tg_chat_id is not None else ""),
                "",
                "🧹 将执行：撤回邀请/移除成员 + 恢复兑换码 + 删除使用记录",
                "ℹ️ 若成员已不存在，仍会恢复兑换码并删除记录（以系统返回为准）",
            ]
            text = "\n".join([warning + "\n".join([l for l in lines if l]).strip()]).strip()

            keyboard = {
                "inline_keyboard": [
                    [{"text": "✅ 确认撤销", "callback_data": f"v1:wd:ok:{token}"}],
                    [{"text": "❎ 取消", "callback_data": f"v1:wd:cancel:{token}"}],
                ]
            }

            await send_message(
                bot_token,
                chat_id,
                text,
                reply_to_message_id=reply_to_message_id,
                reply_markup=keyboard,
            )

        except Exception as e:
            _metric_inc("withdraw", "fail")
            _metric_touch("withdraw")
            logger.error(f"准备撤销确认失败: {e}")
            try:
                if bot_token:
                    err_text = _truncate(_mask_secrets(str(e)), 260) or "系统异常"
                    await send_message(
                        bot_token,
                        chat_id,
                        f"⚠️ 撤销失败：系统异常\n原因：{err_text}",
                        reply_to_message_id=reply_to_message_id,
                    )
            except Exception:
                pass


def _friendly_withdraw_error(error: str) -> str:
    err = _truncate(_mask_secrets(error), 260)
    err_lower = err.lower()

    if not err:
        return "❌ 撤销失败：未知错误"

    if "记录" in err and "不存在" in err:
        return "ℹ️ 记录不存在或已撤销。"

    if any(k in err for k in ["无权限", "没有权限"]) or any(k in err_lower for k in ["forbidden", "not allowed", "permission"]):
        return "⛔ 无权限撤销该记录。"

    if any(k in err for k in ["从 Team 移除成员失败", "撤回邀请", "删除成员"]):
        return f"⚠️ 撤销失败：{err}"

    return f"❌ 撤销失败：{err}"


async def _process_withdraw_execute(
    chat_id: int,
    reply_to_message_id: Optional[int],
    record_id: int,
    *,
    is_superadmin: bool,
) -> None:
    """
    后台任务：执行撤销并回发结果到 Telegram
    """
    async with AsyncSessionLocal() as db:
        bot_token = ""
        try:
            tg_enabled = (await settings_service.get_setting(db, "tg_enabled", "false") or "false").lower() == "true"
            if not tg_enabled:
                return

            bot_token = await settings_service.get_setting(db, "tg_bot_token", "")
            if not bot_token:
                logger.error("TG Bot Token 未配置，无法发送消息")
                return

            withdraw_enabled = (await settings_service.get_setting(db, "tg_withdraw_enabled", "true") or "true").lower() == "true"
            if not withdraw_enabled:
                await send_message(
                    bot_token,
                    chat_id,
                    "🚫 撤销功能已禁用（后台可开启 tg_withdraw_enabled）。",
                    reply_to_message_id=reply_to_message_id,
                )
                return

            # 读取记录摘要（撤销后记录会被删除）
            row = (
                await db.execute(
                    select(
                        RedemptionRecord,
                        Team.team_name,
                        Team.status,
                        Team.expires_at,
                    )
                    .join(Team, Team.id == RedemptionRecord.team_id, isouter=True)
                    .where(RedemptionRecord.id == int(record_id))
                )
            ).first()
            if not row:
                await send_message(
                    bot_token,
                    chat_id,
                    "ℹ️ 记录不存在或已撤销。",
                    reply_to_message_id=reply_to_message_id,
                )
                _metric_inc("withdraw", "fail")
                _metric_touch("withdraw")
                return

            record, team_name, team_status, team_expires_at = row
            if not is_superadmin:
                if (record.source or "").strip().lower() != "tg" or record.tg_chat_id != int(chat_id):
                    await send_message(
                        bot_token,
                        chat_id,
                        "⛔ 无权限撤销该记录。",
                        reply_to_message_id=reply_to_message_id,
                    )
                    _metric_inc("withdraw", "fail")
                    _metric_touch("withdraw")
                    return

            logger.info(
                f"TG withdraw execute: actor_chat_id={chat_id}, is_superadmin={is_superadmin}, record_id={record.id}, email={record.email}"
            )

            result = await redemption_service.withdraw_record(int(record.id), db)
            if result.get("success"):
                msg = (result.get("message") or "").strip()
                team_name_text = (team_name or "-").strip()
                lines = [
                    f"✅ 撤销成功{('：' + msg) if msg else ''}".strip("："),
                    f"🆔 记录ID: {record.id}",
                    f"📧 邮箱: {record.email}",
                    f"👥 Team: {record.team_id} ({team_name_text})",
                    f"🎟️ 兑换码: {record.code}",
                    f"📅 原兑换时间: {_format_dt(record.redeemed_at)}",
                    f"📍 来源: {(record.source or 'user').strip().lower() or 'user'}",
                ]
                await send_message(bot_token, chat_id, "\n".join(lines).strip(), reply_to_message_id=reply_to_message_id)
                _metric_inc("withdraw", "success")
                _metric_touch("withdraw")
            else:
                err = result.get("error") or "撤销失败"
                await send_message(bot_token, chat_id, _friendly_withdraw_error(str(err)), reply_to_message_id=reply_to_message_id)
                _metric_inc("withdraw", "fail")
                _metric_touch("withdraw")
        except Exception as e:
            _metric_inc("withdraw", "fail")
            _metric_touch("withdraw")
            logger.error(f"处理 TG 撤销任务失败: {e}")
            try:
                if bot_token:
                    err_text = _truncate(_mask_secrets(str(e)), 260) or "系统异常"
                    await send_message(
                        bot_token,
                        chat_id,
                        f"⚠️ 撤销失败：系统异常\n原因：{err_text}",
                        reply_to_message_id=reply_to_message_id,
                    )
            except Exception:
                pass


async def _process_withdraw_command(
    chat_id: int,
    reply_to_message_id: Optional[int],
    rest: str,
    *,
    is_superadmin: bool,
) -> None:
    rest = (rest or "").strip()
    if not rest:
        return

    first = (rest.split() or [""])[0].strip()
    if first.isdigit():
        try:
            record_id = int(first)
        except Exception:
            record_id = 0
        await _process_withdraw_prepare_confirm(chat_id, reply_to_message_id, record_id, is_superadmin=is_superadmin)
        return

    email_match = _EMAIL_RE.search(rest)
    if not email_match:
        # 兜底：提示用法（由上层发送也可，这里再做一次保护）
        async with AsyncSessionLocal() as db:
            bot_token = await settings_service.get_setting(db, "tg_bot_token", "")
            if bot_token:
                await send_message(bot_token, chat_id, "📧 请输入有效邮箱或记录ID。\n用法：/withdraw 邮箱 或 /withdraw 记录ID", reply_to_message_id=reply_to_message_id)
        return

    await _process_withdraw_send_candidates(chat_id, reply_to_message_id, email_match.group(0), is_superadmin=is_superadmin)


async def _handle_callback_query(
    chat_id: int,
    reply_to_message_id: Optional[int],
    data: Optional[str],
    *,
    callback_query_id: str,
    bot_token: str,
    withdraw_enabled: bool,
    is_superadmin: bool,
) -> None:
    """
    处理 InlineKeyboard 回调：
    - v1:ws:pick:<token>:<record_id>  选择候选记录
    - v1:ws:cancel:<token>           取消候选选择
    - v1:wd:ok:<token>               确认撤销
    - v1:wd:cancel:<token>           取消撤销
    """
    data = (data or "").strip()
    if not data or not bot_token:
        return

    _cleanup_pending()

    parts = data.split(":")
    if len(parts) < 3 or parts[0] != "v1":
        return

    kind = parts[1]
    action = parts[2]

    # 撤销相关流程统一受开关控制
    if kind in {"ws", "wd"} and not withdraw_enabled:
        asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="撤销已禁用"))
        asyncio.create_task(
            send_message(
                bot_token,
                chat_id,
                "🚫 撤销功能已禁用（后台可开启 tg_withdraw_enabled）。",
                reply_to_message_id=reply_to_message_id,
            )
        )
        return

    if kind == "ws":
        # 候选选择阶段
        if len(parts) < 4:
            asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="参数错误"))
            return
        token = parts[3]
        pending = _pending_withdraw_select.get(token)
        if not pending or int(pending.get("chat_id") or 0) != int(chat_id):
            asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="已过期"))
            asyncio.create_task(
                send_message(
                    bot_token,
                    chat_id,
                    "⏳ 操作已过期，请重新发送 /withdraw。",
                    reply_to_message_id=reply_to_message_id,
                )
            )
            return

        if action == "cancel":
            _pending_withdraw_select.pop(token, None)
            asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="已取消"))
            asyncio.create_task(
                send_message(bot_token, chat_id, "👌 已取消，不会执行撤销。", reply_to_message_id=reply_to_message_id)
            )
            return

        if action == "pick":
            if len(parts) < 5:
                asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="参数错误"))
                return
            try:
                record_id = int(parts[4])
            except Exception:
                record_id = 0
            allowed_ids = set(int(x) for x in (pending.get("record_ids") or []) if isinstance(x, int) or str(x).isdigit())
            if record_id <= 0 or record_id not in allowed_ids:
                asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="无效选择"))
                return

            _pending_withdraw_select.pop(token, None)
            asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="已选择"))
            asyncio.create_task(_process_withdraw_prepare_confirm(chat_id, reply_to_message_id, record_id, is_superadmin=is_superadmin))
            return

        asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="未知操作"))
        return

    if kind == "wd":
        # 二次确认阶段
        if len(parts) < 4:
            asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="参数错误"))
            return
        token = parts[3]
        pending = _pending_withdraw_confirm.get(token)
        if not pending or int(pending.get("chat_id") or 0) != int(chat_id):
            asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="已过期"))
            asyncio.create_task(
                send_message(
                    bot_token,
                    chat_id,
                    "⏳ 确认已过期，请重新发起 /withdraw。",
                    reply_to_message_id=reply_to_message_id,
                )
            )
            return

        record_id = int(pending.get("record_id") or 0)

        if action == "cancel":
            _pending_withdraw_confirm.pop(token, None)
            asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="已取消"))
            asyncio.create_task(
                send_message(bot_token, chat_id, "👌 已取消，不会执行撤销。", reply_to_message_id=reply_to_message_id)
            )
            return

        if action == "ok":
            # 幂等：先移除 token，避免重复点击重复执行
            _pending_withdraw_confirm.pop(token, None)
            asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="开始撤销…"))
            asyncio.create_task(
                send_message(bot_token, chat_id, "⏳ 正在撤销，请稍候…", reply_to_message_id=reply_to_message_id)
            )
            # 以“当前配置”为准：不要因为 pending 创建时是超管就保留特权
            asyncio.create_task(
                _process_withdraw_execute(chat_id, reply_to_message_id, record_id, is_superadmin=is_superadmin)
            )
            return

        asyncio.create_task(answer_callback_query(bot_token, callback_query_id, text="未知操作"))
        return

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
        redeem_raw = await settings_service.get_setting(db, "tg_redeem_chat_ids", "")
        bot_token = await settings_service.get_setting(db, "tg_bot_token", "")
        withdraw_enabled_raw = await settings_service.get_setting(db, "tg_withdraw_enabled", "true")
        super_admin_raw = await settings_service.get_setting(db, "tg_super_admin_chat_ids", "")

    # 校验 secret token（防止伪造回调）
    header_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not secret_token or header_secret != secret_token:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="invalid telegram secret token")

    # 解析白名单与超管
    try:
        allowed_chat_ids = _parse_chat_ids(allowed_raw)
    except Exception:
        allowed_chat_ids = set()
    try:
        redeem_chat_ids = _parse_chat_ids(redeem_raw)
    except Exception:
        redeem_chat_ids = set()
    try:
        super_admin_chat_ids = _parse_chat_ids(super_admin_raw)
    except Exception:
        super_admin_chat_ids = set()
    withdraw_enabled = (str(withdraw_enabled_raw or "true").strip().lower() == "true")

    # callback_query（InlineKeyboard 按钮回调）
    cb_id, cb_chat_id, cb_message_id, cb_data, cb_chat_type = _extract_callback_query(update)
    if cb_id and cb_chat_id is not None:
        if not allowed_chat_ids or cb_chat_id not in allowed_chat_ids:
            # 兜底：即使未授权也要 answerCallbackQuery，避免 Telegram 客户端一直 loading
            if bot_token:
                asyncio.create_task(answer_callback_query(bot_token, cb_id, text="未授权"))
            return {"ok": True}

        # 仅私聊支持敏感操作
        if cb_chat_type != "private":
            if bot_token:
                asyncio.create_task(answer_callback_query(bot_token, cb_id, text="仅支持私聊"))
                asyncio.create_task(
                    send_message(
                        bot_token,
                        cb_chat_id,
                        "🛡️ 为安全起见，该操作仅支持私聊使用。",
                        reply_to_message_id=cb_message_id,
                    )
                )
            return {"ok": True}

        is_superadmin = cb_chat_id in super_admin_chat_ids
        await _handle_callback_query(
            cb_chat_id,
            cb_message_id,
            cb_data,
            callback_query_id=cb_id,
            bot_token=bot_token,
            withdraw_enabled=withdraw_enabled,
            is_superadmin=is_superadmin,
        )
        return {"ok": True}

    chat_id, message_id, text, chat_type, reply_text, entities = _extract_message(update)
    if chat_id is None:
        return {"ok": True}

    # 校验白名单 chat_id
    if not allowed_chat_ids or chat_id not in allowed_chat_ids:
        return {"ok": True}

    text = (text or "").strip()
    if not text:
        return {"ok": True}

    # 优先用 entities 提取命令，兜底再用 regex
    cmd_raw, rest_from_entities = _extract_command_from_entities(text, entities)
    cmd = (cmd_raw.split("@", 1)[0].lower() if cmd_raw else "")
    is_superadmin = chat_id in super_admin_chat_ids
    can_redeem = bool(is_superadmin or (not redeem_chat_ids) or (chat_id in redeem_chat_ids))

    # /start /help
    if cmd in ("/start", "/help") or text.lower().startswith("/start") or text.lower().startswith("/help"):
        if bot_token:
            asyncio.create_task(
                send_message(
                    bot_token,
                    chat_id,
                    _build_help_text(is_superadmin=is_superadmin, can_redeem=can_redeem),
                    reply_to_message_id=message_id,
                )
            )
        return {"ok": True}

    # /status 查看业务状态（只读）
    if cmd == "/status" or _STATUS_CMD_RE.match(text):
        if chat_type != "private":
            if bot_token:
                asyncio.create_task(
                    send_message(
                        bot_token,
                        chat_id,
                        "🛡️ 为安全起见，/status 仅支持超管私聊本 Bot 使用。",
                        reply_to_message_id=message_id,
                    )
                )
            return {"ok": True}
        if not is_superadmin:
            if bot_token:
                asyncio.create_task(
                    send_message(
                        bot_token,
                        chat_id,
                        "⛔ 无权限：/status 仅超级管理员可用。",
                        reply_to_message_id=message_id,
                    )
                )
            return {"ok": True}

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

        logger.info(f"TG audit: action=status, actor_chat_id={chat_id}, full={is_full}")

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
            records_total = None
            records_today = None
            records_this_week = None
            records_this_month = None
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

            # 使用记录披露（口径对齐 /admin/records：total/today/this_week/this_month）
            now = get_now()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            week_start = today_start - timedelta(days=today_start.weekday())
            month_start = today_start.replace(day=1)
            stats_row = (
                await db.execute(
                    select(
                        func.count(RedemptionRecord.id).label("total"),
                        func.sum(case((RedemptionRecord.redeemed_at >= today_start, 1), else_=0)).label("today"),
                        func.sum(case((RedemptionRecord.redeemed_at >= week_start, 1), else_=0)).label("this_week"),
                        func.sum(case((RedemptionRecord.redeemed_at >= month_start, 1), else_=0)).label("this_month"),
                    )
                )
            ).one()
            records_total = int(stats_row.total or 0)
            records_today = int(stats_row.today or 0)
            records_this_week = int(stats_row.this_week or 0)
            records_this_month = int(stats_row.this_month or 0)

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
            records_total=records_total,
            records_today=records_today,
            records_this_week=records_this_week,
            records_this_month=records_this_month,
            full=is_full,
            redeem_24h=redeem_24h,
            redeem_7d=redeem_7d,
            expiring_teams=expiring_teams,
            bot_metrics=({k: dict(v) for k, v in _bot_metrics.items()} if is_full else None),
            rate_limit_alert_counts=(dict(_rate_limit_alert_counts) if is_full else None),
        )

        if bot_token:
            asyncio.create_task(send_message(bot_token, chat_id, status_text, reply_to_message_id=message_id))
        return {"ok": True}

    # /records 邮箱 [数量|all]（仅私聊）
    if cmd == "/records" or _RECORDS_CMD_RE.match(text):
        _metric_inc("records", "calls")
        _metric_touch("records")

        if chat_type != "private":
            if bot_token:
                asyncio.create_task(
                    send_message(bot_token, chat_id, "🛡️ 为安全起见，查询记录仅支持私聊本 Bot 使用。", reply_to_message_id=message_id)
                )
            return {"ok": True}

        limited = _rate_limited(chat_id, _rate_limit_records, _RECORDS_RATE_LIMIT_SECONDS) or _minute_limited(
            chat_id, _records_minute_bucket, _RECORDS_PER_MINUTE_LIMIT
        )
        if limited:
            _metric_inc("records", "rate_limited")
            _metric_touch("records")
            if bot_token:
                asyncio.create_task(send_message(bot_token, chat_id, _tg_text("rate_limited"), reply_to_message_id=message_id))
                if _track_rate_limit_hit(chat_id, "records"):
                    asyncio.create_task(
                        send_message(
                            bot_token,
                            chat_id,
                            "⚠️ 检测到频繁触发限流（/records）。请稍后再试，或减少重复操作。",
                            reply_to_message_id=message_id,
                        )
                    )
            return {"ok": True}

        rest = rest_from_entities
        if not rest:
            m = _RECORDS_CMD_RE.match(text)
            rest = (m.group(1) or "").strip() if m else ""
        rest = (rest or "").strip()

        email_match = _EMAIL_RE.search(rest or "")
        if not email_match:
            if bot_token:
                asyncio.create_task(
                    send_message(
                        bot_token,
                        chat_id,
                        _tg_text("invalid_email", help=_build_help_text(is_superadmin=is_superadmin, can_redeem=can_redeem)),
                        reply_to_message_id=message_id,
                    )
                )
            return {"ok": True}

        email = email_match.group(0)
        tokens = [t for t in (rest.split() if rest else []) if t]
        is_all = bool(is_superadmin and any(t.lower() == "all" for t in tokens))
        limit = 5
        if not is_all:
            for t in reversed(tokens):
                if t.isdigit():
                    try:
                        limit = int(t)
                    except Exception:
                        limit = 5
                    break
        if limit < 1:
            limit = 1
        if limit > 20:
            limit = 20

        asyncio.create_task(_process_records(chat_id, message_id, email, limit=limit, is_all=is_all, is_superadmin=is_superadmin))
        return {"ok": True}

    # /withdraw 邮箱|record_id（仅私聊，需确认）
    if cmd == "/withdraw" or _WITHDRAW_CMD_RE.match(text):
        _metric_inc("withdraw", "calls")
        _metric_touch("withdraw")

        if chat_type != "private":
            if bot_token:
                asyncio.create_task(
                    send_message(bot_token, chat_id, "🛡️ 为安全起见，撤销仅支持私聊本 Bot 使用。", reply_to_message_id=message_id)
                )
            return {"ok": True}

        if not withdraw_enabled:
            if bot_token:
                asyncio.create_task(
                    send_message(bot_token, chat_id, "🚫 撤销功能已禁用（后台可开启 tg_withdraw_enabled）。", reply_to_message_id=message_id)
                )
            return {"ok": True}

        limited = _rate_limited(chat_id, _rate_limit_withdraw, _WITHDRAW_RATE_LIMIT_SECONDS) or _minute_limited(
            chat_id, _withdraw_minute_bucket, _WITHDRAW_PER_MINUTE_LIMIT
        )
        if limited:
            _metric_inc("withdraw", "rate_limited")
            _metric_touch("withdraw")
            if bot_token:
                asyncio.create_task(send_message(bot_token, chat_id, _tg_text("rate_limited"), reply_to_message_id=message_id))
                if _track_rate_limit_hit(chat_id, "withdraw"):
                    asyncio.create_task(
                        send_message(
                            bot_token,
                            chat_id,
                            "⚠️ 检测到频繁触发限流（/withdraw）。请稍后再试，或减少重复操作。",
                            reply_to_message_id=message_id,
                        )
                    )
            return {"ok": True}

        rest = rest_from_entities
        if not rest:
            m = _WITHDRAW_CMD_RE.match(text)
            rest = (m.group(1) or "").strip() if m else ""
        rest = (rest or "").strip()
        if not rest:
            if bot_token:
                asyncio.create_task(send_message(bot_token, chat_id, "🧹 用法：/withdraw 邮箱 或 /withdraw 记录ID", reply_to_message_id=message_id))
            return {"ok": True}

        asyncio.create_task(_process_withdraw_command(chat_id, message_id, rest, is_superadmin=is_superadmin))
        return {"ok": True}

    # /redeem 邮箱（支持 entities + regex 兜底）
    if cmd == "/redeem" or _REDEEM_CMD_RE.match(text):
        if chat_type != "private":
            if bot_token:
                asyncio.create_task(
                    send_message(
                        bot_token,
                        chat_id,
                        "🛡️ 为安全起见，自动上车仅支持私聊本 Bot 使用。",
                        reply_to_message_id=message_id,
                    )
                )
            return {"ok": True}
        if not can_redeem:
            if bot_token:
                asyncio.create_task(
                    send_message(
                        bot_token,
                        chat_id,
                        "🚫 当前 Chat ID 未被授权使用 /redeem。\n请联系管理员将你的 Chat ID 加入“/redeem Chat ID（可用人员）”。",
                        reply_to_message_id=message_id,
                    )
                )
            return {"ok": True}

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
                        _tg_text("invalid_email", help=_build_help_text(is_superadmin=is_superadmin, can_redeem=can_redeem)),
                        reply_to_message_id=message_id,
                    )
                )
            return {"ok": True}

        email = email_match.group(0)
        logger.info(f"TG audit: action=redeem_request, actor_chat_id={chat_id}, email={email}")

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
        if not is_superadmin:
            if bot_token:
                asyncio.create_task(
                    send_message(
                        bot_token,
                        chat_id,
                        "⛔ 无权限：补账号导入仅超级管理员可用。",
                        reply_to_message_id=message_id,
                    )
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

        logger.info(f"TG audit: action=importteam_request, actor_chat_id={chat_id}, token_len={len(access_token)}")

        if bot_token:
            asyncio.create_task(
                send_message(bot_token, chat_id, _tg_text("import_received"), reply_to_message_id=message_id)
            )
        asyncio.create_task(_process_import(chat_id, message_id, access_token))
        return {"ok": True}

    # 默认不响应非命令，避免群里噪音
    return {"ok": True}
