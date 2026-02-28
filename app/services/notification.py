import logging
import httpx
import asyncio
import re
import time
from typing import Optional
from app.services.settings import settings_service
from app.services.redemption import RedemptionService
from app.services.team import team_service
from app.services.telegram import send_message
from app.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

_CHAT_ID_SPLIT_RE = re.compile(r"[, \t\r\n]+")

def _parse_chat_ids(raw: str) -> list[int]:
    raw = (raw or "").strip()
    if not raw:
        return []
    parts = _CHAT_ID_SPLIT_RE.split(raw)
    ids: list[int] = []
    seen: set[int] = set()
    for p in parts:
        p = (p or "").strip()
        if not p:
            continue
        try:
            cid = int(p)
        except Exception:
            continue
        if cid in seen:
            continue
        seen.add(cid)
        ids.append(cid)
    return ids

class NotificationService:
    """通知服务类"""

    def __init__(self):
        self.redemption_service = RedemptionService()
        # TG 库存预警去抖（进程内）：10 分钟内最多发送一次，避免刷屏
        self._tg_low_stock_last_sent_at: Optional[float] = None
        self._tg_low_stock_cooldown_seconds: float = 600.0

    async def _send_tg_low_stock_notification(
        self,
        bot_token: str,
        chat_ids: list[int],
        available_seats: int,
        threshold: int,
    ) -> bool:
        if not bot_token or not chat_ids:
            return False

        text = (
            f"库存不足预警：当前总可用车位 {available_seats}，阈值 {threshold}。\n"
            "请及时补货导入新账号（私聊 Bot 使用 /importteam）。"
        )

        results = await asyncio.gather(
            *(send_message(bot_token, chat_id, text) for chat_id in chat_ids),
            return_exceptions=True,
        )

        any_success = False
        for chat_id, res in zip(chat_ids, results):
            if isinstance(res, Exception):
                logger.warning(f"TG 库存预警发送异常: chat_id={chat_id}, err={res}")
                continue
            if isinstance(res, dict) and res.get("success"):
                any_success = True
            else:
                err = res.get("error") if isinstance(res, dict) else str(res)
                logger.warning(f"TG 库存预警发送失败: chat_id={chat_id}, err={err}")

        return any_success

    async def check_and_notify_low_stock(self) -> bool:
        """
        检查库存（车位）并发送通知
        使用独立的数据库会话以支持异步后台任务
        """
        async with AsyncSessionLocal() as db_session:
            try:
                # 1. 获取配置
                webhook_url = await settings_service.get_setting(db_session, "webhook_url", "")
                threshold_str = await settings_service.get_setting(db_session, "low_stock_threshold", "10")
                api_key = await settings_service.get_setting(db_session, "api_key", "")

                tg_enabled = (await settings_service.get_setting(db_session, "tg_enabled", "false") or "false").lower() == "true"
                tg_bot_token = await settings_service.get_setting(db_session, "tg_bot_token", "")
                tg_allowed_chat_ids_raw = await settings_service.get_setting(db_session, "tg_allowed_chat_ids", "")

                # 若既没有 Webhook URL，也没有 TG 通知配置，则无需检查
                has_tg_notify = bool(tg_enabled and tg_bot_token and tg_allowed_chat_ids_raw.strip())
                if not webhook_url and not has_tg_notify:
                    return False

                try:
                    threshold = int(threshold_str)
                except (ValueError, TypeError):
                    threshold = 10

                # 2. 检查可用车位 (作为预警指标)
                available_seats = await team_service.get_total_available_seats(db_session)
                
                logger.info(f"库存检查 - 当前总可用车位: {available_seats}, 触发阈值: {threshold}")

                # 仅根据可用车位触发补货
                if available_seats <= threshold:
                    logger.info(f"检测到车位不足，触发补货预警! Webhook URL: {webhook_url}")
                    notified_any = False

                    # 2.1 发送 Webhook（如有配置）
                    if webhook_url:
                        webhook_ok = await self.send_webhook_notification(webhook_url, available_seats, threshold, api_key)
                        notified_any = notified_any or webhook_ok

                    # 2.2 TG 通知（如启用且配置完整），带去抖
                    if has_tg_notify:
                        now = time.monotonic()
                        if (
                            self._tg_low_stock_last_sent_at is None
                            or now - self._tg_low_stock_last_sent_at >= self._tg_low_stock_cooldown_seconds
                        ):
                            chat_ids = _parse_chat_ids(tg_allowed_chat_ids_raw)
                            if chat_ids:
                                tg_ok = await self._send_tg_low_stock_notification(
                                    tg_bot_token, chat_ids, available_seats, threshold
                                )
                                # 仅在至少成功发送到一个 chat_id 后，才进入冷却期
                                if tg_ok:
                                    self._tg_low_stock_last_sent_at = now
                                notified_any = notified_any or tg_ok
                        else:
                            logger.info("TG 库存预警处于冷却期，跳过发送")

                    return notified_any
                
                return False

            except Exception as e:
                logger.error(f"检查库存并通知过程发生错误: {e}")
                return False

    async def send_webhook_notification(self, url: str, available_seats: int, threshold: int, api_key: Optional[str] = None) -> bool:
        """
        发送 Webhook 通知
        """
        try:
            payload = {
                "event": "low_stock",
                "current_seats": available_seats,
                "threshold": threshold,
                "message": f"库存不足预警：系统总可用车位仅剩 {available_seats}，已低于预警阈值 {threshold}，请及时补货导入新账号。"
            }
            
            headers = {}
            if api_key:
                headers["X-API-Key"] = api_key
                
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                logger.info(f"Webhook 通知发送成功: {url}")
                return True
        except Exception as e:
            logger.error(f"发送 Webhook 通知失败: {e}")
            return False

# 创建全局实例
notification_service = NotificationService()
