"""
自动兑换服务
用于在无需用户输入兑换码的情况下，自动选择可用兑换码并完成兑换上车流程
"""
import logging
from typing import Any, Dict

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import RedemptionCode
from app.services.redemption import RedemptionService
from app.services.redeem_flow import redeem_flow_service
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)

redemption_service = RedemptionService()


def _should_retry_error(error_message: str) -> bool:
    if not error_message:
        return False
    retry_keywords = [
        "兑换码不存在",
        "兑换码已被使用",
        "兑换码已过期",
        "兑换码已被占用",
        "兑换码记录丢失",
    ]
    return any(kw in error_message for kw in retry_keywords)


async def auto_redeem_by_email(
    email: str,
    db_session: AsyncSession,
    *,
    max_attempts: int = 5,
    auto_generate_count: int = 10,
    auto_generate_has_warranty: bool = True,
    auto_generate_warranty_days: int = 30,
) -> Dict[str, Any]:
    """
    自动兑换（只提供邮箱）

    逻辑：
    - 清理过期 unused -> expired
    - 选择一个可用的 unused 兑换码
    - 如果没有可用兑换码，自动生成一批兑换码后重试
    - 调用 redeem_flow_service 完成兑换上车（自动分配 Team）

    Returns:
        结果字典：
        - success: bool
        - message: str | None
        - used_code: str | None
        - generated_codes: int
        - team_info: dict | None
        - error: str | None
    """
    email = (email or "").strip()
    generated_codes = 0
    last_error = "兑换失败"

    for attempt in range(max_attempts):
        try:
            now = get_now()

            # 1) 清理过期但仍是 unused 的兑换码
            try:
                await db_session.execute(
                    update(RedemptionCode)
                    .where(
                        and_(
                            RedemptionCode.status == "unused",
                            RedemptionCode.expires_at.is_not(None),
                            RedemptionCode.expires_at < now,
                        )
                    )
                    .values(status="expired")
                )
                await db_session.commit()
            except Exception as e:
                logger.warning(f"清理过期兑换码失败: {e}")
                await db_session.rollback()

            # 2) 选择一个可用兑换码
            stmt = (
                select(RedemptionCode.code)
                .where(
                    RedemptionCode.status == "unused",
                    or_(
                        RedemptionCode.expires_at.is_(None),
                        RedemptionCode.expires_at > now,
                    ),
                )
                .order_by(RedemptionCode.created_at.asc())
                .limit(1)
            )
            result = await db_session.execute(stmt)
            code = result.scalar_one_or_none()

            # 3) 没有码则自动生成一批（无过期、质保）后重试
            if not code:
                logger.info(
                    f"自动兑换：无可用兑换码，自动生成 {auto_generate_count} 个后重试 (attempt={attempt + 1}/{max_attempts})"
                )
                gen_result = await redemption_service.generate_code_batch(
                    db_session=db_session,
                    count=auto_generate_count,
                    expires_days=None,
                    has_warranty=auto_generate_has_warranty,
                    warranty_days=auto_generate_warranty_days,
                )

                if not gen_result.get("success"):
                    return {
                        "success": False,
                        "message": None,
                        "used_code": None,
                        "generated_codes": generated_codes,
                        "team_info": None,
                        "error": gen_result.get("error") or "自动生成兑换码失败",
                    }

                total_generated = int(gen_result.get("total") or 0)
                if total_generated <= 0:
                    return {
                        "success": False,
                        "message": None,
                        "used_code": None,
                        "generated_codes": generated_codes,
                        "team_info": None,
                        "error": "自动生成兑换码失败：未生成任何兑换码",
                    }

                generated_codes += total_generated
                continue

            # 4) 执行兑换（自动分配 Team）
            logger.info(f"自动兑换: email={email}, code={code} (attempt={attempt + 1}/{max_attempts})")
            redeem_result = await redeem_flow_service.redeem_and_join_team(
                email=email,
                code=code,
                team_id=None,
                db_session=db_session,
            )

            if redeem_result.get("success"):
                return {
                    "success": True,
                    "message": redeem_result.get("message"),
                    "used_code": code,
                    "generated_codes": generated_codes,
                    "team_info": redeem_result.get("team_info"),
                    "error": None,
                }

            last_error = redeem_result.get("error") or "兑换失败"
            if _should_retry_error(last_error) and attempt < max_attempts - 1:
                logger.warning(f"自动兑换失败(可重试): {last_error} (attempt={attempt + 1}/{max_attempts})")
                continue

            return {
                "success": False,
                "message": None,
                "used_code": None,
                "generated_codes": generated_codes,
                "team_info": None,
                "error": last_error,
            }

        except Exception as e:
            await db_session.rollback()
            last_error = str(e)
            logger.error(f"自动兑换异常: {e} (attempt={attempt + 1}/{max_attempts})")
            if attempt < max_attempts - 1:
                continue
            return {
                "success": False,
                "message": None,
                "used_code": None,
                "generated_codes": generated_codes,
                "team_info": None,
                "error": f"自动兑换失败: {last_error}",
            }

    return {
        "success": False,
        "message": None,
        "used_code": None,
        "generated_codes": generated_codes,
        "team_info": None,
        "error": last_error,
    }

