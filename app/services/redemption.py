"""
兑换码管理服务
用于管理兑换码的生成、验证、使用和查询
"""
import logging
import secrets
import string
from typing import Optional, Dict, Any, List, AsyncIterator
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, and_, or_, func, case
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models import RedemptionCode, RedemptionRecord, Team
from app.utils.time_utils import get_now

logger = logging.getLogger(__name__)
_WARNED_UNKNOWN_RECORD_SOURCES: set[str] = set()


class RedemptionService:
    """兑换码管理服务类"""

    def __init__(self):
        """初始化兑换码管理服务"""
        pass

    def _normalize_record_source(self, source: Optional[str]) -> Optional[str]:
        s = (source or "").strip().lower()
        if not s:
            return None
        return s if s in {"user", "admin", "tg"} else None

    def _safe_int(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        try:
            s = str(value).strip()
            if not s:
                return None
            return int(s)
        except Exception:
            return None

    def _parse_yyyy_mm_dd(self, value: Optional[str]) -> Optional[datetime]:
        s = (value or "").strip()
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except Exception:
            return None

    def _clamp_per_page(self, per_page: Any, *, default: int = 20, min_value: int = 1, max_value: int = 200) -> int:
        n = self._safe_int(per_page)
        if n is None:
            return default
        if n < min_value:
            return min_value
        if n > max_value:
            return max_value
        return n

    def _generate_random_code(self, length: int = 16) -> str:
        """
        生成随机兑换码

        Args:
            length: 兑换码长度

        Returns:
            随机兑换码字符串
        """
        # 使用大写字母和数字,排除容易混淆的字符 (0, O, I, 1)
        alphabet = string.ascii_uppercase + string.digits
        alphabet = alphabet.replace('0', '').replace('O', '').replace('I', '').replace('1', '')

        # 生成随机码
        code = ''.join(secrets.choice(alphabet) for _ in range(length))

        # 格式化为 XXXX-XXXX-XXXX-XXXX
        if length == 16:
            code = f"{code[0:4]}-{code[4:8]}-{code[8:12]}-{code[12:16]}"

        return code

    async def get_records_page(
        self,
        db_session: AsyncSession,
        *,
        email: Optional[str] = None,
        code: Optional[str] = None,
        team_id: Optional[int] = None,
        source: Optional[str] = None,
        tg_chat_id: Optional[Any] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page: Any = 1,
        per_page: Any = 20,
    ) -> Dict[str, Any]:
        """
        获取使用记录（分页 + DB 过滤 + 统计 + 来源分布）
        """
        try:
            email_norm = (email or "").strip() or None
            code_norm = (code or "").strip() or None
            team_id_int = self._safe_int(team_id)
            page_int = self._safe_int(page) or 1
            per_page_int = self._clamp_per_page(per_page, default=20, max_value=200)
            source_norm = self._normalize_record_source(source)
            tg_chat_id_int = self._safe_int(tg_chat_id)
            if source_norm != "tg":
                tg_chat_id_int = None

            start_dt = self._parse_yyyy_mm_dd(start_date)
            end_dt = self._parse_yyyy_mm_dd(end_date)
            end_dt_exclusive = (end_dt + timedelta(days=1)) if end_dt else None

            common_filters = []
            if email_norm:
                common_filters.append(RedemptionRecord.email.ilike(f"%{email_norm}%"))
            if code_norm:
                common_filters.append(RedemptionRecord.code.ilike(f"%{code_norm}%"))
            if team_id_int is not None:
                common_filters.append(RedemptionRecord.team_id == team_id_int)
            if start_dt:
                common_filters.append(RedemptionRecord.redeemed_at >= start_dt)
            if end_dt_exclusive:
                common_filters.append(RedemptionRecord.redeemed_at < end_dt_exclusive)

            source_filters = []
            if source_norm:
                if source_norm == "user":
                    source_filters.append(or_(RedemptionRecord.source.is_(None), RedemptionRecord.source == "user"))
                else:
                    source_filters.append(RedemptionRecord.source == source_norm)

                if source_norm == "tg" and tg_chat_id_int is not None:
                    source_filters.append(RedemptionRecord.tg_chat_id == tg_chat_id_int)

            all_filters = [*common_filters, *source_filters]

            # 统计：total/today/this_week/this_month（同当前筛选条件）
            now = get_now()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            week_start = today_start - timedelta(days=today_start.weekday())
            month_start = today_start.replace(day=1)

            stats_stmt = select(
                func.count(RedemptionRecord.id).label("total"),
                func.sum(case((RedemptionRecord.redeemed_at >= today_start, 1), else_=0)).label("today"),
                func.sum(case((RedemptionRecord.redeemed_at >= week_start, 1), else_=0)).label("this_week"),
                func.sum(case((RedemptionRecord.redeemed_at >= month_start, 1), else_=0)).label("this_month"),
            )
            if all_filters:
                stats_stmt = stats_stmt.where(and_(*all_filters))
            stats_row = (await db_session.execute(stats_stmt)).one()
            total = int(stats_row.total or 0)

            stats = {
                "total": total,
                "today": int(stats_row.today or 0),
                "this_week": int(stats_row.this_week or 0),
                "this_month": int(stats_row.this_month or 0),
            }

            # 来源分布：应用通用过滤，但不应用 source/tg_chat_id 过滤
            source_group = func.coalesce(RedemptionRecord.source, "user")
            source_stmt = select(source_group.label("source"), func.count(RedemptionRecord.id).label("count")).group_by(source_group)
            if common_filters:
                source_stmt = source_stmt.where(and_(*common_filters))
            source_rows = (await db_session.execute(source_stmt)).all()
            source_counts: Dict[str, int] = {"user": 0, "admin": 0, "tg": 0}
            for s, c in source_rows:
                s_norm = str(s or "").strip().lower() or "user"
                # 保持与列表展示一致：未知来源按 user 统计，避免 badges 与记录数不一致
                if s_norm not in {"user", "admin", "tg"}:
                    s_norm = "user"
                source_counts[s_norm] += int(c or 0)

            # 分页
            import math
            total_pages = math.ceil(total / per_page_int) if total > 0 else 1
            if page_int < 1:
                page_int = 1
            if total_pages > 0 and page_int > total_pages:
                page_int = total_pages
            offset = (page_int - 1) * per_page_int

            # 查询数据（join Team 获取 team_name）
            stmt = (
                select(RedemptionRecord, Team.team_name)
                .join(Team, Team.id == RedemptionRecord.team_id, isouter=True)
                .order_by(RedemptionRecord.redeemed_at.desc(), RedemptionRecord.id.desc())
            )
            if all_filters:
                stmt = stmt.where(and_(*all_filters))
            stmt = stmt.limit(per_page_int).offset(offset)
            rows = (await db_session.execute(stmt)).all()

            record_list = []
            for record, team_name in rows:
                raw_source = (record.source or "").strip()
                source_val = raw_source.lower() or "user"
                if source_val not in {"user", "admin", "tg"}:
                    if raw_source and raw_source not in _WARNED_UNKNOWN_RECORD_SOURCES:
                        _WARNED_UNKNOWN_RECORD_SOURCES.add(raw_source)
                        logger.warning(f"发现未知 RedemptionRecord.source 值，已按 user 处理: {raw_source!r}")
                    source_val = "user"

                record_list.append(
                    {
                        "id": record.id,
                        "email": record.email,
                        "code": record.code,
                        "team_id": record.team_id,
                        "team_name": team_name,
                        "account_id": record.account_id,
                        "redeemed_at": record.redeemed_at.strftime("%Y-%m-%d %H:%M:%S") if record.redeemed_at else None,
                        "is_warranty_redemption": bool(record.is_warranty_redemption),
                        "source": source_val,
                        "tg_chat_id": record.tg_chat_id,
                    }
                )

            return {
                "success": True,
                "records": record_list,
                "stats": stats,
                "source_counts": source_counts,
                "pagination": {
                    "current_page": page_int,
                    "total_pages": total_pages,
                    "total": total,
                    "per_page": per_page_int,
                },
                "error": None,
            }

        except Exception as e:
            logger.error(f"获取使用记录分页失败: {e}")
            return {
                "success": False,
                "records": [],
                "stats": {"total": 0, "today": 0, "this_week": 0, "this_month": 0},
                "source_counts": {"user": 0, "admin": 0, "tg": 0},
                "pagination": {"current_page": 1, "total_pages": 1, "total": 0, "per_page": 20},
                "error": f"获取使用记录失败: {str(e)}",
            }

    async def iter_records_for_export(
        self,
        db_session: AsyncSession,
        *,
        email: Optional[str] = None,
        code: Optional[str] = None,
        team_id: Optional[int] = None,
        source: Optional[str] = None,
        tg_chat_id: Optional[Any] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[Dict[str, Any]]:
        email_norm = (email or "").strip() or None
        code_norm = (code or "").strip() or None
        team_id_int = self._safe_int(team_id)
        source_norm = self._normalize_record_source(source)
        tg_chat_id_int = self._safe_int(tg_chat_id)
        if source_norm != "tg":
            tg_chat_id_int = None

        start_dt = self._parse_yyyy_mm_dd(start_date)
        end_dt = self._parse_yyyy_mm_dd(end_date)
        end_dt_exclusive = (end_dt + timedelta(days=1)) if end_dt else None

        filters = []
        if email_norm:
            filters.append(RedemptionRecord.email.ilike(f"%{email_norm}%"))
        if code_norm:
            filters.append(RedemptionRecord.code.ilike(f"%{code_norm}%"))
        if team_id_int is not None:
            filters.append(RedemptionRecord.team_id == team_id_int)
        if start_dt:
            filters.append(RedemptionRecord.redeemed_at >= start_dt)
        if end_dt_exclusive:
            filters.append(RedemptionRecord.redeemed_at < end_dt_exclusive)

        if source_norm:
            if source_norm == "user":
                filters.append(or_(RedemptionRecord.source.is_(None), RedemptionRecord.source == "user"))
            else:
                filters.append(RedemptionRecord.source == source_norm)
            if source_norm == "tg" and tg_chat_id_int is not None:
                filters.append(RedemptionRecord.tg_chat_id == tg_chat_id_int)

        last_redeemed_at: Optional[datetime] = None
        last_id: Optional[int] = None

        while True:
            stmt = (
                select(RedemptionRecord, Team.team_name)
                .join(Team, Team.id == RedemptionRecord.team_id, isouter=True)
                .order_by(RedemptionRecord.redeemed_at.desc(), RedemptionRecord.id.desc())
                .limit(chunk_size)
            )
            where_filters = list(filters)
            if last_id is not None:
                # redeemed_at 允许为空（历史/异常数据）。若最后一条 redeemed_at 为 NULL，必须退化为按 id 分页，
                # 否则会重复拉取同一批数据导致导出死循环。
                if last_redeemed_at is None:
                    where_filters.append(
                        and_(RedemptionRecord.redeemed_at.is_(None), RedemptionRecord.id < last_id)
                    )
                else:
                    # redeemed_at 降序（NULL 通常排在最后），先走常规 keyset，再把 NULL 行纳入后续分页
                    where_filters.append(
                        or_(
                            RedemptionRecord.redeemed_at < last_redeemed_at,
                            and_(RedemptionRecord.redeemed_at == last_redeemed_at, RedemptionRecord.id < last_id),
                            RedemptionRecord.redeemed_at.is_(None),
                        )
                    )
            if where_filters:
                stmt = stmt.where(and_(*where_filters))

            rows = (await db_session.execute(stmt)).all()
            if not rows:
                break

            for record, team_name in rows:
                source_val = (record.source or "user").strip().lower()
                if source_val not in {"user", "admin", "tg"}:
                    source_val = "user"

                yield {
                    "id": record.id,
                    "email": record.email,
                    "code": record.code,
                    "team_id": record.team_id,
                    "team_name": team_name,
                    "account_id": record.account_id,
                    "redeemed_at": record.redeemed_at.isoformat() if record.redeemed_at else None,
                    "is_warranty_redemption": bool(record.is_warranty_redemption),
                    "source": source_val,
                    "tg_chat_id": record.tg_chat_id,
                }

            last_record, _ = rows[-1]
            last_redeemed_at = last_record.redeemed_at
            last_id = last_record.id

    async def generate_code_single(
        self,
        db_session: AsyncSession,
        code: Optional[str] = None,
        expires_days: Optional[int] = None,
        has_warranty: bool = False,
        warranty_days: int = 30
    ) -> Dict[str, Any]:
        """
        生成单个兑换码

        Args:
            db_session: 数据库会话
            code: 自定义兑换码 (可选,如果不提供则自动生成)
            expires_days: 有效期天数 (可选,如果不提供则永久有效)
            has_warranty: 是否为质保兑换码 (默认 False)

        Returns:
            结果字典,包含 success, code, message, error
        """
        try:
            # 1. 生成或使用自定义兑换码
            if not code:
                # 生成随机码,确保唯一性
                max_attempts = 10
                for _ in range(max_attempts):
                    code = self._generate_random_code()

                    # 检查是否已存在
                    stmt = select(RedemptionCode).where(RedemptionCode.code == code)
                    result = await db_session.execute(stmt)
                    existing = result.scalar_one_or_none()

                    if not existing:
                        break
                else:
                    return {
                        "success": False,
                        "code": None,
                        "message": None,
                        "error": "生成唯一兑换码失败,请重试"
                    }
            else:
                # 检查自定义兑换码是否已存在
                stmt = select(RedemptionCode).where(RedemptionCode.code == code)
                result = await db_session.execute(stmt)
                existing = result.scalar_one_or_none()

                if existing:
                    return {
                        "success": False,
                        "code": None,
                        "message": None,
                        "error": f"兑换码 {code} 已存在"
                    }

            # 2. 计算过期时间
            expires_at = None
            if expires_days:
                expires_at = get_now() + timedelta(days=expires_days)

            # 3. 创建兑换码记录
            redemption_code = RedemptionCode(
                code=code,
                status="unused",
                expires_at=expires_at,
                has_warranty=has_warranty,
                warranty_days=warranty_days
            )

            db_session.add(redemption_code)
            await db_session.commit()

            logger.info(f"生成兑换码成功: {code}")

            return {
                "success": True,
                "code": code,
                "message": f"兑换码生成成功: {code}",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"生成兑换码失败: {e}")
            return {
                "success": False,
                "code": None,
                "message": None,
                "error": f"生成兑换码失败: {str(e)}"
            }

    async def generate_code_batch(
        self,
        db_session: AsyncSession,
        count: int,
        expires_days: Optional[int] = None,
        has_warranty: bool = False,
        warranty_days: int = 30
    ) -> Dict[str, Any]:
        """
        批量生成兑换码

        Args:
            db_session: 数据库会话
            count: 生成数量
            expires_days: 有效期天数 (可选)
            has_warranty: 是否为质保兑换码 (默认 False)

        Returns:
            结果字典,包含 success, codes, total, message, error
        """
        try:
            if count <= 0 or count > 1000:
                return {
                    "success": False,
                    "codes": [],
                    "total": 0,
                    "message": None,
                    "error": "生成数量必须在 1-1000 之间"
                }

            # 计算过期时间
            expires_at = None
            if expires_days:
                expires_at = get_now() + timedelta(days=expires_days)

            # 批量生成兑换码
            codes = []
            for i in range(count):
                # 生成唯一兑换码
                max_attempts = 10
                for _ in range(max_attempts):
                    code = self._generate_random_code()

                    # 检查是否已存在 (包括本次批量生成的)
                    if code not in codes:
                        stmt = select(RedemptionCode).where(RedemptionCode.code == code)
                        result = await db_session.execute(stmt)
                        existing = result.scalar_one_or_none()

                        if not existing:
                            codes.append(code)
                            break
                else:
                    logger.warning(f"生成第 {i+1} 个兑换码失败")
                    continue

            # 批量插入数据库
            for code in codes:
                redemption_code = RedemptionCode(
                    code=code,
                    status="unused",
                    expires_at=expires_at,
                    has_warranty=has_warranty,
                    warranty_days=warranty_days
                )
                db_session.add(redemption_code)

            await db_session.commit()

            logger.info(f"批量生成兑换码成功: {len(codes)} 个")

            return {
                "success": True,
                "codes": codes,
                "total": len(codes),
                "message": f"成功生成 {len(codes)} 个兑换码",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"批量生成兑换码失败: {e}")
            return {
                "success": False,
                "codes": [],
                "total": 0,
                "message": None,
                "error": f"批量生成兑换码失败: {str(e)}"
            }

    async def validate_code(
        self,
        code: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        验证兑换码

        Args:
            code: 兑换码
            db_session: 数据库会话

        Returns:
            结果字典,包含 success, valid, reason, redemption_code, error
        """
        try:
            # 1. 查询兑换码
            stmt = select(RedemptionCode).where(RedemptionCode.code == code)
            result = await db_session.execute(stmt)
            redemption_code = result.scalar_one_or_none()

            if not redemption_code:
                return {
                    "success": True,
                    "valid": False,
                    "reason": "兑换码不存在",
                    "redemption_code": None,
                    "error": None
                }

            # 2. 检查状态
            allowed_statuses = ["unused", "warranty_active"]
            if redemption_code.has_warranty:
                allowed_statuses.append("used")

            if redemption_code.status not in allowed_statuses:
                status_text = "已过期" if redemption_code.status == "expired" else redemption_code.status
                reason = "兑换码已被使用" if redemption_code.status == "used" else f"兑换码{status_text}"
                return {
                    "success": True,
                    "valid": False,
                    "reason": reason,
                    "redemption_code": None,
                    "error": None
                }

            # 3. 检查是否过期 (仅针对未使用的兑换码执行首次激活截止时间检查)
            if redemption_code.status == "unused" and redemption_code.expires_at:
                if redemption_code.expires_at < get_now():
                    # 更新状态为 expired
                    redemption_code.status = "expired"
                    # 不在服务层内部 commit，让调用方决定事务边界
                    # await db_session.commit() 

                    return {
                        "success": True,
                        "valid": False,
                        "reason": "兑换码已过期 (超过首次兑换截止时间)",
                        "redemption_code": None,
                        "error": None
                    }

            # 4. 验证通过
            return {
                "success": True,
                "valid": True,
                "reason": "兑换码有效",
                "redemption_code": {
                    "id": redemption_code.id,
                    "code": redemption_code.code,
                    "status": redemption_code.status,
                    "expires_at": redemption_code.expires_at.isoformat() if redemption_code.expires_at else None,
                    "created_at": redemption_code.created_at.isoformat() if redemption_code.created_at else None
                },
                "error": None
            }

        except Exception as e:
            logger.error(f"验证兑换码失败: {e}")
            return {
                "success": False,
                "valid": False,
                "reason": None,
                "redemption_code": None,
                "error": f"验证兑换码失败: {str(e)}"
            }

    async def use_code(
        self,
        code: str,
        email: str,
        team_id: int,
        account_id: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        使用兑换码

        Args:
            code: 兑换码
            email: 使用者邮箱
            team_id: Team ID
            account_id: Account ID
            db_session: 数据库会话

        Returns:
            结果字典,包含 success, message, error
        """
        try:
            # 1. 验证兑换码
            validate_result = await self.validate_code(code, db_session)

            if not validate_result["success"]:
                return {
                    "success": False,
                    "message": None,
                    "error": validate_result["error"]
                }

            if not validate_result["valid"]:
                return {
                    "success": False,
                    "message": None,
                    "error": validate_result["reason"]
                }

            # 2. 更新兑换码状态
            stmt = select(RedemptionCode).where(RedemptionCode.code == code)
            result = await db_session.execute(stmt)
            redemption_code = result.scalar_one_or_none()

            redemption_code.status = "used"
            redemption_code.used_by_email = email
            redemption_code.used_team_id = team_id
            redemption_code.used_at = get_now()

            # 3. 创建使用记录
            redemption_record = RedemptionRecord(
                email=email,
                code=code,
                team_id=team_id,
                account_id=account_id
            )

            db_session.add(redemption_record)
            await db_session.commit()

            logger.info(f"使用兑换码成功: {code} -> {email}")

            return {
                "success": True,
                "message": "兑换码使用成功",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"使用兑换码失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"使用兑换码失败: {str(e)}"
            }

    async def get_all_codes(
        self,
        db_session: AsyncSession,
        page: int = 1,
        per_page: int = 50,
        search: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取所有兑换码

        Args:
            db_session: 数据库会话
            page: 页码
            per_page: 每页数量
            search: 搜索关键词 (兑换码或邮箱)

        Returns:
            结果字典,包含 success, codes, total, total_pages, current_page, error
        """
        try:
            # 1. 构建基础查询
            count_stmt = select(func.count(RedemptionCode.id))
            stmt = select(RedemptionCode).order_by(RedemptionCode.created_at.desc())

            # 2. 如果提供了搜索关键词,添加过滤条件
            if search:
                search_filter = or_(
                    RedemptionCode.code.ilike(f"%{search}%"),
                    RedemptionCode.used_by_email.ilike(f"%{search}%")
                )
                count_stmt = count_stmt.where(search_filter)
                stmt = stmt.where(search_filter)

            # 3. 获取总数
            count_result = await db_session.execute(count_stmt)
            total = count_result.scalar() or 0

            # 4. 计算分页
            import math
            total_pages = math.ceil(total / per_page) if total > 0 else 1
            if page < 1:
                page = 1
            if page > total_pages and total_pages > 0:
                page = total_pages
            
            offset = (page - 1) * per_page

            # 5. 查询分页数据
            stmt = stmt.limit(per_page).offset(offset)
            result = await db_session.execute(stmt)
            codes = result.scalars().all()

            # 构建返回数据
            code_list = []
            for code in codes:
                code_list.append({
                    "id": code.id,
                    "code": code.code,
                    "status": code.status,
                    "created_at": code.created_at.isoformat() if code.created_at else None,
                    "expires_at": code.expires_at.isoformat() if code.expires_at else None,
                    "used_by_email": code.used_by_email,
                    "used_team_id": code.used_team_id,
                    "used_at": code.used_at.isoformat() if code.used_at else None,
                    "has_warranty": code.has_warranty,
                    "warranty_days": code.warranty_days,
                    "warranty_expires_at": code.warranty_expires_at.isoformat() if code.warranty_expires_at else None
                })

            logger.info(f"获取所有兑换码成功: 第 {page} 页, 共 {len(code_list)} 个 / 总数 {total}")

            return {
                "success": True,
                "codes": code_list,
                "total": total,
                "total_pages": total_pages,
                "current_page": page,
                "error": None
            }

        except Exception as e:
            logger.error(f"获取所有兑换码失败: {e}")
            return {
                "success": False,
                "codes": [],
                "total": 0,
                "error": f"获取所有兑换码失败: {str(e)}"
            }

    async def get_unused_count(
        self,
        db_session: AsyncSession
    ) -> int:
        """
        获取未使用的兑换码数量
        """
        try:
            stmt = select(func.count(RedemptionCode.id)).where(RedemptionCode.status == "unused")
            result = await db_session.execute(stmt)
            return result.scalar() or 0
        except Exception as e:
            logger.error(f"获取未使用兑换码数量失败: {e}")
            return 0

    async def get_code_by_code(
        self,
        code: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        根据兑换码查询

        Args:
            code: 兑换码
            db_session: 数据库会话

        Returns:
            结果字典,包含 success, code_info, error
        """
        try:
            stmt = select(RedemptionCode).where(RedemptionCode.code == code)
            result = await db_session.execute(stmt)
            redemption_code = result.scalar_one_or_none()

            if not redemption_code:
                return {
                    "success": False,
                    "code_info": None,
                    "error": f"兑换码 {code} 不存在"
                }

            code_info = {
                "id": redemption_code.id,
                "code": redemption_code.code,
                "status": redemption_code.status,
                "created_at": redemption_code.created_at.isoformat() if redemption_code.created_at else None,
                "expires_at": redemption_code.expires_at.isoformat() if redemption_code.expires_at else None,
                "used_by_email": redemption_code.used_by_email,
                "used_team_id": redemption_code.used_team_id,
                "used_at": redemption_code.used_at.isoformat() if redemption_code.used_at else None
            }

            return {
                "success": True,
                "code_info": code_info,
                "error": None
            }

        except Exception as e:
            logger.error(f"查询兑换码失败: {e}")
            return {
                "success": False,
                "code_info": None,
                "error": f"查询兑换码失败: {str(e)}"
            }

    async def get_unused_codes(
        self,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        获取未使用的兑换码

        Args:
            db_session: 数据库会话

        Returns:
            结果字典,包含 success, codes, total, error
        """
        try:
            stmt = select(RedemptionCode).where(
                RedemptionCode.status == "unused"
            ).order_by(RedemptionCode.created_at.desc())

            result = await db_session.execute(stmt)
            codes = result.scalars().all()

            # 构建返回数据
            code_list = []
            for code in codes:
                code_list.append({
                    "id": code.id,
                    "code": code.code,
                    "status": code.status,
                    "created_at": code.created_at.isoformat() if code.created_at else None,
                    "expires_at": code.expires_at.isoformat() if code.expires_at else None
                })

            return {
                "success": True,
                "codes": code_list,
                "total": len(code_list),
                "error": None
            }

        except Exception as e:
            logger.error(f"获取未使用兑换码失败: {e}")
            return {
                "success": False,
                "codes": [],
                "total": 0,
                "error": f"获取未使用兑换码失败: {str(e)}"
            }

    async def get_all_records(
        self,
        db_session: AsyncSession,
        email: Optional[str] = None,
        code: Optional[str] = None,
        team_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        获取所有兑换记录 (支持筛选)

        Args:
            db_session: 数据库会话
            email: 邮箱模糊搜索
            code: 兑换码模糊搜索
            team_id: Team ID 筛选

        Returns:
            结果字典,包含 success, records, total, error
        """
        try:
            stmt = select(RedemptionRecord)
            
            # 添加筛选条件
            filters = []
            if email:
                filters.append(RedemptionRecord.email.ilike(f"%{email}%"))
            if code:
                filters.append(RedemptionRecord.code.ilike(f"%{code}%"))
            if team_id:
                filters.append(RedemptionRecord.team_id == team_id)
                
            if filters:
                stmt = stmt.where(and_(*filters))
                
            stmt = stmt.order_by(RedemptionRecord.redeemed_at.desc())
            
            result = await db_session.execute(stmt)
            records = result.scalars().all()

            # 构建返回数据
            record_list = []
            for record in records:
                record_list.append({
                    "id": record.id,
                    "email": record.email,
                    "code": record.code,
                    "team_id": record.team_id,
                    "account_id": record.account_id,
                    "redeemed_at": record.redeemed_at.isoformat() if record.redeemed_at else None
                })

            logger.info(f"获取所有兑换记录成功: 共 {len(record_list)} 条")

            return {
                "success": True,
                "records": record_list,
                "total": len(record_list),
                "error": None
            }

        except Exception as e:
            logger.error(f"获取所有兑换记录失败: {e}")
            return {
                "success": False,
                "records": [],
                "total": 0,
                "error": f"获取所有兑换记录失败: {str(e)}"
            }

    async def delete_code(
        self,
        code: str,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        删除兑换码

        Args:
            code: 兑换码
            db_session: 数据库会话

        Returns:
            结果字典,包含 success, message, error
        """
        try:
            # 查询兑换码
            stmt = select(RedemptionCode).where(RedemptionCode.code == code)
            result = await db_session.execute(stmt)
            redemption_code = result.scalar_one_or_none()

            if not redemption_code:
                return {
                    "success": False,
                    "message": None,
                    "error": f"兑换码 {code} 不存在"
                }

            # 删除兑换码
            await db_session.delete(redemption_code)
            await db_session.commit()

            logger.info(f"删除兑换码成功: {code}")

            return {
                "success": True,
                "message": f"兑换码 {code} 已删除",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"删除兑换码失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"删除兑换码失败: {str(e)}"
            }

    async def update_code(
        self,
        code: str,
        db_session: AsyncSession,
        has_warranty: Optional[bool] = None,
        warranty_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """更新兑换码信息"""
        return await self.bulk_update_codes([code], db_session, has_warranty, warranty_days)

    async def withdraw_record(
        self,
        record_id: int,
        db_session: AsyncSession
    ) -> Dict[str, Any]:
        """
        撤回使用记录 (删除记录,恢复兑换码,并在 Team 中移除成员/邀请)

        Args:
            record_id: 记录 ID
            db_session: 数据库会话

        Returns:
            结果字典
        """
        try:
            from app.services.team import team_service
            
            # 1. 查询记录
            stmt = select(RedemptionRecord).where(RedemptionRecord.id == record_id).options(
                selectinload(RedemptionRecord.redemption_code)
            )
            result = await db_session.execute(stmt)
            record = result.scalar_one_or_none()

            if not record:
                return {"success": False, "error": f"记录 ID {record_id} 不存在"}

            # 2. 调用 TeamService 移除成员/邀请
            logger.info(f"正在从 Team {record.team_id} 中移除成员 {record.email}")
            team_result = await team_service.remove_invite_or_member(
                record.team_id,
                record.email,
                db_session
            )

            if not team_result["success"]:
                # 即使 Team 移除失败，如果是因为成员已经不在了，我们也继续处理数据库
                if "成员已不存在" not in str(team_result.get("message", "")) and "用户不存在" not in str(team_result.get("error", "")):
                    return {
                        "success": False, 
                        "error": f"从 Team 移除成员失败: {team_result.get('error') or team_result.get('message')}"
                    }

            # 3. 恢复兑换码状态
            code = record.redemption_code
            if code:
                # 如果是质保兑换，且还有其他记录，状态可能不应该直接回 unused
                # 但根据逻辑，目前一个码一个记录（除了质保补发可能产生新记录，但那是两个不同的码吧？）
                # 查了一下模型，RedemptionCode 有 used_by_email 等字段，说明它是单次使用的设计
                code.status = "unused"
                code.used_by_email = None
                code.used_team_id = None
                code.used_at = None
                # 特殊处理质保字段
                if code.has_warranty:
                    code.warranty_expires_at = None

            # 4. 删除使用记录
            await db_session.delete(record)
            await db_session.commit()

            logger.info(f"撤回记录成功: {record_id}, 邮箱: {record.email}, 兑换码: {record.code}")

            return {
                "success": True,
                "message": f"成功撤回记录并恢复兑换码 {record.code}"
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"撤回记录失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "error": f"撤回失败: {str(e)}"}

    async def bulk_update_codes(
        self,
        codes: List[str],
        db_session: AsyncSession,
        has_warranty: Optional[bool] = None,
        warranty_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        批量更新兑换码信息

        Args:
            codes: 兑换码列表
            db_session: 数据库会话
            has_warranty: 是否为质保兑换码 (可选)
            warranty_days: 质保天数 (可选)

        Returns:
            结果字典
        """
        try:
            if not codes:
                return {"success": True, "message": "没有需要更新的兑换码"}

            # 构建更新语句
            values = {}
            if has_warranty is not None:
                values[RedemptionCode.has_warranty] = has_warranty
            if warranty_days is not None:
                values[RedemptionCode.warranty_days] = warranty_days

            if not values:
                return {"success": True, "message": "没有提供更新内容"}

            stmt = update(RedemptionCode).where(RedemptionCode.code.in_(codes)).values(values)
            await db_session.execute(stmt)
            await db_session.commit()

            logger.info(f"成功批量更新 {len(codes)} 个兑换码")

            return {
                "success": True,
                "message": f"成功批量更新 {len(codes)} 个兑换码",
                "error": None
            }

        except Exception as e:
            await db_session.rollback()
            logger.error(f"批量更新兑换码失败: {e}")
            return {
                "success": False,
                "message": None,
                "error": f"批量更新失败: {str(e)}"
            }


# 创建全局兑换码服务实例
redemption_service = RedemptionService()
