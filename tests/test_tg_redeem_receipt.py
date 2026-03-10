import importlib.util
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch


class _DummyExpression:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return ("eq", self.name, other)

    def __ge__(self, other):
        return ("ge", self.name, other)

    def __gt__(self, other):
        return ("gt", self.name, other)

    def __lt__(self, other):
        return ("lt", self.name, other)

    def is_(self, other):
        return ("is", self.name, other)

    def is_not(self, other):
        return ("is_not", self.name, other)

    def in_(self, other):
        return ("in", self.name, other)

    def notin_(self, other):
        return ("notin", self.name, other)

    def asc(self):
        return ("asc", self.name)

    def desc(self):
        return ("desc", self.name)

    def label(self, value):
        return ("label", self.name, value)


class _DummySelect:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def select_from(self, *args, **kwargs):
        return self

    def where(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self


class _DummyFunc:
    def __getattr__(self, name):
        def _call(*args, **kwargs):
            return _DummyExpression(f"func.{name}")

        return _call


class _StubAPIRouter:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def post(self, *args, **kwargs):
        return lambda func: func

    def get(self, *args, **kwargs):
        return lambda func: func


class _StubHTTPException(Exception):
    def __init__(self, status_code=None, detail=None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class _StubTokenParser:
    JWT_PATTERN = r"eyJ[a-zA-Z0-9._-]+"
    REFRESH_TOKEN_PATTERN = r"rt-[a-zA-Z0-9._-]+"


class _StubRedemptionRecord:
    id = _DummyExpression("redemption_record.id")
    email = _DummyExpression("redemption_record.email")
    code = _DummyExpression("redemption_record.code")
    team_id = _DummyExpression("redemption_record.team_id")
    account_id = _DummyExpression("redemption_record.account_id")
    redeemed_at = _DummyExpression("redemption_record.redeemed_at")
    source = _DummyExpression("redemption_record.source")
    tg_chat_id = _DummyExpression("redemption_record.tg_chat_id")


class _StubRedemptionCode:
    code = _DummyExpression("redemption_code.code")
    status = _DummyExpression("redemption_code.status")
    expires_at = _DummyExpression("redemption_code.expires_at")
    has_warranty = _DummyExpression("redemption_code.has_warranty")


class _StubTeam:
    id = _DummyExpression("team.id")
    team_name = _DummyExpression("team.team_name")
    status = _DummyExpression("team.status")
    expires_at = _DummyExpression("team.expires_at")
    current_members = _DummyExpression("team.current_members")
    max_members = _DummyExpression("team.max_members")


async def _noop_async(*args, **kwargs):
    return None


class _StubRedemptionService:
    pass


def _install_stub_modules():
    app_pkg = types.ModuleType("app")
    app_pkg.__path__ = []
    services_pkg = types.ModuleType("app.services")
    services_pkg.__path__ = []
    utils_pkg = types.ModuleType("app.utils")
    utils_pkg.__path__ = []

    fastapi_module = types.ModuleType("fastapi")
    fastapi_module.APIRouter = _StubAPIRouter
    fastapi_module.HTTPException = _StubHTTPException
    fastapi_module.Request = type("Request", (), {})
    fastapi_module.status = types.SimpleNamespace(HTTP_403_FORBIDDEN=403)

    sqlalchemy_module = types.ModuleType("sqlalchemy")
    sqlalchemy_module.and_ = lambda *args: ("and", args)
    sqlalchemy_module.or_ = lambda *args: ("or", args)
    sqlalchemy_module.case = lambda *args, **kwargs: ("case", args, kwargs)
    sqlalchemy_module.select = lambda *args, **kwargs: _DummySelect(*args, **kwargs)
    sqlalchemy_module.func = _DummyFunc()

    database_module = types.ModuleType("app.database")
    database_module.AsyncSessionLocal = lambda: None

    models_module = types.ModuleType("app.models")
    models_module.RedemptionCode = _StubRedemptionCode
    models_module.RedemptionRecord = _StubRedemptionRecord
    models_module.Team = _StubTeam

    auto_redeem_module = types.ModuleType("app.services.auto_redeem")
    auto_redeem_module.auto_redeem_by_email = _noop_async

    redemption_module = types.ModuleType("app.services.redemption")
    redemption_module.RedemptionService = _StubRedemptionService

    settings_module = types.ModuleType("app.services.settings")
    settings_module.settings_service = types.SimpleNamespace(get_setting=_noop_async)

    team_module = types.ModuleType("app.services.team")
    team_module.team_service = types.SimpleNamespace(
        get_total_available_seats=_noop_async,
        import_team_single=_noop_async,
    )

    telegram_module = types.ModuleType("app.services.telegram")
    telegram_module.answer_callback_query = _noop_async
    telegram_module.send_message = _noop_async

    token_parser_module = types.ModuleType("app.utils.token_parser")
    token_parser_module.TokenParser = _StubTokenParser

    time_utils_module = types.ModuleType("app.utils.time_utils")
    time_utils_module.get_now = lambda: datetime(2026, 3, 10, 12, 0, 0)

    sys.modules.update(
        {
            "app": app_pkg,
            "app.services": services_pkg,
            "app.utils": utils_pkg,
            "fastapi": fastapi_module,
            "sqlalchemy": sqlalchemy_module,
            "app.database": database_module,
            "app.models": models_module,
            "app.services.auto_redeem": auto_redeem_module,
            "app.services.redemption": redemption_module,
            "app.services.settings": settings_module,
            "app.services.team": team_module,
            "app.services.telegram": telegram_module,
            "app.utils.token_parser": token_parser_module,
            "app.utils.time_utils": time_utils_module,
        }
    )


def _load_tg_module():
    _install_stub_modules()
    module_name = "tests._tg_under_test"
    sys.modules.pop(module_name, None)
    module_path = Path(__file__).resolve().parents[1] / "app" / "routes" / "tg.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


tg = _load_tg_module()


class _FakeScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _FakeSession:
    def __init__(self, count_value: int):
        self.count_value = count_value
        self.executed_statements = []

    async def execute(self, statement):
        self.executed_statements.append(statement)
        return _FakeScalarResult(self.count_value)


class _FakeAsyncSessionContext:
    def __init__(self, session: _FakeSession):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


class TgRedeemReceiptTests(unittest.IsolatedAsyncioTestCase):
    def _make_session_factory(self, count_value: int):
        session = _FakeSession(count_value=count_value)

        def factory():
            return _FakeAsyncSessionContext(session)

        return session, factory

    async def test_process_redeem_success_includes_daily_count_and_available_seats(self):
        session, session_factory = self._make_session_factory(count_value=3)
        send_message_mock = AsyncMock()
        auto_redeem_mock = AsyncMock(
            return_value={
                "success": True,
                "message": "邀请码已发送",
                "used_code": "ABCD1234",
                "generated_codes": 0,
                "team_info": {
                    "team_id": 12,
                    "team_name": "Example Team",
                    "expires_at": "2026-03-31T23:59:59",
                },
                "error": None,
            }
        )
        get_available_seats_mock = AsyncMock(return_value=18)

        async def fake_get_setting(_db, key, default=""):
            values = {
                "tg_enabled": "true",
                "tg_bot_token": "bot-token",
            }
            return values.get(key, default)

        with patch.object(tg, "AsyncSessionLocal", new=session_factory), \
             patch.object(tg.settings_service, "get_setting", new=AsyncMock(side_effect=fake_get_setting)), \
             patch.object(tg, "auto_redeem_by_email", new=auto_redeem_mock), \
             patch.object(tg.team_service, "get_total_available_seats", new=get_available_seats_mock), \
             patch.object(tg, "send_message", new=send_message_mock):
            await tg._process_redeem(123456, 42, "user@example.com")

        auto_redeem_mock.assert_awaited_once_with(
            "user@example.com",
            session,
            source="tg",
            tg_chat_id=123456,
        )
        get_available_seats_mock.assert_awaited_once_with(session)
        self.assertEqual(len(session.executed_statements), 1)

        send_message_mock.assert_awaited_once()
        self.assertEqual(send_message_mock.await_args.args[0], "bot-token")
        self.assertEqual(send_message_mock.await_args.args[1], 123456)
        receipt_text = send_message_mock.await_args.args[2]
        self.assertEqual(send_message_mock.await_args.kwargs["reply_to_message_id"], 42)

        self.assertIn("✅ 兑换成功：邀请码已发送", receipt_text)
        self.assertIn("🎟️ 兑换码: ABCD1234", receipt_text)
        self.assertIn("👥 Team: Example Team (ID: 12)", receipt_text)
        self.assertIn("📅 到期时间: 2026-03-31 23:59:59", receipt_text)
        self.assertIn("📊 当前 Chat ID 今日已兑换: 3", receipt_text)
        self.assertIn("📦 当前总可用车位: 18", receipt_text)

    async def test_process_redeem_failure_does_not_include_stats(self):
        session, session_factory = self._make_session_factory(count_value=99)
        send_message_mock = AsyncMock()
        auto_redeem_mock = AsyncMock(
            return_value={
                "success": False,
                "message": None,
                "used_code": None,
                "generated_codes": 0,
                "team_info": None,
                "error": "当前无可用 Team",
            }
        )
        get_available_seats_mock = AsyncMock(return_value=18)

        async def fake_get_setting(_db, key, default=""):
            values = {
                "tg_enabled": "true",
                "tg_bot_token": "bot-token",
            }
            return values.get(key, default)

        with patch.object(tg, "AsyncSessionLocal", new=session_factory), \
             patch.object(tg.settings_service, "get_setting", new=AsyncMock(side_effect=fake_get_setting)), \
             patch.object(tg, "auto_redeem_by_email", new=auto_redeem_mock), \
             patch.object(tg.team_service, "get_total_available_seats", new=get_available_seats_mock), \
             patch.object(tg, "send_message", new=send_message_mock):
            await tg._process_redeem(123456, 84, "user@example.com")

        auto_redeem_mock.assert_awaited_once_with(
            "user@example.com",
            session,
            source="tg",
            tg_chat_id=123456,
        )
        get_available_seats_mock.assert_not_awaited()
        self.assertEqual(session.executed_statements, [])

        send_message_mock.assert_awaited_once()
        self.assertEqual(send_message_mock.await_args.args[0], "bot-token")
        self.assertEqual(send_message_mock.await_args.args[1], 123456)
        receipt_text = send_message_mock.await_args.args[2]
        self.assertEqual(send_message_mock.await_args.kwargs["reply_to_message_id"], 84)

        self.assertIn("🚫 当前没有可用车位", receipt_text)
        self.assertNotIn("当前 Chat ID 今日已兑换", receipt_text)
        self.assertNotIn("当前总可用车位", receipt_text)


if __name__ == "__main__":
    unittest.main()
