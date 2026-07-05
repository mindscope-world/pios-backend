# tests/conftest.py
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from app.db.session import Base, get_db
from app.core.security import hash_password
from main import app

TEST_DB_URL = "postgresql+asyncpg://pios:password@localhost:5432/pios_test"

test_engine = create_async_engine(TEST_DB_URL, echo=False)
TestSession = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_test_db():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    # Seed test users
    async with TestSession() as db:
        from app.models.all_models import User, Symbol, RiskLimit
        users = [
            User(email="admin@pi-os.io",      password_hash=hash_password("admin123"),  full_name="Alex Chen",    role="admin"),
            User(email="trader@pi-os.io",     password_hash=hash_password("trader123"), full_name="Sarah Kim",    role="trader"),
            User(email="quant@pi-os.io",      password_hash=hash_password("quant123"),  full_name="Marcus Webb",  role="quant"),
            User(email="viewer@pi-os.io",     password_hash=hash_password("viewer123"), full_name="Priya Sharma", role="viewer"),
            User(email="compliance@pi-os.io", password_hash=hash_password("comply123"), full_name="David Osei",   role="compliance"),
        ]
        db.add_all(users)

        symbols = [
            Symbol(symbol="BTC/USDT", base_asset="BTC", quote_asset="USDT", asset_class="crypto",   exchange="BINANCE"),
            Symbol(symbol="ETH/USDT", base_asset="ETH", quote_asset="USDT", asset_class="crypto",   exchange="BINANCE"),
            Symbol(symbol="AAPL",     base_asset="AAPL",quote_asset="USD",  asset_class="equities", exchange="NYSE"),
        ]
        db.add_all(symbols)

        limits = [
            RiskLimit(name="Max Drawdown",     scope="global", limit_type="max_drawdown_pct", limit_value=15.0, breach_action="KILL_SWITCH"),
            RiskLimit(name="Daily Loss Limit", scope="global", limit_type="daily_loss_limit", limit_value=5000.0, breach_action="BLOCK"),
            RiskLimit(name="Max Position",     scope="global", limit_type="max_position_usd", limit_value=20000.0, breach_action="ALERT"),
        ]
        db.add_all(limits)
        await db.commit()

    # Override the DB dependency to use test DB
    async def override_get_db():
        async with TestSession() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = override_get_db
    yield
    await test_engine.dispose()
