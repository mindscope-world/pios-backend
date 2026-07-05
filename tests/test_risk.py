# tests/test_risk.py
"""
Covers A2: risk_service.compute_risk_metrics / trigger_kill_switch used to be called
with swapped/mistyped arguments from app/api/v1/endpoints/risk.py, so both endpoints
raised at runtime before ever exercising the real VaR/CVaR/kill-switch logic.

These tests hit the real endpoints (not mocks) against seeded data, so a future
signature/argument-order regression fails the suite immediately instead of only at
runtime in production.
"""
import inspect
import json

import numpy as np
import pytest
from datetime import datetime, timedelta, timezone
from httpx import AsyncClient, ASGITransport
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from main import app
from app.services.risk_service import compute_risk_metrics, trigger_kill_switch

TEST_DB_URL = "postgresql+asyncpg://pios:password@localhost:5432/pios_test"
_engine = create_async_engine(TEST_DB_URL, echo=False)
_Session = async_sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)


async def get_token(client, email="admin@pi-os.io", password="admin123"):
    r = await client.post("/api/v1/auth/login", json={"email": email, "password": password})
    if r.status_code != 200:
        return None
    return r.json()["access_token"]


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def _get_user_id(email="admin@pi-os.io"):
    from app.models.all_models import User
    async with _Session() as db:
        result = await db.execute(select(User).where(User.email == email))
        user = result.scalar_one_or_none()
        return user.id if user else None


# ── Signature-consistency contract ──────────────────────────────────────────────
# Cheap guardrail for the bug this task fixed: db first, primitives (not a User
# object) for the acting user -- matches order_service.py's endpoint->service
# convention. If a future refactor reorders these params without updating the
# risk.py call sites, this test (plus the live-endpoint tests below) catches it.

def test_compute_risk_metrics_signature_is_db_first():
    assert list(inspect.signature(compute_risk_metrics).parameters) == ["db", "user_id"]


def test_trigger_kill_switch_signature_is_db_first():
    assert list(inspect.signature(trigger_kill_switch).parameters) == [
        "db", "data", "user_id", "user_email",
    ]


# ── GET /risk/metrics ────────────────────────────────────────────────────────────

@pytest.mark.anyio
async def test_risk_metrics_computes_real_var_from_seeded_equity_curve(client):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded admin user")

    user_id = await _get_user_id()
    if not user_id:
        pytest.skip("Admin user not resolvable")

    from app.models.all_models import PnLSnapshot

    # Deterministic 15-point equity curve (alternating +1% / -0.5% daily) so
    # var95/var99/cvar can be derived independently and compared exactly.
    equity = 100_000.0
    curve = [equity]
    for i in range(14):
        pct = 0.01 if i % 2 == 0 else -0.005
        equity = equity * (1 + pct)
        curve.append(equity)

    async with _Session() as db:
        base_time = datetime.now(timezone.utc) - timedelta(days=len(curve))
        for i, eq in enumerate(curve):
            db.add(PnLSnapshot(
                user_id=user_id,
                total_equity=eq,
                realized_pnl=0,
                unrealized_pnl=0,
                cash_balance=eq,
                snapshot_at=base_time + timedelta(days=i),
            ))
        await db.commit()

    r = await client.get("/api/v1/risk/metrics", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    data = r.json()

    # Reproduce risk_service.compute_risk_metrics's historical-VaR branch independently.
    eq_arr = np.array(curve)
    ret = np.diff(eq_arr) / eq_arr[:-1]
    var95_pct = float(np.percentile(ret, 5))
    var99_pct = float(np.percentile(ret, 1))
    tail = ret[ret <= var95_pct]
    cvar_pct = float(np.mean(tail)) if len(tail) else var95_pct * 1.3
    expected_var95 = round(abs(var95_pct) * curve[-1], 2)
    expected_var99 = round(abs(var99_pct) * curve[-1], 2)
    expected_cvar = round(abs(cvar_pct) * curve[-1], 2)

    assert data["var95"] == pytest.approx(expected_var95, rel=1e-3)
    assert data["var99"] == pytest.approx(expected_var99, rel=1e-3)
    assert data["cvar"] == pytest.approx(expected_cvar, rel=1e-3)
    assert data["kill_switch_armed"] is True
    assert isinstance(data["drawdown_current"], float)
    assert isinstance(data["leverage"], float)


@pytest.mark.anyio
async def test_risk_metrics_unauth(client):
    r = await client.get("/api/v1/risk/metrics")
    assert r.status_code in (401, 403)


# ── POST /risk/killswitch ──────────────────────────────────────────────────────

@pytest.mark.anyio
async def test_kill_switch_cancels_orders_in_db_and_at_broker(client, monkeypatch):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded admin user")

    from app.models.all_models import User, Broker, Symbol, Order, Position
    from app.core.security import encrypt_credentials

    cancelled_broker_order_ids = []

    class FakeAdapter:
        async def cancel_order(self, broker_order_id):
            cancelled_broker_order_ids.append(broker_order_id)
            return {"status": "CANCELLED"}

    import app.services.risk_service as risk_service_module
    monkeypatch.setattr(risk_service_module, "get_adapter", lambda broker: FakeAdapter())

    async with _Session() as db:
        user = (await db.execute(select(User).where(User.email == "admin@pi-os.io"))).scalar_one()
        symbol = (await db.execute(select(Symbol).limit(1))).scalar_one()

        broker = Broker(
            owner_id=user.id, name="KillSwitch Test Broker", broker_type="CUSTOM",
            is_paper=True, credentials_enc=encrypt_credentials(json.dumps({"api_key": "x"})),
        )
        db.add(broker)
        await db.flush()

        order = Order(
            client_order_id="PI-KILLTEST1",
            user_id=user.id, broker_id=broker.id, symbol_id=symbol.id,
            side="BUY", order_type="MARKET", qty=1.0,
            broker_order_id="FAKE-BROKER-ORDER-1",
        )
        order.transition("SUBMITTED")
        db.add(order)

        position = Position(
            user_id=user.id, broker_id=broker.id, symbol_id=symbol.id,
            side="LONG", qty=1.0, avg_cost=100.0, is_open=True,
        )
        db.add(position)

        await db.commit()
        order_id, position_id = order.id, position.id

    r = await client.post(
        "/api/v1/risk/killswitch",
        headers={"Authorization": f"Bearer {token}"},
        json={"reason": "test kill switch"},
    )
    assert r.status_code == 202
    event = r.json()
    assert event["orders_cancelled"] >= 1
    assert event["positions_closed"] >= 1

    # The broker adapter was actually invoked for the open order (not DB-only).
    assert "FAKE-BROKER-ORDER-1" in cancelled_broker_order_ids

    async with _Session() as db:
        refreshed_order = (await db.execute(select(Order).where(Order.id == order_id))).scalar_one()
        assert refreshed_order.status == "CANCELLED"

        refreshed_position = (await db.execute(select(Position).where(Position.id == position_id))).scalar_one()
        assert refreshed_position.is_open is False


@pytest.mark.anyio
async def test_kill_switch_forbidden_for_non_admin(client):
    token = await get_token(client, "trader@pi-os.io", "trader123")
    if not token:
        pytest.skip("No seeded trader user")
    r = await client.post(
        "/api/v1/risk/killswitch",
        headers={"Authorization": f"Bearer {token}"},
        json={"reason": "should be blocked"},
    )
    assert r.status_code == 403
