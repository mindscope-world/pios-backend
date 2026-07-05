# tests/test_orders.py
import pytest
from httpx import AsyncClient, ASGITransport
from main import app


async def get_token(client, email="admin@pi-os.io", password="admin123"):
    r = await client.post("/api/v1/auth/login", json={"email": email, "password": password})
    if r.status_code != 200:
        return None
    return r.json()["access_token"]


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.anyio
async def test_list_orders_unauth(client):
    r = await client.get("/api/v1/orders")
    assert r.status_code == 401


@pytest.mark.anyio
async def test_list_orders_auth(client):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded user")
    r = await client.get("/api/v1/orders", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    data = r.json()
    assert "items" in data
    assert "total" in data


@pytest.mark.anyio
async def test_submit_order_missing_broker(client):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded user")
    r = await client.post(
        "/api/v1/orders",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "broker_id": "00000000-0000-0000-0000-000000000000",
            "symbol": "BTC/USDT",
            "side": "BUY",
            "order_type": "MARKET",
            "qty": 0.01,
        },
    )
    # 404 broker not found or 403/422
    assert r.status_code in (404, 422, 403)


@pytest.mark.anyio
async def test_viewer_cannot_trade(client):
    token = await get_token(client, "viewer@pi-os.io", "viewer123")
    if not token:
        pytest.skip("No seeded viewer")
    r = await client.post(
        "/api/v1/orders",
        headers={"Authorization": f"Bearer {token}"},
        json={"broker_id": "00000000-0000-0000-0000-000000000000",
              "symbol": "BTC/USDT", "side": "BUY", "order_type": "MARKET", "qty": 0.01},
    )
    assert r.status_code == 403
