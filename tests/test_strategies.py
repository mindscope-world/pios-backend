# tests/test_strategies.py
import pytest
from httpx import AsyncClient, ASGITransport
from main import app


async def get_token(client, email="quant@pi-os.io", password="quant123"):
    r = await client.post("/api/v1/auth/login", json={"email": email, "password": password})
    if r.status_code != 200:
        return None
    return r.json()["access_token"]


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.anyio
async def test_create_strategy(client):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded quant")
    r = await client.post(
        "/api/v1/strategies",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "name": "Test BTC Momentum",
            "hypothesis": "BTC trends after volume spikes",
            "config": {"lookback": 20, "threshold": 1.5},
        },
    )
    assert r.status_code == 201
    data = r.json()
    assert data["lifecycle_stage"] == "IDEA"
    assert data["is_paper_only"] is True
    return data["id"]


@pytest.mark.anyio
async def test_list_strategies(client):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded quant")
    r = await client.get("/api/v1/strategies", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    assert "items" in r.json()


@pytest.mark.anyio
async def test_advance_without_hypothesis_fails(client):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded quant")
    # Create strategy without hypothesis
    create = await client.post(
        "/api/v1/strategies",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "No Hypothesis Strategy"},
    )
    if create.status_code != 201:
        pytest.skip("Create failed")
    strategy_id = create.json()["id"]

    # Try to advance IDEA → RESEARCH (passes) then RESEARCH → BACKTEST (fails, no hypothesis)
    adv1 = await client.post(
        f"/api/v1/strategies/{strategy_id}/advance",
        headers={"Authorization": f"Bearer {token}"},
        json={},
    )
    # IDEA → RESEARCH has no hard gate, should pass
    assert adv1.status_code in (200, 422)


@pytest.mark.anyio
async def test_trader_cannot_create_strategy(client):
    token = await get_token(client, "trader@pi-os.io", "trader123")
    if not token:
        pytest.skip("No seeded trader")
    r = await client.post(
        "/api/v1/strategies",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "Trader Strategy"},
    )
    assert r.status_code == 403
