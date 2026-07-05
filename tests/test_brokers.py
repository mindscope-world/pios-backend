# tests/test_brokers.py
import pytest
from httpx import AsyncClient, ASGITransport
from main import app


async def get_token(client, email="trader@pi-os.io", password="trader123"):
    r = await client.post("/api/v1/auth/login", json={"email": email, "password": password})
    if r.status_code != 200:
        return None
    return r.json()["access_token"]


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.anyio
async def test_list_brokers_empty(client):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded trader")
    r = await client.get("/api/v1/brokers", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    assert r.json()["items"] == []


@pytest.mark.anyio
async def test_add_paper_broker(client):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded trader")
    r = await client.post(
        "/api/v1/brokers",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "name": "My Alpaca Paper",
            "broker_type": "ALPACA",
            "is_paper": True,
            "credentials": {"api_key": "PAPER_KEY", "api_secret": "PAPER_SECRET"},
            "config": {},
        },
    )
    assert r.status_code == 201
    data = r.json()
    assert data["name"] == "My Alpaca Paper"
    assert data["is_paper"] is True
    assert "credentials_enc" not in data  # never exposed


@pytest.mark.anyio
async def test_viewer_cannot_add_broker(client):
    token = await get_token(client, "viewer@pi-os.io", "viewer123")
    if not token:
        pytest.skip("No seeded viewer")
    r = await client.post(
        "/api/v1/brokers",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "x", "broker_type": "ALPACA", "is_paper": True,
              "credentials": {"api_key": "k", "api_secret": "s"}, "config": {}},
    )
    assert r.status_code == 403
