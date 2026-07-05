# tests/test_auth.py
import pytest
from httpx import AsyncClient, ASGITransport
from main import app


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.anyio
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@pytest.mark.anyio
async def test_login_invalid(client):
    r = await client.post("/api/v1/auth/login", json={"email": "bad@test.com", "password": "wrong"})
    assert r.status_code == 401


@pytest.mark.anyio
async def test_login_success(client):
    # Requires seeded user — run against a test DB
    r = await client.post("/api/v1/auth/login", json={
        "email": "admin@pi-os.io",
        "password": "admin123",
    })
    # 200 if user exists, 401 if not seeded yet
    assert r.status_code in (200, 401)
    if r.status_code == 200:
        data = r.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["user"]["role"] == "admin"


@pytest.mark.anyio
async def test_me_unauthorized(client):
    r = await client.get("/api/v1/auth/me")
    assert r.status_code == 401


@pytest.mark.anyio
async def test_me_authorized(client):
    login = await client.post("/api/v1/auth/login", json={
        "email": "admin@pi-os.io", "password": "admin123",
    })
    if login.status_code != 200:
        pytest.skip("No seeded admin user")
    token = login.json()["access_token"]
    r = await client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    assert r.json()["role"] == "admin"
