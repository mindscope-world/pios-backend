# tests/test_intelligence_endpoints.py
"""
Covers A3: 25 of 38 routes in app/api/v1/endpoints/intelligence.py called
get_redis("<name>", user.id), but get_redis() takes zero arguments -- guaranteed
TypeError on every call. Separately, the intended key names didn't match what
app/workers/intelligence_worker.py actually writes (symbol-keyed via
normalize_symbol(), not user-keyed).

This seeds a fake payload under the exact key intelligence_worker.py would write
for each of the 16 worker-cached routes, and asserts the corresponding endpoint
returns it verbatim -- this is the test that would have caught both original bugs
immediately. It also smoke-tests the remaining 9 routes that call service
functions directly (live, not worker-cached), confirming they return 200 rather
than crashing.
"""
import json

import pytest
from httpx import AsyncClient, ASGITransport

from main import app
from app.core.redis import get_redis

TEST_SYMBOL = "BTC/USDT"
TEST_SYMBOL_KEY = "BTCUSDT"  # intelligence_worker.normalize_symbol("BTC/USDT")

# endpoint path -> Redis key prefix intelligence_worker.py actually writes for it.
# (Some prefixes intentionally differ from the endpoint name -- e.g. /regime/current
# reads "regime_history" because that's the worker's own local variable name for
# compute_regime_current()'s output; /alpha/state reads "alpha_state", not
# "alpha_factory_state". See intelligence.py's inline comments for each.)
WORKER_CACHED_ENDPOINTS = {
    "/api/v1/intelligence/decision/feed":          "decision_feed",
    "/api/v1/intelligence/regime/current":         "regime_history",
    "/api/v1/intelligence/regime/trend":           "regime_trend",
    "/api/v1/intelligence/ofi":                    "order_flow",
    "/api/v1/intelligence/gmig/snapshot":          "gmig_snapshot",
    "/api/v1/intelligence/gmig/radar":             "gmig_radar",
    "/api/v1/intelligence/adaptation/feed":        "adaptation_feed",
    "/api/v1/intelligence/adaptation/active":      "adaptation_active",
    "/api/v1/intelligence/adaptation/drift":       "adaptation_drift",
    "/api/v1/intelligence/alpha/state":            "alpha_state",
    "/api/v1/intelligence/alpha/darwin":           "alpha_darwin",
    "/api/v1/intelligence/features":               "features",
    "/api/v1/intelligence/command-center/current": "command_center",
    "/api/v1/intelligence/scenarios/simulations":  "scenarios",
    "/api/v1/intelligence/traces":                 "decision_traces",
    "/api/v1/intelligence/why-not-trade":          "why_not_trade",
}

# Routes that are NOT worker-cached -- they call a service function directly
# (live per-request computation). These should never depend on a Redis key.
DIRECT_SERVICE_ENDPOINTS = [
    "/api/v1/intelligence/decision/current",
    "/api/v1/intelligence/ofi/chart",
    "/api/v1/intelligence/montecarlo",
    "/api/v1/intelligence/signal-conflict",
    "/api/v1/intelligence/ofi/auto",
    "/api/v1/intelligence/montecarlo/auto",
    "/api/v1/intelligence/signal-conflict/auto",
    "/api/v1/intelligence/ofi/enhanced",
    "/api/v1/intelligence/gmig/enhanced",
]


async def get_token(client, email="admin@pi-os.io", password="admin123"):
    r = await client.post("/api/v1/auth/login", json={"email": email, "password": password})
    if r.status_code != 200:
        return None
    return r.json()["access_token"]


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def test_all_38_routes_are_registered():
    paths = {r.path for r in app.routes if getattr(r, "path", "").startswith("/api/v1/intelligence")}
    assert len(paths) == 38


@pytest.mark.anyio
@pytest.mark.parametrize("path,key_prefix", list(WORKER_CACHED_ENDPOINTS.items()))
async def test_worker_cached_endpoint_returns_seeded_payload(client, path, key_prefix):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded admin user")

    # Seed the exact key intelligence_worker.py would write for this data type.
    payload = {"marker": f"fake-{key_prefix}-payload", "symbol": TEST_SYMBOL_KEY}
    r = get_redis()
    await r.set(f"{key_prefix}:{TEST_SYMBOL_KEY}", json.dumps(payload))

    resp = await client.get(
        path,
        params={"symbol": TEST_SYMBOL},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == payload


@pytest.mark.anyio
@pytest.mark.parametrize("path", DIRECT_SERVICE_ENDPOINTS)
async def test_direct_service_endpoint_returns_200_not_crash(client, path):
    token = await get_token(client)
    if not token:
        pytest.skip("No seeded admin user")

    resp = await client.get(path, headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200, resp.text
