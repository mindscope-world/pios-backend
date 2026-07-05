import pytest
from types import SimpleNamespace

from app.services.intelligence import montecarlo_service as mc_mod


@pytest.mark.anyio
async def test_compute_monte_carlo_insufficient(monkeypatch):
    async def fake_get_symbol(db, symbol=None):
        return SimpleNamespace(id=1, symbol=symbol or "BTC/USDT")

    async def fake_recent_ticks(db, sym_id, limit):
        # fewer than required 20 ticks
        return [SimpleNamespace(price=100.0 + i) for i in range(5)]

    monkeypatch.setattr(mc_mod, "get_symbol_by_name", fake_get_symbol)
    monkeypatch.setattr(mc_mod, "recent_ticks", fake_recent_ticks)

    res = await mc_mod.compute_monte_carlo(current_user=None, db=None, symbol="BTC/USDT", simulations=100, horizon_days=5)
    assert isinstance(res, dict)
    assert res.get("error") == "insufficient_tick_data"
    assert res.get("tick_count") == 5


@pytest.mark.anyio
async def test_compute_monte_carlo_basic_run(monkeypatch):
    async def fake_get_symbol(db, symbol=None):
        return SimpleNamespace(id=1, symbol=symbol or "BTC/USDT")

    async def fake_recent_ticks(db, sym_id, limit):
        # provide 100 ticks with increasing prices
        return [SimpleNamespace(price=100.0 + i) for i in range(100)]

    async def fake_open_positions(db, user_id):
        return []

    monkeypatch.setattr(mc_mod, "get_symbol_by_name", fake_get_symbol)
    monkeypatch.setattr(mc_mod, "recent_ticks", fake_recent_ticks)
    monkeypatch.setattr(mc_mod, "open_positions", fake_open_positions)

    res = await mc_mod.compute_monte_carlo(current_user=SimpleNamespace(id=1), db=None, symbol="BTC/USDT", simulations=200, horizon_days=10)
    assert isinstance(res, dict)
    assert "sim_count" in res and res["sim_count"] == 200
    assert "cases" in res and isinstance(res["cases"], list)
    assert "histogram" in res and isinstance(res["histogram"], list)
