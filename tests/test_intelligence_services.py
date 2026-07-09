import importlib
import inspect
from types import SimpleNamespace
import pytest

MODULES = [
    "capital_service",
    "market_candles_service",
    "cross_market_service",
    "ofi_service",
    "behavior_service",
    "montecarlo_service",
    "command_center_service",
    "adaptation_service",
    "signal_conflict_service",
    "scenarios_service",
    "decision_service",
    "regime_service",
    "why_not_trade_service",
    "features_service",
]

COMMON_HELPERS = [
    "get_symbol_by_name",
    "recent_ticks",
    "open_positions",
    "get_user_by_id",
    "get_system_user",
    "fetch_ticks",
    "get_positions",
]


def _make_stub(name):
    async def _stub(*args, **kwargs):
        if "symbol" in name:
            return SimpleNamespace(id=1, symbol=kwargs.get("symbol") or "BTC/USDT")
        if "tick" in name or "recent" in name or "fetch" in name:
            return [SimpleNamespace(price=100.0 + i) for i in range(50)]
        if "open" in name or "positions" in name:
            return []
        if "user" in name:
            return SimpleNamespace(id=1)
        return None

    return _stub


@pytest.mark.anyio
async def test_all_intelligence_compute_functions_run(monkeypatch):
    """Ensure each compute_* coroutine in intelligence services runs without raising.

    We stub common DB/helper functions to keep tests isolated from infrastructure.
    """
    base = "app.services.intelligence"

    for mod_name in MODULES:
        mod = importlib.import_module(f"{base}.{mod_name}")

        # stub common helpers if present in module
        for helper in COMMON_HELPERS:
            if hasattr(mod, helper):
                monkeypatch.setattr(mod, helper, _make_stub(helper))

        # find all async compute_ functions
        funcs = [
            fn for _, fn in inspect.getmembers(mod, inspect.iscoroutinefunction)
            if fn.__name__.startswith("compute_")
        ]

        for fn in funcs:
            # build kwargs depending on signature
            sig = inspect.signature(fn)
            kwargs = {}
            for pname, param in sig.parameters.items():
                if pname in ("current_user", "user", "admin"):
                    kwargs[pname] = SimpleNamespace(id=1)
                elif pname in ("db",):
                    kwargs[pname] = None
                elif pname in ("symbol",):
                    kwargs[pname] = "BTC/USDT"
                elif pname in ("resolution",):
                    kwargs[pname] = "1"
                elif pname in ("limit",):
                    kwargs[pname] = 100
                elif pname in ("simulations",):
                    kwargs[pname] = 50
                elif pname in ("horizon_days",):
                    kwargs[pname] = 1
                elif pname in ("user_id",):
                    kwargs[pname] = 1
                else:
                    # rely on default where possible
                    if param.default is inspect._empty:
                        # give a generic safe fallback
                        kwargs[pname] = None

            try:
                result = await fn(**kwargs)
            except Exception as e:
                pytest.fail(f"{mod_name}.{fn.__name__} raised {e!r}")

            # basic sanity: should return something serializable-ish (not raise, and not be an unexpected None)
            assert result is not None, f"{mod_name}.{fn.__name__} returned None"
