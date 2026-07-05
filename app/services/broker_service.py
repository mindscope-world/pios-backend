# app/services/broker_service.py
"""
Broker adapter factory.
Each adapter wraps a specific broker SDK behind a common interface.
Traders register their own brokers; this service manages the lifecycle.
"""
import json
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import decrypt_credentials, encrypt_credentials
from app.models.all_models import Broker, Order
from app.schemas.all_schemas import BrokerCreate, BrokerTestResult


# ── Abstract Adapter ──────────────────────────────────────────────────────────

class BrokerAdapter(ABC):
    """Common interface every broker adapter must implement."""

    def __init__(self, broker: Broker, credentials: dict):
        self.broker = broker
        self.creds = credentials

    @abstractmethod
    async def test_connection(self) -> BrokerTestResult:
        """Verify credentials and measure latency."""
        ...

    @abstractmethod
    async def get_account(self) -> dict:
        """Return balance, buying power, account status."""
        ...

    @abstractmethod
    async def submit_order(self, order: Order) -> dict:
        """Submit order to broker. Return broker_order_id + status."""
        ...

    @abstractmethod
    async def cancel_order(self, broker_order_id: str) -> dict:
        ...

    @abstractmethod
    async def get_positions(self) -> list[dict]:
        ...

    @abstractmethod
    async def get_fills(self, since: datetime | None = None) -> list[dict]:
        ...


# ── Alpaca Adapter ────────────────────────────────────────────────────────────

class AlpacaAdapter(BrokerAdapter):
    async def _client(self):
        from alpaca.trading.client import TradingClient
        return TradingClient(
            api_key=self.creds["api_key"],
            secret_key=self.creds["api_secret"],
            paper=self.broker.is_paper,
        )

    async def test_connection(self) -> BrokerTestResult:
        try:
            t0 = time.perf_counter()
            client = await self._client()
            account = client.get_account()
            latency = (time.perf_counter() - t0) * 1000
            return BrokerTestResult(
                success=True,
                latency_ms=round(latency, 2),
                message="Connected",
                account_info={
                    "buying_power": float(account.buying_power),
                    "equity": float(account.equity),
                    "status": str(account.status),
                },
            )
        except Exception as e:
            return BrokerTestResult(success=False, latency_ms=None, message=str(e))

    async def get_account(self) -> dict:
        client = await self._client()
        a = client.get_account()
        return {"buying_power": float(a.buying_power), "equity": float(a.equity)}

    async def submit_order(self, order: Order) -> dict:
        from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
        client = await self._client()
        side = OrderSide.BUY if order.side == "BUY" else OrderSide.SELL
        tif  = TimeInForce.GTC

        if order.order_type == "MARKET":
            req = MarketOrderRequest(symbol=order.symbol.symbol, qty=order.qty, side=side, time_in_force=tif)
        else:
            req = LimitOrderRequest(symbol=order.symbol.symbol, qty=order.qty, side=side, limit_price=order.price, time_in_force=tif)

        result = client.submit_order(req)
        return {"broker_order_id": str(result.id), "status": str(result.status)}

    async def cancel_order(self, broker_order_id: str) -> dict:
        client = await self._client()
        client.cancel_order_by_id(broker_order_id)
        return {"status": "CANCELLED"}

    async def get_positions(self) -> list[dict]:
        client = await self._client()
        return [
            {"symbol": p.symbol, "qty": float(p.qty), "side": "LONG" if float(p.qty) > 0 else "SHORT",
             "avg_entry_price": float(p.avg_entry_price), "unrealized_pl": float(p.unrealized_pl)}
            for p in client.get_all_positions()
        ]

    async def get_fills(self, since: datetime | None = None) -> list[dict]:
        # Alpaca doesn't have a direct fills endpoint — derive from orders
        return []


# ── CCXT Adapter (Binance, OKX, Bybit …) ─────────────────────────────────────

class CCXTAdapter(BrokerAdapter):
    def _exchange(self):
        import ccxt
        exchange_class = getattr(ccxt, self.broker.exchange_id or "binance")
        cfg: dict[str, Any] = {
            "apiKey": self.creds.get("api_key"),
            "secret": self.creds.get("api_secret"),
            "enableRateLimit": True,
        }
        if self.creds.get("passphrase"):
            cfg["password"] = self.creds["passphrase"]
        if self.broker.is_paper or self.broker.config.get("sandbox"):
            cfg["options"] = {"defaultType": "spot"}
        return exchange_class(cfg)

    async def test_connection(self) -> BrokerTestResult:
        try:
            t0 = time.perf_counter()
            ex = self._exchange()
            balance = ex.fetch_balance()
            latency = (time.perf_counter() - t0) * 1000
            total = {k: v for k, v in balance.get("total", {}).items() if v and v > 0}
            return BrokerTestResult(success=True, latency_ms=round(latency, 2), message="Connected", account_info={"balances": total})
        except Exception as e:
            return BrokerTestResult(success=False, latency_ms=None, message=str(e))

    async def get_account(self) -> dict:
        ex = self._exchange()
        balance = ex.fetch_balance()
        return {k: v for k, v in balance.get("total", {}).items() if v and v > 0}

    async def submit_order(self, order: Order) -> dict:
        ex = self._exchange()
        symbol = order.symbol.symbol.replace("/", "")  # BTC/USDT → BTCUSDT for some exchanges
        side = order.side.lower()
        otype = "market" if order.order_type == "MARKET" else "limit"
        params = {}
        result = ex.create_order(symbol, otype, side, float(order.qty), float(order.price) if order.price else None, params)
        return {"broker_order_id": str(result["id"]), "status": result.get("status", "SUBMITTED").upper()}

    async def cancel_order(self, broker_order_id: str) -> dict:
        return {"status": "CANCELLED"}

    async def get_positions(self) -> list[dict]:
        ex = self._exchange()
        try:
            positions = ex.fetch_positions()
            return [{"symbol": p["symbol"], "qty": p["contracts"], "side": p["side"].upper()} for p in positions if p["contracts"]]
        except Exception:
            return []

    async def get_fills(self, since: datetime | None = None) -> list[dict]:
        return []


# ── Paper / Simulation Adapter ────────────────────────────────────────────────

class PaperAdapter(BrokerAdapter):
    """Instant fills at market price — for testing without a real broker."""

    async def test_connection(self) -> BrokerTestResult:
        return BrokerTestResult(success=True, latency_ms=0.1, message="Paper trading active", account_info={"balance": 100000})

    async def get_account(self) -> dict:
        return {"buying_power": 100000, "equity": 100000}

    async def submit_order(self, order: Order) -> dict:
        import uuid as _uuid
        return {"broker_order_id": str(_uuid.uuid4()), "status": "FILLED", "avg_price": float(order.price or 0)}

    async def cancel_order(self, broker_order_id: str) -> dict:
        return {"status": "CANCELLED"}

    async def get_positions(self) -> list[dict]:
        return []

    async def get_fills(self, since: datetime | None = None) -> list[dict]:
        return []


# ── Factory ───────────────────────────────────────────────────────────────────

ADAPTER_MAP = {
    "ALPACA": AlpacaAdapter,
    "BINANCE": CCXTAdapter,
    "CCXT": CCXTAdapter,
    "PAPER": PaperAdapter,
}


def get_adapter(broker: Broker) -> BrokerAdapter:
    """Decrypt credentials and instantiate the correct adapter."""
    creds_json = decrypt_credentials(broker.credentials_enc)
    creds = json.loads(creds_json)
    adapter_cls = ADAPTER_MAP.get(broker.broker_type.upper(), PaperAdapter)
    return adapter_cls(broker, creds)


# ── DB helpers ────────────────────────────────────────────────────────────────

async def get_broker_or_404(db: AsyncSession, broker_id: uuid.UUID, owner_id: uuid.UUID, role: str) -> Broker:
    from fastapi import HTTPException, status
    q = select(Broker).where(Broker.id == broker_id, Broker.is_active == True)  # noqa: E712
    if role not in ("admin",):
        q = q.where(Broker.owner_id == owner_id)
    result = await db.execute(q)
    broker = result.scalar_one_or_none()
    if not broker:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Broker not found")
    return broker


async def create_broker(db: AsyncSession, data: BrokerCreate, owner_id: uuid.UUID) -> Broker:
    creds_enc = encrypt_credentials(json.dumps(data.credentials.model_dump()))
    broker = Broker(
        owner_id=owner_id,
        name=data.name,
        broker_type=data.broker_type.upper(),
        exchange_id=data.exchange_id,
        is_paper=data.is_paper,
        credentials_enc=creds_enc,
        config=data.config,
    )
    db.add(broker)
    await db.flush()
    return broker
