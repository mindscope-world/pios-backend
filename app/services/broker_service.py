# app/services/broker_service.py
"""
Broker adapter factory.
Each adapter wraps a specific broker SDK behind a common interface.
Traders register their own brokers; this service manages the lifecycle.
"""
import asyncio
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
from app.services.brokers.mt5.adapter import MT5Adapter


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

    @staticmethod
    def _alpaca_symbol(symbol: str) -> str:
        """
        App symbol → Alpaca symbol. Alpaca quotes crypto against USD, and a
        (paper) account funds trades from its USD buying power — it holds no
        USDT/USDC, so sending "BTC/USDT" verbatim gets rejected with
        "insufficient balance for USDT". Map USDT/USDC-quoted pairs to the
        /USD pair (mirrors the market-data routing in market_data_service).
        Equities pass through unchanged.
        """
        s = symbol.strip().upper()
        if "/" in s:
            base, quote = s.split("/", 1)
            if quote in ("USDT", "USDC"):
                quote = "USD"
            return f"{base}/{quote}"
        return s

    @staticmethod
    def _order_status(result) -> str:
        return str(getattr(result.status, "value", result.status)).upper()

    async def get_order(self, broker_order_id: str) -> dict:
        """Broker-side view of one order — consumed by the fill-sync loop."""
        client = await self._client()
        o = await asyncio.to_thread(client.get_order_by_id, broker_order_id)
        return {
            "status": self._order_status(o),
            "filled_qty": float(o.filled_qty or 0),
            "avg_price": float(o.filled_avg_price or 0),
        }

    async def get_order_fills(self, broker_order_id: str, since: datetime | None = None) -> list[dict]:
        """
        Individual per-execution fills for one order, oldest first.

        The order object (get_order/get_order_by_id) only ever exposes the
        *cumulative* filled_qty and a running average price — if several
        fills land between two reconciliation passes (e.g. the trade-update
        stream was disconnected, or the 15s poller just happened to catch
        two prints at once), there's no way to recover the individual print
        prices from it. Alpaca's Account Activities API does carry them, so
        the fill-sync loop calls this to replay each execution as its own
        Fill row instead of collapsing them into one delta at the average.

        No documented `order_id` filter on this endpoint, so activities are
        fetched by date range (`since`, defaulting to the order's own
        lifetime — callers pass the order's created_at) and filtered
        client-side; a paper/dev order's activity list is small enough that
        this is cheap. Returns [] on any failure so callers can fall back
        to the single-delta-at-average behavior rather than lose the fill.
        """
        client = await self._client()
        params: dict = {"direction": "asc", "page_size": 100}
        if since is not None:
            params["after"] = since.isoformat()
        try:
            activities = await asyncio.to_thread(client.get, "/account/activities/FILL", params)
        except Exception:
            return []
        fills = []
        for a in activities or []:
            if not isinstance(a, dict) or a.get("order_id") != broker_order_id:
                continue
            try:
                fills.append({
                    "id": a.get("id"),
                    "price": float(a["price"]),
                    "qty": float(a["qty"]),
                    "transaction_time": a.get("transaction_time") or "",
                })
            except (KeyError, TypeError, ValueError):
                continue
        fills.sort(key=lambda f: f["transaction_time"])
        return fills

    async def _sellable_qty(self, client, symbol: str, requested: float) -> float:
        """
        Alpaca takes the crypto taker fee in the *base* asset, so after
        buying 0.0005 BTC only ~0.00049875 is sellable — "sell exactly what
        I bought" rejects on insufficient balance. When the shortfall is
        fee-sized (≤2%), clamp the sell to what the account actually holds;
        anything larger is a genuine oversell and is left to reject honestly.
        """
        try:
            pos = await asyncio.to_thread(client.get_open_position, symbol.replace("/", ""))
            available = float(getattr(pos, "qty_available", None) or pos.qty or 0)
        except Exception:
            return requested
        if 0 < available < requested and available >= requested * 0.98:
            return available
        return requested

    async def submit_order(self, order: Order) -> dict:
        from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
        client = await self._client()
        side = OrderSide.BUY if order.side == "BUY" else OrderSide.SELL
        tif  = TimeInForce.GTC
        symbol = self._alpaca_symbol(order.symbol.symbol)
        qty = float(order.qty)
        if side == OrderSide.SELL and "/" in symbol:
            qty = await self._sellable_qty(client, symbol, qty)

        if order.order_type == "MARKET":
            req = MarketOrderRequest(symbol=symbol, qty=qty, side=side, time_in_force=tif)
        else:
            req = LimitOrderRequest(symbol=symbol, qty=qty, side=side, limit_price=float(order.price), time_in_force=tif)

        # TradingClient is synchronous — keep it off the event loop
        result = await asyncio.to_thread(client.submit_order, req)

        # Poll briefly toward a terminal state: marketable orders fill within
        # ~a second, and there is no fill-sync loop for Alpaca — an order
        # acknowledged as 'accepted'/'pending_new' would otherwise sit
        # SUBMITTED in the app forever even though it filled at the broker.
        status = self._order_status(result)
        for attempt in range(10):
            if status == "FILLED":
                return {
                    "broker_order_id": str(result.id),
                    "status": "FILLED",
                    "avg_price": float(result.filled_avg_price or 0),
                    "filled_qty": float(result.filled_qty or qty),
                }
            if status in ("REJECTED", "CANCELED", "EXPIRED"):
                raise RuntimeError(f"Alpaca order {status.lower()} (broker order {result.id})")
            if attempt < 9:
                await asyncio.sleep(0.5)
                result = await asyncio.to_thread(client.get_order_by_id, result.id)
                status = self._order_status(result)

        # Still working (e.g. a resting LIMIT) — hand back as submitted
        return {"broker_order_id": str(result.id), "status": status}

    async def cancel_order(self, broker_order_id: str) -> dict:
        client = await self._client()
        client.cancel_order_by_id(broker_order_id)
        return {"status": "CANCELLED"}

    async def get_positions(self) -> list[dict]:
        client = await self._client()
        positions = await asyncio.to_thread(client.get_all_positions)
        return [
            {"symbol": p.symbol, "qty": float(p.qty), "side": "LONG" if float(p.qty) > 0 else "SHORT",
             "avg_entry_price": float(p.avg_entry_price), "unrealized_pl": float(p.unrealized_pl or 0)}
            for p in positions
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


# ── OANDA Adapter (v20 REST) ──────────────────────────────────────────────────

class OandaAdapter(BrokerAdapter):
    """
    Order execution over OANDA's v20 REST API — the same API the app already
    uses live for forex/metals market data (market_data_service).

    Credentials are per-connection only: `api_key` + `account_id` from the
    broker form (no fallback to the app's OANDA_* market-data env keys — that
    would let any user's misconfigured connection trade on the operator's
    account). `is_paper` picks the host: True → practice (fxpractice), False
    → live (fxtrade), mirroring AlpacaAdapter's paper flag.

    VOLUME SEMANTICS: OANDA sizes orders in whole UNITS of the base currency
    (1 unit = 1 EUR on EUR_USD, 1 oz on XAU_USD; minimum 1). The app's qty is
    sent as units after integer rounding; sub-1 quantities are rejected with
    an explicit message rather than silently rounded to zero — a trader
    thinking in lots (0.08) must not have that quietly become nothing.
    """

    _PRACTICE_BASE = "https://api-fxpractice.oanda.com/v3"
    _LIVE_BASE = "https://api-fxtrade.oanda.com/v3"

    def _base(self) -> str:
        return self._PRACTICE_BASE if self.broker.is_paper else self._LIVE_BASE

    def _account_id(self) -> str:
        account_id = self.creds.get("account_id")
        if not (self.creds.get("api_key") and account_id):
            raise ConnectionError(
                "OANDA credentials missing — this connection needs api_key and "
                "account_id (set them on the broker in Connections)."
            )
        return account_id

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.creds.get('api_key')}",
            "Accept-Datetime-Format": "RFC3339",
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        import aiohttp
        url = f"{self._base()}{path}"
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method, url, headers=self._headers(), json=body, timeout=15
            ) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    # v20 rejections carry the reason in orderRejectTransaction
                    # or errorMessage — surface the most specific one.
                    reject = (data or {}).get("orderRejectTransaction", {})
                    detail = (
                        reject.get("rejectReason")
                        or (data or {}).get("errorMessage")
                        or f"HTTP {response.status}"
                    )
                    raise ConnectionError(f"OANDA rejected: {detail}")
                return data or {}

    @staticmethod
    def _instrument(symbol: str) -> str:
        from app.services.market_data_service import _oanda_instrument
        return _oanda_instrument(symbol)

    @staticmethod
    def _price_str(price: float) -> str:
        # v20 wants DecimalNumber strings; trim to ≤5dp without trailing zeros
        # (covers fx 5dp / JPY & metals 3dp). Excess precision from a hand-
        # typed price comes back as an honest PRICE_PRECISION_EXCEEDED.
        return f"{price:.5f}".rstrip("0").rstrip(".")

    def _units(self, order: Order) -> int:
        qty = float(order.qty)
        units = int(round(qty))
        if units < 1:
            raise ConnectionError(
                f"OANDA sizes orders in whole units of the base currency "
                f"(minimum 1; e.g. 1000 = 1000 EUR on EUR/USD) — got {qty}."
            )
        return units if order.side == "BUY" else -units

    async def test_connection(self) -> BrokerTestResult:
        try:
            t0 = time.perf_counter()
            data = await self._request("GET", f"/accounts/{self._account_id()}/summary")
            latency = (time.perf_counter() - t0) * 1000
            a = data.get("account", {})
            return BrokerTestResult(
                success=True,
                latency_ms=round(latency, 2),
                message="Connected",
                account_info={
                    "balance": float(a.get("balance") or 0),
                    "equity": float(a.get("NAV") or 0),
                    "margin_available": float(a.get("marginAvailable") or 0),
                    "currency": a.get("currency"),
                    "open_trades": int(a.get("openTradeCount") or 0),
                },
            )
        except Exception as e:
            return BrokerTestResult(success=False, latency_ms=None, message=str(e))

    async def get_account(self) -> dict:
        data = await self._request("GET", f"/accounts/{self._account_id()}/summary")
        a = data.get("account", {})
        return {
            "balance": float(a.get("balance") or 0),
            "equity": float(a.get("NAV") or 0),
            "buying_power": float(a.get("marginAvailable") or 0),
            "currency": a.get("currency"),
            "unrealized_pl": float(a.get("unrealizedPL") or 0),
        }

    async def submit_order(self, order: Order) -> dict:
        units = self._units(order)
        instrument = self._instrument(order.symbol.symbol)

        spec: dict[str, Any] = {
            "instrument": instrument,
            "units": str(units),
        }
        if order.order_type == "MARKET":
            spec["type"] = "MARKET"
            spec["timeInForce"] = "FOK"
        elif order.order_type == "LIMIT":
            if not order.price:
                raise ConnectionError("LIMIT order without a price")
            spec["type"] = "LIMIT"
            spec["price"] = self._price_str(float(order.price))
            spec["timeInForce"] = "GTC"
        elif order.order_type == "STOP":
            trigger = float(order.stop_price or order.price or 0)
            if not trigger:
                raise ConnectionError("STOP order without a trigger price")
            spec["type"] = "STOP"
            spec["price"] = self._price_str(trigger)
            spec["timeInForce"] = "GTC"
        else:
            raise ConnectionError(f"Order type {order.order_type} not supported on OANDA")

        data = await self._request(
            "POST", f"/accounts/{self._account_id()}/orders", body={"order": spec}
        )

        create = data.get("orderCreateTransaction", {})
        fill = data.get("orderFillTransaction")
        cancel = data.get("orderCancelTransaction")
        broker_order_id = str(create.get("id") or "")

        if fill:
            return {
                "broker_order_id": broker_order_id,
                "status": "FILLED",
                "avg_price": float(fill.get("price") or 0),
                "filled_qty": abs(float(fill.get("units") or 0)),
            }
        if cancel:
            # Created then immediately cancelled broker-side (MARKET_HALTED,
            # INSUFFICIENT_MARGIN, …) — an honest rejection, not a resting order.
            raise ConnectionError(f"OANDA rejected: {cancel.get('reason') or 'cancelled at broker'}")
        return {"broker_order_id": broker_order_id, "status": "SUBMITTED", "avg_price": None}

    async def cancel_order(self, broker_order_id: str) -> dict:
        try:
            await self._request(
                "PUT", f"/accounts/{self._account_id()}/orders/{broker_order_id}/cancel"
            )
            return {"status": "CANCELLED"}
        except ConnectionError:
            return {"status": "CANCEL_FAILED"}

    async def get_order(self, broker_order_id: str) -> dict | None:
        """Broker-side view of one order in the fill-sync shape:
        {status, filled_qty, avg_price}. None when OANDA can't answer.

        GET /orders/{id} only ever returns an order still in the pending
        book (state PENDING/TRIGGERED) -- confirmed live (2026-07-21): once
        an order fills or is cancelled broker-side, this endpoint returns
        "The order ID specified does not exist" forever after, which this
        method used to treat as an unconditional None -- meaning a resting
        order's fill or a broker-side cancel could never be detected by
        oanda_fill_sync's poll, no matter how many passes ran. Confirmed via
        three real orders on the practice account: this endpoint answered
        correctly while genuinely pending, then 100% consistently "doesn't
        exist" the moment each one left that state (2 fills, 1 cancel, same
        error every time) -- not a flaky/occasional failure.
        """
        account = self._account_id()
        try:
            data = await self._request("GET", f"/accounts/{account}/orders/{broker_order_id}")
        except ConnectionError as e:
            if "does not exist" in str(e).lower():
                return await self._get_order_terminal_state(account, broker_order_id)
            return None
        o = data.get("order", {})
        state = str(o.get("state") or "").upper()
        # v20 states → app vocabulary. TRIGGERED = a stop/limit whose trigger
        # hit and is now working — still open from the app's point of view.
        status = {
            "PENDING": "SUBMITTED",
            "TRIGGERED": "SUBMITTED",
            "FILLED": "FILLED",
            "CANCELLED": "CANCELLED",
        }.get(state, "UNKNOWN")

        filled_qty = 0.0
        avg_price = 0.0
        if status == "FILLED":
            filled_qty = abs(float(o.get("units") or 0))
            filling_id = o.get("fillingTransactionID")
            if filling_id:
                try:
                    tx = await self._request(
                        "GET", f"/accounts/{account}/transactions/{filling_id}"
                    )
                    t = tx.get("transaction", {})
                    avg_price = float(t.get("price") or 0)
                    filled_qty = abs(float(t.get("units") or filled_qty))
                except ConnectionError:
                    pass  # keep qty; sync backs the price out or retries
        return {"status": status, "filled_qty": filled_qty, "avg_price": avg_price}

    async def _get_order_terminal_state(self, account: str, broker_order_id: str) -> dict | None:
        """Looks up a no-longer-pending order's outcome via the transactions
        feed instead of the (empty, for terminal orders) single-order
        endpoint. An order's own broker_order_id IS its creation
        transaction's ID (see submit_order), and v20 transaction IDs are
        strictly increasing, so any ORDER_FILL/ORDER_CANCEL for this order
        is guaranteed to have a higher ID -- sinceid=broker_order_id is an
        exact, correct lower bound, not a heuristic. Verified live against
        three real orders (2 fills, 1 cancel): each showed up as the very
        next transaction after its own creation, orderID-matched correctly.
        """
        try:
            data = await self._request(
                "GET", f"/accounts/{account}/transactions/sinceid?id={broker_order_id}"
            )
        except ConnectionError:
            return None
        for tx in data.get("transactions", []):
            if str(tx.get("orderID") or "") != str(broker_order_id):
                continue
            tx_type = tx.get("type")
            if tx_type == "ORDER_FILL":
                return {
                    "status": "FILLED",
                    "filled_qty": abs(float(tx.get("units") or 0)),
                    "avg_price": float(tx.get("price") or 0),
                }
            if tx_type == "ORDER_CANCEL":
                return {"status": "CANCELLED", "filled_qty": 0.0, "avg_price": 0.0}
        # Not resolved in this window yet (e.g. OANDA hasn't recorded the
        # terminal transaction the instant "does not exist" starts firing) --
        # None, same as any other "couldn't determine state" case; retried
        # next pass.
        return None

    async def get_positions(self) -> list[dict]:
        data = await self._request("GET", f"/accounts/{self._account_id()}/openPositions")
        out = []
        for p in data.get("positions", []):
            for side_key, side in (("long", "LONG"), ("short", "SHORT")):
                leg = p.get(side_key) or {}
                units = float(leg.get("units") or 0)
                if units:
                    out.append({
                        "symbol": p.get("instrument", "").replace("_", "/"),
                        "qty": abs(units),
                        "side": side,
                        "avg_price": float(leg.get("averagePrice") or 0),
                        "unrealized_pl": float(leg.get("unrealizedPL") or 0),
                    })
        return out

    async def get_fills(self, since: datetime | None = None) -> list[dict]:
        # MARKET fills arrive inline on submit; resting LIMIT/STOP fills are
        # reconciled by oanda_fill_sync (get_order poll) — nothing consumes a
        # raw fill list here.
        return []


# ── IBKR Adapter ──────────────────────────────────────────────────────────────

class IBKRAdapter(BrokerAdapter):
    """
    Interactive Brokers execution via the TWS/IB Gateway API (`ib_async`).

    SCAFFOLDED, NOT LIVE-VERIFIED: no funded/paper IBKR account or a running
    TWS/IB Gateway instance exists in this environment (an access gap, not a
    code gap — see final_implementation.md §3.2 and Final_development.md §1).
    Every method below is written against ib_async's documented API and this
    adapter's own internal logic was exercised directly (contract/order
    construction, connection lifecycle) with a stub in place of a real TWS
    connection, but no live order has ever round-tripped a real account.
    Treat this the same way this codebase already treats the MT5 EA bridge
    before its first live fill: real code, unverified against the real thing.

    Why TWS/IB Gateway rather than a raw FIX 4.2/4.4 drop: IBKR's FIX
    interface (the guide's Chapter 5 description) is a separate institutional
    arrangement requiring its own agreement, order-flow minimums, and a
    dedicated network connection — not something obtainable in a dev
    environment. The TWS API is IBKR's own documented retail/prosumer
    integration path and is functionally equivalent for this app's purposes
    (order submission, positions, fills) — the same category of pragmatic
    substitute this codebase already uses elsewhere (e.g. the MT5 EA bridge
    standing in for a native MT5 FIX/API connection).

    Credentials (BrokerCredentials): host (default 127.0.0.1), port
    (paper TWS 7497 / live TWS 7496 / paper Gateway 4002 / live Gateway 4001
    — IBKR's own convention, not this app's choice), client_id (any int not
    already in use on that TWS/Gateway instance). There is no username/
    password at the API layer — TWS/Gateway itself is authenticated
    separately (a person logs into the desktop app), and the API socket
    trusts whatever's already logged in there.
    """

    _PAPER_PORT = 7497
    _LIVE_PORT  = 7496

    def _host(self) -> str:
        return self.creds.get("host") or "127.0.0.1"

    def _port(self) -> int:
        port = self.creds.get("port")
        if port:
            return int(port)
        return self._PAPER_PORT if self.broker.is_paper else self._LIVE_PORT

    def _client_id(self) -> int:
        # Random by default (not 0) so concurrent adapter calls from
        # different requests don't collide on the same clientId — TWS/
        # Gateway rejects a second simultaneous connection under one ID.
        cid = self.creds.get("client_id")
        if cid is not None:
            return int(cid)
        import random
        return random.randint(1000, 999_999)

    async def _connect(self):
        from ib_async import IB
        ib = IB()
        await ib.connectAsync(self._host(), self._port(), clientId=self._client_id(), timeout=10)
        return ib

    @staticmethod
    def _contract(symbol: str):
        """
        App symbol -> ib_async Contract. IBKR's practical retail/prosumer
        surface (matching what this adapter targets) is equities and FX —
        futures/options exist in the guide but need contract-month/strike
        metadata this app's Symbol model doesn't carry, so they're not
        attempted here. A slash-quoted pair where both sides look like
        3-letter currency codes routes to Forex; anything else routes to
        Stock (SMART routing, USD) — crypto has no real IBKR product for
        the pairs this app trades, so it's rejected rather than guessed at.
        """
        from ib_async import Forex, Stock
        s = symbol.strip().upper()
        if "/" in s:
            base, quote = s.split("/", 1)
            if len(base) == 3 and len(quote) == 3 and base.isalpha() and quote.isalpha():
                return Forex(f"{base}{quote}")
            raise ConnectionError(
                f"IBKR adapter has no product mapping for {symbol!r} — only "
                f"equities and 3+3-letter FX pairs are supported."
            )
        return Stock(s, "SMART", "USD")

    @staticmethod
    def _ib_order(order: Order):
        from ib_async import LimitOrder, MarketOrder, StopOrder
        action = "BUY" if order.side == "BUY" else "SELL"
        qty = float(order.qty)
        if order.order_type == "MARKET":
            return MarketOrder(action, qty)
        if order.order_type == "LIMIT":
            if not order.price:
                raise ConnectionError("LIMIT order without a price")
            return LimitOrder(action, qty, float(order.price))
        if order.order_type == "STOP":
            trigger = float(order.stop_price or order.price or 0)
            if not trigger:
                raise ConnectionError("STOP order without a trigger price")
            return StopOrder(action, qty, trigger)
        raise ConnectionError(f"Order type {order.order_type} not supported on IBKR")

    async def test_connection(self) -> BrokerTestResult:
        ib = None
        try:
            t0 = time.perf_counter()
            ib = await self._connect()
            summary = await ib.accountSummaryAsync()
            latency = (time.perf_counter() - t0) * 1000
            values = {row.tag: row.value for row in summary}
            return BrokerTestResult(
                success=True,
                latency_ms=round(latency, 2),
                message="Connected",
                account_info={
                    "net_liquidation": float(values.get("NetLiquidation") or 0),
                    "buying_power": float(values.get("BuyingPower") or 0),
                    "currency": values.get("Currency"),
                },
            )
        except Exception as e:
            return BrokerTestResult(success=False, latency_ms=None, message=str(e))
        finally:
            if ib is not None and ib.isConnected():
                ib.disconnect()

    async def get_account(self) -> dict:
        ib = await self._connect()
        try:
            summary = await ib.accountSummaryAsync()
            values = {row.tag: row.value for row in summary}
            return {
                "buying_power": float(values.get("BuyingPower") or 0),
                "equity": float(values.get("NetLiquidation") or 0),
                "currency": values.get("Currency"),
            }
        finally:
            if ib.isConnected():
                ib.disconnect()

    async def submit_order(self, order: Order) -> dict:
        ib = await self._connect()
        try:
            contract = self._contract(order.symbol.symbol)
            await ib.qualifyContractsAsync(contract)
            ib_order = self._ib_order(order)
            trade = ib.placeOrder(contract, ib_order)

            # ib_async updates `trade` in place as TWS/Gateway sends events —
            # poll briefly toward a terminal-or-acknowledged state rather
            # than assuming an immediate fill (mirrors AlpacaAdapter's own
            # short poll for the same reason: a MARKET order at a real
            # exchange still takes a moment to print).
            for _ in range(10):
                status = trade.orderStatus.status
                if status == "Filled":
                    return {
                        "broker_order_id": str(trade.order.orderId),
                        "status": "FILLED",
                        "avg_price": float(trade.orderStatus.avgFillPrice or 0),
                        "filled_qty": float(trade.orderStatus.filled or order.qty),
                    }
                if status in ("Cancelled", "ApiCancelled", "Inactive"):
                    raise ConnectionError(f"IBKR rejected/cancelled order: status={status}")
                await asyncio.sleep(0.5)

            return {
                "broker_order_id": str(trade.order.orderId),
                "status": "SUBMITTED",
                "avg_price": None,
            }
        finally:
            if ib.isConnected():
                ib.disconnect()

    async def cancel_order(self, broker_order_id: str) -> dict:
        ib = await self._connect()
        try:
            target_id = int(broker_order_id)
            for trade in ib.openTrades():
                if trade.order.orderId == target_id:
                    ib.cancelOrder(trade.order)
                    return {"status": "CANCELLED"}
            return {"status": "CANCEL_FAILED"}
        except Exception:
            return {"status": "CANCEL_FAILED"}
        finally:
            if ib.isConnected():
                ib.disconnect()

    async def get_positions(self) -> list[dict]:
        ib = await self._connect()
        try:
            out = []
            for p in ib.positions():
                qty = float(p.position)
                if qty == 0:
                    continue
                sym = p.contract.pair() if hasattr(p.contract, "pair") else p.contract.symbol
                out.append({
                    "symbol": sym,
                    "qty": abs(qty),
                    "side": "LONG" if qty > 0 else "SHORT",
                    "avg_price": float(p.avgCost or 0),
                })
            return out
        finally:
            if ib.isConnected():
                ib.disconnect()

    async def get_fills(self, since: datetime | None = None) -> list[dict]:
        ib = await self._connect()
        try:
            out = []
            for f in ib.fills():
                if since and f.time and f.time < since:
                    continue
                out.append({
                    "broker_order_id": str(f.execution.orderId),
                    "price": float(f.execution.price),
                    "qty": float(f.execution.shares),
                    "side": f.execution.side,
                    "time": f.time.isoformat() if f.time else None,
                })
            return out
        finally:
            if ib.isConnected():
                ib.disconnect()


# ── Tradovate Adapter ─────────────────────────────────────────────────────────

class TradovateAdapter(BrokerAdapter):
    """
    Tradovate futures execution over their REST API.

    SCAFFOLDED, NOT LIVE-VERIFIED: no Tradovate account or API credentials
    exist in this environment (access gap, not a code gap — same status as
    IBKRAdapter above). Written directly against Tradovate's documented v1
    REST API (api.tradovate.com / demo.tradovateapi.com), same raw-aiohttp
    pattern as OandaAdapter, but the request/response shapes below have not
    been exercised against a real Tradovate session.

    Credential mapping (BrokerCredentials only has generic fields — Tradovate
    needs five distinct values, so existing fields are repurposed rather than
    growing the schema for one broker):
      api_key    -> Tradovate username
      api_secret -> Tradovate password
      client_id  -> Tradovate's numeric API "cid" (issued per application)
      passphrase -> Tradovate's API "sec" (client secret string)
      account_id -> Tradovate trading account ID (numeric, required on every order)

    Symbol handling: Tradovate trades futures contracts (e.g. "ESZ6"), which
    carry contract-month/rollover metadata this app's Symbol model doesn't
    have — order.symbol.symbol is forwarded to Tradovate as-is rather than
    inventing a contract-month resolver. Whoever wires this up for real needs
    Symbol rows that already carry a valid Tradovate contract code.
    """

    _DEMO_BASE = "https://demo.tradovateapi.com/v1"
    _LIVE_BASE = "https://live.tradovateapi.com/v1"

    def _base(self) -> str:
        return self._DEMO_BASE if self.broker.is_paper else self._LIVE_BASE

    def _account_id(self) -> str:
        account_id = self.creds.get("account_id")
        if not account_id:
            raise ConnectionError(
                "Tradovate credentials missing account_id (set it on the "
                "broker in Connections)."
            )
        return str(account_id)

    async def _authenticate(self) -> str:
        """
        Returns a fresh access token. No token caching across calls — this
        adapter re-authenticates on every request, same simplicity trade-off
        OandaAdapter makes with per-call sessions, appropriate for a
        never-yet-exercised scaffold rather than a persistent-session design
        that can't be verified against a real account anyway.
        """
        import aiohttp
        body = {
            "name": self.creds.get("api_key"),
            "password": self.creds.get("api_secret"),
            "appId": "PiOSQ",
            "appVersion": "1.0",
            "cid": self.creds.get("client_id"),
            "sec": self.creds.get("passphrase"),
            "deviceId": f"pios-{self.creds.get('client_id')}",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self._base()}/auth/accesstokenrequest", json=body, timeout=15
            ) as response:
                data = await response.json(content_type=None)
                if response.status >= 400 or not data.get("accessToken"):
                    raise ConnectionError(
                        f"Tradovate auth rejected: {data.get('errorText') or data}"
                    )
                return data["accessToken"]

    async def _request(self, method: str, path: str, body: dict | None = None) -> Any:
        import aiohttp
        token = await self._authenticate()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method, f"{self._base()}{path}", headers=headers, json=body, timeout=15
            ) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    detail = (data or {}).get("errorText") or f"HTTP {response.status}"
                    raise ConnectionError(f"Tradovate rejected: {detail}")
                return data

    async def test_connection(self) -> BrokerTestResult:
        try:
            t0 = time.perf_counter()
            token = await self._authenticate()
            latency = (time.perf_counter() - t0) * 1000
            return BrokerTestResult(
                success=bool(token),
                latency_ms=round(latency, 2),
                message="Connected",
                account_info={"account_id": self.creds.get("account_id")},
            )
        except Exception as e:
            return BrokerTestResult(success=False, latency_ms=None, message=str(e))

    async def get_account(self) -> dict:
        cash = await self._request("GET", f"/cashBalance/getcashbalancesnapshot?accountId={self._account_id()}")
        return {
            "buying_power": float((cash or {}).get("amount") or 0),
            "equity": float((cash or {}).get("amount") or 0),
            "currency": (cash or {}).get("currency"),
        }

    async def submit_order(self, order: Order) -> dict:
        action = "Buy" if order.side == "BUY" else "Sell"
        order_type_map = {"MARKET": "Market", "LIMIT": "Limit", "STOP": "Stop"}
        tv_type = order_type_map.get(order.order_type)
        if not tv_type:
            raise ConnectionError(f"Order type {order.order_type} not supported on Tradovate")

        body: dict[str, Any] = {
            "accountId": int(self._account_id()),
            "action": action,
            "symbol": order.symbol.symbol,
            "orderQty": float(order.qty),
            "orderType": tv_type,
            "isAutomated": True,
        }
        if order.order_type == "LIMIT":
            if not order.price:
                raise ConnectionError("LIMIT order without a price")
            body["price"] = float(order.price)
        elif order.order_type == "STOP":
            trigger = float(order.stop_price or order.price or 0)
            if not trigger:
                raise ConnectionError("STOP order without a trigger price")
            body["stopPrice"] = trigger

        data = await self._request("POST", "/order/placeorder", body=body)
        order_id = str((data or {}).get("orderId") or "")
        failure_reason = (data or {}).get("failureReason")
        if failure_reason:
            raise ConnectionError(f"Tradovate rejected: {failure_reason}")
        # Tradovate's placeorder response doesn't inline a fill — status is
        # read back via order/list same as a resting order elsewhere in this
        # app; no fill-sync loop exists for Tradovate yet (would mirror
        # oanda_fill_sync.py's poll pattern once this adapter is live-tested).
        return {"broker_order_id": order_id, "status": "SUBMITTED", "avg_price": None}

    async def cancel_order(self, broker_order_id: str) -> dict:
        try:
            await self._request("POST", "/order/cancelorder", body={"orderId": int(broker_order_id)})
            return {"status": "CANCELLED"}
        except ConnectionError:
            return {"status": "CANCEL_FAILED"}

    async def get_positions(self) -> list[dict]:
        data = await self._request("GET", "/position/list")
        out = []
        for p in data or []:
            if str(p.get("accountId")) != self._account_id():
                continue
            qty = float(p.get("netPos") or 0)
            if qty == 0:
                continue
            out.append({
                "symbol": str(p.get("contractId")),
                "qty": abs(qty),
                "side": "LONG" if qty > 0 else "SHORT",
                "avg_price": float(p.get("netPrice") or 0),
            })
        return out

    async def get_fills(self, since: datetime | None = None) -> list[dict]:
        data = await self._request("GET", "/fill/list")
        out = []
        for f in data or []:
            if str(f.get("accountId")) != self._account_id():
                continue
            out.append({
                "broker_order_id": str(f.get("orderId")),
                "price": float(f.get("price") or 0),
                "qty": float(f.get("qty") or 0),
                "time": f.get("timestamp"),
            })
        return out


# ── LMAX Adapter (FIX 4.4) ─────────────────────────────────────────────────────

class LMAXAdapter(BrokerAdapter):
    """
    LMAX Exchange execution over a raw FIX 4.4 session.

    SCAFFOLDED, NOT LIVE-VERIFIED, and the most speculative of the three
    access-gated adapters (see final_implementation.md §3.2,
    Final_development.md §1): LMAX only offers FIX access to institutional
    clients with a signed agreement and a dedicated network endpoint — there
    is no public sandbox to test message conventions against, unlike IBKR
    (a free paper account) or Tradovate (a free demo account). Every message
    below follows the generic FIX 4.4 session/order specification (message
    types, tag numbers, checksum framing via `simplefix`) precisely, but
    LMAX-specific conventions this adapter cannot verify without a real
    account — exact required tags beyond the FIX 4.4 baseline, symbol
    naming, session-reset behavior — may differ and would need adjusting
    against LMAX's own FIX specification document once real access exists.

    This implements a minimal FIX 4.4 session (TCP connect, Logon, sequence
    numbering, heartbeat/TestRequest response, NewOrderSingle,
    OrderCancelRequest, ExecutionReport parsing) rather than using a
    QuickFIX-style engine — QuickFIX's Python bindings require compiling a
    C++ extension, an unnecessary dependency for a session this simple and
    this unverified.

    Credential mapping (BrokerCredentials repurposed, same pattern as
    TradovateAdapter — LMAX's FIX auth needs more distinct identifiers than
    the schema's generic fields, so existing fields are reused rather than
    growing the schema for one broker):
      host, port -> FIX gateway address (LMAX-assigned, differs by
                    environment/region)
      api_key    -> SenderCompID (tag 49) — this app's identity to LMAX
      passphrase -> TargetCompID (tag 56) — LMAX's own identity, assigned
                    per client
      account_id -> Account (tag 1) — the LMAX trading account number
      api_secret -> Password (tag 554), sent in Logon only if provided —
                    some FIX gateways rely on SenderCompID/TargetCompID +
                    IP allowlisting instead and don't need it
    """

    _HEARTBEAT_INTERVAL = 30

    def __init__(self, broker: Broker, credentials: dict):
        super().__init__(broker, credentials)
        self._seq_num = 1

    def _next_seq(self) -> int:
        n = self._seq_num
        self._seq_num += 1
        return n

    def _header(self, msg, msg_type: str):
        msg.append_pair(8, "FIX.4.4", header=True)
        msg.append_pair(35, msg_type, header=True)
        msg.append_pair(49, self.creds.get("api_key"), header=True)     # SenderCompID
        msg.append_pair(56, self.creds.get("passphrase"), header=True)  # TargetCompID
        msg.append_utc_timestamp(52, header=True)                       # SendingTime
        msg.append_pair(34, self._next_seq(), header=True)              # MsgSeqNum

    async def _connect_and_logon(self):
        import simplefix
        host = self.creds.get("host")
        port = int(self.creds.get("port") or 0)
        if not (host and port):
            raise ConnectionError("LMAX credentials missing host/port for the FIX gateway.")

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=10
        )

        logon = simplefix.FixMessage()
        self._header(logon, "A")
        logon.append_pair(98, 0)                              # EncryptMethod: none
        logon.append_pair(108, self._HEARTBEAT_INTERVAL)       # HeartBtInt
        password = self.creds.get("api_secret")
        if password:
            logon.append_pair(554, password)                  # Password
        writer.write(logon.encode())
        await writer.drain()

        parser = simplefix.FixParser()
        raw = await asyncio.wait_for(reader.read(4096), timeout=10)
        parser.append_buffer(raw)
        response = parser.get_message()
        if response is None or response.get(35) != b"A":
            writer.close()
            raise ConnectionError(
                f"LMAX Logon rejected or no response "
                f"(got MsgType={response.get(35) if response else None})"
            )
        return reader, writer, parser

    @staticmethod
    def _side_tag(order: Order) -> str:
        return "1" if order.side == "BUY" else "2"  # FIX Side: 1=Buy, 2=Sell

    def _new_order_single(self, order: Order):
        import simplefix
        msg = simplefix.FixMessage()
        self._header(msg, "D")
        msg.append_pair(11, str(order.client_order_id or order.id))  # ClOrdID
        msg.append_pair(1, self.creds.get("account_id"))             # Account
        msg.append_pair(55, order.symbol.symbol)                     # Symbol
        msg.append_pair(54, self._side_tag(order))                   # Side
        msg.append_utc_timestamp(60)                                  # TransactTime
        msg.append_pair(38, float(order.qty))                         # OrderQty
        if order.order_type == "MARKET":
            msg.append_pair(40, "1")                                  # OrdType: Market
        elif order.order_type == "LIMIT":
            if not order.price:
                raise ConnectionError("LIMIT order without a price")
            msg.append_pair(40, "2")                                  # OrdType: Limit
            msg.append_pair(44, float(order.price))                   # Price
        elif order.order_type == "STOP":
            trigger = float(order.stop_price or order.price or 0)
            if not trigger:
                raise ConnectionError("STOP order without a trigger price")
            msg.append_pair(40, "3")                                  # OrdType: Stop
            msg.append_pair(99, trigger)                              # StopPx
        else:
            raise ConnectionError(f"Order type {order.order_type} not supported on LMAX")
        msg.append_pair(59, "0")                                      # TimeInForce: Day
        return msg

    @staticmethod
    def _parse_execution_report(msg) -> dict:
        """Maps FIX ExecutionReport (35=8) tags to this app's broker-result shape."""
        ord_status = (msg.get(39) or b"").decode()
        exec_type  = (msg.get(150) or b"").decode()
        status_map = {"0": "SUBMITTED", "1": "PARTIAL", "2": "FILLED",
                      "4": "CANCELLED", "8": "REJECTED"}
        status = status_map.get(ord_status, "SUBMITTED")
        return {
            "broker_order_id": (msg.get(37) or b"").decode(),   # OrderID
            "status": status,
            "avg_price": float(msg.get(6) or 0),                 # AvgPx
            "filled_qty": float(msg.get(14) or 0),               # CumQty
            "exec_type": exec_type,
            "reject_reason": (msg.get(58) or b"").decode() if status == "REJECTED" else None,
        }

    async def test_connection(self) -> BrokerTestResult:
        writer = None
        try:
            t0 = time.perf_counter()
            _, writer, _ = await self._connect_and_logon()
            latency = (time.perf_counter() - t0) * 1000
            return BrokerTestResult(
                success=True, latency_ms=round(latency, 2), message="FIX Logon accepted",
                account_info={"account": self.creds.get("account_id")},
            )
        except Exception as e:
            return BrokerTestResult(success=False, latency_ms=None, message=str(e))
        finally:
            if writer is not None:
                writer.close()

    async def get_account(self) -> dict:
        # LMAX's FIX drop is order/execution-only — account balance is a
        # separate (non-FIX) reporting API this adapter doesn't implement;
        # honestly report that rather than inventing a number.
        raise ConnectionError(
            "LMAX account balance is not available over FIX — LMAX exposes "
            "balances via a separate reporting API not implemented here."
        )

    async def submit_order(self, order: Order) -> dict:
        reader, writer, parser = await self._connect_and_logon()
        try:
            order_msg = self._new_order_single(order)
            writer.write(order_msg.encode())
            await writer.drain()

            # Wait briefly for an ExecutionReport (35=8); a resting order may
            # only send a NEW ack (39=0) rather than an immediate fill.
            for _ in range(10):
                raw = await asyncio.wait_for(reader.read(4096), timeout=2)
                parser.append_buffer(raw)
                msg = parser.get_message()
                if msg is None:
                    continue
                if msg.get(35) == b"8":  # ExecutionReport
                    return self._parse_execution_report(msg)
                if msg.get(35) == b"1":  # TestRequest -> must Heartbeat back
                    hb = __import__("simplefix").FixMessage()
                    self._header(hb, "0")
                    hb.append_pair(112, msg.get(112))
                    writer.write(hb.encode())
                    await writer.drain()
            raise ConnectionError("LMAX: no ExecutionReport received within timeout")
        finally:
            writer.close()

    async def cancel_order(self, broker_order_id: str) -> dict:
        import simplefix
        reader, writer, parser = await self._connect_and_logon()
        try:
            msg = simplefix.FixMessage()
            self._header(msg, "F")  # OrderCancelRequest
            msg.append_pair(41, broker_order_id)   # OrigClOrdID
            msg.append_pair(11, f"cancel-{broker_order_id}")  # ClOrdID
            msg.append_pair(1, self.creds.get("account_id"))
            writer.write(msg.encode())
            await writer.drain()
            for _ in range(10):
                raw = await asyncio.wait_for(reader.read(4096), timeout=2)
                parser.append_buffer(raw)
                resp = parser.get_message()
                if resp is not None and resp.get(35) in (b"8", b"9"):
                    return {"status": "CANCELLED" if resp.get(35) == b"8" else "CANCEL_FAILED"}
            return {"status": "CANCEL_FAILED"}
        except Exception:
            return {"status": "CANCEL_FAILED"}
        finally:
            writer.close()

    async def get_positions(self) -> list[dict]:
        # Position reporting over FIX (RequestForPositions, 35=AN) needs a
        # longer-lived session and multi-message reassembly this scaffold's
        # per-call connect/disconnect model doesn't support yet — honestly
        # unimplemented rather than approximated.
        raise ConnectionError(
            "LMAX position reporting over FIX is not implemented in this "
            "scaffold — needs RequestForPositions (35=AN) session support."
        )

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
        price = float(order.price or 0)
        if price <= 0:
            # MARKET order with no limit price -- fill at the live ticker, never
            # at 0 (a $0 fill poisons positions, P&L, and TCA downstream).
            from app.services.market_data_service import get_live_ticker
            sym = getattr(getattr(order, "symbol", None), "symbol", None)
            if sym:
                try:
                    ticker = await get_live_ticker(sym)
                    price = float(ticker.get("last") or ticker.get("bid") or ticker.get("ask") or 0)
                except Exception:
                    price = 0.0
            if price <= 0:
                raise RuntimeError(
                    f"Paper broker: no live market price available for "
                    f"{sym or order.symbol_id} -- cannot simulate a market fill "
                    f"(retry with a limit price)"
                )
        return {"broker_order_id": str(_uuid.uuid4()), "status": "FILLED", "avg_price": price}

    async def cancel_order(self, broker_order_id: str) -> dict:
        return {"status": "CANCELLED"}

    async def get_positions(self) -> list[dict]:
        return []

    async def get_fills(self, since: datetime | None = None) -> list[dict]:
        return []


# ── Factory ───────────────────────────────────────────────────────────────────

# MT5Adapter duck-types BrokerAdapter rather than subclassing it -- it lives in
# app.services.brokers.mt5.adapter, which the MT5 EA WebSocket endpoint also
# needs to import (for the connection registry); subclassing here would create
# a broker_service <-> mt5.adapter import cycle.
ADAPTER_MAP = {
    "ALPACA": AlpacaAdapter,
    "BINANCE": CCXTAdapter,
    "CCXT": CCXTAdapter,
    "IBKR": IBKRAdapter,
    "LMAX": LMAXAdapter,
    "MT5": MT5Adapter,
    "OANDA": OandaAdapter,
    "PAPER": PaperAdapter,
    "TRADOVATE": TradovateAdapter,
}


class UnsupportedBrokerError(Exception):
    """
    Raised when a broker's type has no real adapter implementation (IBKR,
    LMAX, CUSTOM today) and the broker wasn't explicitly marked
    is_paper=True. Without this, get_adapter() used to silently return
    PaperAdapter for any unmapped type -- a user configuring a real "IBKR"
    connection with is_paper=False would unknowingly get fake instant fills.
    """
    def __init__(self, broker_type: str):
        self.broker_type = broker_type
        super().__init__(
            f"Broker type {broker_type!r} has no real adapter implementation yet "
            f"(supported: {sorted(k for k in ADAPTER_MAP if k != 'PAPER')}). "
            f"Set is_paper=true on this broker connection to use simulated fills "
            f"until a real adapter ships."
        )


def get_adapter(broker: Broker) -> BrokerAdapter:
    """
    Decrypt credentials and instantiate the correct adapter.

    Unmapped broker types (no real adapter) only fall back to PaperAdapter when
    the broker was explicitly created with is_paper=True -- otherwise this
    raises UnsupportedBrokerError rather than silently faking fills.
    """
    creds_json = decrypt_credentials(broker.credentials_enc)
    creds = json.loads(creds_json)

    broker_type = broker.broker_type.upper()
    adapter_cls = ADAPTER_MAP.get(broker_type)
    if adapter_cls is None:
        if not broker.is_paper:
            raise UnsupportedBrokerError(broker_type)
        adapter_cls = PaperAdapter  # explicitly acknowledged as simulated

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
    # Fail fast at creation time rather than only discovering at test/order time
    # that an unsupported broker type was silently going to fake-fill trades.
    broker_type = data.broker_type.upper()
    if broker_type not in ADAPTER_MAP and not data.is_paper:
        raise UnsupportedBrokerError(broker_type)

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
