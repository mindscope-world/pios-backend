# app/models/all_models.py
# Single-file model definitions — import this everywhere so
# Alembic discovers all tables and relationships resolve.
import uuid
from datetime import datetime, timezone
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import (
    JSON, Float, Index, String, Boolean, DateTime, Numeric, Text,
    Integer, SmallInteger, ForeignKey, Enum as SAEnum, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB, INET
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.db.session import Base

# ── Enums ─────────────────────────────────────────────────────────────────────

RoleEnum         = SAEnum("admin","trader","quant","viewer","compliance",            name="user_role")
BrokerTypeEnum   = SAEnum("MT5","ALPACA","BINANCE","IBKR","CCXT","OANDA","LMAX","CUSTOM", name="broker_type")
BrokerStatusEnum = SAEnum("CONNECTED","DISCONNECTED","ERROR","PENDING_AUTH",         name="broker_status")
AssetClassEnum   = AssetClassEnum = SAEnum("crypto", "equities", "forex", "futures", "options", "commodity", "bond", "metal", "index", name="asset_class")
OrderStatusEnum  = SAEnum("NEW","SUBMITTED","PARTIAL","FILLED","CANCELLED","REJECTED","EXPIRED", name="order_status")
OrderSideEnum    = SAEnum("BUY","SELL",                                              name="order_side")
OrderTypeEnum    = SAEnum("MARKET","LIMIT","STOP","STOP_LIMIT","OCO","TWAP","VWAP","ICEBERG", name="order_type")
TIFEnum          = SAEnum("GTC","IOC","FOK","DAY",                                   name="time_in_force")
PosSideEnum      = SAEnum("LONG","SHORT",                                            name="position_side")
StageEnum        = SAEnum("IDEA","RESEARCH","BACKTEST","PAPER","LIVE_SMALL","SCALED","MONITOR","RETIRED", name="lifecycle_stage")
BtStatusEnum     = SAEnum("QUEUED","RUNNING","COMPLETE","FAILED",                    name="backtest_status")
CostModelEnum    = SAEnum("FULL","ZERO_COST","FEES_ONLY","SLIPPAGE_ONLY",            name="cost_model")
BreachEnum       = SAEnum("ALERT","BLOCK","KILL_SWITCH",                             name="breach_action")
LimitScopeEnum   = SAEnum("global","per_strategy","per_symbol","per_user",           name="limit_scope")
SeverityEnum     = SAEnum("P1","P2","P3","P4",                                       name="alert_severity")
DQModuleEnum     = SAEnum("TICK_VALIDATOR","DUPLICATE_FILTER","TIMESTAMP_CORRECTOR","OUTLIER_DETECTOR","CONTINUITY_MONITOR", name="dq_module")
DQResultEnum     = SAEnum("PASS","FLAG","REJECT",                                    name="dq_result")
DQSevEnum        = SAEnum("INFO","WARN","ERROR","CRITICAL",                          name="dq_severity")

class OrderSide:
    BUY  = "BUY"
    SELL = "SELL"
    ALL  = [BUY, SELL]


class OrderType:
    MARKET     = "MARKET"
    LIMIT      = "LIMIT"
    STOP       = "STOP"
    STOP_LIMIT = "STOP_LIMIT"
    ALL = [MARKET, LIMIT, STOP, STOP_LIMIT]


class TIF:
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
    DAY = "DAY"
    ALL = [GTC, IOC, FOK, DAY]


class OrderStatus:
    NEW        = "NEW"        # Allocated by risk gate
    SUBMITTED  = "SUBMITTED"  # Sent to broker wire
    PARTIAL    = "PARTIAL"    # Partially filled
    FILLED     = "FILLED"     # Fully executed
    CANCELLED  = "CANCELLED"
    REJECTED   = "REJECTED"
    EXPIRED    = "EXPIRED"
    ALL = [NEW, SUBMITTED, PARTIAL, FILLED, CANCELLED, REJECTED, EXPIRED]

    # Statuses that count as "open" for risk gate checks
    OPEN = {NEW, SUBMITTED, PARTIAL}


# ── Order state machine ───────────────────────────────────────────────────────
# `None` is the pre-insert state (a freshly constructed Order whose `status`
# attribute hasn't been set yet) — order_service.py calls `.transition()`
# immediately after construction, before the row is ever flushed.
ALLOWED_ORDER_TRANSITIONS: dict[str | None, set[str]] = {
    None:                  {OrderStatus.NEW, OrderStatus.SUBMITTED, OrderStatus.REJECTED},
    OrderStatus.NEW:       {OrderStatus.SUBMITTED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED},
    OrderStatus.SUBMITTED: {OrderStatus.SUBMITTED, OrderStatus.PARTIAL, OrderStatus.FILLED,
                            OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED},
    OrderStatus.PARTIAL:   {OrderStatus.PARTIAL, OrderStatus.FILLED, OrderStatus.CANCELLED,
                            OrderStatus.REJECTED, OrderStatus.EXPIRED},
    # Terminal states — no further transitions permitted.
    OrderStatus.FILLED:    set(),
    OrderStatus.CANCELLED: set(),
    OrderStatus.REJECTED:  set(),
    OrderStatus.EXPIRED:   set(),
}


class InvalidTransitionError(Exception):
    """Raised by Order.transition() when the requested status change isn't permitted."""

    def __init__(self, current_status: str | None, new_status: str, order_id: "uuid.UUID | None" = None):
        self.current_status = current_status
        self.new_status = new_status
        self.order_id = order_id
        where = f"order {order_id} " if order_id else ""
        super().__init__(f"Cannot transition {where}from {current_status!r} to {new_status!r}")


class BrokerType:
    MT5  = "MT5"
    IBKR = "IBKR" 
    ALPACA = "ALPACA"
    OANDA  = "OANDA"
    CCXT = "CCXT"
    LMAX = "LMAX"
    CUSTOM = "CUSTOM"
    BINANCE = "BINANCE"
    ALL  = [MT5, IBKR, ALPACA, OANDA, CCXT, LMAX, CUSTOM, BINANCE]


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)



# ═══════════════════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════════════════

class User(Base):
    __tablename__ = "users"

    id:               Mapped[uuid.UUID]   = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email:            Mapped[str]         = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash:    Mapped[str]         = mapped_column(Text, nullable=False)
    full_name:        Mapped[str]         = mapped_column(String(255), nullable=False)
    role:             Mapped[str]         = mapped_column(RoleEnum, nullable=False, default="viewer")
    is_active:        Mapped[bool]        = mapped_column(Boolean, default=True)
    mfa_enabled:      Mapped[bool]        = mapped_column(Boolean, default=False)
    mfa_secret_enc:   Mapped[str | None]  = mapped_column(Text)           # AES encrypted
    last_login_at:    Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_login_ip:    Mapped[str | None]  = mapped_column(INET)
    failed_logins:    Mapped[int]         = mapped_column(SmallInteger, default=0)
    locked_until:     Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    preferences:      Mapped[dict]        = mapped_column(JSONB, default=dict)
    created_at:       Mapped[datetime]    = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at:       Mapped[datetime]    = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    sessions:    Mapped[list["UserSession"]]  = relationship(back_populates="user", cascade="all, delete-orphan")
    orders:      Mapped[list["Order"]]        = relationship(back_populates="user")
    strategies:  Mapped[list["Strategy"]]     = relationship(back_populates="created_by_user")
    brokers:     Mapped[list["Broker"]]       = relationship(back_populates="owner")

    @property
    def is_locked(self) -> bool:
        if not self.locked_until: return False
        return datetime.now(datetime.timezone.utc) < self.locked_until


class UserSession(Base):
    __tablename__ = "user_sessions"

    id:                  Mapped[uuid.UUID]  = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id:             Mapped[uuid.UUID]  = mapped_column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), index=True)
    refresh_token_hash:  Mapped[str]        = mapped_column(String(64), unique=True)
    device_info:         Mapped[dict | None]= mapped_column(JSONB)
    ip_address:          Mapped[str | None] = mapped_column(INET)
    expires_at:          Mapped[datetime]   = mapped_column(DateTime(timezone=True))
    revoked:             Mapped[bool]       = mapped_column(Boolean, default=False)
    created_at:          Mapped[datetime]   = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped["User"] = relationship(back_populates="sessions")


# ═══════════════════════════════════════════════════════════════════════════════
# BROKERS  (trader-owned connections)
# ═══════════════════════════════════════════════════════════════════════════════

class Broker(Base):
    __tablename__ = "brokers"

    id:               Mapped[uuid.UUID]   = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id:         Mapped[uuid.UUID]   = mapped_column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name:             Mapped[str]         = mapped_column(String(100), nullable=False)
    broker_type:      Mapped[str]         = mapped_column(BrokerTypeEnum, nullable=False)
    exchange_id:      Mapped[str | None]  = mapped_column(String(50))      # for CCXT: "binance" / "okx" …
    is_paper:         Mapped[bool]        = mapped_column(Boolean, default=True)
    is_active:        Mapped[bool]        = mapped_column(Boolean, default=True)
    credentials_enc:  Mapped[str]         = mapped_column(Text, nullable=False)   # AES-256 JSON blob
    config:           Mapped[dict]        = mapped_column(JSONB, default=dict)    # non-sensitive config
    status:           Mapped[str]         = mapped_column(BrokerStatusEnum, default="DISCONNECTED")
    last_heartbeat:   Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    latency_p99_ms:   Mapped[float | None]= mapped_column(Numeric(8, 2))
    error_message:    Mapped[str | None]  = mapped_column(Text)
    created_at:       Mapped[datetime]    = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at:       Mapped[datetime]    = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    owner:  Mapped["User"]        = relationship(back_populates="brokers")
    orders: Mapped[list["Order"]] = relationship(back_populates="broker")


# ═══════════════════════════════════════════════════════════════════════════════
# SYMBOLS
# ═══════════════════════════════════════════════════════════════════════════════

class Symbol(Base):
    __tablename__ = "symbols"

    id:          Mapped[int]          = mapped_column(primary_key=True, autoincrement=True)
    symbol:      Mapped[str]          = mapped_column(String(30), unique=True, nullable=False, index=True)
    base_asset:  Mapped[str]          = mapped_column(String(20), nullable=False)
    quote_asset: Mapped[str]          = mapped_column(String(20), nullable=False)
    asset_class: Mapped[str]          = mapped_column(AssetClassEnum, nullable=False)
    exchange:    Mapped[str]          = mapped_column(String(30), nullable=False)
    tick_size:   Mapped[float | None] = mapped_column(Numeric(20, 10))
    lot_size:    Mapped[float | None] = mapped_column(Numeric(20, 10))
    min_qty:     Mapped[float | None] = mapped_column(Numeric(20, 10))
    is_active:   Mapped[bool]         = mapped_column(Boolean, default=True)
    meta:        Mapped[dict]         = mapped_column(JSONB, default=dict)
    created_at:  Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now())


# ═══════════════════════════════════════════════════════════════════════════════
# ORDERS  +  FILLS
# ═══════════════════════════════════════════════════════════════════════════════

class PlaceOrderRequest(BaseModel):
    """Wire schema posted by React terminal → FastAPI."""

    # ── Identity ─────────────────────────────────────────────────────────────
    client_order_id: Optional[str] = Field(
        None,
        max_length=100,
        description="Idempotency key from UI — maps to Order.client_order_id",
    )

    broker_id: uuid.UUID = Field(
        ..., description="Which broker connection to use"
    )

    strategy_id: Optional[uuid.UUID] = None

    symbol_id: int = Field(
        ..., description="FK to symbols table"
    )

    symbol: str = Field(
        ..., description="Human-readable symbol, e.g. EURUSD"
    )

    # ── Order parameters ─────────────────────────────────────────────────────
    side: str = Field(
        ..., pattern="^(BUY|SELL)$"
    )

    order_type: str = Field(
        "MARKET",
        pattern="^(MARKET|LIMIT|STOP|STOP_LIMIT)$",
    )

    time_in_force: str = Field(
        "GTC",
        pattern="^(GTC|IOC|FOK|DAY)$",
    )

    qty: float = Field(
        ..., gt=0, description="Lot size or quantity"
    )

    price: Optional[float] = Field(
        None,
        description="Required for LIMIT/STOP orders",
    )

    stop_price: Optional[float] = Field(
        None,
        description="Stop trigger price",
    )

    # ── MT5 extras (runtime only → goes into Order.algo_config) ─────────────
    magic: int = Field(
        0, description="MT5 magic number"
    )

    comment: str = Field(
        "", max_length=64
    )

    # ── Validation ───────────────────────────────────────────────────────────
    @field_validator("price")
    @classmethod
    def price_required_for_limit(cls, v, info):
        order_type = info.data.get("order_type", "MARKET")

        if order_type in {"LIMIT", "STOP", "STOP_LIMIT"} and v is None:
            raise ValueError(f"price is required for {order_type} orders")

        return v

# ── Order ─────────────────────────────────────────────────────────────────────

class Order(Base):
    __tablename__ = "orders"

    # ── Primary key & relations ──────────────────────────────────────────────
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    client_order_id: Mapped[str | None] = mapped_column(
        String(100), nullable=True, index=True
    )

    broker_order_id: Mapped[str | None] = mapped_column(
        String(100), nullable=True
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"),  nullable=False, index=True
    )
    
    broker_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("brokers.id"), nullable=False, index=True
    )

    strategy_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("strategies.id"), nullable=True
    )

    symbol_id: Mapped[int] = mapped_column(
        ForeignKey("symbols.id"), nullable=False, index=True
    )

    # ── Order parameters ─────────────────────────────────────────────────────
    side: Mapped[str] = mapped_column(String(10), nullable=False)

    order_type: Mapped[str] = mapped_column(String(20), nullable=False)

    time_in_force: Mapped[str] = mapped_column(
        String(10), nullable=False, default="GTC"
    )

    qty: Mapped[float] = mapped_column(Float, nullable=False)

    filled_qty: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    price: Mapped[float | None] = mapped_column(Float, nullable=True)

    stop_price: Mapped[float | None] = mapped_column(Float, nullable=True)

    avg_fill_price: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ── Status & audit ───────────────────────────────────────────────────────
    status: Mapped[str] = mapped_column(String(20), nullable=False)

    state_history: Mapped[list[dict]] = mapped_column(
        JSON, nullable=False, default=list
    )

    algo_config: Mapped[dict | None] = mapped_column(
        JSON, nullable=True
    )

    reject_reason: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )

    risk_check: Mapped[dict | None] = mapped_column(
        JSON, nullable=True
    )

    # ── Timestamps ───────────────────────────────────────────────────────────
    submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    filled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    cancelled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # ── Relationships ────────────────────────────────────────────────────────
    fills: Mapped[list["Fill"]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan"
    )
    events: Mapped[list["OrderEvent"]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
    )
    user: Mapped["User"] = relationship(back_populates="orders")
    broker: Mapped["Broker"] = relationship(back_populates="orders")
    strategy: Mapped["Strategy"] = relationship(back_populates="orders")
    symbol: Mapped["Symbol"] = relationship()

    # ── State machine ────────────────────────────────────────────────────────
    def transition(self, new_status: str, reason: str | None = None) -> "OrderEvent":
        """
        Move this order to `new_status`.

        Validates against ALLOWED_ORDER_TRANSITIONS, records the change in
        `state_history`, and appends a cascaded OrderEvent (persisted once this
        Order is added to a session and flushed — works standalone in-memory
        too, which is what tests/test_order_transitions.py relies on).

        Raises InvalidTransitionError if the move isn't allowed from the
        current status (e.g. cancelling an already-FILLED order).
        """
        current = self.status
        allowed = ALLOWED_ORDER_TRANSITIONS.get(current, set())
        if new_status not in allowed:
            raise InvalidTransitionError(current, new_status, order_id=self.id)

        self.status = new_status
        self.state_history = [
            *(self.state_history or []),
            {"from": current, "to": new_status, "reason": reason, "at": _now().isoformat()},
        ]
        event = OrderEvent(event_type=new_status)
        self.events.append(event)
        return event

# ── WebSocket fan-out event ───────────────────────────────────────────────────

class OrderEvent(Base):
    __tablename__ = "order_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    event_type: Mapped[str] = mapped_column(
        String(20), nullable=False
    )

    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("orders.id"),
        nullable=False,
        index=True,
    )

    order: Mapped["Order"] = relationship(back_populates="events")

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default="now()",
        nullable=False,
    )


# ── Broker execution result (returned by adapters) ───────────────────────────

class ExecutionResult(Base):
    __tablename__ = "execution_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    success: Mapped[bool] = mapped_column(nullable=False)

    broker_order_id: Mapped[str | None] = mapped_column(
        String(100), nullable=True
    )

    avg_fill_price: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )

    filled_qty: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )

    commission: Mapped[float | None] = mapped_column(
        Float, nullable=True, default=0.0
    )

    error_code: Mapped[int | None] = mapped_column(nullable=True)

    error_message: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )

    raw: Mapped[Any | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default="now()",
        nullable=False,
    )


class Fill(Base):
    __tablename__ = "fills"

    id:             Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id:       Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), ForeignKey("orders.id"), nullable=False, index=True)
    broker_fill_id: Mapped[str | None]   = mapped_column(String(100))
    symbol_id:      Mapped[int]          = mapped_column(ForeignKey("symbols.id"), nullable=False)
    side:           Mapped[str]          = mapped_column(OrderSideEnum, nullable=False)
    qty:            Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    price:          Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    commission:     Mapped[float]        = mapped_column(Numeric(20, 8), default=0)
    slippage_bps:   Mapped[float | None] = mapped_column(Numeric(10, 4))
    spread_cost_bps:Mapped[float | None] = mapped_column(Numeric(10, 4))
    funding_cost:   Mapped[float]        = mapped_column(Numeric(20, 8), default=0)
    total_cost:     Mapped[float]        = mapped_column(Numeric(20, 8), default=0)
    filled_at:      Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at:     Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now())

    order:  Mapped["Order"]  = relationship(back_populates="fills")
    symbol: Mapped["Symbol"] = relationship()


# ═══════════════════════════════════════════════════════════════════════════════
# POSITIONS  +  PNL
# ═══════════════════════════════════════════════════════════════════════════════

class Position(Base):
    __tablename__ = "positions"

    id:             Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id:        Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    broker_id:      Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), ForeignKey("brokers.id"), nullable=False)
    strategy_id:    Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("strategies.id"))
    symbol_id:      Mapped[int]          = mapped_column(ForeignKey("symbols.id"), nullable=False, index=True)
    side:           Mapped[str]          = mapped_column(PosSideEnum, nullable=False)
    qty:            Mapped[float]        = mapped_column(Numeric(20, 8), default=0)
    avg_cost:       Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    realized_pnl:   Mapped[float]        = mapped_column(Numeric(20, 8), default=0)
    unrealized_pnl: Mapped[float]        = mapped_column(Numeric(20, 8), default=0)
    margin_used:    Mapped[float]        = mapped_column(Numeric(20, 8), default=0)
    is_open:        Mapped[bool]         = mapped_column(Boolean, default=True, index=True)
    opened_at:      Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now())
    closed_at:      Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at:     Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    symbol: Mapped["Symbol"] = relationship()
    broker: Mapped["Broker"] = relationship()


class PnLSnapshot(Base):
    __tablename__ = "pnl_snapshots"

    id:             Mapped[int]          = mapped_column(primary_key=True, autoincrement=True)
    user_id:        Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    strategy_id:    Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("strategies.id"))
    total_equity:   Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    realized_pnl:   Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    unrealized_pnl: Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    cash_balance:   Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    drawdown_pct:   Mapped[float | None] = mapped_column(Numeric(8, 4))
    snapshot_at:    Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGIES  +  BACKTESTS
# ═══════════════════════════════════════════════════════════════════════════════

class Strategy(Base):
    __tablename__ = "strategies"

    id:                  Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name:                Mapped[str]          = mapped_column(String(200), nullable=False)
    version:             Mapped[str]          = mapped_column(String(20), default="0.1.0")
    parent_id:           Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("strategies.id"))
    generation:          Mapped[int]          = mapped_column(Integer, default=0)
    created_by:          Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    lifecycle_stage:     Mapped[str]          = mapped_column(StageEnum, default="IDEA", index=True)
    hypothesis:          Mapped[str | None]   = mapped_column(Text)
    description:         Mapped[str | None]   = mapped_column(Text)
    feature_list:        Mapped[list | None]  = mapped_column(JSONB)
    allowed_symbols:     Mapped[list | None]  = mapped_column(JSONB)
    allowed_regimes:     Mapped[list | None]  = mapped_column(JSONB)
    risk_profile:        Mapped[dict]         = mapped_column(JSONB, default=dict)
    config:              Mapped[dict]         = mapped_column(JSONB, default=dict)
    code_hash:           Mapped[str | None]   = mapped_column(String(64))
    training_data_hash:  Mapped[str | None]   = mapped_column(String(64))
    fitness_score:       Mapped[float | None] = mapped_column(Numeric(8, 4))
    sharpe_last:         Mapped[float | None] = mapped_column(Numeric(8, 4))
    gate_history:        Mapped[list]         = mapped_column(JSONB, default=list)
    mutation_log:        Mapped[list]         = mapped_column(JSONB, default=list)
    audit_hash:          Mapped[str | None]   = mapped_column(String(64))
    is_paper_only:       Mapped[bool]         = mapped_column(Boolean, default=True)
    tags:                Mapped[list | None]  = mapped_column(JSONB)
    deployed_at:         Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retired_at:          Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retirement_reason:   Mapped[str | None]   = mapped_column(String(100))
    created_at:          Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at:          Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    created_by_user: Mapped["User"]              = relationship(back_populates="strategies")
    backtest_jobs:   Mapped[list["BacktestJob"]] = relationship(back_populates="strategy", cascade="all, delete-orphan")
    orders:          Mapped[list["Order"]]       = relationship(back_populates="strategy")
    parent:          Mapped["Strategy | None"]   = relationship(remote_side="Strategy.id")


class BacktestJob(Base):
    __tablename__ = "backtest_jobs"

    id:             Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    strategy_id:    Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), ForeignKey("strategies.id"), nullable=False, index=True)
    submitted_by:   Mapped[uuid.UUID]    = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    status:         Mapped[str]          = mapped_column(BtStatusEnum, default="QUEUED", index=True)
    progress_pct:   Mapped[int]          = mapped_column(Integer, default=0)
    celery_task_id: Mapped[str | None]   = mapped_column(String(100))
    start_date:     Mapped[str]          = mapped_column(String(10), nullable=False)
    end_date:       Mapped[str]          = mapped_column(String(10), nullable=False)
    symbols:        Mapped[list]         = mapped_column(JSONB, nullable=False)
    cost_model:     Mapped[str]          = mapped_column(CostModelEnum, default="FULL")
    config:         Mapped[dict]         = mapped_column(JSONB, default=dict)
    sharpe_ratio:   Mapped[float | None] = mapped_column(Numeric(8, 4))
    max_drawdown:   Mapped[float | None] = mapped_column(Numeric(8, 4))
    total_return:   Mapped[float | None] = mapped_column(Numeric(10, 4))
    trade_count:    Mapped[int | None]   = mapped_column(Integer)
    win_rate:       Mapped[float | None] = mapped_column(Numeric(6, 4))
    profit_factor:  Mapped[float | None] = mapped_column(Numeric(8, 4))
    full_report:    Mapped[dict | None]  = mapped_column(JSONB)
    equity_curve:   Mapped[list | None]  = mapped_column(JSONB)
    error_message:  Mapped[str | None]   = mapped_column(Text)
    started_at:     Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at:   Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at:     Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now())

    strategy: Mapped["Strategy"] = relationship(back_populates="backtest_jobs")


# ═══════════════════════════════════════════════════════════════════════════════
# RISK
# ═══════════════════════════════════════════════════════════════════════════════

class RiskLimit(Base):
    __tablename__ = "risk_limits"

    id:            Mapped[int]          = mapped_column(primary_key=True, autoincrement=True)
    name:          Mapped[str]          = mapped_column(String(100), nullable=False)
    scope:         Mapped[str]          = mapped_column(LimitScopeEnum, nullable=False)
    scope_id:      Mapped[str | None]   = mapped_column(String(50))
    limit_type:    Mapped[str]          = mapped_column(String(50), nullable=False)
    limit_value:   Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    current_value: Mapped[float | None] = mapped_column(Numeric(20, 8))
    breach_action: Mapped[str]          = mapped_column(BreachEnum, default="ALERT")
    is_active:     Mapped[bool]         = mapped_column(Boolean, default=True)
    created_by:    Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"))
    created_at:    Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at:    Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class ClockWeightBand(Base):
    """V10.4 D.2 -- admin-tunable min/max exposure band (% of total equity)
    per AlphaClock x regime. Read by clock_bands.constrain() to clamp the
    real-time per-clock exposure computed in capital_service.py."""
    __tablename__ = "clock_weight_bands"

    id:         Mapped[int]          = mapped_column(primary_key=True, autoincrement=True)
    clock:      Mapped[str]          = mapped_column(String(20), nullable=False)
    regime:     Mapped[str]          = mapped_column(String(30), nullable=False)
    min_pct:    Mapped[float]        = mapped_column(Numeric(6, 3), nullable=False)
    max_pct:    Mapped[float]        = mapped_column(Numeric(6, 3), nullable=False)
    is_active:  Mapped[bool]         = mapped_column(Boolean, default=True)
    created_by: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"))
    created_at: Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class KillSwitchEvent(Base):
    __tablename__ = "kill_switch_events"

    id:               Mapped[uuid.UUID]   = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    triggered_by:     Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"))
    trigger_source:   Mapped[str]         = mapped_column(String(30), nullable=False)
    reason:           Mapped[str]         = mapped_column(Text, nullable=False)
    orders_cancelled: Mapped[int]         = mapped_column(Integer, default=0)
    positions_closed: Mapped[int]         = mapped_column(Integer, default=0)
    status:           Mapped[str]         = mapped_column(String(20), default="TRIGGERED")
    completed_at:     Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_detail:     Mapped[str | None]  = mapped_column(Text)
    created_at:       Mapped[datetime]    = mapped_column(DateTime(timezone=True), server_default=func.now())


# ═══════════════════════════════════════════════════════════════════════════════
# ALERTS  +  AUDIT
# ═══════════════════════════════════════════════════════════════════════════════

class Alert(Base):
    __tablename__ = "alerts"

    id:               Mapped[uuid.UUID]   = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    severity:         Mapped[str]         = mapped_column(SeverityEnum, nullable=False, index=True)
    source:           Mapped[str]         = mapped_column(String(50), nullable=False)
    category:         Mapped[str]         = mapped_column(String(50), nullable=False)
    title:            Mapped[str]         = mapped_column(String(200), nullable=False)
    message:          Mapped[str]         = mapped_column(Text, nullable=False)
    strategy_id:      Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("strategies.id"))
    symbol_id:        Mapped[int | None]  = mapped_column(ForeignKey("symbols.id"))
    meta:             Mapped[dict]        = mapped_column(JSONB, default=dict)
    is_acknowledged:  Mapped[bool]        = mapped_column(Boolean, default=False, index=True)
    acknowledged_by:  Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"))
    acknowledged_at:  Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ack_note:         Mapped[str | None]  = mapped_column(Text)
    auto_resolved:    Mapped[bool]        = mapped_column(Boolean, default=False)
    resolved_at:      Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at:       Mapped[datetime]    = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class AuditLog(Base):
    __tablename__ = "audit_log"

    id:            Mapped[int]          = mapped_column(primary_key=True, autoincrement=True)
    event_time:    Mapped[datetime]     = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    actor_id:      Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("users.id"), index=True)
    actor_email:   Mapped[str | None]   = mapped_column(String(255))
    action:        Mapped[str]          = mapped_column(String(100), nullable=False)
    resource_type: Mapped[str]          = mapped_column(String(50), nullable=False)
    resource_id:   Mapped[str | None]   = mapped_column(String(100))
    before_state:  Mapped[dict | None]  = mapped_column(JSONB)
    after_state:   Mapped[dict | None]  = mapped_column(JSONB)
    ip_address:    Mapped[str | None]   = mapped_column(String(45))
    request_id:    Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    record_hash:   Mapped[str]          = mapped_column(String(64), nullable=False)
    prev_hash:     Mapped[str | None]   = mapped_column(String(64))


# ═══════════════════════════════════════════════════════════════════════════════
# DATA QUALITY  +  MARKET DATA  +  REGIME
# ═══════════════════════════════════════════════════════════════════════════════

class DQEvent(Base):
    __tablename__ = "dq_events"

    id:          Mapped[int]         = mapped_column(primary_key=True, autoincrement=True)
    time:        Mapped[datetime]    = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    symbol_id:   Mapped[int | None]  = mapped_column(ForeignKey("symbols.id"), index=True)
    event_type:  Mapped[str]         = mapped_column(String(50), nullable=False)
    module:      Mapped[str]         = mapped_column(DQModuleEnum, nullable=False)
    severity:    Mapped[str]         = mapped_column(DQSevEnum, default="INFO")
    raw_payload: Mapped[dict | None] = mapped_column(JSONB)
    reason:      Mapped[str | None]  = mapped_column(Text)
    resolved:    Mapped[bool]        = mapped_column(Boolean, default=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at:  Mapped[datetime]    = mapped_column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        Index("ix_dqevents_time", "time"),
        Index("ix_dqevents_symbol", "symbol_id"),
    )


class MarketTick(Base):
    __tablename__ = "market_ticks"

    id:            Mapped[int]         = mapped_column(primary_key=True, autoincrement=True)
    time:          Mapped[datetime]    = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    symbol_id:     Mapped[int]         = mapped_column(ForeignKey("symbols.id"), nullable=False, index=True)
    price:         Mapped[float]       = mapped_column(Numeric(20, 8), nullable=False)
    volume:        Mapped[float]       = mapped_column(Numeric(20, 8), nullable=False)
    regime:        Mapped[str | None]  = mapped_column(String(30), nullable=True)
    side:          Mapped[str | None]  = mapped_column(String(50), nullable=True)
    exchange:      Mapped[str]         = mapped_column(String(30), nullable=False)
    quality_score: Mapped[int]         = mapped_column(SmallInteger, default=100)
    flags:         Mapped[list | None] = mapped_column(JSONB)
    dq_result:     Mapped[str]         = mapped_column(DQResultEnum, default="PASS")
    meta:          Mapped[list | None] = mapped_column(JSONB, nullable=True)
    candle_ref:    Mapped[datetime]    = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    
    __table_args__ = (
        Index("ix_markettick_symbol_time", "symbol_id", "time"),
    )


class RegimeState(Base):
    __tablename__ = "regime_states"

    id:            Mapped[int]         = mapped_column(primary_key=True, autoincrement=True)
    time:          Mapped[datetime]    = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    symbol_id:     Mapped[int]         = mapped_column(ForeignKey("symbols.id"), nullable=False, index=True)
    regime_label:  Mapped[str]         = mapped_column(String(30), nullable=False)
    confidence:    Mapped[float]       = mapped_column(Numeric(6, 4), nullable=False)
    hmm_probs:     Mapped[dict | None] = mapped_column(JSONB)
    detected_by:   Mapped[str]         = mapped_column(String(20), default="HMM")
    
# ─────────────────────────────────────────────────────────────────────────────
# 1-minute candles  (high-volume, auto-pruned)
# ─────────────────────────────────────────────────────────────────────────────
class Candle1m(Base):
    __tablename__ = "candles_1m"
 
    id:         Mapped[int]          = mapped_column(primary_key=True, autoincrement=True)
    time:       Mapped[datetime]     = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    symbol_id:  Mapped[int]          = mapped_column(ForeignKey("symbols.id"), nullable=False, index=True)
    open:       Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    high:       Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    low:        Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    close:      Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    volume:     Mapped[float]        = mapped_column(Numeric(30, 8), nullable=False, default=0)
    tick_count: Mapped[int]          = mapped_column(Integer, default=10)
    has_dq:     Mapped[bool]         = mapped_column(Boolean, nullable=False, default=False)
 
    __table_args__ = (
        UniqueConstraint("symbol_id", "time", name="uq_candles1m_symbol_time"),
        Index("ix_candles1m_time", "time"),
    )

class Candle1h(Base):
    __tablename__ = "candles_1h"
 
    id:         Mapped[int]          = mapped_column(primary_key=True, autoincrement=True)
    time:       Mapped[datetime]     = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    symbol_id:  Mapped[int]          = mapped_column(ForeignKey("symbols.id"), nullable=False, index=True)
    open:       Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    high:       Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    low:        Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    close:      Mapped[float]        = mapped_column(Numeric(20, 8), nullable=False)
    volume:     Mapped[float]        = mapped_column(Numeric(30, 8), nullable=False, default=0)
    tick_count: Mapped[int]          = mapped_column(Integer, default=10)
 
    __table_args__ = (
        UniqueConstraint("symbol_id", "time", name="uq_candles1h_symbol_time"),
        Index("ix_candles1h_time", "time"),
    )

