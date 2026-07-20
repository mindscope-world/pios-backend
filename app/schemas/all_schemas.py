# app/schemas/all_schemas.py
# All request/response schemas in one file for clarity.
import uuid
from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, EmailStr, Field, computed_field, field_validator


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════════════════

class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    mfa_code: str | None = None

class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: "UserOut"

class RefreshRequest(BaseModel):
    refresh_token: str

class MFASetupResponse(BaseModel):
    secret: str
    qr_uri: str

class MFAVerifyRequest(BaseModel):
    code: str


# ═══════════════════════════════════════════════════════════════════════════════
# USERS
# ═══════════════════════════════════════════════════════════════════════════════

class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    full_name: str
    role: str = "viewer"

class UserUpdate(BaseModel):
    full_name: str | None = None
    role: str | None = None
    is_active: bool | None = None
    preferences: dict | None = None

class UserOut(BaseModel):
    model_config = {"from_attributes": True}
    id: uuid.UUID
    email: str
    full_name: str
    role: str
    is_active: bool
    mfa_enabled: bool
    last_login_at: datetime | None
    created_at: datetime

class RegisterRequest(BaseModel):
    full_name: str
    role: str = "trader"
    email: EmailStr
    password: str = Field(..., min_length=8)


# ═══════════════════════════════════════════════════════════════════════════════
# BROKERS
# ═══════════════════════════════════════════════════════════════════════════════

class BrokerCredentials(BaseModel):
    """Plaintext credentials — never stored, encrypted on write."""
    api_key: str | None = None
    api_secret: str | None = None
    account_id: str | None = None
    passphrase: str | None = None          # some exchanges need this
    host: str | None = None                # IBKR TWS host
    port: int | None = None                # IBKR TWS port
    client_id: int | None = None           # IBKR client ID

class BrokerCreate(BaseModel):
    name: str
    broker_type: str
    exchange_id: str | None = None         # for CCXT
    # Real adapters exist only for ALPACA/BINANCE/CCXT/MT5/OANDA. For any other
    # broker_type, is_paper=True (the default) is required -- it's the explicit
    # acknowledgement that this connection will get simulated fills, not a real
    # broker integration. Setting is_paper=false for an unsupported type is
    # rejected (UnsupportedBrokerError) rather than silently faking fills.
    is_paper: bool = True
    credentials: BrokerCredentials
    config: dict = Field(default_factory=dict)

class BrokerUpdate(BaseModel):
    name: str | None = None
    is_active: bool | None = None
    credentials: BrokerCredentials | None = None
    config: dict | None = None

class BrokerOut(BaseModel):
    model_config = {"from_attributes": True}
    id: uuid.UUID
    name: str
    broker_type: str
    exchange_id: str | None
    is_paper: bool
    is_active: bool
    status: str
    last_heartbeat: datetime | None
    latency_p99_ms: float | None
    error_message: str | None
    config: dict
    created_at: datetime
    # NOTE: credentials_enc is never included in responses

class BrokerTestResult(BaseModel):
    success: bool
    latency_ms: float | None
    message: str
    account_info: dict | None = None


# ═══════════════════════════════════════════════════════════════════════════════
# SYMBOLS
# ═══════════════════════════════════════════════════════════════════════════════

class SymbolOut(BaseModel):
    model_config = {"from_attributes": True}
    id: int
    symbol: str
    base_asset: str
    quote_asset: str
    asset_class: str
    exchange: str
    is_active: bool


# ═══════════════════════════════════════════════════════════════════════════════
# ORDERS
# ═══════════════════════════════════════════════════════════════════════════════

class OrderCreate(BaseModel):
    broker_id: uuid.UUID
    client_order_id: str | None = Field(
        None, max_length=100,
        description="Caller-supplied idempotency key. Retrying the same key "
                     "for this user is rejected as a duplicate rather than "
                     "double-submitted to the broker.",
    )
    symbol: str
    side: str                              # BUY | SELL
    # MARKET|LIMIT|STOP|STOP_LIMIT|TWAP|VWAP|OCO|ICEBERG. TWAP/VWAP/ICEBERG
    # execute as background slice schedules (execution_algo); STOP_LIMIT/OCO
    # arm app-side and fire on trigger (conditional_orders) — STOP_LIMIT
    # needs price+stop_price, OCO needs price (limit leg) + stop_price (stop
    # leg) and creates two linked rows, one cancelling the other on fill.
    order_type: str = "MARKET"
    qty: float = Field(gt=0)
    price: float | None = None             # required for LIMIT / STOP_LIMIT
    stop_price: float | None = None
    time_in_force: str = "GTC"
    strategy_id: uuid.UUID | None = None
    algo_config: dict | None = None        # slice params consumed by app.services.execution_algo:
                                            # {"slices": int, "interval_seconds": float,
                                            #  "display_qty": float (ICEBERG only)}

# Order types with a real slicing/algorithmic execution engine wired in
# (see app.services.execution_algo.run_algo_order). Update this set as each
# algorithm actually ships; nothing else needs to change.
_ALGORITHMIC_ORDER_TYPES: set[str] = {"TWAP", "VWAP", "ICEBERG"}

# Order types armed app-side and fired by the trigger monitor
# (app.services.conditional_orders): STOP_LIMIT rests until its stop crosses,
# then goes to the broker as a LIMIT; OCO is two linked legs (limit + stop)
# where one filling cancels the other.
_CONDITIONAL_ORDER_TYPES: set[str] = {"STOP_LIMIT", "OCO"}

class OrderOut(BaseModel):
    model_config = {"from_attributes": True}
    id: uuid.UUID
    client_order_id: str | None
    broker_order_id: str | None
    symbol: "SymbolOut | None" = None
    side: str
    order_type: str
    time_in_force: str
    qty: float
    filled_qty: float
    price: float | None
    stop_price: float | None
    avg_fill_price: float | None
    status: str
    state_history: list
    reject_reason: str | None
    strategy_id: uuid.UUID | None
    submitted_at: datetime | None
    filled_at: datetime | None
    created_at: datetime

    @computed_field
    @property
    def execution_style(self) -> str:
        """
        "INSTANT" = filled immediately by the broker adapter in a single call
        (MARKET/LIMIT/STOP).
        "ALGORITHMIC" = executed as a background slice schedule by
        app.services.execution_algo (TWAP/VWAP/ICEBERG) -- the order starts
        SUBMITTED with zero fills and walks to PARTIAL/FILLED as slices land.
        "CONDITIONAL" = armed app-side and fired by the trigger monitor
        (STOP_LIMIT/OCO, app.services.conditional_orders) -- the order rests
        SUBMITTED with no broker_order_id until its trigger crosses.
        """
        if self.order_type in _ALGORITHMIC_ORDER_TYPES:
            return "ALGORITHMIC"
        if self.order_type in _CONDITIONAL_ORDER_TYPES:
            return "CONDITIONAL"
        return "INSTANT"

class FillOut(BaseModel):
    model_config = {"from_attributes": True}
    id: uuid.UUID
    order_id: uuid.UUID
    side: str
    qty: float
    price: float
    commission: float
    slippage_bps: float | None
    funding_cost: float
    total_cost: float
    filled_at: datetime

class CancelOrderResponse(BaseModel):
    order_id: uuid.UUID
    status: str
    message: str

class TCAReport(BaseModel):
    order_id: uuid.UUID
    total_fills: int
    total_qty: float
    avg_fill_price: float
    commission_total: float
    slippage_bps_avg: float | None
    spread_cost_bps_avg: float | None
    funding_cost_total: float
    total_cost: float


# ═══════════════════════════════════════════════════════════════════════════════
# POSITIONS  +  PORTFOLIO
# ═══════════════════════════════════════════════════════════════════════════════

class PositionOut(BaseModel):
    model_config = {"from_attributes": True}
    id: uuid.UUID
    symbol: SymbolOut | None = None
    side: str
    qty: float
    avg_cost: float
    unrealized_pnl: float
    realized_pnl: float
    margin_used: float
    is_open: bool
    opened_at: datetime

class EquityPoint(BaseModel):
    day: int
    value: float
    snapshot_at: datetime

class PortfolioMetricsOut(BaseModel):
    total_equity: float
    equity_change: float
    equity_change_pct: float
    realized_pnl: float
    realized_today: float
    unrealized_pnl: float
    active_strategies: int
    max_drawdown: float
    drawdown_limit: float
    sharpe: float | None = None      # None = insufficient equity history to compute
    win_rate: float | None = None    # None = no closed positions yet


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGIES
# ═══════════════════════════════════════════════════════════════════════════════

AlphaClock = Literal["SHORT_FLOW", "MEDIUM_TREND", "LONG_MACRO"]

class StrategyCreate(BaseModel):
    name: str
    hypothesis: str | None = None
    description: str | None = None
    feature_list: list[str] | None = None
    allowed_symbols: list[int] | None = None
    allowed_regimes: list[str] | None = None
    risk_profile: dict = Field(default_factory=dict)
    config: dict = Field(default_factory=dict)
    tags: list[str] | None = None
    alpha_clock: AlphaClock | None = None

class StrategyUpdate(BaseModel):
    name: str | None = None
    hypothesis: str | None = None
    description: str | None = None
    config: dict | None = None
    risk_profile: dict | None = None
    tags: list[str] | None = None
    alpha_clock: AlphaClock | None = None

class StrategyAdvanceRequest(BaseModel):
    notes: str | None = None

class StrategyOut(BaseModel):
    model_config = {"from_attributes": True}
    id: uuid.UUID
    name: str
    version: str
    lifecycle_stage: str
    generation: int
    parent_id: uuid.UUID | None
    hypothesis: str | None
    fitness_score: float | None
    sharpe_last: float | None
    risk_profile: dict
    config: dict
    is_paper_only: bool
    tags: list | None
    gate_history: list
    deployed_at: datetime | None
    retired_at: datetime | None
    retirement_reason: str | None
    created_at: datetime

    @computed_field
    @property
    def alpha_clock(self) -> AlphaClock | None:
        """V10.4 D.2 -- stored in config.alpha_clock, no dedicated column
        (config is JSONB, so tagging needs no migration)."""
        return self.config.get("alpha_clock") if isinstance(self.config, dict) else None

class BacktestJobCreate(BaseModel):
    start_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    end_date: str   = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    symbols: list[str]
    cost_model: str = "FULL"
    config: dict = Field(default_factory=dict)

class BacktestJobOut(BaseModel):
    model_config = {"from_attributes": True}
    id: uuid.UUID
    strategy_id: uuid.UUID
    status: str
    progress_pct: int
    start_date: str
    end_date: str
    cost_model: str
    sharpe_ratio: float | None
    max_drawdown: float | None
    total_return: float | None
    trade_count: int | None
    win_rate: float | None
    profit_factor: float | None
    full_report: dict | None
    equity_curve: list | None
    error_message: str | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime


# ═══════════════════════════════════════════════════════════════════════════════
# RISK
# ═══════════════════════════════════════════════════════════════════════════════

class RiskLimitCreate(BaseModel):
    name: str
    scope: str = "global"
    scope_id: str | None = None
    limit_type: str
    limit_value: float
    breach_action: str = "ALERT"

class RiskLimitUpdate(BaseModel):
    limit_value: float | None = None
    breach_action: str | None = None
    is_active: bool | None = None

class RiskLimitOut(BaseModel):
    model_config = {"from_attributes": True}
    id: int
    name: str
    scope: str
    limit_type: str
    limit_value: float
    current_value: float | None
    breach_action: str
    is_active: bool
    updated_at: datetime

V104Regime = Literal["LOW_VOL_TREND", "HIGH_VOL_TREND", "RANGE_BOUND", "CRISIS_LIQUIDITY", "MACRO_EVENT"]


class ClockWeightBandCreate(BaseModel):
    clock: AlphaClock
    regime: V104Regime
    min_pct: float = Field(ge=0, le=100)
    max_pct: float = Field(ge=0, le=100)

class ClockWeightBandUpdate(BaseModel):
    min_pct: float | None = Field(default=None, ge=0, le=100)
    max_pct: float | None = Field(default=None, ge=0, le=100)
    is_active: bool | None = None

class ClockWeightBandOut(BaseModel):
    model_config = {"from_attributes": True}
    id: int
    clock: str
    regime: str
    min_pct: float
    max_pct: float
    is_active: bool
    updated_at: datetime


class RiskMetricsOut(BaseModel):
    var95: float
    var99: float
    cvar: float
    drawdown_current: float
    drawdown_limit: float
    leverage: float
    margin_used: float
    margin_avail: float
    daily_loss: float
    daily_loss_limit: float
    kill_switch_armed: bool
    triggers_today: int

class KillSwitchRequest(BaseModel):
    reason: str
    mfa_code: str | None = None            # required if user has MFA enabled

class KillSwitchEventOut(BaseModel):
    model_config = {"from_attributes": True}
    id: uuid.UUID
    trigger_source: str
    reason: str
    orders_cancelled: int
    positions_closed: int
    status: str
    created_at: datetime


# ═══════════════════════════════════════════════════════════════════════════════
# ALERTS
# ═══════════════════════════════════════════════════════════════════════════════

class AlertOut(BaseModel):
    model_config = {"from_attributes": True}
    id: uuid.UUID
    severity: str
    source: str
    category: str
    title: str
    message: str
    strategy_id: uuid.UUID | None
    symbol_id: int | None
    is_acknowledged: bool
    ack_note: str | None
    acknowledged_at: datetime | None
    created_at: datetime

class AlertAckRequest(BaseModel):
    note: str | None = None


# ═══════════════════════════════════════════════════════════════════════════════
# AUDIT
# ═══════════════════════════════════════════════════════════════════════════════

class AuditEntryOut(BaseModel):
    model_config = {"from_attributes": True}
    id: int
    event_time: datetime
    actor_email: str | None
    action: str
    resource_type: str
    resource_id: str | None
    before_state: dict | None
    after_state: dict | None
    record_hash: str
    prev_hash: str | None


# ═══════════════════════════════════════════════════════════════════════════════
# DATA QUALITY
# ═══════════════════════════════════════════════════════════════════════════════

class DQModuleStats(BaseModel):
    name: str
    processed: str
    pass_rate: str
    flag_rate: str
    reject_rate: str
    avg_latency_ms: float

class DQStatsOut(BaseModel):
    total_ticks: int
    pass_rate: float
    flag_rate: float
    reject_rate: float
    gaps: int
    modules: list[DQModuleStats]

class DQEventOut(BaseModel):
    model_config = {"from_attributes": True}
    id: int
    time: datetime
    symbol_id: int | None
    event_type: str
    module: str
    severity: str
    reason: str | None
    resolved: bool


# ═══════════════════════════════════════════════════════════════════════════════
# REGIME  +  FEEDS
# ═══════════════════════════════════════════════════════════════════════════════

class RegimeStateOut(BaseModel):
    model_config = {"from_attributes": True}
    symbol_id: int
    regime_label: str
    confidence: float
    hmm_probs: dict | None
    detected_by: str
    time: datetime

class FeedHealthOut(BaseModel):
    symbol: str
    lag_ms: float
    dq_score: float
    ok: bool
    exchange: str


# ═══════════════════════════════════════════════════════════════════════════════
# COMMON
# ═══════════════════════════════════════════════════════════════════════════════

class PaginatedResponse(BaseModel):
    items: list[Any]
    total: int
    page: int
    page_size: int
    pages: int

class MessageResponse(BaseModel):
    message: str

class HealthResponse(BaseModel):
    status: str
    version: str
    db: str
    redis: str
    services: dict[str, str]
