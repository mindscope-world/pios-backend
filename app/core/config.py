from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from typing import List


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "Pi OS API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    ENVIRONMENT: str
    
    DATABASE_USER: str
    DATABASE_PASSWORD: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    DATABASE_NAME: str


    # Database
    @property
    def DATABASE_URL(self) -> str:
        password = quote_plus(self.DATABASE_PASSWORD)
        return (
            f"postgresql+asyncpg://{self.DATABASE_USER}:{password}"
            f"@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"
        )
    
    @property
    def DATABASE_URL_SYNC(self) -> str:
        password = quote_plus(self.DATABASE_PASSWORD)
        return (
            f"postgresql+psycopg2://{self.DATABASE_USER}:{password}"
            f"@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"
        )
    DB_POOL_SIZE: int = 20
    DB_MAX_OVERFLOW: int = 40

    # Auth
    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int
    REFRESH_TOKEN_EXPIRE_DAYS: int

    # Encryption (broker credentials stored AES-256)
    ENCRYPTION_KEY: str

    # Redis / Celery
    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/1"

    # CORS
    CORS_ORIGINS: List[str] = ["http://localhost:5173", "http://localhost:3000"]

    # Risk defaults
    DEFAULT_MAX_DRAWDOWN_PCT: float = 15.0
    DEFAULT_DAILY_LOSS_LIMIT: float = 5000.0
    DEFAULT_MAX_POSITION_USD: float = 20000.0
    DEFAULT_MAX_LEVERAGE: float = 3.0
    DEFAULT_MAX_OPEN_ORDERS: int = 50

    # Paper-trading equity baseline for per-trader PnL snapshots and the
    # portfolio-metrics fallback when a user has no snapshots yet.
    DEFAULT_STARTING_EQUITY_USD: float = 100_000.0

    # The service account the intelligence worker computes system-wide
    # snapshots as (see app/workers/intelligence_worker.py). Must exist in
    # the users table or the worker skips every symbol. Override per
    # environment via SYSTEM_USER_EMAIL in .env.
    SYSTEM_USER_EMAIL: str = "admin@pios.com"

    # Alpaca (stocks data + trading)
    ALPACA_API_KEY:    str = ""
    ALPACA_API_SECRET: str = ""
    ALPACA_PAPER:      bool = True   # True = paper trading endpoint
    ALPACA_DATA_FEED:  str = "iex"   # iex (free) | sip (paid data subscription)
    # How often the API process reconciles open Alpaca orders with the broker
    # (resting LIMITs that fill after submit-time polling; broker-side cancels)
    ALPACA_FILL_SYNC_INTERVAL_SECS: int = 15
    # Trade-update WebSocket stream: pushes fills/cancels the instant they
    # happen at Alpaca; the poll loop above stays on as the safety net
    ALPACA_TRADE_STREAM_ENABLED: bool = True

    # OANDA market data fallback for forex ticks and candles
    OANDA_API_KEY:      str = ""
    OANDA_ACCOUNT_ID:   str = ""
    OANDA_ENVIRONMENT:  str = "practice"  # practice or live
    OANDA_POLL_INTERVAL_SECS: int = 3

    # Kafka (optional — set to enable market data streaming via Kafka)
    KAFKA_BOOTSTRAP_SERVERS: str = ""   # e.g. "localhost:9092"
    KAFKA_TICK_TOPIC:        str = "market.ticks.raw"
    KAFKA_CONSUMER_GROUP:    str = "pios-db-writer"

    # MLflow tracking server (optional)
    MLFLOW_TRACKING_URI: str = ""   # e.g. "http://localhost:5000"

    # DQ thresholds
    DQ_SPIKE_THRESHOLD: float = 0.05
    DQ_VOLUME_MAX_FACTOR: float = 50.0
    DQ_PRICE_MAX_FACTOR: float = 10.0
    DQ_DEDUP_WINDOW_SECS: int = 10
    DQ_BATCH_SIZE: int = 25
    DQ_PRICE_WINDOW: int = 20

    
     # ── Retention (days) ──────────────────────────────────────
    RETAIN_CANDLES_1M_DAYS: int = 90
    RETAIN_CANDLES_1H_DAYS: int = 730
    RETAIN_DQ_EVENTS_DAYS: int = 30
    RETAIN_MARKET_TICKS_HOURS: int = 48   # raw ticks are high-volume; short window vs. candles

    # ── Aggregation ───────────────────────────────────────────
    CANDLE_INTERVAL_SECONDS: int = 60
 
    # ── Redis stream caps ─────────────────────────────────────
    STREAM_MAX_LEN: int = 100_000
    STREAM_TTL_SECS: int = 86_400
 
    # ── WebSocket ─────────────────────────────────────────────
    WS_HEARTBEAT_SECS: int = 5
    
    @property
    def kafka_enabled(self) -> bool:
        return bool(self.KAFKA_BOOTSTRAP_SERVERS)

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors(cls, v):
        if isinstance(v, str):
            return [o.strip() for o in v.split(",")]
        return v


settings = Settings()