# Pi OS Backend

FastAPI + PostgreSQL + Celery backend for the Pi OS institutional quant trading platform.

## Quick Start

```bash
# 1. Clone and set up environment
cp .env.example .env
# Edit .env — set DB_PASSWORD, SECRET_KEY, ENCRYPTION_KEY

# 2. Start all services with Docker Compose
docker compose up -d

# 3. Verify API is running
curl http://localhost:9000/health
# → {"status":"ok","version":"1.0.0",...}

# 4. Open interactive API docs
open http://localhost:9000/docs
```

## Manual Setup (without Docker)

```bash
# Postgres + Redis must be running locally first

pip install -r requirements.txt

# create migration file

alembic revision --autogenerate -m "migration file name"

# Run database migrations
alembic upgrade head

# Seed initial data + start API
uvicorn main:app --reload --port 9000

# In a separate terminal: Celery worker
celery -A app.workers.celery_app worker --loglevel=info

# Optional: Celery beat scheduler (Darwin evolution, PnL snapshots)
celery -A app.workers.celery_app beat --loglevel=info
```

## API Reference

Full interactive docs at `http://localhost:9000/docs` (Swagger UI).

### Base URL
```
http://localhost:8000/api/v1
```

### Authentication
All endpoints (except `/auth/login`) require:
```
Authorization: Bearer <access_token>
```

### Demo Accounts (seeded on first boot)
| Email | Password | Role |
|---|---|---|
| admin@pi-os.io | admin123 | admin |
| trader@pi-os.io | trader123 | trader |
| quant@pi-os.io | quant123 | quant |
| viewer@pi-os.io | viewer123 | viewer |
| compliance@pi-os.io | comply123 | compliance |

### Endpoints

#### Auth  `/api/v1/auth/`
| Method | Path | Description |
|---|---|---|
| POST | `/login` | Login — returns JWT access + refresh token |
| POST | `/refresh` | Rotate refresh token |
| POST | `/logout` | Revoke session |
| GET | `/me` | Current user profile |
| POST | `/mfa/setup` | Generate TOTP secret + QR URI |
| POST | `/mfa/verify` | Confirm code and enable MFA |

#### Users  `/api/v1/users/`
| Method | Path | Roles |
|---|---|---|
| GET | `/` | admin |
| POST | `/` | admin |
| GET | `/{id}` | self or admin |
| PATCH | `/{id}` | self or admin |
| DELETE | `/{id}` | admin |

#### Brokers  `/api/v1/brokers/`
Traders register their own broker connections. Credentials are AES-256 encrypted at rest.

| Method | Path | Description |
|---|---|---|
| GET | `/` | List own brokers (admin sees all) |
| POST | `/` | Add new broker connection |
| GET | `/{id}` | Get broker |
| PATCH | `/{id}` | Update broker |
| DELETE | `/{id}` | Remove broker |
| POST | `/{id}/test` | Test connection + measure latency |
| GET | `/{id}/account` | Fetch live account balance |

Supported `broker_type` values: `ALPACA`, `BINANCE`, `CCXT`, `IBKR`, `OANDA`, `LMAX`, `CUSTOM`

For CCXT brokers (Binance, OKX, Bybit …), also pass `exchange_id: "binance"` etc.

#### Orders  `/api/v1/orders/`
| Method | Path | Description |
|---|---|---|
| POST | `/` | Submit order (risk-gated) |
| GET | `/` | List orders (paginated) |
| GET | `/{id}` | Order detail + state history |
| DELETE | `/{id}` | Cancel pending order |
| GET | `/{id}/fills` | All fills for an order |
| GET | `/{id}/tca` | Transaction cost analysis |
| GET | `/fills/all` | All fills across all orders |

Supported `order_type` values: `MARKET`, `LIMIT`, `STOP`, `STOP_LIMIT`, `OCO`, `TWAP`, `VWAP`, `ICEBERG`

#### Positions  `/api/v1/positions/`
| Method | Path | Description |
|---|---|---|
| GET | `/` | Open positions |
| GET | `/metrics` | Portfolio KPI metrics for dashboard |
| GET | `/equity-curve` | PnL snapshots for equity chart |

#### Strategies  `/api/v1/strategies/`
8-stage lifecycle: IDEA → RESEARCH → BACKTEST → PAPER → LIVE_SMALL → SCALED → MONITOR → RETIRED

| Method | Path | Description |
|---|---|---|
| GET | `/` | List strategies |
| POST | `/` | Create (quant, admin) |
| GET | `/{id}` | Full strategy record |
| PATCH | `/{id}` | Update |
| POST | `/{id}/advance` | Advance lifecycle stage (gate-checked) |
| POST | `/{id}/retire` | Retire strategy |
| DELETE | `/{id}` | Delete (non-live only) |
| POST | `/{id}/backtest` | Submit backtest job (async Celery) |
| GET | `/{id}/backtest` | List backtest jobs |
| GET | `/backtest/{job_id}` | Poll job status + results |

#### Risk  `/api/v1/risk/`
| Method | Path | Description |
|---|---|---|
| GET | `/metrics` | Live VaR, CVaR, drawdown, leverage |
| GET | `/limits` | All active risk limits |
| POST | `/limits` | Create risk limit (admin) |
| PATCH | `/limits/{id}` | Update limit value / action |
| DELETE | `/limits/{id}` | Deactivate limit |
| POST | `/killswitch` | Cancel all orders + close positions |
| GET | `/killswitch/history` | Kill switch event log |

#### Data Quality  `/api/v1/data/`
| Method | Path | Description |
|---|---|---|
| GET | `/quality/summary` | DQ stats: pass/flag/reject rates |
| GET | `/quality/events` | DQ event log (paginated) |
| GET | `/feeds/health` | Feed health: lag + DQ score per symbol |
| GET | `/regime/{symbol}` | Current regime state for a symbol |
| GET | `/symbols` | List all active symbols |

#### Alerts  `/api/v1/alerts/`
| Method | Path | Description |
|---|---|---|
| GET | `/` | List alerts (filter by severity, acked, source) |
| GET | `/{id}` | Single alert |
| POST | `/{id}/acknowledge` | Acknowledge with optional note |
| POST | `/acknowledge-all` | Bulk acknowledge |

#### Audit  `/api/v1/audit/`
| Method | Path | Description |
|---|---|---|
| GET | `/` | Immutable audit chain (paginated) |
| GET | `/verify` | Verify SHA-256 chain integrity |

## Project Structure

```
pi_os_backend/
├── main.py                          # App factory, lifespan, seed
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── alembic.ini
├── alembic/env.py                   # Migrations
├── tests/
│   ├── conftest.py                  # Test DB + seeded users
│   ├── test_auth.py
│   ├── test_orders.py
│   ├── test_brokers.py
│   └── test_strategies.py
└── app/
    ├── core/
    │   ├── config.py                # Pydantic settings from .env
    │   ├── security.py              # JWT, bcrypt, AES-256, audit hash
    │   └── deps.py                  # FastAPI dependencies + role guards
    ├── db/
    │   ├── session.py               # Async SQLAlchemy engine + get_db
    │   └── sync_session.py          # Sync session for Celery workers
    ├── models/
    │   └── all_models.py            # All 15 SQLAlchemy models
    ├── schemas/
    │   └── all_schemas.py           # All Pydantic request/response schemas
    ├── services/
    │   ├── audit_service.py         # SHA-256 audit chain writer
    │   ├── broker_service.py        # Adapter factory + CCXT/Alpaca/Paper
    │   ├── order_service.py         # Order flow: risk gate → broker → fill
    │   ├── risk_service.py          # Kill switch + VaR metrics
    │   └── strategy_service.py      # Lifecycle management + gate logic
    ├── api/v1/
    │   ├── router.py                # Aggregates all routers
    │   └── endpoints/
    │       ├── auth.py              # Login, refresh, MFA
    │       ├── users.py             # User CRUD
    │       ├── brokers.py           # Trader-managed broker connections
    │       ├── orders.py            # Order submission + fills + TCA
    │       ├── positions.py         # Positions + portfolio metrics
    │       ├── strategies.py        # Strategy lifecycle + backtesting
    │       ├── risk.py              # VaR, limits, kill switch
    │       ├── alerts.py            # Alert management
    │       ├── audit.py             # Immutable audit chain
    │       └── data_quality.py      # DQ stats, feed health, regime
    └── workers/
        ├── celery_app.py            # Celery + beat schedule
        └── backtest_worker.py       # Async backtest + Darwin evolution tasks
```

## Adding a New Broker

1. Create a class extending `BrokerAdapter` in `app/services/broker_service.py`
2. Implement: `test_connection`, `get_account`, `submit_order`, `cancel_order`, `get_positions`, `get_fills`
3. Add the broker type string to `ADAPTER_MAP`
4. Add the type to `BrokerTypeEnum` in `app/models/all_models.py`
5. Run `alembic revision --autogenerate -m "add_broker_type"` + `alembic upgrade head`

Traders can then register this broker type from the UI without any backend redeploy.

## Running Tests

```bash
# Requires a running test database: pios_test
createdb pios_test

pip install pytest pytest-asyncio anyio httpx
pytest tests/ -v
```

## Production Checklist

- [ ] Set `SECRET_KEY` to `openssl rand -hex 32` output
- [ ] Set `ENCRYPTION_KEY` to exactly 32 random bytes
- [ ] Set `DEBUG=false`
- [ ] Use managed PostgreSQL (Hetzner, Supabase, etc.) — not the docker compose DB
- [ ] Enable SSL on database connection
- [ ] Add `DB_PASSWORD` to secrets manager, not .env
- [ ] Set `CORS_ORIGINS` to your production frontend domain only
- [ ] Enable rate limiting (nginx or FastAPI middleware)
- [ ] Run at least 2 uvicorn workers: `--workers 4`
