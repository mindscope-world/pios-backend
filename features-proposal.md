# Features Proposal

## Built Features

### Authentication & User Management
- User registration with email verification
- Login/logout with JWT access and refresh tokens
- Multi-factor authentication (TOTP) setup and verification
- Password hashing (bcrypt) and secure storage
- Role-based access control (admin, quant, trader, viewer, compliance)
- User CRUD operations (admin-only for list/create/delete, self for update/profile)

### Broker Connections
- Trader-managed broker connections (API keys encrypted AES-256 at rest)
- Supported broker types: ALPACA, BINANCE, CCXT, IBKR, OANDA, LMAX, CUSTOM
- Connection testing + latency measurement
- Live account balance fetching
- CRUD operations for broker configurations

### Order Management
- Order submission with risk gating (pre-trade checks)
- Supported order types: MARKET, LIMIT, STOP, STOP_LIMIT, OCO, TWAP, VWAP, ICEBERG
- Order lifecycle tracking (status, fills, cancellations)
- Order listing with pagination, filtering by status, symbol, strategy
- Individual order detail including fill history and transaction cost analysis (TCA)
- Bulk fill listing across all orders
- Order cancellation (pending orders only)

### Positions & Portfolio
- Open positions listing
- Portfolio KPI metrics (dashboard-ready)
- Equity curve (PnL snapshots) for charting
- Position-level analytics (unrealized PnL, cost basis, etc.)

### Strategy Lifecycle
- 8-stage lifecycle: IDEA → RESEARCH → BACKTEST → PAPER → LIVE_SMALL → SCALED → MONITOR → RETIRED
- Strategy CRUD (quant/admin for create, self for own strategies)
- Stage advancement with gate checks (e.g., minimum Sharpe, max drawdown)
- Strategy retirement (manual or automated)
- Deletion of non-live strategies
- Backtest job submission (async Celery worker)
- Backtest job polling and results retrieval
- Strategy configuration and risk profile storage (JSON)

### Risk Management
- Real-time risk metrics: VaR, CVaR, drawdown, leverage
- Configurable risk limits (name, scope, type, value, breach action)
- Limit CRUD (admin only)
- Emergency kill switch: cancel all orders + close all positions (MFA-protected)
- Kill switch event history and audit trail

### Data Quality & Market Data
- Data quality summary: pass/flag/reject rates per module
- Data quality event log (paginated, filterable by severity/module/symbol)
- Feed health: lag and DQ score per symbol
- Regime state detection per symbol (trend/mean-revert/breakout/crisis/range)
- Active symbol listing (by asset class)
- Market data services: live ticker, order book, recent trades, OHLCV, technical indicators, multi-asset snapshot, market breadth, funding rates

### Alerting System
- Alert listing (filter by severity, acknowledgment state, source)
- Individual alert retrieval
- Alert acknowledgment (single and bulk)
- Alert severity levels: P1 (critical) to P4 (info)
- Alert sources: system, risk, data quality, strategy, etc.

### Audit & Compliance
- Immutable SHA-256 chained audit log
- Audit log listing (filter by action, resource type, actor email)
- Audit chain integrity verification endpoint
- Audit events for: user login/register, MFA enabled, broker CRUD, order events, strategy lifecycle, risk limit changes, kill switch, etc.

### Real-Time Communications
- WebSocket endpoint for channel/subscription-based updates (positions, orders, alerts, intelligence data)
- Server-Sent Events (SSE) streams for market data, analytics, notifications
- Real-time market data WebSocket with price, order book, trades, technical indicators
- Notification streams for constraint changes, regime shifts, feed outages

### Intelligence & Decision Support
- Decision context and feed (Redis-backed)
- Regime current/trend
- Order Flow Intelligence (OFI) signals and charts
- Cross-Market Intelligence (GMIG) snapshots and radar
- Monte Carlo simulations (manual and auto)
- Alpha factory state and Darwin evolution metrics
- Signal conflict and rejection statistics
- Feature store (computed features per symbol)
- Command center dashboard
- Quant core gates (8-gate pipeline visualization)
- Scenario simulations
- Decision traces (audit of decision-making process)
- Why not trade analysis (constraints preventing execution)
- Live market data endpoints: ticker, order book, trades, technical indicators, multi-asset snapshot, market breadth, funding rates
- Enhanced OFI/GMIG with live orderbook + trades
- Real-time streams: market data WebSocket, SSE market stream, SSE notification stream

### Workers & Background Processing
- Celery application with beat scheduler
- Backtest worker: async backtesting + Darwin evolution strategy optimization
- Candle aggregator: rolling OHLCV window construction
- DB writer: persistence of market ticks to PostgreSQL
- Decision stream worker: real-time behavior monitoring, quant core gates, signal conflict detection
- Data quality pipeline: tick validation, duplicate filtering, timestamp correction, outlier detection, continuity monitoring
- Intelligence worker: regeneration of intelligence caches (Redis) from DB
- Market DB writer: persistence of aggregate market data
- Market ingestion worker: normalization and storage of exchange data
- Orchestrator: coordination of complex workflows
- Retention task: data retention policy enforcement (archival/deletion)
- Tick ingestor: raw tick ingestion from exchanges

### Core Services & Infrastructure
- Order service: order submission flow (risk gate → broker adapter → fill processing)
- Risk service: risk metric computation, kill switch triggering
- Strategy service: lifecycle management, stage advancement logic
- Audit service: SHA-256 chained audit writing
- Broker service: adapter factory for CCXT, Alpaca, Paper brokers
- Market data service: exchange integration, technical indicator computation
- Publisher: Redis pub/sub for inter-service communication
- Quant engine: HMM regime detection, GARCH volatility, signal processing, OFI computation
- Helpers: common utilities (symbol resolution, regime fetching, tick retrieval, Sharpe ratio, etc.)
- Security: JWT handling, password hashing, AES-256 encryption, audit hash chaining

### Database & Migrations
- SQLAlchemy ORM with 15+ models (User, Broker, Order, Position, Strategy, BacktestJob, Fill, PnLSnapshot, AuditLog, RegimeState, MarketTick, Symbol, DQEvent, Alert, RiskLimit, KillSwitchEvent, UserSession)
- Alembic for schema migrations
- PostgreSQL as primary datastore
- Redis for caching, pub/sub, and transient intelligence data

### API & Deployment
- FastAPI with automatic OpenAPI/Swagger UI documentation
- Comprehensive input validation via Pydantic schemas
- Graceful error handling with informative messages
- Docker Compose for local development (Postgres, Redis, API, workers)
- Manual setup instructions (Postgres + Redis + pip install)
- Environment-based configuration (Pydantic settings)
- Health check endpoint (`/health`)

## Partially Built

### Technical Indicators Service
- File: `app/services/intelligence/indicators_service.py`
- Status: Empty placeholder (0 lines)
- Intended purpose: Centralized technical indicator computation
- Note: Many indicators are already computed via `market_data_service.compute_technical_indicators` and used in intelligence endpoints; this service may consolidate or extend that functionality.

### Intelligence Service Facades
- Several intelligence service files (e.g., adaptation_service.py, behavior_service.py, capital_service.py, etc.) contain substantial implementation.
- However, the actual data population for Redis-backed intelligence endpoints is handled by `intelligence_worker.py`, which aggregates data from multiple sources.
- Some services may act as thin facades over Redis data; the heavy lifting resides in the workers.

## Not Implemented (Planned or Future Enhancements)

### Broker & Exchange Features
- API key rotation and automated expiration for broker/exchange connections
- Support for additional broker types (e.g., traditional banks, crypto custodians)
- Advanced order types (e.g., trailing stop, scale orders, algorithmic orders from brokers)
- Fractional share trading support

### Portfolio Optimization & Analytics
- Mean-variance optimization, risk parity, Black-Litterman portfolio construction
- Efficient frontier visualization
- Factor exposure analysis (style, sector, macro)
- Performance attribution (Brinson, return decomposition)
- Transaction cost modeling beyond basic TCA (market impact, timing)

### Data Expansion
- Fundamental data integration (earnings, balance sheets, cash flows)
- News sentiment and event-driven trading signals
- Alternative data sources (satellite, web scraping, credit card transactions)
- On-chain blockchain data for crypto assets
- Macroeconomic indicators (interest rates, inflation, GDP)

### Machine Learning & AI
- Model registry for ML-driven strategy signals
- Online learning / adaptive model updating
- Natural language processing for news/events analysis
- Reinforcement learning for execution algorithms
- AI-assisted strategy ideation and hypothesis generation

### Strategy Development
- Visual strategy builder (drag-and-drop conditions)
- Custom strategy scripting language (Python-based with sandbox)
- Strategy parameter optimization grid (beyond Darwin evolution)
- Walk-forward analysis and robustness testing
- Strategy export/import (portability between instances)

### User Experience & Collaboration
- Multi-tenancy and white-labeling for institutional clients
- Role-based access control extensions (custom roles, permission matrices)
- Social trading / copy trading platform (lead/follower mechanics)
- Paper trading / simulated trading environment with virtual cash
- Detailed performance reporting (PDF, Excel, email scheduling)
- Mobile push notifications (via FCM/APNs) for critical alerts
- In-app messaging and collaboration tools (comments on strategies, trade discussion)

### Operations & DevOps
- Advanced monitoring (Prometheus + Grafana dashboards)
- Distributed tracing (Jaeger/OpenTelemetry)
- Chaos engineering and fault injection testing
- Blue-green deployment strategies
- Kubernetes Helm charts for production deployment
- Disaster recovery and backup automation

### Compliance & Security
- Data loss prevention (DLP) and encryption at rest for backups
- Advanced intrusion detection and prevention
- Regular third-party penetration testing and security audits
- GDPR/CCPA compliance tooling (data subject requests, right to erasure)
- Trade surveillance and market abuse detection (washout, spoofing)

---
*Document generated based on codebase review as of 2026-06-18.*