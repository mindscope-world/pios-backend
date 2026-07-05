# Pi OS Backend — Development Status

Analysis of actual code behavior (not documentation claims) as of 2026-07-05. Every item below was verified by reading the real source — file:line citations included so claims can be re-checked as the code changes. Where prior docs (`features-proposal.md`, `PiOSQ_Specification.md`) already covered a feature at the "exists/doesn't exist" level, this document goes one level deeper: does it actually run without crashing, and is the logic real or a placeholder.

---

## 1. Fully Implemented (real logic, correctly wired)

### Auth, MFA, sessions
JWT access/refresh, bcrypt password hashing, Fernet-encrypted secrets, real `pyotp` TOTP setup/verify enforced at login, refresh-token rotation with revocation, account lockout after 5 failed logins, role guards. (`app/core/security.py`, `app/api/v1/endpoints/auth.py`, `app/core/deps.py`)

### Broker adapters — Alpaca and CCXT/Binance only
Real SDK calls: `alpaca-py TradingClient` for order submit/cancel/positions; real `ccxt` exchange calls for Binance and other CCXT venues. (`app/services/broker_service.py:61-181`)

### Strategy lifecycle gates
Real, specific promotion thresholds — not a rubber stamp: BACKTEST requires a non-empty hypothesis; PAPER requires a completed backtest with Sharpe ≥ 0.8, max drawdown ≤ 15%, ≥ 200 trades; LIVE_SMALL/SCALED require admin clearance. (`app/services/strategy_service.py:130-182`)

### Backtesting engine
Genuine walk-forward simulation: pulls real `MarketTick` history (falls back to a seeded synthetic series only if no ticks exist), splits 4–12 folds, fits HMM regime + GARCH volatility per fold, computes OFI signals, runs a real z-score mean-reversion simulation with a cost model, runs Monte Carlo projections, logs to MLflow. (`app/workers/backtest_worker.py:27-182`) Caveat: the simulated trading rule is a generic fixed z-score strategy applied to every backtest regardless of the specific `Strategy`'s own config/hypothesis — the strategy's actual logic is never consulted during simulation.

### Darwin evolution
Really implemented, not just a doc claim: ranks live strategies by fitness, retires bottom 20% below a Sharpe threshold, spawns mutated children from top 20% (Gaussian noise on numeric params), auto-queues their backtests, runs nightly via Celery beat. (`app/workers/backtest_worker.py:185-250,507-524`)

### Quant math library (`app/services/quant_engine.py`, 1485 lines)
Real, not decorative: HMM regime detection via `hmmlearn.GaussianHMM` with bootstrap CI and an honestly-labeled momentum-fallback path (`"MOMENTUM_FALLBACK"`) if the library is unavailable; GARCH(1,1) volatility via `arch_model` with a `"ROLLING_STD"` fallback; real Monte Carlo path simulation; HRP/CVaR portfolio allocation via `PyPortfolioOpt`/`cvxpy`; ADF stationarity test for signal conflict; tsfresh/LOF/NetworkX-based feature and outlier computation. Note: docstrings advertise "GARCH/EGARCH" but only plain GARCH(1,1) is ever called — EGARCH does not exist in code, only in comments.

### Market data integration (`app/services/market_data_service.py`, 1077 lines)
Real exchange calls — `ccxt.async_support` for crypto, OANDA REST v20 for forex, `yfinance` for equities/indices. No synthetic/random data generation found; failures return explicit error/empty responses.

### Why-not-trade constraint engine
Real 7-constraint engine combining HMM regime, LOF-based data quality, feed staleness, OFI stop-hunt/vacuum detection, live spread/liquidity, position concentration, and kill-switch state — all derived from DB + live market data. (`app/services/intelligence/why_not_trade_service.py`)

### Ingestion pipeline (crypto/forex/stocks providers → DQ → candles → DB)
`orchestrator.py`, `dq_pipeline.py` (real rolling-window spike/duplicate/outlier detection), `candle_aggregator.py`, `market_db_writer.py`, `db_writer.py`, `retension_task.py` are all real and wired together via the `ingestor` Docker Compose service.

### Redis pub/sub backbone
Real singleton client, 30-channel subscription fan-out to WebSocket manager, publisher writing to pub/sub + list buffer + optional Kafka, started as a background task at app startup. (`app/core/redis.py`, `app/core/pubsub.py`, `app/services/publisher.py`, `main.py:31`)

### Data models & migrations
21 real SQLAlchemy models, a clean linear 8-migration Alembic chain with no branch heads, all tables accounted for.

---

## 2. Partially Implemented — real logic present, but broken wiring or hardcoded pieces

These are the highest-value findings: code that *looks* finished (real functions, real math) but fails at runtime or silently returns canned values.

| Area | What's real | What's broken/stubbed |
|---|---|---|
| **Order submit/cancel** (`app/services/order_service.py`) | Full risk-gate → broker → fill flow design | Calls `order.transition(...)` (lines 111,139,142,145,191) but `Order` has **no `transition()` method anywhere in the codebase** — every `POST /orders` and `DELETE /orders/{id}` raises `AttributeError` at runtime. |
| **Risk metrics endpoint** (`GET /risk/metrics`) | `risk_service.compute_risk_metrics()` (risk_service.py:78-200) is a genuinely sophisticated VaR/CVaR/GARCH/drawdown/leverage calculator | `risk.py:25` calls it with swapped/wrong-typed arguments (`(db, user_id)` vs. the real signature `(current_user, db)`) — guaranteed `AttributeError`/`TypeError`. Same bug on `POST /risk/killswitch` (risk.py:125 vs. risk_service.py:30-34). |
| **Kill switch** | Once reachable, real logic: cancels DB orders, closes DB positions, writes alert + audit | Blocked by the same `transition()` bug; also never actually calls the broker to cancel orders — DB-only. |
| **Portfolio/position metrics** (`positions.py`) | Real equity/PnL/drawdown from `PnLSnapshot`/`Position` | `sharpe=1.84` and `win_rate=58.3` are hardcoded literals with a comment "computed by Darwin engine in production"; `drawdown_limit=15.0` hardcoded instead of read from `RiskLimit`. Also duplicates `positions_service.py`'s logic inline rather than calling it. |
| **Intelligence REST endpoints** (`app/api/v1/endpoints/intelligence.py`) | 1039 lines, 38 routes, real compute services underneath | **25 of 38 endpoints call `get_redis("<name>", user.id)`, but `get_redis()` takes zero arguments** (`app/core/redis.py:9`) — guaranteed `TypeError` on every call to `/decision/current`, `/regime/current`, `/ofi`, `/gmig/snapshot`, `/montecarlo`, `/adaptation/feed`, `/alpha/state`, `/signal-conflict`, `/features`, `/command-center/current`, `/scenarios/simulations`, `/traces`, `/why-not-trade`, etc. Even if the signature were fixed, the Redis key names they'd request (e.g. `"decision_feed"` keyed by user id) don't match what `intelligence_worker.py` actually writes (`decision_feed:{symbol}`, keyed by symbol) — a second independent bug. Endpoints that bypass this pattern (`/market/*`, `/rejection-stats`, `/quant-core/gates`, `/ws/market`, `/stream`, `/notifications/*`) are real and functional. |
| **MarketTick ingestion** | Ingestion pipeline writes real OHLCV to `Candle1m`/`Candle1h` | **The `MarketTick` table is never written by any code path** in the repo (only ever `SELECT`ed). Since nearly every intelligence service (`decision`, `why_not_trade`, `regime`, `features`, `ofi`, `signal_conflict`, `montecarlo`, `scenarios`, `cross_market`) sources its price series via `recent_ticks()` against `MarketTick`, the entire intelligence layer runs on a data source that's structurally never populated — it will return "no_market_data" fallbacks in a real deployment unless something outside this repo backfills the table. |
| **`regime_service.compute_regime_current`** | Real regime/duration history logic | `await`s `compute_technical_indicators`, which is a synchronous function — raises `TypeError`, silently swallowed by a bare `except: pass`, so the `"technicals"` field is always empty in practice. |
| **`cross_market_service.compute_gmig_enhanced`** | — | References `symbols` before it's defined (used at line 164, defined at line 188) — guaranteed `NameError`. Confirmed dead (never called elsewhere), but still broken if it ever is. |
| **`capital_service`** | Real HRP allocation fed by actual tick returns | `gmig_modifier: 1.0` hardcoded despite implying a cross-market adjustment that's never computed; `compute_rebalance` creates a placeholder `BacktestJob` row instead of dispatching a real Celery task (docstring admits this). |
| **`decision_service` / `command_center_service`** | Real DB + live market data aggregation, real 8-gate pipeline call | `"behavior_score": 85` hardcoded in both; `compute_decision_traces` fabricates confidence scores/logic strings from `order.status` templates rather than deriving them from actual gate history. |
| **`adaptation_service.compute_adaptation_drift`** | Real DB-driven event derivation | Fallback path fabricates `live_sharpe = bt_sharpe * (0.85 + 0.1*i/len(jobs))` — an arbitrary formula presented as a live metric when no primary symbol/ticks exist. |
| **TWAP / VWAP / ICEBERG / OCO order types** | Accepted as valid enum values through the whole order pipeline | **Zero slicing/algorithmic execution logic anywhere.** `PaperAdapter.submit_order()` always instantly fills the full quantity regardless of order type — these are enum labels with no behavior behind them. |
| **Broker types IBKR / OANDA / LMAX / CUSTOM / MT5** | Advertised in `BrokerTypeEnum` and README | No adapter class exists for any of them; `get_adapter()` silently falls back to `PaperAdapter` for any unmapped type — a user who configures "IBKR" gets fake instant fills with no warning. |

---

## 3. Dead / Orphaned Code (present in repo, never reached by the running app)

- **A second, entirely disconnected broker-execution stack**: `app/services/brokers/{broker_router.py,risk_gate.py,mt5/*}`, `app/services/execution/{execution_service.py,smart_router.py}`, backed by an in-memory `app/db/order_store.py` (its own docstring says "Replace with SQLAlchemy + Postgres for production"). Never imported by `app/api/v1/router.py` or `main.py`. Contains a literal hardcoded stub string (`smart_router.py:28`: `"CCXT architecture pipeline initialization pending."`) and broken imports missing the `app.` package prefix. The `order_store.py`/`risk_gate.py` code also calls `.model_copy()` and reads a `.nonce` attribute on `Order` as if it were a Pydantic model — it isn't, and would crash immediately if ever wired in. The MT5 WebSocket bridge itself (`mt5/adapter.py`) is well-written but unreachable.
- **`app/workers/decision_stream_worker.py`** — real logic, but hardcodes its own Redis URL and symbol list, isn't imported anywhere, and isn't in `docker-compose.yml` — an orphaned duplicate of `intelligence_worker.py`.
- **`app/workers/tick_ingestor.py`**'s `publish_tick()` — never called anywhere; the live path uses `app/services/publisher.py` instead.
- **`celery_app.py`'s `beat_schedule`** is fully overwritten (not merged) by `backtest_worker.py:519-524` at import time — the schedule defined in `celery_app.py` never actually runs.
- **`app/services/intelligence/indicators_service.py`** — confirmed 0 bytes, empty placeholder file.

---

## 4. Not Implemented (confirmed absent, matches prior docs)

Repo-wide search confirms these referenced-in-spec concepts do not exist anywhere in code:
- Mechanism Observatory (mutual information / spectral independence, `N_eff` formal estimator, research priority score, mechanism hierarchy)
- Quant Core Orchestrator as an 8-*engine* combiner (the "8-gate" *pipeline* terminology in `decision_service`/`command_center_service`/`intelligence.py` is real and distinct from this — don't conflate the two)
- Optuna / TPESampler hyperparameter optimization, walk-forward validator as a standalone gated component
- GJR-GARCH, EGARCH (only plain GARCH(1,1) exists despite comments)
- Smart Order Router features: venue selection, anti-PFOF, adverse-selection scoring, anti-front-running
- Execution algorithm suite (TWAP/VWAP/POV/Iceberg/Sniper as actual slicing algorithms, not enum labels)
- Prometheus/Grafana/OpenTelemetry/AlertManager monitoring stack
- Portfolio optimization beyond the HRP/CVaR primitives in `quant_engine.py` (no Black-Litterman, no efficient frontier, no factor/attribution analysis)
- Fundamental data, news sentiment, alternative data, on-chain data feeds
- ML model registry, drift detection, online learning, NLP/RL components
- Visual strategy builder, custom scripting language, strategy import/export
- Multi-tenancy, social/copy trading, paper-trading virtual cash mode, PDF/Excel reporting, mobile push notifications

---

## 5. Infrastructure Gaps (deployment/process, not feature code)

- **No migration step anywhere in the deploy path.** Dockerfile/compose/CI never run `alembic upgrade head`; the only schema-provisioning mechanism actually exercised is `Base.metadata.create_all`, called manually by `scripts/seed_users.py` and `tests/conftest.py`. The Alembic chain is clean but effectively unused.
- **CI runs no tests.** `.gitlab-ci.yml` has a single `build`-only stage (docker build/push on tag), no test stage at all.
- **Test suite is thin and skip-heavy.** 19 tests collect cleanly, but most auth/broker/order/strategy tests `pytest.skip()` on missing seed data or assert broad status-code ranges (e.g. `in (404, 422, 403)`) rather than specific behavior — green mostly means "didn't 500," not "correct." `conftest.py` requires a live local Postgres and errors (not skips) if unavailable.
- **`scripts/seed_users.py` is never invoked automatically** — not called from `main.py`, compose, or CI; must be run by hand.
- **Config/env drift**: `.env.example` documents `DATABASE_URL`/`DATABASE_URL_SYNC` as settable, but these are computed properties in `config.py`, not real fields — silently ignored due to `extra="ignore"`. Conversely ~20 real config vars (Alpaca/OANDA/Kafka/MLflow/DQ thresholds/retention windows) are undocumented in `.env.example`.
- **Dependency risk**: `requirements.txt` is a raw 288-package freeze including large unrelated stacks (torch, jax, full CUDA/nvidia-*, Flask, litestar, huey, peewee, GitPython). It pins `psycopg[binary]` (psycopg 3) while `config.py` builds a `postgresql+psycopg2://` sync URL requiring the separate `psycopg2` package — plausible import-time failure for the Celery worker unless something else transitively pulls in `psycopg2`.
- **Sloppy-code fingerprints** suggesting AI-generated or copy-pasted sections: `from gunicorn.config import User` (unused, wrong `User`) in `regime_service.py` and `command_center_service.py`; `from gitdb import db` (unused, unrelated package) in `intelligence.py`; debug `print()` statements left in `brokers.py`'s `add_broker`.
- **`api_doc.md` is not real API documentation** — it covers only 3 of the router's ~109 actual endpoints (market ingestion architecture notes only).

---

## Priority Fix List (highest leverage, ranked by blast radius)

1. **`Order.transition()` missing** — blocks all order submit/cancel/kill-switch. One method addition unblocks the entire trading path.
2. **`get_redis()` signature mismatch in `intelligence.py`** — 25 of 38 intelligence endpoints 500 on every call. Fix the call sites or the function signature, then reconcile the Redis key naming mismatch with `intelligence_worker.py`.
3. **`risk.py` argument order/type mismatch** calling `compute_risk_metrics`/`trigger_kill_switch` — trivial fix, currently makes real risk logic unreachable.
4. **`MarketTick` never written** — decide whether to write raw ticks alongside candles, or refactor intelligence services to read from `Candle1m` instead; currently the entire intelligence layer is data-starved by construction.
5. Decide the fate of the orphaned `app/services/brokers/*` + `app/services/execution/*` + `order_store.py` subtree — either wire it in properly or delete it; right now it's dead code that will confuse future contributors into thinking two competing execution paths are both live.
6. Add an `alembic upgrade head` step to the deploy path (Dockerfile CMD or an entrypoint script) so schema provisioning doesn't depend on `create_all` scripts being run by hand.
7. Add a real test stage to `.gitlab-ci.yml`.

---

*Generated from direct source inspection across `app/api`, `app/services`, `app/workers`, `app/models`, `alembic/versions`, `tests/`, and deployment config. Supersedes the implementation-depth claims (not the feature inventory) in `features-proposal.md` and `PiOSQ_Specification.md` — those remain useful for the high-level feature list and the "not yet built" roadmap items.*
