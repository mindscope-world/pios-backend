# Pi OS Backend — Implementation Plan

Scope: this plan covers only the two categories that are not already done — **Partially Implemented (Broken Wiring)** items and **Not Yet Implemented** items — plus the Dead Code and Infrastructure items that block them. It does not re-litigate what's already `WORKING` (see `project_dev.md` §1 / OpenCurrent Labs report §3).

It reconciles three prior documents so this becomes the single source of truth for "what to build next":
- `project_dev.md` — the source-level audit (this session)
- `Pi_OS_Development_Status_and_10Day_Plan.pdf` — OpenCurrent Labs' 10-day stabilization plan for the same findings
- `development-priorities.md` — the original 16–24 week roadmap for spec-compliance features (written before this audit; several of its premises are corrected below, e.g. it proposes building an 8-gate orchestrator from scratch when a working 8-gate pipeline already exists in `decision_service.py`/`command_center_service.py`)

Two tracks run in sequence, not in parallel: **Track A (Days 1–10)** repairs broken wiring so the platform is demonstrable. **Track B (Weeks 3+)** builds genuinely unbuilt capability on top of a now-stable base. Attempting Track B work before Track A is complete would mean building new features on top of endpoints that 500 on every call.

---

## Track A — Broken Wiring Repairs (Days 1–10)

Each item follows the same shape: root cause → fix → files → acceptance criteria → effort. This is the same backlog as the OpenCurrent Labs report §4/§7, expanded to implementation detail.

### A1. Order submit/cancel/kill-switch crash — CRITICAL

**Root cause**: `app/services/order_service.py` calls `order.transition(...)` at 5 call sites (lines 111, 139, 142, 145, 191) and `risk_service.py:44` does the same, but `Order` (`app/models/all_models.py:268`) is a plain SQLAlchemy model with no `transition()` method anywhere in the codebase.

**Fix**:
1. Add a `transition(self, new_status: OrderStatusEnum, reason: str | None = None) -> None` method to `Order` that validates the transition against an explicit allowed-transitions map (e.g. `PENDING → {SUBMITTED, REJECTED}`, `SUBMITTED → {FILLED, PARTIALLY_FILLED, CANCELLED, REJECTED}`, etc.), sets `status`, appends an `OrderEvent` row, and raises `InvalidTransitionError` on an illegal move rather than silently no-op'ing.
2. Audit all 5+ call sites to confirm they pass a real `OrderStatusEnum` value, not a string.
3. Write a unit test per transition path (valid and invalid) directly against the model, independent of the API layer.

**Files**: `app/models/all_models.py` (add method + exception class), `app/services/order_service.py`, `app/services/risk_service.py`, new `tests/test_order_transitions.py`.

**Acceptance criteria**: `POST /orders`, `DELETE /orders/{id}`, and `POST /risk/killswitch` all succeed against the paper adapter without raising `AttributeError`; an illegal transition (e.g. cancelling an already-filled order) returns a 4xx with a clear message, not a 500.

**Effort**: 1.5 days (Days 2–3, per OpenCurrent Labs schedule — includes end-to-end verification against paper, then Alpaca/CCXT sandbox).

---

### A2. Risk metrics & kill-switch argument mismatch — CRITICAL

**Root cause**: `risk_service.compute_risk_metrics(current_user: User, db: AsyncSession)` is called as `compute_risk_metrics(db, current_user.id)` in `risk.py:25` — arguments swapped and wrong types (`UUID` where `User` expected). Same pattern for `trigger_kill_switch`: defined `(current_user, db, data, user_email)`, called as `(db, data, admin.id, admin.email)` in `risk.py:125`.

**Fix**:
1. Fix both call sites in `app/api/v1/endpoints/risk.py` to match the real signatures.
2. Standardize the parameter order convention across all service functions in this file going forward (`db` first, consistent with the rest of the codebase's endpoint→service convention) to prevent recurrence, and add a lightweight signature-consistency check (mypy or a simple call-site grep in CI) so a mismatch fails fast next time rather than at runtime.
3. Extend the kill switch to actually call the broker adapter to cancel orders remotely (currently DB-only — positions are flagged closed and orders cancelled locally, but nothing is sent to the broker). Add `broker_service.get_adapter(...).cancel_order(...)` calls inside the kill-switch loop, with a per-order try/except so one broker failure doesn't abort the whole sweep.

**Files**: `app/api/v1/endpoints/risk.py`, `app/services/risk_service.py`.

**Acceptance criteria**: `GET /risk/metrics` returns real VaR/CVaR/drawdown/leverage numbers against seeded test data; `POST /risk/killswitch` cancels orders both in the DB and (for Alpaca/CCXT brokers) at the broker, and is verified against a known test scenario with expected VaR values.

**Effort**: 1 day (Day 4).

---

### A3. Intelligence REST endpoints — double bug, 25 of 38 routes — CRITICAL

**Root cause (two independent bugs stacked)**:
1. `app/core/redis.py:9` defines `get_redis() -> redis.Redis` with **zero parameters**, but 25 call sites in `app/api/v1/endpoints/intelligence.py` call it as `get_redis("<name>", user.id)` — guaranteed `TypeError`.
2. Even once fixed, the intended Redis key names these endpoints request (e.g. `"decision_feed"` keyed by `user.id`) don't match what `intelligence_worker.py` actually writes (`f"decision_feed:{symbol}"`, keyed by symbol, e.g. `decision_feed:BTCUSDT`).

**Fix**:
1. Introduce a small helper `async def get_intelligence_key(key_prefix: str, symbol: str) -> dict | None` in `app/core/redis.py` that does `client = get_redis(); raw = await client.get(f"{key_prefix}:{symbol}"); return json.loads(raw) if raw else None` — this makes the symbol-keyed convention explicit and reusable rather than ad hoc per endpoint.
2. Update all 25 broken call sites in `intelligence.py` to resolve a symbol first (most already accept a `symbol` query param or default to the user's primary symbol via `helpers.primary_symbol()`), then call the new helper with the correct key prefix matching what `intelligence_worker.py` writes for that data type.
3. Add one integration test that starts a fake Redis key for each of the 14 intelligence-worker-populated keys and asserts each corresponding GET endpoint returns it — this is the single highest-value test to add since it would have caught both bugs immediately.

**Files**: `app/core/redis.py`, `app/api/v1/endpoints/intelligence.py` (25 call sites — grep `get_redis(` to enumerate), new test in `tests/test_intelligence_endpoints.py`.

**Acceptance criteria**: all 38 routes in `intelligence.py` return 200 with real payloads (not `TypeError`/`NameError`) when Redis has been populated by the worker for a known symbol.

**Effort**: 1 day (Day 6).

---

### A4. MarketTick never written — STRUCTURAL

**Root cause**: `grep -rn "MarketTick(" app/` finds only the model definition — every reference elsewhere is a `SELECT`. The live ingestion path (`tick_ingestor.py` → `dq_pipeline.py` → `candle_aggregator.py` → `db_writer.py`) writes only `Candle1m`/`Candle1h`, never `MarketTick`. Nearly every intelligence service reads its price/volume series via `helpers.recent_ticks()` against `MarketTick` — so the intelligence layer is structurally data-starved.

**Fix — this requires an owner decision (see OpenCurrent Labs report §9) between two options:**

**Option 1 — Write raw ticks going forward** (higher fidelity, more storage/write load):
- Add a `MarketTick` insert to `db_writer.py`'s batch-flush path alongside the existing `Candle1m` upsert, sourced from the same in-memory buffer already populated by `market_db_writer.py`'s consumer loop.
- Add a retention policy for `MarketTick` to `retension_task.py` (raw ticks are high-volume; keep a much shorter window than candles, e.g. 24–72h, matching what tick-level analytics actually need).
- Pros: enables genuine tick-level analytics (OFI, LOF outlier detection, spike detection) exactly as `quant_engine.py` and the DQ pipeline were designed to consume. Cons: meaningfully more DB write volume and storage; needs a retention/partitioning strategy from day one, not bolted on later.

**Option 2 — Redirect intelligence services to read from `Candle1m`** (lower effort, lower fidelity):
- Change `helpers.recent_ticks()` (and equivalent call sites in `regime_service.py`, `ofi_service.py`, `features_service.py`, `why_not_trade_service.py`, `signal_conflict_service.py`, `cross_market_service.py`) to query `Candle1m` instead, adapting field access (`close`/`volume` instead of `price`/`volume`).
- OFI (order-flow imbalance) specifically loses fidelity under this option since it depends on tick-level buy/sell splits that don't exist at 1-minute-candle granularity — this would need to degrade to a candle-based proxy or be flagged as reduced-accuracy.
- Pros: no schema/write-path change, ships faster. Cons: permanently caps intelligence-layer accuracy at 1-minute resolution; OFI-dependent features (crowding, stop-hunt detection in `why_not_trade_service.py`) become notably weaker.

**Recommendation**: Option 1 if the platform's differentiator is genuinely tick-level microstructure signals (OFI, LOF); Option 2 if the near-term goal is "endpoints return real, non-fake data" and tick-level precision can wait for a later phase. Given the audit found OFI/microstructure work already exists and is meaningful (`quant_engine.py:684-757`), Option 1 is the more defensible default — but this is explicitly an owner call, not a technical one.

**Files (Option 1)**: `app/workers/db_writer.py`, `app/workers/retension_task.py`, possibly a new migration for a partitioned/time-bucketed `market_ticks` table if raw tick volume is large enough to need it.

**Acceptance criteria**: `SELECT count(*) FROM market_ticks WHERE created_at > now() - interval '5 minutes'` returns a nonzero, growing count under live ingestion; `why_not_trade_service`, `ofi_service`, `regime_service` etc. no longer return `"no_market_data"` fallbacks in a running environment with active ingestion.

**Effort**: 1 day for Option 2, 2–3 days for Option 1 including retention policy (Day 5 in the 10-day plan assumes Option 2's speed; Option 1 should be flagged as a likely spillover into Track B if chosen).

---

### A5. Regime computation async bug — MODERATE

**Root cause**: `regime_service.py:88` does `tech = await compute_technical_indicators(...)`, but `compute_technical_indicators` (`market_data_service.py:627`) is a synchronous `def`. Awaiting a non-awaitable raises `TypeError`, silently swallowed by a bare `except Exception: pass` at lines 89-90 — so `"technicals"` is always empty with no visible error.

**Fix**: remove the `await` (call synchronously), or if the intent was to run it off the event loop (it does real numeric work over full OHLCV history), wrap it in `asyncio.to_thread(...)`. Replace the bare `except Exception: pass` with a logged exception at minimum, so future breakage is visible instead of silent.

**Files**: `app/services/intelligence/regime_service.py`.

**Effort**: 0.25 day (part of Day 7 sweep).

---

### A6. Hardcoded/broken metrics across intelligence & portfolio services — MODERATE

Bundle of small, independent fixes best done together as one sweep (Day 7):

| Bug | File:line | Fix |
|---|---|---|
| `sharpe=1.84`, `win_rate=58.3` hardcoded | `positions.py:115-116`, `positions_service.py:106-107` | Compute from `PnLSnapshot`/`Fill` history (rolling Sharpe already has a formula in `helpers.py` per README — reuse it) or, if not yet reliable, return `null` with an explicit `"status": "insufficient_history"` rather than a fabricated number. |
| `drawdown_limit=15.0` hardcoded | `positions.py` | Read from `RiskLimit` table for the user/strategy, matching the pattern already used correctly in `risk_service.py`. |
| `compute_gmig_enhanced` `NameError` (`symbols` used before definition) | `cross_market_service.py:164` (defined line 188) | Move the definition above first use; add a regression test since this function is currently unreachable and untested. |
| `"behavior_score": 85` hardcoded | `decision_service.py:284`, `command_center_service.py:283` | Either wire to the real scoring already implemented in `behavior_service.py` (it computes a genuine override-rate/frequency-deviation score) instead of duplicating a fake constant, or remove the field until wired. |
| Decision-trace confidence/logic templated from `order.status` text | `decision_service.py:457-533` | Derive from actual recorded gate results (the 8-gate pipeline already logs pass/fail per gate) rather than a status-string template. |
| `gmig_modifier: 1.0` hardcoded | `capital_service.py:101,107,116` | Wire to `cross_market_service.compute_gmig_snapshot` output once A6's NameError fix lands, or remove the field. |
| Synthetic `live_sharpe = bt_sharpe * (0.85 + 0.1*i/len(jobs))` | `adaptation_service.py:166` | Replace with real live-vs-backtest Sharpe comparison once `PnLSnapshot` history exists for the strategy, gated on `MarketTick`/candle-driven positions actually accumulating equity history (depends on A4). |
| `compute_rebalance` writes a placeholder `BacktestJob` row instead of dispatching a real Celery task | `capital_service.py:145-151` | Replace with an actual `celery_app.send_task(...)` call matching the existing `backtest_worker` task-dispatch pattern used elsewhere in the codebase. |
| Dead/wrong imports (`from gunicorn.config import User`, `from gitdb import db`) | `regime_service.py:4`, `command_center_service.py:5`, `intelligence.py:19` | Delete — confirmed unused, remove to stop confusing future readers/linters. |
| Debug `print()` left in | `brokers.py` `add_broker` (lines 61,64-66) | Remove or convert to `logger.debug(...)`. |

**Effort**: 1 day (Day 7).

---

### A7. TWAP/VWAP/Iceberg/OCO accepted with no execution behavior — MODERATE (bridges into Track B)

**Root cause**: These are valid `OrderTypeEnum` values accepted through the whole order pipeline, but no slicing/algorithmic logic exists anywhere — `PaperAdapter.submit_order()` always instantly fills the full quantity regardless of type, with no indication to the caller that this is happening.

**Track A fix (stabilization, not full build)**: make the gap honest rather than silent.
1. Add a `execution_style: "INSTANT" | "ALGORITHMIC"` field to the order response, defaulting to `"INSTANT"` for all types until real algos exist (A7 continues in Track B §B-Execution below).
2. Per OpenCurrent Labs report §9, get an explicit owner decision: label these order types as "market-order-equivalent" in the API/response until real slicing ships, or remove them from the selectable enum for now. Either is a few hours of work; silently accepting them with no behavior is the one option to rule out immediately.

**Files**: `app/schemas/all_schemas.py` (response field), `app/api/v1/endpoints/orders.py` docstring/validation, `app/models/all_models.py` (`OrderTypeEnum` if removing options).

**Effort**: 0.5 day for the labeling fix; real algorithmic execution is Track B (§B-Execution, multi-week).

---

### A8. Unsupported broker types silently fall back to paper trading — MODERATE

**Root cause**: `ADAPTER_MAP` (`broker_service.py:211-216`) only contains `ALPACA/BINANCE/CCXT/PAPER`; `get_adapter()` does `ADAPTER_MAP.get(broker.broker_type.upper(), PaperAdapter)` — any unmapped type (IBKR, OANDA, LMAX, CUSTOM, MT5) silently gets fake instant fills.

**Fix (Track A, stabilization only — real adapters are Track B)**:
1. Change the fallback from silently returning `PaperAdapter` to raising a clear `UnsupportedBrokerError` (surfaced as a 4xx at the API layer) when a broker's type has no real adapter, **unless** the broker was explicitly created with a `paper_mode: true` flag.
2. Add that `paper_mode` flag to `Broker` (new column, small migration) so intentional paper-trading is distinguished from "silently pretending to support a broker we don't."

**Files**: `app/services/broker_service.py`, `app/models/all_models.py` (+migration), `app/api/v1/endpoints/brokers.py`.

**Effort**: 0.5 day. Real adapters for IBKR/OANDA execution/LMAX/MT5 are Track B (§B-Brokers).

---

## Track A Summary Timeline

| Day | Item(s) | Owner decisions needed |
|---|---|---|
| 1 | Environment setup, reproduce all bugs above, confirm scope | Dead-code keep/delete (Track A/B split below) |
| 2–3 | A1 Order lifecycle | — |
| 4 | A2 Risk engine | — |
| 5 | A4 MarketTick (Option 1 vs 2) | **Required**: which option |
| 6 | A3 Intelligence endpoints | — |
| 7 | A5, A6 secondary bug sweep | — |
| 8 | Dead code resolution + Celery beat schedule overwrite fix (see below) | **Required**: remove vs. complete orphaned execution stack |
| 9 | Deploy/CI hardening (migrations in deploy path, CI test stage, dependency conflict, env docs) | — |
| 10 | Full regression pass, updated status report, handover | — |

---

## Dead Code Resolution (Day 8)

Per OpenCurrent Labs report §5 and §9, recommendation is **removal, not completion**, for the 10-day window:

- Remove `app/services/brokers/*` (broker_router.py, risk_gate.py, mt5/*), `app/services/execution/*` (execution_service.py, smart_router.py), `app/db/order_store.py` — the real order path (`order_service.py`, fixed in A1) is the one to bring to production quality, not this parallel stack. Exception: `mt5/adapter.py`'s WebSocket bridge logic is well-written and worth preserving as a reference/starting point when MT5 support is actually scoped in Track B — move it to a `docs/reference/` or a feature branch rather than deleting outright, but remove it from the live `app/services/` tree.
- Remove `app/workers/decision_stream_worker.py` (orphaned duplicate of `intelligence_worker.py`, hardcodes its own Redis URL/symbol list, not deployed).
- Remove the dead `tick_ingestor.py:publish_tick()` function (never called; `publisher.py` is the live path) — or the whole file if nothing else in it is used.
- Fix the Celery beat schedule overwrite: `backtest_worker.py:519-524` replaces `celery_app.conf.beat_schedule` wholesale instead of merging with the 2 entries defined in `celery_app.py:14-28`. Consolidate into one schedule definition (recommend keeping it in `celery_app.py` since that's the conventional location, and have `backtest_worker.py` add its entries via `.update()` rather than assignment).
- Delete `app/services/intelligence/indicators_service.py` (confirmed empty) or implement it as the real consolidation point for technical indicators — recommend deferring to Track B since `market_data_service.compute_technical_indicators` already serves this need adequately for now; don't build a facade with nothing behind it.

---

## Infrastructure & Process Hardening (Day 9)

| Gap | Fix |
|---|---|
| No migration step in deploy path | Add `alembic upgrade head` to the Docker image's entrypoint (or a dedicated init container/job in `docker-compose.yml`) so schema provisioning doesn't depend on manually running `scripts/seed_users.py`'s `create_all`. |
| No CI test stage | Add a `test` stage to `.gitlab-ci.yml` that spins up Postgres+Redis services, runs `pytest tests/ -v`, and gates the `build` stage on it passing. |
| Dependency conflict risk | `requirements.txt` pins `psycopg[binary]` (v3) while `config.py` builds a `postgresql+psycopg2://` sync URL. Confirm which the sync engine actually needs and pin `psycopg2-binary` explicitly rather than relying on a transitive pull-in. Also worth a follow-up pass to prune the 288-package freeze down to actual imports (torch/jax/CUDA/Flask/litestar/huey/peewee/GitPython appear unused) — lower priority than the correctness fix, but flagged here since it's the same file. |
| Config/env drift | Remove `DATABASE_URL`/`DATABASE_URL_SYNC` from `.env.example` (they're computed properties, not real settings — currently silently ignored) and add the ~20 undocumented real vars (Alpaca/OANDA/Kafka/MLflow/DQ thresholds/retention windows) with sensible defaults documented inline. |
| Thin, skip-heavy tests | Not fully fixable in one day — Day 9/10 should at minimum replace the broadest assertions (`status_code in (404,422,403)`) in `test_orders.py`/`test_strategies.py` with specific expected codes now that A1/A2 make the underlying endpoints actually work, and remove the `pytest.skip()` fallbacks now that CI provisions a seeded test DB via A9's CI fix. |

---

## Track B — Roadmap for Genuinely Unbuilt Features (post Day 10)

Everything here was confirmed absent by repo-wide search. This is real, multi-week engineering — not wiring fixes — and should not start until Track A ships, since building new intelligence/execution features on top of currently-crashing endpoints would just compound the debugging surface. Phases are ordered by dependency and value, adapted from `development-priorities.md`'s original plan but corrected against what the audit found already exists.

### Phase 1 (Weeks 3–6): Execution capability — closes A7/A8's gaps for real

**B-Execution: Algorithmic order types**
- Implement TWAP, VWAP, POV, Iceberg, Sniper-Limit as real slicing algorithms operating on the existing `order_service.py` submit path — not a parallel stack (learn from the Track A dead-code decision: one execution path, not two).
- Each algo needs: a scheduler (Celery-beat-driven slice submission, or an asyncio task per active algorithmic order), partial-fill aggregation back onto the parent `Order`, and a cancel-in-flight path.
- Files: new `app/services/execution/` package (reusing the *name* but not the orphaned code removed in Track A), `app/workers/execution_scheduler.py` (new), extends `order_service.py`.
- Depends on: A1 (order transitions) and A6 (fill aggregation) being solid first.
- Estimated: 3–4 weeks for TWAP/VWAP/POV, +1 week for Iceberg/Sniper.

**B-Brokers: Additional adapters**
- IBKR (via `ib_insync` or native FIX), OANDA execution (the market-data side already exists — extend to real order placement), LMAX, and MT5 (re-integrate the preserved `mt5/adapter.py` WebSocket bridge from the Track A dead-code decision — this is genuinely closer to done than a from-scratch build).
- Each adapter implements the same `BrokerAdapter` interface as the working Alpaca/CCXT ones (`test_connection`, `get_account`, `submit_order`, `cancel_order`, `get_positions`, `get_fills` — see README's "Adding a New Broker" section, which is accurate and should be followed as-is).
- Estimated: 1–2 weeks per adapter depending on SDK maturity; MT5 is fastest since the bridge logic already exists and mainly needs re-wiring + testing.

### Phase 2 (Weeks 5–10): Quant capability deepening

**B-Volatility: GJR-GARCH / EGARCH**
- `quant_engine.estimate_volatility_garch` currently only calls plain `arch_model(vol="Garch")`. Extend to support `vol="EGARCH"` and GJR asymmetric variants per asset class, matching what the docstrings already (inaccurately) claim.
- Low risk — this is additive to an already-working function, not new architecture.
- Estimated: 1 week.

**B-Optimization: Optuna + walk-forward validation**
- The backtest engine (`backtest_worker.py`) already does walk-forward folds manually; formalize this into a reusable `WalkForwardValidator` and add Optuna (`TPESampler`) for hyperparameter search per strategy, gating PAPER→LIVE_SMALL promotion on OOS Sharpe/drawdown thresholds in addition to the existing gate checks in `strategy_service._check_gate()`.
- Depends on: the backtest engine's current "same generic z-score rule for every strategy" limitation (noted in `project_dev.md` §1) — worth fixing alongside this phase so hyperparameter search actually tunes each strategy's own logic rather than a shared placeholder rule.
- Estimated: 2–3 weeks.

**B-Portfolio: Beyond HRP/CVaR**
- Add Black-Litterman with GMIG-derived views (once `cross_market_service` is fixed and reliable per A6), factor exposure/attribution analysis, efficient frontier computation. Builds directly on the already-real `PyPortfolioOpt`/`cvxpy` usage in `quant_engine.py`.
- Estimated: 2–3 weeks.

### Phase 3 (Weeks 9–16): Institutional differentiators

**B-Mechanism Observatory** — the largest single item, correctly deprioritized behind Phases 1–2 since nothing else depends on it yet:
- Mutual-information + spectral-overlap mechanism independence scoring, `N_eff` participation-ratio estimator with bootstrap CI, research priority score, mechanism registry/hierarchy.
- Note the correction to `development-priorities.md`'s original framing: this remains fully unbuilt and is the correct scope — no changes needed to that document's technical approach, just its sequencing (it assumed this was P0; the wiring-bug audit makes clear it should follow Track A + Phases 1–2).
- Estimated: 6–7 weeks per the original plan's own breakdown, largely still valid.

**B-Orchestrator formalization**: the audit found a real, working 8-gate *decision pipeline* already in `decision_service.py`/`command_center_service.py`/`build_quant_core_gates`. Do not build a second, parallel "Quant Core Orchestrator" from scratch as `development-priorities.md` originally proposed — instead, formalize and harden the existing pipeline (add the Correlation and Crowding gates it's currently missing, tighten latency, make gate results persisted/auditable rather than derived ad hoc) so it becomes the single orchestration layer. This meaningfully de-scopes what was previously estimated as a 7-week from-scratch build.
- Estimated: 3–4 weeks (reduced from original 7, since the core pipeline already exists).

**B-Smart Order Router**: venue selection, adverse-selection scoring, anti-front-running, anti-PFOF. Depends on Phase 1's multi-broker adapters existing first (nothing to route between with only 2 broker types).
- Estimated: 3–4 weeks.

### Phase 4 (Weeks 15+): Operationalization & long-tail features

- Monitoring stack (Prometheus/Grafana/OpenTelemetry/AlertManager) — 2–3 weeks, can actually start in parallel with Phase 2/3 since it's independent instrumentation work, not sequential.
- Data expansion (fundamental data, news sentiment, alt data, on-chain) — scope per business priority, not a fixed estimate.
- ML pipeline (model registry, drift detection, online learning) — depends on Phase 2's Optuna/experiment-tracking groundwork.
- UX/collaboration features (visual strategy builder, reporting, multi-tenancy, social trading) — product-prioritization dependent, no technical blockers from this audit.
- Compliance/security hardening (DLP, penetration testing, surveillance/market-abuse detection) — should not wait for Phase 3/4; schedule a dedicated security review once Track A's order/risk paths are stable, since that's the highest-value time to check for injection/auth/encryption issues before more execution surface is added.

---

## Sequencing Summary

```
Days 1-10   Track A: stabilization (broken wiring, dead code, infra)
Weeks 3-6   Phase 1: execution algos + broker adapters
Weeks 5-10  Phase 2: volatility/optimization/portfolio deepening
Weeks 9-16  Phase 3: Mechanism Observatory, orchestrator hardening, SOR
Weeks 15+   Phase 4: monitoring, ML pipeline, UX, compliance (parallelizable)
```

Phases 1 and 2 overlap by design (both can proceed once Track A's order/risk/intelligence paths are confirmed stable at Day 10). Phase 3's Mechanism Observatory has no hard dependency on Phase 1 but is sequenced after it because it's the largest, least time-critical item and shouldn't compete for engineering attention with execution capability that directly unblocks broker/order functionality.

---

## Owner Decisions Required Before Track A Starts

Carried forward from the OpenCurrent Labs report §9, since they gate sequencing above:

1. **MarketTick fix**: write raw ticks (Option 1) vs. redirect to candles (Option 2) — affects Day 5 scope and Phase 2/3 fidelity ceiling.
2. **Dead code**: confirm removal (recommended) vs. attempting to complete the orphaned execution/broker stack.
3. **TWAP/VWAP/Iceberg/OCO labeling**: mark as market-order-equivalent vs. hide from selection until Phase 1 ships real slicing logic.
4. **Broker scope for Track A**: confirm Alpaca + CCXT/Binance remain the only supported brokers until Phase 1; every other configured type currently silently fakes fills (fixed to fail loudly in A8, but still unsupported until Phase 1 adapters land).

---

*This plan supersedes `development-priorities.md`'s sequencing (not its technical content, which is largely reused above) and operationalizes the OpenCurrent Labs 10-day report into file-level implementation detail. See `project_dev.md` for the underlying audit evidence.*
