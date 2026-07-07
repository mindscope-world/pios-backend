# PiOSQ Development Priority Plan

## Executive Summary

This document outlines the prioritized feature development plan for PiOSQ to achieve full v10 specification compliance. Priorities are based on criticality, dependencies, and impact on the system's ability to function as an institutional-grade quant trading platform.

**Current Status**: Solid foundation with core trading functionality, significant gaps in advanced institutional components

**Target**: Full 5-domain, 35+ sub-layer architecture with Mechanism Observatory and Quant Core Orchestrator

**Estimated Timeline**: 16 weeks for core P0-P1 features, 24 weeks for complete specification

---

## Urgency Priority Matrix

| Priority | Feature | Impact | Dependency Risk | Timeline |
|----------|---------|--------|-----------------|----------|
| **P0** | Mechanism Observatory (E7) | Critical differentiator | Blocks Alpha Engine & Portfolio Engine | Weeks 1-7 |
| **P0** | Quant Core Orchestrator (8-gate) | Safety/compliance | Required before live execution expansion | Weeks 1-7 |
| **P1** | Complete 5-Module Data Quality Layer | Data integrity foundation | Blocks all downstream accuracy | Weeks 1-3 |
| **P1** | Optuna + Walk-Forward Validation | Strategy credibility | Required for strategy registry gates | Weeks 4-5 |
| **P1** | Remaining Quant Engines (E2, E5) | Coverage/completeness | E7 depends on E2 | Weeks 6-8 |
| **P2** | Execution Algorithms (TWAP/VWAP/etc.) | Institutional capability | Required for large order execution | Weeks 1-4 (P2) |
| **P2** | Smart Order Router (SOR) | Cost optimization | Required for venue arbitrage | Weeks 5-8 (P2) |
| **P2** | Monitoring Stack (Prometheus/Grafana) | Observability | Required for production operations | Weeks 1-3 (P2) |

---

## Frontend-Driven Feature Requests (from client design mockup, 2026-07-06)

The client supplied `pi-osq-execution-intelligence-v3.html` as the new frontend design + feature source of truth. Auditing it against this backend surfaced several UI concepts with **no backend support today** — not partial, entirely absent. Per the client's explicit decision, the frontend now renders these honestly as "not yet available" rather than simulating them, and they're recorded here as future backend work:

| Feature | Mockup concept | Current backend gap |
|---------|----------------|----------------------|
| **Autonomous execution modes** | Semi-auto ("system entered trade, user can override/adjust") and Automatic ("trades entered and managed by the system") execution modes, with asymmetric mode-switch friction (manual→auto requires confirm, auto→manual is instant) | Every order is user-initiated via `POST /orders`. There is no autonomous order-submission engine — nothing evaluates a decision and submits an order on its own initiative. This is a large, high-risk, real-money-adjacent project (needs its own design: sizing authority, per-mode risk limits, audit trail, kill interaction), not an incremental addition. |
| **System-triggered kill + resume checklist** | Kill switch has two distinct trigger paths (manual vs. system/model-initiated), with system-triggered pauses requiring a deliberate "review and resume" checklist before trading resumes; manual pauses resume with one click | `POST /risk/killswitch` is a one-shot cancel-all/close-all action, not a state toggle. `RiskMetricsOut.kill_switch_armed` is a static readiness flag, not a paused/active/resumable state. `KillSwitchEventOut.trigger_source` exists in the schema but only the literal `"manual"` value is ever written anywhere in the code — nothing currently triggers a kill switch autonomously, so system vs. manual can't be distinguished today even at the data level. |
| **Concrete trade-setup fields** | Decision card shows Entry / Stop / Target / R:R / Risk% / Size for the current signal | No endpoint returns entry/stop/target price levels or a risk:reward ratio. `/intelligence/decision/current`, `/command-center/current`, and `/why-not-trade` return a decision (ALLOW/BLOCK/WAIT/REDUCE), confidence, and a sizing figure (`final_size_lot`) — no directional entry/exit levels. |
| **Calibration digest** | 7-day audit aggregate: eligible setups taken vs. skipped, avg size vs. max allowed, rejection-reason breakdown (crowding / high difficulty / confidence / macro window / regime mismatch) | No aggregation endpoint exists. Per-symbol `/why-not-trade` and `/traces` are real and per-order, but nothing rolls them up into a rolling-window digest with a rejection-reason taxonomy. |
| **Broker order success rate** | Per-broker and aggregate "execution health" (order success %, best latency path, 7d execution failures, avg slippage) | `BrokerOut` has `latency_p99_ms`/`status`/`error_message` (real) but no fill-success-rate rollup by broker; fills aren't currently attributed to a broker for aggregation. |

None of these were built as client-side simulations in the current frontend — they're either omitted with an inline note, or (execution mode) shown as a real switch that renders an honest "not available" state for semi/automatic while Manual wires to the real order-submission path.

---

## P0: CRITICAL PRIORITY (Start Immediately)

### 1. Mechanism Observatory (D3 Engine 7)

**Rationale**: This is the architectural centerpiece that differentiates PiOSQ from ordinary trading platforms. Without it, the system lacks:

- Formal independence testing between alpha sources
- Effective dimensionality estimation (N_eff) with confidence intervals
- Research capital allocation framework (RPS-driven research queue)
- Mechanism decay detection and health monitoring

**Specifications from Architecture Document**:

```python
# Formal Mechanism Independence Score
I(A,B) = 1 - MI_norm(A,B) - λ_shared / (λ_A + λ_B)

Where:
- MI_norm = MI(R_A, R_B) / sqrt(H(R_A) · H(R_B)) ∈ [0,1]
- λ_shared = eigenvalues explained jointly / total eigenvalues
- Two mechanisms independent when I(A,B) ≥ 0.70

# N_eff Formal Estimator (Participation Ratio)
N_eff = 1 / Σ(p_i²) where p_i = λ_i / Σλ_j

# Research Priority Score
RPS = (1 - fitness_score) × decay_weight × independence_weight
```

**Implementation Plan**:

| Week | Deliverable | Acceptance Criteria |
|------|-------------|---------------------|
| 1 | Mechanism registry data model + PostgreSQL persistence | Registry with 5 bootstrapped mechanisms (ORDER_FLOW, STRUCTURAL_CARRY, BEHAVIORAL, RELATIVE_VALUE, MACRO_FLOW) |
| 2 | `mechanism_independence()` with mutual information + spectral analysis | Returns independence score, 95% CI, is_independent flag |
| 3 | `n_effective_formal()` with bootstrap CI (200 resamplings) | Returns N_eff point estimate + [ci_low, ci_high] |
| 4 | `research_priority_score()` + compression_ratio calculation | RPS with urgency classification (CRITICAL/HIGH/MEDIUM/LOW) |
| 5 | `mechanism_decay_check()` + D1.6 Feature Store integration | Writes mech_health_mult to Redis, publishes to Kafka |
| 6 | API endpoints (`/api/v1/mechanism/*`) + dashboard widgets | All 8 endpoints operational with sample responses |
| 7 | Integration testing + adversarial validation | 1000+ test cases covering edge conditions |

**Key Dependencies**: D1.6 Feature Store (Redis) must be operational

**Files to Create/Modify**:
- `app/services/intelligence/mechanism_observatory.py` (NEW)
- `app/api/v1/endpoints/mechanism.py` (NEW)
- `app/models/mechanism_registry.py` (NEW)
- `alembic/versions/add_mechanism_registry.py` (NEW)

**Required Libraries**:
```
scikit-learn>=1.4.0  # mutual_info_regression
numpy>=1.26.0
scipy>=1.13.0  # eigvalsh, sqrtm
statsmodels>=0.14.0  # entropy calculations
```

---

### 2. Quant Core Orchestrator (8-Gate Check)

**Rationale**: Every trade before execution runs through this orchestrator—it's the instant gatekeeper that combines all 7 engines. Without this, signals bypass critical safety checks.

**The 8 Gates**:

| Gate | Engine | Check | Action on Failure |
|------|--------|-------|-------------------|
| QC-1 | Regime Engine | HMM + Bayesian + GARCH + Macro event → 5 regime states | BLOCK if regime not in strategy.allowed_regimes |
| QC-2 | Monte Carlo | 10,000 simulations: vol shock, flash crash, spread widening, liquidity shock | BLOCK if P30 drawdown > 30% |
| QC-3 | Crowding Detector | Social velocity + funding rate + COT + GEX + vol skew composite | BLOCK if score > 0.85, REDUCE 50% if > 0.65 |
| QC-4 | Strategy Confidence | Rolling Sharpe, alpha decay, hit rate, drawdown depth (0-100 score) | size = base_size × confidence_score / 100 |
| QC-5 | Correlation Engine | Cross-strategy correlation matrix, max correlated exposure 30% | REDUCE if new trade pushes correlated exposure above 30% |
| QC-6 | Transaction Cost | Spread + slippage + latency + market impact + liquidity depth | BLOCK if expected_profit < expected_total_cost |
| QC-7 | Execution Intelligence | Select optimal order type based on liquidity + urgency + size | Route to TWAP/VWAP/Iceberg/Limit per conditions |
| QC-8 | Kill Switch | Daily loss 5% / drawdown 20% / latency 200ms / exchange disconnect | KILL_ALL positions and halt all strategies |

**Implementation Plan**:

| Week | Deliverable | Acceptance Criteria |
|------|-------------|---------------------|
| 1-2 | Gate architecture + registration system | Pluggable gate interface, all 8 gates registered |
| 2-4 | Implement gates QC-1 through QC-4 | Each gate returns ALLOW/REDUCE/BLOCK with reason |
| 4-5 | Implement gates QC-5 through QC-8 | Correlation, cost, router, kill switch operational |
| 5-6 | Orchestrator orchestration logic (parallel → combine) | Gates run in parallel, results combined in <10ms |
| 6-7 | Latency profiling + optimization (1ms target for risk gates) | P99 < 5ms end-to-end |
| 7 | Integration with D4 OMS ordering pipeline | Every order goes through orchestrator before submission |

**Key Dependencies**: E1 (Microstructure), E2 (Volatility), E3 (Regime), E4 (Alpha), Monte Carlo engine must be operational

**Files to Create/Modify**:
- `app/services/quant_core_orchestrator.py` (NEW)
- `app/services/gates/base_gate.py` (NEW)
- `app/services/gates/regime_gate.py` (NEW)
- `app/services/gates/monte_carlo_gate.py` (NEW)
- `app/services/gates/crowding_gate.py` (NEW)
- `app/services/gates/confidence_gate.py` (NEW)
- `app/services/gates/correlation_gate.py` (NEW)
- `app/services/gates/transaction_cost_gate.py` (NEW)
- `app/services/gates/execution_intelligence_gate.py` (NEW)
- `app/services/gates/kill_switch_gate.py` (Already exists in risk_service.py, enhance)
- `app/workers/orchestrator.py` (MODIFY to integrate with order flow)

---

## P1: HIGH PRIORITY (After P0 Core)

### 3. Complete 5-Module Data Quality Layer

**Rationale**: Garbage data in = garbage signals everywhere. This is the foundation of everything above.

**Module Specifications**:

```python
# Module 1: Tick Validator
SPIKE_THRESHOLD = 0.05  # 5% move in 100ms = likely error
VOLUME_MAX_FACTOR = 50   # 50× rolling avg volume = anomalous
PRICE_MAX_FACTOR = 10    # 10× rolling max = reject outright

# Module 2: Duplicate Filter (SHA-256 + 10-second TTL sliding window)
def filter(tick):
    h = hashlib.sha256(f'{tick.symbol}|{tick.price}|{tick.volume}|{tick.timestamp}'.encode()).hexdigest()
    return FilterResult.REJECT if h in self.seen else FilterResult.PASS

# Module 3: Timestamp Corrector
CRYPTO_GAP_THRESHOLD_MS = 500
EQUITY_GAP_THRESHOLD_MIN = 5

# Module 4: Outlier Detector
Z_SCORE_THRESHOLD = 4.0
IQR_MULTIPLIER = 3.0
LOF_THRESHOLD = 2.5

# Module 5: Continuity Monitor
EXPECTED_TICKS_PER_MIN = 60  # per symbol
STALE_THRESHOLD_MS = 500
```

**Implementation Plan**:

| Week | Deliverable | Acceptance Criteria |
|------|-------------|---------------------|
| 1 | Tick Validator + Duplicate Filter | Schema validation, spike detection, SHA-256 dedup |
| 2 | Timestamp Corrector + Outlier Detector | TZ normalization, Z-score/IQR/LOF detection |
| 3 | Continuity Monitor + dead-letter queue | Tick rate monitoring, gap detection, stale feed alerts |
| 3 | DQ metrics (`/data/quality/*`) + D5.1 monitoring alerts | All endpoints operational with DQ event logging |
| 4 | Integration tests with adversarial dirty data | 500+ test cases with malformed/duplicated/tampered data |

**Key Dependencies**: Kafka ingestion pipeline (or Redis-based equivalent)

**Files to Create/Modify**:
- `app/services/data_quality/tick_validator.py` (ENHANCE)
- `app/services/data_quality/duplicate_filter.py` (NEW)
- `app/services/data_quality/timestamp_corrector.py` (NEW)
- `app/services/data_quality/outlier_detector.py` (NEW)
- `app/services/data_quality/continuity_monitor.py` (NEW)
- `app/workers/dq_pipeline.py` (MODIFY to integrate all 5 modules)

---

### 4. Optuna + Walk-Forward Validation

**Rationale**: Required for strategy registry lifecycle gates. Without Bayesian hyperparameter optimization and walk-forward validation, strategies cannot advance past PAPER trading stage.

**Configuration**:

```python
class OptunaStrategyOptimiser:
    N_TRIALS = 200
    N_JOBS = 8  # Ray-parallel
    MIN_SHARPE_IS = 0.5  # in-sample minimum
    
    sampler = TPESampler(seed=42)      # Bayesian TPE
    pruner = MedianPruner(n_startup_trials=20, n_warmup_steps=50)

class WalkForwardValidator:
    MIN_SHARPE_OOS = 0.8
    MAX_DD_OOS = 0.15
    MIN_TRADES = 200
    N_SPLITS = 12  # 12-fold expanding window
```

**Implementation Plan**:

| Week | Deliverable | Acceptance Criteria |
|------|-------------|---------------------|
| 4 | Optuna study setup with MLflow storage | Study runs with TPE sampler, results logged to MLflow |
| 4 | Parameter search space design for all 5 strategies | Each strategy has optimized parameters (entry_z, stop_atr_mult, lookback, etc.) |
| 5 | Walk-forward splitter (12-fold expanding) + OOS evaluation | Splitter yields train/test pairs, metrics computed on test only |
| 5 | Gate integration with Strategy Registry advancement | Strategies fail to advance ifSharpe_OOS < 0.8 or max_dd > 15% |
| 5 | Distributed trial execution with Ray | 8 parallel trials per study, 200 trials complete in reasonable time |

**Key Dependencies**: Backtesting engine (existing D2.2) needs integration with Optuna

**Files to Create/Modify**:
- `app/services/optuna_optimiser.py` (NEW)
- `app/services/walk_forward_validator.py` (NEW)
- `app/workers/backtest_worker.py` (MODIFY to integrate Optuna)
- `app/api/v1/endpoints/strategies.py` (MODIFY to add validation results to backtest response)

---

### 5. Remaining Quant Engines

#### E2 - Volatility Modeling Engine

**Specifications**:

```python
# GJR-GARCH(1,1): σ²(t+1) = ω + α·ε²(t) + γ·I⁻(t)·ε²(t) + β·σ²(t)
from arch import arch_model
model = arch_model(returns, vol='GJR', p=1, q=1)

# EGARCH for leverage effect (capturing asymmetric response to bad news)
# APARCH for fat-tail crypto regime transitions
# Heston calibration for options (QuantLib)
# Realized volatility from high-frequency ticks
```

**Implementation Plan** (Weeks 6-7 of P1 phase):
- Week 6: GJR-GARCH + EGARCH + APARCH models per asset class (5 configs)
- Week 7: Realized volatility computation from ticks, GARCH v-factor for Kelly

**Files to Create/Modify**:
- `app/services/intelligence/volatility_engine.py` (NEW)
- `app/models/garch_models.py` (NEW)

---

#### E5 - Portfolio Engine (Multi-Strategy Optimizer)

**Specifications**:

```python
# Allocation QP Objective:
# min ||w - w_prior||²
# subject to: |P_f| ≤ C_f (factor caps), w_i ≤ cap_i (instrument), Σ|w_i| ≤ portfolio_cap

# RANK_TO_CAP mapping (using CI lower bound for conservatism):
RANK_TO_CAP = {1: 0.08, 2: 0.12, 3: 0.15, 4: 0.20, >=4: 0.20}
portfolio_cap = RANK_TO_CAP.get(alpha_rank.ci_95[0], 0.08)

# Methods: HRP (default), Black-Litterman, CVaR, Mean-Variance (Ledoit-Wolf)
from RiskfolioLib import HRP, BlackLitterman, CVaR
from PyPortfolioOpt import LedoitWolf
```

**Implementation Plan** (Weeks 7-8 of P1 phase):
- Week 7: Ledoit-Wolf covariance shrinkage + HRP optimizer
- Week 8: Black-Litterman with GMIG forward views + CVaR optimization + SLSQP QP solver

**Files to Create/Modify**:
- `app/services/intelligence/portfolio_engine.py` (NEW)
- `app/services/allocator.py` (NEW)

---

## P2: MEDIUM PRIORITY (Enhance After Foundation)

### 6. Execution Algorithm Suite

**Algorithms to Implement**:

| Algorithm | When Used | Parameters |
|-----------|-----------|------------|
| TWAP | Large orders in calm markets | duration_mins: 5-60 |
| VWAP | Institutional-size orders | participation: 5-15% |
| POV | Market impact scales with volume | participation: 10-25% |
| Iceberg | Large orders where full size invites front-running | show_qty: 10% total |
| Sniper Limit | Favourable microstructure (absorption signal) | TTL: 30s |
| Market | Breakout signals requiring priority over price | max_slippage: 0.2% |

**Implementation Plan** (Weeks 1-4 of P2 phase):
- Week 1-2: TWAP + VWAP + POV algorithms
- Week 3: Iceberg + Sniper Limit algorithms
- Week 4: Market order with adaptive slippage + integration with execution intel gate

**Files to Create/Modify**:
- `app/services/execution/twap_algo.py` (NEW)
- `app/services/execution/vwap_algo.py` (NEW)
- `app/services/execution/pov_algo.py` (NEW)
- `app/services/execution/iceberg_algo.py` (NEW)
- `app/services/execution/sniper_algo.py` (NEW)
- `app/services/execution/market_algo.py` (NEW)

---

### 7. Smart Order Router (SOR)

**Features**:
- Venue selection (best price + deepest liquidity for order size)
- Anti-PFOF (direct market access via IBKR FIX for T2 tier)
- Passive rebate capture (earn maker rebates when microstructure favorable)
- Adverse selection scoring (blacklist venues where fills move against us within 1s)
- Anti-front-running (randomized TWAP/VWAP slice timing, variable slice sizing)

**Implementation Plan** (Weeks 5-8 of P2 phase):
- Week 5-6: Venue selection + anti-PFOF routing
- Week 7: Adverse selection scoring + passive rebate capture
- Week 8: Anti-front-running logic + integration with execution algo suite

**Files to Create/Modify**:
- `app/services/execution/smart_order_router.py` (NEW)
- `app/services/execution/venue_selector.py` (NEW)
- `app/services/execution/adverse_selection_scoring.py` (NEW)

---

### 8. Monitoring Stack (Prometheus + Grafana + AlertManager)

**Metrics to Collect**:

```python
from prometheus_client import Histogram, Gauge, Counter

ORDER_LATENCY = Histogram('pi_order_latency_ms', 
    'Order-to-fill latency in milliseconds',
    buckets=[1, 2, 5, 10, 20, 50, 100, 200, 500])

STRATEGY_SHARPE = Gauge('pi_strategy_sharpe_30d',
    'Rolling 30-day Sharpe ratio', ['strategy_id'])

FEATURE_PSI = Gauge('pi_feature_psi',
    'Population Stability Index per feature', ['feature_name'])

PORTFOLIO_DRAWDOWN = Gauge('pi_portfolio_drawdown_pct',
    'Current portfolio drawdown as percentage of equity')

MECHANISM_CR = Gauge('pi_mechanism_compression_ratio',
    'Compression ratio CR = N_eff / N_raw')

N_EFF = Gauge('pi_n_effective',
    'Effective dimensionality of alpha opportunities')
```

**Alert Rules**:
- P99 order latency > 10ms → warning, > 200ms → kill switch
- Strategy Sharpe < 50% of backtest Sharpe → quarantine
- PSI > 0.2 → drift alert, > 0.5 → auto-retrain trigger
- CR < 1% → mechanism concentration alert
- Agent disconnect > 30s → on-call alert

**Implementation Plan** (Weeks 1-3 of P2 phase):
- Week 1: Prometheus metrics instrumentation across all services
- Week 2: Grafana dashboard setup (6 dashboards: Execution, Orders, Portfolio, Strategy, Mechanism, System)
- Week 3: AlertManager configuration + PagerDuty/Slack routing + automated response triggers

**Files to Create/Modify**:
- `app/monitoring/prometheus_metrics.py` (NEW)
- `app/monitoring/alert_rules.yaml` (NEW)
- `dashboards/execution.json` (NEW)
- `dashboards/portfolio.json` (NEW)
- `dashboards/mechanism.json` (NEW)
- `grafana/provisioning/dashboards.yml` (NEW)

---

## Development Roadmap Timeline

### Phase 1: Critical Path (Weeks 1-8)

```
Week 1-7:  P0 - Mechanism Observatory (Engine 7)
Week 1-7:  P0 - Quant Core Orchestrator (8-gate)
Week 1-3:  P1 - Complete 5-Module DQ Layer
Week 4-5:  P1 - Optuna + Walk-Forward Validator
Week 6-8:  P1 - Remaining Quant Engines (E2: Volatility, E5: Portfolio)
```

### Phase 2: Portfolio + Execution Foundation (Weeks 9-16)

```
Week 9-12: P2 - Execution Algorithm Suite (TWAP/VWAP/POV/Iceberg/Sniper)
Week 13-16: P2 - Smart Order Router with anti-PFOF + adverse selection
```

### Phase 3: Operationalization (Weeks 13-18)

```
Week 13-14: P2 - Monitoring Stack (Prometheus + Grafana + AlertManager)
Week 15-18: Integration testing + adversarial validation across all layers
```

---

## Resource Requirements

### Team Composition (Recommended)

| Role | Count | Responsibilities |
|------|-------|------------------|
| Senior Quant Developer | 2 | Mechanism Observatory, Quant Core Orchestrator, Volatility/Portfolio engines |
| Backend Developer | 1 | Data Quality Layer, Optuna integration, API endpoints |
| ML Engineer | 1 | ML pipeline, drift detection, Optuna studies |
| DevOps Engineer (part-time) | 0.5 | Monitoring stack, Kafka/Flink infrastructure, deployment |

### Infrastructure Requirements

| Component | Specification |
|-----------|---------------|
| Training Cluster | NVIDIA A100 (80GB) × 1-2 nodes for Monte Carlo + ML training |
| Redis Cluster | 3-node Redis 7 Cluster for Feature Store online tier |
| Kafka Cluster | 3-broker Kafka for 16 topics (or managed equivalent like Confluent Cloud) |
| PostgreSQL | Managed PostgreSQL with read replicas for analytics queries |
| Prometheus/Grafana | Managed monitoring or self-hosted on 2 vCPU/8GB instance |

---

## Key Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Mechanism independence mathematics too complex | Medium | High | Start with simplified correlation, add full MI/spectral in iteration 2 |
| N_eff CI estimation too computationally expensive | Low | Medium | Bootstrap resamplings = 200 (reducible to 100 for MVP) |
| Latency targets (<1ms for risk gates, <10ms for orchestrator) not met | Medium | Critical | Start with optimized Rust sidecar, benchmark early |
| Kafka/Flink infrastructure complexity exceeds timeline | High | Medium | Use Redis/pubsub as interim, migrate to Kafka later |
| Backtest-to-live divergence (strategies pass validation but fail live) | Medium | High | Enforce strict walk-forward gates, over-validate in MVP |
| Missing mathematical libraries (QuantLib, RiskfolioLib) have breaking changes | Low | Medium | Pin exact versions, maintain fork if needed |

---

## Success Criteria

### End of P0 Phase (Week 7)

- [ ] Mechanism Observatory operational with all 8 API endpoints
- [ ] Quant Core Orchestrator routing all test orders through 8 gates
- [ ] N_eff formal estimator returning point estimate with 95% CI
- [ ] Latency profiling shows orchestrator <10ms P99
- [ ] Unit test coverage > 80% for P0 components

### End of P1 Phase (Week 8)

- [ ] All 5 Data Quality modules integrated and processing ticks
- [ ] Optuna optimizing strategy parameters with 200 trials completing
- [ ] Walk-forward validator blocking strategies with OOS Sharpe < 0.8
- [ ] Volatility Engine producing GARCH forecasts per asset class
- [ ] Portfolio Engine running HRP allocation on validated signals
- [ ] D64% of production code paths covered by tests

### End of P2 Phase (Week 16)

- [ ] Execution Algorithm Suite selecting optimal algo per order characteristics
- [ ] Smart Order Router routing to best venue with adverse selection detection
- [ ] Monitoring dashboards showing live metrics + threshold alerts
- [ ] Integration test suite passing with 95%+ scenarios
- [ ] Zero critical bugs in production deployments

---

## Appendix: Implementation Checklist

### P0: Mechanism Observatory
- [ ] `app/models/mechanism_registry.py` - Create
- [ ] `app/services/intelligence/mechanism_observatory.py` - Create
- [ ] `app/api/v1/endpoints/mechanism.py` - Create
- [ ] `alembic/versions/add_mechanism_registry.py` - Create migration
- [ ] Unit tests for mechanism_independence() with bootstrap CI
- [ ] Unit tests for n_effective_formal() with spectral decomposition
- [ ] Unit tests for research_priority_score()
- [ ] Integration tests with adversarial mechanism correlations

### P0: Quant Core Orchestrator
- [ ] `app/services/quant_core_orchestrator.py` - Create
- [ ] `app/services/gates/` directory - Create
- [ ] 8 gate implementations (one per file)
- [ ] Oracle-based latency tests (1ms target for risk gates)
- [ ] Concurrency tests (all 8 gates running in parallel)
- [ ] Integration tests with OMS order pipeline

### P1: Data Quality Layer
- [ ] `app/services/data_quality/` directory - Create
- [ ] 5 module implementations (one per file)
- [ ] Queue-based dead-letter handling for rejected ticks
- [ ] Unit tests with malformed tick injection
- [ ] Integration tests with advesarial dirty data

### P1: Optuna + Walk-Forward
- [ ] `app/services/optuna_optimiser.py` - Create
- [ ] `app/services/walk_forward_validator.py` - Create
- [ ] Parameter space configuration for all 5 strategies
- [ ] MLflow experiment setup with artifact storage
- [ ] Ray distributed trial execution tests

### P2: Execution Algorithms
- [ ] 6 algo implementations (one per file in `app/services/execution/`)
- [ ] Algo selection logic with market condition checks
- [ ] Partial fill handling for TWAP/VWAP/P/OV
- [ ] Integration tests with broker simulation

### P2: Smart Order Router
- [ ] `app/services/execution/smart_order_router.py` - Create
- [ ] Venue selection with latency scoring
- [ ] Adverse selection scoring with blacklisting
- [ ] Anti-PFOF routing for T2 tier

### P2: Monitoring Stack
- [ ] Prometheus metrics instrumentation across all services
- [ ] Grafana dashboard JSON exports (6 dashboards)
- [ ] AlertManager YAML configuration
- [ ] PagerDuty/Slack integration webhook handlers

---

*Document created: 2026-06-18*
*Version: 1.0*
*Review cycle: Bi-weekly with engineering lead*