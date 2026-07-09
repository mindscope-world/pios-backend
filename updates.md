# Updates — Day 8 Dead Code Resolution → Feature Implementation

Context: the OpenCurrent Labs audit (§5, §9) recommended *removing* the parallel
`brokers/` + `execution/` stack rather than completing it, since the real order
path (`order_service.py`) was already the production one. That direction was
reversed: MT5 broker execution and algorithmic order slicing were core
requirements, so they were built for real against the live path instead of
being deleted. Everything below was implemented, then exercised against a
running server + real Postgres + a simulated MT5 EA — not just written and
assumed correct. Two genuine concurrency bugs were found and fixed this way
(details under "Fully built").

---

## Fully built

### 1. Celery beat schedule consolidation
`backtest_worker.py` was overwriting `celery_app.conf.beat_schedule` wholesale
on import, silently dropping whatever `celery_app.py` had already registered
depending on import order. Fixed:
- `celery_app.py` now owns the canonical schedule (`snapshot-pnl-5min`).
- `backtest_worker.py` merges its 5 tasks in via `.update()`.
- Deduped a `darwin-weekly` entry that would have double-fired against the
  more complete `darwin-nightly` entry (same task, overlapping schedule).

Verified: schedule merge produces exactly 5 unique entries, no duplicate keys.

### 2. Risk gate hardening (`app/services/order_service.py`)
Two checks ported from the dead `risk_gate.py` into the live path, augmenting
(not replacing) the existing DB-backed max-position/daily-loss checks:

- **Max open orders per user** — DB-backed count against
  `settings.DEFAULT_MAX_OPEN_ORDERS` (default 50).
- **`client_order_id` idempotency** — retrying the same key now returns 409
  instead of silently double-submitting to the broker. `OrderCreate` gained a
  `client_order_id` field so callers can actually supply one.

**This one had a real bug that testing caught, not review.** The first cut
was a SELECT-then-INSERT check — safe for sequential retries, but two
genuinely concurrent requests with the same key (the actual case a retry
protection exists for) could both pass the SELECT before either committed.
Fired 10 truly concurrent same-key submissions at it: multiple succeeded.
Fixed with a partial unique index (Alembic migration `a1b2c3d4e5f6`:
`UNIQUE (user_id, client_order_id) WHERE client_order_id IS NOT NULL`) plus
an `IntegrityError` → 409 handler in `submit_order`. Re-ran the same 10
concurrent submissions: exactly 1 succeeded, 9 got 409.

### 3. MT5 broker bridge
MT5 has no REST API — execution happens through an Expert Advisor (EA)
running in the trader's terminal, which has to connect *to us* and stay
connected. Built:

- `app/services/brokers/mt5/adapter.py` — `MT5Connection` (one live EA
  socket, correlation-id/future bookkeeping to match a request to its async
  reply), `MT5BridgeRegistry` (process-wide `broker_id → connection` map),
  `MT5Adapter` (duck-types `BrokerAdapter`, built fresh per call like every
  other adapter, delegates to the registry for the actual connection).
- `app/api/v1/endpoints/mt5_bridge.py` — `/ws/mt5/{broker_id}`, where the EA
  connects and authenticates with a shared bridge token (the broker's
  `passphrase` field, AES-encrypted at rest like every other broker's
  credentials — no new secret-storage mechanism needed).
- `MT5` added to `broker_service.ADAPTER_MAP`, so `is_paper=false` is now
  accepted for MT5 broker connections instead of raising
  `UnsupportedBrokerError`.
- Frontend: `MT5` added to `REAL_BROKER_TYPES` (`api/types.ts`), which
  unlocks the live-trading toggle in `BrokerFormModal` — without this the
  new backend capability would have been unreachable through the only
  client. Passphrase field gets an MT5-specific hint. Typechecks clean.

Verified against a scripted fake EA over a real WebSocket connection:
correct HANDSHAKE/ACK, a MARKET order round-tripping to a real
`broker_order_id` and fill price, wrong-token rejection (1008 close), and a
disconnected-EA order failing in <1s with a clear rejection reason instead
of hanging.

### 4. TWAP / VWAP / ICEBERG execution algorithms
Previously: `OrderType` included these values but every order type
instant-filled identically (explicitly flagged as a Track B gap in the
existing code's own comments). Built `app/services/execution_algo.py`:

- Algorithmic orders return immediately as `SUBMITTED` with zero fills
  (correct — a real schedule can run for minutes, longer than an HTTP
  request should block) and a background asyncio task executes the slice
  schedule, walking the order through `PARTIAL → FILLED` as slices land.
  Each slice becomes its own `Fill` row against the same parent `Order`.
- **TWAP** — equal-sized slices at a fixed interval.
- **ICEBERG** — slices sized by `display_qty`, count is data-dependent.
- **VWAP** — approximated with a U-shaped participation curve (heavier at
  the first/last slices), *not* a real historical volume profile — no
  intraday volume-profile data source is wired in yet. Documented inline as
  a simplification to replace once one exists.
- `_ALGORITHMIC_ORDER_TYPES` in `all_schemas.py` updated so
  `OrderOut.execution_style` correctly reports `"ALGORITHMIC"` for these
  three and `"INSTANT"` for everything else (OCO still has no engine).

Verified all three live against the fake EA:
- TWAP: 3 slices at a 2s interval, fills spaced ~2s apart, `FILLED` at total
  qty with correct volume-weighted `avg_fill_price`.
- ICEBERG: `qty=10, display_qty=3` → slices `[3.0, 3.0, 3.0, 1.0]`, exactly
  as expected from the data-dependent slicing logic.
- VWAP: `qty=10, slices=4` → `[2.92, 2.08, 2.08, 2.92]`, correct symmetric
  U-curve, sum exactly `10.0` (rounding remainder absorbed correctly).

**Two real concurrency bugs found by testing, fixed, and re-verified:**
1. *Task-scheduling race*: the background task opened its own DB session and
   queried for the order before the HTTP request's transaction had
   committed — "order not found" on a real order. Fixed by moving task
   scheduling (`start_algo_execution`) to the router, strictly after
   `await db.commit()`, rather than inside `submit_order` itself.
2. *Cancel/fill lost-update race*: cancelling an order mid-schedule while
   `adapter.cancel_order()` blocked for ~10s (broker timeout) let the
   background task keep committing fills from a separate session. Whichever
   session committed last won the columns it touched — reproduced a
   corrupted order left as `CANCELLED` with `filled_qty=5.0` (fully filled).
   Fixed with `SELECT ... FOR UPDATE` row locking in both `cancel_order` and
   the per-slice loop, serializing the two writers. Re-ran the identical
   scenario: order correctly ends `CANCELLED` with only the 2 slices that
   had genuinely completed before the cancel.

### Dead code removed (confirmed zero references, including deployment configs)
`brokers/risk_gate.py`, `brokers/broker_router.py`, `mt5/client.py`,
`mt5/models.py`, `mt5/ea_builder.py`, `execution/execution_service.py`,
`execution/smart_router.py`, `db/order_store.py`,
`intelligence/indicators_service.py` (confirmed empty),
`workers/decision_stream_worker.py`, `workers/tick_ingestor.py`. These were
a stale prototype written against an abandoned Pydantic `Order` model (a
`nonce` field that never existed on the real SQLAlchemy model, `.model_dump()`
calls on ORM objects) with two files that had broken import paths and would
crash on load — not partially-done features, genuinely non-functional code.
Their reusable ideas (risk rules, the MT5 WebSocket bridge protocol, the
slicing concept) were extracted and rebuilt properly against the real models
rather than resurrected as-is.

---

## Partially built / known limitations

- **MT5 registry is not multi-worker safe.** `mt5_registry` is an
  in-process singleton; the app's `Dockerfile` runs `uvicorn --workers 2`.
  An EA connected to worker A is invisible to worker B — an order request
  landing on the wrong worker gets a clean "EA not connected" rejection
  (fails safe, doesn't corrupt anything) rather than routing correctly. This
  is *not* on par with the app's existing Redis-backed cross-worker fan-out
  (there's already a "Redis listener" at startup for other things). Needs
  either sticky routing on `broker_id` at the load balancer, or a Redis
  pub/sub relay for `PLACE_ORDER`/`ORDER_RESULT`, before this runs safely
  with more than one worker process. Documented prominently in
  `mt5/adapter.py`'s module docstring so it isn't missed at deploy time.
- **VWAP has no real volume-profile data.** The U-shaped curve is a
  reasonable stand-in, not actual historical intraday volume weighting.
  Swap in a real profile once `market_data_service` exposes one.
- **MT5Adapter.get_account() / get_positions()** are implemented (send a
  request, await a correlated reply) but only `PLACE_ORDER`/`CANCEL_ORDER`
  were exercised against the fake EA. A real EA would need to implement
  `GET_ACCOUNT` / `GET_POSITIONS` message handling to make those two calls
  meaningful — untested against real MT5 behavior since there's no real
  MT5 terminal available in this environment.
- **Algo order slices always execute as MARKET**, regardless of the parent
  order's `price`/`stop_price` — a deliberate simplification (child slices
  fire immediately; the parent order_type just selects the slicing
  strategy). Not a limit-price-aware execution algorithm.

## Pending / not done

- **Real MT5 EA (MetaTrader-side) implementation.** This work built and
  verified the server-side bridge and protocol against a scripted fake EA.
  An actual MQL4/MQL5 Expert Advisor implementing the wire protocol
  (`HANDSHAKE`, `PLACE_ORDER` → `ORDER_RESULT`, `CANCEL_ORDER` →
  `CANCEL_RESULT`, `GET_ACCOUNT`, `GET_POSITIONS`, `PING` → `PONG`) still
  needs to be written and paired with a real terminal.
- **Cross-worker MT5 relay** (see limitation above) — needed before
  multi-worker deployment.
- **Redis-backed idempotency/locking pattern reuse** — the `FOR UPDATE`
  fix works for single-Postgres-instance correctness; if the order path
  ever moves to a sharded/read-replica setup this needs revisiting.
- Pre-existing, unrelated: the pytest suite has event-loop-scope flakiness
  across files this work never touched (`test_auth.py`, `test_strategies.py`,
  etc.) — reproduces on a single isolated test, not something introduced
  here. Not fixed as part of this work; out of scope.
