# Pi OS Frontend — React + Tailwind Build Guide

This guide is written against the actual, current state of the `pios-backend-main` API (verified by direct source inspection, not documentation claims — see `project_dev.md`). It's meant to be handed to whoever builds the frontend, in a separate repo, against this backend. No frontend exists in this repo today.

Where the backend has a known gap or gotcha, this guide says so explicitly and tells you how to handle it in the UI — building against this backend without that context will produce a frontend that silently shows wrong data in a few specific, predictable places (called out in §8).

---

## 1. Recommended Stack

| Concern | Choice | Why |
|---|---|---|
| Build tool | **Vite** | The backend's `CORS_ORIGINS` default is `["http://localhost:5173", "http://localhost:3000"]` — 5173 is Vite's default dev port. It's already the assumed frontend. |
| Framework | **React 18 + TypeScript** | Strict typing matters here — this is a trading terminal; a `string` vs `string | null` mistake on `sharpe`/`win_rate` (see §8) is a real bug, not a lint nit. |
| Styling | **Tailwind CSS** (as requested) | Pair with a small design-tokens layer (§9) rather than raw utility soup — this UI has real semantic color needs (P&L sign, regime state, decision verdicts, DQ severity). |
| Server state | **TanStack Query (react-query)** | The API is REST + polling-friendly SSE/WS. Query handles caching, refetch-on-focus, and background polling for the worker-cached intelligence endpoints (§8) far better than hand-rolled `useEffect` fetching. |
| Client/UI state | **Zustand** | Small, no boilerplate, good for auth token state, active symbol, WS connection state, order-ticket draft state. Don't reach for Redux here. |
| Routing | **React Router v6** | Standard choice; role-based route guards map directly onto `require_roles(*roles)` from the backend (§5). |
| Charts | **lightweight-charts** (TradingView's OSS library) for price/candles; **Recharts** for everything else (equity curves, allocation donuts, Sharpe/PSI bars) | `lightweight-charts` is purpose-built for OHLCV + volume overlays and is what the backend's `compute_tradingview_payload` (`/market/ticks/*`) is shaped for. Don't use a general charting lib for candlesticks. |
| Forms | **React Hook Form + Zod** | Order tickets need tight client-side validation (qty > 0, price required for LIMIT/STOP, etc.) mirrored from the backend's own Pydantic validators — Zod schemas can mirror those 1:1 (§6). |
| HTTP client | **ky** or plain `fetch` wrapped once | Keep it thin; put JWT-refresh logic in one interceptor, not scattered across call sites. |

Don't reach for Next.js unless there's a specific SSR/SEO need — this is an authenticated, real-time internal terminal, not a public site. A pure SPA (Vite) is simpler and matches what the backend already expects.

---

## 2. Project Structure

```
pios-frontend/
├── .env.local                    # VITE_API_BASE_URL=http://localhost:9000
├── src/
│   ├── api/
│   │   ├── client.ts             # fetch wrapper + JWT attach + refresh-on-401
│   │   ├── auth.ts                # /auth/* calls
│   │   ├── orders.ts               # /orders/*
│   │   ├── positions.ts
│   │   ├── strategies.ts
│   │   ├── risk.ts
│   │   ├── brokers.ts
│   │   ├── intelligence.ts        # all 38 /intelligence/* routes
│   │   ├── alerts.ts
│   │   ├── audit.ts
│   │   └── types.ts               # generated or hand-mirrored response types
│   ├── realtime/
│   │   ├── useChannelSocket.ts    # /api/v1/ws channel-subscription protocol
│   │   ├── useMarketStream.ts     # SSE /intelligence/stream
│   │   └── useNotificationStream.ts  # SSE /intelligence/notifications/stream
│   ├── stores/
│   │   ├── authStore.ts           # Zustand: user, tokens, role
│   │   └── terminalStore.ts       # active symbol, layout prefs
│   ├── components/
│   │   ├── ui/                    # buttons, tables, badges — Tailwind primitives
│   │   ├── charts/
│   │   └── trading/               # OrderTicket, PositionRow, GateCard, RegimeBadge
│   ├── pages/
│   │   ├── auth/Login.tsx
│   │   ├── dashboard/CommandCenter.tsx
│   │   ├── orders/OrdersPage.tsx
│   │   ├── positions/PositionsPage.tsx
│   │   ├── strategies/StrategiesPage.tsx
│   │   ├── risk/RiskPage.tsx
│   │   ├── brokers/BrokersPage.tsx
│   │   ├── intelligence/          # one file per screen, see §8
│   │   ├── alerts/AlertsPage.tsx
│   │   └── audit/AuditPage.tsx
│   ├── routes.tsx                 # role-guarded route table
│   └── main.tsx
```

---

## 3. Environment Setup

```bash
# .env.local
VITE_API_BASE_URL=http://localhost:9000
VITE_WS_BASE_URL=ws://localhost:9000
```

The backend's own `docker-compose.yml` runs the API on **9000** externally (README's `/api/v1` base-URL section says 8000, but that's stale — trust `docker-compose.yml`/`Dockerfile`'s actual port mapping, confirm with `curl http://localhost:9000/health` before wiring the client).

CORS is already configured for Vite's dev server — no proxy config needed in `vite.config.ts` for local dev, but if you add a proxy anyway, keep credentials/headers passthrough intact for the `Authorization` header.

---

## 4. Auth Flow

**Demo accounts** (seeded by `scripts/seed_users.py` — must be run manually, it's not wired into app startup):

| Email | Password | Role |
|---|---|---|
| `admin@pios.com` | `admin@123` | admin |
| `trader@pios.com` | `trader@123` | trader |
| `quant@pios.com` | `quant@123` | quant |
| `viewer@pios.com` | `viewer@123` | viewer |
| `compliance@pios.com` | `compliance@123` | compliance |

**Login sequence**:
1. `POST /api/v1/auth/login` `{email, password}` → `{access_token, refresh_token, token_type}`. If `mfa_enabled` on the account, the backend enforces it at login (returns 401 without a valid flow — check the actual response shape against `auth.py` before assuming a two-step challenge/response; it may require the code up front rather than a separate step).
2. Store `access_token` in memory (Zustand), **not** `localStorage`, if you can avoid it — this is a trading terminal, XSS exposure on the access token is a real-money risk. `refresh_token` can go in an httpOnly cookie if the backend sets one, otherwise memory + a `sessionStorage` fallback for tab-refresh survival is the pragmatic middle ground (there's no first-party cookie flow here — confirm before assuming one).
3. Every request attaches `Authorization: Bearer <access_token>`.
4. On a 401, call `POST /api/v1/auth/refresh` with the refresh token once, retry the original request once, and if that also fails, force logout. Do this in **one** central place in `client.ts` — not per-hook — or you'll get refresh storms under concurrent requests (dedupe with an in-flight promise).
5. `GET /api/v1/auth/me` on app boot (with a stored token) to rehydrate the session before rendering protected routes.

**MFA setup** (`/auth/mfa/setup` → QR provisioning URI, `/auth/mfa/verify` → confirm code) is a real, working TOTP flow (pyotp-backed) — worth a real settings-page implementation, not a stub.

**Role-based route guarding** — mirror the backend's actual guards exactly, don't invent a different client-side taxonomy:
- `require_admin` → **admin only**
- `require_trade_exec` → **admin + trader** (order submission, broker CRUD, strategy creation, kill switch)
- Everything else → any authenticated role, but some endpoints self-scope by `owner_id` (brokers/orders list only their own unless admin)

```tsx
// routes.tsx sketch
<Route element={<RequireRole roles={["admin", "trader"]} />}>
  <Route path="/orders/new" element={<OrderTicket />} />
  <Route path="/brokers" element={<BrokersPage />} />
</Route>
<Route element={<RequireRole roles={["admin"]} />}>
  <Route path="/risk/limits" element={<RiskLimitsAdmin />} />
</Route>
```

---

## 5. API Client Layer

Group calls by backend domain (matches `app/api/v1/router.py` exactly — 16 endpoint modules, ~109 routes total):

`auth · users · brokers · market · orders · positions · strategies · risk · alerts · audit · data_quality · intelligence (38 routes) · behavior · capital · execution_quality (tca/data-integrity/market-ticks) · websocket`

A few contract details worth encoding into your TypeScript types **now**, because they reflect real backend behavior, not oversights you'll "fix later":

```ts
// types.ts
interface OrderOut {
  id: string;
  order_type: "MARKET" | "LIMIT" | "STOP" | "STOP_LIMIT" | "OCO" | "TWAP" | "VWAP" | "ICEBERG";
  execution_style: "INSTANT" | "ALGORITHMIC";  // see §8 — currently ALWAYS "INSTANT"
  status: "NEW" | "SUBMITTED" | "PARTIAL" | "FILLED" | "CANCELLED" | "REJECTED" | "EXPIRED";
  state_history: { from: string | null; to: string; reason: string | null; at: string }[];
  // ...
}

interface PortfolioMetricsOut {
  sharpe: number | null;     // null = insufficient equity history, NOT zero
  win_rate: number | null;   // null = no closed positions yet, NOT zero
  drawdown_limit: number;    // real RiskLimit value now, not a hardcoded 15.0
  // ...
}
```

**Order submission validation** — mirror the backend's Pydantic rule client-side so users get instant feedback instead of a round-trip 422: `price` is required when `order_type` is `LIMIT` or `STOP_LIMIT` (Zod: `.refine()` on the discriminated union).

**Broker creation** — `is_paper` is not just a sandbox toggle. For any `broker_type` other than `ALPACA`/`BINANCE`/`CCXT`, `is_paper` **must** be `true` or broker creation returns `422 UnsupportedBrokerError`. Surface this in the UI *before* submission — e.g., disable the "live trading" toggle and show "simulated fills only" copy whenever the selected broker type isn't one of the three real ones, rather than letting the user hit the 422.

**Kill switch** — `POST /risk/killswitch` requires `mfa_code` in the body if the acting admin has MFA enabled; the endpoint 400s with a clear message if it's missing. Build the kill-switch confirmation modal to prompt for the code inline when the user's own `mfa_enabled` is true (check `/auth/me`), not as an afterthought — this is the single most destructive action in the app (cancels all orders, closes all positions) and deserves its own confirm-with-consequences UI (show `orders_cancelled`/`positions_closed` counts from the response after it fires).

---

## 6. Real-Time Data Layer

There are **three** distinct real-time mechanisms in this backend — use each for what it's actually for, don't default to one everywhere:

### 6.1 Channel-subscription WebSocket — `/api/v1/ws`
Generic pub/sub bridge over 31 Redis channels (`why_not_trade`, `command_center`, `decision_feed`, `regime_history`, `order_flow`, `positions`, `equity_curve`, `alerts`, `trading_view_ticks`, `market_ticks`, etc. — see `app/core/pubsub.py:CHANNELS` for the authoritative list).

Protocol: connect, then send `{"type": "subscribe", "channel": "<name>", "symbol": "<SYM>"}`. You get a `{"type": "subscribed", ...}` ack, then pushed messages as that Redis channel publishes. Send `{"type": "ping"}` for keepalive.

This is the right channel for **live-updating dashboard tiles** tied to a symbol (regime badge, OFI gauge, command-center summary) — one shared WS connection, many channel subscriptions multiplexed over it. Build one `useChannelSocket` hook that manages a **single shared connection** (via a module-level singleton or a context provider) and lets components subscribe/unsubscribe to channels declaratively; don't open one WS per component.

### 6.2 SSE — `/api/v1/intelligence/stream`
Price + analytics + why-not-trade events for a list of symbols, auto-discovering active symbols from the DB if none given. One-way, simpler than WS, auto-reconnects natively via `EventSource`. Use this for a **market ticker strip** or **live price panel** — don't hand-roll WS reconnection logic for something `EventSource` already does for free.

```ts
const es = new EventSource(`${API_BASE}/intelligence/stream?symbols=BTC/USDT,ETH/USDT`);
es.addEventListener("price", (e) => { /* JSON.parse(e.data) */ });
es.addEventListener("analytics", (e) => { /* regime + technicals + signal conflict */ });
es.addEventListener("heartbeat", () => {});
```
Note: `EventSource` doesn't support custom headers, so auth here is query-param based (confirm the exact param name against `market_stream`'s dependency before wiring — it may need `?token=` like the WS endpoints do, since `Authorization` headers aren't available to `EventSource`).

### 6.3 SSE — `/api/v1/intelligence/notifications/stream`
Push notifications for constraint changes, regime shifts, feed staleness, kill-switch events. Use for a **toast/notification center**, decoupled from the main dashboard state.

### 6.4 The worker-cache gotcha (read this before building any `/intelligence/*` screen)
16 of the 38 `/intelligence/*` endpoints (decision-feed, regime, OFI, GMIG, adaptation, alpha, features, command-center, scenarios, traces, why-not-trade) read from a **Redis cache populated by a background worker** (`app/workers/intelligence_worker.py`) on a rolling loop, keyed **per-symbol** with a short TTL. This means:
- **Poll, don't expect instant freshness.** A `useQuery` with `refetchInterval: 3000-5000` is the right pattern for these — they update on the worker's cadence, not on-demand.
- **They can return `{"error": "not_yet_computed", "symbol": "..."}`** if the worker hasn't populated that symbol yet (new symbol, worker just restarted, etc.) — every one of these screens needs a real empty/loading state for this, not just a network-error state. Don't treat this shape as an exception — check for the `error` key and render a "waiting for data" state.
- The other ~13 `/intelligence/*` endpoints (`/decision/current`, `/montecarlo`, `/signal-conflict`, `/ofi/chart`, `/ofi/auto`, `/ofi/enhanced`, `/gmig/enhanced`, `/montecarlo/auto`, `/signal-conflict/auto`, `/market/*`) are computed **live, per-request** — no cache lag, but slower per-call (real DB + live exchange calls). Don't poll these as aggressively; on-demand + a 15-30s stale time is more appropriate.

---

## 7. Screens, Mapped to Backend Domains

| Screen | Backend surface | Notes |
|---|---|---|
| **Login / MFA setup** | `/auth/*` | Real TOTP flow, build it properly. |
| **Command Center (home dashboard)** | `/intelligence/command-center/current`, `/intelligence/quant-core/gates`, `/positions/metrics` | The 8-gate pipeline (`/quant-core/gates`) is real and computed live — good candidate for a gate-by-gate status strip (pass/fail/latency per gate). |
| **Order Ticket + Order Book** | `POST /orders`, `/orders`, `/orders/{id}/fills`, `/orders/{id}/tca` | Show `execution_style` badge on every order row (§8). Cancel button disabled once `status` is terminal (`FILLED`/`CANCELLED`/`REJECTED`/`EXPIRED`) — the backend now returns a clean 409 either way, but don't make users discover that by clicking. |
| **Positions & Portfolio** | `/positions`, `/positions/metrics`, `/positions/equity-curve` | Render `sharpe`/`win_rate` as "—" (not "0" or "0.0") when `null`. |
| **Strategy Lifecycle** | `/strategies`, `/strategies/{id}/advance`, `/strategies/{id}/backtest` | 8-stage pipeline (IDEA→...→RETIRED) — a horizontal stepper component with the current stage highlighted and a disabled "advance" button with a tooltip explaining the gate requirement (Sharpe ≥ 0.8, DD ≤ 15%, ≥200 trades for PAPER) is more useful than a bare dropdown. |
| **Risk & Kill Switch** | `/risk/metrics`, `/risk/limits`, `/risk/killswitch` | Kill switch needs the confirm-with-consequences treatment from §5. |
| **Broker Connections** | `/brokers/*` | Disable "live" toggle for unsupported types per §5. Show `latency_p99_ms`/`status`/`error_message` from the last `/test` call. |
| **Alerts** | `/alerts/*` | Severity-colored list (P1–P4), bulk-acknowledge action. |
| **Audit Log** | `/audit/*`, `/audit/verify` | A "chain verified ✓" indicator calling `/audit/verify` is a nice, cheap trust signal for a compliance-facing screen. |
| **Intelligence suite** (separate screens or a tabbed workspace) | Regime, OFI, GMIG, Monte Carlo, Signal Conflict, Adaptation Feed, Alpha Factory/Darwin, Behavior Monitor, Capital Allocation | See §6.4 for cache behavior per endpoint before building any of these. |

---

## 8. Backend Gaps the Frontend Must Handle Gracefully

These are real, current backend states (verified — see `project_dev.md`/`implementation-plan.md` for the full audit), not hypotheticals:

1. **TWAP/VWAP/OCO/ICEBERG are label-only.** They're selectable order types, but `execution_style` in the response is `"INSTANT"` for every order today — there is no slicing/algorithmic execution engine yet. **Show the `execution_style` value on every order, and consider a UI-level caveat** ("executes instantly, same as Market — algorithmic execution not yet available") when a user picks one of these four types, so they aren't surprised by an instant full fill.
2. **Only Alpaca and CCXT/Binance are real broker integrations.** Everything else (IBKR, OANDA, LMAX, MT5, Custom) requires `is_paper: true` and gets simulated fills. See §5.
3. **`sharpe`/`win_rate` are nullable**, not defaulted to a plausible-looking number. Always render a real empty state, never coerce to 0.
4. **Worker-cached `/intelligence/*` endpoints** can return a "not yet computed" shape for a given symbol. See §6.4.
5. **Mechanism Observatory, Optuna/walk-forward validation, Smart Order Router, and a Prometheus/Grafana-fed monitoring screen do not exist on the backend at all.** Don't build UI for these — they're real roadmap items (see `implementation-plan.md`'s Track B), not something the frontend is missing wiring for.
6. **Rate/latency**: several `/market/*` and live-enrichment paths (`/decision/current`, `/ofi/enhanced`, `/gmig/enhanced`) make real outbound exchange calls per request. Build loading states for these that assume 1-3s, not instant.

---

## 9. Design System with Tailwind

Trading terminals live or die on information density and consistent semantic color — don't let utility classes drift ad hoc per component. Define tokens once:

```js
// tailwind.config.js — extend, don't replace
theme: {
  extend: {
    colors: {
      pnl: { positive: '#22c55e', negative: '#ef4444', neutral: '#94a3b8' },
      regime: {
        bull: '#22c55e', bear: '#ef4444', range: '#eab308',
        crisis: '#dc2626', recovery: '#3b82f6',
      },
      decision: { allow: '#22c55e', block: '#ef4444', wait: '#eab308', reduce: '#f97316' },
      severity: { p1: '#dc2626', p2: '#f97316', p3: '#eab308', p4: '#3b82f6' },
      dq: { pass: '#22c55e', flag: '#eab308', reject: '#ef4444' },
    },
  },
}
```

Then build a handful of primitive components (`<PnLValue>`, `<RegimeBadge>`, `<DecisionBadge>`, `<SeverityDot>`) that consume these tokens once — every screen in §7 reuses the same 6-8 primitives instead of re-deriving color logic per page.

Default to a **dark theme** (standard for trading terminals, reduces eye strain for all-day monitoring) with a light-theme toggle as a nice-to-have, not a requirement.

Use a monospace font (`font-mono`) for all numeric columns (prices, quantities, P&L) — proportional fonts make columns of numbers visually misalign and are genuinely harder to scan quickly.

---

## 10. Suggested Build Phases

1. **Auth + shell**: login, MFA setup, role-guarded routing, API client with refresh handling. Nothing works without this.
2. **Orders + Positions**: the core trading loop — order ticket, order list, positions table, portfolio metrics. Get real fills flowing end-to-end against a Paper broker before anything else.
3. **Risk + Brokers**: risk metrics dashboard, kill switch (with real confirm UX), broker CRUD + test-connection.
4. **Strategies**: lifecycle stepper, backtest submission + polling, Darwin evolution visibility (read-only is fine initially).
5. **Real-time layer**: wire the channel WS + SSE streams once the REST-driven screens above are solid — real-time is additive polish on top of working request/response screens, not a prerequisite for them.
6. **Intelligence suite**: build these last and screen-by-screen, referencing §6.4's cache-vs-live split for each one as you go.
7. **Alerts + Audit**: lower-traffic, compliance-facing screens; straightforward CRUD/list UIs.

---

*This guide reflects the backend as of the Track A stabilization work (A1–A8) documented in `project_dev.md` and `implementation-plan.md`. Re-check §8 against those documents as Track B items ship — several of today's "label only" and "null by design" caveats are expected to become real features over time, and this guide will go stale exactly where that document's Track B roadmap says it will.*
