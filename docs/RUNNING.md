# Running Pi OS locally — the two-process requirement

The API server alone is **not** a complete dev environment. Several
frontend screens (most visibly the manual order ticket on `/execution`)
depend on data that only the **intelligence worker** produces, and the
worker itself depends on a **system user** existing in the database.
Miss either and the UI sits on "Waiting for the intelligence worker to
compute …" forever with no error anywhere.

## 1. Processes

```bash
# Terminal 1 — API server
uvicorn main:app --reload --port 9000

# Terminal 2 — intelligence worker (REQUIRED for command-center data,
# the order ticket, and every worker-cached /intelligence/* endpoint)
python -m app.workers.intelligence_worker
```

Both use the same Postgres/Redis containers (`docker-compose up -d db redis`
or `./run-local.sh`, whichever your setup uses).

## 2. The system user

The worker computes system-wide snapshots as a service account, looked up
by email at startup of every cycle:

- Default: `admin@pios.com`
- Override: set `SYSTEM_USER_EMAIL=<email>` in `.env`
  (see `app/core/config.py::Settings.SYSTEM_USER_EMAIL`)

The account must exist in the `users` table. On a fresh database, create it
first — e.g.:

```bash
curl -X POST http://localhost:9000/api/v1/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email": "admin@pios.com", "password": "<pick-one>", "full_name": "System"}'
```

If it's missing, the worker now logs a loud `❌ System user … not found`
line each cycle (it used to no-op silently) and computes nothing.

## 3. Symptoms → causes

| Symptom | Cause |
|---|---|
| Order ticket stuck on "Waiting for the intelligence worker…" | Worker process not running, or system user missing |
| `/intelligence/*` endpoints return `{"error": "not_yet_computed"}` forever | Same as above |
| Worker logs `❌ System user … not found` | Register the account or fix `SYSTEM_USER_EMAIL` |
| Live prices absent but command-center tiles fine | SSE stream is served by the API process — check exchange connectivity, not the worker |

## 4. Realtime channels (for reference)

- `GET /api/v1/ws` — multiplexed WebSocket. The worker broadcasts
  system-snapshot channels (`command_center`, …) per symbol; the API
  process publishes **user-scoped** `orders` / `positions` events after
  fills/cancels and global `alerts` events (see
  `app/services/trade_events.py`).
- `GET /api/v1/intelligence/stream` — SSE price/analytics/why-not-trade
  ticker, served by the API process directly (worker not required).
