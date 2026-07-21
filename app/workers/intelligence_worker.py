import asyncio
import json
import logging
import signal
from typing import Any, List
import orjson
from fastapi.encoders import jsonable_encoder
from redis.asyncio import Redis


def _dumps(obj) -> bytes:
    """orjson.dumps with FastAPI's encoder as the unknown-type fallback.

    The compute_* services return API-layer values (ORM rows like Position/
    EquityPoint, Decimals, datetimes) that FastAPI would encode in a normal
    response path. Bare orjson raised on them here — and one bad payload
    (positions/equity_curve once the system user has real trading history)
    aborted the whole per-symbol persist+publish block, silently killing
    every pub/sub broadcast below the failing line for every symbol."""
    return orjson.dumps(obj, default=jsonable_encoder)
from app.helpers.helpers import get_symbol_by_name, latest_regime
from app.services.intelligence import prs_service
from app.models.all_models import Symbol, User
from app.services.intelligence.command_center_service import compute_command_center_current
from app.services.intelligence.why_not_trade_service import compute_why_not_trade
from app.services.intelligence.scenarios_service import compute_scenarios
from app.services.intelligence.ofi_service import compute_ofi
from app.services.intelligence.features_service import compute_features
from app.services.intelligence.decision_service import compute_decision_feed, compute_decision_traces
from app.services.intelligence.cross_market_service import compute_gmig_snapshot, compute_gmig_radar
from app.services.intelligence.regime_service import compute_regime_current, compute_regime_trend
from app.services.alpha_service import compute_alpha_darwin, compute_alpha_factory_state
from app.services.intelligence.adaptation_service import compute_adaptation_feed, compute_adaptation_active, compute_adaptation_drift
from app.services.intelligence.behavior_service import compute_behavior_session, compute_behavior_trend, compute_behavior_overrides
from app.services.risk_service import compute_risk_metrics
from app.services.intelligence.capital_service import compute_capital_allocation
from app.services.execution_service import compute_data_integrity_status
from app.services.data_quality_service import compute_data_quality_summary
from app.services.positions_service import compute_positions, compute_portfolio_metrics, compute_equity_curve
from app.services.strategy_service import compute_list_strategies
from app.services.alerts_service import compute_list_alerts
from app.services.trading_view_service import compute_tradingview_payload, compute_live_tick_payload
from app.core.config import settings
from app.db.session import AsyncSessionLocal
from sqlalchemy import select
from app.core.deps import get_system_user, get_user_by_id

REDIS = Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=False)

SYMBOL_REFRESH_INTERVAL = 10     # seconds
USER_REFRESH_INTERVAL = 15       # seconds
LOOP_INTERVAL = 0.1              # main loop delay
PRS_GRADE_INTERVAL = 60          # seconds -- V10.1 PRS grading pass (labeled MVP, see prs_service.py)

# How long each Redis snapshot stays readable. This must comfortably outlive
# one full worker cycle (every symbol × ~25 service computations, many with
# live exchange calls — realistically 30s to a few minutes), or the cache is
# expired most of the time and every consumer sits on "waiting for the
# intelligence worker" between passes. A stale-but-present snapshot (payloads
# carry their own evaluated_at) beats an absent one; 300s rides out slow
# cycles while still dying off if the worker actually stops.
SNAPSHOT_TTL_S = 300
MAX_CONCURRENCY = 50             # prevent overload


log = logging.getLogger(__name__)
# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def normalize_symbol(symbol: str) -> str:
    return symbol.replace("/", "")


def _publishable_payload(payload: Any) -> dict:
    if isinstance(payload, dict):
        return payload
    return {"payload": payload}


async def fetch_symbols():
    # is_active filter: retired/test rows must not burn a live-data fetch
    # (and an error log) every single cycle — deactivating a symbol row is
    # how coverage is switched off without deleting order history.
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Symbol.symbol).where(Symbol.is_active == True))  # noqa: E712
        return [row[0] for row in result.fetchall()]

async def fetch_users():
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(User.id))
        return [row[0] for row in result.fetchall()]


# ─────────────────────────────────────────────
# CORE PROCESSING
# ─────────────────────────────────────────────

async def process_symbol(symbol: str):
    """
    Per-symbol pipeline: runs why_not_trade + command_center for the
    primary symbol, then publishes results to Redis pub/sub channels.
    No FastAPI dependencies — creates its own db session directly.
    """
    key = normalize_symbol(symbol)

    async with AsyncSessionLocal() as db:
        # Fetch the symbol row so services can use it
        sym_row = await get_symbol_by_name(db, symbol)
        if not sym_row:
            print(f"⚠️  Symbol {symbol} not found in DB, skipping.")
            return

        # compute_why_not_trade needs: symbol, strategy_id, limit, user, db
        # In the worker context there's no authenticated user, so we pass
        # None for strategy_id and use a system/service user.
        # Adjust get_system_user() to return whatever user drives the engine.
        system_user = await get_system_user(db)
        if not system_user:
            from app.core.config import settings
            print(
                f"❌  System user {settings.SYSTEM_USER_EMAIL!r} not found in the users "
                f"table — the intelligence worker cannot compute anything without it, and "
                f"the frontend order ticket will sit on 'waiting for the intelligence "
                f"worker' forever. Fix: register that account (POST /auth/register or the "
                f"users admin screen), or point SYSTEM_USER_EMAIL in .env at an existing "
                f"user. See docs/RUNNING.md."
            )
            return

        # Per-symbol computations MUST receive `symbol` — these results are
        # cached under this symbol's key, and without the argument each
        # service falls back to the primary symbol, silently filing (say)
        # BTC/USDT data under command_center:EURUSD.
        try:
            why_not_trade = await compute_why_not_trade(
                current_user=system_user,
                db=db,
                symbol=symbol,
            )
        except Exception as e:
            print(f"⚠️  compute_why_not_trade failed for {symbol}: {e}")
            why_not_trade = {"error": str(e)}

        try:
            command_center = await compute_command_center_current(current_user=system_user, db=db, symbol=symbol)
        except Exception as e:
            print(f"⚠️  compute_command_center_current failed for {symbol}: {e}")
            command_center = {"error": str(e)}

        # V10.1 PRS substrate (labeled MVP -- see prs_service.py docstring):
        # record this cycle's decision so it can be graded later against
        # actual price movement. Never fatal to the rest of the pipeline.
        try:
            if not command_center.get("error"):
                regime_row = await latest_regime(db, sym_row.id)
                await prs_service.record_decision(
                    db, sym_row.id,
                    decision=command_center.get("decision", "WAIT"),
                    confidence=command_center.get("confidence") or 0.0,
                    regime_label=regime_row.regime_label if regime_row else "RANGE",
                    price=command_center.get("live_market", {}).get("price"),
                )
        except Exception as e:
            print(f"⚠️  PRS record_decision failed for {symbol}: {e}")

        try:
            scenarios = await compute_scenarios(current_user=system_user, db=db, symbol=symbol)
        except Exception as e:
            print(f"⚠️  scenarios failed for {symbol}: {e}")
            scenarios = {"error": str(e)}
        
        try:
            decision_feed = await compute_decision_feed(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️  Decision feed failed for {symbol}: {e}")
            decision_feed = {"error": str(e)}
        
        try:
            decision_traces = await compute_decision_traces(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️  Decision traces failed for {symbol}: {e}")
            decision_traces = {"error": str(e)}
        
        try:
            order_flow = await compute_ofi(current_user=system_user, db=db, symbol=symbol)
        except Exception as e:
            print(f"⚠️  order flow intell failed for {symbol}: {e}")
            order_flow = {"error": str(e)}
        
        try:
            features = await compute_features(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Features failed for {symbol}: {e}")
            features = {"error": str(e)}
        
        try:
            gmig_snapshot = await compute_gmig_snapshot(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Cross market snapshot failed for {symbol}: {e}")
            gmig_snapshot = {"error": str(e)}
            
        try:
            gmig_radar = await compute_gmig_radar(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Cross market Radar failed for {symbol}: {e}")
            gmig_radar = {"error": str(e)}
        
        try:
            regime_history = await compute_regime_current(current_user=system_user, db=db, symbol=symbol)
        except Exception as e:
            print(f"⚠️ Regime history failed for {symbol}: {e}")
            regime_history = {"error": str(e)}
        
        try:
            regime_trend = await compute_regime_trend(db=db)
        except Exception as e:
            print(f"⚠️ Regime trend failed for {symbol}: {e}")
            regime_trend = {"error": str(e)}
        
        try:
            alpha_state = await compute_alpha_factory_state(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Alpha State failed for {symbol}: {e}")
            alpha_state = {"error": str(e)}
        
        try:
            alpha_darwin = await compute_alpha_darwin(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Alpha Darwin failed for {symbol}: {e}")
            alpha_darwin = {"error": str(e)}
        
        try:
            adaptation_feed = await compute_adaptation_feed(db=db)
        except Exception as e:
            print(f"⚠️ Adaptation feed failed for {symbol}: {e}")
            adaptation_feed = {"error": str(e)}
        
        try:
            adaptation_active = await compute_adaptation_active(db=db)
        except Exception as e:
            print(f"⚠️ Adaptation active failed for {symbol}: {e}")
            adaptation_active = {"error": str(e)}
        
        try:
            adaptation_drift = await compute_adaptation_drift(db=db)
        except Exception as e:
            print(f"⚠️ Adaptation drift failed for {symbol}: {e}")
            adaptation_drift = {"error": str(e)}
        
        try:
            behavior_session = await compute_behavior_session(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Behavior session failed for {symbol}: {e}")
            behavior_session = {"error": str(e)}
        
        try:
            behavior_trend = await compute_behavior_trend(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Behavior trend failed for {symbol}: {e}")
            behavior_trend = {"error": str(e)}
        
        try:
            behavior_overrides = await compute_behavior_overrides(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Behavior overrides failed for {symbol}: {e}")
            behavior_overrides = {"error": str(e)}
        
        # try:
        #     risk_metrics = await compute_risk_metrics(current_user=system_user, db=db)
        # except Exception as e:
        #     print(f"⚠️ Risk Metrics failed for {symbol}: {e}")
        #     risk_metrics = {"error": str(e)}
        
        try:
            capital_allocation = await compute_capital_allocation(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Capital Allocation failed for {symbol}: {e}")
            capital_allocation = {"error": str(e)}
        
        # DISABLED (2026-07-21) -- unlike every other function in this loop,
        # compute_rebalance() is not a read-only snapshot: every call creates
        # a real BacktestJob row (config={"type": "REBALANCE"}), dispatches a
        # real Celery task, and writes a real "REBALANCE_TRIGGERED" audit_log
        # row. Calling it once per symbol per worker cycle (this loop runs
        # over the full symbol universe every SNAPSHOT_TTL_S-ish) had been
        # spamming backtest_jobs (33,000+ rows found live, all attached to
        # whichever strategy the system user happens to own -- see
        # strategy_service._check_gate's comment) and audit_log with fake
        # triggered-rebalance entries for as long as this worker has run.
        # Nothing consumes the capital_rebalance:{key} cache key or pubsub
        # channel below (checked repo-wide) -- this call served no purpose
        # other than its side effects. Do not re-enable without first making
        # compute_rebalance side-effect-free for a read-only caller, or
        # giving this loop a real interval/dedup guard.
        capital_rebalance = {"disabled": "see 2026-07-21 comment in this file"}

        try:
            data_integrity = await compute_data_integrity_status(db=db)
        except Exception as e:
            print(f"⚠️ Data integrity failed for {symbol}: {e}")
            data_integrity = {"error": str(e)}
        
        # try:
        #     data_quality = await compute_data_quality_summary(db=db)
        # except Exception as e:
        #     print(f"⚠️ Data quality failed for {symbol}: {e}")
        #     data_quality = {"error": str(e)}
        
        try:
            positions = await compute_positions(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Positions failed for {symbol}: {e}")
            positions = {"error": str(e)}
        
        # try:
        #     portfolio_metrics = await compute_portfolio_metrics(current_user=system_user, db=db)
        # except Exception as e:
        #     print(f"⚠️ Portfolio metrics failed for {symbol}: {e}")
        #     portfolio_metrics = {"error": str(e)}
        
        try:
            equity_curve = await compute_equity_curve(current_user=system_user, db=db)
        except Exception as e:
            print(f"⚠️ Equity curve failed for {symbol}: {e}")
            equity_curve = {"error": str(e)}
        
        # try:
        #     strategies = await compute_list_strategies(db=db)
        # except Exception as e:
        #     print(f"⚠️ Strategies failed for {symbol}: {e}")
        #     strategies = {"error": str(e)}
        
        # try:
        #     alerts = await compute_list_alerts(current_user=system_user, db=db)
        # except Exception as e:
        #     print(f"⚠️ Alerts failed for {symbol}: {e}")
        #     alerts = {"error": str(e)}
        
        try:
            tv_payload = await compute_tradingview_payload(symbol=symbol, resolution="1", db=db)
        except Exception as e:
            print(f"⚠️ tradingview failed for {symbol}: {e}")
            tv_payload = {"error": str(e)}
            
        try:
            market_ticks = await compute_live_tick_payload(symbol=symbol, db=db)
        except Exception as e:
            print(f"⚠️ market_ticks failed for {symbol}: {e}")
            market_ticks = {"error": str(e)}

    # ── Persist to Redis (short TTL — these are live snapshots) ──────────
    await REDIS.set(f"why_not_trade:{key}",  _dumps(why_not_trade),  ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"command_center:{key}", _dumps(command_center), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"scenarios:{key}", _dumps(scenarios), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"decision_feed:{key}", _dumps(decision_feed), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"decision_traces:{key}", _dumps(decision_traces), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"order_flow:{key}", _dumps(order_flow), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"features:{key}", _dumps(features), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"gmig_snapshot:{key}", _dumps(gmig_snapshot), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"gmig_radar:{key}", _dumps(gmig_radar), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"regime_history:{key}", _dumps(regime_history), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"regime_trend:{key}", _dumps(regime_trend), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"alpha_state:{key}", _dumps(alpha_state), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"alpha_darwin:{key}", _dumps(alpha_darwin), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"adaptation_feed:{key}", _dumps(adaptation_feed), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"adaptation_active:{key}", _dumps(adaptation_active), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"adaptation_drift:{key}", _dumps(adaptation_drift), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"behavior_session:{key}", _dumps(behavior_session), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"behavior_trend:{key}", _dumps(behavior_trend), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"behavior_overrides:{key}", _dumps(behavior_overrides), ex=SNAPSHOT_TTL_S)
    # await REDIS.set(f"risk_metrics:{key}", _dumps(risk_metrics), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"capital_allocation:{key}", _dumps(capital_allocation), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"capital_rebalance:{key}", _dumps(capital_rebalance), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"data_integrity:{key}", _dumps(data_integrity), ex=SNAPSHOT_TTL_S)
    # await REDIS.set(f"data_quality:{key}", _dumps(data_quality), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"positions:{key}", _dumps(positions), ex=SNAPSHOT_TTL_S)
    # await REDIS.set(f"portfolio_metrics:{key}", _dumps(portfolio_metrics), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"equity_curve:{key}", _dumps(equity_curve), ex=SNAPSHOT_TTL_S)
    # await REDIS.set(f"strategies:{key}", _dumps(strategies), ex=SNAPSHOT_TTL_S)
    # await REDIS.set(f"alerts:{key}", _dumps(alerts), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"trading_view_ticks:{symbol}", _dumps(tv_payload), ex=SNAPSHOT_TTL_S)
    await REDIS.set(f"market_ticks:{symbol}", _dumps(market_ticks), ex=SNAPSHOT_TTL_S)


    # ── Pub/sub broadcast ────────────────────────────────────────────────
    await REDIS.publish("why_not_trade", _dumps({"symbol": key, **_publishable_payload(why_not_trade)}),)
    await REDIS.publish("command_center", _dumps({"symbol": key, **_publishable_payload(command_center)}),)
    await REDIS.publish("scenarios", _dumps({"symbol": key, **_publishable_payload(scenarios)}),)
    await REDIS.publish("decision_feed", _dumps({"symbol": key, **_publishable_payload(decision_feed)}),)
    await REDIS.publish("decision_traces", _dumps({"symbol": key, **_publishable_payload(decision_traces)}),)
    await REDIS.publish("order_flow", _dumps({"symbol": key, **_publishable_payload(order_flow)}),)
    await REDIS.publish("features", _dumps({"symbol": key, **_publishable_payload(features)}),)
    await REDIS.publish("gmig_snapshot", _dumps({"symbol": key, **_publishable_payload(gmig_snapshot)}),)
    await REDIS.publish("gmig_radar", _dumps({"symbol": key, **_publishable_payload(gmig_radar)}),)
    await REDIS.publish("regime_history", _dumps({"symbol": key, **_publishable_payload(regime_history)}),)
    await REDIS.publish("regime_trend", _dumps({"symbol": key, **_publishable_payload(regime_trend)}),)
    await REDIS.publish("alpha_state", _dumps({"symbol": key, **_publishable_payload(alpha_state)}),)
    await REDIS.publish("alpha_darwin", _dumps({"symbol": key, **_publishable_payload(alpha_darwin)}),)
    await REDIS.publish("adaptation_feed", _dumps({"symbol": key, **_publishable_payload(adaptation_feed)}),)
    await REDIS.publish("adaptation_active", _dumps({"symbol": key, **_publishable_payload(adaptation_active)}),)
    await REDIS.publish("adaptation_drift", _dumps({"symbol": key, **_publishable_payload(adaptation_drift)}),)
    await REDIS.publish("behavior_session", _dumps({"symbol": key, **_publishable_payload(behavior_session)}),)
    await REDIS.publish("behavior_trend", _dumps({"symbol": key, **_publishable_payload(behavior_trend)}),)
    await REDIS.publish("behavior_overrides", _dumps({"symbol": key, **_publishable_payload(behavior_overrides)}),)
    # await REDIS.publish("risk_metrics", _dumps({"symbol": key, **_publishable_payload(risk_metrics)}),)
    await REDIS.publish("capital_allocation", _dumps({"symbol": key, **_publishable_payload(capital_allocation)}),)
    await REDIS.publish("capital_rebalance", _dumps({"symbol": key, **_publishable_payload(capital_rebalance)}),)
    await REDIS.publish("data_integrity", _dumps({"symbol": key, **_publishable_payload(data_integrity)}),)
    # await REDIS.publish("data_quality", _dumps({"symbol": key, **_publishable_payload(data_quality)}),)
    await REDIS.publish("positions", _dumps({"symbol": key, **_publishable_payload(positions)}),)
    # await REDIS.publish("portfolio_metrics", _dumps({"symbol": key, **_publishable_payload(portfolio_metrics)}),)
    await REDIS.publish("equity_curve", _dumps({"symbol": key, **_publishable_payload(equity_curve)}),)
    # await REDIS.publish("strategies", _dumps({"symbol": key, **_publishable_payload(strategies)}),)
    # await REDIS.publish("alerts", _dumps({"symbol": key, **_publishable_payload(alerts)}),)
    await REDIS.publish("trading_view_ticks", _dumps({"symbol": symbol, **_publishable_payload(tv_payload)}),)
    await REDIS.publish("market_ticks", _dumps({"symbol": symbol, **_publishable_payload(market_ticks)}),)

    print(
        f"📢  {symbol} | decision={why_not_trade.get('final_decision')} "
        f"| reason={why_not_trade.get('reason', '—')}"
    )


async def process_user(user_id: int, symbol: str):
    """
    Per-user, per-symbol pipeline: enriches the shared command_center
    snapshot with the user's portfolio context, then publishes a
    personalised command to Redis.
    """
    key = normalize_symbol(symbol)

    # ── Guard: need portfolio + a valid command center snapshot ──────────
    portfolio_raw = await REDIS.get(f"portfolio:{user_id}")
    if not portfolio_raw:
        return  # portfolio not yet cached — skip silently

    decision_raw = await REDIS.get(f"command_center:{key}")
    if not decision_raw:
        return  # symbol not yet evaluated — skip silently

    # ── Fetch this user's personalised command center ────────────────────
    async with AsyncSessionLocal() as db:
        user = await get_user_by_id(db, user_id)
        if not user:
            return

        try:
            command = await compute_command_center_current(
                db=db,
                current_user=user,
            )
        except Exception as e:
            print(f"⚠️  compute_command_center_current failed for user {user_id}/{symbol}: {e}")
            return

    # ── Persist + broadcast ───────────────────────────────────────────────
    await REDIS.set(f"command_center:{user_id}:{key}", _dumps(command), ex=2,)
    await REDIS.publish(
        "command_center",
        _dumps({"user_id": user_id, "symbol": key, **_publishable_payload(command)}),
    )

# ─────────────────────────────────────────────
# WORKER LOOP
# ─────────────────────────────────────────────

class Worker:
    def __init__(self):
        self.symbols: List[str] = []
        self.users: List[int] = []
        self.running = True
        self.sem = asyncio.Semaphore(MAX_CONCURRENCY)
        self._background_tasks: list[asyncio.Task] = []

    async def refresh_symbols(self):
        while self.running:
            try:
                self.symbols = await fetch_symbols()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"refresh_symbols error: {e}")
            await asyncio.sleep(SYMBOL_REFRESH_INTERVAL)

    async def refresh_users(self):
        while self.running:
            try:
                self.users = await fetch_users()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"refresh_users error: {e}")
            await asyncio.sleep(USER_REFRESH_INTERVAL)

    async def grade_prs(self):
        """V10.1 PRS grading pass (labeled MVP, see prs_service.py) — grades
        every QuantDecision whose horizon has elapsed, independent of the
        per-symbol pipeline above."""
        while self.running:
            try:
                async with AsyncSessionLocal() as db:
                    graded = await prs_service.grade_pending_decisions(db)
                    if graded:
                        log.info(f"PRS: graded {graded} decision(s)")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"grade_prs error: {e}")
            await asyncio.sleep(PRS_GRADE_INTERVAL)

    async def safe_run(self, coro):
        async with self.sem:
            task = asyncio.ensure_future(coro)
            try:
                await task
            except asyncio.CancelledError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise
            except Exception as e:
                log.error(f"safe_run error: {e}")

    async def run(self):
        self._background_tasks = [
            asyncio.create_task(self.refresh_symbols(), name="refresh_symbols"),
            asyncio.create_task(self.refresh_users(),   name="refresh_users"),
            asyncio.create_task(self.grade_prs(),        name="grade_prs"),
        ]

        try:
            while self.running:
                if not self.symbols or not self.users:
                    await asyncio.sleep(1)
                    continue

                symbol_tasks = [
                    asyncio.ensure_future(self.safe_run(process_symbol(s)))
                    for s in self.symbols
                ]
                results = await asyncio.gather(*symbol_tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
                        log.error(f"symbol task error: {r}")

                user_tasks = [
                    asyncio.ensure_future(self.safe_run(process_user(u, s)))
                    for u in self.users
                    for s in self.symbols
                ]
                results = await asyncio.gather(*user_tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
                        log.error(f"user task error: {r}")

                await asyncio.sleep(LOOP_INTERVAL)

        except asyncio.CancelledError:
            log.info("Worker.run() cancelled — cleaning up")
            # Cancel any in-flight tasks
            for task in symbol_tasks + user_tasks:
                task.cancel()
            await asyncio.gather(*symbol_tasks, *user_tasks, return_exceptions=True)
            raise

        finally:
            await self._cancel_background_tasks()

    async def _cancel_background_tasks(self):
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    def stop(self):
        self.running = False


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

worker = Worker()


def shutdown():
    print("🛑 Shutting down worker...")
    worker.stop()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    try:
        loop.run_until_complete(worker.run())
    finally:
        loop.close()