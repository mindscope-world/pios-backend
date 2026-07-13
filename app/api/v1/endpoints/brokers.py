# app/api/v1/endpoints/brokers.py
import uuid
import json
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.deps import get_current_user, require_trade_exec
from app.core.security import encrypt_credentials
from app.db.session import get_db
from app.models.all_models import Broker, User
from app.schemas.all_schemas import (
    BrokerCreate, BrokerUpdate, BrokerOut,
    BrokerTestResult, PaginatedResponse, MessageResponse,
)
from app.services.broker_service import create_broker, get_adapter, get_broker_or_404
from app.services.audit_service import write_audit

router = APIRouter(prefix="/brokers", tags=["brokers"])


@router.get("", response_model=PaginatedResponse)
async def list_brokers(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Traders see only their own brokers.
    Admins see all brokers across all traders.
    """
    q = select(Broker).where(Broker.is_active == True)  # noqa: E712
    if current_user.role != "admin":
        q = q.where(Broker.owner_id == current_user.id)

    count_result = await db.execute(select(func.count()).select_from(q.subquery()))
    total = count_result.scalar_one()

    q = q.order_by(Broker.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(q)
    brokers = result.scalars().all()

    return PaginatedResponse(
        items=[BrokerOut.model_validate(b) for b in brokers],
        total=total, page=page, page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.post("", response_model=BrokerOut, status_code=201)
async def add_broker(
    data: BrokerCreate,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    """
    Traders register their own broker connection.
    Credentials are AES-256 encrypted before storage.
    """
    broker = await create_broker(db, data, current_user.id)
    await write_audit(
        db, "BROKER_ADDED", "broker", str(broker.id),
        actor_id=current_user.id, actor_email=current_user.email,
        after_state={"name": broker.name, "type": broker.broker_type, "is_paper": broker.is_paper},
    )
    await db.commit()
    return broker


@router.get("/{broker_id}", response_model=BrokerOut)
async def get_broker(
    broker_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)
    return broker


@router.patch("/{broker_id}", response_model=BrokerOut)
async def update_broker(
    broker_id: uuid.UUID,
    data: BrokerUpdate,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)

    if data.name is not None:
        broker.name = data.name
    if data.is_active is not None:
        broker.is_active = data.is_active
    if data.config is not None:
        broker.config = {**broker.config, **data.config}
    if data.credentials is not None:
        broker.credentials_enc = encrypt_credentials(json.dumps(data.credentials.model_dump()))

    await write_audit(db, "BROKER_UPDATED", "broker", str(broker_id),
                      actor_id=current_user.id, actor_email=current_user.email)
    await db.commit()
    return broker


@router.delete("/{broker_id}", response_model=MessageResponse)
async def remove_broker(
    broker_id: uuid.UUID,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)
    broker.is_active = False
    await write_audit(db, "BROKER_REMOVED", "broker", str(broker_id),
                      actor_id=current_user.id, actor_email=current_user.email)
    await db.commit()
    return MessageResponse(message="Broker removed")


@router.post("/{broker_id}/test", response_model=BrokerTestResult)
async def test_broker(
    broker_id: uuid.UUID,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    """Test connectivity and measure latency for this broker."""
    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)
    adapter = get_adapter(broker)
    result = await adapter.test_connection()

    from datetime import datetime, timezone
    broker.status = "CONNECTED" if result.success else "ERROR"
    broker.last_heartbeat = datetime.now(timezone.utc)
    broker.latency_p99_ms = result.latency_ms
    broker.error_message = None if result.success else result.message
    await db.commit()

    return result


@router.get("/{broker_id}/account", response_model=dict)
async def broker_account(
    broker_id: uuid.UUID,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    """Fetch live account balance / buying power from broker."""
    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)
    adapter = get_adapter(broker)
    try:
        return await adapter.get_account()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Broker error: {e}")


def _recon_symbol_key(symbol: str) -> str:
    """Normalize both symbol conventions to one comparison key: the app's
    "BTC/USDT" and Alpaca's "BTCUSD" (crypto quotes vs USD there) both →
    "BTCUSD"; equities pass through ("AAPL")."""
    s = symbol.strip().upper().replace("/", "")
    for alt in ("USDT", "USDC"):
        if s.endswith(alt):
            s = s[: -len(alt)] + "USD"
    return s


@router.get("/{broker_id}/reconciliation", response_model=dict)
async def broker_reconciliation(
    broker_id: uuid.UUID,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    """
    Broker↔app position reconciliation: what the broker account actually
    holds vs the requesting trader's app positions on this connection.
    Order-level fill sync keeps *orders* converged; this surfaces holdings
    drift — e.g. assets traded on the same broker account outside this app,
    or base-asset fee erosion on crypto.
    """
    from datetime import datetime, timezone
    from app.models.all_models import Position

    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)
    adapter = get_adapter(broker)
    try:
        broker_positions = await adapter.get_positions()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Broker error: {e}")

    result = await db.execute(
        select(Position)
        .options(selectinload(Position.symbol))
        .where(
            Position.user_id == current_user.id,
            Position.broker_id == broker_id,
            Position.is_open == True,  # noqa: E712
        )
    )
    app_positions = result.scalars().all()

    # Signed quantities keyed by normalized symbol
    broker_by_key: dict[str, dict] = {}
    for p in broker_positions:
        key = _recon_symbol_key(p["symbol"])
        broker_by_key[key] = {"symbol": p["symbol"], "qty": float(p["qty"])}
    app_by_key: dict[str, dict] = {}
    for p in app_positions:
        key = _recon_symbol_key(p.symbol.symbol if p.symbol else "")
        signed = float(p.qty) if p.side == "LONG" else -float(p.qty)
        entry = app_by_key.setdefault(key, {"symbol": p.symbol.symbol if p.symbol else key, "qty": 0.0})
        entry["qty"] += signed

    items = []
    for key in sorted(set(broker_by_key) | set(app_by_key)):
        b = broker_by_key.get(key)
        a = app_by_key.get(key)
        b_qty = b["qty"] if b else 0.0
        a_qty = a["qty"] if a else 0.0
        drift = b_qty - a_qty
        tolerance = max(abs(b_qty), abs(a_qty)) * 1e-6
        if b and not a:
            status = "BROKER_ONLY"
        elif a and not b:
            status = "APP_ONLY"
        elif abs(drift) <= tolerance:
            status = "MATCHED"
        else:
            status = "DRIFT"
        items.append({
            "symbol": (b or a)["symbol"],
            "broker_qty": b_qty,
            "app_qty": a_qty,
            "drift_qty": round(drift, 12),
            "status": status,
        })

    return {
        "broker_id": str(broker_id),
        "in_sync": all(i["status"] == "MATCHED" for i in items),
        "items": items,
        "note": (
            "Broker holdings are account-wide; app positions count only fills "
            "placed through this app by you. Drift is expected when the same "
            "broker account is traded elsewhere."
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


class AdoptHoldingRequest(BaseModel):
    symbol: str  # either convention — "BTCUSD" (broker) or "BTC/USDT" (app)


@router.post("/{broker_id}/reconciliation/adopt", response_model=dict)
async def adopt_broker_holding(
    broker_id: uuid.UUID,
    payload: AdoptHoldingRequest,
    current_user: User = Depends(require_trade_exec),
    db: AsyncSession = Depends(get_db),
):
    """
    Explicitly import one broker-side holding into the requesting trader's
    app positions (sets the app position to the broker's qty / avg entry).
    App positions are deliberately fill-based, so external holdings never
    import silently — this is the audited, user-initiated exception for
    accounts also traded outside the app.
    """
    from datetime import datetime, timezone
    from app.models.all_models import Position, Symbol
    from app.services.positions_service import write_pnl_snapshot
    from app.services.trade_events import publish_position_event

    broker = await get_broker_or_404(db, broker_id, current_user.id, current_user.role)
    adapter = get_adapter(broker)
    try:
        broker_positions = await adapter.get_positions()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Broker error: {e}")

    want = _recon_symbol_key(payload.symbol)
    holding = next(
        (p for p in broker_positions if _recon_symbol_key(p["symbol"]) == want), None
    )
    if holding is None or not float(holding["qty"]):
        raise HTTPException(status_code=404, detail=f"Broker holds no {payload.symbol}")

    # Map to a symbols-table row (both conventions in the table are possible)
    sym_result = await db.execute(select(Symbol).where(Symbol.is_active == True))  # noqa: E712
    symbol_row = next(
        (s for s in sym_result.scalars().all() if _recon_symbol_key(s.symbol) == want), None
    )
    if symbol_row is None:
        raise HTTPException(
            status_code=422,
            detail=f"No symbols-table row matches {payload.symbol} — seed it first",
        )

    signed_qty = float(holding["qty"])
    side = "LONG" if signed_qty > 0 else "SHORT"
    avg_cost = float(holding.get("avg_entry_price") or 0)

    pos_result = await db.execute(
        select(Position).where(
            Position.user_id == current_user.id,
            Position.broker_id == broker_id,
            Position.symbol_id == symbol_row.id,
            Position.is_open == True,  # noqa: E712
        )
    )
    position = pos_result.scalars().first()
    if position is None:
        position = Position(
            user_id=current_user.id,
            broker_id=broker_id,
            symbol_id=symbol_row.id,
            side=side,
            qty=abs(signed_qty),
            avg_cost=avg_cost,
        )
        db.add(position)
    else:
        position.side = side
        position.qty = abs(signed_qty)
        position.avg_cost = avg_cost
        position.is_open = True
    await db.flush()
    await write_pnl_snapshot(db, current_user.id)

    await write_audit(
        db, action="POSITION_ADOPTED", resource_type="position",
        resource_id=str(position.id), actor_id=current_user.id,
        actor_email=current_user.email,
        after_state={"symbol": symbol_row.symbol, "side": side,
                     "qty": str(abs(signed_qty)), "avg_cost": str(avg_cost),
                     "source": "broker_reconciliation_adopt"},
    )
    await db.commit()
    await publish_position_event(current_user.id, symbol_name=symbol_row.symbol)

    return {
        "adopted": True,
        "symbol": symbol_row.symbol,
        "side": side,
        "qty": abs(signed_qty),
        "avg_cost": avg_cost,
        "adopted_at": datetime.now(timezone.utc).isoformat(),
    }
