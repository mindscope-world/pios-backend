"""
Seeds a `symbols` row for every pair the Execution/Intelligence symbol
pickers offer, so each one gets intelligence-worker coverage and order
placement (the worker watches exactly the active rows of this table —
fetch_symbols() in intelligence_worker.py — and the order ticket 404s
without a row).

Crypto pairs come from the live Alpaca listing (list_alpaca_crypto_symbols,
the same source the frontend picker uses) in the app's X/USDT convention;
fiat/metals are the picker's static majors list in the DB's slash-less
convention (EURUSD — see get_symbol_by_name, which accepts both forms).

Idempotent: existing rows are left untouched. Run after the DB is up:

    python scripts/seed_picker_symbols.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select  # noqa: E402

from app.db.session import AsyncSessionLocal  # noqa: E402
from app.models.all_models import Symbol  # noqa: E402
from app.services.market_data_service import list_alpaca_crypto_symbols  # noqa: E402

# Mirrors FOREX_METAL_SYMBOLS in the frontend's ExecutionPage.tsx
FIAT_METAL_PAIRS = [
    ("EURUSD", "forex"), ("GBPUSD", "forex"), ("USDJPY", "forex"),
    ("USDCHF", "forex"), ("AUDUSD", "forex"), ("USDCAD", "forex"),
    ("NZDUSD", "forex"), ("EURGBP", "forex"), ("EURJPY", "forex"),
    ("GBPJPY", "forex"), ("XAUUSD", "metal"), ("XAGUSD", "metal"),
]


async def main() -> None:
    crypto = await list_alpaca_crypto_symbols()

    async with AsyncSessionLocal() as db:
        existing = {
            s for (s,) in (await db.execute(select(Symbol.symbol))).all()
        }
        added = 0

        for entry in crypto:
            sym = entry["symbol"]  # "ETH/USDT"
            if sym in existing:
                continue
            base = entry["base"]
            db.add(Symbol(
                symbol=sym, base_asset=base, quote_asset="USDT",
                asset_class="crypto", exchange="BINANCE", is_active=True, meta={},
            ))
            added += 1

        for sym, klass in FIAT_METAL_PAIRS:
            if sym in existing:
                continue
            db.add(Symbol(
                symbol=sym, base_asset=sym[:3], quote_asset=sym[3:],
                asset_class=klass, exchange="OANDA", is_active=True, meta={},
            ))
            added += 1

        await db.commit()
    print(f"seed_picker_symbols: {added} new rows ({len(existing)} already present)")


if __name__ == "__main__":
    asyncio.run(main())
