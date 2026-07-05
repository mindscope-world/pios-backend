import asyncio

from app.core.config import settings
from app.services.symbol_loader import load_symbols
from app.services.publisher import Publisher

from app.providers.crypto import CryptoProvider
from app.providers.stocks import AlpacaProvider
from app.providers.forex import ForexProvider
from app.workers.market_db_writer import market_db_writer
from app.workers.retension_task import retention_loop, rollup_loop


async def run_market_workers(session, redis):
    print("🚀 Starting market workers...")

    symbols = await load_symbols(session)
    publisher = Publisher(redis)

    # ─────────────────────────────────────────────
    # GROUP SYMBOLS BY PROVIDER TYPE
    # ─────────────────────────────────────────────
    crypto_symbols = []
    stock_symbols = []
    forex_pairs = []
    forex_symbol_map = {}

    for s in symbols:
        try:
            asset_class = (s.asset_class or "").strip().lower()

            # normalize equivalents
            if asset_class in ("equities", "equity", "stock", "stocks"):
                asset_class = "stock"

            elif asset_class in ("fx", "forex", "currency", "commodity", "metal", "bond", "index"):
                asset_class = "forex"

            elif asset_class in ("crypto", "cryptocurrency"):
                asset_class = "crypto"

            # routing
            if asset_class == "crypto":
                crypto_symbols.append(s)

            elif asset_class == "stock":
                stock_symbols.append(s.symbol)

            elif asset_class == "forex":
                forex_pairs.append(s.symbol)
                forex_symbol_map[s.symbol] = s.id

            else:
                print(f"⚠️ Unknown asset_class: {s.asset_class}")

        except Exception as e:
            print(f"⚠️ Symbol parsing error for {getattr(s, 'symbol', None)}: {e}")

    # ─────────────────────────────────────────────
    # TASK REGISTRY
    # ─────────────────────────────────────────────
    tasks = []

    # ─────────────────────────────────────────────
    # CORE WORKERS
    # ─────────────────────────────────────────────
    tasks.append(
        asyncio.create_task(
            market_db_writer(redis),
            name="market_db_writer"
        )
    )

    tasks.append(
        asyncio.create_task(
            rollup_loop(),
            name="rollup_loop"
        )
    )

    tasks.append(
        asyncio.create_task(
            retention_loop(),
            name="retention_loop"
        )
    )

    # ─────────────────────────────────────────────
    # CRYPTO PROVIDERS
    # ─────────────────────────────────────────────
    if crypto_symbols:
        try:
            crypto_fallback = ["kraken", "okx", "bybit", "kucoin"]
            tasks.append(
                asyncio.create_task(
                    CryptoProvider(crypto_fallback, crypto_symbols).start(publisher.publish),
                    name="crypto_kraken_fallback"
                )
            )
        except Exception as e:
            print(f"❌ Failed crypto provider: {e}")

    # ─────────────────────────────────────────────
    # STOCKS (ALPACA)
    # ─────────────────────────────────────────────
    if stock_symbols:
        try:
            tasks.append(
                asyncio.create_task(
                    AlpacaProvider(
                        symbols=stock_symbols,
                        api_key=settings.ALPACA_API_KEY,
                        secret=settings.ALPACA_API_SECRET,
                        paper=settings.ALPACA_PAPER,
                    ).start(publisher.publish),
                    name="alpaca"
                )
            )
        except Exception as e:
            print(f"❌ Failed stock provider: {e}")

    # ─────────────────────────────────────────────
    # FOREX PROVIDER
    # ─────────────────────────────────────────────
    if forex_pairs:
        try:
            tasks.append(
                asyncio.create_task(
                    ForexProvider(forex_pairs, forex_symbol_map).start(publisher.publish),
                    name="forex"
                )
            )
        except Exception as e:
            print(f"❌ Failed forex provider: {e}")

    print(f"📡 Started {len(tasks)} providers")

    # ─────────────────────────────────────────────
    # RUN SAFELY (LONG-RUNNING WORKERS)
    # ─────────────────────────────────────────────
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("🛑 Market workers shutdown requested")

        for t in tasks:
            t.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        print(f"⚠️ Unexpected orchestrator failure: {e}")