"""
Seeds the default demo users, symbols, and risk limits.
Run once after your DB is up:

    # Inside the running api container:
    docker compose exec api python scripts/seed_users.py

    # Or directly if running locally:
    python scripts/seed_users.py
"""
import asyncio
import sys
import os

# Make sure the app root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select
from app.db.session import AsyncSessionLocal, engine, Base
from app.models.all_models import User, Symbol, RiskLimit
from app.core.security import hash_password


USERS = [
    {"email": "admin@pios.com",      "password": "admin@123",  "full_name": "Alex Chen",    "role": "admin"},
    {"email": "trader@pios.com",     "password": "trader@123", "full_name": "Sarah Kim",    "role": "trader"},
    {"email": "quant@pios.com",      "password": "quant@123",  "full_name": "Marcus Webb",  "role": "quant"},
    {"email": "viewer@pios.com",     "password": "viewer@123", "full_name": "Priya Sharma", "role": "viewer"},
    {"email": "compliance@pios.com", "password": "compliance@123", "full_name": "David Osei",   "role": "compliance"},
]

# SYMBOLS = [
#     {"symbol": "BTC/USDT", "base_asset": "BTC",  "quote_asset": "USDT", "asset_class": "crypto",   "exchange": "BINANCE"},
#     {"symbol": "ETH/USDT", "base_asset": "ETH",  "quote_asset": "USDT", "asset_class": "crypto",   "exchange": "BINANCE"},
#     {"symbol": "SOL/USDT", "base_asset": "SOL",  "quote_asset": "USDT", "asset_class": "crypto",   "exchange": "BINANCE"},
#     {"symbol": "AAPL",     "base_asset": "AAPL", "quote_asset": "USD",  "asset_class": "equities", "exchange": "NYSE"},
#     {"symbol": "TSLA",     "base_asset": "TSLA", "quote_asset": "USD",  "asset_class": "equities", "exchange": "NASDAQ"},
#     {"symbol": "EUR/USD",  "base_asset": "EUR",  "quote_asset": "USD",  "asset_class": "forex",    "exchange": "LMAX"},
#     {"symbol": "GBP/USD",  "base_asset": "GBP",  "quote_asset": "USD",  "asset_class": "forex",    "exchange": "LMAX"},
# ]

SYMBOLS = [

    # ──────────────────────────────────────────────────────────────────────────
    # 1. CRYPTO  — routed to CryptoProvider
    # ──────────────────────────────────────────────────────────────────────────

    # BTC pairs
    {"symbol": "BTC/USDT",  "base_asset": "BTC",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "BTC/USDC",  "base_asset": "BTC",  "quote_asset": "USDC", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "BTC/USD",   "base_asset": "BTC",  "quote_asset": "USD",  "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "BTC/EUR",   "base_asset": "BTC",  "quote_asset": "EUR",  "asset_class": "crypto", "exchange": "kraken"},

    # ETH pairs
    {"symbol": "ETH/USDT",  "base_asset": "ETH",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "ETH/USDC",  "base_asset": "ETH",  "quote_asset": "USDC", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "ETH/USD",   "base_asset": "ETH",  "quote_asset": "USD",  "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "ETH/BTC",   "base_asset": "ETH",  "quote_asset": "BTC",  "asset_class": "crypto", "exchange": "kraken"},

    # Large-cap alts
    {"symbol": "SOL/USDT",  "base_asset": "SOL",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "SOL/USD",   "base_asset": "SOL",  "quote_asset": "USD",  "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "XRP/USDT",  "base_asset": "XRP",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "XRP/USD",   "base_asset": "XRP",  "quote_asset": "USD",  "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "ADA/USDT",  "base_asset": "ADA",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "ADA/USD",   "base_asset": "ADA",  "quote_asset": "USD",  "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "DOGE/USDT", "base_asset": "DOGE", "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "DOGE/USD",  "base_asset": "DOGE", "quote_asset": "USD",  "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "AVAX/USDT", "base_asset": "AVAX", "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "AVAX/USD",  "base_asset": "AVAX", "quote_asset": "USD",  "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "DOT/USDT",  "base_asset": "DOT",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "LINK/USDT", "base_asset": "LINK", "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "MATIC/USDT","base_asset": "MATIC","quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "LTC/USDT",  "base_asset": "LTC",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "LTC/USD",   "base_asset": "LTC",  "quote_asset": "USD",  "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "UNI/USDT",  "base_asset": "UNI",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "ATOM/USDT", "base_asset": "ATOM", "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "BCH/USDT",  "base_asset": "BCH",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "BCH/USD",   "base_asset": "BCH",  "quote_asset": "USD",  "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "XLM/USDT",  "base_asset": "XLM",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "NEAR/USDT", "base_asset": "NEAR", "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "ICP/USDT",  "base_asset": "ICP",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "FIL/USDT",  "base_asset": "FIL",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "ETC/USDT",  "base_asset": "ETC",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "AAVE/USDT", "base_asset": "AAVE", "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "APT/USDT",  "base_asset": "APT",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "ARB/USDT",  "base_asset": "ARB",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "OP/USDT",   "base_asset": "OP",   "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "SUI/USDT",  "base_asset": "SUI",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},
    {"symbol": "TRX/USDT",  "base_asset": "TRX",  "quote_asset": "USDT", "asset_class": "crypto", "exchange": "kraken"},

    # ──────────────────────────────────────────────────────────────────────────
    # 2. EQUITIES  — routed to AlpacaProvider
    # ──────────────────────────────────────────────────────────────────────────

    # Mega-cap tech
    {"symbol": "AAPL",  "base_asset": "AAPL",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "MSFT",  "base_asset": "MSFT",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "NVDA",  "base_asset": "NVDA",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "GOOGL", "base_asset": "GOOGL", "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "AMZN",  "base_asset": "AMZN",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "META",  "base_asset": "META",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "TSLA",  "base_asset": "TSLA",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "AVGO",  "base_asset": "AVGO",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "ORCL",  "base_asset": "ORCL",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "AMD",   "base_asset": "AMD",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "INTC",  "base_asset": "INTC",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "QCOM",  "base_asset": "QCOM",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "CRM",   "base_asset": "CRM",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "ADBE",  "base_asset": "ADBE",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "NFLX",  "base_asset": "NFLX",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},

    # Finance
    {"symbol": "JPM",   "base_asset": "JPM",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "BAC",   "base_asset": "BAC",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "GS",    "base_asset": "GS",    "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "MS",    "base_asset": "MS",    "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "WFC",   "base_asset": "WFC",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "V",     "base_asset": "V",     "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "MA",    "base_asset": "MA",    "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "BRK.B", "base_asset": "BRK.B", "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "SPGI",  "base_asset": "SPGI",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "BLK",   "base_asset": "BLK",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},

    # Healthcare
    {"symbol": "JNJ",   "base_asset": "JNJ",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "UNH",   "base_asset": "UNH",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "LLY",   "base_asset": "LLY",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "PFE",   "base_asset": "PFE",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "ABBV",  "base_asset": "ABBV",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "MRK",   "base_asset": "MRK",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},

    # Consumer / Retail
    {"symbol": "WMT",   "base_asset": "WMT",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "COST",  "base_asset": "COST",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "HD",    "base_asset": "HD",    "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "MCD",   "base_asset": "MCD",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "NKE",   "base_asset": "NKE",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "SBUX",  "base_asset": "SBUX",  "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},

    # Energy
    {"symbol": "XOM",   "base_asset": "XOM",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "CVX",   "base_asset": "CVX",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},
    {"symbol": "COP",   "base_asset": "COP",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NYSE"},

    # ETFs
    {"symbol": "SPY",   "base_asset": "SPY",   "quote_asset": "USD", "asset_class": "equities", "exchange": "ARCA"},
    {"symbol": "QQQ",   "base_asset": "QQQ",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},
    {"symbol": "IWM",   "base_asset": "IWM",   "quote_asset": "USD", "asset_class": "equities", "exchange": "ARCA"},
    {"symbol": "DIA",   "base_asset": "DIA",   "quote_asset": "USD", "asset_class": "equities", "exchange": "ARCA"},
    {"symbol": "GLD",   "base_asset": "GLD",   "quote_asset": "USD", "asset_class": "equities", "exchange": "ARCA"},
    {"symbol": "TLT",   "base_asset": "TLT",   "quote_asset": "USD", "asset_class": "equities", "exchange": "NASDAQ"},

    # ──────────────────────────────────────────────────────────────────────────
    # 3. FOREX  — routed to ForexProvider (OANDA)
    # ──────────────────────────────────────────────────────────────────────────

    # Majors
    {"symbol": "EUR/USD", "base_asset": "EUR", "quote_asset": "USD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "GBP/USD", "base_asset": "GBP", "quote_asset": "USD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/JPY", "base_asset": "USD", "quote_asset": "JPY", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/CHF", "base_asset": "USD", "quote_asset": "CHF", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/CAD", "base_asset": "USD", "quote_asset": "CAD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "AUD/USD", "base_asset": "AUD", "quote_asset": "USD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "NZD/USD", "base_asset": "NZD", "quote_asset": "USD", "asset_class": "forex", "exchange": "OANDA"},

    # Minors / crosses
    {"symbol": "EUR/GBP", "base_asset": "EUR", "quote_asset": "GBP", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "EUR/JPY", "base_asset": "EUR", "quote_asset": "JPY", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "EUR/CHF", "base_asset": "EUR", "quote_asset": "CHF", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "EUR/CAD", "base_asset": "EUR", "quote_asset": "CAD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "EUR/AUD", "base_asset": "EUR", "quote_asset": "AUD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "EUR/NZD", "base_asset": "EUR", "quote_asset": "NZD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "GBP/JPY", "base_asset": "GBP", "quote_asset": "JPY", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "GBP/CHF", "base_asset": "GBP", "quote_asset": "CHF", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "GBP/CAD", "base_asset": "GBP", "quote_asset": "CAD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "GBP/AUD", "base_asset": "GBP", "quote_asset": "AUD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "GBP/NZD", "base_asset": "GBP", "quote_asset": "NZD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "AUD/JPY", "base_asset": "AUD", "quote_asset": "JPY", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "AUD/CAD", "base_asset": "AUD", "quote_asset": "CAD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "AUD/CHF", "base_asset": "AUD", "quote_asset": "CHF", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "AUD/NZD", "base_asset": "AUD", "quote_asset": "NZD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "NZD/JPY", "base_asset": "NZD", "quote_asset": "JPY", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "NZD/CAD", "base_asset": "NZD", "quote_asset": "CAD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "NZD/CHF", "base_asset": "NZD", "quote_asset": "CHF", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "CAD/JPY", "base_asset": "CAD", "quote_asset": "JPY", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "CAD/CHF", "base_asset": "CAD", "quote_asset": "CHF", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "CHF/JPY", "base_asset": "CHF", "quote_asset": "JPY", "asset_class": "forex", "exchange": "OANDA"},

    # Exotics
    {"symbol": "USD/SGD", "base_asset": "USD", "quote_asset": "SGD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/HKD", "base_asset": "USD", "quote_asset": "HKD", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/MXN", "base_asset": "USD", "quote_asset": "MXN", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/ZAR", "base_asset": "USD", "quote_asset": "ZAR", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/TRY", "base_asset": "USD", "quote_asset": "TRY", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/SEK", "base_asset": "USD", "quote_asset": "SEK", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/NOK", "base_asset": "USD", "quote_asset": "NOK", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/DKK", "base_asset": "USD", "quote_asset": "DKK", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/PLN", "base_asset": "USD", "quote_asset": "PLN", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/CZK", "base_asset": "USD", "quote_asset": "CZK", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/HUF", "base_asset": "USD", "quote_asset": "HUF", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/CNH", "base_asset": "USD", "quote_asset": "CNH", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "USD/INR", "base_asset": "USD", "quote_asset": "INR", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "EUR/SEK", "base_asset": "EUR", "quote_asset": "SEK", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "EUR/NOK", "base_asset": "EUR", "quote_asset": "NOK", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "EUR/PLN", "base_asset": "EUR", "quote_asset": "PLN", "asset_class": "forex", "exchange": "OANDA"},
    {"symbol": "EUR/TRY", "base_asset": "EUR", "quote_asset": "TRY", "asset_class": "forex", "exchange": "OANDA"},

    # ──────────────────────────────────────────────────────────────────────────
    # 4. METALS  — routed to ForexProvider (OANDA CFD)
    # ──────────────────────────────────────────────────────────────────────────

    {"symbol": "XAU/USD", "base_asset": "XAU", "quote_asset": "USD", "asset_class": "metal", "exchange": "OANDA"},
    {"symbol": "XAG/USD", "base_asset": "XAG", "quote_asset": "USD", "asset_class": "metal", "exchange": "OANDA"},
    {"symbol": "XPT/USD", "base_asset": "XPT", "quote_asset": "USD", "asset_class": "metal", "exchange": "OANDA"},
    {"symbol": "XPD/USD", "base_asset": "XPD", "quote_asset": "USD", "asset_class": "metal", "exchange": "OANDA"},
    {"symbol": "XAU/EUR", "base_asset": "XAU", "quote_asset": "EUR", "asset_class": "metal", "exchange": "OANDA"},
    {"symbol": "XAU/GBP", "base_asset": "XAU", "quote_asset": "GBP", "asset_class": "metal", "exchange": "OANDA"},
    {"symbol": "XAU/JPY", "base_asset": "XAU", "quote_asset": "JPY", "asset_class": "metal", "exchange": "OANDA"},
    {"symbol": "XAU/AUD", "base_asset": "XAU", "quote_asset": "AUD", "asset_class": "metal", "exchange": "OANDA"},
    {"symbol": "XAU/CHF", "base_asset": "XAU", "quote_asset": "CHF", "asset_class": "metal", "exchange": "OANDA"},

    # ──────────────────────────────────────────────────────────────────────────
    # 5. INDICES  — routed to ForexProvider (OANDA CFD)
    # ──────────────────────────────────────────────────────────────────────────

    # US
    {"symbol": "SPX500/USD",  "base_asset": "SPX500",  "quote_asset": "USD", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "NAS100/USD",  "base_asset": "NAS100",  "quote_asset": "USD", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "US30/USD",    "base_asset": "US30",    "quote_asset": "USD", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "US2000/USD",  "base_asset": "US2000",  "quote_asset": "USD", "asset_class": "index", "exchange": "OANDA"},

    # Europe
    {"symbol": "UK100/GBP",   "base_asset": "UK100",   "quote_asset": "GBP", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "DE30/EUR",    "base_asset": "DE30",    "quote_asset": "EUR", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "FR40/EUR",    "base_asset": "FR40",    "quote_asset": "EUR", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "EU50/EUR",    "base_asset": "EU50",    "quote_asset": "EUR", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "ES35/EUR",    "base_asset": "ES35",    "quote_asset": "EUR", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "IT40/EUR",    "base_asset": "IT40",    "quote_asset": "EUR", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "NL25/EUR",    "base_asset": "NL25",    "quote_asset": "EUR", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "CH20/CHF",    "base_asset": "CH20",    "quote_asset": "CHF", "asset_class": "index", "exchange": "OANDA"},

    # Asia-Pacific
    {"symbol": "JP225/USD",   "base_asset": "JP225",   "quote_asset": "USD", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "AU200/AUD",   "base_asset": "AU200",   "quote_asset": "AUD", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "HK33/HKD",    "base_asset": "HK33",    "quote_asset": "HKD", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "SG30/SGD",    "base_asset": "SG30",    "quote_asset": "SGD", "asset_class": "index", "exchange": "OANDA"},
    {"symbol": "TWIX/USD",    "base_asset": "TWIX",    "quote_asset": "USD", "asset_class": "index", "exchange": "OANDA"},

    # ──────────────────────────────────────────────────────────────────────────
    # 6. COMMODITIES  — routed to ForexProvider (OANDA CFD)
    # ──────────────────────────────────────────────────────────────────────────

    # Energy
    {"symbol": "BCO/USD",    "base_asset": "BCO",    "quote_asset": "USD", "asset_class": "commodity", "exchange": "OANDA"},
    {"symbol": "WTICO/USD",  "base_asset": "WTICO",  "quote_asset": "USD", "asset_class": "commodity", "exchange": "OANDA"},
    {"symbol": "NATGAS/USD", "base_asset": "NATGAS", "quote_asset": "USD", "asset_class": "commodity", "exchange": "OANDA"},

    # Agricultural
    {"symbol": "CORN/USD",   "base_asset": "CORN",   "quote_asset": "USD", "asset_class": "commodity", "exchange": "OANDA"},
    {"symbol": "SOYBN/USD",  "base_asset": "SOYBN",  "quote_asset": "USD", "asset_class": "commodity", "exchange": "OANDA"},
    {"symbol": "WHEAT/USD",  "base_asset": "WHEAT",  "quote_asset": "USD", "asset_class": "commodity", "exchange": "OANDA"},
    {"symbol": "SUGAR/USD",  "base_asset": "SUGAR",  "quote_asset": "USD", "asset_class": "commodity", "exchange": "OANDA"},
    {"symbol": "COPPER/USD", "base_asset": "COPPER", "quote_asset": "USD", "asset_class": "commodity", "exchange": "OANDA"},

    # ──────────────────────────────────────────────────────────────────────────
    # 7. BONDS  — routed to ForexProvider (OANDA CFD)
    # ──────────────────────────────────────────────────────────────────────────

    {"symbol": "USB02Y/USD", "base_asset": "USB02Y", "quote_asset": "USD", "asset_class": "bond", "exchange": "OANDA"},
    {"symbol": "USB05Y/USD", "base_asset": "USB05Y", "quote_asset": "USD", "asset_class": "bond", "exchange": "OANDA"},
    {"symbol": "USB10Y/USD", "base_asset": "USB10Y", "quote_asset": "USD", "asset_class": "bond", "exchange": "OANDA"},
    {"symbol": "USB30Y/USD", "base_asset": "USB30Y", "quote_asset": "USD", "asset_class": "bond", "exchange": "OANDA"},
    {"symbol": "UK10YB/GBP", "base_asset": "UK10YB", "quote_asset": "GBP", "asset_class": "bond", "exchange": "OANDA"},
    {"symbol": "DE10YB/EUR", "base_asset": "DE10YB", "quote_asset": "EUR", "asset_class": "bond", "exchange": "OANDA"},

]

RISK_LIMITS = [
    {"name": "Max Drawdown",       "limit_type": "max_drawdown_pct",  "limit_value": 15.0,    "breach_action": "KILL_SWITCH"},
    {"name": "Daily Loss Limit",   "limit_type": "daily_loss_limit",  "limit_value": 5000.0,  "breach_action": "BLOCK"},
    {"name": "Max Position (USD)", "limit_type": "max_position_usd",  "limit_value": 20000.0, "breach_action": "ALERT"},
    {"name": "Max Leverage",       "limit_type": "max_leverage",      "limit_value": 3.0,     "breach_action": "BLOCK"},
]


async def seed():
    # Ensure tables exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as db:

        # ── Users ─────────────────────────────────────────────────────────────
        print("Seeding users...")
        for u in USERS:
            print(f'Password for {u["email"]}: {u["password"]}')
            existing = await db.execute(select(User).where(User.email == u["email"]))
            if existing.scalar_one_or_none():
                print(f"  skip  {u['email']} (already exists)")
                continue
            db.add(User(
                email         = u["email"],
                password_hash = hash_password(u["password"]),
                full_name     = u["full_name"],
                role          = u["role"],
                is_active     = True,
            ))
            print(f"  added {u['email']} [{u['role']}]")

        # ── Symbols ───────────────────────────────────────────────────────────
        print("\nSeeding symbols...")
        for s in SYMBOLS:
            existing = await db.execute(select(Symbol).where(Symbol.symbol == s["symbol"]))
            if existing.scalar_one_or_none():
                print(f"  skip  {s['symbol']}")
                continue
            db.add(Symbol(**s))
            print(f"  added {s['symbol']}")

        # ── Risk limits ───────────────────────────────────────────────────────
        print("\nSeeding risk limits...")
        for r in RISK_LIMITS:
            existing = await db.execute(
                select(RiskLimit).where(RiskLimit.limit_type == r["limit_type"])
            )
            if existing.scalar_one_or_none():
                print(f"  skip  {r['name']}")
                continue
            db.add(RiskLimit(scope="global", is_active=True, **r))
            print(f"  added {r['name']}")

        await db.commit()

    print("\nDone. Summary:")
    print("  Users:       admin / trader / quant / viewer / compliance")
    print("  Symbols:     BTC/USDT, ETH/USDT, SOL/USDT, AAPL, TSLA, EUR/USD, GBP/USD")
    print("  Risk limits: drawdown, daily loss, max position, max leverage")


if __name__ == "__main__":
    asyncio.run(seed())