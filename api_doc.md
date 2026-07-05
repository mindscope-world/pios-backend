# API Endpoints for Forex and Crypto Data Access

## 🔄 Forex Data Access

### Primary Data Sources (Background Workers):

1. **OANDA Streaming API** (Preferred)
   - **URL:** `stream-fxpractice.oanda.com/v3` (practice) or `stream-fxtrade.oanda.com/v3` (live)
   - **Endpoint:** `/accounts/{account_id}/pricing/stream`
   - **Method:** GET
   - **Parameters:** `instruments` (comma-separated, e.g., "EUR_USD,GBP_USD")
   - **Headers:** 
     - `Authorization: Bearer {OANDA_API_KEY}`
     - `Accept-Datetime-Format: RFC3339`
   - **Description:** Real-time chunked JSON stream of price updates

2. **OANDA REST API** (Fallback)
   - **URL:** `api-fxpractice.oanda.com/v3` (practice) or `api-fxtrade.oanda.com/v3` (live)
   - **Endpoint:** `/accounts/{account_id}/pricing`
   - **Method:** GET
   - **Parameters:** `instruments` (comma-separated)
   - **Same headers as streaming API**
   - **Description:** One-shot REST snapshot when streaming unavailable

3. **Public Forex API** (Fallback when no OANDA credentials)
   - **URL:** `https://open.er-api.com/v6/latest/{base}` (base: USD, EUR, GBP, JPY)
   - **Method:** GET
   - **Description:** Free fallback rates service

### HTTP API Endpoints (Client Access to Stored Data):
- `GET /market/ticks/{base}/{quote}` - Historical ticks for forex pair
- `GET /market/tickers` - Latest snapshots for multiple forex pairs  
- `GET /market/tickers/latest` - Latest tickers with optional filtering

### Real-time WebSocket Access:
- **Endpoint:** `ws://{host}/ws` (or wss://)
- **Subscription:** Channel `"ticks"` with symbol `"EURUSD"` (normalized, no slash)
- **Message Format:** `{type: "tick", ...forex data...}`

## ₿ Crypto Data Access

### Primary Data Sources (Background Workers):

1. **Cryptocurrency Exchange WebSockets** (via ccxt.pro)
   - **Connections:** Direct WebSocket to exchanges (Binance, Coinbase, Kraken, etc.)
   - **Methods:**
     - `watch_trades_for_symbols` - Single subscription for all symbols (preferred)
     - `watch_trades` per symbol - Fallback method
   - **Description:** Real-time trade streams from cryptocurrency exchanges

### HTTP API Endpoints (Client Access to Stored Data):
- Same as forex:
  - `GET /market/ticks/{base}/{quote}` - Historical ticks for crypto pair (e.g., BTC/USDT)
  - `GET /market/tickers` - Latest snapshots for multiple crypto pairs
  - `GET /market/tickers/latest` - Latest tickers with optional filtering

### Real-time WebSocket Access:
- **Endpoint:** `ws://{host}/ws` (same as forex)
- **Subscription:** Channel `"ticks"` with symbol `"BTCUSDT"` (normalized, no slash)
- **Message Format:** `{type: "tick", ...crypto data...}`

## 🏗️ System Architecture Summary

1. **Data Collection Layer:** Background workers run `ForexProvider` and `CryptoProvider` classes
2. **Transport Layer:** Data published to Redis via `Publisher` service (`market_ticks` channel)
3. **Distribution Layer:** Redis listener (`core/pubsub.py`) forwards to WebSocket manager
4. **Real-time Delivery:** WebSocket server broadcasts to subscribed clients
5. **Persistence Layer:** Database workers store data for historical queries
6. **API Layer:** REST endpoints serve historical data from database

This provides dual access methods: **real-time WebSocket streaming** for live data and **REST API endpoints** for historical data retrieval.