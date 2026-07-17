# Pi OS MT5 Bridge EA

`PiOSBridgeEA.mq5` connects a MetaTrader 5 terminal to the Pi OS backend's
EA bridge (`/api/v1/ws/mt5/{broker_id}`), so an MT5-type broker connection
in Pi OS executes real orders in the terminal. Pure MQL5 — WebSocket client
(plain `ws://` and TLS `wss://`) over the terminal's native socket API, no
DLLs, no external libraries.

## Install & compile

1. In MT5: **File → Open Data Folder** → copy `PiOSBridgeEA.mq5` into
   `MQL5/Experts/`.
2. Open MetaEditor (F4), open the file, **Compile** (F7). It must compile
   with 0 errors (warnings are OK). Report any errors back — they're fixable
   without a terminal.

## Terminal settings (both required)

1. **Tools → Options → Expert Advisors → "Allow WebRequest for listed URL"**
   — add the server host. This allowlist also gates raw socket connections,
   so without it `SocketConnect` fails instantly. Add the host only, e.g.:
   - `https://sort-council-sen-mostly.trycloudflare.com` (public tunnel), or
   - `http://192.168.x.x` (LAN, direct to the backend).
2. Enable **Algo Trading** (toolbar button must be green) and tick
   "Allow Algo Trading" in the EA's dialog when attaching.

## EA inputs

| Input | Meaning | Public tunnel | Same LAN |
|---|---|---|---|
| `InpServerHost` | host only, no scheme/path | `sort-council-sen-mostly.trycloudflare.com` | the backend machine's LAN IP |
| `InpServerPort` | | `443` | `9000` |
| `InpUseTLS` | | `true` | `false` |
| `InpBrokerId` | the Pi OS broker connection's UUID | shown in the broker detail modal / API | same |
| `InpPassphrase` | the passphrase set when creating the MT5 broker in Pi OS | | |
| `InpSymbolSuffix` | broker symbol suffix if any (`.a`, `.raw`, …) | | |
| `InpMapUSDTtoUSD` | `BTC/USDT` → `BTCUSD` for MT5 brokers quoting in USD | `true` | |

**Volume semantics:** the app's order qty arrives as MT5 **lots**, clamped
to the symbol's min/step/max. (An app order of qty `0.01` on XAU/USD = 0.01
lots.)

**Tunnel caveat:** the trycloudflare hostname changes every time cloudflared
restarts — the EA input (and the terminal allowlist entry) must be updated
to the new host. For anything long-lived, prefer the LAN address or a named
Cloudflare tunnel with a stable hostname.

## Pairing check

Attach the EA → the chart comment should walk `upgrading → pairing →
CONNECTED`, and the Experts log prints
`PiOS bridge: paired -- EA connected for broker <id>`. In Pi OS, the broker's
**Test connection** now reports "EA connected" with a latency, and orders
routed to that broker execute in the terminal.

If it stays on `pairing` and reconnects every ~10 s, the broker id or
passphrase is wrong (the server closes the socket on a bad token without a
reply — the EA logs this hint).

## What the EA implements

| Server request | EA action / reply |
|---|---|
| `PING` | `PONG` (drives the Test-connection latency number) |
| `GET_ACCOUNT` | `ACCOUNT_INFO` — buying_power (free margin), equity, balance, currency, login, server, leverage |
| `GET_POSITIONS` | `POSITIONS` — per position: symbol (suffix stripped), signed qty in lots, avg entry, unrealized P&L |
| `PLACE_ORDER` MARKET | `CTrade.Buy/Sell`, replies with ticket + inline fill (price/qty) → app status FILLED |
| `PLACE_ORDER` LIMIT / STOP | pending order placed, replies with ticket, no fill → app status SUBMITTED |
| `CANCEL_ORDER` | `OrderDelete` on the pending ticket, `CANCEL_RESULT` |
| `GET_ORDER` | `ORDER_STATUS` — the ticket's current state in app vocabulary (SUBMITTED / PARTIAL / FILLED / CANCELLED / EXPIRED / REJECTED, `UNKNOWN` if the ticket is in neither the working set nor history) with cumulative filled lots + volume-weighted average price from the order's history deals. Drives the backend's 15 s fill-sync poll |
| WS ping/close | pong / clean reconnect (also sends its own keep-alive ping every 30 s) |

**Unsolicited pushes (no server request):** `OnTradeTransaction` sends an
`ORDER_UPDATE` frame the moment a deal executes against one of the EA's
tickets (carrying both the cumulative order state *and* that deal's actual
print price/volume, so the app records the fill at the real execution
price) and when a pending order dies broker-side (cancelled / expired /
rejected). Manual terminal trades (magic 0) are filtered out; the server
additionally ignores tickets it never placed.

TWAP/VWAP/ICEBERG and STOP_LIMIT/OCO never reach the EA — the backend
slices/arms those app-side and sends plain MARKET/LIMIT children.

## Resting-order fill sync (how LIMIT/STOP fills reach the app)

Two paths, mirroring the app's Alpaca integration:

1. **Push** — `ORDER_UPDATE` from `OnTradeTransaction`, instant, real print
   price. The status walks SUBMITTED → PARTIAL → FILLED on the Orders page
   with no refresh.
2. **Poll safety net** — every `MT5_FILL_SYNC_INTERVAL_SECS` (default 15 s)
   the backend asks `GET_ORDER` for each open MT5 ticket whose EA is
   connected (`app/services/mt5_fill_sync.py`). This catches fills that
   landed while the EA was disconnected or a push that raced order
   creation. Broker-side cancels/expiries/rejects are mirrored too.

## Known limitations (backend-side, not EA bugs)

- The bridge registry is in-process: run the backend API as a single worker
  (the dev setup does), or MT5 orders can land on a worker that doesn't
  hold the EA's connection.
