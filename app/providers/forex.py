import logging
import aiohttp
import asyncio
from datetime import datetime, timezone
from app.core.config import settings
from app.providers.base import BaseProvider

log = logging.getLogger(__name__)

# ── OANDA endpoints ───────────────────────────────────────────────────────────
# Pricing/streaming lives on stream-* hosts, NOT api-* hosts.
# api-* is for order management only — using it for pricing returns 403.
_STREAM_PRACTICE = "https://stream-fxpractice.oanda.com/v3"
_STREAM_LIVE     = "https://stream-fxtrade.oanda.com/v3"

# REST snapshot (api-* host) — used only when stream is unavailable
_REST_PRACTICE   = "https://api-fxpractice.oanda.com/v3"
_REST_LIVE       = "https://api-fxtrade.oanda.com/v3"

_STREAM_PATH     = "/accounts/{account_id}/pricing/stream"  # chunked stream
_SNAPSHOT_PATH   = "/accounts/{account_id}/pricing"         # one-shot REST

_OANDA_BATCH_SIZE = 20


def _oanda_instrument(symbol: str) -> str:
    """DB symbols come in both conventions (EUR/USD slashed, XAUUSD
    slash-less — setup.md §4) but OANDA instrument codes always need the
    underscore (EUR_USD). Slash-less input previously passed through
    unchanged (e.g. "XAUUSD"), which isn't a valid OANDA instrument --
    OANDA 400s the *entire* batched stream request over one bad code in
    the comma-joined instruments list, so every forex/metal symbol silently
    got no live ticks, not just the malformed one."""
    if "/" in symbol:
        return symbol.replace("/", "_")
    return f"{symbol[:3]}_{symbol[3:]}" if len(symbol) >= 6 else symbol


def _chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i: i + size]


class ForexProvider(BaseProvider):
    def __init__(self, pairs, symbol_map):
        super().__init__(pairs)
        self.symbol_map = symbol_map

    async def start(self, publish):
        use_oanda = bool(settings.OANDA_API_KEY and settings.OANDA_ACCOUNT_ID)

        if not use_oanda:
            log.warning("OANDA credentials not set — falling back to public forex rates")
            await self._run_public_fallback(publish)
            return

        live = settings.OANDA_ENVIRONMENT == "live"
        stream_base   = _STREAM_LIVE   if live else _STREAM_PRACTICE
        rest_base     = _REST_LIVE     if live else _REST_PRACTICE
        account_id    = settings.OANDA_ACCOUNT_ID

        stream_url    = stream_base + _STREAM_PATH.format(account_id=account_id)
        snapshot_url  = rest_base   + _SNAPSHOT_PATH.format(account_id=account_id)

        headers = {
            "Authorization":         f"Bearer {settings.OANDA_API_KEY}",
            "Accept-Datetime-Format": "RFC3339",
        }

        try:
            await self._run_stream(stream_url, headers, publish)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"ForexProvider stream failed: {e} — falling back to REST snapshot poll")
            await self._run_snapshot_poll(snapshot_url, headers, publish)

    # ─────────────────────────────────────────────────────────────────────────
    # Path 1 — Chunked streaming  (stream-fxpractice / stream-fxtrade)
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_stream(self, stream_url, headers, publish):
        """
        Connects to the OANDA pricing stream and reads chunked JSON lines.
        Each line is either a PRICE message or a HEARTBEAT.
        Reconnects with exponential backoff on any network error.
        """
        instruments = [_oanda_instrument(p) for p in self.symbols]
        backoff = 3

        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    for batch in _chunked(instruments, _OANDA_BATCH_SIZE):
                        params = {"instruments": ",".join(batch)}
                        async with session.get(
                            stream_url,
                            headers = headers,
                            params  = params,
                            timeout = aiohttp.ClientTimeout(
                                connect   = 15,
                                sock_read = 0,   # 0 = no read timeout (stream is indefinite)
                            ),
                        ) as resp:
                            if resp.status == 401:
                                log.error("OANDA stream 401 — invalid API token. Not retrying.")
                                return
                            if resp.status == 403:
                                log.error(
                                    f"OANDA stream 403 — check that:\n"
                                    f"  • Token belongs to a {settings.OANDA_ENVIRONMENT} account\n"
                                    f"  • Account ID {settings.OANDA_ACCOUNT_ID} matches that token\n"
                                    f"  • URL is stream-fx{'trade' if settings.OANDA_ENVIRONMENT == 'live' else 'practice'}.oanda.com  ← (not api-fx*)"
                                )
                                return
                            if resp.status != 200:
                                log.error(f"OANDA stream HTTP {resp.status} — retrying in {backoff}s")
                                await asyncio.sleep(backoff)
                                backoff = min(backoff * 2, 60)
                                continue

                            backoff = 3  # reset on successful connect
                            log.info(
                                f"ForexProvider: streaming {len(batch)} instrument(s) "
                                f"from {stream_url}"
                            )

                            async for raw_line in resp.content:
                                line = raw_line.strip()
                                if not line:
                                    continue
                                try:
                                    import json
                                    msg = json.loads(line)
                                except Exception:
                                    continue

                                if msg.get("type") == "HEARTBEAT":
                                    continue  # keepalive — no action needed

                                if msg.get("type") != "PRICE":
                                    continue

                                await self._publish_price(msg, publish, source="oanda_stream")

            except asyncio.CancelledError:
                log.info("ForexProvider stream: cancelled — shutting down")
                raise

            except aiohttp.ClientConnectorError as e:
                log.error(
                    f"ForexProvider stream connector error: {e} — "
                    f"retrying in {backoff}s"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

            except Exception as e:
                log.warning(f"ForexProvider stream error: {e} — retrying in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    # ─────────────────────────────────────────────────────────────────────────
    # Path 2 — REST snapshot poll  (api-fxpractice / api-fxtrade)
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_snapshot_poll(self, snapshot_url, headers, publish):
        """
        Falls back to polling GET /pricing (one-shot per interval).
        Higher latency than streaming but works if stream host is unreachable.
        """
        instruments = [_oanda_instrument(p) for p in self.symbols]
        backoff = 3

        log.warning(
            f"ForexProvider: using REST snapshot poll every "
            f"{settings.OANDA_POLL_INTERVAL_SECS}s — latency higher than stream"
        )

        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    for batch in _chunked(instruments, _OANDA_BATCH_SIZE):
                        params = {"instruments": ",".join(batch)}
                        async with session.get(
                            snapshot_url,
                            headers = headers,
                            params  = params,
                            timeout = aiohttp.ClientTimeout(total=15),
                        ) as resp:
                            if resp.status == 401:
                                log.error("OANDA snapshot 401 — invalid API token. Not retrying.")
                                return
                            if resp.status == 403:
                                log.error(
                                    f"OANDA snapshot 403 — token/account mismatch or wrong environment. "
                                    f"Not retrying."
                                )
                                return
                            if resp.status == 429:
                                log.warning(f"OANDA snapshot 429 rate limited — backing off {backoff}s")
                                await asyncio.sleep(backoff)
                                backoff = min(backoff * 2, 60)
                                continue

                            resp.raise_for_status()
                            data = await resp.json()
                            backoff = 3

                        for price in data.get("prices", []):
                            await self._publish_price(price, publish, source="oanda_rest")

                except asyncio.CancelledError:
                    log.info("ForexProvider snapshot poll: cancelled — shutting down")
                    raise

                except aiohttp.ClientError as e:
                    log.warning(f"ForexProvider snapshot error: {e} — retrying in {backoff}s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue

                except Exception as e:
                    log.error(f"ForexProvider snapshot unexpected error: {e} — retrying in {backoff}s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue

                await asyncio.sleep(settings.OANDA_POLL_INTERVAL_SECS)

    # ─────────────────────────────────────────────────────────────────────────
    # Path 3 — Public fallback  (no OANDA credentials)
    # ─────────────────────────────────────────────────────────────────────────

    async def _run_public_fallback(self, publish):
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    await self._publish_public_rates(session, publish)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.error(f"ForexProvider public fallback error: {e}")
                await asyncio.sleep(settings.OANDA_POLL_INTERVAL_SECS)

    # ─────────────────────────────────────────────────────────────────────────
    # Shared helpers
    # ─────────────────────────────────────────────────────────────────────────

    async def _publish_price(self, price: dict, publish, source: str):
        instrument = price.get("instrument")
        if not instrument:
            return

        # Mirror image of _oanda_instrument's bug: this unconditionally
        # inserted a slash (EUR_USD -> EUR/USD), but self.symbol_map is
        # keyed by the DB's own symbol string -- for slash-less rows
        # (XAUUSD) that lookup always missed and every price was silently
        # dropped (not an error, just a `return`, which is why this went
        # unnoticed). Try both conventions.
        slashed   = instrument.replace("_", "/")
        unslashed = instrument.replace("_", "")
        symbol    = slashed if slashed in self.symbol_map else unslashed
        symbol_id = self.symbol_map.get(symbol)
        if not symbol_id:
            return

        bids = price.get("bids") or []
        asks = price.get("asks") or []
        if not bids or not asks:
            return

        bid = float(bids[0].get("price", 0))
        ask = float(asks[0].get("price", 0))
        if bid <= 0 or ask <= 0:
            return

        await publish({
            "symbol_id":   symbol_id,
            "symbol":      symbol,
            "price":       round((bid + ask) / 2, 6),
            "bid":         bid,
            "ask":         ask,
            "volume":      0,
            "asset_class": "forex",
            "source":      source,
            "timestamp":   price.get("time", datetime.now(timezone.utc).isoformat()),
        })

    async def _publish_public_rates(self, session, publish):
        url = "https://open.er-api.com/v6/latest/"
        for base in ["USD", "EUR", "GBP", "JPY"]:
            async with session.get(url + base, timeout=aiohttp.ClientTimeout(total=15)) as r:
                data  = await r.json()
                rates = data.get("rates", {})

                for pair in self.symbols:
                    if not pair.startswith(base):
                        continue
                    quote     = pair.split("/")[1]
                    symbol_id = self.symbol_map.get(pair)
                    if quote not in rates or not symbol_id:
                        continue

                    await publish({
                        "symbol_id":   symbol_id,
                        "symbol":      pair,
                        "price":       float(rates[quote]),
                        "volume":      0,
                        "asset_class": "forex",
                        "source":      "public_forex",
                        "timestamp":   datetime.now(timezone.utc).isoformat(),
                    })