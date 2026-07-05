"""
In-memory OHLCV candle builder.

One CandleAggregator instance per process (singleton `aggregator`).
Both the Redis consumer and Kafka consumer feed ticks into it.

Design decisions for small EC2:
  - Pure in-memory: no DB reads during aggregation
  - Completed buckets are returned to the caller for batched DB writes
  - Memory: 50 symbols × ~5 open buckets × ~200 bytes = negligible
"""
from __future__ import annotations

from datetime import datetime, timezone

from app.core.config import settings


def _bucket_ts(ts: datetime) -> datetime:
    """Floor a timestamp to the nearest candle interval boundary."""
    interval = settings.CANDLE_INTERVAL_SECONDS
    floored  = (ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts)
    second   = (floored.second // interval) * interval
    return floored.replace(second=second, microsecond=0)


class CandleAggregator:

    def __init__(self):
        # key: (symbol_id, bucket_datetime)  →  value: candle dict
        self._buckets: dict[tuple[int, datetime], dict] = {}

    def ingest(self, tick: dict) -> list[dict]:
        """
        Feed one validated tick.
        Returns a list of completed candle dicts (may be empty).
        Completed = bucket timestamp is strictly older than current tick's bucket.
        """
        sym_id  = int(tick["symbol_id"])
        price   = float(tick["price"])
        volume  = float(tick.get("volume", 0))
        ts      = _parse_time(tick.get("time"))
        bucket  = _bucket_ts(ts)
        key     = (sym_id, bucket)

        if key not in self._buckets:
            self._buckets[key] = {
                "symbol_id":  sym_id,
                "time":       bucket,
                "open":       price,
                "high":       price,
                "low":        price,
                "close":      price,
                "volume":     volume,
                "tick_count": 1,
                "has_dq":     bool(tick.get("flags")),
            }
        else:
            c = self._buckets[key]
            c["high"]        = max(c["high"], price)
            c["low"]         = min(c["low"],  price)
            c["close"]       = price
            c["volume"]     += volume
            c["tick_count"] += 1
            c["has_dq"]      = c["has_dq"] or bool(tick.get("flags"))

        # Flush any bucket for this symbol that is older than current bucket
        completed = [
            self._buckets.pop(k)
            for k in list(self._buckets)
            if k[0] == sym_id and k[1] < bucket
        ]
        return completed

    def flush_all(self) -> list[dict]:
        """Force-flush all open buckets (call on shutdown)."""
        candles = list(self._buckets.values())
        self._buckets.clear()
        return candles


# Module-level singleton
aggregator = CandleAggregator()


def _parse_time(ts) -> datetime:
    if not ts:
        return datetime.now(timezone.utc)
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(ts))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)