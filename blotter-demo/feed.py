#!/usr/bin/env python3
"""Continuous synthetic price feed for the blotter demo.

Emits a steady trickle of bid prices for a small basket of instruments into
``core_price_demo``, flushing ~20x/second so the live view (and the blotter on
top of it) visibly moves. Prices random-walk around illustrative 2026 mid
levels; they are demo values, NOT a live market feed.

Config via env:
  QDB_ADDR            host:port of the QWP endpoint (default questdb:9000)
  QDB_CONF            full conf string (overrides QDB_ADDR if set)
  QDB_TABLE           base table name (default core_price_demo)
  FEED_FLUSH_HZ       flushes per second (default 20)
  FEED_ROWS_PER_FLUSH rows appended per flush (default 100)
"""
import os
import random
import time

from questdb import Sender, TimestampNanos

# Illustrative mid prices (not live market data). Crypto + major FX.
BOOK = {
    "BTC-USD": 95_000.0,
    "ETH-USD": 3_400.0,
    "SOL-USD": 180.0,
    "XRP-USD": 2.30,
    "DOGE-USD": 0.38,
    "EUR-USD": 1.0900,
    "GBP-USD": 1.2700,
    "USD-JPY": 150.00,
    "AUD-USD": 0.6600,
    "USD-CAD": 1.3600,
}

CONF = os.environ.get(
    "QDB_CONF", "ws::addr=" + os.environ.get("QDB_ADDR", "questdb:9000") + ";"
)
TABLE = os.environ.get("QDB_TABLE", "core_price_demo")
FLUSHES_PER_SEC = float(os.environ.get("FEED_FLUSH_HZ", "20"))
ROWS_PER_FLUSH = int(os.environ.get("FEED_ROWS_PER_FLUSH", "100"))


def jitter(px):
    # ~2 bps gaussian step per tick, floored so a long walk can't go negative.
    return max(px * 0.5, px + px * random.gauss(0.0, 0.0002))


def main():
    prices = dict(BOOK)
    symbols = list(BOOK)
    interval = 1.0 / FLUSHES_PER_SEC
    sent = 0
    window_start = time.time()
    print(f"[feed] {CONF} -> {TABLE} | {FLUSHES_PER_SEC:g} flush/s x "
          f"{ROWS_PER_FLUSH} rows = ~{FLUSHES_PER_SEC * ROWS_PER_FLUSH:.0f} rows/s",
          flush=True)
    with Sender.from_conf(CONF) as s:
        while True:
            tick = time.perf_counter()
            for _ in range(ROWS_PER_FLUSH):
                sym = random.choice(symbols)
                prices[sym] = jitter(prices[sym])
                s.row(
                    TABLE,
                    symbols={"symbol": sym},
                    columns={"bid_price": round(prices[sym], 6)},
                    at=TimestampNanos.now(),
                )
                sent += 1
            s.flush()
            now = time.time()
            if now - window_start >= 5.0:
                print(f"[feed] ~{sent / (now - window_start):.0f} rows/s", flush=True)
                sent = 0
                window_start = now
            slack = interval - (time.perf_counter() - tick)
            if slack > 0:
                time.sleep(slack)


if __name__ == "__main__":
    main()
