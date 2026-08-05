#!/usr/bin/env python3
"""Continuous synthetic price feed for the blotter demo.

Emits bid prices for a small basket of instruments into ``core_price_demo`` at a
configurable target rate, flushing several times a second so the live view (and
the blotter on top of it) visibly moves. Prices random-walk around illustrative
2026 mid levels; they are demo values, NOT a live market feed.

Throughput path: each flush builds one vectorized Polars DataFrame (numpy under
the hood) and sends it with a single ``Sender.dataframe(...)`` call. Over
QWP/WebSocket that is a direct columnar bulk load, so one process sustains
~1M rows/s on a laptop — the row-by-row API can't. To go beyond one process,
set FEED_WORKERS.

Config via env:
  QDB_ADDR            host:port of the QWP endpoint (default questdb:9000)
  QDB_CONF            full conf string (overrides QDB_ADDR if set)
  QDB_TABLE           base table name (default core_price_demo)
  FEED_TARGET_RPS     target rows/second, total across workers (default 2000)
  FEED_FLUSH_HZ       flushes per second per worker (default 20); batch size is
                      target_rps / workers / flush_hz
  FEED_WORKERS        parallel sender processes (default 1); each targets an
                      equal share of FEED_TARGET_RPS. Only needed to push well
                      past what one process can generate.
"""
import os
import time

import numpy as np
import polars as pl

from questdb import Sender

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
TARGET_RPS = float(os.environ.get("FEED_TARGET_RPS", "2000"))
FLUSHES_PER_SEC = float(os.environ.get("FEED_FLUSH_HZ", "20"))
WORKERS = max(1, int(os.environ.get("FEED_WORKERS", "1")))


def worker(worker_id, rows_per_flush):
    """One sender process: generate and ship `rows_per_flush` rows every 1/FLUSH_HZ s."""
    # Distinct stream per worker so the walks don't move in lockstep. (Runtime
    # process, so a seeded default_rng is fine and keeps each worker independent.)
    rng = np.random.default_rng(1000 + worker_id)
    symbols = np.array(list(BOOK))
    mids = np.array(list(BOOK.values()), dtype=np.float64)
    floors = mids * 0.5  # a long walk can't drift below half the start level
    n_sym = len(symbols)
    interval = 1.0 / FLUSHES_PER_SEC

    sent = 0
    window_start = time.time()
    with Sender.from_conf(CONF) as s:
        while True:
            tick = time.perf_counter()

            # Drift each symbol's mid once per batch (~2 bps), then draw this
            # batch's rows around those mids with a little intra-batch noise.
            mids = np.maximum(floors, mids * (1.0 + rng.normal(0.0, 0.0002, n_sym)))
            idx = rng.integers(0, n_sym, rows_per_flush)
            px = mids[idx] * (1.0 + rng.normal(0.0, 0.0001, rows_per_flush))

            now_ns = int(time.time() * 1e9)
            ts = np.datetime64(now_ns, "ns") + np.arange(rows_per_flush).astype("timedelta64[ns]")

            df = pl.DataFrame({
                "timestamp": ts,
                "symbol": symbols[idx],
                "bid_price": np.round(px, 6),
            })
            s.dataframe(df, table_name=TABLE, symbols=["symbol"], at="timestamp")
            s.flush()
            sent += rows_per_flush

            now = time.time()
            if now - window_start >= 5.0:
                print(f"[feed w{worker_id}] ~{sent / (now - window_start):.0f} rows/s",
                      flush=True)
                sent = 0
                window_start = now

            slack = interval - (time.perf_counter() - tick)
            if slack > 0:
                time.sleep(slack)


def main():
    per_worker_rps = TARGET_RPS / WORKERS
    rows_per_flush = max(1, round(per_worker_rps / FLUSHES_PER_SEC))
    effective_rps = rows_per_flush * FLUSHES_PER_SEC * WORKERS
    print(f"[feed] {CONF} -> {TABLE} | target ~{TARGET_RPS:.0f} rows/s | "
          f"{WORKERS} worker(s) x {FLUSHES_PER_SEC:g} flush/s x {rows_per_flush} rows "
          f"= ~{effective_rps:.0f} rows/s", flush=True)

    if WORKERS == 1:
        worker(0, rows_per_flush)
        return

    import multiprocessing as mp
    procs = [mp.Process(target=worker, args=(i, rows_per_flush), daemon=True)
             for i in range(WORKERS)]
    for p in procs:
        p.start()
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    main()
