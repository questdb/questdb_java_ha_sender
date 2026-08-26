#!/usr/bin/env python3
"""Slippage per fill: fetched over N connections in parallel, printed in order as
it streams.

Combines the two earlier variants. slippage_stream.py streams but on one
connection; slippage_parallel.py uses N connections but buffers everything into a
single DataFrame before you see anything. This does both: N connections pull
concurrently while rows print in true timestamp order, starting immediately.

The ordering trick is that each worker owns a *bounded* queue. Workers all fetch
at once, but the printer drains range 0 to completion, then range 1, and so on.
Because the ranges are contiguous and consumed in order, what you see is exactly
the ordering a single query would have produced. The bound is what keeps this from
being a disguised "buffer everything": a worker that runs ahead blocks once its
queue is full, so peak memory is at most readers x QUEUE_DEPTH batches rather than
the whole result. That backpressure reaches the server through QWP's flow control.

    ILP_TOKEN=... python slippage_parallel_stream.py [readers]
    QDB_CONF="ws::addr=localhost:9000;" python slippage_parallel_stream.py 8

Needs the questdb 5.0 client plus polars and pyarrow.
"""
import os
import queue
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import polars as pl
import questdb

READERS = int(sys.argv[1]) if len(sys.argv) > 1 else 8
QUEUE_DEPTH = 4          # batches a worker may run ahead before it blocks
DONE = object()          # sentinel: this range produced its last batch

SQL = """
SELECT
    t.timestamp,
    t.symbol,
    t.ecn,
    t.counterparty,
    t.side,
    t.passive,
    t.price,
    t.quantity,
    m.best_bid,
    m.best_ask,
    (m.best_bid + m.best_ask) / 2 AS mid,
    (m.best_ask - m.best_bid) AS spread,
    CASE t.side
        WHEN 'buy'  THEN (t.price - (m.best_bid + m.best_ask) / 2)
                         / ((m.best_bid + m.best_ask) / 2) * 10000
        WHEN 'sell' THEN ((m.best_bid + m.best_ask) / 2 - t.price)
                         / ((m.best_bid + m.best_ask) / 2) * 10000
    END AS slippage_bps,
    CASE t.side
        WHEN 'buy'  THEN (t.price - m.best_ask) / m.best_ask * 10000
        WHEN 'sell' THEN (m.best_bid - t.price) / m.best_bid * 10000
    END AS slippage_vs_tob_bps
FROM fx_trades t
ASOF JOIN market_data m ON (symbol)
WHERE t.timestamp >= '{lo}' AND t.timestamp < '{hi}'
"""

CONF = os.environ.get("QDB_CONF") or (
    f"wss::addr={os.environ.get('QDB_ADDR', '172.31.42.41:9000')};"
    f"token={os.environ['ILP_TOKEN']};tls_verify=unsafe_off;")


def ranges(n):
    """Yesterday UTC split into n contiguous half-open [lo, hi) spans.

    The last span ends at today's midnight, so the spans tile yesterday exactly:
    no fill is counted twice and none is dropped. Same window as `IN '$yesterday'`.
    """
    today = datetime.now(timezone.utc).date()
    start = datetime(today.year, today.month, today.day, tzinfo=timezone.utc) - timedelta(days=1)
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    edges = [start + timedelta(days=1) * i / n for i in range(n + 1)]
    return [(edges[i].strftime(fmt), edges[i + 1].strftime(fmt)) for i in range(n)]


def worker(i, lo, hi, q, errors):
    """Stream one range into its bounded queue. q.put blocks when full, which is
    the backpressure that keeps peak memory bounded."""
    try:
        with questdb.connect(CONF) as db:
            for df in db.query(SQL.format(lo=lo, hi=hi)).iter_polars():
                if df.height:      # a result can carry a schema-only batch of 0 rows
                    q.put(df)
    except Exception as e:  # noqa: BLE001
        errors.append(f"reader {i} [{lo}, {hi}): {e}")
    finally:
        q.put(DONE)


def main():
    spans = ranges(READERS)
    queues = [queue.Queue(maxsize=QUEUE_DEPTH) for _ in spans]
    errors = []

    t0 = time.monotonic()
    for i, (lo, hi) in enumerate(spans):
        threading.Thread(target=worker, args=(i, lo, hi, queues[i], errors),
                         daemon=True).start()

    rows = 0
    first = None
    for i, q in enumerate(queues):
        while True:
            df = q.get()
            if df is DONE:
                break
            if first is None:
                first = time.monotonic() - t0
            rows += df.height
            print(f"--- range {i} | batch of {df.height:,} rows | {rows:,} so far ---",
                  flush=True)
            print(df, flush=True)

    elapsed = time.monotonic() - t0
    if errors:
        for e in errors:
            print(e, file=sys.stderr)
        return 1

    print(f"\n{rows:,} rows over {READERS} connection(s) in {elapsed:.2f}s "
          f"({rows / elapsed:,.0f} rows/s); first rows after {first * 1000:.0f} ms")
    return 0


if __name__ == "__main__":
    sys.exit(main())
