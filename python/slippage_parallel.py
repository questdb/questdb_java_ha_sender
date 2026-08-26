#!/usr/bin/env python3
"""Slippage per fill, pulled over N connections and merged into one DataFrame.

Same query as slippage.py. The ASOF JOIN is per-row work done while the result
streams, so the cost scales with rows retrieved, and a single connection is also
capped by the client's socket buffer at ~426 KB per RTT. Splitting yesterday into
N equal time ranges and giving each its own connection lifts both limits, the same
way read_bench --readers does.

Ranges are contiguous and consumed in order, so concatenating them yields exactly
the timestamp ordering a single query would have produced. No preliminary query is
needed: the day's bounds are known, so the split is pure arithmetic.

    ILP_TOKEN=... python slippage_parallel.py [readers]
    QDB_CONF="ws::addr=localhost:9000;" python slippage_parallel.py 8

Needs the questdb 5.0 client plus polars and pyarrow.
"""
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import polars as pl
import questdb

# SYMBOL columns arrive as polars Categorical, and each connection's result carries
# its own Categories identity (a per-result UUID in the questdb_symbol namespace).
# pl.concat refuses to mix them: "Categories name mismatch ... failed to vstack
# column 'symbol'". concat(how="vertical_relaxed") does not rescue it either, it
# fails with "failed to determine supertype of cat and cat". Casting every frame's
# categorical columns to one shared named Categories first is what makes the merge
# work, and it is cheaper than casting them to String (188 ms vs 216 ms over 9M
# rows) while keeping the dtype.
CATEGORIES = pl.Categories("slippage")

READERS = int(sys.argv[1]) if len(sys.argv) > 1 else 8

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

    Half-open with the last span ending at today's midnight, so the spans tile
    yesterday exactly: no fill is counted twice and none is dropped. This is the
    same window `WHERE t.timestamp IN '$yesterday'` selects.
    """
    today = datetime.now(timezone.utc).date()
    start = datetime(today.year, today.month, today.day, tzinfo=timezone.utc) - timedelta(days=1)
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    edges = [start + timedelta(days=1) * i / n for i in range(n + 1)]
    return [(edges[i].strftime(fmt), edges[i + 1].strftime(fmt)) for i in range(n)]


def main():
    spans = ranges(READERS)
    frames = [None] * READERS      # indexed, so order survives out-of-order completion
    errors = []

    def worker(i, lo, hi):
        try:
            with questdb.connect(CONF) as db:
                frames[i] = db.query(SQL.format(lo=lo, hi=hi)).to_polars()
        except Exception as e:  # noqa: BLE001
            errors.append(f"reader {i} [{lo}, {hi}): {e}")

    t0 = time.monotonic()
    threads = [threading.Thread(target=worker, args=(i, lo, hi))
               for i, (lo, hi) in enumerate(spans)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    fetched = time.monotonic() - t0

    if errors:
        for e in errors:
            print(e, file=sys.stderr)
        return 1

    # Concatenate in range order, which is timestamp order. Harmonise the
    # per-connection Categories identities first, or the concat raises SchemaError.
    got = [f for f in frames if f is not None]
    cat_cols = [c for c, dt in got[0].schema.items() if dt.base_type() == pl.Categorical]
    df = pl.concat(
        [f.with_columns(pl.col(c).cast(pl.Categorical(CATEGORIES)) for c in cat_cols)
         for f in got]
    )
    elapsed = time.monotonic() - t0

    print(f"{df.height:,} rows over {READERS} connection(s) in {elapsed:.2f}s "
          f"({df.height / elapsed:,.0f} rows/s; fetch {fetched:.2f}s, "
          f"concat {elapsed - fetched:.2f}s)")
    print(df)
    return 0


if __name__ == "__main__":
    sys.exit(main())
