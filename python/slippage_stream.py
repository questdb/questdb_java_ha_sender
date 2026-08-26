#!/usr/bin/env python3
"""Slippage per fill, printed batch by batch as the result streams back.

Same query as slippage.py, but iter_polars yields one polars DataFrame per QWP
batch instead of buffering the whole result, so rows appear as they arrive
rather than after the last one lands.

    ILP_TOKEN=... python slippage_stream.py
    QDB_ADDR=host:9000 ILP_TOKEN=... python slippage_stream.py

Needs the questdb 5.0 client plus polars and pyarrow.
"""
import os

import questdb

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
WHERE t.timestamp IN '$yesterday'
ORDER BY t.timestamp
"""

CONF = os.environ.get("QDB_CONF") or (
    f"wss::addr={os.environ.get('QDB_ADDR', '172.31.42.41:9000')};"
    f"token={os.environ['ILP_TOKEN']};tls_verify=unsafe_off;")

rows = 0
with questdb.connect(CONF) as db:
    for df in db.query(SQL).iter_polars():
        if df.height == 0:
            continue      # schema-only batch carries no rows; nothing to show yet
        rows += df.height
        print(f"--- batch of {df.height:,} rows | {rows:,} so far ---", flush=True)
        print(df, flush=True)

print(f"\n{rows:,} rows total")

# Want one frame at the end instead? Batches share a Categories identity, so:
#     import polars as pl
#     df = pl.concat(db.query(SQL).iter_polars())
# though that buffers everything, which is what slippage.py already does via
# to_polars().
