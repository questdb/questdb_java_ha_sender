#!/usr/bin/env python3
"""Slippage per fill, vs mid and vs top-of-book, as a polars DataFrame.

    ILP_TOKEN=... python slippage.py            # defaults to QDB_ADDR below
    QDB_ADDR=host:9000 ILP_TOKEN=... python slippage.py

Needs the questdb 5.0 client plus polars and pyarrow (to_polars needs both).
Swap to_polars for to_pandas if you want pandas instead.
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
"""
# No ORDER BY: fx_trades is scanned in designated-timestamp order, so rows already
# arrive oldest first. Measured with and without, the plan is identical (no sort).

CONF = (f"wss::addr={os.environ.get('QDB_ADDR', '172.31.42.41:9000')};"
        f"token={os.environ['ILP_TOKEN']};tls_verify=unsafe_off;")

with questdb.connect(CONF) as db:
    df = db.query(SQL).to_polars()

print(df)
