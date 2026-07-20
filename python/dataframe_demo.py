#!/usr/bin/env python3
"""Pandas and polars ingestion + egress round-trip over one QuestDB 5.0 handle.

Showcases the columnar/dataframe capabilities the row-by-row sender does not, all
through a single ``questdb.connect()`` handle:

  * DDL via ``db.execute(sql)`` - run-and-drain, no HTTP ``/exec`` side channel.
  * pandas ingestion via ``db.dataframe`` - numpy-backed pandas is accepted directly
    (SYMBOL columns as pandas ``Categorical``); in 4.x the columnar ``Client.dataframe``
    rejected numpy pandas outright and it had to go through ``Sender.dataframe``.
  * polars ingestion via ``db.dataframe`` - the same call takes polars / pyarrow /
    any Arrow C Stream source natively.
  * egress via ``db.query(...).to_pandas()`` and ``.to_polars()``.

One handle for DDL, ingest and query - the 5.0 shape. (The standalone ``Sender``
and its ``Sender.dataframe`` still exist for point-to-point ILP/HTTP, ILP/TCP and
QWP/UDP needs; over ``ws``/``wss`` they route to the same direct columnar path.)

Usage:
    python dataframe_demo.py [--addr localhost:9000] [--csv ../trades20250728.csv.gz] [--rows 5000]

Requires the questdb 5.0 client (see README.md).
"""

import argparse
import sys
import time

import pandas as pd
import polars as pl

import questdb

TABLE = "df_demo"


def wait_for_count(db, table, at_least, timeout_s=30):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            n = int(db.query(f"select count() from {table}").to_pandas().iloc[0, 0])
            if n >= at_least:
                return n
        except Exception:
            pass
        time.sleep(0.5)
    return -1


def load_pandas(csv_path, rows):
    """Load the CSV into a numpy-backed pandas DataFrame the columnar handle ingests
    directly: float64 values, tz-aware nanosecond timestamps, and the SYMBOL columns
    as pandas ``Categorical`` (what ``db.dataframe``'s columnar v1 path wants for a
    SYMBOL - plain ``object`` strings land as VARCHAR instead)."""
    raw = pd.read_csv(csv_path, nrows=rows)
    return pd.DataFrame({
        "symbol": raw["symbol"].astype("category"),
        "side": raw["side"].astype("category"),
        "price": raw["price"].astype("float64"),
        "amount": raw["amount"].astype("float64"),
        "timestamp": pd.to_datetime(raw["timestamp"], utc=True).dt.as_unit("ns"),
    })


def main(argv):
    ap = argparse.ArgumentParser(description="Pandas/polars ingestion + egress demo")
    ap.add_argument("--addr", default="localhost:9000")
    ap.add_argument("--csv", default="../trades20250728.csv.gz")
    ap.add_argument("--rows", type=int, default=5000)
    args = ap.parse_args(argv)

    pdf = load_pandas(args.csv, args.rows)
    print(f"loaded pandas DataFrame: {pdf.shape[0]} rows\n{pdf.dtypes}\n")
    pldf = pl.from_pandas(pdf)
    print(f"built polars DataFrame: {pldf.height} rows\n")

    with questdb.connect(f"ws::addr={args.addr};") as db:
        db.execute(f"drop table if exists {TABLE}")   # DDL, run-and-drain

        # --- INGESTION (both frames through the one handle) ---
        # numpy-backed pandas - accepted directly in 5.0 (was rejected in 4.x).
        db.dataframe(pdf, table_name=TABLE, symbols=["symbol", "side"], at="timestamp")
        print(f"[ingest] pandas via db.dataframe: {pdf.shape[0]} rows")

        # polars - the same call, Arrow-columnar and native.
        db.dataframe(pldf, table_name=TABLE, symbols=["symbol", "side"], at="timestamp")
        print(f"[ingest] polars via db.dataframe: {pldf.height} rows")

        expected = pdf.shape[0] + pldf.height
        n = wait_for_count(db, TABLE, expected)
        print(f"[ingest] server applied {n} rows (expected {expected})\n")

        # --- EGRESS ---
        pd_out = db.query(
            f"select symbol, count() n, round(avg(price), 2) avg_price "
            f"from {TABLE} order by n desc limit 5"
        ).to_pandas()
        print("[egress] db.query(...).to_pandas():")
        print(pd_out.to_string(index=False))
        print()

        pl_out = db.query(
            f"select side, count() n, round(sum(amount), 4) total_amount "
            f"from {TABLE} group by side order by side"
        ).to_polars()
        print("[egress] db.query(...).to_polars():")
        print(pl_out)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
