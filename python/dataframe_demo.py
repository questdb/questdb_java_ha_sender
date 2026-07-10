#!/usr/bin/env python3
"""Pandas and polars ingestion + egress round-trip over QWP/WebSocket.

Showcases the columnar/dataframe capabilities the row-by-row sender does not:

  * pandas ingestion via ``Sender.dataframe`` (the numpy-backed pandas planner).
  * polars ingestion via ``Client.dataframe`` (the pooled QWP Arrow-columnar path;
    it takes polars / pyarrow / any Arrow C Stream source natively — numpy-backed
    pandas is not accepted there, so pandas goes through ``Sender.dataframe``).
  * egress via ``Client.query(...).to_pandas()`` and ``.to_polars()``.

Usage:
    python dataframe_demo.py [--addr localhost:9000] [--csv ../trades20250728.csv.gz] [--rows 5000]

Requires the QWP/egress build of the questdb client (see README.md).
"""

import argparse
import sys
import time
import urllib.parse
import urllib.request

import pandas as pd
import polars as pl

from questdb.ingress import Sender, Client

TABLE = "df_demo"


def exec_sql(addr, sql):
    """Run a statement over the HTTP /exec endpoint (same host, HTTP port)."""
    url = f"http://{addr}/exec?" + urllib.parse.urlencode({"query": sql})
    with urllib.request.urlopen(url, timeout=10) as resp:
        resp.read()


def wait_for_count(client, table, at_least, timeout_s=30):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            n = int(client.query(f"select count() from {table}").to_pandas().iloc[0, 0])
            if n >= at_least:
                return n
        except Exception:
            pass
        time.sleep(0.5)
    return -1


def load_pandas(csv_path, rows):
    """Load the CSV into a pandas DataFrame with dtypes the ingestion path accepts
    (object strings, float64, tz-aware nanosecond timestamps)."""
    raw = pd.read_csv(csv_path, nrows=rows)
    return pd.DataFrame({
        "symbol": raw["symbol"].astype("object"),
        "side": raw["side"].astype("object"),
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

    exec_sql(args.addr, f"drop table if exists {TABLE}")

    # --- INGESTION ---
    # pandas -> Sender.dataframe (numpy-backed pandas planner) over QWP/WebSocket.
    with Sender.from_conf(f"qwpws::addr={args.addr};auto_flush=off;") as sender:
        sender.dataframe(pdf, table_name=TABLE, symbols=["symbol", "side"], at="timestamp")
        sender.flush()
    print(f"[ingest] pandas via Sender.dataframe: {pdf.shape[0]} rows")

    with Client.from_conf(f"qwpws::addr={args.addr};") as client:
        # polars -> Client.dataframe (Arrow-columnar path, polars is native).
        client.dataframe(pldf, table_name=TABLE, symbols=["symbol", "side"], at="timestamp")
        print(f"[ingest] polars via Client.dataframe: {pldf.height} rows")

        expected = pdf.shape[0] + pldf.height
        n = wait_for_count(client, TABLE, expected)
        print(f"[ingest] server applied {n} rows (expected {expected})\n")

        # --- EGRESS ---
        pd_out = client.query(
            f"select symbol, count() n, round(avg(price), 2) avg_price "
            f"from {TABLE} order by n desc limit 5"
        ).to_pandas()
        print("[egress] Client.query(...).to_pandas():")
        print(pd_out.to_string(index=False))
        print()

        pl_out = client.query(
            f"select side, count() n, round(sum(amount), 4) total_amount "
            f"from {TABLE} group by side order by side"
        ).to_polars()
        print("[egress] Client.query(...).to_polars():")
        print(pl_out)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
