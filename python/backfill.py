#!/usr/bin/env python3
"""Backfill the trades table over a historical time window at a target row density.

Unlike the live senders (which stamp "now") or a file replay, this spreads rows evenly
across [--start, --end) at --rate-per-day rows/day and ingests them with those historical
timestamps, streamed in bounded chunks (flat memory) over the columnar QWP path. Trade
values (symbol/side/price/amount) are cycled from a CSV deck; trade_id is synthesised and
globally unique. Workers each own a contiguous slice of the window.

Default window: yesterday 00:00 UTC .. today 13:00 UTC (computed at run time, UTC).

Example (the default 500M rows/day over ~1.54 days is ~771M rows):
    python backfill.py --addr host:9000 --num-senders 4 \
        --token "$QDB_TOKEN" --tls-verify unsafe_off

Requires the QWP/egress build of the client (see README.md).
"""

import argparse
import gzip
import io
import sys
import threading
import time
from datetime import datetime, timezone, timedelta

import numpy as np
import polars as pl

from questdb.ingress import Client

NS_PER_DAY = 86_400_000_000_000


def use_tls(args):
    return bool(args.tls or args.token or (args.username and args.password))


def display_scheme(args):
    return "wss" if use_tls(args) else "ws"


def build_conf(args):
    # Python's binding requires the qwpws/qwpwss scheme (it rejects ws/wss); only the printed
    # [conf] line shows wss/ws, via display_scheme().
    scheme = "qwpwss" if use_tls(args) else "qwpws"
    parts = [f"{scheme}::addr={args.addr};"]
    if args.token:
        parts.append(f"token={args.token};")
    elif args.username and args.password:
        parts.append(f"username={args.username};password={args.password};")
    if use_tls(args) and args.tls_verify == "unsafe_off":
        parts.append("tls_verify=unsafe_off;")
    return "".join(parts)


def load_deck(path):
    """CSV -> polars deck: symbol/side as Categorical (-> SYMBOL), price/amount Float64."""
    if path.endswith(".gz"):
        with gzip.open(path, "rb") as fh:
            df = pl.read_csv(io.BytesIO(fh.read()))
    else:
        df = pl.read_csv(path)
    for col in ("symbol", "side", "price", "amount"):
        if col not in df.columns:
            raise ValueError(f"CSV missing required column: {col}")
    return df.select(
        pl.col("symbol").cast(pl.Utf8).cast(pl.Categorical),
        pl.col("side").cast(pl.Utf8).cast(pl.Categorical),
        pl.col("price").cast(pl.Float64),
        pl.col("amount").cast(pl.Float64),
    )


def parse_iso(s):
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def run_worker(wid, start_index, count, deck, args, start_ns, step_ns, counts):
    """Backfill rows [start_index, start_index+count): a contiguous time block. Timestamps come
    from the GLOBAL row index (start_ns + gidx*step_ns), so worker ranges never overlap."""
    m = deck.height
    conf = build_conf(args)
    sent = 0
    pos = start_index % m
    with Client.from_conf(conf) as client:
        while sent < count:
            cn = min(args.chunk_rows, count - sent, m - pos)
            sl = deck.slice(pos, cn)                       # zero-copy view into the deck
            gidx = start_index + sent
            ts = np.int64(start_ns) + np.arange(gidx, gidx + cn, dtype=np.int64) * np.int64(step_ns)
            chunk = sl.with_columns(
                pl.Series("timestamp", ts).cast(pl.Datetime("ns", "UTC")),
                (pl.lit("backfill-") + pl.int_range(gidx, gidx + cn).cast(pl.Utf8)).alias("trade_id"),
            )
            client.dataframe(chunk, table_name=args.table, symbols=["symbol", "side"], at="timestamp")
            sent += cn
            counts[wid] = sent
            pos = (pos + cn) % m
    print(f"Worker {wid} finished backfilling {sent:,} rows")


def preflight(conf, timeout_s):
    outcome = {}

    def probe():
        try:
            with Client.from_conf(conf) as c:
                c.query("select 1").to_polars()
            outcome["ok"] = True
        except Exception as e:  # noqa: BLE001
            outcome["err"] = str(e)

    t = threading.Thread(target=probe, daemon=True)
    t.start()
    t.join(timeout_s)
    if t.is_alive():
        return False, f"no response within {timeout_s:g}s (server down / unreachable?)"
    return (True, None) if outcome.get("ok") else (False, outcome.get("err", "unknown error"))


def main(argv):
    ap = argparse.ArgumentParser(description="Backfill trades over a historical window")
    ap.add_argument("--addr", default="localhost:9000")
    ap.add_argument("--start", default=None, help="ISO start (default: yesterday 00:00 UTC)")
    ap.add_argument("--end", default=None, help="ISO end, exclusive (default: today 13:00 UTC)")
    ap.add_argument("--rate-per-day", type=float, default=500_000_000.0,
                    help="row density in rows/day, spread evenly across the window; default 500,000,000")
    ap.add_argument("--csv", default="../trades20250728.csv.gz")
    ap.add_argument("--table", default="trades")
    ap.add_argument("--num-senders", type=int, default=1)
    ap.add_argument("--chunk-rows", type=int, default=1_000_000)
    ap.add_argument("--connect-timeout", type=float, default=10.0)
    ap.add_argument("--token", default=None)
    ap.add_argument("--username", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--tls-verify", choices=["on", "unsafe_off"], default="on")
    args = ap.parse_args(argv)

    for name, val in (("--num-senders", args.num_senders), ("--chunk-rows", args.chunk_rows)):
        if val <= 0:
            print(f"{name} must be > 0", file=sys.stderr)
            return 2
    if args.token is not None and not args.token.strip():
        print("[error] --token is empty (is $QDB_TOKEN set and exported?).", file=sys.stderr)
        return 2

    today = datetime.now(timezone.utc).date()
    midnight = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
    start = parse_iso(args.start) if args.start else midnight - timedelta(days=1)
    end = parse_iso(args.end) if args.end else midnight + timedelta(hours=13)
    if end <= start:
        print(f"--end ({end}) must be after --start ({start})", file=sys.stderr)
        return 2
    if args.rate_per_day <= 0:
        print("--rate-per-day must be > 0", file=sys.stderr)
        return 2

    start_ns = int(start.timestamp()) * 1_000_000_000   # integer ns (float loses precision at 1e18)
    end_ns = int(end.timestamp()) * 1_000_000_000
    step_ns = int(NS_PER_DAY / args.rate_per_day)
    if step_ns <= 0:
        print("--rate-per-day too high (sub-nanosecond spacing)", file=sys.stderr)
        return 2
    n = (end_ns - start_ns) // step_ns
    if n <= 0:
        print("window too short for the requested rate (0 rows)", file=sys.stderr)
        return 2

    deck = load_deck(args.csv)
    if deck.height == 0:
        print("CSV deck is empty", file=sys.stderr)
        return 2

    span_days = (end_ns - start_ns) / NS_PER_DAY
    print(f"[conf]   {display_scheme(args)} tls={use_tls(args)} "
          f"auth={'token' if args.token else 'basic' if args.username else 'none'} | addrs: {args.addr}")
    print(f"[backfill] window {start.isoformat()} .. {end.isoformat()} ({span_days:.2f} days) @ "
          f"{args.rate_per_day:,.0f} rows/day -> {n:,} rows, one every {step_ns / 1000:.1f} us, "
          f"workers={args.num_senders}, chunk-rows={args.chunk_rows:,}")

    if args.connect_timeout > 0:
        ok, msg = preflight(build_conf(args), args.connect_timeout)
        if not ok:
            print(f"[error] preflight failed: {msg}. Is QuestDB up and the token/addr correct?",
                  file=sys.stderr)
            return 2

    counts = [0] * args.num_senders
    stop = threading.Event()
    t0 = time.monotonic()

    def reporter():
        last = 0
        while not stop.wait(1.0):
            now = sum(counts)
            print(f"[progress] backfilled={now:,} rate={now - last:,} rows/s")
            last = now

    rep = threading.Thread(target=reporter, daemon=True)
    rep.start()

    # Split the n rows into contiguous index ranges, one per worker (each a time block).
    base = n // args.num_senders
    rem = n % args.num_senders
    errors = []
    threads = []
    idx = 0
    for wid in range(args.num_senders):
        count = base + (1 if wid < rem else 0)
        start_index = idx
        idx += count

        def wrap(wid=wid, si=start_index, cnt=count):
            try:
                run_worker(wid, si, cnt, deck, args, start_ns, step_ns, counts)
            except Exception as e:  # noqa: BLE001
                errors.append(f"Worker {wid}: {e}")

        t = threading.Thread(target=wrap, daemon=True)
        t.start()
        threads.append(t)
    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(0.2)
    except KeyboardInterrupt:
        stop.set()
        print(f"\nInterrupted. Backfilled ~{sum(counts):,} rows; exiting.", file=sys.stderr)
        return 130
    stop.set()

    if errors:
        for e in errors:
            print(f"Worker failed: {e}", file=sys.stderr)
        return 1

    elapsed = time.monotonic() - t0
    rate = n / elapsed if elapsed > 0 else 0
    print(f"[backfill] submitted {n:,} rows in {elapsed:.1f}s ({rate:,.0f} rows/s). "
          f"Verify server-side: SELECT count() FROM {args.table} "
          f"WHERE timestamp >= '{start.isoformat()}' AND timestamp < '{end.isoformat()}';")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
