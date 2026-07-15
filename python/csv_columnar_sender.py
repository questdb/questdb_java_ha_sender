#!/usr/bin/env python3
"""Fast CSV replay sender for QuestDB - the COLUMNAR QWP path.

This is the throughput-oriented Python sender. It replays a CSV to a QuestDB
``trades`` table over the QWP WebSocket transport using the *column-major* client
path (``Client.dataframe``), which ships whole Arrow columns to the native Rust
client in bulk. That is the ONLY Python path that gets near Java/Rust throughput.

Why not row-by-row? ``Sender.row(...)`` in a Python loop is the SLOWEST path: every
cell crosses the Python/Cython boundary under the GIL. The client's own perf notes
call the row API a "per-cell call ... ~16x slower" than the columnar bulk path.
The column path (``Client.dataframe``) stores raw pointers into the Arrow buffers
("no data copy at append time") and does one memcpy per column at flush. Use it.

Memory stays flat: the CSV is loaded once into a polars DataFrame, and rows are
sent in bounded, zero-copy slices. ``Client.dataframe`` additionally streams each
send internally at ``max_rows_per_batch`` (16384) rows, so nothing balloons - unlike
the row-by-row sender, which buffered up to 100k rows before its first flush and
grew without bound when Python could not reach that threshold fast enough.

Timestamps:
  * ``--timestamp-from-file`` uses the CSV's ``timestamp`` column (parsed to ns).
  * otherwise each batch is stamped with a strictly-increasing "now" (nanoseconds),
    so the target looks live and there is no out-of-order within a worker.

Auth / TLS (QuestDB Enterprise): ``--token`` (bearer) or ``--username``/``--password``
(basic) turn on TLS automatically (scheme ``qwpwss``); ``--tls`` forces TLS with no
auth. Certificate verification is on by default; ``--tls-verify unsafe_off`` for
self-signed certs. Multiple ``--addrs`` give QWP failover.

Usage:
    python csv_columnar_sender.py \
        --addrs host:9000[,host2:9000] \
        --total-events 100000000 \
        --num-senders 1 \
        --chunk-rows 100000 \
        --rate 0 \
        --csv ../trades.csv \
        [--timestamp-from-file] \
        [--token TOK | --username U --password P] [--tls-verify on|unsafe_off]

Requires the QWP/egress build of the client (see README.md).
"""

import argparse
import gzip
import io
import os
import sys
import threading
import time

import numpy as np
import polars as pl

from questdb.ingress import Client

TABLE = "trades"


def use_tls(args):
    return bool(args.tls or args.token or (args.username and args.password))


def build_conf(args):
    """QWP connect string for the columnar client, with optional auth + TLS + failover."""
    scheme = "qwpwss" if use_tls(args) else "qwpws"
    addrs = ",".join(a.strip() for a in args.addrs.split(",") if a.strip())
    parts = [f"{scheme}::addr={addrs};"]
    if args.token:
        parts.append(f"token={args.token};")
    elif args.username and args.password:
        parts.append(f"username={args.username};password={args.password};")
    if use_tls(args) and args.tls_verify == "unsafe_off":
        parts.append("tls_verify=unsafe_off;")
    return "".join(parts)


def preflight(conf, timeout_s):
    """Check the server answers within timeout_s over the full path (TCP+TLS+auth+query),
    in a daemon thread so a hung native connect can't block us. Returns (ok, message).
    Without this, a down/unreachable/misconfigured server makes the ingest client retry
    forever and sit at sent=0."""
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
        return False, f"no response within {timeout_s:g}s (server down / unreachable / wrong host or port?)"
    if outcome.get("ok"):
        return True, None
    return False, outcome.get("err", "unknown error")


def load_base(path, need_timestamp):
    """Load the CSV once into a compact polars DataFrame. symbol/side become
    Categorical (-> SYMBOL), price/amount Float64, and (if needed) timestamp is
    parsed to nanosecond Datetime. trade_id is synthesised per batch at send time."""
    if path.endswith(".gz"):
        with gzip.open(path, "rb") as fh:
            df = pl.read_csv(io.BytesIO(fh.read()))
    else:
        df = pl.read_csv(path)
    for col in ("symbol", "side", "price", "amount"):
        if col not in df.columns:
            raise ValueError(f"CSV missing required column: {col}")
    exprs = [
        pl.col("symbol").cast(pl.Utf8).cast(pl.Categorical),
        pl.col("side").cast(pl.Utf8).cast(pl.Categorical),
        pl.col("price").cast(pl.Float64),
        pl.col("amount").cast(pl.Float64),
    ]
    if need_timestamp:
        if "timestamp" not in df.columns:
            raise ValueError("CSV missing required column: timestamp")
        exprs.append(
            pl.col("timestamp").cast(pl.Utf8).str.to_datetime(time_unit="ns").alias("timestamp")
        )
    return df.select(exprs)


def run_worker(wid, events, base, args, counts):
    """Send `events` rows for this worker: cycle over the base frame in bounded
    slices, attach trade_id (+ a live timestamp unless reading it from file), and
    ship each slice columnar via Client.dataframe."""
    m = base.height
    from_file = args.timestamp_from_file
    conf = build_conf(args)

    # Rate pacing (aggregate rows/s across all workers): deadline schedule, sleep
    # only when ahead. interval = ns per row for this worker's share.
    interval_ns = (1_000_000_000.0 * args.num_senders / args.rate) if args.rate > 0 else 0.0

    sent = 0
    pos = 0
    last_ts = 0
    pace_start = time.perf_counter_ns()
    with Client.from_conf(conf) as client:
        while sent < events:
            n = min(args.chunk_rows, events - sent, m - pos)
            sl = base.slice(pos, n)  # zero-copy view into the base frame

            # trade_id, vectorised (native polars, no Python per-row loop).
            add = [(pl.lit(f"{wid}-") + pl.int_range(sent, sent + n).cast(pl.Utf8)).alias("trade_id")]
            if not from_file:
                base_ns = max(time.time_ns(), last_ts + 1)
                ts = np.arange(n, dtype=np.int64) + base_ns  # strictly increasing -> no O3
                last_ts = int(ts[-1])
                add.append(pl.Series("timestamp", ts).cast(pl.Datetime("ns", "UTC")).alias("timestamp"))
            chunk = sl.with_columns(add)

            client.dataframe(chunk, table_name=TABLE, symbols=["symbol", "side"], at="timestamp")

            sent += n
            counts[wid] = sent
            pos = (pos + n) % m

            if interval_ns > 0.0:
                target = pace_start + int(sent * interval_ns)
                sleep_ns = target - (time.perf_counter_ns() - pace_start)
                if sleep_ns > 1_000_000:
                    time.sleep(sleep_ns / 1_000_000_000)
    print(f"Sender {wid} finished sending {sent} events")


def main(argv):
    ap = argparse.ArgumentParser(description="Fast columnar CSV replay sender for QuestDB")
    ap.add_argument("--addrs", default="localhost:9000",
                    help="comma-separated host:port (QWP/WebSocket port, usually :9000)")
    ap.add_argument("--total-events", type=int, default=1_000_000)
    ap.add_argument("--num-senders", type=int, default=1,
                    help="worker threads, each with its own columnar client")
    ap.add_argument("--chunk-rows", type=int, default=100_000,
                    help="rows per Client.dataframe call (pacing granularity + peak-memory bound)")
    ap.add_argument("--rate", type=int, default=0,
                    help="target aggregate rows/second across ALL workers (0 = flat out)")
    ap.add_argument("--csv", default="../trades20250728.csv.gz")
    ap.add_argument("--timestamp-from-file", action="store_true",
                    help="use the CSV timestamp column instead of a live 'now' stamp")
    # Enterprise auth / TLS
    ap.add_argument("--token", default=None, help="bearer token (turns on TLS)")
    ap.add_argument("--username", default=None, help="basic-auth username (turns on TLS)")
    ap.add_argument("--password", default=None, help="basic-auth password")
    ap.add_argument("--tls", action="store_true", help="force TLS (qwpwss) with no auth")
    ap.add_argument("--tls-verify", choices=["on", "unsafe_off"], default="on",
                    help="certificate verification; use unsafe_off for self-signed")
    ap.add_argument("--connect-timeout", type=float, default=10.0,
                    help="seconds to wait for the server on a preflight check before failing "
                         "loudly (0 = skip; the ingest client would otherwise retry forever)")
    args = ap.parse_args(argv)

    for name, val in (("--total-events", args.total_events), ("--num-senders", args.num_senders),
                      ("--chunk-rows", args.chunk_rows)):
        if val <= 0:
            print(f"{name} must be > 0", file=sys.stderr)
            return 2
    if args.rate < 0:
        print("--rate must be >= 0", file=sys.stderr)
        return 2
    if not os.path.exists(args.csv):
        print(f"CSV file not found: {args.csv}", file=sys.stderr)
        return 2
    # An empty --token (typically an unset env var, e.g. --token "$ILP_TOKEN" with
    # ILP_TOKEN unset) silently disables auth+TLS. Against a TLS/auth server the client
    # then hangs retrying the handshake at sent=0, so fail loudly instead.
    if args.token is not None and not args.token.strip():
        print("[error] --token is empty (is $ILP_TOKEN set and exported?). It would connect "
              "with NO auth/TLS and hang against a secured server. Set the token, or drop "
              "--token to connect plaintext on purpose.", file=sys.stderr)
        return 2
    if args.username and not (args.password or "").strip():
        print("[error] --username given but --password is empty.", file=sys.stderr)
        return 2

    base = load_base(args.csv, args.timestamp_from_file)
    if base.height == 0:
        print("CSV has no data rows.", file=sys.stderr)
        return 2

    conf = build_conf(args)
    print(f"[conf]   {conf.split('::')[0]} tls={use_tls(args)} "
          f"auth={'token' if args.token else 'basic' if args.username else 'none'} "
          f"| addrs: {args.addrs}")

    if args.connect_timeout > 0:
        t0 = time.monotonic()
        ok, msg = preflight(conf, args.connect_timeout)
        if not ok:
            print(f"[error] preflight failed: {msg}. Is QuestDB up and the token/addr correct? "
                  f"(set --connect-timeout 0 to skip this check)", file=sys.stderr)
            return 2
        print(f"[preflight] server reachable ({time.monotonic() - t0:.2f}s)")

    print(f"Ingestion started (columnar QWP). base={base.height:,} rows, "
          f"total={args.total_events:,}, workers={args.num_senders}, chunk-rows={args.chunk_rows:,}, "
          f"timestamps={'file' if args.timestamp_from_file else 'live-now'}, "
          f"pacing={'rate ' + str(args.rate) + ' rows/s' if args.rate else 'flat out'}")

    counts = [0] * args.num_senders
    stop = threading.Event()
    start = time.monotonic()

    def reporter():
        last = 0
        while not stop.is_set():
            stop.wait(1.0)
            now = sum(counts)
            print(f"[progress] sent={now:,} rate={now - last:,} rows/s")
            last = now

    rep = threading.Thread(target=reporter, daemon=True)
    rep.start()

    base_events = args.total_events // args.num_senders
    rem = args.total_events % args.num_senders
    errors = []

    def wrapper(wid, ev):
        try:
            run_worker(wid, ev, base, args, counts)
        except Exception as e:  # noqa: BLE001
            errors.append(f"Sender {wid}: {e}")

    # Daemon workers + a polling join so Ctrl+C is honoured even while a worker is
    # blocked in a native connect/flush: KeyboardInterrupt fires between join timeouts,
    # and daemon threads don't hold the process open once main returns.
    threads = []
    for wid in range(args.num_senders):
        ev = base_events + (1 if wid < rem else 0)
        t = threading.Thread(target=wrapper, args=(wid, ev), daemon=True)
        t.start()
        threads.append(t)
    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(0.2)
    except KeyboardInterrupt:
        stop.set()
        print(f"\nInterrupted. Sent ~{sum(counts):,} rows; exiting.", file=sys.stderr)
        return 130
    stop.set()

    if errors:
        for e in errors:
            print(f"Worker failed: {e}", file=sys.stderr)
        return 1

    elapsed = time.monotonic() - start
    rate = args.total_events / elapsed if elapsed > 0 else 0
    print(f"All workers completed. events={args.total_events:,} elapsed={elapsed:.3f}s "
          f"throughput={rate:,.0f} rows/s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
