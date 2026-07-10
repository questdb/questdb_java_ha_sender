#!/usr/bin/env python3
"""Parallel CSV replay sender for QuestDB, a Python port of the Java/Rust ha_sender.

Replays a CSV of trades in a loop across N worker threads over one of three transports
(``--protocol``):

  * ``qwp``    - QWP over WebSocket: store-and-forward (un-acked frames spill to disk and
                 replay after an outage), transactional commit, multi-host failover. A
                 background probe polls the latest ingested timestamp and the serving
                 node's live role over a QWP query client (``Client``).
  * ``qwpudp`` - QWP over UDP: fire-and-forget datagrams to :9007. Ingest-only,
                 unauthenticated, single-endpoint, best-effort (no ack, no failover).
  * ``ilp``    - the legacy ILP/HTTP transport.

Requires the QWP/egress build of the questdb Python client (see README.md); the PyPI
release does not expose QWP or the query client.
"""

import argparse
import gzip
import csv as csvmod
import os
import sys
import threading
import time
from datetime import datetime, timezone

from questdb.ingress import Sender, Client, TimestampNanos, ServerTimestamp

PROBE_QUERY = "select timestamp from trades limit -1"
# Enterprise lifecycle status of whichever node the query client is connected to. Returns
# the LIVE role (current_role / target_role), unlike the QWP handshake which only refreshes
# on reconnect and so goes stale after an in-place primary<->replica switch.
STATUS_QUERY = "switch status"


def parse_args(argv):
    p = argparse.ArgumentParser(description="Parallel CSV replay sender for QuestDB")
    p.add_argument("--addrs", default="localhost:9000",
                   help="Comma-separated host:port. QWP/WebSocket + ILP use :9000; UDP uses :9007.")
    p.add_argument("--token", default=None, help="Bearer token (QWP/WebSocket + ILP only).")
    p.add_argument("--username", default=None, help="Basic-auth username.")
    p.add_argument("--password", default=None, help="Basic-auth password.")
    p.add_argument("--total-events", type=int, default=1_000_000)
    p.add_argument("--delay-ms", type=int, default=50)
    p.add_argument("--num-senders", type=int, default=10)
    p.add_argument("--retry-timeout", type=int, default=360_000,
                   help="retry_timeout (ILP) / reconnect_max_duration_millis (QWP), in ms.")
    p.add_argument("--csv", default="./trades20250728.csv.gz")
    p.add_argument("--timestamp-from-file", action="store_true")
    p.add_argument("--seconds-offset", type=int, default=0)
    p.add_argument("--protocol", default="qwp", choices=["qwp", "qwpudp", "ilp"])
    p.add_argument("--sender-id", default="ha_sender")
    p.add_argument("--store-forward-dir", default="/tmp/qdb-sf")
    p.add_argument("--batch-size", type=int, default=10_000)
    p.add_argument("--batches-per-transaction", type=int, default=10)
    p.add_argument("--probe-interval-ms", type=int, default=1000)
    p.add_argument("--enterprise", action="store_true")
    p.add_argument("--zone", default="eu-west-1")
    return p.parse_args(argv)


def addr_list(args):
    return [a.strip() for a in args.addrs.split(",") if a.strip()]


def has_token(args):
    return bool(args.token)


def has_basic(args):
    return bool(args.username) and bool(args.password)


def tls(args):
    return has_token(args) or has_basic(args)


def _append_auth(parts, args):
    if has_token(args):
        parts.append(f"token={args.token};")
    elif has_basic(args):
        parts.append(f"username={args.username};password={args.password};")


def build_ingest_conf(args, worker_id):
    """Ingestion connect string for the configured transport (same keys as the Rust port)."""
    addrs = addr_list(args)
    if args.protocol == "qwpudp":
        return f"qwpudp::addr={addrs[0]};auto_flush=off;"

    if args.protocol == "ilp":
        scheme = "https" if tls(args) else "http"
        parts = [f"{scheme}::addr={','.join(addrs)};", "auto_flush=off;"]
        _append_auth(parts, args)
        if tls(args):
            parts.append("tls_verify=unsafe_off;")
        parts.append(f"retry_timeout={args.retry_timeout};")
        return "".join(parts)

    # qwp (WebSocket)
    scheme = "qwpwss" if tls(args) else "qwpws"
    who = f"{args.sender_id}-{worker_id}"
    sf = os.path.join(args.store_forward_dir, who)
    os.makedirs(sf, exist_ok=True)
    parts = [f"{scheme}::addr={','.join(addrs)};", "auto_flush=off;"]
    _append_auth(parts, args)
    if tls(args):
        parts.append("tls_verify=unsafe_off;")
    parts.append(f"sender_id={who};")
    parts.append(f"sf_dir={sf};")
    parts.append(f"reconnect_max_duration_millis={args.retry_timeout};")
    parts.append("reconnect_initial_backoff_millis=100;")
    parts.append("reconnect_max_backoff_millis=5000;")
    if args.enterprise:
        parts.append("request_durable_ack=true;")
    return "".join(parts)


def build_client_conf(args):
    """Connect string for the QWP query client (probe): target=any (replica-fallback
    reads), failover on, zone bias. Same hosts/auth as the senders."""
    addrs = addr_list(args)
    scheme = "qwpwss" if tls(args) else "qwpws"
    parts = [f"{scheme}::addr={','.join(addrs)};", "target=any;", "failover=true;"]
    _append_auth(parts, args)
    if tls(args):
        parts.append("tls_verify=unsafe_off;")
    if args.zone:
        parts.append(f"zone={args.zone};")
    return "".join(parts)


def load_csv(path, need_timestamp):
    """Load the CSV (optionally gzipped). Returns a list of (symbol, side, price, amount,
    ts_nanos) tuples; ts_nanos is 0 unless need_timestamp."""
    opener = gzip.open if path.endswith(".gz") else open
    rows = []
    with opener(path, "rt", encoding="utf-8", newline="") as fh:
        reader = csvmod.reader(fh)
        header = next(reader, None)
        if header is None:
            return rows
        idx = {name.strip(): i for i, name in enumerate(header)}
        for col in ("symbol", "side", "price", "amount"):
            if col not in idx:
                raise ValueError(f"CSV missing required column: {col}")
        if need_timestamp and "timestamp" not in idx:
            raise ValueError("CSV missing required column: timestamp")
        for rec in reader:
            if not rec:
                continue
            ts_nanos = 0
            if need_timestamp:
                raw = rec[idx["timestamp"]].strip()
                # ISO-8601, e.g. 2025-07-28T12:34:56.789012Z
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                ts_nanos = int(dt.timestamp() * 1_000_000_000)
            rows.append((
                rec[idx["symbol"]].strip(),
                rec[idx["side"]].strip(),
                float(rec[idx["price"]].strip()),
                float(rec[idx["amount"]].strip()),
                ts_nanos,
            ))
    return rows


def run_worker(worker_id, total_events, args, rows, counts):
    print(f"Sender {worker_id} will send {total_events} events")
    is_qwp = args.protocol == "qwp"
    is_udp = args.protocol == "qwpudp"
    # Single worker on a QWP transport stamps rows client-side (monotonic, no O3); ILP or
    # more than one worker use server-side timestamps (ServerTimestamp).
    per_row_ts = (is_qwp or is_udp) and args.num_senders == 1
    # Flush cadence: qwp commits every batch-size*batches-per-transaction; qwpudp/ilp flush
    # every batch-size rows (auto_flush is off, so the buffer is drained explicitly).
    commit_every = args.batch_size * args.batches_per_transaction if is_qwp else args.batch_size

    conf = build_ingest_conf(args, worker_id)
    n = len(rows)
    sent = 0
    with Sender.from_conf(conf) as sender:
        for i in range(total_events):
            symbol, side, price, amount, ts_nanos = rows[i % n]
            if args.timestamp_from_file:
                at = TimestampNanos(ts_nanos + args.seconds_offset * 1_000_000_000)
            elif args.seconds_offset != 0:
                at = TimestampNanos(time.time_ns() + args.seconds_offset * 1_000_000_000)
            elif per_row_ts:
                at = TimestampNanos.now()
            else:
                at = ServerTimestamp
            sender.row(
                "trades",
                symbols={"symbol": symbol, "side": side},
                columns={"price": price, "amount": amount, "trade_id": f"{worker_id}-{i + 1}"},
                at=at,
            )
            sent += 1
            counts[worker_id] = sent
            if sent % commit_every == 0:
                sender.flush()
            if args.delay_ms > 0:
                time.sleep(args.delay_ms / 1000.0)
        sender.flush()
        # QWP/WebSocket: drain any un-acked store-and-forward frames before closing.
        if is_qwp:
            sender.close_drain()
    print(f"Sender {worker_id} finished sending {sent} events")


def _switch_status_roles(client):
    """Return (current_role, target_role) from `switch status`, or (None, None)."""
    try:
        df = client.query(STATUS_QUERY).to_pandas()
    except Exception:
        return None, None
    if len(df) == 0:
        return None, None
    current = target = None
    last = df.iloc[-1]
    for col in df.columns:
        lc = col.lower()
        if "role" not in lc:
            continue
        val = last[col]
        if "current" in lc:
            current = val
        elif "target" in lc:
            target = val
        elif current is None:
            current = val
    return current, target


def run_probe(args, stop):
    conf = build_client_conf(args)
    try:
        client = Client.from_conf(conf)
    except Exception as e:
        print(f"[query client] connect failed: {e}")
        return
    print("[query client] connected")
    was_down = False
    try:
        while not stop.is_set():
            try:
                df = client.query(PROBE_QUERY).to_pandas()
                if was_down:
                    print("[query client] connection restored")
                    was_down = False
                if len(df):
                    latest = df.iloc[-1, 0]
                    current, target = _switch_status_roles(client)
                    if current is not None:
                        switching = target is not None and str(target).lower() != str(current).lower()
                        sw = f" (switching -> {target})" if switching else ""
                        served = f" served by role={current}{sw}"
                    else:
                        served = " (live 'switch status' unavailable, e.g. OSS or missing SYSTEM ADMIN)"
                    print(f"[probe] latest trades timestamp = {latest}{served}")
            except Exception as e:
                if not was_down:
                    print(f"[query client] connection lost ({e}), will retry")
                    was_down = True
            stop.wait(args.probe_interval_ms / 1000.0)
    finally:
        client.close()


def main(argv):
    args = parse_args(argv)

    for name, val in (("--num-senders", args.num_senders), ("--total-events", args.total_events),
                      ("--batch-size", args.batch_size),
                      ("--batches-per-transaction", args.batches_per_transaction)):
        if val <= 0:
            print(f"{name} must be > 0", file=sys.stderr)
            return 2

    addrs = addr_list(args)
    if not addrs:
        print("--addrs must contain at least one host:port", file=sys.stderr)
        return 2
    if args.protocol == "qwpudp":
        if len(addrs) > 1:
            print("--protocol qwpudp supports a single host:port only (UDP is connectionless, "
                  f"there is no failover); got {len(addrs)} addresses", file=sys.stderr)
            return 2
        if args.token or args.username or args.password:
            print("[warn] --protocol qwpudp is unauthenticated (UDP accepts any connection); "
                  "ignoring --token/--username/--password", file=sys.stderr)

    if not os.path.exists(args.csv):
        print(f"CSV file not found: {args.csv}", file=sys.stderr)
        return 2
    rows = load_csv(args.csv, args.timestamp_from_file)
    if not rows:
        print("CSV has no data rows.", file=sys.stderr)
        return 2

    if args.protocol == "qwp":
        print(f"Ingestion started. Protocol: qwp (WebSocket, sender-id={args.sender_id}, "
              f"store-and-forward={args.store_forward_dir}, batch-size={args.batch_size}, "
              f"batches-per-transaction={args.batches_per_transaction}, "
              f"retry-timeout-ms={args.retry_timeout}) | addrs: {','.join(addrs)}")
    elif args.protocol == "qwpudp":
        print(f"Ingestion started. Protocol: qwpudp (QWP/UDP datagrams, ingest-only, "
              f"unauthenticated, no store-and-forward, no failover; query client disabled, "
              f"batch-size={args.batch_size}) | addr: {addrs[0]}")
    else:
        print(f"Ingestion started. Protocol: ilp (HTTP, retry-timeout-ms={args.retry_timeout}) "
              f"| addrs: {','.join(addrs)}")

    counts = [0] * args.num_senders
    stop = threading.Event()
    start = time.monotonic()

    def reporter():
        last = 0
        while not stop.is_set():
            stop.wait(1.0)
            now = sum(counts)
            print(f"[progress] sent={now} rate={now - last} rows/s")
            last = now

    threads = []
    rep = threading.Thread(target=reporter, daemon=True)
    rep.start()

    probe_thread = None
    if args.protocol == "qwp" and args.probe_interval_ms > 0:
        probe_thread = threading.Thread(target=run_probe, args=(args, stop), daemon=True)
        probe_thread.start()

    base = args.total_events // args.num_senders
    rem = args.total_events % args.num_senders
    errors = []

    def worker_wrapper(wid, events):
        try:
            run_worker(wid, events, args, rows, counts)
        except Exception as e:  # noqa: BLE001
            errors.append(f"Sender {wid}: {e}")

    for wid in range(args.num_senders):
        events = base + (1 if wid < rem else 0)
        t = threading.Thread(target=worker_wrapper, args=(wid, events))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    stop.set()
    if probe_thread:
        probe_thread.join(timeout=2.0)

    if errors:
        for e in errors:
            print(f"Worker failed: {e}", file=sys.stderr)
        return 1

    elapsed = time.monotonic() - start
    rate = args.total_events / elapsed if elapsed > 0 else 0
    print(f"All workers completed. protocol={args.protocol} events={args.total_events} "
          f"elapsed={elapsed:.3f} s throughput={rate:,.0f} rows/s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
