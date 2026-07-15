#!/usr/bin/env python3
"""Stream a table through polars, enrich it, and write it back - chunked.

Memory-efficient polars round-trip over the QWP query client. Rather than
materializing the whole result set, it streams the read in Arrow record batches
and ingests each enriched batch before pulling the next, so peak memory is one
batch (not all ``--limit`` rows):

  * READ  - ``Client.query(sql).iter_arrow()`` yields ``pyarrow.RecordBatch`` chunks
    one at a time from a server-side cursor (verified streaming in the client:
    ``iter_arrow`` pulls via ``line_reader_cursor_next_batch``; ``to_polars`` by
    contrast is "materialise-whole" and would buffer every row in RAM);
    ``pl.from_arrow(batch)`` wraps each into a polars DataFrame (zero-copy).
  * ENRICH - add an ``enriched_rnd`` column: a random ``A``-``Z`` value per row.
  * WRITE - ``Client.dataframe(chunk, ...)`` ingests each polars chunk into
    ``enriched_<table>_demo`` over the QWP Arrow-columnar path.

Both the read and the write are polars DataFrames. No pandas in the round-trip.
The read and the write use separate ``Client`` connections so the open read
stream and the ingest never share a socket.

Column typing is automatic: QuestDB SYMBOL columns come back as polars ``Categorical``
and VARCHAR as ``String``, and ``Client.dataframe``'s default ``symbols='auto'`` maps
``Categorical -> SYMBOL`` and ``String -> VARCHAR`` on the way back in. ``enriched_rnd``
is built as a ``Categorical`` so it too lands as a SYMBOL.

Auth / TLS (QuestDB Enterprise): pass ``--token`` (bearer) or ``--username``/``--password``
(basic). Either turns on TLS automatically (scheme ``qwpwss``); ``--tls`` forces TLS with
no auth. Certificate verification is on by default; use ``--tls-verify unsafe_off`` for
self-signed certs. The same credentials are applied to the HTTP ``/exec`` calls used for
drop/verify.

Usage:
    python enrich_polars_demo.py TABLE \
        [--addr host:9000] [--limit 200000000] [--timestamp-col timestamp] \
        [--seed 42] [--keep] \
        [--token TOK | --username U --password P] [--tls] [--tls-verify on|unsafe_off]

By default the target table is dropped and recreated on each run; pass ``--keep``
to append instead. ``--limit`` defaults to the latest 200 million rows by designated
timestamp, streamed ascending so re-ingest stays in timestamp order. Requires the
QWP/egress build of the client (see README.md).
"""

import argparse
import base64
import json
import ssl
import sys
import time
import urllib.parse
import urllib.request

import numpy as np
import polars as pl

from questdb.ingress import Client

LETTERS = np.array(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))


def use_tls(args):
    return bool(args.tls or args.token or (args.username and args.password))


def build_conf(args):
    """QWP connect string for the reader/writer, with optional auth + TLS."""
    scheme = "qwpwss" if use_tls(args) else "qwpws"
    parts = [f"{scheme}::addr={args.addr};"]
    if args.token:
        parts.append(f"token={args.token};")
    elif args.username and args.password:
        parts.append(f"username={args.username};password={args.password};")
    if use_tls(args) and args.tls_verify == "unsafe_off":
        parts.append("tls_verify=unsafe_off;")
    return "".join(parts)


def exec_sql(args, sql):
    """Run a statement over the HTTP(S) /exec endpoint (auth-aware), return JSON."""
    scheme = "https" if use_tls(args) else "http"
    host = args.addr.split(",")[0]
    url = f"{scheme}://{host}/exec?" + urllib.parse.urlencode({"query": sql})
    req = urllib.request.Request(url)
    if args.token:
        req.add_header("Authorization", f"Bearer {args.token}")
    elif args.username and args.password:
        cred = base64.b64encode(f"{args.username}:{args.password}".encode()).decode()
        req.add_header("Authorization", f"Basic {cred}")
    ctx = None
    if use_tls(args) and args.tls_verify == "unsafe_off":
        ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
        return json.load(resp)


def wait_for_count(args, table, at_least, timeout_s=60):
    """Poll until the table holds >= at_least rows (QWP commits asynchronously)."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            n = int(exec_sql(args, f"select count() from {table}")["dataset"][0][0])
            if n >= at_least:
                return n
        except Exception:
            pass
        time.sleep(0.25)
    return -1


def enrich(chunk, rng):
    """Add an `enriched_rnd` column of random A-Z letters (polars Categorical, so
    it lands as a QuestDB SYMBOL). `rng` carries state across chunks so the random
    values are not identical batch-to-batch."""
    rnd = LETTERS[rng.integers(0, 26, size=chunk.height)]
    return chunk.with_columns(pl.Series("enriched_rnd", rnd, dtype=pl.Categorical))


def main(argv):
    ap = argparse.ArgumentParser(description="Chunked polars read -> enrich -> write")
    ap.add_argument("table", help="source table name")
    ap.add_argument("--addr", default="localhost:9000", help="host:port (QWP/HTTP port)")
    ap.add_argument("--limit", type=int, default=200_000_000,
                    help="max rows to read (latest N by timestamp); default 200,000,000")
    ap.add_argument("--timestamp-col", default="timestamp",
                    help="designated timestamp column of the source table")
    ap.add_argument("--seed", type=int, default=42, help="RNG seed for enriched_rnd")
    ap.add_argument("--keep", action="store_true",
                    help="append to the target instead of dropping it first")
    # Enterprise auth / TLS
    ap.add_argument("--token", default=None, help="bearer token (turns on TLS)")
    ap.add_argument("--username", default=None, help="basic-auth username (turns on TLS)")
    ap.add_argument("--password", default=None, help="basic-auth password")
    ap.add_argument("--tls", action="store_true", help="force TLS (qwpwss) with no auth")
    ap.add_argument("--tls-verify", choices=["on", "unsafe_off"], default="on",
                    help="certificate verification; use unsafe_off for self-signed")
    args = ap.parse_args(argv)

    ts_col = args.timestamp_col
    target = f"enriched_{args.table}_demo"
    rng = np.random.default_rng(args.seed)
    conf = build_conf(args)
    print(f"[conf]   {'wss' if use_tls(args) else 'ws'} tls={use_tls(args)} "
          f"auth={'token' if args.token else 'basic' if args.username else 'none'}")

    if not args.keep:
        try:
            exec_sql(args, f"drop table if exists {target}")
        except Exception as e:
            print(f"[warn]   could not drop '{target}' via /exec ({e}); "
                  f"continuing (rows will append)", file=sys.stderr)

    # `limit -N` returns the latest N rows in designated-timestamp (ascending)
    # order already, so the stream re-ingests in order with no explicit sort.
    sql = f"select * from {args.table} limit -{args.limit}"

    total = 0
    t0 = time.monotonic()
    # Separate connections: the read stream stays open while each chunk is written.
    with Client.from_conf(conf) as reader, Client.from_conf(conf) as writer:
        result = reader.query(sql)
        for i, batch in enumerate(result.iter_arrow()):
            chunk = pl.from_arrow(batch)          # RecordBatch -> polars, zero-copy
            if chunk.height == 0:
                continue
            chunk = enrich(chunk, rng)
            writer.dataframe(chunk, table_name=target, at=ts_col)
            total += chunk.height
            print(f"[chunk {i}] {chunk.height:,} rows enriched + written "
                  f"(running total {total:,})")

    if total == 0:
        print("[read] source table is empty - nothing to enrich", file=sys.stderr)
        return 1
    print(f"[done]   streamed {total:,} rows -> '{target}' in "
          f"{time.monotonic() - t0:.1f}s (peak memory ~one batch)")

    # --- VERIFY server-side (QWP applies the commit asynchronously) ---
    expected = total if not args.keep else 0
    n = wait_for_count(args, target, expected)
    print(f"[verify] '{target}' now holds {n:,} rows")
    try:
        dist = exec_sql(
            args,
            f"select enriched_rnd, count() n from {target} "
            f"group by enriched_rnd order by enriched_rnd"
        )["dataset"]
        print(f"[verify] enriched_rnd distinct values: {len(dist)} (sample: {dist[:5]})")
    except Exception as e:
        print(f"[warn]   verify query failed: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
