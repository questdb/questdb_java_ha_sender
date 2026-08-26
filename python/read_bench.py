#!/usr/bin/env python3
"""Read the last N rows of a table as fast as Python allows, reporting rows/sec and
throughput (MB/s, Gb/s) live.

Throughput is measured as the decoded Arrow payload (``batch.nbytes``), i.e. the data
volume handed to the reader, not the compressed QWP wire bytes (which the client does
not expose) - so it is a data-throughput figure, generally larger than the on-wire rate.

Fastest path, by design: consume the QWP query cursor's Arrow batches directly
(``db.query(sql).iter_arrow()``) and just tally ``batch.num_rows``. No pandas
or polars conversion - the Rust client already decodes the wire into Arrow inside
``iter_arrow`` (that decode is the unavoidable "read" work, done in native code),
so the Python loop does nothing but add row counts. Converting each batch to
polars (``pl.from_arrow``) or pandas (``iter_pandas``) would only add a per-batch
materialisation step on top and measure a slower number. (``to_arrow``/``to_polars``
are "materialise-whole" - they buffer the entire result; ``iter_arrow`` streams.)

A single connection is capped by the client's socket buffer at ~426 KB per RTT (the
QWP client hardcodes a 4 MiB buffer that default Linux clamps; see ../boost_tcp.sh),
so on a real network one reader is round-trip bound no matter how lean the loop is.
``--readers N`` runs N parallel connections and reports the aggregate rows/sec, which
is the way past the per-connection wall. ``--readers 1`` (default) is the plain
single stream.

How the row range is divided across those readers is ``--split``:

``rows`` (default) cuts the range into ``--chunks`` row-offset slices using QuestDB's
``LIMIT -m, -n`` (take the last m rows, then drop the last n, giving the half-open
range ``[-m, -n)``), and workers pull chunks off a shared queue. Two consequences
matter. Chunks hold an equal number of rows regardless of how unevenly the data is
distributed in time, and with ``chunks > readers`` no single worker owns the whole
cold end of the range - work is interleaved and self-balancing, since a worker that
lands on cached data just takes the next chunk. It also needs no preliminary query
and never has to know the designated timestamp's name.

The offset skip is metadata-only, not a scan: ``PageFrameRecordCursorImpl.skipRows``
walks whole page frames subtracting ``partitionHi - partitionLo`` and only descends
into the frame where the skip lands, so skipping billions of rows costs microseconds
(measured: 10.4B rows skipped in 12ms). Do not be alarmed by "Row forward scan" in
EXPLAIN - no column data is decoded for skipped frames.

``time`` is the original behaviour: one preliminary query finds the range's min/max
timestamp, which is cut into N equal time spans, one per reader. Equal *time*, not
equal rows, so skew in the data's time distribution makes readers finish at wildly
different times. Its one advantage is that the boundaries are absolute timestamps
computed once, so all readers agree on them even if the table is being written to
concurrently; row offsets are re-evaluated per connection and can shift under
appends. Use ``time`` for a live table, ``rows`` for everything else.

Auth / TLS (QuestDB Enterprise): pass ``--token`` (bearer) or ``--username``/``--password``
(basic). Either turns on TLS automatically (scheme ``wss``); ``--tls`` forces TLS with
no auth. Certificate verification is on by default; use ``--tls-verify unsafe_off`` for
self-signed certs.

Usage:
    python read_bench.py TABLE \
        [--addr host:9000] [--limit 10000000] [--readers 1] \
        [--split rows|time] [--chunks N] \
        [--token TOK | --username U --password P] [--tls] [--tls-verify on|unsafe_off]

Requires the questdb 5.0 client (see README.md).
"""

import argparse
import queue
import sys
import threading
import time
from datetime import datetime, timezone

import questdb


def use_tls(args):
    return bool(args.tls or args.token or (args.username and args.password))


def build_conf(args):
    """QWP connect string for the reader(s), with optional auth + TLS."""
    scheme = "wss" if use_tls(args) else "ws"
    parts = [f"{scheme}::addr={args.addr};"]
    if args.token:
        parts.append(f"token={args.token};")
    elif args.username and args.password:
        parts.append(f"username={args.username};password={args.password};")
    if use_tls(args) and args.tls_verify == "unsafe_off":
        parts.append("tls_verify=unsafe_off;")
    return "".join(parts)


def ns_to_iso(ns):
    """Epoch nanoseconds -> ISO-8601 string with nanosecond precision (unit-safe
    for both TIMESTAMP and TIMESTAMP_NS columns in a SQL literal)."""
    sec, frac = divmod(int(ns), 1_000_000_000)
    dt = datetime.fromtimestamp(sec, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{frac:09d}Z"


def row_chunks(args):
    """Build `--chunks` SELECTs covering the last `--limit` rows as equal row-count
    slices, oldest first, using QuestDB's `LIMIT -m, -n`.

    Per the LIMIT reference, `LIMIT -m, -n` takes the last m records then drops the
    last n of those, i.e. the half-open range [-m, -n) - so consecutive chunks tile
    the range with no gap and no overlap. The newest chunk has n == 0, which is the
    documented `LIMIT -n, 0` == `LIMIT -n` form.

    Needs no preliminary query: the bounds are arithmetic on --limit alone.
    """
    table, limit, chunks = args.table, args.limit, args.chunks
    edges = [(limit * i) // chunks for i in range(chunks + 1)]
    sqls = []
    for i in range(chunks):
        lo = limit - edges[i]          # rows from the end, inclusive start
        hi = limit - edges[i + 1]      # rows from the end, exclusive end
        if lo <= hi:                   # empty chunk (chunks > limit); nothing to read
            continue
        if hi == 0:
            sqls.append(f"select * from {table} limit -{lo}")
        else:
            sqls.append(f"select * from {table} limit -{lo}, -{hi}")
    return sqls


def time_slices(args, conf):
    """Build one SELECT per reader by splitting the last-N rows' timestamp range into
    N equal time spans. Costs one preliminary query and assumes --timestamp-col names
    the designated timestamp."""
    table, ts, limit, readers = args.table, args.timestamp_col, args.limit, args.readers
    if readers <= 1:
        return [f"select * from {table} limit -{limit}"]
    # Range of the latest `limit` rows, as epoch nanoseconds.
    with questdb.connect(conf) as db:
        mm = db.query(
            f"select min({ts}) lo, max({ts}) hi "
            f"from (select {ts} from {table} limit -{limit})"
        ).to_polars()
    if mm.height == 0 or mm["lo"][0] is None:
        return [f"select * from {table} limit -{limit}"]
    lo = mm["lo"].dt.epoch("ns")[0]
    hi = mm["hi"].dt.epoch("ns")[0]
    if hi <= lo:  # single instant / one row - can't split by time
        print("[warn] timestamp range too narrow to split; using 1 reader", file=sys.stderr)
        return [f"select * from {table} limit -{limit}"]
    span = hi - lo
    edges = [lo + (span * i) // readers for i in range(readers + 1)]
    edges[-1] = hi
    sqls = []
    for i in range(readers):
        a = ns_to_iso(edges[i])
        b = ns_to_iso(edges[i + 1])
        op = "<=" if i == readers - 1 else "<"  # last slice includes the max row
        sqls.append(f"select * from {table} where {ts} >= '{a}' and {ts} {op} '{b}'")
    return sqls


def run_reader(idx, work, conf, counts, byts, chunks_done, errors,
               sample_n, sample_out, sample_lock):
    """One worker: hold a single connection and drain chunks off `work` until empty.

    `db.reader()` leases one pooled connection for the lease's lifetime and runs its
    queries sequentially on it, so a worker is one connection no matter how many
    chunks it processes. The lease must be used only on the thread that created it,
    which is what this function is.
    """
    try:
        n = 0
        b = 0
        with questdb.connect(conf) as db:
            with db.reader() as reader:
                while True:
                    try:
                        sql = work.get_nowait()
                    except queue.Empty:
                        break
                    # Every chunk hits the same table and the same SYMBOL columns, so keep
                    # the connection's symbol dictionary warm instead of rebuilding it per
                    # chunk. On the lease's first query the dictionary is empty anyway.
                    result = reader.query(sql, reset_symbol_dict=False)
                    for batch in result.iter_arrow():
                        n += batch.num_rows    # count rows and decoded Arrow bytes
                        b += batch.nbytes      # (buffer bytes of this batch's columns)
                        # Reuse Arrow data we already received: wrap the first non-empty
                        # batch any worker sees as a polars DataFrame (zero-copy) and keep
                        # its head (one-off, no extra query, no re-scan). The num_rows test
                        # is load-bearing: a chunk whose offsets fall past the end of the
                        # table (--limit larger than the table) still yields a schema-only
                        # batch of 0 rows, and those chunks finish first, so latching on any
                        # first batch would print an empty sample with a correct schema.
                        if sample_n > 0 and batch.num_rows > 0 and sample_out[0] is None:
                            with sample_lock:
                                if sample_out[0] is None:
                                    import polars as pl
                                    sample_out[0] = pl.from_arrow(batch).head(sample_n)
                        counts[idx] = n
                        byts[idx] = b
                    counts[idx] = n
                    byts[idx] = b
                    chunks_done[idx] += 1
    except Exception as e:  # noqa: BLE001
        errors.append(f"reader {idx}: {e}")


def main(argv):
    ap = argparse.ArgumentParser(description="Read last N rows as fast as possible, report rows/sec")
    ap.add_argument("table", help="source table name")
    ap.add_argument("--addr", default="localhost:9000", help="host:port (QWP/HTTP port)")
    ap.add_argument("--limit", type=int, default=10_000_000,
                    help="number of most-recent rows to read; default 10,000,000")
    ap.add_argument("--readers", type=int, default=1,
                    help="parallel reader connections; default 1")
    ap.add_argument("--split", choices=["rows", "time"], default="rows",
                    help="how to divide the range: 'rows' (equal row counts via LIMIT "
                         "offsets, striped over --chunks, no preliminary query) or "
                         "'time' (equal timestamp spans, one slice per reader, needs a "
                         "preliminary query but is stable under concurrent writes); "
                         "default rows")
    ap.add_argument("--chunks", type=int, default=0,
                    help="number of row-offset chunks to stripe across readers "
                         "(--split rows only); default 0 = readers * 8. More chunks "
                         "balance better; each costs one extra query round trip")
    ap.add_argument("--timestamp-col", default="timestamp",
                    help="designated timestamp column (--split time only)")
    ap.add_argument("--report-interval", type=float, default=0.5,
                    help="seconds between progress lines; default 0.5")
    ap.add_argument("--sample", type=int, default=5,
                    help="after the scan, show this many rows as a polars DataFrame (0 to disable)")
    # Enterprise auth / TLS
    ap.add_argument("--token", default=None, help="bearer token (turns on TLS)")
    ap.add_argument("--username", default=None, help="basic-auth username (turns on TLS)")
    ap.add_argument("--password", default=None, help="basic-auth password")
    ap.add_argument("--tls", action="store_true", help="force TLS (wss) with no auth")
    ap.add_argument("--tls-verify", choices=["on", "unsafe_off"], default="on",
                    help="certificate verification; use unsafe_off for self-signed")
    args = ap.parse_args(argv)

    if args.readers < 1:
        print("--readers must be >= 1", file=sys.stderr)
        return 2

    conf = build_conf(args)
    print(f"[conf]   {'wss' if use_tls(args) else 'ws'} tls={use_tls(args)} "
          f"auth={'token' if args.token else 'basic' if args.username else 'none'}")
    if args.split == "rows":
        args.chunks = max(args.chunks or args.readers * 8, args.readers)
        sqls = row_chunks(args)
        readers = min(args.readers, len(sqls))
    else:
        sqls = time_slices(args, conf)
        readers = len(sqls)   # time mode is one slice per reader, capped by fallbacks

    work = queue.Queue()
    for sql in sqls:
        work.put(sql)

    print(f"[scan]   reading last {args.limit:,} rows of '{args.table}' "
          f"across {readers} reader(s), {len(sqls)} {args.split} chunk(s) ...")

    counts = [0] * readers
    byts = [0] * readers
    chunks_done = [0] * readers
    errors = []
    sample_out = [None]   # first Arrow batch any worker sees, as a few polars rows
    sample_lock = threading.Lock()
    stop = threading.Event()

    def reporter():
        last_r, last_b = 0, 0
        # stop.wait() returns True once the event is set -> exit without a final
        # stray line after [done]; it returns False on timeout -> print a tick.
        while not stop.wait(args.report_interval):
            r, b = sum(counts), sum(byts)
            rps = (r - last_r) / args.report_interval
            mbps = (b - last_b) / args.report_interval / 1e6
            print(f"[scan]   {r:>14,} rows | {rps:>13,.0f} rows/s | {mbps:>9,.1f} MB/s")
            last_r, last_b = r, b

    rep = threading.Thread(target=reporter, daemon=True)
    rep.start()

    t0 = time.monotonic()
    threads = []
    for i in range(readers):
        t = threading.Thread(target=run_reader,
                             args=(i, work, conf, counts, byts, chunks_done, errors,
                                   args.sample, sample_out, sample_lock))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.monotonic() - t0
    stop.set()

    if errors:
        for e in errors:
            print(f"reader failed: {e}", file=sys.stderr)
        return 1

    total = sum(counts)
    tbytes = sum(byts)
    if total == 0:
        print(f"[done]   '{args.table}' returned 0 rows", file=sys.stderr)
        return 1
    rate = total / elapsed if elapsed > 0 else float("inf")
    mbps = tbytes / elapsed / 1e6 if elapsed > 0 else float("inf")
    gbps = tbytes * 8 / elapsed / 1e9 if elapsed > 0 else float("inf")
    gib = tbytes / (1024 ** 3)
    print(f"[done]   {total:,} rows, {gib:.2f} GiB decoded in {elapsed:.3f}s "
          f"across {readers} reader(s)")
    print(f"[done]   {rate:,.0f} rows/s | {mbps:,.1f} MB/s | {gbps:.2f} Gb/s "
          f"(decoded Arrow payload, not wire bytes)")
    if readers > 1:
        per = "  ".join(f"r{i}={c:,}({chunks_done[i]}ch)" for i, c in enumerate(counts))
        print(f"[done]   per-reader rows: {per}")

    # An extract of the data we actually received: a few rows sliced off the first Arrow batch
    # during the scan (no extra query, no re-scan) and rendered as a polars DataFrame.
    if args.sample > 0 and sample_out[0] is not None:
        import polars as pl
        df = sample_out[0]
        print(f"\n[sample] first {df.height} row(s) received, as a polars DataFrame "
              f"(reused from the scanned Arrow batches - no extra query):")
        with pl.Config(tbl_rows=args.sample, tbl_cols=-1):
            print(df)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
