#!/usr/bin/env python3
"""A live blotter: poll a QuestDB table (or live view) N times/second and redraw in place.

Runs `--query` verbatim, or builds `select * from TABLE [WHERE ...] limit N` from
`--table`/`--where`/`--limit`, and refreshes a terminal table as fast as configured.
Great for demoing live views.

Params (provide --table OR --query):
  * --table NAME   - table / live view name; a `select * from NAME [WHERE ...] limit N` is built.
  * --query SQL    - full SQL run verbatim (CTEs, window functions, etc.). When set,
                     --table/--where/--limit are ignored. A trailing ';' is stripped.
  * --where COND   - optional filter (table mode). If the first word (left-trimmed) is not
                     "where", "WHERE " is prepended; otherwise used verbatim. Trailing clauses
                     like ORDER BY ride along. NOT sanitised - demo use only.
  * --limit N      - table mode, default -10 (last 10 rows). Clamped to +/-100.

At most 100 rows are displayed (the query/limit governs how many are fetched).
Refresh rate: --rate Hz, default 5, clamped to 20.

Auth / TLS (Enterprise): --token or --username/--password (turns on TLS); --tls forces TLS
with no auth; --tls-verify unsafe_off for self-signed certs; --token-file/--token-label to
read the token from a file.

Usage:
    python blotter.py --table trades --where "symbol='BTC-USDT'" --limit -20 --rate 10 \
        [--addr host:9000] [--token TOK --tls-verify unsafe_off]
    python blotter.py --query "with x as (select * from core_price_lv where symbol='EURUSD' \
        order by timestamp desc limit 50) select *, avg(bid_price) over (partition by symbol) \
        as avg50 from x" --addr host:9000 --token TOK --tls-verify unsafe_off

Requires the questdb 5.0 client (see README.md). Ctrl+C to quit.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import polars as pl

import questdb


def use_tls(args):
    return bool(args.tls or args.token or (args.username and args.password))


def build_conf(args):
    scheme = "wss" if use_tls(args) else "ws"
    parts = [f"{scheme}::addr={args.addr};"]
    if args.token:
        parts.append(f"token={args.token};")
    elif args.username and args.password:
        parts.append(f"username={args.username};password={args.password};")
    if use_tls(args) and args.tls_verify == "unsafe_off":
        parts.append("tls_verify=unsafe_off;")
    return "".join(parts)


def build_sql(table, where, limit):
    parts = [f"select * from {table}"]
    if where and where.strip():
        c = where.strip()
        first = c.split(None, 1)[0].lower()
        parts.append(c if first == "where" else "WHERE " + c)
    parts.append(f"limit {limit}")
    return " ".join(parts)


def draw(frame):
    """Redraw in place, writing ONLY the lines that changed since the last frame (diff render).
    Over SSH the terminal repaint is the bottleneck, so skipping unchanged lines (borders,
    header, static rows) is what lets the refresh rate go up. Each changed line is positioned
    absolutely, rewritten, then cleared to end-of-line (to erase any longer previous content)."""
    prev = getattr(draw, "_prev", [])
    lines = frame.split("\n")
    out = []
    for i in range(max(len(lines), len(prev))):
        new = lines[i] if i < len(lines) else ""
        old = prev[i] if i < len(prev) else None
        if new != old:
            out.append(f"\033[{i + 1};1H{new}\033[K")   # move to row, write, clear tail
    if out:
        sys.stdout.write("".join(out))
        sys.stdout.flush()
    draw._prev = lines


def main(argv):
    ap = argparse.ArgumentParser(description="Live QuestDB blotter")
    ap.add_argument("--table", default=None, help="table or live view name (built into a select)")
    ap.add_argument("--query", default=None,
                    help="full SQL to run verbatim; when set, --table/--where/--limit are ignored")
    ap.add_argument("--where", default=None, help="filter condition ('WHERE' prepended if absent)")
    ap.add_argument("--limit", type=int, default=-10, help="row limit, default -10, clamped to +/-100")
    ap.add_argument("--rate", type=float, default=5.0, help="refreshes per second, default 5, max 20")
    ap.add_argument("--addr", default="localhost:9000")
    ap.add_argument("--once", action="store_true", help="render a single frame (no ANSI) and exit")
    ap.add_argument("--token", default=None)
    ap.add_argument("--token-file", default=None,
                    help="read the bearer token from this file (keeps it out of the command line)")
    ap.add_argument("--token-label", default=None,
                    help="if --token-file has 'label token' lines, pick the token for this label")
    ap.add_argument("--username", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--tls-verify", choices=["on", "unsafe_off"], default="on")
    args = ap.parse_args(argv)

    # Load the token from a file if requested (keeps the secret off the command line).
    if args.token_file:
        try:
            with open(os.path.expanduser(args.token_file), encoding="utf-8") as fh:
                content = fh.read()
        except OSError as e:
            print(f"[error] cannot read --token-file: {e}", file=sys.stderr)
            return 2
        if args.token_label:
            tok = None
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                parts = line.split(None, 1)
                if len(parts) == 2 and parts[0] == args.token_label:
                    tok = parts[1].strip()
                    break
            if not tok:
                print(f"[error] label '{args.token_label}' not found in {args.token_file}",
                      file=sys.stderr)
                return 2
            args.token = tok
        else:
            args.token = content.strip()

    if args.rate <= 0:
        print("--rate must be > 0", file=sys.stderr)
        return 2
    if args.rate > 20:
        args.rate = 20.0
        print("[warn] --rate clamped to 20 Hz", file=sys.stderr)

    # A full --query runs verbatim (--table/--where/--limit ignored); otherwise build from --table.
    if args.query and args.query.strip():
        sql = args.query.strip().rstrip(";").strip()
    elif args.table:
        if abs(args.limit) > 100:
            args.limit = 100 if args.limit > 0 else -100
            print(f"[warn] --limit clamped to {args.limit}", file=sys.stderr)
        if args.limit == 0:
            print("--limit must be non-zero", file=sys.stderr)
            return 2
        sql = build_sql(args.table, args.where, args.limit)
    else:
        print("provide --table or --query", file=sys.stderr)
        return 2

    conf = build_conf(args)
    sql_line = " ".join(sql.split())   # collapse newlines/whitespace for the one-line header
    interval = 1.0 / args.rate
    scheme = "wss" if use_tls(args) else "ws"

    def render(db):
        t0 = time.perf_counter()
        try:
            df = db.query(sql).to_polars()
            ms = (time.perf_counter() - t0) * 1000.0
            with pl.Config(tbl_rows=min(df.height, 100), tbl_cols=-1, tbl_hide_dataframe_shape=True):
                body = str(df)
            status = (f"{df.height} rows | q={ms:5.1f} ms | {args.rate:g} Hz | "
                      f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
        except Exception as e:  # noqa: BLE001
            body = f"[error] {e}"
            status = f"query failed | {datetime.now(timezone.utc):%H:%M:%S} UTC"
        return f"SQL: {sql_line}\n     {status}\n{body}"

    if args.once:
        with questdb.connect(conf) as db:
            print(f"[conf]   {scheme} tls={use_tls(args)} | addr: {args.addr}")
            print(render(db))
        return 0

    sys.stdout.write("\033[2J\033[?25l")  # clear once, hide cursor
    try:
        with questdb.connect(conf) as db:
            while True:
                t0 = time.perf_counter()
                draw(render(db))
                time.sleep(max(0.0, interval - (time.perf_counter() - t0)))
    except KeyboardInterrupt:
        pass
    finally:
        sys.stdout.write("\033[?25h\n")   # restore cursor
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
