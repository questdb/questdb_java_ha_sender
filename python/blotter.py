#!/usr/bin/env python3
"""A live blotter: poll a QuestDB table (or live view) N times/second and redraw in place.

Builds `select * from TABLE [WHERE ...] limit N` from three params and refreshes a
terminal table as fast as configured. Great for demoing live views.

Params:
  * table          - positional, the table / live view name.
  * --where COND   - optional filter. If the first word (left-trimmed) is not "where",
                     "WHERE " is prepended; otherwise it is used verbatim. Trailing clauses
                     like ORDER BY just ride along (e.g. --where "symbol='EURUSD' order by timestamp").
                     NOT sanitised - demo use only.
  * --limit N      - default -10 (last 10 rows). Clamped to +/-100. |N| rows are shown.

Refresh rate: --rate Hz, default 5, clamped to 20.

Auth / TLS (Enterprise): --token or --username/--password (turns on TLS); --tls forces TLS
with no auth; --tls-verify unsafe_off for self-signed certs.

Usage:
    python blotter.py trades --where "symbol='BTC-USDT'" --limit -20 --rate 10 \
        [--addr host:9000] [--token TOK --tls-verify unsafe_off]

Requires the QWP/egress build of the client (see README.md). Ctrl+C to quit.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import polars as pl

from questdb.ingress import Client


def use_tls(args):
    return bool(args.tls or args.token or (args.username and args.password))


def build_conf(args):
    scheme = "qwpwss" if use_tls(args) else "qwpws"   # Python requires the qwp* scheme
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
    """Redraw in place: home the cursor, clear each line to its end as we write, then clear
    anything left below (so a shorter frame does not leave stale rows)."""
    lines = frame.split("\n")
    sys.stdout.write("\033[H" + "\033[K\n".join(lines) + "\033[K\033[0J")
    sys.stdout.flush()


def main(argv):
    ap = argparse.ArgumentParser(description="Live QuestDB blotter")
    ap.add_argument("table", help="table or live view name")
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

    # Clamp limit magnitude to 100 (keep sign) and rate to 20 Hz.
    if abs(args.limit) > 100:
        args.limit = 100 if args.limit > 0 else -100
        print(f"[warn] --limit clamped to {args.limit}", file=sys.stderr)
    if args.limit == 0:
        print("--limit must be non-zero", file=sys.stderr)
        return 2
    if args.rate <= 0:
        print("--rate must be > 0", file=sys.stderr)
        return 2
    if args.rate > 20:
        args.rate = 20.0
        print("[warn] --rate clamped to 20 Hz", file=sys.stderr)

    conf = build_conf(args)
    sql = build_sql(args.table, args.where, args.limit)
    rows = abs(args.limit)
    interval = 1.0 / args.rate
    scheme = "wss" if use_tls(args) else "ws"

    def render(client):
        t0 = time.perf_counter()
        try:
            df = client.query(sql).to_polars()
            ms = (time.perf_counter() - t0) * 1000.0
            with pl.Config(tbl_rows=rows, tbl_cols=-1, tbl_hide_dataframe_shape=True):
                body = str(df)
            status = (f"{df.height} rows | q={ms:5.1f} ms | {args.rate:g} Hz | "
                      f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
        except Exception as e:  # noqa: BLE001
            body = f"[error] {e}"
            status = f"query failed | {datetime.now(timezone.utc):%H:%M:%S} UTC"
        return f"SQL: {sql}\n     {status}\n{body}"

    if args.once:
        with Client.from_conf(conf) as client:
            print(f"[conf]   {scheme} tls={use_tls(args)} | addr: {args.addr}")
            print(render(client))
        return 0

    sys.stdout.write("\033[2J\033[?25l")  # clear once, hide cursor
    try:
        with Client.from_conf(conf) as client:
            while True:
                t0 = time.perf_counter()
                draw(render(client))
                time.sleep(max(0.0, interval - (time.perf_counter() - t0)))
    except KeyboardInterrupt:
        pass
    finally:
        sys.stdout.write("\033[?25h\n")   # restore cursor
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
