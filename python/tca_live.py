#!/usr/bin/env python3
"""Live TCA dashboard: slippage, markout curves, and local min/max around each fill.

Redraws once a second. Every panel is a rolling window, so memory and query cost stay
flat however long it runs.

The point of the layout is the DELAY each panel needs. Anything that looks forward in
time cannot be evaluated until that forward window has actually elapsed, so each query
shifts its window back by exactly its own forward horizon:

    panel            window              forward horizon   delay
    slippage         $now-1m .. $now      none              none, fully live
    markout +-1m     $now-2m .. $now-1m   +1m               1m
    markout past     $now-1m .. $now      none              none, fully live
    min/max +-10s    $now-70s .. $now-10s +10s              10s

The two markout panels side by side are the interesting comparison: the delayed one
shows the complete curve either side of the fill, the live one shows only what has
already happened. Same data, different tradeoff between completeness and latency.

    ILP_TOKEN=... python tca_live.py --addr host:9000
    python tca_live.py --conf "ws::addr=localhost:9000;"

Needs the questdb 5.0 client plus polars and pyarrow.
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone

import polars as pl
import questdb

# ---------------------------------------------------------------- queries
# Kept verbatim as authored, including the window literals, so what the dashboard runs
# is exactly what you would paste into the console. All reduction happens in polars.

SLIPPAGE_SQL = """
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
WHERE t.timestamp IN '$now-1m..$now'{sym}
ORDER BY t.timestamp
"""

# Markout body shared by both panels; only RANGE and the window differ.
#
# Grouped by ecn and horizon only. That is 52 rows per tick (4 ecns x 13 offsets) rather than
# 20,111, and the average the server computes is the one displayed. Grouping by symbol, ecn,
# counterparty and passive then folding three of them away in polars required a weighted mean,
# sum(avg * n) / sum(n), so a counterparty with 3 fills would not count the same as one with
# 3,000. Letting the server group at the displayed granularity deletes that code entirely.
# Two markout statements per panel, deliberately.
#
# _MARKOUT_RAW_SQL is grouped by symbol, ecn, passive and horizon_sec, carrying n,
# avg_markout_bps and total_pnl. counterparty is deliberately NOT a key: with 1,061 distinct
# counterparties and ~1,600 fills a minute, including it put 1 fill in nearly every cell
# (measured busiest cell: n=2), so avg() and sum() were aggregating a single observation and
# the numbers were noise. Dropping it collapses ~10,800 cells to ~1,700 and gives each a real
# sample.
#
# _MARKOUT_PIVOT_SQL is the same calculation grouped only by ecn and horizon_sec, ~52 rows, so
# the pivot is a pure reshape of a server-side aggregate. Folding the four-key result down in
# polars instead would need a weighted mean, sum(avg * n) / sum(n), to stop a counterparty with
# 3 fills counting the same as one with 3,000. Asking the server for the granularity actually
# displayed removes that arithmetic and the chance of getting it wrong.

_MARKOUT_BODY = """
    count() AS n,
    avg(
        CASE t.side
            WHEN 'buy'  THEN ((m.best_bid + m.best_ask) / 2 - t.price)
                             / t.price * 10000
            WHEN 'sell' THEN (t.price - (m.best_bid + m.best_ask) / 2)
                             / t.price * 10000
        END
    ) AS avg_markout_bps,
    sum(
        CASE t.side
            WHEN 'buy'  THEN ((m.best_bid + m.best_ask) / 2 - t.price)
                             * t.quantity
            WHEN 'sell' THEN (t.price - (m.best_bid + m.best_ask) / 2)
                             * t.quantity
        END
    ) AS total_pnl
FROM fx_trades t
HORIZON JOIN market_data m ON (symbol)
    RANGE {rng} AS h
WHERE t.timestamp IN '{window}'{sym}
"""

_MARKOUT_RAW_SQL = ("""
SELECT
    t.symbol,
    t.ecn,
    t.passive,
    h.offset / 1000000000 AS horizon_sec,"""
    + _MARKOUT_BODY
    + """GROUP BY t.symbol, t.ecn, t.passive, horizon_sec
ORDER BY t.symbol, t.ecn, t.passive, horizon_sec
""")

_MARKOUT_PIVOT_SQL = ("""
SELECT
    t.ecn,
    h.offset / 1000000000 AS horizon_sec,"""
    + _MARKOUT_BODY
    + """GROUP BY t.ecn, horizon_sec
ORDER BY t.ecn, horizon_sec
""")

# Per panel: the full curve is shifted a minute back so its +1m side is complete; the past-only
# curve has no forward horizon and runs right up to now.
_MARKOUT_RANGES = {
    "markout-full": ("FROM -1m TO 1m STEP 10s", "$now-2m..$now-1m"),
    "markout-past": ("FROM -1m TO 0 STEP 10s", "$now-1m..$now"),
}


MINMAX_SQL = """
SELECT
    t.symbol,
    t.timestamp,
    t.side,
    t.price,
    min(p.ask_price) AS min_ask,
    max(p.bid_price) AS max_bid
FROM fx_trades t
WINDOW JOIN core_price p
    ON (symbol)
    RANGE BETWEEN 10 seconds PRECEDING AND 10 seconds FOLLOWING
    EXCLUDE PREVAILING
WHERE t.timestamp IN '$now-70s..$now-10s'{sym}
"""


def build_sql(symbol):
    """Statements per panel. --symbol is applied in SQL, not filtered client-side.

    Markout panels map to {"raw": ..., "pivot": ...}; the other panels to a single string.
    """
    sym = f"\n    AND t.symbol = '{symbol}'" if symbol else ""
    out = {
        "slippage": SLIPPAGE_SQL.format(sym=sym),
        "minmax": MINMAX_SQL.format(sym=sym),
    }
    for panel, (rng, window) in _MARKOUT_RANGES.items():
        out[panel] = {
            "raw": _MARKOUT_RAW_SQL.format(rng=rng, window=window, sym=sym),
            "pivot": _MARKOUT_PIVOT_SQL.format(rng=rng, window=window, sym=sym),
        }
    return out


PANELS = ("slippage", "markout-full", "markout-past", "minmax")


# ---------------------------------------------------------------- rendering
def build_conf(args):
    if args.conf:
        return args.conf
    tls = bool(args.token or (args.username and args.password))
    parts = [("wss" if tls else "ws") + "::addr=" + args.addr + ";"]
    if args.token:
        parts.append("token=" + args.token + ";")
    elif args.username and args.password:
        parts.append("username=" + args.username + ";password=" + args.password + ";")
    if tls and args.tls_verify == "unsafe_off":
        parts.append("tls_verify=unsafe_off;")
    return "".join(parts)


def markout_curve(df):
    """Pivot the horizon offsets across columns, which is how a markout curve reads.

    The server already grouped at the displayed granularity, so there is nothing to
    aggregate here: one row per ecn, one column per offset.
    """
    if df.height == 0:
        return df
    wide = df.pivot(on="horizon_sec", index="ecn", values="avg_markout_bps",
                    aggregate_function=None)
    # Group order is not curve order; sort the offsets numerically so the row reads left to
    # right from the most negative to the most positive.
    horizon_cols = sorted((c for c in wide.columns if c != "ecn"), key=lambda c: int(c))
    # n is per (ecn, horizon) and every fill matches once per offset, so max across horizons
    # is the fill count. sum would count fill-by-horizon pairs and, with 13 offsets, report
    # 6,084 fills for an ecn that had 365.
    fills = df.group_by("ecn").agg(pl.col("n").max().alias("fills"))
    return (wide.select(["ecn"] + horizon_cols)
                .join(fills, on="ecn", how="left")
                .sort("fills", descending=True))


def width_cfg(args):
    """polars Config kwargs for table width: only pin it when --width was given.

    Returning {} rather than a 0 matters. tbl_width_chars=0 is not "auto", it is a zero-width
    table: polars then squeezes every column to ~3 characters and a nanosecond timestamp
    unfolds over twenty lines. Leaving the key out lets polars size to the terminal.
    """
    return {"tbl_width_chars": args.width} if args.width > 0 else {}


def fixed(df, specs):
    """Render selected columns as pre-padded strings so the table width stops changing.

    polars sizes each column to its widest CURRENT value, so a window without a Currenex fill,
    or whose numbers all happen to be small, redraws narrower and every border shifts. There is
    no config for this: tbl_width_chars only truncates, it never pads (verified). Formatting to
    a fixed width here is the only thing that holds the layout still.

    specs maps column -> format spec, e.g. "<8" for a label or ">7.2f" for a right-aligned
    number. Widths must match the real maxima; padding wider than the content pushes the table
    past the terminal and polars then truncates every cell to a single ellipsis.
    """
    exprs = []
    for col, spec in specs.items():
        if col not in df.columns:
            continue
        width = int("".join(c for c in spec.split(".")[0] if c.isdigit()) or 0)
        blank = " " * width
        exprs.append(
            pl.col(col)
              .map_elements(lambda v, _s=spec, _b=blank: _b if v is None else format(v, _s),
                            return_dtype=pl.Utf8)
              .alias(col)
        )
    return df.with_columns(exprs) if exprs else df


# Widest block seen per key.# Widest block seen per key. polars sizes a column to its widest VALUE, so when
# no Currenex fill lands in the window the ecn column loses a character and everything to the
# right of it jumps sideways. Remembering the high-water mark per panel and padding to it keeps
# the right-hand table anchored, at the cost of never shrinking back during a session.
_LEFT_WIDTH = {}


def side_by_side(left, right, gap=3, key=None, floor=0):
    """Place two rendered blocks next to each other, line by line.

    polars pads every line of a table to the same width, so the left block only needs its own
    max width to line up. The shorter block is padded with blanks so the columns stay aligned
    all the way down. `key` opts the panel into the sticky width above, `floor` pins a minimum
    so the right half is anchored from the very first tick rather than after the sticky width
    has converged (measured: ~15 ticks, 85 -> 87 chars, then stable).
    """
    lcol = left.split("\n")
    rcol = right.split("\n")
    width = max((len(x) for x in lcol), default=0)
    if key is not None:
        width = _LEFT_WIDTH[key] = max(width, _LEFT_WIDTH.get(key, 0), floor)
    pad = " " * gap
    rows = max(len(lcol), len(rcol))
    out = []
    for i in range(rows):
        l = lcol[i] if i < len(lcol) else ""
        r = rcol[i] if i < len(rcol) else ""
        out.append((l.ljust(width) + pad + r).rstrip())
    return "\n".join(out)


def render(frames, timings, args, tick):
    out = []
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    scope = args.symbol if args.symbol else "all symbols"
    out.append(f"TCA live  {now}Z   tick {tick}   refresh {args.interval_ms}ms   {scope}")
    out.append("")

    for name in args.panels:
        df = frames.get(name)
        ms = timings.get(name)
        took = f"{ms:.0f}ms" if ms is not None else "n/a"
        if df is None:
            out.append(f"--- {name}: query failed ({took}) ---")
            out.append("")
            continue

        if name == "slippage":
            out.append(f"--- slippage, last 1m, live  [{df.height:,} fills, {took}] ---")
            if df.height == 0:
                out.append("(no fills in window)")
            else:
                if args.view in ("raw", "both"):
                    out.append(f"  raw, the query as authored, one row per fill "
                               f"(last {min(args.raw_rows, df.height)} of {df.height:,}):")
                    padded = fixed(df.tail(args.raw_rows), {
                        "ecn": "<8", "side": "<4", "passive": "<5",
                        "price": ">9.5f", "quantity": ">13.2f",
                        "best_bid": ">9.5f", "best_ask": ">9.5f", "mid": ">9.5f",
                        "spread": ">7.5f", "slippage_bps": ">8.4f",
                        "slippage_vs_tob_bps": ">8.4f",
                    })
                    with pl.Config(tbl_rows=args.raw_rows, tbl_cols=-1,
                                   tbl_hide_dataframe_shape=True, tbl_hide_column_data_types=True, **width_cfg(args)):
                        out.append(str(padded))
                if args.view in ("pivot", "both"):
                    # A plain mean is correct here: every row is exactly one fill, so there is
                    # no weighting trap of the kind the markout grouping had.
                    out.append("  summarised by ecn (mean over fills, computed client-side):")
                    summary = (df.group_by("ecn")
                                 .agg(pl.len().alias("fills"),
                                      pl.col("slippage_bps").mean().alias("avg_bps"),
                                      pl.col("slippage_vs_tob_bps").mean().alias("avg_tob_bps"))
                                 .sort("fills", descending=True))
                    summary = fixed(summary, {"ecn": "<8", "fills": ">5",
                                              "avg_bps": ">7.3f", "avg_tob_bps": ">7.3f"})
                    with pl.Config(tbl_rows=args.rows,
                                   tbl_hide_dataframe_shape=True, tbl_hide_column_data_types=True, **width_cfg(args)):
                        out.append(str(summary))

        elif name in ("markout-full", "markout-past"):
            label = ("markout -1m..+1m, delayed 1m so the future is complete"
                     if name == "markout-full"
                     else "markout -1m..0, live, past horizons only")
            out.append(f"--- {label}  [{took}] ---")
            wanted = [v for v in ("raw", "pivot") if args.view in (v, "both")]
            blocks = []
            for variant, vdf in zip(wanted, df):
                if vdf is None:
                    blocks.append((variant, f"  {variant}: query failed"))
                    continue
                if vdf.height == 0:
                    blocks.append((variant, f"  {variant}: (no fills in window)"))
                    continue
                if variant == "raw":
                    if args.raw_sort == "query":
                        shown, how = vdf, "the query's own order"
                    elif args.raw_sort == "n":
                        shown, how = vdf.sort("n", descending=True), "busiest cells first"
                    else:
                        shown = vdf.sample(min(args.raw_rows, vdf.height), shuffle=True)
                        how = "random sample"
                    head = (f"  raw, grouped by symbol/ecn/passive/horizon ({how}, "
                            f"{min(args.raw_rows, vdf.height)} of {vdf.height:,}):")

                    padded = fixed(shown.head(args.raw_rows), {
                        "ecn": "<8", "n": ">5",
                        "avg_markout_bps": f">9.{args.precision}f",
                        "total_pnl": f">14.{args.precision}f",
                    })
                    with pl.Config(tbl_rows=args.raw_rows, tbl_cols=-1, float_precision=2,
                                   tbl_hide_dataframe_shape=True,
                                   tbl_hide_column_data_types=True, **width_cfg(args)):
                        blocks.append((variant, head + "\n" + str(padded)))
                else:
                    head = (f"  pivoted, a second query by ecn/horizon "
                            f"({vdf.height} rows), avg_markout_bps:")
                    curve = markout_curve(vdf)
                    specs = {c: f">8.{args.precision}f" for c in curve.columns
                             if c not in ("ecn", "fills")}
                    specs.update({"ecn": "<8", "fills": ">5"})
                    with pl.Config(tbl_rows=args.rows, tbl_cols=-1, float_precision=2,
                                   tbl_hide_dataframe_shape=True,
                                   tbl_hide_column_data_types=True, **width_cfg(args)):
                        blocks.append((variant, head + "\n" + str(fixed(curve, specs))))

            if args.layout == "side" and len(blocks) == 2:
                out.append(side_by_side(blocks[0][1], blocks[1][1], key=name,
                                        floor=args.left_width))
            else:
                for _, block in blocks:
                    out.append(block)

        elif name == "minmax":
            out.append(f"--- min/max +-10s around fill, delayed 10s  "
                       f"[{df.height:,} fills, {took}] ---")
            # No transformation here: this IS the query output, just the newest rows of it.
            out.append(f"  raw, the query as authored "
                       f"(last {min(args.minmax_rows, df.height)} of {df.height:,}):")
            if df.height:
                padded = fixed(df.tail(args.minmax_rows), {
                    "side": "<4", "price": ">10.5f",
                    "min_ask": ">10.5f", "max_bid": ">10.5f",
                })
                with pl.Config(tbl_rows=args.rows, tbl_cols=-1,
                               tbl_hide_dataframe_shape=True, tbl_hide_column_data_types=True,
                               **width_cfg(args)):
                    out.append(str(padded))
            else:
                out.append("(no fills in window)")

    # Repaint in one write: separate prints let the terminal show a half-drawn frame.
    sys.stdout.write("\033[H\033[2J" + "\n".join(out) + "\n")
    sys.stdout.flush()


# ---------------------------------------------------------------- main
def main(argv):
    ap = argparse.ArgumentParser(description="Live TCA dashboard over QWP")
    ap.add_argument("--addr", default="localhost:9000", help="host:port")
    ap.add_argument("--conf", default=None,
                    help="full connect string; overrides --addr and the auth flags")
    ap.add_argument("--token", default=os.environ.get("ILP_TOKEN") or None)
    ap.add_argument("--username", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--tls-verify", choices=["on", "unsafe_off"], default="unsafe_off")
    ap.add_argument("--interval-ms", type=int, default=1000,
                    help="milliseconds between redraws; default 1000. Named to match the Java "
                         "sender's --probe-interval-ms / --delay-ms")
    ap.add_argument("--symbol", default=None,
                    help="restrict every panel to one symbol, e.g. EURUSD. Applied in SQL, so "
                         "the server does the filtering")
    ap.add_argument("--rows", type=int, default=8, help="max rows per panel")
    ap.add_argument("--layout", choices=["side", "stacked"], default="side",
                    help="with --view both, place raw and pivoted next to each other rather "
                         "than one above the other, to avoid vertical scrolling; needs a wide "
                         "terminal, see the width the header reports")
    ap.add_argument("--precision", type=int, default=2,
                    help="decimals for the markout figures. Values render at a FIXED width, so "
                         "-3.25 and -12.40 occupy the same space and the table stops resizing "
                         "as a value crosses a power of ten; default 2")
    ap.add_argument("--left-width", type=int, default=0,
                    help="minimum width for the left half in a side-by-side panel. Default 0 "
                         "lets it find its own high-water mark, which settles after ~15 ticks; "
                         "set ~90 to anchor the right half from the first frame")
    ap.add_argument("--minmax-rows", type=int, default=5,
                    help="rows for the min/max panel, the tallest table; default 5")
    ap.add_argument("--view", choices=["pivot", "raw", "both"], default="both",
                    help="the query's own long-format output, the derived/pivoted view, or "
                         "both; default both, rendered side by side so it still fits")
    ap.add_argument("--raw-sort", choices=["sample", "n", "query"], default="sample",
                    help="which raw markout rows to show. 'sample' takes a fresh random draw "
                         "each tick; 'n' the busiest cells; 'query' the statement's own ORDER BY. "
                         "Default sample, because the authored grouping puts ~1 fill in every "
                         "(symbol, ecn, counterparty, passive, horizon) cell (measured max n=2), "
                         "so both other orderings keep landing on the same alphabetically first "
                         "rows and look frozen while the data moves underneath")
    ap.add_argument("--raw-rows", type=int, default=6,
                    help="rows of raw markout output to show when --view includes raw; kept "
                         "small because the full result is 28 rows past-only and 52 full")
    ap.add_argument("--width", type=int, default=0,
                    help="force a table drawing width in chars. Default 0 leaves it to polars, "
                         "which sizes to the terminal. Note this is the width tables are DRAWN "
                         "at, not a cap: setting it pads narrow tables out to that width too")
    ap.add_argument("--panels", default=",".join(PANELS),
                    help="comma-separated subset of: " + ", ".join(PANELS))
    args = ap.parse_args(argv)

    args.panels = [p.strip() for p in args.panels.split(",") if p.strip()]
    bad = [p for p in args.panels if p not in PANELS]
    if bad:
        print("unknown panel(s): " + ", ".join(bad), file=sys.stderr)
        return 2

    sql = build_sql(args.symbol)

    conf = build_conf(args)
    tick = 0
    with questdb.connect(conf) as db:
        # One pinned connection for the whole session: these are several queries in a
        # row, which is exactly what a reader lease is for.
        with db.reader() as reader:
            while True:
                started = time.monotonic()
                frames, timings = {}, {}
                for name in args.panels:
                    spec = sql[name]
                    # Markout panels carry {"raw","pivot"}; run only what --view will draw so a
                    # --view pivot run does not pay for the 20,000-row authored query.
                    wanted = ([spec] if isinstance(spec, str)
                              else [spec[v] for v in ("raw", "pivot")
                                    if args.view in (v, "both")])
                    t0 = time.monotonic()
                    got = []
                    for stmt in wanted:
                        try:
                            got.append(reader.query(stmt.strip()).to_polars())
                        except Exception as e:  # noqa: BLE001
                            # Keep drawing. A dashboard that dies on one bad tick is worse
                            # than one that shows which panel is failing and why.
                            got.append(None)
                            print(f"[{name}] {e}", file=sys.stderr)
                    frames[name] = got[0] if isinstance(spec, str) else got
                    timings[name] = (time.monotonic() - t0) * 1000.0
                tick += 1
                render(frames, timings, args, tick)

                # Hold the cadence, and say so when the queries cannot keep up rather
                # than silently drifting.
                spent_ms = (time.monotonic() - started) * 1000.0
                if spent_ms < args.interval_ms:
                    time.sleep((args.interval_ms - spent_ms) / 1000.0)
                else:
                    print(f"[warn] tick took {spent_ms:.0f}ms, longer than the "
                          f"{args.interval_ms}ms refresh", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        sys.exit(130)
