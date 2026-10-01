// The SQL, shared by every transport. Kept byte-identical to python/tca_live.py so the two
// dashboards are comparable, and so what this serves is what you would paste into the console.
//
// Each panel shifts its window back by exactly its own forward horizon, because anything
// looking forward cannot be evaluated until that window has elapsed:
//
//   panel          window                forward   delay
//   slippage       $now-1m .. $now       none      live
//   markout +-1m   $now-2m .. $now-1m    +1m       1m
//   markout past   $now-1m .. $now       none      live
//   min/max +-10s  $now-70s .. $now-10s  +10s      10s

const symFilter = (symbol) => (symbol ? `\n    AND t.symbol = '${symbol}'` : "");

export const slippageSql = (symbol) => `
SELECT
    t.timestamp, t.symbol, t.ecn, t.side, t.price,
    (m.best_bid + m.best_ask) / 2 AS mid,
    CASE t.side
        WHEN 'buy'  THEN (t.price - (m.best_bid + m.best_ask) / 2)
                         / ((m.best_bid + m.best_ask) / 2) * 10000
        WHEN 'sell' THEN ((m.best_bid + m.best_ask) / 2 - t.price)
                         / ((m.best_bid + m.best_ask) / 2) * 10000
    END AS slippage_bps
FROM fx_trades t
ASOF JOIN market_data m ON (symbol)
WHERE t.timestamp IN '$now-1m..$now'${symFilter(symbol)}
ORDER BY t.timestamp`;

// Grouped by ecn and horizon only: the granularity actually displayed, so the average the
// server computes is the one shown and nothing is re-aggregated in JavaScript.
const markoutSql = (range, window, symbol) => `
SELECT
    t.ecn,
    h.offset / 1000000000 AS horizon_sec,
    count() AS n,
    avg(
        CASE t.side
            WHEN 'buy'  THEN ((m.best_bid + m.best_ask) / 2 - t.price) / t.price * 10000
            WHEN 'sell' THEN (t.price - (m.best_bid + m.best_ask) / 2) / t.price * 10000
        END
    ) AS avg_markout_bps,
    sum(
        CASE t.side
            WHEN 'buy'  THEN ((m.best_bid + m.best_ask) / 2 - t.price) * t.quantity
            WHEN 'sell' THEN (t.price - (m.best_bid + m.best_ask) / 2) * t.quantity
        END
    ) AS total_pnl
FROM fx_trades t
HORIZON JOIN market_data m ON (symbol)
    RANGE ${range} AS h
WHERE t.timestamp IN '${window}'${symFilter(symbol)}
GROUP BY t.ecn, horizon_sec
ORDER BY t.ecn, horizon_sec`;

export const markoutFullSql = (symbol) =>
  markoutSql("FROM -1m TO 1m STEP 10s", "$now-2m..$now-1m", symbol);

export const markoutPastSql = (symbol) =>
  markoutSql("FROM -1m TO 0 STEP 10s", "$now-1m..$now", symbol);

export const minMaxSql = (symbol) => `
SELECT
    t.symbol, t.timestamp, t.side, t.price,
    min(p.ask_price) AS min_ask,
    max(p.bid_price) AS max_bid
FROM fx_trades t
WINDOW JOIN core_price p
    ON (symbol)
    RANGE BETWEEN 10 seconds PRECEDING AND 10 seconds FOLLOWING
    EXCLUDE PREVAILING
WHERE t.timestamp IN '$now-70s..$now-10s'${symFilter(symbol)}`;

export const PANELS = ["slippage", "markout-full", "markout-past", "minmax"];

export const sqlFor = (panel, symbol) => ({
  "slippage": slippageSql,
  "markout-full": markoutFullSql,
  "markout-past": markoutPastSql,
  "minmax": minMaxSql,
}[panel](symbol));

// Symbols currently trading, for the filter dropdown. LATEST ON collapses to one row per
// symbol, and the 30s window keeps it to instruments actually active right now rather than
// every symbol the table has ever seen.
export const symbolsSql = () => `
SELECT symbol FROM fx_trades
WHERE timestamp IN '$now-30s..$now'
LATEST ON timestamp PARTITION BY symbol`;

// === Streaming scan ===================================================================
//
// LIMIT -N is the point of the demo: it asks for the LAST N rows, which QuestDB answers by
// skipping whole page frames on partition metadata rather than reading them. The scan then
// streams back in batches, so the client's memory is flat regardless of N.
export const SCAN_TABLES = ["core_price", "market_data", "fx_trades"];

/**
 * Split the last `rows` rows into `chunks` equal row-count slices, oldest first.
 *
 * `LIMIT -m, -n` takes the last m rows then drops the last n of them, i.e. the half-open
 * range [-m, -n), so consecutive slices tile the range with no gap and no overlap. The
 * newest slice has n == 0, which is the documented `LIMIT -n, 0` == `LIMIT -n` form. The
 * bounds are arithmetic on `rows` alone, so no preliminary count query is needed.
 *
 * Two reasons to slice rather than issue one statement. QuestDB applies its own
 * `query.timeout` server-side (60s on the instance this was built against), which one
 * 200M-row statement blows straight through; and slices can be read concurrently over
 * separate connections, which is what makes a remote scan bearable. This mirrors
 * python/read_bench.py --split rows.
 */
// Projections the scan can request. "no-timestamp" exists because the designated timestamp
// is GORILLA-encoded on the wire: delta-of-delta bit packing, which cannot be handed over as
// a view and has to be unpacked value by value through a bit reader using BigInt arithmetic,
// eagerly, before a batch is delivered. Every other fixed-width column is a zero-copy view.
// Profiling a scan put ~60% of active CPU in readTimestampView plus its bit reader, with the
// BigInt allocation driving most of the GC on top. Dropping that one column is therefore the
// single biggest lever on read throughput, at the cost of not seeing the timestamp.
export const SCAN_PROJECTIONS = {
  "all": () => "*",
  // Keeps every column AND the timestamp's value, but casts it to LONG so the wire column is
  // a plain fixed-width one. The decoder only considers Gorilla for DATE/TIMESTAMP/
  // TIMESTAMP_NANOS; a LONG goes through readFixedView, which is a zero-copy view over the
  // frame. Same information, same row count, none of the per-value BigInt bit unpacking.
  // Costs 8 uncompressed bytes per row on the wire, which is the right trade on a LAN and
  // the wrong one on a thin WAN link.
  "epoch-long": (table) => SCAN_COLUMNS[table]
    .map((c) => (c === "timestamp" ? "cast(timestamp as long) AS timestamp" : c)).join(", "),
  "no-timestamp": (table) => SCAN_COLUMNS[table].filter((c) => c !== "timestamp").join(", "),
};

const SCAN_COLUMNS = {
  core_price: ["timestamp", "symbol", "ecn", "bid_price", "bid_volume", "ask_price",
               "ask_volume", "reason", "indicator1", "indicator2"],
  market_data: null,   // resolved as "*" only; see SCAN_PROJECTIONS usage
  fx_trades: ["timestamp", "symbol", "ecn", "trade_id", "side", "passive", "price",
              "quantity", "counterparty", "order_id"],
};

export const scanChunks = (table, rows, chunks = 1, projection = "all") => {
  if (!SCAN_TABLES.includes(table)) throw new Error(`unknown table: ${table}`);
  if (!(projection in SCAN_PROJECTIONS)) throw new Error(`unknown projection: ${projection}`);
  const cols = SCAN_COLUMNS[table] ? SCAN_PROJECTIONS[projection](table) : "*";
  const limit = Math.max(1, Math.trunc(rows));
  const n = Math.max(1, Math.trunc(chunks));
  const out = [];
  for (let i = 0; i < n; i++) {
    const lo = limit - Math.floor((limit * i) / n);        // rows from the end, inclusive
    const hi = limit - Math.floor((limit * (i + 1)) / n);  // rows from the end, exclusive
    if (lo <= hi) continue;                                // empty slice: chunks > rows
    out.push(hi === 0
      ? `SELECT ${cols} FROM ${table} LIMIT -${lo}`
      : `SELECT ${cols} FROM ${table} LIMIT -${lo}, -${hi}`);
  }
  return out;
};

// === OHLC =============================================================================
//
// Candles are built in the database, not the browser: SAMPLE BY turns every trade in the
// window into one bar per interval, so what crosses the wire is a few thousand bars rather
// than the millions of trades behind them. That is what keeps the chart snappy on a window
// covering hours of trading.

/** Bar intervals offered, mapped to their width in seconds. */
export const OHLC_INTERVALS = {
  "1s": 1, "5s": 5, "15s": 15, "1m": 60, "5m": 300, "15m": 900, "1h": 3600,
};

/** Lookback windows offered, mapped to their span in seconds. */
export const OHLC_LOOKBACKS = {
  "1m": 60, "2m": 120, "5m": 300, "30m": 1800, "2h": 7200, "6h": 21600, "24h": 86400,
  "7d": 604800,
};

/**
 * Where the data actually ends.
 *
 * The window is anchored to the newest row rather than to $now, because a demo instance
 * whose ingestion has paused still has hours of perfectly good history, and anchoring to
 * $now shows an empty chart the moment the writer stops. When ingestion IS live the two are
 * the same thing.
 */
export const ohlcLastRowSql = (symbol) => `
SELECT max(timestamp) AS last_row
FROM fx_trades${symbol ? `\nWHERE symbol = '${symbol}'` : ""}`;

// Explicit >= / <= rather than the IN 'a..b' interval form used by the TCA panels: that
// shorthand parses the $now-relative literals those queries use, but rejects an absolute
// ISO timestamp pair outright ("Invalid date"). These windows are absolute, being anchored
// to the newest row, so they are expressed as comparisons.
const window = (fromIso, toIso) =>
  `timestamp >= '${fromIso}' AND timestamp <= '${toIso}'`;

/**
 * Symbols traded in the window, busiest first.
 *
 * Ordered by activity rather than by name so the tab can open on an instrument that actually
 * has a candle in most buckets. Opening on whatever sorts first alphabetically gave a chart
 * full of gaps, which says nothing about either the data or the database.
 */
export const ohlcSymbolsSql = (fromIso, toIso) => `
SELECT symbol, count() AS trades
FROM fx_trades
WHERE ${window(fromIso, toIso)}
GROUP BY symbol
ORDER BY trades DESC`;

/**
 * One bar per interval, plus the volume and the per-bar VWAP.
 *
 * `vwap` here is the volume-weighted price WITHIN the bar. The running session VWAP drawn
 * over the candles is accumulated from these on the client, which needs no extra SQL: the
 * numerator is vwap * volume, and both are already here.
 */
export const ohlcSql = ({ symbol, interval, fromIso, toIso }) => {
  if (!(interval in OHLC_INTERVALS)) throw new Error(`unknown interval: ${interval}`);
  if (!symbol) throw new Error("a symbol is required");
  return `
SELECT
    timestamp,
    first(price) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price) AS close,
    sum(quantity) AS volume,
    sum(price * quantity) / sum(quantity) AS vwap,
    count() AS trades
FROM fx_trades
WHERE symbol = '${symbol}'
    AND ${window(fromIso, toIso)}
SAMPLE BY ${interval}
ORDER BY timestamp`;
};
