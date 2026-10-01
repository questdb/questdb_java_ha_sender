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
export const scanChunks = (table, rows, chunks = 1) => {
  if (!SCAN_TABLES.includes(table)) throw new Error(`unknown table: ${table}`);
  const limit = Math.max(1, Math.trunc(rows));
  const n = Math.max(1, Math.trunc(chunks));
  const out = [];
  for (let i = 0; i < n; i++) {
    const lo = limit - Math.floor((limit * i) / n);        // rows from the end, inclusive
    const hi = limit - Math.floor((limit * (i + 1)) / n);  // rows from the end, exclusive
    if (lo <= hi) continue;                                // empty slice: chunks > rows
    out.push(hi === 0
      ? `SELECT * FROM ${table} LIMIT -${lo}`
      : `SELECT * FROM ${table} LIMIT -${lo}, -${hi}`);
  }
  return out;
};
