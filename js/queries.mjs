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
