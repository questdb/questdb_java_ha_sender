# js — QWP demos in a browser

Three tabs over one QWP connection: live TCA panels, a streaming scan, and an OHLC
chart. `node server.mjs --help` lists every flag; this file covers setup and the
things that are not obvious from the flag list.

## Why there is a server at all

QuestDB accepts a browser WebSocket upgrade only when the request's `Origin` matches
its `Host`, which blocks cross-site WebSocket hijacking and answers a cross-origin
upgrade with HTTP 400. A different port is a different origin, so a page on `:8080`
cannot open a QWP socket to QuestDB on `:9000` however local it is. (CORS never
applies — WebSocket upgrades are not subject to it.)

So Node holds the QWP connection and the page talks to Node. `data.mjs` is the only
module that knows QuestDB exists, so when `@questdb/browser-client` is published and
the server carries the upgrade patch, the query moves into the page and nothing else
changes.

## Install

Two dependencies: the QWP client, and TradingView Lightweight Charts for the OHLC
tab. No framework, no bundler, no build step — the page is plain ES modules.

The client is not on npm yet, so build it from source:

```sh
git clone <nodejs-questdb-client> && cd nodejs-questdb-client
pnpm install                                   # needs Node >= 20.18.1
pnpm --filter @questdb/nodejs-client build
```

`pnpm install` at the **root** is required first: `bunchee`, `rollup` and
`typescript` are devDependencies of the workspace, so building from inside
`packages/nodejs-client` will not resolve them.

Then point this app at wherever you cloned it and install:

```sh
cd js
npm pkg set dependencies.@questdb/nodejs-client=file:/path/to/nodejs-questdb-client/packages/nodejs-client
npm install
```

That edits one line of `package.json`; it is a local diff, not something to commit.

## Run

```sh
npm start                                      # localhost:9000, http://localhost:8080
npm start -- --scheme wss --addrs a:9000,b:9000,c:9000 --token-file ~/token.txt
```

Pass the token with `--token-file`, never on the command line: argv is readable
through `ps`, and the configuration string is logged at startup (the token is
replaced with `***`).

`--addrs` takes the comma-separated list the client resolves with ordered failover,
so naming every node is how read HA is exercised rather than merely claimed.

## Tabs

**TCA live** — slippage, two markout panels and a local min/max, the same SQL as
`python/tca_live.py`. Each panel shows the statement it is running, fetched from the
same builder the query path uses so the two cannot drift.

**Streaming scan** — reads the last N rows of a table and reports only how far it has
got, so neither side holds the result. 200M rows of `core_price` run at ~826k rows/s
against a remote cluster with the server at 147MB RSS: memory tracks reader count,
not rows read.

**OHLC & VWAP** — candles with a session VWAP and a volume histogram, zoom and pan by
wheel, drag or the buttons, double-click or Fit to reset. Candles and bid/ask both
arrive on one pushed stream; the bid and ask lines move between candle boundaries, so
there is motion inside the second. Defaults to 1s bars over 1m, chosen for bar width
rather than coverage.

## Things worth knowing

**Compression is not free.** `--compression zstd` is worth ~1.9x over a WAN, where
bandwidth is the constraint, and costs 2.9x on a LAN, where it is not: the client's
zstd decoder is pure JavaScript on the event-loop thread.

**Readers do not scale past a core.** Parallel readers overlap I/O, not CPU — the
client decodes in JavaScript on one thread. Throughput plateaus around 4 readers
(measured 3.10M rows/s at 4 and 3.11M at 16).

**The timestamp column dominates the scan.** The designated timestamp is Gorilla
encoded, so it cannot be handed over as a view and is unpacked per value with BigInt
arithmetic. Every other fixed-width column is zero copy. The scan tab's `columns`
control trades this off: `all` 2.5M rows/s, `ts as epoch long` 6.3M, `no timestamp`
8.7M.

**A chart cannot tick faster than the data becomes visible, and that is a property of
the WRITER.** With a sender on the default `auto_flush_interval=1000`, the newest
visible row advances in ~1.0s steps however fast rows are produced, and polling
faster only re-reads identical data: measured 7 distinct values across 43 reads in
6s. With QWP ingestion flushing every 50ms, the same measurement gave 43 distinct
values in 43 reads — the limit became the reader's own round trip. Check this before
blaming the chart; it is one query in a loop.

**Live updates are pushed, not polled, and that is not an optimisation.** A browser
throttles `setInterval` to 1Hz in a background tab but does not throttle an incoming
stream: measured in one page at one moment, timer-driven bars ran at 1.0/s while
stream-driven quotes ran at 5.1/s. Pushing also means a browser far from the server
sees updates delayed by a constant rather than rate-limited by its round trip. Both
bars and quotes arrive on one SSE connection, and the poll-rate control sets how
often the SERVER polls QuestDB.

**Only closed bars are immutable.** The bar currently being formed changes with every
trade that lands inside it, so it is redrawn at the stream rate. Refreshing bars
"once per bar" looks correct and pins the newest candle to the bar width instead.

**How alive it looks is mostly bar WIDTH.** 1s bars over 5m is 300 candles a few
pixels across, where a new one per second is invisible; the same bars over 1m is ~50
candles of ~30px, where each lands as a step. If it reads as static, widen the bars
before touching anything else.

If the chart still looks wrong, read the freshness tile: it reports how far behind the
newest row is, the measured updates per second, the round trip, and how long since a
new candle, which is enough to tell these causes apart.

**Materialized views may not be readable.** `bbo_1s` and `core_price_1s` return
`Access denied` for the `kafka` user on the demo cluster, so the app reads base
tables.
