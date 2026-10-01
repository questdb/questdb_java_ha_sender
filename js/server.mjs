// The STATIC layer plus a thin JSON API. Deliberately knows nothing about QuestDB: all of
// that lives in data.mjs, so swapping to @questdb/browser-client later touches one file.
//
// Why a server at all, when the browser client exists? QuestDB accepts a browser WebSocket
// upgrade only when the request's Origin matches its Host, which blocks cross-site WebSocket
// hijacking, and answers a cross-origin upgrade with HTTP 400. A different PORT is a different
// origin, so a page on :8080 cannot talk to QuestDB on :9000 directly however local it is.
// Serving the page and the data from one origin sidesteps that entirely. (CORS itself never
// applies: WebSocket upgrades are not subject to it.)
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join, normalize } from "node:path";
import { Data } from "./data.mjs";
import {
  OHLC_INTERVALS, OHLC_LOOKBACKS, PANELS, SCAN_PROJECTIONS, SCAN_TABLES, sqlFor,
} from "./queries.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const PUBLIC = join(HERE, "public");

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
};

const port = Number(arg("port", 8080));
const symbol = arg("symbol", null);

// --token-file rather than a token on the command line: argv is world-readable through ps,
// and the configuration string is logged at startup. The file is read here and the value
// never leaves this process except on the wire to QuestDB.
const tokenFile = arg("token-file", null);
const token = tokenFile ? (await readFile(tokenFile, "utf8")).trim() : null;

// The cluster is configuration, not a constant. --addrs takes the comma-separated list the
// QWP client already understands natively: it tries them in order and fails over to the next
// when one is unreachable, so naming every node is how read HA is exercised rather than
// merely claimed.
//
//   --scheme wss --addrs enterprise-primary:9000,172.31.42.41:9000,10.0.0.8:9000
//
// tls_verify defaults to unsafe_off because these clusters present self-signed certificates;
// it is applied only to wss, announced at startup, and --tls-verify on restores checking.
const scheme = arg("scheme", "ws");
const addrs = arg("addrs", arg("addr", "localhost:9000"));
const tlsVerify = arg("tls-verify", "unsafe_off");
const compression = arg("compression", null);
const maxBatchRows = arg("max-batch-rows", null);

// Each parallel reader borrows its own query connection, and the pool defaults to 4: asking
// for 8 readers without raising this fails with "timed out waiting for a QWP query from the
// pool". The readers the UI may request are capped to this number below.
const queryPoolMax = Number(arg("query-pool-max", 16));

const opts = [`addr=${addrs}`, `query_pool_max=${queryPoolMax}`];
if (scheme === "wss") opts.push(`tls_verify=${tlsVerify}`);
if (compression) opts.push(`compression=${compression}`);
if (maxBatchRows) opts.push(`max_batch_rows=${maxBatchRows}`);
const base = arg("conf", `${scheme}::${opts.join(";")};`);
const withSemi = base.endsWith(";") ? base : `${base};`;
const conf = token ? `${withSemi}token=${token};` : withSemi;
// What gets logged. The token is replaced, not truncated: a prefix is still a secret.
const safeConf = token ? `${withSemi}token=***;` : withSemi;

// QWP returns 64-bit integers as BigInt, which JSON.stringify refuses outright.
const bigints = (_k, v) => (typeof v === "bigint" ? v.toString() : v);

// The scan in flight, if any. One at a time: see /api/scan. `scanSettled` resolves once that
// scan has handed its query connections back, which a new scan must wait for.
let scanAbort = null;
let scanSettled = Promise.resolve();

const TYPES = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
                ".mjs": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8" };

// The chart library is served from node_modules rather than a CDN: the demo has to work on
// a conference network, or none at all.
const VENDOR = {
  "/vendor/lightweight-charts.mjs":
    join(HERE, "node_modules/lightweight-charts/dist/lightweight-charts.standalone.production.mjs"),
};

const data = new Data(conf);
await data.connect();
console.log(`[js] connected: ${safeConf}`);
if (scheme === "wss" && tlsVerify === "unsafe_off") {
  console.log("[js] WARNING: TLS certificate verification is OFF (--tls-verify on to enable)");
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === "/api/tick") {
    try {
      const payload = await data.tick(url.searchParams.get("symbol") || symbol);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(payload, bigints));
    } catch (error) {
      // One bad tick must not take the dashboard down; the page shows the message instead.
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: String(error?.message ?? error) }));
    }
    return;
  }

  if (url.pathname === "/api/symbols") {
    try {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(await data.symbols()));
    } catch (error) {
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: String(error?.message ?? error) }));
    }
    return;
  }

  if (url.pathname === "/api/ohlc") {
    try {
      const payload = await data.ohlc({
        symbol: url.searchParams.get("symbol") || symbol || "",
        interval: url.searchParams.get("interval") ?? "5s",
        lookback: url.searchParams.get("lookback") ?? "30m",
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(payload, bigints));
    } catch (error) {
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: String(error?.message ?? error) }));
    }
    return;
  }

  if (url.pathname === "/api/ohlc-options") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({
      intervals: Object.keys(OHLC_INTERVALS), lookbacks: Object.keys(OHLC_LOOKBACKS),
    }));
    return;
  }

  if (VENDOR[url.pathname]) {
    try {
      res.writeHead(200, { "content-type": TYPES[".mjs"] });
      res.end(await readFile(VENDOR[url.pathname]));
    } catch {
      res.writeHead(404).end("vendor file missing; run npm install");
    }
    return;
  }

  if (url.pathname === "/api/scan-tables") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(SCAN_TABLES));
    return;
  }

  // Server-sent events, because the scan is a long one-way stream of progress and SSE
  // reconnects, framing and backpressure come free with the transport. Single-flight: a new
  // scan cancels the one in progress, so a page reload cannot leave an orphan reading 200
  // million rows into a socket nobody is listening to.
  if (url.pathname === "/api/scan") {
    const table = url.searchParams.get("table") ?? SCAN_TABLES[0];
    const rows = Number(url.searchParams.get("rows") ?? 200_000_000);
    const readers = Number(url.searchParams.get("readers") ?? 4);
    // Rows per query. Bounded so one slice always finishes inside QuestDB's query.timeout;
    // see Data.scan.
    const chunkRows = Number(url.searchParams.get("chunk_rows") ?? 500_000);
    const projection = url.searchParams.get("projection") ?? "all";
    if (!SCAN_TABLES.includes(table) || !Number.isFinite(rows) || rows < 1
        || !Number.isFinite(readers) || readers < 1 || readers > queryPoolMax
        || !Number.isFinite(chunkRows) || chunkRows < 1
        || !(projection in SCAN_PROJECTIONS)) {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({
        error: `bad scan request: table=${table} rows=${rows} readers=${readers}` }));
      return;
    }

    const control = new AbortController();

    // Claim the single-flight slot SYNCHRONOUSLY, before any await: two starts in quick
    // succession would otherwise both read the old slot, both wait on the same scan, and
    // both run. Whoever claims last wins, and an older claimant bails out below.
    const previousAbort = scanAbort;
    const previousSettled = scanSettled;
    scanAbort = control;
    let markSettled;
    scanSettled = new Promise((resolve) => { markSettled = resolve; });

    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      // The dashboard is same-origin, but a proxy that buffers would defeat the whole point.
      "x-accel-buffering": "no",
    });

    const send = (event, payload) => {
      if (res.writableEnded) return;
      res.write(`event: ${event}\ndata: ${JSON.stringify(payload, bigints)}\n\n`);
    };

    // The browser going away is the normal way a scan ends early: stop reading immediately
    // rather than finishing 200 million rows for nobody.
    res.on("close", () => control.abort());

    // Stop the previous scan AND wait for it to let go. Cancelling a query is not instant:
    // the client drains the cancelled statement before returning its connection to the pool,
    // so starting 16 fresh readers while the old 16 are still draining fails with "timed out
    // waiting for a QWP query from the pool". This wait is what makes "stop, change the
    // parameters, start again" work at any reader count, and it is announced rather than
    // silent: without the event the page sits on a frozen zero for seconds with no reason.
    if (previousAbort) {
      previousAbort.abort();
      send("waiting", { table, readers });
      await previousSettled.catch(() => {});
    }

    // Overtaken while waiting, or the browser gave up: do not start a second scan.
    if (control.signal.aborted || scanAbort !== control) {
      markSettled();
      res.end();
      return;
    }

    try {
      await data.scan({ table, rows, readers, chunkRows, projection },
                      (p) => send(p.done ? "done" : "progress", p),
                      control.signal);
    } catch (error) {
      if (!control.signal.aborted) send("failed", { error: String(error?.message ?? error) });
    } finally {
      if (scanAbort === control) scanAbort = null;
      markSettled();
      res.end();
    }
    return;
  }

  // The exact statement a panel is running, so the page can show it verbatim. Built from the
  // same sqlFor() the query path uses, so it cannot drift from what actually ran.
  if (url.pathname === "/api/sql") {
    const panel = url.searchParams.get("panel");
    if (!PANELS.includes(panel)) {
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: `unknown panel: ${panel}` }));
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({
      panel,
      sql: sqlFor(panel, url.searchParams.get("symbol") || symbol).trim(),
    }));
    return;
  }

  // Static files, path-traversal guarded.
  const rel = url.pathname === "/" ? "/index.html" : url.pathname;
  const file = join(PUBLIC, normalize(rel));
  if (!file.startsWith(PUBLIC)) {
    res.writeHead(403).end("forbidden");
    return;
  }
  try {
    const body = await readFile(file);
    const ext = file.slice(file.lastIndexOf("."));
    res.writeHead(200, { "content-type": TYPES[ext] ?? "application/octet-stream" });
    res.end(body);
  } catch {
    res.writeHead(404).end("not found");
  }
});

server.listen(port, () => console.log(`[js] http://localhost:${port}`));

// Ctrl+C must ALWAYS work. Installing a handler replaces node's default terminate
// behaviour, so awaiting the QWP close unguarded means a stalled close leaves a process
// that ignores Ctrl+C entirely and has to be killed by PID. The graceful path is therefore
// raced against a deadline, and a second signal leaves immediately.
let stopping = false;
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    if (stopping) process.exit(130);
    stopping = true;
    console.log("\n[js] shutting down");
    server.close();
    // Keep-alive sockets from the open dashboard would otherwise hold the loop open well
    // past the last request.
    server.closeAllConnections?.();
    const deadline = setTimeout(() => {
      console.log("[js] close timed out, exiting anyway");
      process.exit(0);
    }, 2000);
    data.close().catch(() => {}).finally(() => { clearTimeout(deadline); process.exit(0); });
  });
}
