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
import { PANELS, sqlFor } from "./queries.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const PUBLIC = join(HERE, "public");

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
};

const port = Number(arg("port", 8080));
const conf = arg("conf", `ws::addr=${arg("addr", "localhost:9000")};`);
const symbol = arg("symbol", null);

const TYPES = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
                ".css": "text/css; charset=utf-8" };

const data = new Data(conf);
await data.connect();
console.log(`[js] connected: ${conf}`);

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === "/api/tick") {
    try {
      const payload = await data.tick(url.searchParams.get("symbol") || symbol);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(payload, (_k, v) =>
        // QWP returns 64-bit integers as BigInt, which JSON.stringify refuses outright.
        typeof v === "bigint" ? v.toString() : v));
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
