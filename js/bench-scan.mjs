// How should a scan be sliced, and how many readers should pull the slices?
//
// Both knobs were chosen against a slow WAN, where one statement had to finish inside
// QuestDB's 60s query.timeout and 500k-row slices were the safe answer. Next to the database
// that reasoning inverts: a 500k slice finishes in tens of milliseconds, so per-statement
// setup - a new request, its round trip, its first batch - stops being a rounding error and
// starts being most of the slice's life. 200M rows at 500k is 400 statements.
//
// This sweeps slice size against reader count and prints the matrix, using the same
// scanChunks() the app uses so the slicing under test is the slicing that ships.
//
//   node bench-scan.mjs <addrs> [rows] [--token-file PATH] [--table T]
//                       [--chunks 500000,5000000] [--readers 1,4,16]
//                       [--projection all|epoch-long|no-timestamp]
//
//   node bench-scan.mjs 172.31.42.41:9000,172.31.41.35:9000 20000000 \
//       --token-file ~/qwp_token.txt.spx
//
// The hot path is deliberately identical to the app's: count rows, touch no values.
import { readFileSync } from "node:fs";
import { connectQwpNodeClient } from "@questdb/nodejs-client";
import { scanChunks } from "./queries.mjs";

const argv = process.argv.slice(2);
const flag = (name, fallback) => {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 && argv[i + 1] ? argv[i + 1] : fallback;
};
const positional = argv.filter((a, i) =>
  !a.startsWith("--") && !(i > 0 && argv[i - 1].startsWith("--")));

const addrs = positional[0] ?? "localhost:9000";
const rows = Number(positional[1] ?? 20_000_000);
const table = flag("table", "core_price");
const projection = flag("projection", "all");
const chunkSizes = flag("chunks", "500000,2000000,5000000,20000000")
  .split(",").map(Number).filter(Number.isFinite);
const readerCounts = flag("readers", "1,2,4,8,16")
  .split(",").map(Number).filter(Number.isFinite);
const tokenFile = flag("token-file", process.env.QDB_TOKEN_FILE ?? null);
const token = tokenFile ? readFileSync(tokenFile, "utf8").trim() : null;

const poolMax = Math.max(...readerCounts);
const conf = token
  ? `wss::addr=${addrs};tls_verify=unsafe_off;query_pool_max=${poolMax};token=${token};`
  : `ws::addr=${addrs};query_pool_max=${poolMax};`;

console.log(`${table} (${projection}), last ${rows.toLocaleString("en-US")} rows, ${addrs}`);
const db = await connectQwpNodeClient(conf);

/** One scan at a given slice size and reader count. Mirrors Data.scan's work queue. */
async function scan(chunkRows, readers) {
  const sqls = scanChunks(table, rows, Math.max(readers, Math.ceil(rows / chunkRows)),
                          projection);
  const queue = sqls.slice();
  let seen = 0;
  const started = performance.now();

  const reader = async () => {
    const lease = await db.borrowQuery();
    try {
      while (queue.length > 0) {
        const sql = queue.shift();
        const query = await lease.queryViews(sql, (batch) => { seen += batch.rowCount; },
                                             { timeoutMs: 0 });
        await query.completion;
      }
    } finally {
      await lease.close();
    }
  };

  await Promise.all(Array.from({ length: Math.min(readers, sqls.length) }, () => reader()));
  const ms = performance.now() - started;
  return { slices: sqls.length, rows: seen, ms, rate: (seen / ms) * 1000 };
}

const results = [];
for (const chunkRows of chunkSizes) {
  for (const readers of readerCounts) {
    try {
      // Best of two: one run on a shared cluster says very little.
      const a = await scan(chunkRows, readers);
      const b = await scan(chunkRows, readers);
      const best = a.rate > b.rate ? a : b;
      results.push({ chunkRows, readers, ...best });
      process.stdout.write(".");
    } catch (error) {
      results.push({ chunkRows, readers, error: String(error?.message ?? error) });
      process.stdout.write("x");
    }
  }
}
await db.close();

const million = (n) => `${(n / 1e6).toFixed(2)}M`;
console.log(`\n\n${"slice".padStart(10)}${"slices".padStart(8)}`
  + readerCounts.map((r) => `${r} rdr`.padStart(10)).join(""));
for (const chunkRows of chunkSizes) {
  const row = results.filter((r) => r.chunkRows === chunkRows);
  const slices = row.find((r) => r.slices)?.slices ?? "-";
  console.log(
    million(chunkRows).padStart(10)
      + String(slices).padStart(8)
      + readerCounts.map((readers) => {
        const cell = row.find((r) => r.readers === readers);
        return (cell?.error ? "err" : million(cell?.rate ?? 0)).padStart(10);
      }).join(""),
  );
}

const ok = results.filter((r) => !r.error);
if (ok.length) {
  const top = ok.reduce((a, b) => (a.rate > b.rate ? a : b));
  console.log(`\nbest: ${million(top.rate)} rows/s at ${million(top.chunkRows)} slices `
    + `× ${top.readers} readers (${top.slices} slices, ${top.ms.toFixed(0)}ms)`);
  // A slice must still finish inside the server's query.timeout, which is what the original
  // 500k sizing was protecting against. Report the worst case actually observed.
  const perSlice = (top.ms / Math.ceil(top.slices / top.readers)) / 1000;
  console.log(`at that setting each slice took about ${perSlice.toFixed(1)}s; `
    + "keep it well inside QuestDB's query.timeout (60s by default)");
}
for (const r of results.filter((x) => x.error)) {
  console.log(`${million(r.chunkRows)} x ${r.readers}: ${r.error}`);
}
