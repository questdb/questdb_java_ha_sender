// Does reading timestamps without BigInt actually pay?
//
// The scan in this folder never reads a value on its hot path - it counts rows and leaves the
// columns alone - so swapping client builds only exercises the decoder, never the accessor.
// This reads the timestamp of EVERY row, three ways, so the API is the thing under test:
//
//   none        touch no values, only rowCount          (the scan's hot path, for reference)
//   long        getLong(), which mints a BigInt per row (what the client has always done)
//   longnumber  getLongNumber(), a primitive number     (feat/qwp-long-number-gorilla only)
//
// A checksum is accumulated so no read can be optimised away, and each mode runs twice with
// the best taken, because a single run on a busy cluster says very little.
//
// RUN IT NEXT TO THE DATABASE. Measured over a WAN this reports the link, not the decoder:
// on a slow link `none` came out SLOWER than `long`, which is impossible if decoding were the
// bottleneck and is the clearest possible sign the numbers are noise. Sanity-check every run
// against that: if `none` is not comfortably the fastest, the result is meaningless.
//
//   node bench-longnumber.mjs <addrs> [rows] [--token-file PATH]
//   node bench-longnumber.mjs 172.31.42.41:9000,172.31.41.35:9000 2000000
import { readFileSync } from "node:fs";
import { connectQwpNodeClient } from "@questdb/nodejs-client";

const argv = process.argv.slice(2);
const flag = (name, fallback) => {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 && argv[i + 1] ? argv[i + 1] : fallback;
};
const positional = argv.filter((a, i) =>
  !a.startsWith("--") && !(i > 0 && argv[i - 1].startsWith("--")));

const addrs = positional[0] ?? "localhost:9000";
const rows = Number(positional[1] ?? 2_000_000);
const table = flag("table", "core_price");
const tokenFile = flag("token-file", process.env.QDB_TOKEN_FILE ?? null);
const token = tokenFile ? readFileSync(tokenFile, "utf8").trim() : null;

const conf = token
  ? `wss::addr=${addrs};tls_verify=unsafe_off;query_pool_max=4;token=${token};`
  : `ws::addr=${addrs};query_pool_max=4;`;

console.log(`${table}, last ${rows.toLocaleString("en-US")} rows, ${addrs}`);
const db = await connectQwpNodeClient(conf);

async function run(mode) {
  const lease = await db.borrowQuery();
  let seen = 0, checksum = 0, started = 0;
  try {
    started = performance.now();
    const query = await lease.queryViews(
      `SELECT * FROM ${table} LIMIT -${rows}`,
      (batch) => {
        const count = batch.rowCount;
        seen += count;
        if (mode === "none") return;
        const column = batch.column(0);          // the designated timestamp
        if (mode === "long") {
          for (let r = 0; r < count; r++) checksum += Number(column.getLong(r) & 0xffffn);
        } else {
          // Throws outside JavaScript's safe integer range, so a TIMESTAMP_NANOS column
          // fails loudly here rather than returning a wrong number.
          for (let r = 0; r < count; r++) checksum += (column.getLongNumber(r) ?? 0) % 65536;
        }
      },
      // The server applies its own query.timeout, so keep `rows` small enough that one
      // statement finishes inside it; this benchmark deliberately does not slice.
      { timeoutMs: 0 },
    );
    await query.completion;
  } finally {
    await lease.close();
  }
  const ms = performance.now() - started;
  return { mode, rows: seen, ms, rate: (seen / ms) * 1000, checksum };
}

const best = new Map();
for (const mode of ["none", "long", "longnumber", "none", "long", "longnumber"]) {
  try {
    const result = await run(mode);
    if (!best.has(mode) || result.rate > best.get(mode).rate) best.set(mode, result);
  } catch (error) {
    console.log(`${mode}: ${String(error?.message ?? error)}`);
  }
}
await db.close();

console.log(`\n${"mode".padEnd(12)}${"rows".padStart(14)}${"ms".padStart(9)}${"rows/s".padStart(14)}`);
for (const mode of ["none", "long", "longnumber"]) {
  const r = best.get(mode);
  if (!r) { console.log(`${mode.padEnd(12)}${"unavailable".padStart(14)}`); continue; }
  console.log(
    mode.padEnd(12)
      + r.rows.toLocaleString("en-US").padStart(14)
      + r.ms.toFixed(0).padStart(9)
      + Math.round(r.rate).toLocaleString("en-US").padStart(14),
  );
}

const none = best.get("none"), long = best.get("long"), num = best.get("longnumber");
if (long && num) {
  console.log(`\ngetLongNumber vs getLong: ${(((num.rate - long.rate) / long.rate) * 100).toFixed(1)}%`);
}
if (none && long && none.rate < long.rate) {
  console.log("WARNING: reading nothing was slower than reading every row, so this run is "
    + "bound by the link or the server, not by decoding. The comparison above is noise.");
}
