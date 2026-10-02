// The STREAMING SCAN tab.
//
// The server reads the table over QWP in batches and never holds the result; this page is
// told only how far it has got, plus the last few rows it saw. So neither side's memory
// grows with the scan, which is the claim the tab exists to demonstrate.
//
// Transport is Server-Sent Events: a long one-way stream of progress is exactly what SSE is
// for, and closing the EventSource closes the socket, which is what tells the server to stop
// reading. No polling, and no way to leave a scan running after the tab goes away.
import { EPOCH_DIGITS, epochStamp, fmtStructured } from "/format.js";

const tableEl = document.getElementById("scan-table");
const rowsEl = document.getElementById("scan-rows");
const projectionEl = document.getElementById("scan-projection");
const readersEl = document.getElementById("scan-readers");
const goEl = document.getElementById("scan-go");
const statsEl = document.getElementById("scan-stats");
const panelEl = document.getElementById("scan-panel");

// The list comes from the server, which builds it from the same SCAN_TABLES that validates
// the request, so the dropdown cannot offer a table the scan would reject.
const tables = await (await fetch("/api/scan-tables")).json();
tableEl.innerHTML = tables.map((t) => `<option>${t}</option>`).join("");

let source = null;

const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

const int = (n) => Number(n).toLocaleString("en-US");

// Rows/s runs from thousands to tens of millions, so a fixed unit is unreadable at one end
// or the other.
const rate = (n) =>
  n >= 1e6 ? `${(n / 1e6).toFixed(2)}M` : n >= 1e3 ? `${(n / 1e3).toFixed(1)}k` : n.toFixed(0);

const secs = (ms) => (ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : `${ms.toFixed(0)}ms`);

// Decoded volume, reported the same way python/read_bench.py does so the two are comparable:
// GiB for the total a human reads, MB/s and Gb/s for the rate.
const gib = (b) => b / 1024 ** 3;
const volume = (b) => (gib(b) >= 1 ? `${gib(b).toFixed(2)} GiB` : `${(b / 1e6).toFixed(0)} MB`);

function fmt(v) {
  if (v === null || v === undefined) return "null";
  if (typeof v === "number") return Number.isInteger(v) ? String(v) : v.toFixed(5);
  // Epochs arrive as digit strings; the unit is inferred, not assumed. See format.js.
  if (typeof v === "string" && EPOCH_DIGITS.test(v)) return epochStamp(v);
  // SELECT * reaches types the TCA panels never select: UUID, LONG256, DECIMAL, arrays.
  if (typeof v === "object") return fmtStructured(v);
  return String(v);
}

function stat(label, value, note = "") {
  return `<div class=stat><b>${esc(value)}</b><span>${esc(label)}${
    note ? ` &middot; ${esc(note)}` : ""}</span></div>`;
}

function render(p, state) {
  const rowsPerSec = p.ms > 0 ? (p.rows / p.ms) * 1000 : 0;
  statsEl.innerHTML = [
    stat("rows read", int(p.rows), state),
    stat("batches", int(p.batches),
         p.batches ? `${int(Math.round(p.rows / p.batches))} rows/batch` : ""),
    stat("elapsed", secs(p.ms),
         p.chunks ? `chunk ${int(p.chunksDone)}/${int(p.chunks)}` : ""),
    stat("throughput", `${rate(rowsPerSec)} rows/s`,
         p.readers ? `${p.readers} reader${p.readers === 1 ? "" : "s"}${
           p.projection && p.projection !== "all" ? ` \u00b7 ${p.projection}` : ""}` : ""),
    stat("transfer", p.ms > 0 ? `${((p.bytes ?? 0) * 8 / p.ms / 1e6).toFixed(2)} Gb/s` : "-",
         `${volume(p.bytes ?? 0)} decoded \u00b7 ${
           p.ms > 0 ? ((p.bytes ?? 0) / p.ms / 1e3).toFixed(0) : 0} MB/s`),
  ].join("");

  const head = (p.columns ?? []).map((c) => `<th>${esc(c)}</th>`).join("");
  const body = (p.tail ?? []).map((r) =>
    `<tr>${r.map((v) => `<td>${esc(fmt(v))}</td>`).join("")}</tr>`).join("");

  panelEl.innerHTML = `
    <h2>${esc(p.table ?? "")} <span>last ${p.tail?.length ?? 0} rows received${
      p.chunks > 1 ? ` &middot; first of ${int(p.chunks)} slices shown` : ""}</span></h2>
    <pre class=sql>${esc(p.sql ?? "")}</pre>
    <table><thead><tr>${head}</tr></thead><tbody>${body}</tbody>
      <caption>the page holds these ${p.tail?.length ?? 0} rows and four counters, nothing
        more: ${int(p.rows)} rows streamed through it</caption></table>`;
}

function stop() {
  source?.close();          // closing the socket is what stops the server reading
  source = null;
  goEl.textContent = "Start scan";
}

function start() {
  stop();
  const qs = new URLSearchParams({ table: tableEl.value, rows: String(rowsEl.value || 1),
                                  readers: readersEl.value,
                                  projection: projectionEl.value });
  const blank = { table: tableEl.value, rows: 0, batches: 0, ms: 0, columns: [], tail: [],
                  sql: "", chunks: 0, chunksDone: 0, readers: Number(readersEl.value) };
  render(blank, "starting");
  goEl.textContent = "Stop scan";

  source = new EventSource(`/api/scan?${qs}`);
  // The server is draining the scan this one is replacing. Cancelling a query is not
  // instant, so say so rather than showing a frozen zero.
  source.addEventListener("waiting", () => render(blank, "waiting for the previous scan"));
  source.addEventListener("progress", (ev) => render(JSON.parse(ev.data), "streaming"));
  source.addEventListener("done", (ev) => { render(JSON.parse(ev.data), "complete"); stop(); });
  source.addEventListener("failed", (ev) => {
    panelEl.innerHTML = `<p class=err>${esc(JSON.parse(ev.data).error)}</p>`;
    stop();
  });
  // EventSource retries by default; a scan is not idempotent progress, so a dropped stream
  // ends the run rather than silently restarting it from zero.
  source.addEventListener("error", () => { if (source?.readyState === 2) stop(); });
}

goEl.addEventListener("click", () => (source ? stop() : start()));

/** Leaving the tab must stop the scan: the point is that nothing runs off-screen. */
export function stopScan() { stop(); }
