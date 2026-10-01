// The PAGE. Renders whatever the API returns: column names come from the server, so the SQL
// can change without touching this file.
//
// When @questdb/browser-client is installable, this is where the QWP session goes: replace
// fetchTick() with connectQwpBrowserEgress({url: new URL("/read/v1", location.href)}) and
// iterate the batches here instead. Nothing else in the page changes.
import { pnlChart, pnlLegend } from "/chart.js";
import { EPOCH_DIGITS, epochTime } from "/format.js";

const PANELS = [
  ["slippage", "slippage, last 1m", "live"],
  ["markout-full", "markout -1m..+1m", "delayed 1m so the future is complete"],
  ["markout-past", "markout -1m..0", "live, past horizons only"],
  ["minmax", "min/max +-10s around fill", "delayed 10s"],
];

const params = new URLSearchParams(location.search);
let intervalMs = Number(params.get("interval") ?? 1000);
const maxRows = Number(params.get("rows") ?? 10);
// Aggregate results are 28 and 52 rows; show enough to span every venue rather
// than a single one.
const rawRows = Number(params.get("rawrows") ?? 16);

const panelsEl = document.getElementById("panels");
const chartsEl = document.getElementById("charts");
const metaEl = document.getElementById("meta");
const symEl = document.getElementById("sym");
const refreshEl = document.getElementById("refresh");

let symbol = params.get("symbol") ?? "";
const sqlOpen = new Set();      // panels whose SQL is expanded, kept across redraws
const sqlText = new Map();

const fmt = (v) => {
  if (v === null || v === undefined) return "null";
  if (typeof v === "number") return Number.isInteger(v) ? String(v) : v.toFixed(2);
  // Epochs arrive as digit strings; the unit is inferred, not assumed. See format.js.
  if (typeof v === "string" && EPOCH_DIGITS.test(v)) return epochTime(v);
  if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}T/.test(v)) return v.slice(11, 23);
  return String(v);
};

const esc = (s) => String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

// Reshape (ecn, horizon_sec, n, avg_markout_bps, total_pnl) into a markout curve: one row per
// ecn, one column per horizon. A pure reshape of rows already fetched, so it costs no extra
// query and the average shown is the one the server produced.
//
// fills is the MAX of n across horizons, not the sum. HORIZON JOIN matches every fill once per
// offset, so summing counts fill-by-horizon pairs and would report several times the real count.
function pivot(columns, rows) {
  const ix = Object.fromEntries(columns.map((c, i) => [c, i]));
  const horizons = [...new Set(rows.map((r) => Number(r[ix.horizon_sec])))].sort((a, b) => a - b);
  const byEcn = new Map();
  for (const row of rows) {
    const ecn = row[ix.ecn];
    if (!byEcn.has(ecn)) byEcn.set(ecn, { cells: new Map(), fills: 0 });
    const e = byEcn.get(ecn);
    e.cells.set(Number(row[ix.horizon_sec]), row[ix.avg_markout_bps]);
    e.fills = Math.max(e.fills, Number(row[ix.n]));
  }
  return {
    columns: ["ecn", ...horizons.map(String), "fills"],
    rows: [...byEcn.entries()]
      .sort((a, b) => b[1].fills - a[1].fills)
      .map(([ecn, e]) => [ecn, ...horizons.map((h) => e.cells.get(h) ?? null), e.fills]),
  };
}

// `newest` picks which end of the result to show, and it is not cosmetic.
//   time-ordered panels (slippage, minmax) are ORDER BY timestamp ASC, so the TAIL is the
//   live end; taking the head showed fills from a minute ago and looked frozen.
//   aggregate panels are ORDER BY ecn, horizon, so there is no "newest" at all, and taking
//   the tail showed only the alphabetically last venue: one ECN instead of four.
function table(columns, rows, cap = maxRows, newest = true) {
  if (!rows.length) return "<p class=err>(no rows in window)</p>";
  const head = columns.map((c) => `<th>${esc(c)}</th>`).join("");
  const shown = newest ? rows.slice(-cap) : rows.slice(0, cap);
  const body = shown.map((row) =>
    "<tr>" + row.map((v) => {
      const cls = typeof v === "number" && !Number.isInteger(v)
        ? (v < 0 ? " class=neg" : " class=pos") : "";
      return `<td${cls}>${esc(fmt(v))}</td>`;
    }).join("") + "</tr>").join("");
  const more = rows.length > cap
    ? `<caption>${rows.length.toLocaleString()} rows, showing ${newest ? "newest" : "first"} `
      + `${cap}</caption>` : "";
  return `<table>${more}<thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function render(payload) {
  panelsEl.innerHTML = PANELS.map(([key, title, note]) => {
    const p = payload[key];
    if (!p) return "";
    const ms = p.ms !== undefined ? ` ${p.ms.toFixed(0)}ms` : "";
    let body;
    if (p.error) {
      body = `<p class=err>${esc(p.error)}</p>`;
    } else if (key.startsWith("markout") && p.rows?.length) {
      // Curve first because it is the readable form; the query's own rows underneath so the
      // reshape can be checked against them rather than taken on trust.
      const curve = pivot(p.columns, p.rows);
      body = `<div class=sub>pivoted: avg_markout_bps by horizon (seconds)</div>`
           + table(curve.columns, curve.rows, curve.rows.length)
           + `<div class=sub>raw, as the query returns it (${p.rows.length} rows)</div>`
           + table(p.columns ?? [], p.rows ?? [], rawRows, false);
    } else {
      body = table(p.columns ?? [], p.rows ?? []);
    }
    const sql = sqlOpen.has(key)
      ? `<pre class=sql>${esc(sqlText.get(key) ?? "loading...")}</pre>` : "";
    return `<section>
        <button class=sqlbtn data-panel="${key}">${sqlOpen.has(key) ? "hide" : "SQL"}</button>
        <h2>${title} <span>${note}${ms}</span></h2>${body}${sql}</section>`;
  }).join("");
}

// The P&L curves live in their own full-width row at the bottom, so the two markout panels
// can be compared side by side at the same scale rather than squeezed into the grid cells.
function renderCharts(payload) {
  chartsEl.innerHTML = "";
  for (const [key, title, note] of PANELS) {
    if (!key.startsWith("markout")) continue;
    const p = payload[key];
    if (!p?.rows?.length || p.error) continue;
    const svg = pnlChart(p.columns, p.rows, { key });
    if (!svg) continue;
    const card = document.createElement("section");
    const head = document.createElement("h2");
    head.innerHTML = `${title} P&amp;L <span>${note} &middot; total_pnl by horizon (s)</span>`;
    card.append(head, pnlLegend(p.columns, p.rows), svg);
    chartsEl.append(card);
  }
}

// One delegated listener: render() replaces the whole grid each tick, so per-button handlers
// would be rebound constantly and lost mid-click.
panelsEl.addEventListener("click", async (ev) => {
  const btn = ev.target.closest(".sqlbtn");
  if (!btn) return;
  const key = btn.dataset.panel;
  if (sqlOpen.has(key)) {
    sqlOpen.delete(key);
  } else {
    sqlOpen.add(key);
    // Fetched from the server, built by the same sqlFor() the query path uses, so what is
    // displayed cannot drift from what actually ran.
    const qs = new URLSearchParams({ panel: key, ...(symbol ? { symbol } : {}) });
    const res = await fetch(`/api/sql?${qs}`);
    const body = await res.json();
    sqlText.set(key, body.sql ?? body.error ?? "unavailable");
  }
  await tick();
});

symEl.addEventListener("change", async () => {
  symbol = symEl.value;
  sqlText.clear();          // the statements embed the filter, so they must be refetched
  for (const key of sqlOpen) {
    const qs = new URLSearchParams({ panel: key, ...(symbol ? { symbol } : {}) });
    sqlText.set(key, (await (await fetch(`/api/sql?${qs}`)).json()).sql ?? "");
  }
  await tick();
});

async function loadSymbols() {
  try {
    const list = await (await fetch("/api/symbols")).json();
    if (!Array.isArray(list)) return;
    const keep = symbol;
    symEl.innerHTML = `<option value="">ALL</option>`
      + list.map((s) => `<option value="${esc(s)}">${esc(s)}</option>`).join("");
    symEl.value = keep;      // a refresh must not silently reset the user's choice
  } catch { /* leave the previous list in place */ }
}

async function tick() {
  const qs = symbol ? `?symbol=${encodeURIComponent(symbol)}` : "";
  try {
    const res = await fetch(`/api/tick${qs}`);
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error ?? res.statusText);
    render(payload);
    renderCharts(payload);
    panelsEl.classList.remove("stale");
    const cadence = intervalMs ? `refresh ${intervalMs}ms` : "refresh off";
    metaEl.textContent = `${new Date().toISOString().slice(11, 19)}Z  ${cadence}`;
  } catch (error) {
    // Dim rather than blank: the last good frame greyed out is more useful during an outage
    // than an empty page.
    panelsEl.classList.add("stale");
    metaEl.innerHTML = `<span class=err>${esc(error.message)}, retrying</span>`;
  }
}

// One timer, replaced whenever the cadence changes. Off (0) clears it and leaves the last
// frame on screen at full opacity: paused is a choice, not an outage, so it must not look
// like one.
let timer = null;
function applyInterval() {
  if (timer !== null) clearInterval(timer);
  timer = intervalMs ? setInterval(tick, intervalMs) : null;
}

refreshEl.addEventListener("change", async () => {
  intervalMs = Number(refreshEl.value);
  applyInterval();
  if (intervalMs) await tick();          // a faster cadence should show immediately
  else metaEl.textContent = `${new Date().toISOString().slice(11, 19)}Z  refresh off`;
});

// ?interval= still wins on load; snap the control to it, falling back to 1000ms when the
// query string asks for a cadence the dropdown does not offer.
const offered = [...refreshEl.options].map((o) => Number(o.value));
if (!offered.includes(intervalMs)) intervalMs = 1000;
refreshEl.value = String(intervalMs);

// === Tabs =============================================================================
//
// Only the visible tab queries. Four TCA statements a second against a cluster is not
// something to leave running behind a tab nobody is looking at, so switching away clears the
// timers outright rather than hiding the output.
let symbolsTimer = null;

async function startTca() {
  await loadSymbols();
  await tick();
  applyInterval();
  symbolsTimer = setInterval(loadSymbols, 30_000);  // instruments come and go
}

function stopTca() {
  if (timer !== null) { clearInterval(timer); timer = null; }
  if (symbolsTimer !== null) { clearInterval(symbolsTimer); symbolsTimer = null; }
}

// Loaded on first use, so a session that never opens the scan tab never fetches its code.
let scanModule = null;

const VIEWS = {
  tca: { view: "view-tca", ctl: "ctl-tca",
         start: startTca,
         stop: () => { stopTca(); metaEl.textContent = ""; } },
  scan: { view: "view-scan", ctl: "ctl-scan",
          start: async () => { scanModule ??= await import("/scan.js"); },
          stop: () => scanModule?.stopScan() },
};

let current = null;

async function show(name) {
  if (current === name) return;
  if (current) VIEWS[current].stop();
  current = name;
  // Every view and control row is set explicitly, not just the one being left behind: a
  // direct ?tab=scan load has no previous tab, and the markup ships with the TCA tab mounted.
  for (const [key, v] of Object.entries(VIEWS)) {
    const on = key === name;
    document.getElementById(v.view).hidden = !on;
    document.getElementById(v.ctl).hidden = !on;
  }
  for (const b of tabsEl.querySelectorAll("button")) {
    b.classList.toggle("on", b.dataset.tab === name);
  }
  await VIEWS[name].start();
}

const tabsEl = document.getElementById("tabs");
tabsEl.addEventListener("click", (ev) => {
  const btn = ev.target.closest("button[data-tab]");
  if (btn && !btn.disabled && VIEWS[btn.dataset.tab]) show(btn.dataset.tab);
});

await show(params.get("tab") === "scan" ? "scan" : "tca");
