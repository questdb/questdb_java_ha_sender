// The OHLC & VWAP tab.
//
// Candles are aggregated by QuestDB with SAMPLE BY, so what arrives is a few thousand bars
// rather than the millions of trades behind them. The chart is TradingView's Lightweight
// Charts: canvas rather than SVG, which is what makes dragging and wheel-zooming stay smooth
// at this many bars. It is served from node_modules, not a CDN, so the demo survives a bad
// conference network.
import {
  CandlestickSeries, HistogramSeries, LineSeries, createChart,
} from "/vendor/lightweight-charts.mjs";

const symEl = document.getElementById("rt-symbol");
const intervalEl = document.getElementById("rt-interval");
const lookbackEl = document.getElementById("rt-lookback");
const goEl = document.getElementById("rt-go");
const fitEl = document.getElementById("rt-fit");
const zoomInEl = document.getElementById("rt-zoomin");
const zoomOutEl = document.getElementById("rt-zoomout");
const liveEl = document.getElementById("rt-live");
const tickEl = document.getElementById("rt-tick");
const sqlBtn = document.getElementById("rt-sql");
const sqlEl = document.getElementById("rt-sqltext");
const statsEl = document.getElementById("rt-stats");
const titleEl = document.getElementById("rt-title");
const noteEl = document.getElementById("rt-note");
const chartEl = document.getElementById("rt-chart");

const INK = "#d7dce3", DIM = "#78828f", LINE = "#242a33", PANEL = "#161a21";
const UP = "#4ec9a5", DOWN = "#e2705f", VWAP = "#c98500", VOL = "#2f3a4a";

const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const int = (n) => Number(n).toLocaleString("en-US");

let chart = null, candles = null, vwapLine = null, volume = null;
let timer = null, lastBarTime = 0, decimals = 5, loading = false;
// Running VWAP totals carried past the loaded window, so the live tail can extend the line.
let vwapPv = 0, vwapQty = 0;

/** Built once. Rebuilding per reload would throw the viewport away on every parameter change. */
function ensureChart() {
  if (chart) return;
  chart = createChart(chartEl, {
    layout: { background: { color: PANEL }, textColor: DIM, fontSize: 11,
              fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace" },
    grid: { vertLines: { color: LINE }, horzLines: { color: LINE } },
    rightPriceScale: { borderColor: LINE },
    timeScale: {
      borderColor: LINE, timeVisible: true, secondsVisible: true,
      // A live chart that does not follow the newest bar is just a static one that happens
      // to be mutating off-screen.
      shiftVisibleRangeOnNewBar: true, rightOffset: 3,
    },
    crosshair: { mode: 0 },   // free crosshair: read any price, not just a bar's close
    // The point of the tab: wheel-zoom, drag-pan and pinch all on, with kinetic scrolling
    // so a flick keeps gliding.
    handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true,
                    vertTouchDrag: true },
    handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true },
    kineticScroll: { touch: true, mouse: true },
  });

  candles = chart.addSeries(CandlestickSeries, {
    upColor: UP, downColor: DOWN, borderUpColor: UP, borderDownColor: DOWN,
    wickUpColor: UP, wickDownColor: DOWN,
  });

  // VWAP rides on the price scale with the candles: same units, so it belongs on the same
  // axis. (A second y-axis for a second unit would be the thing to avoid.)
  vwapLine = chart.addSeries(LineSeries, {
    color: VWAP, lineWidth: 2, priceLineVisible: false, lastValueVisible: true,
    title: "VWAP",
  });

  // Volume is a different unit, so it gets its own hidden scale pinned to the bottom fifth
  // rather than being forced onto the price axis.
  volume = chart.addSeries(HistogramSeries, {
    priceScaleId: "vol", color: VOL, priceFormat: { type: "volume" },
  });
  chart.priceScale("vol").applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });

  chart.subscribeCrosshairMove(onCrosshair);
  // Wheel-zoom has no natural way back, and reloading to escape a zoom would be absurd.
  // Double-click anywhere on the plot restores the full window, same as the Fit button.
  chartEl.addEventListener("dblclick", () => chart.timeScale().fitContent());
  new ResizeObserver(() => chart.applyOptions({ width: chartEl.clientWidth }))
    .observe(chartEl);
}

/** The hovered bar, read out as text. A chart you cannot read values off is a picture. */
function onCrosshair(param) {
  const bar = param?.seriesData?.get(candles);
  if (!bar) { noteEl.textContent = ""; return; }
  const v = param.seriesData.get(vwapLine)?.value;
  const vol = param.seriesData.get(volume)?.value;
  const d = decimals;
  noteEl.innerHTML =
    `O <b>${bar.open.toFixed(d)}</b>&nbsp; H <b>${bar.high.toFixed(d)}</b>&nbsp; ` +
    `L <b>${bar.low.toFixed(d)}</b>&nbsp; C <b>${bar.close.toFixed(d)}</b>` +
    (v === undefined ? "" : `&nbsp; VWAP <b>${v.toFixed(d)}</b>`) +
    (vol === undefined ? "" : `&nbsp; vol <b>${int(Math.round(vol))}</b>`);
}

/**
 * Turn the server's rows into the three series.
 *
 * The running VWAP is accumulated here rather than in SQL: the per-bar VWAP and volume that
 * come back are exactly the numerator and denominator needed, so the session line costs no
 * extra query.
 */
function toSeries(columns, rows) {
  const ix = Object.fromEntries(columns.map((c, i) => [c, i]));
  const bars = [], vwaps = [], vols = [];
  let pv = 0, qty = 0;
  for (const r of rows) {
    // Nanosecond epoch -> whole seconds, which is the unit the chart's time axis uses.
    const time = Math.floor(Number(r[ix.timestamp]) / 1e9);
    const open = Number(r[ix.open]), close = Number(r[ix.close]);
    if (!Number.isFinite(open)) continue;      // an empty SAMPLE BY bucket
    bars.push({ time, open, high: Number(r[ix.high]), low: Number(r[ix.low]), close });
    const vol = Number(r[ix.volume]) || 0;
    const barVwap = Number(r[ix.vwap]);
    if (Number.isFinite(barVwap)) { pv += barVwap * vol; qty += vol; }
    if (qty > 0) vwaps.push({ time, value: pv / qty });
    vols.push({ time, value: vol, color: close >= open ? "#1f3f38" : "#3f2420" });
  }
  return { bars, vwaps, vols, pv, qty };
}

/** FX quotes need five decimals; JPY crosses need three. Taken from the data, not guessed. */
function decimalsFor(bars) {
  const sample = bars.at(-1)?.close ?? 1;
  return sample >= 50 ? 3 : 5;
}

function stat(label, value, note = "") {
  return `<div class=stat><b>${esc(value)}</b><span>${esc(label)}${
    note ? ` &middot; ${esc(note)}` : ""}</span></div>`;
}

async function load({ keepView = false } = {}) {
  if (loading) return;
  loading = true;
  goEl.textContent = "Loading...";
  try {
    const qs = new URLSearchParams({
      symbol: symEl.value || "", interval: intervalEl.value, lookback: lookbackEl.value,
    });
    const res = await fetch(`/api/ohlc?${qs}`);
    const body = await res.json();
    if (!res.ok || body.error) throw new Error(body.error ?? res.statusText);

    if (Array.isArray(body.symbols) && body.symbols.length) {
      const keep = symEl.value || body.symbol;
      symEl.innerHTML = body.symbols.map((s) => `<option>${esc(s)}</option>`).join("");
      symEl.value = body.symbols.includes(keep) ? keep : body.symbol;
    }
    sqlEl.textContent = body.sql ?? "";

    ensureChart();
    const series = toSeries(body.columns ?? [], body.rows ?? []);
    const { bars, vwaps, vols } = series;
    vwapPv = series.pv;
    vwapQty = series.qty;
    decimals = decimalsFor(bars);
    candles.applyOptions({ priceFormat: { type: "price", precision: decimals,
                                          minMove: Number(`1e-${decimals}`) } });
    vwapLine.applyOptions({ priceFormat: { type: "price", precision: decimals,
                                           minMove: Number(`1e-${decimals}`) } });

    const view = keepView ? chart.timeScale().getVisibleLogicalRange() : null;
    candles.setData(bars);
    vwapLine.setData(vwaps);
    volume.setData(vols);
    if (view) chart.timeScale().setVisibleLogicalRange(view);
    else chart.timeScale().fitContent();

    lastBarTime = bars.at(-1)?.time ?? 0;

    const span = bars.length
      ? `${new Date(bars[0].time * 1000).toISOString().slice(0, 19).replace("T", " ")} .. ${
          new Date(lastBarTime * 1000).toISOString().slice(11, 19)}`
      : "";
    const trades = (body.rows ?? []).reduce((a, r) => a + Number(r.at(-1) ?? 0), 0);
    titleEl.innerHTML = `${esc(body.symbol ?? "")} <span>${esc(body.interval)} bars &middot; ` +
      `${esc(body.lookback)} window</span>`;
    statsEl.innerHTML = [
      stat("bars", int(bars.length), body.empty ?? span),
      stat("trades aggregated", int(trades), "server-side SAMPLE BY"),
      stat("query", `${Math.round(body.ms)}ms`, "3 statements, 1 connection"),
      stat("last", bars.at(-1) ? bars.at(-1).close.toFixed(decimals) : "-",
           vwaps.at(-1) ? `vwap ${vwaps.at(-1).value.toFixed(decimals)}` : ""),
      `<div class=stat id=rt-age><b>-</b><span>newest row</span></div>`,
    ].join("");
    showAge();
    noteEl.textContent = bars.length ? "" : (body.empty ?? "no bars in this window");
  } catch (error) {
    statsEl.innerHTML = "";
    noteEl.innerHTML = `<span class=err>${esc(error.message)}</span>`;
  } finally {
    loading = false;
    goEl.textContent = "Reload";
  }
}

/**
 * Live tail.
 *
 * Only the newest bars are fetched and pushed through series.update(), which appends or
 * replaces the last bar WITHOUT touching the viewport. Re-running setData every second would
 * fight whatever the user is currently zoomed into.
 */
async function tailTick() {
  if (loading || !chart) return;
  try {
    const qs = new URLSearchParams({
      symbol: symEl.value || "", interval: intervalEl.value, lookback: "1m",
    });
    const body = await (await fetch(`/api/ohlc?${qs}`)).json();
    if (body.error || !body.rows?.length) return;
    const { bars, vols } = toSeries(body.columns, body.rows);
    const first = bars.findIndex((b) => b.time >= lastBarTime);
    if (first < 0) return;
    for (let i = first; i < bars.length; i++) {
      candles.update(bars[i]);
      const vol = vols[i];
      if (vol) volume.update(vol);
      // Extend the session VWAP too. It is accumulated over the LOADED window, so the tail's
      // own running total cannot be reused; this carries the line forward from the totals the
      // full load ended on. Without it the one line meant to move stopped at load time.
      const barVwap = Number(body.rows[i]?.[body.columns.indexOf("vwap")]);
      const barVol = Number(body.rows[i]?.[body.columns.indexOf("volume")]) || 0;
      if (Number.isFinite(barVwap) && bars[i].time > lastBarTime) {
        vwapPv += barVwap * barVol;
        vwapQty += barVol;
      }
      if (vwapQty > 0) vwapLine.update({ time: bars[i].time, value: vwapPv / vwapQty });
      lastBarTime = bars[i].time;
    }
    showAge();
  } catch { /* a dropped tick is not worth interrupting the chart for */ }
}

/**
 * How far behind the newest row is.
 *
 * "The chart looks static" has two completely different causes: the page is not tailing, or
 * nothing is being written. Without this they are indistinguishable, so the age is shown and
 * it keeps counting whether or not bars arrive. A number climbing past a few seconds means
 * the writer is idle, not that the chart is broken.
 */
function showAge() {
  const tile = document.getElementById("rt-age");
  if (!tile) return;
  if (!lastBarTime) { tile.innerHTML = "<b>-</b><span>newest row</span>"; return; }
  const seconds = Math.max(0, Math.round(Date.now() / 1000 - lastBarTime));
  const text = seconds < 90 ? `${seconds}s`
    : seconds < 5400 ? `${Math.round(seconds / 60)}m`
    : `${(seconds / 3600).toFixed(1)}h`;
  const state = seconds <= 15 ? "live" : "writer idle or paused";
  tile.innerHTML = `<b>${text}</b><span>behind newest row &middot; ${state}</span>`;
}

// Ticks regardless of whether data arrives, so a stalled writer shows as a rising number
// rather than as a page that appears frozen.
setInterval(showAge, 1000);

function setLive(on) {
  if (timer !== null) { clearInterval(timer); timer = null; }
  // Polling faster than the bar width is the point: between bar boundaries the newest candle
  // still grows as trades land, so a 250ms poll on 1s bars redraws it four times before it
  // closes. At 1000ms the candle only ever appeared finished, which is what made the chart
  // look like it was barely ticking.
  if (on) timer = setInterval(tailTick, Number(tickEl.value) || 250);
}

goEl.addEventListener("click", () => load({ keepView: false }));
fitEl.addEventListener("click", () => chart?.timeScale().fitContent());

// Wheel and pinch are the natural gestures, but nothing on screen says so, and a trackpad
// pinch is not obvious either. These drive the same visible range the wheel does.
function zoomBy(factor) {
  const scale = chart?.timeScale();
  const range = scale?.getVisibleLogicalRange();
  if (!range) return;
  const middle = (range.from + range.to) / 2;
  const half = ((range.to - range.from) / 2) * factor;
  scale.setVisibleLogicalRange({ from: middle - half, to: middle + half });
}
zoomInEl.addEventListener("click", () => zoomBy(0.6));
zoomOutEl.addEventListener("click", () => zoomBy(1 / 0.6));
symEl.addEventListener("change", () => load());
intervalEl.addEventListener("change", () => load());
lookbackEl.addEventListener("change", () => load());
liveEl.addEventListener("change", () => setLive(liveEl.checked));
tickEl.addEventListener("change", () => setLive(liveEl.checked));
sqlBtn.addEventListener("click", () => {
  sqlEl.hidden = !sqlEl.hidden;
  sqlBtn.textContent = sqlEl.hidden ? "SQL" : "hide";
});

// Populated from the server so the dropdowns cannot offer an interval the query would reject.
const options = await (await fetch("/api/ohlc-options")).json();
// Defaults chosen for MOTION, not for coverage. 5s bars over 30m is 360 candles a few
// pixels wide with a new one every five seconds, which reads as a still image. 1s bars over
// 5m is one new candle per second at a visible width, and the poll interval matches the bar
// width so every tick draws something.
intervalEl.innerHTML = options.intervals
  .map((i) => `<option${i === "1s" ? " selected" : ""}>${i}</option>`).join("");
lookbackEl.innerHTML = options.lookbacks
  .map((l) => `<option${l === "5m" ? " selected" : ""}>${l}</option>`).join("");

await load();
setLive(liveEl.checked);   // the tab ships live: a realtime chart should arrive moving

/** Leaving the tab stops the live tail: nothing queries off-screen. */
export function stopOhlc() { setLive(false); }
export function startOhlc() {
  if (chart) load({ keepView: true });
  setLive(liveEl.checked);
}
