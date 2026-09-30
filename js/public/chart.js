// Markout P&L curves as inline SVG. No chart library: the whole thing is a handful of
// polylines, and a dependency would be more code than the chart.
//
// Palette is categorical slots 1-4 of the reference dark theme, validated against this
// dashboard's surface (#161a21) rather than assumed: worst adjacent pair is
// #c98500 <-> #199e70 at dE 8.4 protan / 19.8 normal vision, above both thresholds.
const SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500"];

// Colour follows the ENTITY, not its rank. The tables sort by fill count and the symbol
// filter changes which venues appear, so assigning by array index would repaint the
// survivors every time the ordering shifted. This map is stable for the page's lifetime.
const ecnColour = new Map();
function colourFor(ecn) {
  if (!ecnColour.has(ecn)) ecnColour.set(ecn, SERIES[ecnColour.size % SERIES.length]);
  return ecnColour.get(ecn);
}

const NS = "http://www.w3.org/2000/svg";
const el = (name, attrs = {}) => {
  const node = document.createElementNS(NS, name);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  return node;
};

// P&L is money: thousands separators, and a compact form once it runs to millions.
const money = (v) =>
  Math.abs(v) >= 1e6 ? `${(v / 1e6).toFixed(2)}M`
  : Math.abs(v) >= 1e3 ? `${(v / 1e3).toFixed(1)}k`
  : v.toFixed(0);

/**
 * Build the P&L curve for one markout panel straight from the raw rows.
 *
 * Plots total_pnl against horizon, one line per ECN. total_pnl is what the query already
 * computed per (ecn, horizon), so nothing is recomputed here: this is the same number the
 * table shows, drawn.
 */
export function pnlChart(columns, rows, { width = 560, height = 210, key = "" } = {}) {
  const ix = Object.fromEntries(columns.map((c, i) => [c, i]));
  const horizons = [...new Set(rows.map((r) => Number(r[ix.horizon_sec])))].sort((a, b) => a - b);
  if (horizons.length < 2) return null;

  const byEcn = new Map();
  for (const row of rows) {
    const ecn = row[ix.ecn];
    if (!byEcn.has(ecn)) byEcn.set(ecn, new Map());
    byEcn.get(ecn).set(Number(row[ix.horizon_sec]), Number(row[ix.total_pnl]));
  }

  const pad = { t: 10, r: 12, b: 26, l: 62 };
  const plotW = width - pad.l - pad.r;
  const plotH = height - pad.t - pad.b;

  const values = [...byEcn.values()].flatMap((m) => [...m.values()]).filter(Number.isFinite);
  if (!values.length) return null;
  // Always include zero: a P&L curve is read against break-even, so a range that excludes
  // it would hide whether a venue is up or down.
  let lo = Math.min(0, ...values);
  let hi = Math.max(0, ...values);
  if (lo === hi) { lo -= 1; hi += 1; }
  const padY = (hi - lo) * 0.08;
  lo -= padY; hi += padY;

  const x = (h) => pad.l + ((h - horizons[0]) / (horizons.at(-1) - horizons[0])) * plotW;
  const y = (v) => pad.t + (1 - (v - lo) / (hi - lo)) * plotH;

  const svg = el("svg", {
    width: "100%", viewBox: `0 0 ${width} ${height}`,
    role: "img", "aria-label": "Markout P&L by horizon, one line per ECN",
  });

  // Recessive chrome: solid hairlines one shade off the surface, never dashed.
  for (let i = 0; i <= 4; i++) {
    const gy = pad.t + (plotH * i) / 4;
    svg.append(el("line", { x1: pad.l, x2: pad.l + plotW, y1: gy, y2: gy,
                            stroke: "#242a33", "stroke-width": 1 }));
    const gv = hi - ((hi - lo) * i) / 4;
    const label = el("text", { x: pad.l - 8, y: gy + 3, "text-anchor": "end",
                               fill: "#78828f", "font-size": 9.5 });
    label.textContent = money(gv);
    svg.append(label);
  }

  // Break-even, drawn brighter than the grid because it is the reference the curve is read
  // against, but still chrome rather than data.
  if (lo < 0 && hi > 0) {
    svg.append(el("line", { x1: pad.l, x2: pad.l + plotW, y1: y(0), y2: y(0),
                            stroke: "#3b4450", "stroke-width": 1 }));
  }

  for (const h of horizons) {
    const t = el("text", { x: x(h), y: height - 8, "text-anchor": "middle",
                           fill: "#78828f", "font-size": 9.5 });
    t.textContent = `${h}`;
    svg.append(t);
  }
  // The zero horizon is the fill itself: mark it so "before" and "after" are readable.
  if (horizons.includes(0)) {
    svg.append(el("line", { x1: x(0), x2: x(0), y1: pad.t, y2: pad.t + plotH,
                            stroke: "#3b4450", "stroke-width": 1 }));
  }

  for (const [ecn, cells] of byEcn) {
    const pts = horizons.filter((h) => Number.isFinite(cells.get(h)))
                        .map((h) => [x(h), y(cells.get(h))]);
    if (!pts.length) continue;
    const colour = colourFor(ecn);
    svg.append(el("polyline", {
      points: pts.map(([px, py]) => `${px},${py}`).join(" "),
      fill: "none", stroke: colour, "stroke-width": 2,
      "stroke-linejoin": "round", "stroke-linecap": "round",
    }));
    // A 2px surface ring keeps overlapping endpoints legible without drawing borders.
    const [ex, ey] = pts.at(-1);
    svg.append(el("circle", { cx: ex, cy: ey, r: 4, fill: colour,
                              stroke: "#161a21", "stroke-width": 2 }));
  }

  attachHover(svg, { key, horizons, byEcn, x, y, pad, plotW, plotH, width });
  return svg;
}

// The hovered horizon per chart, kept module-level because renderCharts() rebuilds the SVG
// every tick: without this the crosshair would blink out from under a stationary cursor once
// a second. The value is a horizon, not a pixel, so it stays meaningful if the chart resizes.
const hoverAt = new Map();

// Crosshair + tooltip, drawn in SVG rather than HTML so restoring it after a re-render is
// just calling draw() again, with no positioning relative to a moving page.
function attachHover(svg, { key, horizons, byEcn, x, y, pad, plotW, plotH, width }) {
  const layer = el("g", { "pointer-events": "none" });
  svg.append(layer);

  const nearest = (px) => horizons.reduce(
    (best, h) => (Math.abs(x(h) - px) < Math.abs(x(best) - px) ? h : best), horizons[0]);

  function draw(h) {
    layer.replaceChildren();
    if (h == null) return;
    layer.append(el("line", { x1: x(h), x2: x(h), y1: pad.t, y2: pad.t + plotH,
                              stroke: "#5b6675", "stroke-width": 1 }));

    // Sorted by P&L so the tooltip reads as a ranking at this horizon, which is the question
    // someone hovering a markout curve is actually asking.
    const hits = [...byEcn]
      .map(([ecn, cells]) => [ecn, cells.get(h)])
      .filter(([, v]) => Number.isFinite(v))
      .sort((a, b) => b[1] - a[1]);
    if (!hits.length) return;

    for (const [ecn, v] of hits) {
      layer.append(el("circle", { cx: x(h), cy: y(v), r: 3.5, fill: colourFor(ecn),
                                  stroke: "#161a21", "stroke-width": 2 }));
    }

    const lines = [`${h}s`, ...hits.map(([ecn, v]) => `${ecn}  ${money(v)}`)];
    const chars = Math.max(...lines.map((t) => t.length));
    const boxW = chars * 5.9 + 26;
    const boxH = lines.length * 13 + 8;
    // Flip to the left of the crosshair when the box would run past the plot's right edge.
    const left = x(h) + 10 + boxW > pad.l + plotW ? x(h) - 10 - boxW : x(h) + 10;
    const top = Math.min(pad.t + 2, pad.t + plotH - boxH);

    const box = el("g", { transform: `translate(${left},${top})` });
    box.append(el("rect", { width: boxW, height: boxH, rx: 4,
                            fill: "#0b0e12", stroke: "#242a33", "stroke-width": 1 }));
    lines.forEach((text, i) => {
      const ty = 17 + i * 13;
      if (i > 0) {
        box.append(el("rect", { x: 8, y: ty - 7, width: 7, height: 7, rx: 1.5,
                                fill: colourFor(hits[i - 1][0]) }));
      }
      const label = el("text", { x: i === 0 ? 8 : 21, y: ty, "font-size": 10,
                                 fill: i === 0 ? "#78828f" : "#d7dce3" });
      label.textContent = text;
      box.append(label);
    });
    layer.append(box);
  }

  // Hit target is the whole plot rectangle, not the 2px lines: the crosshair snaps to the
  // nearest horizon, so anywhere in the column counts as pointing at it.
  const hit = el("rect", { x: pad.l, y: pad.t, width: plotW, height: plotH,
                           fill: "transparent", style: "cursor:crosshair" });
  hit.addEventListener("pointermove", (ev) => {
    const box = svg.getBoundingClientRect();
    const h = nearest(((ev.clientX - box.left) / box.width) * width);
    hoverAt.set(key, h);
    draw(h);
  });
  hit.addEventListener("pointerleave", () => { hoverAt.delete(key); draw(null); });
  svg.append(hit);

  draw(hoverAt.get(key));   // restore after a tick rebuilt the chart under the cursor
}

/** Legend: four or fewer series, so every one is named. Text wears text tokens, never the
 *  series colour; the swatch beside it carries identity. */
export function pnlLegend(columns, rows) {
  const ix = Object.fromEntries(columns.map((c, i) => [c, i]));
  const ecns = [...new Set(rows.map((r) => r[ix.ecn]))];
  const box = document.createElement("div");
  box.className = "legend";
  box.innerHTML = ecns.map((e) =>
    `<span><i style="background:${colourFor(e)}"></i>${e}</span>`).join("");
  return box;
}
