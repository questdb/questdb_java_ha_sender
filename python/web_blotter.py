#!/usr/bin/env python3
"""Web blotter: a tiny local server that queries QuestDB and serves a live table in the browser.

Rendering happens in the browser (fast, local, flashing cells) and only the queries cross the
network - so it avoids the terminal-over-SSH bottleneck of the text blotter. The token stays in
this process (never sent to the browser), and the browser only ever talks to localhost, so there
is no CORS or cert hassle:

    browser  ->  http://localhost:PORT  (this server)  ->  QuestDB (QWP, with your token)

Same query params as blotter.py: --table (+ --where/--limit) or a verbatim --query. Auth/TLS via
--token/--username/--password/--tls-verify, or --token-file/--token-label. --rate sets the browser
poll rate. Open the printed URL. Ctrl+C to quit.

    python web_blotter.py --table core_price_lv --where "symbol='EURUSD'" --limit -20 --rate 5 \
        --addr host:9000 --token-file ~/prj/python/ent_tokens.txt --token-label disaster \
        --tls-verify unsafe_off [--port 8080]

Requires the QWP/egress build of the client (see README.md).
"""

import argparse
import json
import os
import sys
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from questdb.ingress import Client


def use_tls(args):
    return bool(args.tls or args.token or (args.username and args.password))


def build_conf(args):
    scheme = "qwpwss" if use_tls(args) else "qwpws"
    parts = [f"{scheme}::addr={args.addr};"]
    if args.token:
        parts.append(f"token={args.token};")
    elif args.username and args.password:
        parts.append(f"username={args.username};password={args.password};")
    if use_tls(args) and args.tls_verify == "unsafe_off":
        parts.append("tls_verify=unsafe_off;")
    return "".join(parts)


def build_sql(table, where, limit):
    parts = [f"select * from {table}"]
    if where and where.strip():
        c = where.strip()
        first = c.split(None, 1)[0].lower()
        parts.append(c if first == "where" else "WHERE " + c)
    parts.append(f"limit {limit}")
    return " ".join(parts)


def load_token(args):
    if not args.token_file:
        return
    with open(os.path.expanduser(args.token_file), encoding="utf-8") as fh:
        content = fh.read()
    if args.token_label:
        for line in content.splitlines():
            line = line.strip()
            parts = line.split(None, 1)
            if len(parts) == 2 and parts[0] == args.token_label:
                args.token = parts[1].strip()
                return
        sys.exit(f"[error] label '{args.token_label}' not found in {args.token_file}")
    args.token = content.strip()


def df_to_payload(df):
    """polars DataFrame -> JSON-friendly {columns, rows} (datetimes as ISO strings)."""
    rows = []
    for r in df.iter_rows():
        rows.append([v.isoformat(sep=" ", timespec="milliseconds") if isinstance(v, datetime) else v
                     for v in r])
    return {"columns": df.columns, "rows": rows}


HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>__TITLE__</title>
<style>
  body { background:#0b0e14; color:#cbd5e1; font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
         margin:0; padding:16px; }
  h1 { font-size:14px; color:#7dd3fc; margin:0 0 4px; font-weight:600; }
  .sql { color:#64748b; font-size:12px; margin-bottom:2px; word-break:break-all; }
  .status { color:#94a3b8; font-size:12px; margin-bottom:10px; }
  .status b { color:#e2e8f0; font-weight:600; }
  table { border-collapse:collapse; font-size:13px; }
  th,td { padding:3px 12px; text-align:right; white-space:nowrap; }
  th { color:#7dd3fc; border-bottom:1px solid #1e293b; position:sticky; top:0; background:#0b0e14; }
  td.txt, th.txt { text-align:left; color:#e2e8f0; }
  tbody tr:nth-child(even){ background:#0f1420; }
  td.up   { animation: fu 0.9s ease-out; }
  td.down { animation: fd 0.9s ease-out; }
  @keyframes fu { 0%{ background:#14532d; color:#4ade80 } 100%{} }
  @keyframes fd { 0%{ background:#7f1d1d; color:#f87171 } 100%{} }
  .err { color:#f87171; }
</style></head>
<body>
  <h1>__TITLE__</h1>
  <div class="sql">__SQL__</div>
  <div class="status" id="status">connecting...</div>
  <table><thead id="thead"></thead><tbody id="tbody"></tbody></table>
<script>
  const INTERVAL = __INTERVAL__;
  let prev = null, prevKeyed = null;

  function fmt(v){
    if (v === null) return "";
    if (typeof v === "number") return Number.isInteger(v) ? v : v.toLocaleString(undefined,{maximumFractionDigits:8});
    return v;
  }
  function render(data, ms){
    const cols = data.columns, rows = data.rows;
    document.getElementById("thead").innerHTML =
      "<tr>" + cols.map(c => `<th class="${/[a-z_]/i.test(c)?'txt':''}">${c}</th>`).join("") + "</tr>";
    // Flash cells whose value changed vs the same (row,col) last frame.
    const body = rows.map((r,i) => "<tr>" + r.map((v,j) => {
      const isNum = typeof v === "number";
      let cls = isNum ? "" : "txt";
      if (prev && prev.rows[i] && isNum && typeof prev.rows[i][j] === "number" && v !== prev.rows[i][j])
        cls += v > prev.rows[i][j] ? " up" : " down";
      return `<td class="${cls.trim()}">${fmt(v)}</td>`;
    }).join("") + "</tr>").join("");
    document.getElementById("tbody").innerHTML = body;
    const now = new Date().toISOString().replace("T"," ").replace("Z"," UTC").slice(0,23);
    document.getElementById("status").innerHTML =
      `<b>${rows.length}</b> rows | q=<b>${ms}</b> ms | ${(1000/INTERVAL).toFixed(0)} Hz target | ${now}`;
    prev = data;
  }
  async function tick(){
    const t0 = performance.now();
    try {
      const resp = await fetch("/data", {cache:"no-store"});
      const data = await resp.json();
      const ms = Math.round(performance.now() - t0);
      if (data.error) document.getElementById("status").innerHTML = `<span class="err">query error: ${data.error}</span>`;
      else render(data, ms);
    } catch (e) {
      document.getElementById("status").innerHTML = `<span class="err">connection error: ${e}</span>`;
    }
    setTimeout(tick, INTERVAL);
  }
  tick();
</script>
</body></html>
"""


def main(argv):
    ap = argparse.ArgumentParser(description="Web blotter (local server + browser UI)")
    ap.add_argument("--table", default=None)
    ap.add_argument("--query", default=None, help="full SQL; when set, --table/--where/--limit ignored")
    ap.add_argument("--where", default=None)
    ap.add_argument("--limit", type=int, default=-20)
    ap.add_argument("--rate", type=float, default=5.0, help="browser poll rate (Hz)")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="127.0.0.1",
                    help="bind address; 0.0.0.0 exposes it externally (NO auth on the web server - "
                         "scope your security group). Default 127.0.0.1.")
    ap.add_argument("--addr", default="localhost:9000")
    ap.add_argument("--token", default=None)
    ap.add_argument("--token-file", default=None)
    ap.add_argument("--token-label", default=None)
    ap.add_argument("--username", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--tls-verify", choices=["on", "unsafe_off"], default="on")
    args = ap.parse_args(argv)
    load_token(args)

    if args.query and args.query.strip():
        sql = args.query.strip().rstrip(";").strip()
    elif args.table:
        if abs(args.limit) > 100:
            args.limit = 100 if args.limit > 0 else -100
        sql = build_sql(args.table, args.where, args.limit)
    else:
        print("provide --table or --query", file=sys.stderr)
        return 2
    interval_ms = max(50, int(1000 / max(0.1, args.rate)))

    conf = build_conf(args)
    client = Client.from_conf(conf)
    lock = threading.Lock()
    page = (HTML.replace("__TITLE__", f"blotter: {args.table or 'query'}")
                .replace("__SQL__", " ".join(sql.split()))
                .replace("__INTERVAL__", str(interval_ms)))

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"   # keep-alive: reuse one TCP connection across polls

        def log_message(self, *a):
            pass

        def _send(self, code, ctype, body):
            b = body.encode("utf-8") if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            try:
                self.wfile.write(b)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            if self.path == "/" or self.path.startswith("/?"):
                self._send(200, "text/html; charset=utf-8", page)
            elif self.path.startswith("/data"):
                try:
                    with lock:
                        df = client.query(sql).to_polars()
                    self._send(200, "application/json", json.dumps(df_to_payload(df)))
                except Exception as e:  # noqa: BLE001
                    self._send(200, "application/json", json.dumps({"error": str(e)}))
            else:
                self._send(404, "text/plain", "not found")

    class Server(ThreadingHTTPServer):
        daemon_threads = True        # Ctrl+C exits immediately, even with a request in flight
        allow_reuse_address = True

    httpd = Server((args.host, args.port), Handler)
    shown = "localhost" if args.host in ("127.0.0.1", "localhost") else "<this-host>"
    print(f"[web-blotter] serving on {args.host}:{args.port}  ->  open http://{shown}:{args.port}/  "
          f"(Ctrl+C to quit)")
    if args.host == "0.0.0.0":
        print("[web-blotter] bound to 0.0.0.0 - reachable on this host's IP; the web server has NO "
              "auth, so make sure the port is firewalled to trusted clients.")
    print(f"[web-blotter] SQL: {' '.join(sql.split())}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
