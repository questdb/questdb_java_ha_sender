// The DATA layer: the only file that knows QuestDB exists.
//
// Today it runs the queries here in Node, over QWP, using @questdb/nodejs-client. When
// @questdb/browser-client is installable and the server carries the browser-upgrade patch,
// this whole file is replaced by a pass-through proxy for /read/v1 and /exec, the query moves
// into the page, and neither server.mjs nor public/ has to change.
import { connectQwpNodeClient, QwpEgressQueryError } from "@questdb/nodejs-client";
import {
  OHLC_INTERVALS, OHLC_LOOKBACKS, PANELS, ohlcLastRowSql, ohlcSql, ohlcSymbolsSql,
  scanChunks, sqlFor, symbolsSql,
} from "./queries.mjs";

/**
 * Copy the last `sample` rows out of a reusable batch view.
 *
 * Called only when a progress frame is due, never on the hot path: the view's buffers are
 * recycled as soon as the callback returns, so anything shown in the page has to be copied,
 * and copying is exactly what the scan is otherwise avoiding.
 */
function copyTail(batch, sample) {
  const from = Math.max(0, batch.rowCount - sample);
  const out = [];
  for (let r = from; r < batch.rowCount; r++) {
    const row = new Array(batch.columnCount);
    for (let c = 0; c < batch.columnCount; c++) row[c] = batch.get(r, c);
    out.push(row);
  }
  return out;
}

export class Data {
  constructor(conf) {
    this.conf = conf;
    this.db = null;
  }

  async connect() {
    // Eager: an unreachable server should fail at startup rather than serving a dashboard
    // that can never show anything.
    this.db = await connectQwpNodeClient(this.conf);
  }

  /**
   * Run every panel once and return {panel: {columns, rows, ms}} plus any per-panel error.
   *
   * A fresh lease per tick, not one held for the process lifetime: a terminal rejection is
   * latched by the connection and raised by the next call on that lease, so a long-lived
   * lease can be poisoned permanently. This is the same lesson the Python dashboard learned.
   */
  async tick(symbol) {
    const out = {};
    const lease = await this.db.borrowQuery();
    try {
      for (const panel of PANELS) {
        const started = performance.now();
        try {
          out[panel] = { ...(await this.#run(lease, sqlFor(panel, symbol))),
                         ms: performance.now() - started };
        } catch (error) {
          if (!(error instanceof QwpEgressQueryError)) throw error;
          // A rejected statement is this panel's problem, not the dashboard's: report it and
          // keep the other three drawing.
          out[panel] = { error: `status=${error.status} ${error.message}`,
                         ms: performance.now() - started };
        }
      }
    } finally {
      await lease.close();
    }
    return out;
  }

  /** Symbols active in the last 30s, sorted, for the filter dropdown. */
  async symbols() {
    const lease = await this.db.borrowQuery();
    try {
      const { rows } = await this.#run(lease, symbolsSql());
      return rows.map((r) => String(r[0])).sort();
    } finally {
      await lease.close();
    }
  }

  async #run(lease, sql) {
    const query = await lease.query(sql);
    const rows = [];
    let columns = null;
    for await (const batch of query) {
      // batch.columns is a PROPERTY, an array of QwpResultColumn, each extending
      // QwpResultColumnSchema so it carries .name. Taking the names off the batch means the
      // page renders whatever the SQL selected, with no schema hardcoded anywhere.
      columns ??= batch.columns.map((c) => c.name);
      for (const row of batch.rows()) rows.push(row);
    }
    await query.completion;
    return { columns, rows };
  }

  /**
   * Stream a scan of `table`'s last `rows` rows, reporting progress as it goes.
   *
   * Nothing is accumulated: counters, plus a rolling tail of the last `sample` rows, are the
   * only things that outlive a batch. Memory is therefore flat whether the scan covers a
   * thousand rows or two hundred million, which is the whole point of the demo.
   *
   * queryViews() rather than query(): it hands back a REUSABLE batch view and never
   * materialises rows into JS arrays, so the decoder is not fighting the garbage collector
   * for the duration of the scan. The view is invalid once the callback returns, so the tail
   * rows are copied out before then.
   */
  async scan({ table, rows: limit, readers = 4, chunkRows = 500_000, chunks = 0,
               projection = "all", sample = 10, progressMs = 100 }, onProgress, signal) {
    const workers = Math.max(1, Math.trunc(readers));

    // Slice count is derived from a bounded number of ROWS PER QUERY, not from a fixed
    // number of slices. QuestDB caps how long one statement may run (query.timeout, 60s on
    // the cluster this was built against), so what has to stay bounded is the work in a
    // single query, not the work in the scan. A fixed slice count fails exactly where it
    // matters: 64 slices of 200M rows is 3.1M rows each, which at per-reader speed is ~86s
    // and times out, while the same 64 slices of 4M rows finish comfortably.
    //
    // At least one slice per reader, so nobody sits idle on a small scan.
    const sliced = Math.ceil(Math.max(1, limit) / Math.max(1, chunkRows));
    const sqls = scanChunks(table, limit, chunks || Math.max(workers, sliced), projection);
    const started = performance.now();

    let rows = 0, batches = 0, columns = null, tail = [], chunksDone = 0, lastSent = 0;
    const snapshot = (done) => ({
      table, sql: sqls[0] ?? "", chunks: sqls.length, chunksDone, readers: workers,
      projection,
      columns, rows, batches, tail, done, ms: performance.now() - started,
    });

    const queue = sqls.slice();
    const running = new Set();
    const abort = () => { for (const q of running) q.cancel().catch(() => {}); };
    signal?.addEventListener("abort", abort, { once: true });

    // One lease per reader, held for that reader's whole share of the queue. Leases are
    // serial, so concurrency comes from running several of them, not from one.
    const reader = async () => {
      const lease = await this.db.borrowQuery();
      try {
        while (queue.length > 0 && !signal?.aborted) {
          const sql = queue.shift();
          const query = await lease.queryViews(sql, (batch) => {
            // THE HOT PATH, and deliberately the whole of it: two counters and a row count
            // the batch already knows. No row is visited, nothing is materialised, and the
            // cost per batch does not depend on how many rows it carries.
            rows += batch.rowCount;
            batches += 1;

            const now = performance.now();
            if (now - lastSent < progressMs) return;
            lastSent = now;

            if (columns === null) columns = batch.columns.map((c) => c.name);
            // Only now, ~10 times a second, are any rows read at all, and only `sample` of
            // them. Copied, because the view is recycled when this callback returns.
            // An EMPTY batch must not clear the tail: slicing a small table into many
            // ranges yields plenty of zero-row batches, and whichever one landed on the
            // last progress tick would leave the page showing no rows at all.
            if (batch.rowCount > 0) tail = copyTail(batch, sample);
            onProgress(snapshot(false));
          }, {
            // The client's session default is 15s, which a slice of millions of rows runs
            // well past. QuestDB applies its own server-side query.timeout independently,
            // and that one is what the slicing above is sized against.
            timeoutMs: 0,
          });
          running.add(query);
          try {
            await query.completion;
          } finally {
            running.delete(query);
          }
          chunksDone += 1;
        }
      } finally {
        await lease.close();
      }
    };

    try {
      // allSettled, not all: Promise.all rejects on the FIRST failing reader and returns
      // while its siblings are still draining, so scan() would resolve with connections
      // still borrowed and the next scan would find an empty pool. Every reader has to
      // reach its finally and give its lease back before this method returns.
      const results = await Promise.allSettled(
        Array.from({ length: Math.min(workers, sqls.length) }, () => reader()));
      const failed = results.find((r) => r.status === "rejected");
      if (failed && !signal?.aborted) throw failed.reason;
      if (!signal?.aborted) onProgress(snapshot(true));
    } finally {
      signal?.removeEventListener("abort", abort);
    }
  }

  /**
   * Candles for one instrument, with the window anchored to the newest row in the table.
   *
   * Three cheap statements on one lease: where the data ends, which symbols were trading
   * near that point, and the bars themselves. Anchoring costs one extra query and buys a
   * chart that still works on an instance whose writer has stopped.
   */
  async ohlc({ symbol, interval = "5s", lookback = "30m" }) {
    if (!(interval in OHLC_INTERVALS)) throw new Error(`unknown interval: ${interval}`);
    if (!(lookback in OHLC_LOOKBACKS)) throw new Error(`unknown lookback: ${lookback}`);

    const started = performance.now();
    const lease = await this.db.borrowQuery();
    try {
      const anchor = await this.#run(lease, ohlcLastRowSql(symbol));
      const lastNs = anchor.rows[0]?.[0];
      if (lastNs === null || lastNs === undefined) {
        return { symbol, interval, lookback, columns: [], rows: [], symbols: [],
                 ms: performance.now() - started, empty: "no rows in fx_trades" };
      }

      // Timestamps are nanoseconds here; Date works in milliseconds. Dividing through
      // BigInt first keeps the epoch exact, since ns since 1970 is well past 2^53.
      const lastMs = Number(BigInt(lastNs) / 1000000n);
      const toIso = new Date(lastMs).toISOString();
      const fromIso = new Date(lastMs - OHLC_LOOKBACKS[lookback] * 1000).toISOString();
      // The picker lists what traded near the anchor, not what is trading now.
      const symFromIso = new Date(lastMs - 1800 * 1000).toISOString();

      const picker = await this.#run(lease, ohlcSymbolsSql(symFromIso, toIso));
      const symbols = picker.rows.map((r) => String(r[0])).sort();
      const chosen = symbol || symbols[0];
      if (!chosen) {
        return { symbol: null, interval, lookback, columns: [], rows: [], symbols,
                 ms: performance.now() - started, empty: "no symbols in the window" };
      }

      const bars = await this.#run(lease, ohlcSql({ symbol: chosen, interval, fromIso, toIso }));
      return {
        symbol: chosen, interval, lookback, symbols,
        columns: bars.columns, rows: bars.rows,
        sql: ohlcSql({ symbol: chosen, interval, fromIso, toIso }).trim(),
        from: fromIso, to: toIso,
        ms: performance.now() - started,
      };
    } finally {
      await lease.close();
    }
  }

  async close() {
    if (this.db) await this.db.close();
    this.db = null;
  }
}
