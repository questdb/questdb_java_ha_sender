// The DATA layer: the only file that knows QuestDB exists.
//
// Today it runs the queries here in Node, over QWP, using @questdb/nodejs-client. When
// @questdb/browser-client is installable and the server carries the browser-upgrade patch,
// this whole file is replaced by a pass-through proxy for /read/v1 and /exec, the query moves
// into the page, and neither server.mjs nor public/ has to change.
import { connectQwpNodeClient, QwpEgressQueryError } from "@questdb/nodejs-client";
import { PANELS, sqlFor, symbolsSql } from "./queries.mjs";

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

  async close() {
    if (this.db) await this.db.close();
    this.db = null;
  }
}
