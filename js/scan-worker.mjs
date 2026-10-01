// One scan worker: its OWN QWP client, on its own thread.
//
// Why a thread at all. The client decodes result batches in JavaScript, and zstd is a pure-JS
// decoder too, so decoding is CPU work on whatever thread owns the connection. Several
// "readers" sharing one event loop therefore only overlap I/O waits, and on a fast link there
// is no wait left to hide: measured throughput was identical at 4 and 16 readers (3.10M vs
// 3.11M rows/s). Real parallelism needs real threads.
//
// Workers never send rows to the main thread on the hot path. Counters live in shared memory
// and are bumped with Atomics, so a batch costs two atomic adds and nothing is serialised.
import { parentPort, workerData } from "node:worker_threads";
import { connectQwpNodeClient } from "@questdb/nodejs-client";

const { conf, id } = workerData;

// Shared counter slots, by index. Written by every worker, read by the main thread.
const ROWS = 0, BATCHES = 1, CHUNKS = 2;
// Shared control slots: the work-queue cursor, and the abort flag.
const CURSOR = 0, ABORTED = 1;

let db = null;
let current = null;        // the in-flight query, so an abort can cancel it
let aborting = false;

/** Copy the last `sample` rows out of the reusable view before it is recycled. */
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

async function run({ sqls, counters, control, sample, tailEveryMs }) {
  const counts = new BigInt64Array(counters);
  const ctl = new Int32Array(control);
  let lastTail = 0;

  const lease = await db.borrowQuery();
  try {
    for (;;) {
      if (Atomics.load(ctl, ABORTED) === 1) break;
      // Work STEALING, not a fixed split: slices of equal row count are not slices of equal
      // time, so a worker that drew cheap ones keeps pulling instead of idling.
      const index = Atomics.add(ctl, CURSOR, 1);
      if (index >= sqls.length) break;

      const query = await lease.queryViews(sqls[index], (batch) => {
        Atomics.add(counts, ROWS, BigInt(batch.rowCount));
        Atomics.add(counts, BATCHES, 1n);

        // The only rows that ever cross the thread boundary, a few times a second.
        const now = Date.now();
        if (now - lastTail < tailEveryMs) return;
        lastTail = now;
        parentPort.postMessage({
          type: "tail",
          columns: batch.columns.map((c) => c.name),
          rows: copyTail(batch, sample),
        });
      }, { timeoutMs: 0 });

      current = query;
      try {
        await query.completion;
      } finally {
        current = null;
      }
      Atomics.add(counts, CHUNKS, 1n);
    }
  } finally {
    await lease.close();
  }
}

parentPort.on("message", async (msg) => {
  try {
    if (msg.type === "abort") {
      aborting = true;
      await current?.cancel().catch(() => {});
      return;
    }
    if (msg.type === "run") {
      aborting = false;
      await run(msg);
      parentPort.postMessage({ type: "done" });
    }
  } catch (error) {
    // An abort tears a query down on purpose; that is not a failure worth reporting.
    parentPort.postMessage({
      type: "done",
      error: aborting ? null : String(error?.message ?? error),
    });
  }
});

// Connect once, at startup: a scan should not pay for a handshake per run.
try {
  db = await connectQwpNodeClient(conf);
  parentPort.postMessage({ type: "ready", id });
} catch (error) {
  parentPort.postMessage({ type: "ready", id, error: String(error?.message ?? error) });
}
