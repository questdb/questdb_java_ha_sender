# Sample Sender with Multiple Hosts for HA Ingestion

## Compile

`mvn  -DskipTests clean package`

The QuestDB client version is a Maven property (`questdb.client.version`, default
`1.3.2`, the Maven Central release). The QWP (WebSocket) wire protocol differs
between release and master builds, so the client **must match the target server
build**. To ingest into a server built from master, build with:

```
mvn -DskipTests clean package -Dquestdb.client.version=1.3.5-SNAPSHOT
```

A mismatch is not reported cleanly; it surfaces as `unknown msg_kind 0x18` or
`invalid column type code: 0x0`, or as rows silently not landing.

## Usage Example

```
java -jar target/ilp_sender-1.0-SNAPSHOT.jar \
--protocol qwp \
--addrs enterprise-primary:9000,enterprise-replica:9000,enterprise-replica2:9000 \
--token "$ILP_TOKEN" \
--total-events 50000000 \
--delay-ms 0 \
--num-senders 8 \
--csv ./trades20250728.csv.gz \
--timestamp-from-file false \
--retry-timeout 360000 \
--sender-id ha_sender \
--store-forward-dir /tmp/qdb-sf \
--batch-size 10000 \
--batches-per-transaction 10
```

## Transport (`--protocol`)

- `--protocol qwp` (default): QWP over WebSocket. Adds store-and-forward (un-acked
  frames spill to disk and replay after an outage) and transactional commit. Use the
  server's **HTTP port** (`:9000`) in `--addrs`.
- `--protocol ilp`: the previous HTTP/ILP transport, unchanged. Ignores the QWP-only
  flags below.

QWP-only flags:

- `--sender-id` (default `ha_sender`): store-and-forward key. **Must be unique per
  process/server**; set it explicitly when running more than one process on a host.
  Each worker gets its own `<sender-id>-<worker>` sub-key and spill directory.
- `--store-forward-dir` (default `/tmp/qdb-sf`): where un-acked frames spill. When you
  re-point at a **different server build, clear this directory once**. Stale frames
  from another build replay from the start and fail.
- `--batch-size` (default `10000`): rows per auto-flush (one deferred append). A batch
  is *also* capped at 512 KiB (`autoFlushBytes`, kept under the ~1 MB WebSocket frame
  limit). With these small trade rows the byte cap fills first at ~8-9k rows, so raising
  `--batch-size` much above `10000` has little effect, because the byte cap binds, not
  the row count.
- `--batches-per-transaction` (default `10`): an explicit `flush()` commits every
  `batch-size × batches-per-transaction` rows atomically per table. See
  [Commit cadence](#commit-cadence-freshness-vs-throughput) below.
- `--probe-interval-ms` (default `1000`, `0` disables): interval for the probe thread
  that polls the latest ingested timestamp. See [Probe](#probe-qwp-only) below.
- `--enterprise` (default `false`): request durable acks (`requestDurableAck`), holding
  spilled frames until the server confirms a durable commit. **Enterprise only**: an OSS
  server rejects the WebSocket upgrade, so leave it off against OSS. See
  [Failover and connection narration](#failover-and-connection-narration-qwp).
- `--zone` (default `eu-west-1`): preferred zone for the **query client** (probe), added
  to its connect string as `zone=`. Biases failover toward same-zone instances on
  Enterprise; a no-op on OSS. The ingestion path is zone-blind (it must follow the
  primary), so this does not affect the senders.

## Commit cadence (freshness vs throughput)

On QWP the sender runs in `transactional(true)` mode. That decouples *flushing* from
*committing*:

- **Auto-flush** (triggered by `--batch-size` rows, the 512 KiB byte cap, or a 1 s
  timer, whichever is first) only sends a **deferred append**: data reaches the server
  but is **not** committed to the WAL, so it is not yet queryable or durable.
- **Commit** happens on the *explicit* `flush()` the sender issues every
  `batch-size × batches-per-transaction` rows (and once at close). This is what makes
  the rows visible.

Because the commit trigger is **rows, not time**, how often new data appears depends on
the ingestion rate:

```
seconds between commits (per worker) = (batch-size × batches-per-transaction) / rows-per-second-per-worker
```

- **Backfill / burst:** a large `--batches-per-transaction` means fewer commits and
  higher throughput. At millions of rows/s the commit still lands sub-second, so a
  live `SELECT count()` ticks up several times per second. The 1 s auto-flush timer
  never fires (the byte cap trips in milliseconds).
- **Real-time / low-rate:** the same large value means data can sit uncommitted (invisible)
  for many seconds. Keep `--batches-per-transaction` small when freshness matters more
  than raw throughput.

## Probe (QWP only)

When the transport is QWP, a separate thread polls the latest ingested timestamp and
prints it to stdout once per `--probe-interval-ms` (default `1000` ms; `0` disables). It
runs `select timestamp from trades limit -1` over a **QWP query client** and is fully
independent of the senders, so it does not affect ingestion.

```
[probe] latest trades timestamp = 2026-07-01T14:31:40.591545Z (raw=1782916300591545)
```

The query client uses the **same host list and token/auth** as the senders and enables
**failover** when more than one host is given (`ws|wss::addr=h1,h2,...;...;failover=on`), so
if a host stops responding it moves to the next one automatically and narrates the
transition (see [Failover and connection narration](#failover-and-connection-narration-qwp)).
Before the `trades` table exists it prints `[query client] server error: table does not
exist` each interval, then switches to timestamps once ingestion begins. ILP ignores
`--probe-interval-ms`.

On connect the probe prints the instance actually serving it:

```
[query client] connected, serving node=<nodeId> role=<primary|replica|standalone> zone=<zoneId> cluster=<clusterId>
```

On OSS `node`/`zone` are `(none)` (the server advertises neither); on Enterprise they
identify the serving host and its zone. The preferred zone is set with `--zone` (default
`eu-west-1`), added to the query client's connect string. `withTarget`
(primary/replica/any) is available in the API but not wired to a flag yet.

## Failover and connection narration (QWP)

Pass more than one host in `--addrs` (comma-separated) and the QWP clients fail over across
them automatically when a host stops responding. Both clients narrate the transitions on
stdout so an outage and recovery are visible.

The **ingestion client** (one per worker) reports connection-state changes via the client's
`SenderConnectionListener`, prefixed `[ingestion client <sender-id>-<worker>]`:

```
[ingestion client ha_sender-0] connected to h1:9000
[ingestion client ha_sender-0] connection lost to h1:9000 (...), will retry
[ingestion client ha_sender-0] endpoint h1:9000 failed (...), trying next
[ingestion client ha_sender-0] failed over h1:9000 -> h2:9000
[ingestion client ha_sender-0] reconnected to h2:9000
[ingestion client ha_sender-0] all endpoints unreachable, backing off
```

(also `auth failed` and `reconnect budget exhausted` terminal states.)

The **query client** (the probe) reports, prefixed `[query client]`:

```
[query client] connection lost (...), will retry
[query client] failed over -> node=<nodeId> role=<primary|replica> zone=<zoneId>
[query client] connection restored
```

Durability note: `transactional(true)` and store-and-forward are plain OSS features and are
always on for QWP. **Durable ack** (`--enterprise`) is the extra "durably persisted"
confirmation, which only **QuestDB Enterprise** implements; an OSS server rejects it during
the WebSocket upgrade (`server does not support durable ack`), which the ingestion client
narrates as an `endpoint failed` and retries. Leave `--enterprise` off against OSS.

## Timestamps and throughput

The goal here is **maximum ingestion throughput**: run many workers (`--num-senders`)
and/or many processes against the server at once, with **no out-of-order (O3)** writes
slowing the server down. The exact row timestamps do not matter for this workload.

That is why the default (`--timestamp-from-file false`, `--seconds-offset 0`) uses
`atNow()`: the server stamps rows on arrival, so every worker's stream is naturally
non-decreasing and O3 is avoided by construction. The table is auto-created with a
**microsecond** designated timestamp, which is fine. Nanosecond resolution buys
nothing here, since we are replaying the same CSV in a loop and only care about volume.

Note: over QWP, `atNow()` stamps a whole auto-flush batch with one timestamp (the
server defers the stamp per batch). That is still O3-safe and lands every row on a
normal table. It only matters if the target table has **dedup upsert keys** on the
designated timestamp, where same-timestamp rows would collapse. In that case use
`--timestamp-from-file` or `--seconds-offset` so each row carries its own timestamp.

**Single-worker QWP exception:** when the transport is QWP and `--num-senders 1`, the
default path stamps each row with the current microsecond **client-side**
(`sender.at(Instant.now())`) instead of `atNow()`. A single thread produces monotonic
timestamps, so O3 is still avoided, and this sidesteps the per-batch stamping above
(giving near-distinct per-row timestamps). ILP, or QWP with more than one worker, keep
using server-side `atNow()`, which stays O3-safe across concurrent senders.

## Benchmarking throughput

On completion the sender prints the ingestion time and rate, for example:

```
All workers completed. protocol=qwp events=30000000 elapsed=3.398 s throughput=8,829,671 rows/s
```

The timer starts right before the workers begin sending (after the CSV is loaded) and
stops when they all finish, so it measures ingestion only. It is a **client-side** time:
QWP commits and the server WAL apply are asynchronous, so always confirm the final
`SELECT count()` on the server has settled after the run.

How to measure meaningfully:

- **Use a large volume (30M+ rows).** Smaller runs are dominated by JVM/connection
  warmup and under-report throughput (a 5M run measured ~half the rate of a 30M run).
- **Run each config several times and take the median/best.** Run-to-run variance is
  large (roughly ±20% here), so a single number is unreliable.
- **Reset between runs:** `DROP TABLE trades`, delete the `--store-forward-dir`, and keep
  the server otherwise idle.

### Reaching 8M+ rows/s on a single machine

Measured on a local Mac ingesting into a stock **QuestDB 9.4.3** on the *same machine*
(loopback, server left at its **default configuration**, no server-side tuning), QWP
sustained **over 8 million rows/s** for a 30M-row load with:

```
java -jar target/ilp_sender-1.0-SNAPSHOT.jar \
  --protocol qwp \
  --addrs localhost:9000 \
  --total-events 30000000 \
  --num-senders 4 \
  --delay-ms 0 \
  --batch-size 10000 \
  --batches-per-transaction 100
```

What moved the needle, and what did not:

- **`--batches-per-transaction 100`** (fewer commits) was the one lever with a real,
  repeatable gain: roughly `6.5-7.5M` rows/s at the default `10` versus `8-9M` at `100`.
- **`--num-senders` beyond 4 did not help**: 8 workers matched 4. Ingestion is
  server/connection-bound past that point, not client-parallelism-bound.
- **`--batch-size` above ~10k did not help**: the 512 KiB byte cap fills first, so more
  rows per batch changes nothing.
- For reference, ILP (`--protocol ilp`) on the same 30M load ran ~3.3M rows/s, so QWP
  was ~2.6x faster here.

Build the client to match the server (see [Compile](#compile)); these numbers used the
QWP WebSocket transport.


For a similar sender, but for telemetry data, see the instructions (and license info) at [the Telemetry Readme](./TelemetryReadme.md)
