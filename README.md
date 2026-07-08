# Sample Sender with Multiple Hosts for HA Ingestion

## Compile

`mvn  -DskipTests clean package`

The QuestDB client version is a Maven property (`questdb.client.version`, default
`1.3.6-SNAPSHOT`). This build **requires** it: the QWP transport and the lifecycle
`switch` features used here (`switch status`, the current connection-event set) are
not stable in earlier clients. `1.3.6-SNAPSHOT` is **not on Maven Central** — install
it locally first, from the `feat/connect-timeout` branch of `java-questdb-client`:

```
# in a checkout of java-questdb-client on feat/connect-timeout:
mvn -DskipTests clean install
```

Then build this project normally (`mvn -DskipTests clean package`), or pin another
build with `-Dquestdb.client.version=...`. The QWP (WebSocket) wire protocol **must
match the target server build** — a mismatch is not reported cleanly; it surfaces as
`unknown msg_kind 0x18` or `invalid column type code: 0x0`, or as rows silently not
landing.

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
--connect-timeout-ms 3000 \
--sender-id ha_sender \
--store-forward-dir /tmp/qdb-sf \
--batch-size 10000 \
--batches-per-transaction 10
```

`--retry-timeout` (default `360000` ms) is the overall "keep retrying" budget and applies to
**both** transports: it drives `retryTimeoutMillis` on ILP and `reconnectMaxDurationMillis` on
QWP. That is a different timeout from `--connect-timeout-ms` (below), which bounds a single
connect attempt.

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
- `--connect-timeout-ms` (default `3000`, `0` uses the OS default): upper bound on a **single**
  TCP connect attempt, for both the senders (`connectTimeoutMillis`) and the probe
  (`connect_timeout=`). Without it a black-holed host (route accepts, never answers) rides the
  OS connect timeout (~minutes) before failing over to the next `--addrs` host; with it, failover
  happens in seconds. The ILP path is left unbounded so `--protocol ilp` stays identical.
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
[probe] latest trades timestamp = 2026-07-01T14:31:40.591545Z (raw=1782916300591545) served by role=PRIMARY node=n1 zone=eu-west-1
```

The `served by role=...` is the **live** lifecycle role (`current_role` from `switch status`)
of the node currently answering the probe, run on it each poll. It is authoritative: the QWP
handshake `SERVER_INFO` role is only refreshed on a *reconnect*, so after an in-place
primary&rarr;replica switch (which does not drop the read connection) that role goes stale,
whereas `switch status` always reflects the truth, so the role flips the moment the serving
node changes role. `node`/`zone` come from the handshake `SERVER_INFO`. While a switch is in
flight, `(switching -> ROLE)` is appended (the `target_role` differs from `current_role`). If
the status query is unavailable (OSS, or the token lacks `SYSTEM ADMIN`), the probe falls back
to the handshake role, explicitly labelled `(handshake role; live 'switch status' unavailable,
may be stale)`.

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

(also an `auth failed` terminal state; any other event kind the client emits is narrated
generically by the `default` branch.)

A failed QWP WebSocket upgrade is otherwise hard to read from the raw client error, so
wherever it surfaces (the `endpoint ... failed` line and the fatal `Sender N got error` /
`Worker failed` lines) the sender appends a hint. It uses the client's **typed** signals, not
string-matching, so it tells the causes apart:

- **`QwpAuthFailedException` (HTTP 401/403)** — bad/expired credentials or missing permission:
  `-- auth rejected (HTTP 401): bad/expired credentials or missing permission; check --token or --user/--password`
- **`WebSocketUpgradeException.isRoleMismatch()` (HTTP 421)** — the endpoint is a replica, i.e.
  no primary is available among `--addrs` to accept writes:
  `-- endpoint is not writable (role=REPLICA): no primary available among --addrs to accept writes`
- **`WebSocketUpgradeException` status 400** — a missing or malformed token (e.g. an unset token
  env var); the server refuses the upgrade before it can return a 401:
  `-- HTTP 400 on the upgrade: likely a missing or malformed ILP auth token (e.g. an unset token env var); check --token or --user/--password`

```
Sender 0 got error: io.questdb.client...WebSocketUpgradeException: WebSocket upgrade failed: HTTP/1.1 400 Bad request -- HTTP 400 on the upgrade: likely a missing or malformed ILP auth token (e.g. an unset token env var); check --token or --user/--password
```

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

## Row id and dedup (`trade_id`)

Every row the trades sender writes carries a `trade_id` column of the form
`<worker>-<sequence>` (for example `0-1`, `0-2`, ...), where the sequence is 1-based and
monotonic per sender. It is a high-cardinality **VARCHAR**, deliberately not a symbol (each
row is unique, so a symbol column would blow up the symbol table).

Two uses:

- **Completeness check, timestamp-independent.** Each sender emits a contiguous `1..N`, so you
  can confirm nothing was lost and spot gaps regardless of how rows were timestamped.
- **Idempotent replay via dedup.** Store-and-forward is at-least-once: on a failover the client
  replays the un-acked in-flight transaction to the new primary, re-writing the rows the old
  primary had already durably persisted (send 100,000 rows, kill the primary mid-transaction,
  and you land ~100,101). Enabling dedup collapses those back to exactly once:

  ```sql
  ALTER TABLE trades DEDUP ENABLE UPSERT KEYS(timestamp, trade_id);
  ```

  Dedup keys must include the designated timestamp, so the match is on `(timestamp, trade_id)`.
  `trade_id` also keeps legitimately-distinct rows that share a microsecond apart (common at
  millions of rows/s), which a `(symbol, side, timestamp)` key alone would wrongly merge.

**Requirement:** dedup only catches replays when the row carries a **client-side timestamp**, so
the timestamp is baked into the spilled frame and reproduced identically on replay. That means a
single worker (the default `at(Instant.now())` path) or `--timestamp-from-file` /
`--seconds-offset`. Under multi-worker `atNow()` the new primary assigns a fresh timestamp on
replay and dedup cannot match. See [Timestamps and throughput](#timestamps-and-throughput).

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

## Primary failover watchdog

`qdb-primary-watchdog.sh` is a standalone Bash watchdog for an Enterprise cluster. Give it the
ordered list of every node's lifecycle admin endpoint (`host:9003`); it finds (or, on a
fresh cluster, elects) the current primary, monitors its role, and on failure promotes another
node via the lifecycle REST API.

Behaviour:

- **Detect / bootstrap:** on startup it sweeps the list and monitors whichever node reports
  `currentRole:PRIMARY`. If **no** node is primary yet (a freshly-started cluster) it promotes
  the first healthy node in list order, so the cluster always ends up with a primary. If every
  node is unhealthy it keeps retrying rather than exiting. Split-brain is not a concern here:
  QuestDB stops the offending node if two ever claim primary.
- **Monitor the role, not just health:** each interval it reads the primary's `currentRole`.
  A node that stops responding **or** is silently demoted to `REPLICA` out-of-band both count
  as a failure, so it reacts to a demotion even while the node is still reachable.
- **Grace period:** after detecting loss it waits `QDB_WD_GRACE_PERIOD` seconds (default `5`,
  `0` disables) before acting, then re-checks. This lets an operator do a manual switch (demote
  the primary, promote a replica) without the watchdog racing them, as long as it completes
  within the window.
- **Adopt if already switched:** it then re-checks the cluster; if another node is already
  `PRIMARY` (an operator switch, or the old primary recovered) it simply adopts it, no new switch.
- **Step down first:** otherwise it sends a best-effort `{"role":"replica"}` to the old primary,
  so a false positive (it is actually still up) cannot leave two primaries.
- **Promote the first healthy node in order:** it then promotes the first **healthy** node in
  list order (preferred/earliest first, skipping the dead one), retrying with exponential
  backoff. Unreachable candidates are skipped. With `a,b,c`, if `b` is primary and dies it fails
  back to `a` when `a` is healthy, and only reaches `c` if `a` is unhealthy or cannot be promoted.
  It then monitors the new primary.
- **Never exits on its own:** if no node can be promoted (whole cluster down) it keeps retrying
  every interval until one can take over. It stops only on `SIGINT`/`SIGTERM`.

All endpoints are `https` (Enterprise only).

### Configuration

Required, no defaults: the node list and `QDB_REST_TOKEN`. Everything else has a default and is
overridable via a `QDB_WD_*` variable. Precedence: **CLI arg (servers) > environment > config
file > defaults**; a value already in the environment is never overridden by the file.

```bash
# node list as the first argument:
QDB_REST_TOKEN=... ./qdb-primary-watchdog.sh 172.31.42.41:9003,172.31.41.35:9003,10.0.0.8:9003
# or entirely from the environment / a config file:
export QDB_REST_TOKEN=...  QDB_WD_SERVERS=...  QDB_WD_FAIL_THRESHOLD=2
./qdb-primary-watchdog.sh
```

The script auto-loads a config file if present (first of `/etc/qdb-primary-watchdog.env`,
`~/.config/qdb-primary-watchdog.env`, or one beside the script; or `$QDB_WD_CONFIG`).
[`qdb-primary-watchdog.env.example`](./qdb-primary-watchdog.env.example) is a copy-ready
template of the same settings.

**Required (no defaults):**

| Variable | Purpose |
| --- | --- |
| `QDB_REST_TOKEN` | Bearer token for the lifecycle REST API. |
| `QDB_WD_SERVERS` | Ordered `host:9003,...` node list. The first CLI argument overrides it. |

**Optional (defaults shown):**

| Variable | Default | Purpose |
| --- | --- | --- |
| `QDB_WD_CONFIG` | *(auto-discover)* | Explicit path to the config file to load, bypassing discovery. |
| `QDB_WD_TARGET_ROLE` | `primary` | Role a node is switched to on failover. |
| `QDB_WD_SWITCH_TIMEOUT_MS` | `5000` | `timeout_ms` in the switch payload, and the promote backoff base. |
| `QDB_WD_STEPDOWN_CONNECT_TIMEOUT` | `3` | Connect timeout (s) for the old-primary step-down; no overall timeout. |
| `QDB_WD_CHECK_CONNECT_TIMEOUT` | `2` | Connect timeout (s) for each role/health poll. |
| `QDB_WD_CHECK_MAX_TIME` | `3` | Overall max time (s) for each role/health poll. |
| `QDB_WD_FAIL_THRESHOLD` | `3` | Consecutive non-primary reads before failover. |
| `QDB_WD_CHECK_INTERVAL` | `1` | Seconds between role checks. |
| `QDB_WD_GRACE_PERIOD` | `5` | Seconds to wait after detecting loss before acting (`0` disables), so a manual operator switch can settle; re-checked after. |
| `QDB_WD_SWITCH_RETRIES` | `3` | Promote attempts, with exponential backoff between them. |
| `QDB_WD_SWITCH_POLL_INTERVAL` | `1` | Seconds between lifecycle polls after a switch. |
| `QDB_WD_SWITCH_POLL_MAX` | `30` | Max polls to wait for a switch to settle. |

### Run it as a service

The same script and config file work by hand or under systemd. Install with
[`qdb-primary-watchdog.service`](./qdb-primary-watchdog.service):

```bash
sudo cp qdb-primary-watchdog.sh  /usr/local/bin/qdb-primary-watchdog.sh
sudo chmod +x                    /usr/local/bin/qdb-primary-watchdog.sh
sudo cp qdb-primary-watchdog.env.example /etc/qdb-primary-watchdog.env
sudo chmod 600 /etc/qdb-primary-watchdog.env      # holds the token
sudoedit /etc/qdb-primary-watchdog.env            # set QDB_REST_TOKEN and QDB_WD_SERVERS
sudo cp qdb-primary-watchdog.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now qdb-primary-watchdog
journalctl -u qdb-primary-watchdog -f
```

`Restart=always` brings it back on crash (the watchdog no longer exits on its own, even when the
whole cluster is down; it keeps retrying).

**Run exactly one watchdog per cluster.** Two instances would try to promote different nodes and
fight (split-brain). Put it on a separate monitoring host or one designated node.

For a similar sender, but for telemetry data, see the instructions (and license info) at [the Telemetry Readme](./TelemetryReadme.md)
