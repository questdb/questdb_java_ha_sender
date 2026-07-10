# Rust HA sender

A Rust port of the Java `CsvParallelSender`. It replays a CSV of trades in a loop across
N worker threads over one of three QuestDB transports, and (on QWP/WebSocket) runs a probe
that reports the latest ingested timestamp and the serving node's live role.

It is built on the Rust/C client (`questdb-rs`) from
[`c-questdb-client`](https://github.com/questdb/c-questdb-client), which powers the C, C++,
and Python clients too.

## Transports (`--protocol`)

- `qwp` (default): **QWP over WebSocket**. Store-and-forward (un-acked frames spill to disk
  and replay after an outage), transactional commit, and multi-host failover. A background
  probe polls the latest ingested timestamp and the serving node's live role over a QWP
  query client (the `Reader`). Use the server's WebSocket/HTTP port (`:9000`).
- `qwpudp`: **QWP over UDP**, fire-and-forget datagrams to `:9007`. Ingest-only,
  unauthenticated, single-endpoint (no failover), best-effort (no ack, no store-and-forward).
  Keep `--batch-size` small: UDP has no backpressure, so a large flush burst overflows the
  server's receive buffer and drops datagrams wholesale.
- `ilp`: the legacy **ILP over HTTP** transport.

## Build

The client is a **path dependency** on a local `c-questdb-client` checkout, expected at
`../../../questdb/c-questdb-client/questdb-rs` (i.e. `~/prj/questdb/c-questdb-client`
alongside `~/prj/java/questdb_java_ha_sender`). Adjust the path in `Cargo.toml` if your
checkout is elsewhere. The QWP wire protocol must match the target server build.

```
cargo build --release
```

Requires a Rust toolchain new enough for the client's 2024 edition (1.85+; tested on 1.93).

## Run

```
./target/release/csv_parallel_sender \
  --protocol qwp \
  --addrs localhost:9000 \
  --total-events 5000000 \
  --num-senders 4 \
  --delay-ms 0 \
  --batch-size 10000 \
  --batches-per-transaction 10 \
  --csv ../trades20250728.csv.gz
```

UDP (single host, small batch, no auth, no probe):

```
./target/release/csv_parallel_sender \
  --protocol qwpudp --addrs localhost:9007 \
  --total-events 100000 --num-senders 1 --batch-size 500 \
  --csv ../trades20250728.csv.gz
```

QWP batch errors and UDP loss are asynchronous or silent, so always confirm the result with
a server-side `SELECT count() FROM trades` after a run.

## Flags

| Flag | Default | Notes |
| --- | --- | --- |
| `--addrs` | `localhost:9000` | Comma-separated `host:port`. QWP/WebSocket + ILP use `:9000`; UDP uses `:9007` and accepts one host only. |
| `--token` | | Bearer token (QWP/WebSocket + ILP). UDP rejects auth. |
| `--username` / `--password` | | Basic auth (QWP/WebSocket + ILP). |
| `--total-events` | `1000000` | Rows across all workers. |
| `--delay-ms` | `50` | Per-row sleep. |
| `--num-senders` | `10` | Worker threads (one `Sender` each). |
| `--retry-timeout` | `360000` | `retry_timeout` (ILP) / `reconnect_max_duration_millis` (QWP). |
| `--csv` | `./trades20250728.csv.gz` | `.csv` or `.csv.gz`; needs `symbol,side,price,amount[,timestamp]`. |
| `--timestamp-from-file` | `false` | Use the CSV timestamp column instead of "now". |
| `--seconds-offset` | `0` | Shift each timestamp by N seconds. |
| `--protocol` | `qwp` | `qwp` \| `qwpudp` \| `ilp`. |
| `--sender-id` | `ha_sender` | Store-and-forward key base; each worker gets `<sender-id>-<worker>`. |
| `--store-forward-dir` | `/tmp/qdb-sf` | Spill directory (QWP/WebSocket). |
| `--batch-size` | `10000` | Rows per flush. QWP commits every `batch-size × batches-per-transaction`; UDP/ILP flush every `batch-size`. |
| `--batches-per-transaction` | `10` | QWP transaction size, in batches. |
| `--probe-interval-ms` | `1000` | Probe poll interval (QWP/WebSocket only; `0` disables). |
| `--enterprise` | `false` | Request durable acks (Enterprise only). |
| `--zone` | `eu-west-1` | Preferred zone for the query client / probe. |

## Timestamps

- **Single worker on a QWP transport** (`qwp` or `qwpudp`, `--num-senders 1`): each row is
  stamped with the current microsecond client-side (monotonic, so no out-of-order), giving
  distinct per-row timestamps.
- **ILP, or more than one worker**: rows use server-side `at_now()` (O3-safe across workers;
  a whole batch shares one timestamp).
- `--timestamp-from-file` / `--seconds-offset` stamp each row explicitly from the CSV.

## Probe (QWP/WebSocket only)

A background thread runs `select timestamp from trades limit -1` each interval and prints
the latest ingested timestamp, then asks the serving node for its live role with
`switch status`:

```
[query client] connected, serving node=n1 role=PRIMARY zone=eu-west-1 cluster=...
[probe] latest trades timestamp = 2026-07-01T14:31:40.591545Z (raw=1782916300591545) served by role=PRIMARY node=n1 zone=eu-west-1
```

`switch status` gives the authoritative live role (`current_role`, plus `target_role` while
a switch is in flight, shown as `(switching -> ROLE)`). It is Enterprise-only and needs
SYSTEM ADMIN; on OSS the probe falls back to the QWP handshake role, labelled
`(handshake role; live 'switch status' unavailable, may be stale)`. The query client uses
`target=any` (so reads fall back to a replica when no primary is available) and fails over
across the `--addrs` hosts automatically.

## Differences from the Java client

- **No auto-flush.** The Rust/C client deliberately has no auto-flush (it rejects every
  `auto_flush*` key except `off`); batching is driven purely by explicit `flush()` calls.
  This port reproduces the Java cadence by flushing at the same row boundaries, so behavior
  matches; there is simply no background flush timer or byte cap.
- **No `--connect-timeout-ms`.** The Rust QWP transport exposes no single-connect timeout
  knob (only the overall `reconnect_max_duration`), so that flag is omitted here.
- **Single bearer token only.** QWP (WebSocket/HTTP) auth is a single `token` (or
  username/password). The split `x`/`y` key components only exist for legacy TCP-ILP ECDSA
  auth, which this tool does not use.

## Validated

Smoke-tested against a local QuestDB (OSS) with all rows accounted for server-side:

| Transport | Run | Result |
| --- | --- | --- |
| `ilp` | 2000 rows, 1 worker | 2000 / 2000 |
| `qwpudp` | 2000 rows, 1 worker | 2000 / 2000 |
| `qwp` | 5000 rows, 1 worker | 5000 / 5000, distinct per-row timestamps; probe reported live timestamps + role |
| `qwp` | 20000 rows, 4 workers | 20000 / 20000, one store-and-forward dir per worker |

### QWP failover (primary crash)

Two servers from the same build (primary on `:9100`, fallback on `:9000`), a single-worker
`qwp` run of 60000 rows with `--addrs :9100,:9000` and the probe active. The primary was
hard-killed mid-run:

- The sender **completed all 60000 rows with exit 0**, despite its primary crashing.
- The primary took `trade_id` `0-1`…`0-20000`; the fallback took `0-20001`…`0-60000`
  (40000 rows, all distinct). Combined: a **contiguous 1–60000 with zero loss and zero
  duplication** — store-and-forward replayed the in-flight transaction to the new host and
  the handoff landed exactly on a transaction boundary.
- The probe kept reporting across the crash and reconnected to the fallback transparently.

Note: the probe's `on_failover_reset` callback fires only on *mid-query* failover; its
`limit -1` polls return in microseconds, so a between-poll reconnect is silent (no
`failed over` line). Comparing `reader.current_addr()` across polls would surface it.
