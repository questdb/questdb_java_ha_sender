# C++ HA sender

A C++ port of the Java/Rust/Python `CsvParallelSender`. It replays a CSV of trades in a loop
across N worker threads over QuestDB's QWP (WebSocket/UDP) and ILP/HTTP transports, with a
probe that reads back the latest ingested timestamp and the serving node's live role.

Built on the C/C++ client headers from
[`c-questdb-client`](https://github.com/questdb/c-questdb-client) (`line_sender.hpp` for
ingestion, `line_reader.hpp` for the egress query client) — the same C/Rust core the Rust and
Python ports use. C++17.

## Build

Requires a `c-questdb-client` checkout (v7.0.0+ / `main`, which has QWP + the egress reader), a
Rust toolchain (`cargo`, pulled in via the bundled Corrosion), a C++17 compiler, and zlib. The
project builds the client as a CMake subdirectory (Corrosion compiles the Rust FFI), so point it
at your checkout:

```bash
cmake -B build -S . -DQUESTDB_CLIENT_DIR=/path/to/c-questdb-client
cmake --build build -j
```

`QUESTDB_CLIENT_DIR` defaults to `~/prj/questdb/c-questdb-client`. The CMake forces
`QUESTDB_ENABLE_READER=ON` (the query client / probe) and `QUESTDB_ENABLE_INSECURE_SKIP_VERIFY=ON`
(`tls_verify=unsafe_off`). The binary is `build/csv_parallel_sender`. The QWP wire protocol must
match the target server build.

## Transports (`--protocol`)

- `qwp` (default): QWP over WebSocket. Per-worker store-and-forward, transactional commit,
  multi-host failover, and the probe. Use the WebSocket/HTTP port (`:9000`).
- `qwpudp`: QWP over UDP, fire-and-forget datagrams to `:9007`. Ingest-only, unauthenticated,
  single-endpoint (no failover), best-effort.
- `ilp`: the legacy ILP/HTTP transport.

## Run

```bash
./build/csv_parallel_sender \
  --protocol qwp \
  --addrs localhost:9000 \
  --total-events 100000 \
  --num-senders 4 \
  --delay-ms 0 \
  --batch-size 10000 \
  --batches-per-transaction 10 \
  --csv ../trades20250728.csv.gz
```

Flags mirror the other ports: `--addrs`, `--token`/`--username`/`--password`, `--total-events`,
`--delay-ms`, `--rate`, `--num-senders`, `--retry-timeout`, `--csv`, `--timestamp-from-file`,
`--seconds-offset`, `--protocol`, `--sender-id`, `--store-forward-dir`, `--batch-size`,
`--batches-per-transaction`, `--probe-interval-ms`, `--enterprise`, `--zone`.
(`--timestamp-from-file` and `--enterprise` are presence flags.) Confirm results server-side with
`SELECT count() FROM trades`.

## Pacing: `--delay-ms` vs `--rate`

- `--delay-ms` (default `50`): a fixed sleep after **every row**, per worker.
- `--rate` (default `0` = off): a **target aggregate rate in rows/second across all workers**.
  Each worker paces to its share (`rate / num-senders`) against a deadline schedule, sending
  rows back-to-back and sleeping only when it runs *ahead* — reaching high targets a per-row
  sleep never could. When `> 0` it takes precedence over `--delay-ms` (a warning prints if both
  are set). Measured: `--rate 5000`, 20k rows, 4 workers → ~5.0 s, steady ~5000 rows/s.

## Timestamps

The generator sends timestamps at **nanosecond** resolution (`timestamp_nanos`), and QuestDB
stores them at the **target column's** resolution: a `TIMESTAMP_NS` column keeps full nanos, a
micros `TIMESTAMP` column truncates the extra digits (silently, never an error). **If the table
does not exist, the server auto-creates it as `TIMESTAMP_NS`.** Pre-create `trades` with a
`timestamp` (micros) column if you want micros. The CSV parser already reads fractional seconds
to full nanosecond precision. Verified: replaying the nanosecond FX chunk with
`--timestamp-from-file` preserved `...192508297Z` in an auto-created `TIMESTAMP_NS` table.

## Probe (QWP/WebSocket only)

For `qwp`, a background thread uses the egress `reader` to run `select timestamp from trades
limit -1` each interval and prints the latest ingested timestamp, then `switch status` for the
serving node's live role. Unlike the Python port, the C++ `reader` exposes `server_info()`
(role/node/cluster), so the probe **does** fall back to the QWP handshake role when
`switch status` is unavailable (OSS / missing SYSTEM ADMIN), labelled `(handshake role; ...)` —
matching Java/Rust. The reader uses `target=any` (replica-fallback reads) and fails over across
the `--addrs` hosts.

## Differences from the other ports

- **No auto-flush** (like Rust; unlike Java/Python) — the C/C++ client has none, so batching is
  driven purely by explicit `flush()` at the same boundaries as the other ports.
- **Both the sender and reader use the `ws`/`wss` scheme** (like Rust; `qwpws`/`qwpwss` are the
  deprecated aliases — the client maps `ws`->QwpWs, `wss`->QwpWss). Python still requires `qwpws`
  because its binding does not yet accept the `ws`/`wss` aliases.
- Row values from the CSV are validated UTF-8 via `utf8_view` at build time (throws on invalid
  input); fixed column names use the `_cn` / `_tn` literals.
- Threads via `std::thread`, one `line_sender` per worker (unique `sender_id` + spill dir).

## Validated

Against a local QuestDB (OSS), all rows accounted for server-side:

| What | Result |
| --- | --- |
| `qwp`, 4000 rows, 1 worker | 4000 / 4000, distinct per-row timestamps; probe reported live timestamps + handshake role fallback |
| `ilp`, 2000 rows, 2 workers | 2000 / 2000 |
| `qwpudp` (paced) | rows land; best-effort, lossy under fast bursts |
| `qwp`, `--rate 5000`, nanos FX, `--timestamp-from-file`, 4 workers | steady ~5000 rows/s; auto-created `TIMESTAMP_NS` table preserved `...192508297Z` |
| QWP failover (primary crash) | see below |

### QWP failover (primary crash)

Two servers from the same build (primary `:9100`, fallback `:9000`), a single-worker `qwp`
run of 40000 rows with `--addrs :9100,:9000` and the probe active. The primary was
hard-killed mid-run:

- The sender **completed all 40000 rows with exit 0**, despite its primary crashing.
- The primary took `trade_id` seq 1–7500; the fallback took 7501–40000 (32500 rows, all
  distinct). Combined: **contiguous 1–40000, zero loss, zero duplication** — store-and-forward
  replayed the in-flight transaction to the new host, handing off exactly on a transaction
  boundary.
- The probe failed over from the primary to the fallback.
