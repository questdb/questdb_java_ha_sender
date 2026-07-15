# Python HA sender

A Python port of the Java/Rust `CsvParallelSender`, plus a pandas/polars dataframe demo.
It replays a CSV of trades in a loop across N worker threads over QuestDB's QWP
(WebSocket/UDP) and ILP/HTTP transports, with a probe that reads back the latest ingested
timestamp and the serving node's live role.

Built on the QuestDB Python client (`questdb.ingress`), which wraps the same C/Rust client
as the other ports.

## Which script to use

This port has grown a few scripts. Pick by what you need:

| Script | Use it for | Path / speed |
| --- | --- | --- |
| `csv_columnar_sender.py` | **High-throughput ingestion** - bulk CSV replay at volume | Columnar QWP (`Client.dataframe`, Arrow/polars). Fastest Python path; flat memory. |
| `csv_parallel_sender.py` | **HA / failover demos** - store-and-forward durability, the live probe, `qwpudp`/`ilp` comparison, and the per-event `row()` API | Row-by-row QWP/UDP/ILP. Correct but slow for volume - keep `--total-events` and the batch window small. |
| `read_bench.py` | Measuring read/scan throughput (rows/s) of a table | Streaming `iter_arrow()` egress. |
| `enrich_polars_demo.py` | Read a table as polars, enrich, write it back as polars | Streaming read + columnar write. |
| `dataframe_demo.py` | pandas/polars ingestion + egress round-trip showcase | `Sender.dataframe` / `Client.dataframe`. |

**Row-by-row vs columnar.** `sender.row()` in a Python loop is the *slowest* path: every cell
crosses the Python/Cython boundary under the GIL (the client's own perf notes call it "~16x
slower" than the columnar bulk path). `Client.dataframe` ships whole Arrow columns to the
native client - the only Python path that reaches Java/Rust-class throughput. So use
`csv_columnar_sender.py` for volume, and reach for `csv_parallel_sender.py` **only** when you
need store-and-forward failover, which the columnar path deliberately bypasses (dataframe
ingestion uses the direct column sender, not store-and-forward). Do not push high volume
(e.g. 100M rows) through the row-by-row sender - its per-row Python cost plus a large flush
window will pin the buffer in memory and can OOM the host.

```bash
# Fast columnar ingestion (recommended for volume):
python csv_columnar_sender.py \
  --addrs host:9000 --total-events 100000000 \
  --num-senders 2 --chunk-rows 100000 --csv ../trades.csv \
  --token "$QDB_TOKEN" --tls-verify unsafe_off
```

## Network tuning for throughput (important on real networks)

The Rust/C/Python QWP clients hardcode a 4 MiB socket buffer, which default Linux silently
clamps to ~416 KB and pins each connection at ~426 KB **per RTT** - a hard throughput cap on
a real network (e.g. ~210k rows/s at 20 ms RTT, while Java, which does not touch socket
buffers, does ~52 MB/s). Two levers:

- Run [`boost_tcp.sh`](../boost_tcp.sh) on **both** the sender and server hosts to raise
  `net.core.wmem_max`/`rmem_max` (and friends) so the client's 4 MiB buffer request actually
  goes through instead of being clamped. It is runtime-only (resets on reboot); persist via
  `/etc/sysctl.d/` if you want it permanent.
- Prefer **multiple senders** (`--num-senders`): the cap is per connection, so parallel
  connections multiply throughput (~11M rows/s with 2 connections same-zone EC2 in the
  client team's tests, vs ~4M with 1). The tuning affects egress too, so it also speeds up
  `read_bench.py` on a high-RTT link.

## Important: the client must be built from source

The QWP transports (`qwpws`/`qwpwss`/`qwpudp`) **and the query client / egress reader are not
in the PyPI release** (`questdb` 4.x on PyPI is ILP-only: `Protocol` has just
`Tcp/Tcps/Http/Https`, and there is no `Client`/`QueryResult`). They live on the
**`sm_qwp_dataframe_bench`** branch of
[`py-questdb-client`](https://github.com/questdb/py-questdb-client), which bumps the bundled
C client to the QWP + egress build. You must build that branch.

Requirements: **Python ≥ 3.10** (the branch is 3.10+; the PyPI-era 3.9 venv will not work),
a Rust toolchain (`cargo`), and a C compiler. Build steps (a git worktree keeps your
`main` checkout untouched):

```bash
cd ~/prj/python/py-questdb-client
git worktree add --detach /tmp/pyqdb_qwp origin/sm_qwp_dataframe_bench
cd /tmp/pyqdb_qwp
git submodule update --init          # bundled c-questdb-client (QWP + egress)

python3.12 -m venv /tmp/pyqdb_venv
/tmp/pyqdb_venv/bin/pip install -U pip "cython>=3.1.2" "setuptools>=80.9.0" numpy pandas polars pyarrow
/tmp/pyqdb_venv/bin/pip install -e .   # compiles Cython + the Rust FFI
```

Verify:

```python
from questdb.ingress import Protocol, Client   # Client only exists on the QWP build
print([p for p in dir(Protocol) if not p.startswith('_')])
# -> ['Http', 'Https', 'QwpUdp', 'QwpWs', 'QwpWss', 'Tcp', 'Tcps']
```

Then run the scripts with that interpreter (`/tmp/pyqdb_venv/bin/python`). `pandas`, `polars`,
and `pyarrow` are only needed for the dataframe paths.

## Transports (`--protocol`)

- `qwp` (default): QWP over WebSocket. Per-worker store-and-forward, transactional commit,
  multi-host failover, and the probe. Use the WebSocket/HTTP port (`:9000`).
- `qwpudp`: QWP over UDP, fire-and-forget datagrams to `:9007`. Ingest-only,
  unauthenticated, single-endpoint (no failover), best-effort. **Especially lossy in
  Python**: the Cython `row()` loop emits datagrams so fast that even a single worker can
  overrun the server's UDP receive buffer (a full run can vanish). Pace it with `--delay-ms`
  and keep `--batch-size` small, and treat results as best-effort.
- `ilp`: the legacy ILP/HTTP transport.

## Run

```bash
/tmp/pyqdb_venv/bin/python csv_parallel_sender.py \
  --protocol qwp \
  --addrs localhost:9000 \
  --total-events 100000 \
  --num-senders 4 \
  --delay-ms 0 \
  --batch-size 10000 \
  --batches-per-transaction 10 \
  --csv ../trades20250728.csv.gz
```

Flags mirror the Java/Rust ports: `--addrs`, `--token`/`--username`/`--password`,
`--total-events`, `--delay-ms`, `--rate`, `--num-senders`, `--retry-timeout`, `--csv`,
`--timestamp-from-file`, `--seconds-offset`, `--protocol`, `--sender-id`,
`--store-forward-dir`, `--batch-size`, `--batches-per-transaction`, `--probe-interval-ms`,
`--enterprise`, `--zone`. Confirm results server-side with `SELECT count() FROM trades`.

## Pacing: `--delay-ms` vs `--rate`

- `--delay-ms` (default `50`): a fixed sleep after **every row**, per worker.
- `--rate` (default `0` = off): a **target aggregate rate in rows/second across all workers**.
  Each worker paces to its share (`rate / num-senders`) against a deadline schedule, sending
  rows back-to-back and sleeping only when it runs *ahead*. When `> 0` it takes precedence over
  `--delay-ms` (a warning prints if both are set). Note Python's per-row throughput ceiling is
  lower than Java/Rust (the GIL / Python-level row building), so very high `--rate` targets may
  not be reached; the pacing simply never sleeps in that case.

## Timestamps

The generator sends timestamps at **nanosecond** resolution (`TimestampNanos`), and QuestDB
stores them at the **target column's** resolution: a `TIMESTAMP_NS` column keeps full nanos, a
micros `TIMESTAMP` column truncates the extra digits (silently, never an error). **If the table
does not exist, the server auto-creates it as `TIMESTAMP_NS`.** Pre-create `trades` with a
`timestamp` (micros) column if you want micros. The CSV timestamp is parsed to full nanoseconds
(the fractional-seconds string is read directly, since Python's `datetime` is microsecond-only
and would otherwise drop the last three digits of a `TIMESTAMP_NS` source).

## Probe (QWP/WebSocket only)

For `qwp`, a background thread uses the pooled query client (`Client`) to run
`select timestamp from trades limit -1` each interval and prints the latest ingested
timestamp, then `switch status` for the serving node's live role. `switch status` is
Enterprise-only (needs SYSTEM ADMIN); on OSS the probe prints
`(live 'switch status' unavailable, ...)`. The client uses `target=any` (replica-fallback
reads) and fails over across the `--addrs` hosts.

## Dataframe demo (pandas / polars ingestion + egress)

`dataframe_demo.py` shows the columnar paths the row sender does not:

```bash
/tmp/pyqdb_venv/bin/python dataframe_demo.py --addr localhost:9000 --csv ../trades20250728.csv.gz --rows 5000
```

- **pandas ingestion** via `Sender.dataframe(df, ...)` — the numpy-backed pandas planner.
- **polars ingestion** via `Client.dataframe(df, ...)` — the pooled QWP Arrow-columnar path,
  which takes polars / pyarrow / any Arrow C Stream source natively. (Numpy-backed pandas is
  **not** accepted by `Client.dataframe` — it raises `UnsupportedDataFrameShapeError` — so
  pandas goes through `Sender.dataframe`, or convert with `pyarrow.Table.from_pandas`.)
- **egress** via `Client.query(sql).to_pandas()` and `.to_polars()` (also `.to_arrow()` /
  `iter_pandas()`; the result exposes the Arrow PyCapsule interface for zero-copy consumers).

## Differences from the Java/Rust ports

- **The client is pre-release / build-from-source** (see above) — not `pip install questdb`.
- **Ingestion and querying are split across two classes**: `Sender` (ingest) and `Client`
  (pooled QWP: query + Arrow-columnar dataframe ingest). The probe uses `Client`.
- **Auto-flush exists** in the Python client (unlike the Rust client) and is verified to work
  over QWP (row-threshold flush fires mid-stream; see Validated). This port sets
  `auto_flush=off` and flushes at the same batch boundaries as the Java/Rust ports for
  identical cadence.
- **Config-string scheme is `qwpws`/`qwpwss` for both the sender and the query client**
  (the Rust reader used `ws`/`wss`; Python only accepts the `qwp*` schemes).
- **The probe has no handshake-role fallback** — because the Python binding does not expose it.
  Java/Rust fall back to the QWP handshake role when `switch status` is unavailable. That role
  exists in the underlying C client (`line_reader_server_info_role`/`_role_byte`) and the Rust
  reader (`server_info()`), but the Python `Client` on this branch does not wrap it, so this
  port prints "unavailable" on OSS. Likewise the query path's `on_failover_reset` callback is
  wired internally in Cython but not surfaced to Python, so there is no user-facing failover
  narration.
- Threads (like Java/Rust). Python's GIL is released during client I/O, but row-building is
  Python-level, so row-by-row throughput is well below Java/Rust — use the dataframe path
  for volume.

## Validated

Against a local QuestDB (OSS), all rows accounted for server-side:

| What | Result |
| --- | --- |
| `qwp`, 3000 rows, 1 worker | 3000 / 3000, distinct per-row timestamps; probe reported live timestamps |
| `qwp`, `--rate 5000`, nanos FX, `--timestamp-from-file`, 4 workers | 20000 / 20000, steady ~5000 rows/s; auto-created `TIMESTAMP_NS` table preserved `...192508297Z` |
| `ilp`, 2000 rows, 2 workers | 2000 / 2000 |
| `qwpudp` (paced) | rows land; best-effort, lossy under fast bursts |
| `dataframe_demo` | pandas + polars ingest (10000 rows), egress to pandas and polars |
| auto-flush | `auto_flush_rows=1000`, 2500 rows, no manual flush: 2000 landed mid-stream (2 auto-flushes), close drained to 2500 |
| QWP failover (primary crash) | see below |

### QWP failover (primary crash)

Two servers from the same build (primary `:9100`, fallback `:9000`), a single-worker `qwp`
run of 40000 rows with `--addrs :9100,:9000` and the probe active. The primary was
hard-killed mid-run:

- The sender **completed all 40000 rows with exit 0**, despite its primary crashing.
- The primary took `trade_id` seq 1–18500; the fallback took 18501–40000 (21500 rows, all
  distinct). Combined: **contiguous 1–40000, zero loss, zero duplication** — store-and-forward
  replayed the in-flight transaction to the new host, handing off exactly on a transaction
  boundary.
- The probe failed over from the primary to the fallback (visible as a second
  `connection lost -> restored` cycle once post-failover data committed on the new host).
