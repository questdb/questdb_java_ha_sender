# Python HA sender

A Python port of the Java/Rust `CsvParallelSender`, plus a pandas/polars dataframe demo.
It replays a CSV of trades in a loop across N worker threads over QuestDB's QWP
(WebSocket/UDP) and ILP/HTTP transports, with a probe that reads back the latest ingested
timestamp and the serving node's live role.

Built on the QuestDB Python client **5.0**, which wraps the same C/Rust client as the
other ports. The 5.0 shape: `questdb.connect()` returns one pooled `QuestDB` handle that
queries, bulk-loads DataFrames and lends row senders; `questdb.Sender` is the standalone
connection-level API for point-to-point ILP/HTTP, ILP/TCP and QWP/UDP. (The old
`from questdb.ingress import Client` split is gone; `questdb.ingress` still imports as a
deprecated 4.x shim.)

## Which script to use

This port has grown a few scripts. Pick by what you need:

| Script | Use it for | Path / speed |
| --- | --- | --- |
| `csv_columnar_sender.py` | **High-throughput ingestion** - bulk CSV replay at volume | Columnar QWP (`db.dataframe`, Arrow/polars). Fastest Python path; flat memory. |
| `csv_parallel_sender.py` | **HA / failover demos** - store-and-forward durability, the live probe, `qwpudp`/`ilp` comparison, and the per-event `row()` API | Row-by-row QWP/UDP/ILP. Correct but slow for volume - keep `--total-events` and the batch window small. |
| `read_bench.py` | Measuring read/scan throughput (rows/s) of a table | Streaming `iter_arrow()` egress. |
| `enrich_polars_demo.py` | Read a table as polars, enrich, write it back as polars | Streaming read + columnar write. |
| `dataframe_demo.py` | pandas/polars ingestion + egress round-trip showcase | `db.dataframe` (pandas + polars), `db.execute`, dual egress. |

**Row-by-row vs columnar.** `sender.row()` in a Python loop is the *slowest* path: every cell
crosses the Python/Cython boundary under the GIL (the client's own perf notes call it "~16x
slower" than the columnar bulk path). `db.dataframe` ships whole Arrow columns to the
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

Client **5.0.0** is unreleased, so it is not on PyPI yet (`questdb` 4.x on PyPI is ILP-only:
`Protocol` has just `Tcp/Tcps/Http/Https`, no `QuestDB`/`connect`/`QueryResult`). The QWP
transports (`ws`/`wss`/`udp`), the `questdb.connect()` query/ingest handle and the egress
reader live on the **`jh_experiment_new_ilp`** branch of
[`py-questdb-client`](https://github.com/questdb/py-questdb-client), which bumps the bundled
C client to the QWP + egress build. You must build that branch.

Requirements: **Python 3.12 or 3.13** (3.10+ works, but avoid CPython **3.14 + PyArrow 25.x**,
which segfaults on a threaded worker-first Arrow allocation - an upstream PyArrow/3.14 bug, not
the client; stay on a stable PyArrow 23.x/24.x), a Rust toolchain (`cargo`), and a C compiler.
Build steps (a git worktree keeps your `main` checkout untouched):

```bash
cd ~/prj/python/py-questdb-client
git worktree add --detach /tmp/pyqdb_v5 origin/jh_experiment_new_ilp
cd /tmp/pyqdb_v5
git submodule update --init --recursive   # bundled c-questdb-client (QWP + egress)

python3.12 -m venv /tmp/pyqdb_venv
/tmp/pyqdb_venv/bin/pip install -U pip "cython>=3.1.2" "setuptools>=80.9.0" numpy pandas polars pyarrow
/tmp/pyqdb_venv/bin/pip install -e .   # compiles Cython + the Rust FFI
```

Verify:

```python
import questdb
from questdb import Protocol, QuestDB          # top-level in 5.0 (was questdb.ingress.Client)
print(questdb.__version__, hasattr(questdb, 'connect'))
# -> 5.0.0 True
print([p for p in dir(Protocol) if not p.startswith('_')])
# -> ['Http', 'Https', 'Tcp', 'Tcps', 'Udp', 'Ws', 'Wss']
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

For `qwp`, a background thread uses a `questdb.connect()` query handle to run
`select timestamp from trades limit -1` each interval and prints the latest ingested
timestamp, then `switch status` for the serving node's live role. `switch status` is
Enterprise-only (needs SYSTEM ADMIN); on OSS the probe prints
`(live 'switch status' unavailable, ...)`. The handle uses `target=any` (replica-fallback
reads) and fails over across the `--addrs` hosts. (5.0 also exposes `db.server_info()` for the
handshake role/epoch/capabilities, should a probe want the role without `switch status`.)

## Dataframe demo (pandas / polars ingestion + egress)

`dataframe_demo.py` shows the columnar paths the row sender does not:

```bash
/tmp/pyqdb_venv/bin/python dataframe_demo.py --addr localhost:9000 --csv ../trades20250728.csv.gz --rows 5000
```

- **DDL** via `db.execute(sql)` — run-and-drain (drops the table), no HTTP `/exec` side channel.
- **pandas ingestion** via `db.dataframe(df, ...)` — numpy-backed pandas is accepted directly on
  the columnar handle (4.x's `Client.dataframe` rejected it). One caveat, exercised by the demo:
  columnar v1 wants SYMBOL columns as pandas `Categorical` (or `string[pyarrow]`); plain `object`
  strings land as VARCHAR, and forcing them to SYMBOL raises a now-per-column
  `UnsupportedDataFrameShapeError`. The demo casts `symbol`/`side` to `category`.
- **polars ingestion** via `db.dataframe(df, ...)` — the same call takes polars / pyarrow / any
  Arrow C Stream source natively.
- **egress** via `db.query(sql).to_pandas()` and `.to_polars()` (also `.to_arrow()` /
  `iter_pandas()`; the result exposes the Arrow PyCapsule interface for zero-copy consumers).

## Differences from the Java/Rust ports

- **The client is pre-release / build-from-source** (see above) — not `pip install questdb`.
- **One handle for QWP, plus a standalone `Sender`.** `questdb.connect()` returns a pooled
  `QuestDB` handle that queries (`db.query`), runs DDL (`db.execute`), bulk-loads DataFrames
  (`db.dataframe`) and lends row senders (`db.sender()`). The columnar scripts and the probe use
  that handle. The row-by-row HA sender keeps a standalone `questdb.Sender` **per worker** —
  each with its own `sender_id` and `sf_dir` for independent store-and-forward — which the shared
  pool would not give.
- **Auto-flush exists** on the standalone `Sender` (unlike the Rust client) and is verified over
  QWP (row-threshold flush fires mid-stream; see Validated). This port sets `auto_flush=off` and
  flushes at the same batch boundaries as the Java/Rust ports. (The **pooled** `db.sender()` has
  no auto-flush by default — flushing there is always explicit.)
- **Config-string scheme is `ws`/`wss` (and `udp`)**, matching the Rust/C ports — the 5.0 binding
  renamed the Python-only `qwpws`/`qwpwss`/`qwpudp` away, so a conf string now copies verbatim
  between clients. (`--protocol qwp`/`qwpudp`/`ilp` are just this port's transport selectors; they
  map to `ws`/`udp`/`http` on the wire.)
- **Naive `datetime` is UTC everywhere** in 5.0 (4.x read naive *scalars* in the machine's local
  zone). These scripts already pass tz-aware `datetime.now(timezone.utc)` / integer nanos, so the
  change is a no-op here — but a naive value now emits a one-per-process `UserWarning`.
- **The probe still has no handshake-role fallback** — it prints "unavailable" on OSS when
  `switch status` is missing. 5.0 *does* wrap the handshake role as `db.server_info().role`
  (`ServerInfo`: role/epoch/capabilities/cluster/node/zone), so a fallback is now possible; this
  port has not wired it in. Connection narration is also available now via the `connection_listener=`
  callback on `connect()`/`Sender` (`ConnectionEvent`s: connected/disconnected/reconnected/failed_over/...),
  which this port does not use.
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
