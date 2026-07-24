# One-command live blotter demo

A self-contained Docker demo: QuestDB with **LIVE VIEW**, the unreleased
**questdb 5.0 Python client**, a synthetic price feed, and the terminal blotter
rendering on top of a live view. Your colleague needs **nothing but Docker** — no
Python, no Rust, no client build.

## Run it

```bash
cd blotter-demo
docker compose build            # one-time, slow: builds QuestDB + the client from source
docker compose run --rm demo    # QuestDB starts, then the blotter draws in your terminal
```

`run` (not `up`) is deliberate — it gives the blotter a real TTY for the in-place
redraw. **Ctrl+C** quits the blotter; the feed stops with it. Afterwards:

```bash
docker compose down             # stop QuestDB
```

## Running it again

The slow part is the one-time `docker compose build`. Once the images exist, every
`docker compose run --rm demo` skips the build entirely and just starts containers,
so repeat runs are fast — the only cost is a few seconds of JVM startup as QuestDB
cold-starts (the `--rm`/`down` flow disposes the container each time, so the table,
live view, and history start fresh on every run — intended for a demo). A rebuild
only happens if you edit a file a Docker layer depends on, and even then only the
cheap final layers re-run — never the Rust/Maven builds — unless you change the
pinned client/server commits.

If a `docker compose down` reports "network still in use", a run container lingered
from a Ctrl+C; clear it with `docker compose down --remove-orphans`.

## Removing everything at the end

This demo creates four kinds of Docker artifact: two built images
(`blotter-demo-questdb`, `blotter-demo-demo`), a network and containers, build
cache (the Rust + Maven layers — the bulk of the disk), and base images it pulls
(`python:3.12-slim`, `eclipse-temurin:25-jdk`, `eclipse-temurin:25-jre`).

**Removing an image is not destructive the way deleting a volume is** — any image
can be re-pulled from Docker Hub on demand, so the worst case of over-removing is a
re-download later. No data is lost. The demo defines no named volumes.

### Step 1 — containers, network, and the two built images (scoped, safe)

```bash
docker compose down --rmi local --remove-orphans
```

Touches only this project. `--rmi local` deletes the two images the demo built;
`--remove-orphans` clears any run container left by a Ctrl+C.

### Step 2 — the base images this demo pulled

Docker does not record when an image was pulled, so "was it already there before?"
can only be answered by snapshotting the image list **before** the first build.

**Before your first `docker compose build`**, record what already exists:

```bash
docker image ls --format '{{.Repository}}:{{.Tag}}' | sort > ~/blotter-demo-images-before.txt
```

**At cleanup**, remove each base image only if it was NOT in that snapshot (i.e.
only if this demo pulled it):

```bash
for img in python:3.12-slim eclipse-temurin:25-jdk eclipse-temurin:25-jre; do
  grep -qxF "$img" ~/blotter-demo-images-before.txt || docker rmi "$img"
done
```

If you did **not** take the snapshot, you cannot tell which were pre-existing.
Either leave them, or just remove all three — re-pulling is harmless (no data loss),
it only costs a download if another project needs them later:

```bash
docker rmi python:3.12-slim eclipse-temurin:25-jdk eclipse-temurin:25-jre
```

### Step 3 — reclaim the build cache (the big disk win)

```bash
docker builder prune -f
```

`docker builder prune` removes only BuildKit's cached build layers. It **never
deletes images, containers, or volumes** — those are safe. But it is **system-wide,
not scoped to this demo**: it clears the build cache of every project, so other
projects' next builds recompute their layers (slower, but nothing is lost — cache
only). There is no per-project build-cache prune. If this demo is your only heavy
Docker build, it is effectively scoped anyway.

### Not recommended

`docker system prune -af` reclaims everything Docker is not currently using **across
all projects**, not just this demo — only run it if you genuinely mean machine-wide.

## What it does

1. **`questdb`** — built from source at the commit that first ships `LIVE VIEW`
   (`90a1b54c…`) with the web console (`-P build-web-console`). The server is
   reachable from the host at **http://localhost:19000** (console + `/exec`); this
   host port is deliberately 19000, not 9000, so it never collides with a local
   QuestDB. The blotter reaches the server internally over the compose network.
2. **`demo`** — builds the 5.0 client (`ea54b6f…`, Rust 1.91.1 + Cython), then:
   - waits for QuestDB,
   - `curl` creates `core_price_demo` (base table) and `core_price_lv` (live view),
   - runs `feed.py` in the background (~2000 rows/s of synthetic crypto/FX bids),
   - runs `blotter.py` in the foreground, polling the live view at 10 Hz.

The live view:

```sql
CREATE LIVE VIEW IF NOT EXISTS core_price_lv
FLUSH EVERY 5s IN MEMORY 5s START FROM NOW AS
SELECT timestamp, symbol, bid_price, avg(bid_price) OVER w AS moving_avg
FROM core_price_demo
WINDOW w AS (PARTITION BY symbol ORDER BY timestamp ANCHOR DAILY '00:00');
```

`START FROM NOW` is required by this build (the view tracks data from creation
time onward). A live view serves results from memory, so ingested rows appear
immediately; `FLUSH EVERY` is only the disk-commit cadence and does not gate what
the blotter sees. Visible motion comes from the **feed** flushing to the base
table often.

The blotter queries the live view directly — `select * from core_price_lv limit -20`
— so it streams the newest 20 rows (timestamp, symbol, bid, moving average),
refreshed at `--rate` Hz.

## Knobs

- **Feed rate / flush frequency.** `FEED_FLUSH_HZ` (default 20, i.e. a flush every
  50 ms) and `FEED_ROWS_PER_FLUSH` (default 100) on the `demo` service set how
  fast rows land and how often they are flushed. 20 flushes/s keeps the blotter
  visibly moving. Prices are illustrative 2026 levels, not live market data.
- **Blotter refresh.** `--rate` Hz in `entrypoint.sh` (default 10) sets how often
  the terminal redraws.
- **What the blotter shows.** Defaults to the last 20 rows of the live view
  (`--table core_price_lv --limit -20`). Adjust the count with `--limit`, or pass
  `--query "<sql>"` in `entrypoint.sh` to run any SQL against the view verbatim.

## Pins and durability

Both sources are pinned by commit SHA for reproducibility:

| Component | Repo | Commit |
| --- | --- | --- |
| QuestDB server | `questdb/questdb` | `90a1b54c98b10fad5304b1ad817a69cda25e52ad` |
| Python client | `questdb/py-questdb-client` | `ea54b6f474062c144aa6395facad42e77c99e6f6` |

A pinned SHA is fetchable from GitHub only while it stays reachable from a ref.
The questdb commit is expected to merge to `main`, so it survives. The **client
commit lives on `jh_experiment_new_ilp`, which may be deleted before it merges** —
if that happens the build's `git checkout` will fail. Run `./vendor.sh` to
snapshot the client source into `blotter-demo/vendor/` and follow its printed instructions
to switch the Dockerfile to the local tarball.

## When the client ships / questdb merges

- Once `LIVE VIEW` lands in a nightly, delete `questdb.Dockerfile` and set
  `image: questdb/questdb:nightly` on the `questdb` service.
- Once the client is on PyPI, replace the whole `client-build` stage with
  `pip install questdb==<version>`.

## Sharing without a rebuild

To hand a colleague an image that needs no build at all, either push to a registry
(`docker compose build && docker compose push`, after adding `image:` names) or
`docker save`/`docker load` a tarball. Both are architecture-specific — an amd64
image runs under emulation on Apple Silicon. Letting them `docker compose build`
locally avoids that by building for their arch.
