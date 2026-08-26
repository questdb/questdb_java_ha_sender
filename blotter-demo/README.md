# One-command live blotter demo

A self-contained Docker demo: QuestDB with **LIVE VIEW**, the **questdb 5.0 Python
client**, a synthetic price feed, and the terminal blotter rendering on top of a
live view. Your colleague needs **nothing but Docker** — no Python, no client
install.

Nothing is built from source anymore: QuestDB comes from the official `nightly`
image (the first images to ship `LIVE VIEW`), and the client installs from PyPI
(`questdb==5.0.0`). The only image `build` does is layer the feed/blotter scripts
onto `python:3.12-slim` — a few seconds.

## Run it

```bash
cd blotter-demo
docker compose build            # one-time, fast: pip installs the client + copies scripts
docker compose run --rm demo    # QuestDB starts, then the blotter draws in your terminal
```

`run` (not `up`) is deliberate — it gives the blotter a real TTY for the in-place
redraw. **Ctrl+C** quits the blotter; the feed stops with it. Afterwards:

```bash
docker compose down             # stop QuestDB
```

## Running it again

Once the `demo` image exists and the `nightly` image is pulled, every
`docker compose run --rm demo` just starts containers, so repeat runs are fast —
the only cost is a few seconds of JVM startup as QuestDB cold-starts (the
`--rm`/`down` flow disposes the container each time, so the table, live view, and
history start fresh on every run — intended for a demo). A rebuild only happens if
you edit `feed.py`, `blotter.py`, or `entrypoint.sh` — and even then only the cheap
COPY layers re-run.

To get a newer QuestDB, re-pull the moving tag: `docker compose pull questdb`.

If a `docker compose down` reports "network still in use", a run container lingered
from a Ctrl+C; clear it with `docker compose down --remove-orphans`.

## Removing everything at the end

This demo creates: one built image (`blotter-demo-demo`), a network and containers,
a little build cache (the pip layer), and two base images it pulls
(`python:3.12-slim`, `questdb/questdb:nightly`).

**Removing an image is not destructive the way deleting a volume is** — any image
can be re-pulled on demand, so the worst case of over-removing is a re-download
later. No data is lost. The demo defines no named volumes.

### Step 1 — containers, network, and the built image (scoped, safe)

```bash
docker compose down --rmi local --remove-orphans
```

Touches only this project. `--rmi local` deletes the `blotter-demo-demo` image the
demo built; `--remove-orphans` clears any run container left by a Ctrl+C.

### Step 2 — the base images this demo pulled

Docker does not record when an image was pulled, so "was it already there before?"
can only be answered by snapshotting the image list **before** the first build.

**Before your first `docker compose build`/`pull`**, record what already exists:

```bash
docker image ls --format '{{.Repository}}:{{.Tag}}' | sort > ~/blotter-demo-images-before.txt
```

**At cleanup**, remove each base image only if it was NOT in that snapshot (i.e.
only if this demo pulled it):

```bash
for img in python:3.12-slim questdb/questdb:nightly; do
  grep -qxF "$img" ~/blotter-demo-images-before.txt || docker rmi "$img"
done
```

If you did **not** take the snapshot, you cannot tell which were pre-existing.
Either leave them, or just remove both — re-pulling is harmless (no data loss),
it only costs a download if another project needs them later:

```bash
docker rmi python:3.12-slim questdb/questdb:nightly
```

### Step 3 — reclaim the build cache

```bash
docker builder prune -f
```

`docker builder prune` removes only BuildKit's cached build layers. It **never
deletes images, containers, or volumes** — those are safe. But it is **system-wide,
not scoped to this demo**: it clears the build cache of every project, so other
projects' next builds recompute their layers (slower, but nothing is lost — cache
only). There is no per-project build-cache prune. This demo's own cache is now tiny
(just the pip layer), so this step mostly matters if other projects share the daemon.

### Not recommended

`docker system prune -af` reclaims everything Docker is not currently using **across
all projects**, not just this demo — only run it if you genuinely mean machine-wide.

## What it does

1. **`questdb`** — the official `questdb/questdb:nightly` image, the first to ship
   `LIVE VIEW`. Reachable from the host at **http://localhost:19000** (console +
   `/exec`); this host port is deliberately 19000, not 9000, so it never collides
   with a local QuestDB. The blotter reaches the server internally over the compose
   network at `questdb:9000`.
2. **`demo`** — `python:3.12-slim` + `pip install questdb==5.0.0 polars pyarrow`,
   then:
   - waits for QuestDB,
   - `curl` creates `core_price_demo` (base table) and `core_price_lv` (live view),
   - runs `feed.py` in the background (synthetic crypto/FX bids; ~2000 rows/s by
     default, tunable to 100K+/s via `FEED_TARGET_RPS` — see Knobs),
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

- **Feed rate.** `FEED_TARGET_RPS` (default 2000) on the `demo` service sets the
  target rows/second. The feed builds one vectorized Polars DataFrame per flush
  and ships it with a single columnar `Sender.dataframe(...)` call, so a single
  process sustains ~1M rows/s — set `FEED_TARGET_RPS=100000` (or higher) to stress
  ingestion:
  ```bash
  docker compose run --rm -e FEED_TARGET_RPS=100000 demo
  ```
  Prices are illustrative 2026 levels, not live market data.
- **Flush cadence / workers.** `FEED_FLUSH_HZ` (default 20) is flushes/second per
  worker; batch size is `FEED_TARGET_RPS / FEED_WORKERS / FEED_FLUSH_HZ`.
  `FEED_WORKERS` (default 1) forks that many parallel sender processes, each
  targeting an equal share — only needed to push past what one process can
  generate. The feed prints its measured `~N rows/s` every 5 s; confirm the true
  server-side rate with
  `select count()/10.0 from core_price_demo where timestamp > dateadd('s', -10, now())`.
- **Blotter refresh.** `--rate` Hz in `entrypoint.sh` (default 10) sets how often
  the terminal redraws.
- **What the blotter shows.** Defaults to the last 20 rows of the live view
  (`--table core_price_lv --limit -20`). Adjust the count with `--limit`, or pass
  `--query "<sql>"` in `entrypoint.sh` to run any SQL against the view verbatim.

## Versions

| Component | Source | Version |
| --- | --- | --- |
| QuestDB server | `questdb/questdb:nightly` | first images with `LIVE VIEW` (merged 2026-08, commit `73685fa`) |
| Python client | PyPI `questdb` | `5.0.0` |

`:nightly` is a **moving tag** — every pull may bring a newer build, so
reproducibility is weaker than a fixed version. For a "colleague runs it once" demo
that is fine. To freeze it, pin the `image:` in `docker-compose.yml` by digest
(`questdb/questdb@sha256:…`).

## When QuestDB 10.0 ships

`LIVE VIEW` is expected in the **10.0** stable release. Once it lands, swap the
moving nightly tag for the pinned release in `docker-compose.yml`:

```yaml
    image: questdb/questdb:10.0
```

That is the only change — everything else already uses released components.

## Sharing without a rebuild

To hand a colleague an image that needs no build at all, either push to a registry
(`docker compose build && docker compose push`, after adding `image:` names) or
`docker save`/`docker load` a tarball. Both are architecture-specific — an amd64
image runs under emulation on Apple Silicon. Letting them `docker compose build`
locally (a few seconds now) avoids that by building for their arch.
