# Go HA sender (QWP)

A Go port of the Java/Rust/C++ `CsvParallelSender`, on the
[`go-questdb-client`](https://github.com/questdb/go-questdb-client) **v4 QWP
(WebSocket)** transport. It replays a CSV of trades in a loop across N worker
goroutines **and** runs a concurrent probe that queries the latest ingested
timestamp and the serving node's role — ingest and query at the same time, with
HA (per-worker store-and-forward + multi-host failover).

## Build note (unreleased QWP)

The full QWP transport (store-and-forward, typed errors, failover, query client)
is on the client's `main` but **not in a published tag yet** (it landed after
`v4.2.0`). `go.mod` therefore has a `replace` pointing at a local checkout:

```
replace github.com/questdb/go-questdb-client/v4 => /Users/j/prj/go/go-questdb-client
```

Adjust that path for your machine (or drop it once a v4.x with QWP ships). Needs
Go 1.23+.

## Run

```bash
# QWP (default): 2 HA senders + live probe, server-assigned timestamps
go run . -addrs localhost:9000 -total-events 100000 -num-senders 2 \
    -csv ../trades20250728.csv.gz

# Enterprise TLS + auth, multi-host failover, durable ACKs, self-signed cert
go run . -protocol qwp \
    -addrs node-a:9000,node-b:9000 \
    -token "$QDB_TOKEN" -tls-verify unsafe_off \
    -enterprise -total-events 1000000 -num-senders 4

# ILP over HTTP (ingest only, no probe) for comparison
go run . -protocol ilp -addrs localhost:9000 -total-events 100000 -num-senders 2
```

Key flags mirror the sibling ports: `-addrs`, `-token`/`-username`/`-password`,
`-total-events`, `-num-senders`, `-csv`, `-timestamp-from-file`, `-batch-size`,
`-batches-per-transaction`, `-sender-id`, `-store-forward-dir`, `-retry-timeout`,
`-probe-interval-ms`, `-enterprise`, `-zone`, `-tls-verify`, `-delay-ms`.

The run prints **submitted** (client-side, published to the cursor engine) vs.
**acknowledged** (server-durable, confirmed via `FlushAndGetSequence` +
`AwaitAckedFsn`). That distinction is QWP-specific — see
`QWP_vs_ILP_in_Go.md` (kept at the TSBS repo root) for the full contrast, which
is written to hand to an agent adapting TSBS ingestion to QWP.
