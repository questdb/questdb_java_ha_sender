module qwphasender

go 1.23

require github.com/questdb/go-questdb-client/v4 v4.2.0

require (
	github.com/coder/websocket v1.8.14 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	golang.org/x/sys v0.16.0 // indirect
)

// The full QWP WebSocket transport (store-and-forward ingest, typed errors,
// failover, query client) landed on the client's `main` AFTER the v4.2.0 tag
// (commit ffec03a, PR #62), so it is not in a published release yet. Point at
// the local checkout until a v4.x with QWP ships; then drop this replace and
// bump the require above.
replace github.com/questdb/go-questdb-client/v4 => /Users/j/prj/go/go-questdb-client
