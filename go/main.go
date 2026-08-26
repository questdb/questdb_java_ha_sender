// Parallel CSV replay sender for QuestDB - a Go port of the Java/Rust/C++
// CsvParallelSender, on the go-questdb-client v4 QWP (WebSocket) transport.
//
// It replays a CSV of trades in a loop across N worker goroutines and, at the
// same time, runs a background probe that queries the latest ingested timestamp
// and the serving node's role - ingest and query concurrently, with HA:
//
//   - QWP over WebSocket (--protocol qwp, default): each worker owns its own
//     store-and-forward sender (un-acked frames spill to disk under sf_dir and
//     replay after an outage), with multi-host failover across --addrs. A probe
//     goroutine uses a QwpQueryClient to poll `select ... from trades limit -1`
//     and the handshake role (QwpServerInfo).
//   - ILP over HTTP (--protocol ilp): the legacy line protocol, ingest only
//     (QuestDB has no query client for ILP - the probe is QWP-only).
//
// Unlike ILP, a QWP Flush() never waits for the server ACK - it publishes into
// the sender's cursor engine and an I/O goroutine delivers/replays in the
// background. We report client-side "submitted" live and confirm server-side
// "acknowledged" at the end via FlushAndGetSequence + AwaitAckedFsn. See
// QWP_vs_ILP_in_Go.md for the full contrast.
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	qdb "github.com/questdb/go-questdb-client/v4"
)

const (
	probeQuery = "select cast(timestamp as varchar) ts from trades limit -1"
	destTable  = "trades"
)

type config struct {
	addrs        string
	token        string
	username     string
	password     string
	totalEvents  int64
	numSenders   int
	delayMs      int64
	csvPath      string
	tsFromFile   bool
	protocol     string // qwp | ilp
	senderID     string
	sfDir        string
	batchSize    int64
	batchesPerTx int64
	retryTimeout int64
	probeMs      int64
	enterprise   bool
	zone         string
	tlsVerify    string // on | unsafe_off
}

type trade struct {
	symbol, side string
	price, amount float64
	tsNanos       int64 // used only with --timestamp-from-file
}

func parseFlags() *config {
	c := &config{}
	flag.StringVar(&c.addrs, "addrs", "localhost:9000", "comma-separated host:port; QWP/ILP use :9000")
	flag.StringVar(&c.token, "token", "", "bearer token (turns on TLS)")
	flag.StringVar(&c.username, "username", "", "basic-auth username (turns on TLS)")
	flag.StringVar(&c.password, "password", "", "basic-auth password")
	flag.Int64Var(&c.totalEvents, "total-events", 1_000_000, "total rows across all workers")
	flag.IntVar(&c.numSenders, "num-senders", 4, "worker goroutines (each its own sender)")
	flag.Int64Var(&c.delayMs, "delay-ms", 0, "per-row sleep in ms (0 = flat out)")
	flag.StringVar(&c.csvPath, "csv", "../trades20250728.csv.gz", "CSV(.gz) with symbol,side,price,amount[,timestamp]")
	flag.BoolVar(&c.tsFromFile, "timestamp-from-file", false, "use the CSV timestamp column instead of server 'now'")
	flag.StringVar(&c.protocol, "protocol", "qwp", "transport: qwp | ilp")
	flag.StringVar(&c.senderID, "sender-id", "ha_sender", "store-and-forward sender id base (per worker: <id>-<n>)")
	flag.StringVar(&c.sfDir, "store-forward-dir", "/tmp/qdb-sf-go", "store-and-forward spill dir (QWP); empty \"\" = memory mode (no disk durability, raw happy-path ingest)")
	flag.Int64Var(&c.batchSize, "batch-size", 10_000, "rows per flush unit")
	flag.Int64Var(&c.batchesPerTx, "batches-per-transaction", 10, "flush every batch-size*this rows (QWP)")
	flag.Int64Var(&c.retryTimeout, "retry-timeout", 360_000, "reconnect_max_duration_millis (QWP) / retry budget")
	flag.Int64Var(&c.probeMs, "probe-interval-ms", 1000, "probe poll interval in ms (QWP; 0 disables)")
	flag.BoolVar(&c.enterprise, "enterprise", false, "request durable server ACKs (sf_durability=disk)")
	flag.StringVar(&c.zone, "zone", "", "query-side zone locality hint (QWP)")
	flag.StringVar(&c.tlsVerify, "tls-verify", "on", "on | unsafe_off (for self-signed certs)")
	flag.Parse()
	return c
}

func (c *config) tls() bool { return c.token != "" || (c.username != "" && c.password != "") }

func (c *config) addrList() []string {
	var out []string
	for _, a := range strings.Split(c.addrs, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

func (c *config) appendAuth(sb *strings.Builder) {
	if c.token != "" {
		fmt.Fprintf(sb, "token=%s;", c.token)
	} else if c.username != "" && c.password != "" {
		fmt.Fprintf(sb, "username=%s;password=%s;", c.username, c.password)
	}
	if c.tls() && c.tlsVerify == "unsafe_off" {
		sb.WriteString("tls_verify=unsafe_off;")
	}
}

// ingestConf builds the per-worker connect string for the configured transport.
func (c *config) ingestConf(worker int) string {
	addrs := strings.Join(c.addrList(), ",")
	var sb strings.Builder
	if c.protocol == "ilp" {
		scheme := "http"
		if c.tls() {
			scheme = "https"
		}
		fmt.Fprintf(&sb, "%s::addr=%s;auto_flush=off;", scheme, addrs)
		c.appendAuth(&sb)
		fmt.Fprintf(&sb, "retry_timeout=%d;", c.retryTimeout)
		return sb.String()
	}
	// qwp (WebSocket): failover + reconnect budget, with OPTIONAL store-and-forward.
	scheme := "ws"
	if c.tls() {
		scheme = "wss"
	}
	fmt.Fprintf(&sb, "%s::addr=%s;auto_flush=off;", scheme, addrs)
	c.appendAuth(&sb)
	if c.sfDir != "" {
		// Durable store-and-forward: each worker spills un-acked frames to its
		// own on-disk slot so they replay after an outage/restart.
		who := fmt.Sprintf("%s-%d", c.senderID, worker)
		fmt.Fprintf(&sb, "sf_dir=%s;sender_id=%s;", filepath.Join(c.sfDir, who), who)
		if c.enterprise {
			sb.WriteString("sf_durability=disk;") // fsync frames to disk before ACK
		}
	}
	// else: memory mode - RAM ring, no disk persistence. Rows still land and are
	// ACKed (server WAL-apply), but a process exit before the background send
	// drains can lose unacked rows. This is the raw happy-path ingest.
	fmt.Fprintf(&sb, "reconnect_max_duration_millis=%d;", c.retryTimeout)
	sb.WriteString("reconnect_initial_backoff_millis=100;reconnect_max_backoff_millis=5000;")
	return sb.String()
}

// queryConf builds the QWP query-client connect string for the probe.
func (c *config) queryConf() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "ws%s::addr=%s;target=any;", tlsSuffix(c.tls()), strings.Join(c.addrList(), ","))
	c.appendAuth(&sb)
	if c.zone != "" {
		fmt.Fprintf(&sb, "zone=%s;", c.zone)
	}
	return sb.String()
}

func tlsSuffix(tls bool) string {
	if tls {
		return "s"
	}
	return ""
}

func loadCSV(path string, needTS bool) ([]trade, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var r io.Reader = bufio.NewReader(f)
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		r = gz
	}
	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	header, err := cr.Read()
	if err != nil {
		return nil, err
	}
	idx := map[string]int{}
	for i, name := range header {
		idx[strings.TrimSpace(name)] = i
	}
	for _, col := range []string{"symbol", "side", "price", "amount"} {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("CSV missing column %q", col)
		}
	}
	if needTS {
		if _, ok := idx["timestamp"]; !ok {
			return nil, fmt.Errorf("CSV missing column %q", "timestamp")
		}
	}
	var rows []trade
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		price, _ := strconv.ParseFloat(strings.TrimSpace(rec[idx["price"]]), 64)
		amount, _ := strconv.ParseFloat(strings.TrimSpace(rec[idx["amount"]]), 64)
		t := trade{
			symbol: strings.TrimSpace(rec[idx["symbol"]]),
			side:   strings.TrimSpace(rec[idx["side"]]),
			price:  price,
			amount: amount,
		}
		if needTS {
			ts, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(rec[idx["timestamp"]]))
			if err != nil {
				return nil, fmt.Errorf("bad timestamp %q: %w", rec[idx["timestamp"]], err)
			}
			t.tsNanos = ts.UnixNano()
		}
		rows = append(rows, t)
	}
	return rows, nil
}

// runWorker replays `events` rows over its own sender, then confirms the
// server ACK'd them (QWP) before returning.
func runWorker(ctx context.Context, c *config, worker int, events int64, rows []trade,
	sent *int64, ackedRows *int64) error {

	sender, err := qdb.LineSenderFromConf(ctx, c.ingestConf(worker))
	if err != nil {
		return fmt.Errorf("worker %d connect: %w", worker, err)
	}
	defer sender.Close(ctx)

	qwp, isQwp := sender.(qdb.QwpSender)
	commitEvery := c.batchSize
	if c.protocol == "qwp" {
		commitEvery = c.batchSize * c.batchesPerTx
	}
	n := int64(len(rows))
	var i int64
	for i = 0; i < events; i++ {
		t := rows[i%n]
		b := sender.
			Table(destTable).
			Symbol("symbol", t.symbol).
			Symbol("side", t.side).
			Float64Column("price", t.price).
			Float64Column("amount", t.amount).
			StringColumn("trade_id", fmt.Sprintf("%d-%d", worker, i+1))
		if c.tsFromFile {
			err = b.At(ctx, time.Unix(0, t.tsNanos))
		} else {
			// Server-assigned timestamp: no client clock skew, no O3 across workers.
			err = b.AtNow(ctx)
		}
		if err != nil {
			return fmt.Errorf("worker %d row: %w", worker, err)
		}
		if s := atomic.AddInt64(sent, 1); s%commitEvery == 0 {
			if err := sender.Flush(ctx); err != nil {
				return fmt.Errorf("worker %d flush: %w", worker, err)
			}
		}
		if c.delayMs > 0 {
			time.Sleep(time.Duration(c.delayMs) * time.Millisecond)
		}
		if ctx.Err() != nil {
			break
		}
	}

	// Publish the tail. On QWP, capture the published FSN and wait for the
	// server to ACK it - this is the ingest->durable boundary that ILP's
	// synchronous flush gives you for free but QWP's async flush does not.
	if isQwp {
		fsn, err := qwp.FlushAndGetSequence(ctx)
		if err != nil {
			return fmt.Errorf("worker %d final flush: %w", worker, err)
		}
		if err := qwp.AwaitAckedFsn(ctx, fsn); err != nil {
			return fmt.Errorf("worker %d await ack: %w", worker, err)
		}
		atomic.AddInt64(ackedRows, i)
	} else {
		if err := sender.Flush(ctx); err != nil {
			return fmt.Errorf("worker %d final flush: %w", worker, err)
		}
		atomic.AddInt64(ackedRows, i)
	}
	return nil
}

// runProbe polls the latest ingested timestamp and the serving node's role,
// concurrently with ingestion, over a QWP query client (with failover).
func runProbe(ctx context.Context, c *config) {
	client, err := qdb.QwpQueryClientFromConf(ctx, c.queryConf())
	if err != nil {
		fmt.Printf("[probe] connect failed: %v\n", err)
		return
	}
	defer client.Close(ctx)
	fmt.Printf("[probe] connected to %s\n", client.CurrentEndpoint())

	ticker := time.NewTicker(time.Duration(c.probeMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			latest := queryLatest(ctx, client)
			role, endpoint := "unknown", client.CurrentEndpoint()
			if si := client.ServerInfo(); si != nil {
				role = si.RoleName()
			}
			if latest == "" {
				fmt.Printf("[probe] no rows yet | node=%s role=%s\n", endpoint, role)
			} else {
				fmt.Printf("[probe] latest %s ts=%s | node=%s role=%s\n", destTable, latest, endpoint, role)
			}
		}
	}
}

func queryLatest(ctx context.Context, client *qdb.QwpQueryClient) string {
	q := client.Query(ctx, probeQuery)
	defer q.Close()
	var latest string
	for batch, err := range q.Batches() {
		if err != nil {
			return "" // transient (failover in progress etc.); next tick retries
		}
		if batch.RowCount() > 0 {
			col := batch.Column(0)
			if !col.IsNull(col.RowCount() - 1) {
				latest = col.String(col.RowCount() - 1)
			}
		}
	}
	return latest
}

func main() {
	c := parseFlags()
	if c.protocol != "qwp" && c.protocol != "ilp" {
		fmt.Fprintf(os.Stderr, "--protocol must be qwp or ilp, got %q\n", c.protocol)
		os.Exit(2)
	}
	if c.numSenders < 1 || c.totalEvents < 1 {
		fmt.Fprintln(os.Stderr, "--num-senders and --total-events must be >= 1")
		os.Exit(2)
	}

	rows, err := loadCSV(c.csvPath, c.tsFromFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load CSV: %v\n", err)
		os.Exit(1)
	}
	if len(rows) == 0 {
		fmt.Fprintln(os.Stderr, "CSV has no rows")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	scheme := "ws"
	if c.tls() {
		scheme = "wss"
	}
	if c.protocol == "ilp" {
		scheme = "http"
		if c.tls() {
			scheme = "https"
		}
	}
	fmt.Printf("[conf] protocol=%s scheme=%s tls=%v auth=%s | addrs=%s | table=%s rows=%d workers=%d\n",
		c.protocol, scheme, c.tls(), authKind(c), c.addrs, destTable, len(rows), c.numSenders)

	var sent, ackedRows int64
	start := time.Now()

	// Progress reporter: submitted (client-side) vs acknowledged (server-durable).
	var wgReport sync.WaitGroup
	wgReport.Add(1)
	go func() {
		defer wgReport.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s := atomic.LoadInt64(&sent)
				fmt.Printf("[progress] submitted=%d (+%d/s) acknowledged=%d\n",
					s, s-last, atomic.LoadInt64(&ackedRows))
				last = s
			}
		}
	}()

	// Probe (QWP only): query concurrently with ingest.
	probeCtx, probeStop := context.WithCancel(ctx)
	if c.protocol == "qwp" && c.probeMs > 0 {
		go runProbe(probeCtx, c)
	}

	// Workers: each its own sender, replaying its share of the CSV.
	base := c.totalEvents / int64(c.numSenders)
	rem := c.totalEvents % int64(c.numSenders)
	var wg sync.WaitGroup
	errs := make([]error, c.numSenders)
	for w := 0; w < c.numSenders; w++ {
		events := base
		if int64(w) < rem {
			events++
		}
		wg.Add(1)
		go func(worker int, events int64) {
			defer wg.Done()
			errs[worker] = runWorker(ctx, c, worker, events, rows, &sent, &ackedRows)
		}(w, events)
	}
	wg.Wait()

	probeStop()   // stop the probe once ingestion is done
	stop()        // release the signal context so the reporter exits
	wgReport.Wait()

	failed := false
	for w, e := range errs {
		if e != nil {
			fmt.Fprintf(os.Stderr, "worker %d failed: %v\n", w, e)
			failed = true
		}
	}

	elapsed := time.Since(start)
	total := atomic.LoadInt64(&sent)
	acked := atomic.LoadInt64(&ackedRows)
	fmt.Printf("[done] submitted=%d acknowledged=%d in %.3fs (%.0f rows/s submit)\n",
		total, acked, elapsed.Seconds(), float64(total)/elapsed.Seconds())
	if c.protocol == "qwp" {
		fmt.Printf("[done] 'acknowledged' = rows the server durably confirmed (FlushAndGetSequence + AwaitAckedFsn).\n")
	}
	if failed {
		os.Exit(1)
	}
}

func authKind(c *config) string {
	switch {
	case c.token != "":
		return "token"
	case c.username != "":
		return "basic"
	default:
		return "none"
	}
}
