//! Parallel CSV replay sender for QuestDB, a Rust port of the Java `CsvParallelSender`.
//!
//! It replays a CSV of trades in a loop across N worker threads over one of three
//! transports, selected with `--protocol`:
//!
//! * `qwp` - QWP over WebSocket: store-and-forward (un-acked frames spill to disk and
//!   replay after an outage), transactional commit, multi-host failover. A background probe
//!   thread polls the latest ingested timestamp and the serving node's live role via the
//!   query client (the `Reader`).
//! * `qwpudp` - QWP over UDP: fire-and-forget datagrams to :9007. Ingest-only,
//!   unauthenticated, single-endpoint, best-effort (no ack, no failover).
//! * `ilp` - the legacy ILP/HTTP transport.
//!
//! Unlike the Java client, the Rust/C client has no auto-flush: batching is driven purely
//! by when we call `flush()`. We reproduce the Java cadence explicitly (see `run_worker`).

use std::io::Read;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use questdb::egress::column::ColumnView;
use questdb::egress::reader::{BatchView, Reader};
use questdb::egress::FailoverEvent;
use questdb::ingress::{Sender, TimestampNanos};

const PROBE_QUERY: &str = "select timestamp from trades limit -1";
// Enterprise lifecycle status of whichever node the query client is connected to. Returns
// the LIVE role (current_role / target_role), unlike the QWP handshake SERVER_INFO which is
// only refreshed on a reconnect and so goes stale after an in-place primary<->replica switch.
const STATUS_QUERY: &str = "switch status";

#[derive(Parser, Debug)]
#[command(
    name = "csv_parallel_sender",
    about = "Parallel CSV replay sender for QuestDB (QWP WebSocket/UDP, ILP/HTTP)"
)]
struct Args {
    /// Comma-separated host:port list. QWP/WebSocket + ILP use :9000; QWP/UDP uses :9007.
    #[arg(long, default_value = "localhost:9000")]
    addrs: String,

    /// Bearer token (QWP/WebSocket + ILP only; QWP/UDP is unauthenticated).
    #[arg(long)]
    token: Option<String>,

    /// Basic-auth username (with --password).
    #[arg(long)]
    username: Option<String>,

    /// Basic-auth password (with --username).
    #[arg(long)]
    password: Option<String>,

    /// Total rows to send across all workers.
    #[arg(long, default_value_t = 1_000_000)]
    total_events: u64,

    /// Per-row sleep in milliseconds (0 = as fast as possible). Ignored when --rate > 0.
    #[arg(long, default_value_t = 50)]
    delay_ms: u64,

    /// Target aggregate rows/second across ALL workers (0 = off, use --delay-ms). Takes
    /// precedence over --delay-ms: each worker paces to rate/num-senders against a deadline
    /// schedule, reaching high targets a per-row sleep cannot.
    #[arg(long, default_value_t = 0)]
    rate: u64,

    /// Number of worker threads (each is its own Sender).
    #[arg(long, default_value_t = 10)]
    num_senders: usize,

    /// Overall "keep retrying" budget in ms: retry_timeout (ILP) / reconnect_max_duration (QWP).
    #[arg(long, default_value_t = 360_000)]
    retry_timeout: u64,

    /// CSV path (.csv or .csv.gz) with symbol,side,price,amount[,timestamp] columns.
    #[arg(long, default_value = "./trades20250728.csv.gz")]
    csv: String,

    /// Use the CSV's own timestamp column instead of server/client "now".
    #[arg(long, default_value_t = false)]
    timestamp_from_file: bool,

    /// Shift each timestamp by this many seconds (works with or without --timestamp-from-file).
    #[arg(long, default_value_t = 0)]
    seconds_offset: i64,

    /// Transport: qwp | qwpudp | ilp.
    #[arg(long, default_value = "qwp")]
    protocol: String,

    /// Store-and-forward key base (QWP/WebSocket). Each worker gets <sender-id>-<worker>.
    #[arg(long, default_value = "ha_sender")]
    sender_id: String,

    /// Store-and-forward spill directory (QWP/WebSocket).
    #[arg(long, default_value = "/tmp/qdb-sf")]
    store_forward_dir: String,

    /// Rows per flush. QWP commits every batch-size*batches-per-transaction; UDP flushes
    /// every batch-size rows (keep small for UDP; large bursts overflow the receive buffer).
    #[arg(long, default_value_t = 10_000)]
    batch_size: u64,

    /// QWP/WebSocket transaction size, in batches.
    #[arg(long, default_value_t = 10)]
    batches_per_transaction: u64,

    /// Probe poll interval in ms (QWP/WebSocket only; 0 disables).
    #[arg(long, default_value_t = 1000)]
    probe_interval_ms: u64,

    /// Enterprise only: request durable acks (QWP/WebSocket).
    #[arg(long, default_value_t = false)]
    enterprise: bool,

    /// Preferred zone for the query client / probe (QWP/WebSocket).
    #[arg(long, default_value = "eu-west-1")]
    zone: String,
}

impl Args {
    fn has_token(&self) -> bool {
        self.token.as_deref().is_some_and(|t| !t.is_empty())
    }

    fn has_basic(&self) -> bool {
        self.username.as_deref().is_some_and(|u| !u.is_empty())
            && self.password.as_deref().is_some_and(|p| !p.is_empty())
    }

    fn tls(&self) -> bool {
        self.has_token() || self.has_basic()
    }

    fn addr_list(&self) -> Vec<String> {
        self.addrs
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }
}

struct TradeRow {
    symbol: String,
    side: String,
    price: f64,
    amount: f64,
    /// Nanoseconds since epoch, parsed at load time; only used with --timestamp-from-file.
    ts_nanos: i64,
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

fn main() {
    let args = Args::parse();

    if !matches!(args.protocol.as_str(), "qwp" | "qwpudp" | "ilp") {
        eprintln!("--protocol must be 'qwp', 'qwpudp', or 'ilp', got: {}", args.protocol);
        std::process::exit(2);
    }
    if args.num_senders == 0 {
        eprintln!("--num-senders must be > 0");
        std::process::exit(2);
    }
    if args.total_events == 0 {
        eprintln!("--total-events must be > 0");
        std::process::exit(2);
    }
    if args.batch_size == 0 {
        eprintln!("--batch-size must be > 0");
        std::process::exit(2);
    }
    if args.batches_per_transaction == 0 {
        eprintln!("--batches-per-transaction must be > 0");
        std::process::exit(2);
    }
    // --rate takes precedence: when set it drives pacing and --delay-ms is ignored.
    if args.rate > 0 && args.delay_ms > 0 {
        eprintln!(
            "[warn] --rate {} overrides --delay-ms {} (rate limiting drives pacing; the per-row \
             delay is ignored)",
            args.rate, args.delay_ms
        );
    }
    println!(
        "Pacing: {}",
        if args.rate > 0 {
            format!("rate={} rows/s (aggregate across {} workers)", args.rate, args.num_senders)
        } else {
            format!("delay-ms={}", args.delay_ms)
        }
    );

    let addrs = args.addr_list();
    if addrs.is_empty() {
        eprintln!("--addrs must contain at least one host:port");
        std::process::exit(2);
    }
    // QWP/UDP is single-endpoint (no failover) and unauthenticated. Enforce both upfront so
    // the failure is a clear message here, not a per-worker exception mid-run.
    if args.protocol == "qwpudp" {
        if addrs.len() > 1 {
            eprintln!(
                "--protocol qwpudp supports a single host:port only (UDP is connectionless, \
                 there is no failover); got {} addresses",
                addrs.len()
            );
            std::process::exit(2);
        }
        if args.has_token() || args.username.is_some() || args.password.is_some() {
            eprintln!(
                "[warn] --protocol qwpudp is unauthenticated (UDP accepts any connection); \
                 ignoring --token/--username/--password"
            );
        }
    }

    let rows = match load_csv(&args.csv, args.timestamp_from_file) {
        Ok(rows) if !rows.is_empty() => rows,
        Ok(_) => {
            eprintln!("CSV has no data rows: {}", args.csv);
            std::process::exit(2);
        }
        Err(e) => {
            eprintln!("Failed to read CSV {}: {e}", args.csv);
            std::process::exit(2);
        }
    };

    match args.protocol.as_str() {
        "qwp" => println!(
            "Ingestion started. Protocol: qwp (WebSocket, sender-id={}, store-and-forward={}, \
             batch-size={}, batches-per-transaction={}, retry-timeout-ms={}) | addrs: {}",
            args.sender_id, args.store_forward_dir, args.batch_size, args.batches_per_transaction,
            args.retry_timeout, addrs.join(",")
        ),
        "qwpudp" => println!(
            "Ingestion started. Protocol: qwpudp (QWP/UDP datagrams, ingest-only, unauthenticated, \
             no store-and-forward, no failover; query client disabled, batch-size={}) | addr: {}",
            args.batch_size, addrs[0]
        ),
        _ => println!(
            "Ingestion started. Protocol: ilp (HTTP, retry-timeout-ms={}) | addrs: {}",
            args.retry_timeout, addrs.join(",")
        ),
    }

    let total_sent = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let start = Instant::now();

    let base = args.total_events / args.num_senders as u64;
    let rem = args.total_events % args.num_senders as u64;
    let mut worker_errors: Vec<String> = Vec::new();

    thread::scope(|scope| {
        let args_ref = &args;
        let rows_ref = &rows;
        let sent_ref = &total_sent;
        let stop_ref = &stop;

        // Progress reporter: rows/s (client-side, sent), once per second.
        scope.spawn(move || {
            let mut last = 0u64;
            while !stop_ref.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(1000));
                let now = sent_ref.load(Ordering::Relaxed);
                println!("[progress] sent={now} rate={} rows/s", now - last);
                last = now;
            }
        });

        // Probe (QWP/WebSocket only): ingest-only transports (qwpudp, ilp) have no query path.
        if args_ref.protocol == "qwp" && args_ref.probe_interval_ms > 0 {
            scope.spawn(move || run_probe(args_ref, stop_ref));
        }

        let mut handles = Vec::with_capacity(args.num_senders);
        for id in 0..args.num_senders {
            let events = base + if (id as u64) < rem { 1 } else { 0 };
            handles.push(scope.spawn(move || run_worker(id, events, args_ref, rows_ref, sent_ref)));
        }

        for (id, h) in handles.into_iter().enumerate() {
            match h.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => worker_errors.push(format!("Sender {id}: {e}")),
                Err(_) => worker_errors.push(format!("Sender {id}: panicked")),
            }
        }
        stop_ref.store(true, Ordering::Relaxed);
    });

    if !worker_errors.is_empty() {
        for e in &worker_errors {
            eprintln!("Worker failed: {e}");
        }
        std::process::exit(1);
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 { args.total_events as f64 / elapsed } else { 0.0 };
    println!(
        "All workers completed. protocol={} events={} elapsed={:.3} s throughput={:.0} rows/s",
        args.protocol, args.total_events, elapsed, rate
    );
}

fn run_worker(
    worker_id: usize,
    total_events: u64,
    args: &Args,
    rows: &[TradeRow],
    total_sent: &AtomicU64,
) -> questdb::Result<()> {
    println!("Sender {worker_id} will send {total_events} events");

    let is_qwp = args.protocol == "qwp";
    let is_udp = args.protocol == "qwpudp";
    // Single worker on a QWP transport: stamp each row with the current time (nanos) client-side.
    // A single thread is monotonic (no O3) and this avoids QWP's per-batch atNow() stamping.
    // ILP, or more than one worker, use atNow() (server-side, O3-safe).
    let per_row_ts = (is_qwp || is_udp) && args.num_senders == 1;
    // Flush cadence: qwp commits every batch-size*batches-per-transaction rows; qwpudp flushes
    // every batch-size rows (bounds the datagram burst); ilp flushes every batch-size rows too
    // (the Rust HTTP client has no auto-flush, so the buffer must be drained explicitly).
    let commit_every: u64 = if is_qwp {
        args.batch_size * args.batches_per_transaction
    } else {
        args.batch_size
    };

    // Rate limiting (when --rate > 0): pace this worker to its share of the aggregate target,
    // rate / num-senders rows/second. interval_nanos is the ideal spacing between this worker's
    // rows; we track a deadline from loop entry and sleep only when running ahead, so rows go
    // out back-to-back until we get ahead. Reaches high rates a fixed per-row sleep cannot.
    let interval_nanos: f64 = if args.rate > 0 {
        1_000_000_000.0 * args.num_senders as f64 / args.rate as f64
    } else {
        0.0
    };
    let rate_limited = interval_nanos > 0.0;

    let mut sender = Sender::from_conf(build_ingest_conf(args, worker_id))?;
    let mut buffer = sender.new_buffer();
    let n = rows.len();
    let mut sent: u64 = 0;
    let pace_start = Instant::now();

    for i in 0..total_events {
        let r = &rows[(i as usize) % n];
        buffer
            .table("trades")?
            .symbol("symbol", r.symbol.as_str())?
            .symbol("side", r.side.as_str())?
            .column_f64("price", r.price)?
            .column_f64("amount", r.amount)?
            .column_str("trade_id", format!("{worker_id}-{}", i + 1))?;

        if args.timestamp_from_file {
            let ts = r.ts_nanos + args.seconds_offset * 1_000_000_000;
            buffer.at(TimestampNanos::new(ts))?;
        } else if args.seconds_offset != 0 {
            buffer.at(TimestampNanos::new(now_nanos() + args.seconds_offset * 1_000_000_000))?;
        } else if per_row_ts {
            buffer.at(TimestampNanos::new(now_nanos()))?;
        } else {
            buffer.at_now()?;
        }

        sent += 1;
        total_sent.fetch_add(1, Ordering::Relaxed);

        if sent.is_multiple_of(commit_every) {
            sender.flush(&mut buffer)?;
        }
        if rate_limited {
            // Deadline for the row just sent (sent is 1-based). Sleep only while ahead of
            // schedule; the 1ms floor coalesces many rows into one sleep at high rates.
            let target_nanos = (sent as f64 * interval_nanos) as u64;
            let elapsed_nanos = pace_start.elapsed().as_nanos() as u64;
            if target_nanos > elapsed_nanos {
                let sleep_nanos = target_nanos - elapsed_nanos;
                if sleep_nanos > 1_000_000 {
                    thread::sleep(Duration::from_nanos(sleep_nanos));
                }
            }
        } else if args.delay_ms > 0 {
            thread::sleep(Duration::from_millis(args.delay_ms));
        }
    }

    sender.flush(&mut buffer)?;
    // QWP/WebSocket: drain any un-acked store-and-forward frames before closing.
    if is_qwp {
        sender.close_drain()?;
    }
    println!("Sender {worker_id} finished sending {sent} events");
    Ok(())
}

/// Build the ingestion connect string for the configured transport.
fn build_ingest_conf(args: &Args, worker_id: usize) -> String {
    let addrs = args.addr_list();
    match args.protocol.as_str() {
        "qwpudp" => format!("qwpudp::addr={};", addrs[0]),
        "ilp" => {
            let scheme = if args.tls() { "https" } else { "http" };
            let mut c = format!("{scheme}::addr={};", addrs.join(","));
            append_auth(&mut c, args);
            if args.tls() {
                c.push_str("tls_verify=unsafe_off;");
            }
            c.push_str(&format!("retry_timeout={};", args.retry_timeout));
            c
        }
        _ => {
            // qwp (WebSocket). Use the spec aliases ws/wss (qwpws/qwpwss are deprecated); the
            // client maps ws -> QwpWs and wss -> QwpWss.
            let scheme = if args.tls() { "wss" } else { "ws" };
            let who = format!("{}-{worker_id}", args.sender_id);
            let sf = format!("{}/{who}", args.store_forward_dir);
            let _ = std::fs::create_dir_all(&sf);
            let mut c = format!("{scheme}::addr={};", addrs.join(","));
            append_auth(&mut c, args);
            if args.tls() {
                c.push_str("tls_verify=unsafe_off;");
            }
            c.push_str(&format!("sender_id={who};"));
            c.push_str(&format!("sf_dir={sf};"));
            c.push_str(&format!("reconnect_max_duration_millis={};", args.retry_timeout));
            c.push_str("reconnect_initial_backoff_millis=100;");
            c.push_str("reconnect_max_backoff_millis=5000;");
            if args.enterprise {
                c.push_str("request_durable_ack=true;");
            }
            c
        }
    }
}

fn append_auth(c: &mut String, args: &Args) {
    if args.has_token() {
        c.push_str(&format!("token={};", args.token.as_ref().unwrap()));
    } else if args.has_basic() {
        c.push_str(&format!(
            "username={};password={};",
            args.username.as_ref().unwrap(),
            args.password.as_ref().unwrap()
        ));
    }
}

/// Connect string for the QWP query client (probe): same hosts/auth as the senders,
/// target=any (replica-fallback reads), failover on, zone bias.
fn build_reader_conf(args: &Args) -> String {
    let addrs = args.addr_list();
    let scheme = if args.tls() { "wss" } else { "ws" };
    let mut c = format!("{scheme}::addr={};target=any;failover=true;", addrs.join(","));
    append_auth(&mut c, args);
    if args.tls() {
        c.push_str("tls_verify=unsafe_off;");
    }
    if !args.zone.is_empty() {
        c.push_str(&format!("zone={};", args.zone));
    }
    c.push_str("compression=raw;");
    c
}

/// Probe thread: every interval, poll the latest ingested timestamp and the serving node's
/// live role over a QWP query client. Independent of the senders; fails over automatically.
fn run_probe(args: &Args, stop: &AtomicBool) {
    let mut reader = match Reader::from_conf(build_reader_conf(args)) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[query client] connect failed: {e}");
            return;
        }
    };
    if let Some(si) = reader.server_info() {
        println!(
            "[query client] connected, serving node={} role={} zone={} cluster={}",
            or_none(&si.node_id),
            si.role.as_str(),
            or_none(si.zone_id.as_deref().unwrap_or("")),
            or_none(&si.cluster_id)
        );
    } else {
        println!("[query client] connected");
    }

    let mut was_down = false;
    while !stop.load(Ordering::Relaxed) {
        match read_latest_ts(&mut reader) {
            Ok(latest) => {
                if was_down {
                    println!("[query client] connection restored");
                    was_down = false;
                }
                if let Some(micros) = latest {
                    // trades designated timestamp is microseconds by default.
                    let ts = chrono::DateTime::from_timestamp_micros(micros)
                        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
                        .unwrap_or_else(|| micros.to_string());

                    // Ask the serving node for its live role. A status-query failure must not
                    // look like a connection loss, so it is swallowed (current stays None).
                    let (current, target) = read_switch_status(&mut reader).unwrap_or((None, None));

                    let (node, zone) = match reader.server_info() {
                        Some(si) => (
                            or_none(&si.node_id),
                            or_none(si.zone_id.as_deref().unwrap_or("")),
                        ),
                        None => ("(none)".to_string(), "(none)".to_string()),
                    };

                    let served = match current {
                        Some(role) => {
                            let switching = target
                                .as_deref()
                                .is_some_and(|t| !t.eq_ignore_ascii_case(&role));
                            let sw = match (&target, switching) {
                                (Some(t), true) => format!(" (switching -> {t})"),
                                _ => String::new(),
                            };
                            format!(" served by role={role}{sw} node={node} zone={zone}")
                        }
                        None => {
                            let handshake = reader
                                .server_info()
                                .map(|si| si.role.as_str())
                                .unwrap_or_else(|| "unknown".to_string());
                            format!(
                                " served by role={handshake} node={node} zone={zone} \
                                 (handshake role; live 'switch status' unavailable, may be stale)"
                            )
                        }
                    };
                    println!("[probe] latest trades timestamp = {ts} (raw={micros}){served}");
                }
            }
            Err(e) => {
                if !was_down {
                    println!("[query client] connection lost ({e}), will retry");
                    was_down = true;
                }
            }
        }
        thread::sleep(Duration::from_millis(args.probe_interval_ms));
    }
}

/// Run PROBE_QUERY and return the last non-null timestamp (micros), if any.
fn read_latest_ts(reader: &mut Reader) -> Result<Option<i64>, questdb::egress::Error> {
    let mut latest: Option<i64> = None;
    let mut cursor = reader
        .prepare(PROBE_QUERY)
        .on_failover_reset(|ev: &FailoverEvent| {
            println!(
                "[query client] failed over {} -> {} (attempt {})",
                ev.failed_addr, ev.new_addr, ev.attempts
            );
        })
        .execute()?;
    while let Some(view) = cursor.next_batch()? {
        if let Ok(ColumnView::Timestamp(col)) = view.column(0) {
            for r in 0..view.row_count() {
                if !col.is_null(r) {
                    latest = Some(col.value(r));
                }
            }
        }
    }
    Ok(latest)
}

/// Run STATUS_QUERY and pull current_role / target_role out of the result by column name.
fn read_switch_status(
    reader: &mut Reader,
) -> Result<(Option<String>, Option<String>), questdb::egress::Error> {
    let mut current: Option<String> = None;
    let mut target: Option<String> = None;
    let mut cursor = reader.prepare(STATUS_QUERY).execute()?;
    while let Some(view) = cursor.next_batch()? {
        let names: Vec<String> = view
            .schema()
            .columns()
            .iter()
            .map(|c| c.name.to_lowercase())
            .collect();
        for r in 0..view.row_count() {
            for (i, name) in names.iter().enumerate() {
                if !name.contains("role") {
                    continue;
                }
                let value = col_string(&view, i, r);
                if name.contains("current") {
                    current = value;
                } else if name.contains("target") {
                    target = value;
                } else if current.is_none() {
                    // A single unqualified "role" column (older servers) is the current one.
                    current = value;
                }
            }
        }
    }
    Ok((current, target))
}

/// Read a string value from a Symbol or Varchar column; None for null or non-string types.
fn col_string(view: &BatchView, col: usize, row: usize) -> Option<String> {
    match view.column(col) {
        Ok(ColumnView::Symbol(c)) => c.resolve(row).map(|s| s.to_string()),
        Ok(ColumnView::Varchar(c)) => c.value(row).map(|s| s.to_string()),
        _ => None,
    }
}

fn or_none(s: &str) -> String {
    if s.is_empty() {
        "(none)".to_string()
    } else {
        s.to_string()
    }
}

/// Load the CSV (optionally gzipped) into memory. Requires symbol,side,price,amount and,
/// when `need_timestamp`, a timestamp column parsed to microseconds since epoch.
fn load_csv(path: &str, need_timestamp: bool) -> Result<Vec<TradeRow>, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let reader: Box<dyn Read> = if path.ends_with(".gz") {
        Box::new(flate2::read::GzDecoder::new(file))
    } else {
        Box::new(file)
    };
    let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(reader);

    let headers = rdr.headers()?.clone();
    let idx = |name: &str| -> Result<usize, Box<dyn std::error::Error>> {
        headers
            .iter()
            .position(|h| h.trim() == name)
            .ok_or_else(|| format!("CSV missing required column: {name}").into())
    };
    let i_symbol = idx("symbol")?;
    let i_side = idx("side")?;
    let i_price = idx("price")?;
    let i_amount = idx("amount")?;
    let i_ts = if need_timestamp { Some(idx("timestamp")?) } else { None };

    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        if rec.is_empty() {
            continue;
        }
        let ts_nanos = match i_ts {
            Some(i) => {
                let raw = rec.get(i).unwrap_or("").trim();
                // Parse to nanoseconds so a TIMESTAMP_NS source keeps full precision; a micros
                // target column truncates server-side. timestamp_nanos_opt is None only for
                // dates outside ~1677-2262, which trade data never is.
                chrono::DateTime::parse_from_rfc3339(raw)
                    .map_err(|e| format!("bad timestamp {raw:?}: {e}"))?
                    .timestamp_nanos_opt()
                    .ok_or_else(|| format!("timestamp out of nanosecond range: {raw:?}"))?
            }
            None => 0,
        };
        out.push(TradeRow {
            symbol: rec.get(i_symbol).unwrap_or("").trim().to_string(),
            side: rec.get(i_side).unwrap_or("").trim().to_string(),
            price: rec.get(i_price).unwrap_or("").trim().parse()?,
            amount: rec.get(i_amount).unwrap_or("").trim().parse()?,
            ts_nanos,
        });
    }
    Ok(out)
}
