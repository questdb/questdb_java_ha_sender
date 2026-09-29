package com.example.sender;

import io.questdb.client.Sender;
import io.questdb.client.Sender.LineSenderBuilder;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.opencsv.CSVReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class CsvParallelSender {

    // Defaults mirror your Python script
    private static final String DEFAULT_ADDRS = "questdb:9000";
    private static final long DEFAULT_TOTAL_EVENTS = 1_000_000L;
    private static final int DEFAULT_DELAY_MS = 50;
    // Target aggregate generation rate in rows/second across ALL workers. 0 disables rate
    // limiting and falls back to --delay-ms. When > 0 it takes precedence over --delay-ms:
    // each worker paces itself to its share (rate / num-senders) against a deadline schedule,
    // so the process approximates the target regardless of worker count. Unlike a fixed
    // per-row sleep, it sends rows back-to-back and only sleeps when ahead of schedule, so it
    // can sustain high rates (e.g. 300000) that a --delay-ms of >=1 could never reach.
    private static final long DEFAULT_RATE = 0L;
    private static final int DEFAULT_NUM_SENDERS = 10;
    private static final int DEFAULT_RETRY_TIMEOUT = 360000;
    private static final String DEFAULT_CSV = "./trades20250728.csv.gz";
    private static final boolean DEFAULT_TIMESTAMP_FROM_FILE = false;
    private static final long DEFAULT_SECONDS_OFFSET = 0L;

    // QWP (WebSocket) transport
    private static final String DEFAULT_PROTOCOL = "qwp";
    private static final String DEFAULT_SENDER_ID = "ha_sender";
    private static final String DEFAULT_STORE_FORWARD_DIR = "/tmp/qdb-sf";
    // Upper bound (ms) on a single TCP connect attempt, so a black-holed host fails over fast
    // instead of riding the OS connect timeout. 0 disables (falls back to the OS default).
    // QWP + probe only; the ILP path is left untouched so --protocol ilp stays identical.
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 3000;
    // Batch = one auto-flush append (deferred, no commit under transactional mode).
    // Transaction = batches-per-transaction batches, committed atomically per table
    // by an explicit flush(). See buildSender()/runWorker().
    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final int DEFAULT_BATCHES_PER_TRANSACTION = 10;
    // Probe (QWP only): poll the latest ingested timestamp on an interval, 0 disables.
    private static final long DEFAULT_PROBE_INTERVAL_MS = 1000L;
    // Slice length for the polled final drain. Short enough that the once-per-second reporter
    // and probe keep ticking through it, long enough not to spin.
    private static final long DRAIN_SLICE_MS = 1000L;
    // close()'s own implicit drain budget. Deliberately LOW, not aligned to --retry-timeout:
    // the polled drain above has already waited, and anything still unacked is recovered by the
    // next sender in store-and-forward mode, so blocking again in close() buys no durability
    // and only delays shutdown. Override with --close-flush-timeout-ms.
    private static final long DEFAULT_CLOSE_FLUSH_TIMEOUT_MS = 5_000L;
    private static final String PROBE_QUERY = "select timestamp from trades limit -1";
    // Enterprise lifecycle status of whichever node the query client is currently connected to.
    // Returns the LIVE role (columns like current_role / target_role), unlike the QWP handshake
    // SERVER_INFO which is only refreshed on a reconnect and so goes stale after an in-place
    // primary<->replica switch. The probe runs this each poll to report the true serving role.
    private static final String STATUS_QUERY = "switch status";
    // WebSocketResponse.STATUS_INTERNAL_ERROR. Every transport-level terminal failure is
    // reported with this status (QwpEgressIoThread raises it on socket death), so it is what
    // separates "the connection died" from a SQL-level error on the same onError callback.
    // Inlined rather than imported: WebSocketResponse is a wire-protocol detail of the client.
    private static final byte QWP_STATUS_INTERNAL_ERROR = 0x06;
    // Zone for the query client (egress). Biases failover toward same-zone instances on
    // Enterprise; a no-op on OSS (which advertises no zone). Empty omits the key.
    private static final String DEFAULT_ZONE = "eu-west-1";

    // Rows sent (client-side) across all workers, for the once-per-second progress reporter.
    private static final AtomicLong TOTAL_SENT = new AtomicLong();
    // The ack watermark and the highest published frame sequence, per worker. These come
    // straight from the client (getAckedFsn / flushAndGetSequence) and are what the protocol
    // actually acknowledges, which is also what the client's own errors quote
    // ("targetFsn=10999, ackedFsn=5455"). No row-level ack figure is tracked: see reportTick().
    //
    // BOTH accessors use -1 as a sentinel: getAckedFsn() for "nothing acked yet" and
    // flushAndGetSequence() for "nothing to flush". Neither may reach these fields, or it
    // corrupts a sum and prints nonsense like "frames acked=-1/11". Clamp/guard at every
    // assignment, not at the point of display.
    private static AtomicLong[] ACKED_FSN = new AtomicLong[0];
    private static AtomicLong[] PUBLISHED_FSN = new AtomicLong[0];
    // Set once all rows are submitted. Marks the append-phase boundary for timing; it does
    // NOT silence output, because submission finishing is not the end of the story.
    private static volatile boolean SUBMIT_COMPLETE = false;

    public static void main(String[] args) throws Exception {
        // Parse CLI flags
        Map<String, String> a = parseArgs(args);

        final String addrsCsv = a.getOrDefault("--addrs", DEFAULT_ADDRS);
        final String token = a.get("--token");               // optional
        final String username = a.get("--username");         // optional
        final String password = a.get("--password");         // optional
        final long totalEvents = Long.parseLong(a.getOrDefault("--total-events", String.valueOf(DEFAULT_TOTAL_EVENTS)));
        final int delayMs = Integer.parseInt(a.getOrDefault("--delay-ms", String.valueOf(DEFAULT_DELAY_MS)));
        final long rate = Long.parseLong(a.getOrDefault("--rate", String.valueOf(DEFAULT_RATE)));
        final int numSenders = Integer.parseInt(a.getOrDefault("--num-senders", String.valueOf(DEFAULT_NUM_SENDERS)));
        final int retryTimeout = Integer.parseInt(a.getOrDefault("--retry-timeout", String.valueOf(DEFAULT_RETRY_TIMEOUT)));
        final String csvPath = a.getOrDefault("--csv", DEFAULT_CSV);
        final boolean timestampFromFile = Boolean.parseBoolean(a.getOrDefault("--timestamp-from-file",
                String.valueOf(DEFAULT_TIMESTAMP_FROM_FILE)));
        final long secondsOffset = Long.parseLong(a.getOrDefault("--seconds-offset",
                String.valueOf(DEFAULT_SECONDS_OFFSET)));
        final String protocol = a.getOrDefault("--protocol", DEFAULT_PROTOCOL);
        final String senderIdBase = a.getOrDefault("--sender-id", DEFAULT_SENDER_ID);
        final String storeForwardDir = a.getOrDefault("--store-forward-dir", DEFAULT_STORE_FORWARD_DIR);
        final int batchSize = Integer.parseInt(a.getOrDefault("--batch-size", String.valueOf(DEFAULT_BATCH_SIZE)));
        final int batchesPerTransaction = Integer.parseInt(a.getOrDefault("--batches-per-transaction",
                String.valueOf(DEFAULT_BATCHES_PER_TRANSACTION)));
        final long closeFlushTimeoutMs = Long.parseLong(a.getOrDefault("--close-flush-timeout-ms",
                String.valueOf(DEFAULT_CLOSE_FLUSH_TIMEOUT_MS)));
        final long probeIntervalMs = Long.parseLong(a.getOrDefault("--probe-interval-ms",
                String.valueOf(DEFAULT_PROBE_INTERVAL_MS)));
        // Enterprise-only: request durable acks (data durably uploaded). OSS servers do not
        // support it and the connection is rejected, so it is off by default.
        final boolean enterprise = Boolean.parseBoolean(a.getOrDefault("--enterprise", "false"));
        final String zone = a.getOrDefault("--zone", DEFAULT_ZONE);
        // QWP + probe: bound a single TCP connect attempt (0 disables). ILP is left unchanged.
        final int connectTimeoutMs = Integer.parseInt(a.getOrDefault("--connect-timeout-ms",
                String.valueOf(DEFAULT_CONNECT_TIMEOUT_MS)));

        if (!protocol.equals("qwp") && !protocol.equals("ilp") && !protocol.equals("qwpudp")) {
            System.err.println("--protocol must be 'qwp', 'qwpudp', or 'ilp', got: " + protocol);
            System.exit(2);
        }
        // QWP/UDP is fire-and-forget datagram ingest: the transport rejects authentication
        // (the server accepts any connection on the UDP port), so any credentials passed are
        // meaningless. Warn rather than fail, so the same command line works across transports.
        if (protocol.equals("qwpudp")) {
            final boolean hasAnyAuth = (token != null && !token.isEmpty())
                    || (username != null && !username.isEmpty())
                    || (password != null && !password.isEmpty());
            if (hasAnyAuth) {
                System.err.println("[warn] --protocol qwpudp is unauthenticated (UDP accepts any connection);"
                        + " ignoring --token/--username/--password");
            }
        }
        if (probeIntervalMs < 0) {
            System.err.println("--probe-interval-ms must be >= 0 (0 disables the probe)");
            System.exit(2);
        }
        if (connectTimeoutMs < 0) {
            System.err.println("--connect-timeout-ms must be >= 0 (0 uses the OS connect timeout)");
            System.exit(2);
        }
        if (batchSize <= 0) {
            System.err.println("--batch-size must be > 0");
            System.exit(2);
        }
        if (batchesPerTransaction <= 0) {
            System.err.println("--batches-per-transaction must be > 0");
            System.exit(2);
        }

        if (!Files.exists(Path.of(csvPath))) {
            System.err.println("CSV file not found: " + csvPath);
            System.exit(2);
        }
        if (numSenders <= 0) {
            System.err.println("--num-senders must be > 0");
            System.exit(2);
        }
        if (totalEvents <= 0) {
            System.err.println("--total-events must be > 0");
            System.exit(2);
        }
        if (rate < 0) {
            System.err.println("--rate must be >= 0 (0 disables rate limiting, falling back to --delay-ms)");
            System.exit(2);
        }
        // --rate takes precedence: when set it drives the pacing and --delay-ms is ignored.
        if (rate > 0 && delayMs > 0 && a.containsKey("--delay-ms")) {
            System.err.println("[warn] --rate " + rate + " overrides --delay-ms " + delayMs
                    + " (rate limiting drives pacing; the per-row delay is ignored)");
        }

        final SenderCfg cfg = new SenderCfg(protocol, addrsCsv, token, username, password, retryTimeout,
                senderIdBase, storeForwardDir, batchSize, batchesPerTransaction, numSenders, enterprise, zone,
                connectTimeoutMs, closeFlushTimeoutMs, rate);

        final String pacing = rate > 0
                ? "rate=" + rate + " rows/s (aggregate across " + numSenders + " workers)"
                : "delay-ms=" + delayMs;
        final String conf = buildConf(addrsCsv, token, username, password, retryTimeout);
        System.out.println("Pacing: " + pacing);
        System.out.println("Ingestion started. Protocol: " + protocol
                + (protocol.equals("qwp")
                        ? " (WebSocket, sender-id=" + senderIdBase + ", store-and-forward=" + storeForwardDir
                                + ", batch-size=" + batchSize + ", batches-per-transaction=" + batchesPerTransaction
                                + ", connect-timeout-ms=" + connectTimeoutMs + ", retry-timeout-ms=" + retryTimeout + ")"
                        : protocol.equals("qwpudp")
                                ? " (QWP/UDP datagrams, ingest-only, unauthenticated, no store-and-forward,"
                                        + " no failover; query client disabled, batch-size=" + batchSize + ")"
                                : "")
                + " | config: " + conf.replaceAll("(token=)([^;]+)", "$1***")
                .replaceAll("(password=)([^;]+)", "$1***"));

        final List<TradeRow> rows = loadCsv(csvPath, timestampFromFile);
        if (rows.isEmpty()) {
            System.err.println("CSV has no data rows.");
            System.exit(2);
        }

        final long base = totalEvents / numSenders;
        final long rem = totalEvents % numSenders;
        final ExecutorService exec = Executors.newFixedThreadPool(numSenders);
        final List<Future<?>> futures = new ArrayList<>(numSenders);

        // Time only the ingestion: start right before the workers begin sending.
        final long startNanos = System.nanoTime();

        // Per-worker acknowledged-row counters, summed by the reporter. Fed from the QWP ack
        // watermark (getAckedFsn) - the real committed progress, with no extra query round-trips.
        ACKED_FSN = new AtomicLong[numSenders];
        PUBLISHED_FSN = new AtomicLong[numSenders];
        for (int i = 0; i < numSenders; i++) {
            ACKED_FSN[i] = new AtomicLong();
            PUBLISHED_FSN[i] = new AtomicLong();
        }

        // A non-empty store-and-forward root at startup means a previous run died holding
        // unacked frames. The client replays them before (and alongside) new sends, and that
        // replay is otherwise invisible: it shows up only as seconds of submitted=0 with the
        // acked counter pinned at zero, because the backlog belongs to the previous process's
        // slot, so this process's own counters cannot describe it.
        final long sfStartBytes = sfDirBytes(storeForwardDir);
        if (sfStartBytes > 0) {
            banner("STORE-AND-FORWARD REPLAY ON STARTUP",
                    humanBytes(sfStartBytes) + " of pending frames in " + storeForwardDir,
                    "a previous run left this unacked; it replays before new sends");
        }

        // Records (to ~1s resolution) when all rows were submitted and when all were acknowledged,
        // so the summary can separate the submit phase from the commit-drain tail.
        final AtomicLong appendDoneNanos = new AtomicLong(0);
        final AtomicLong commitDoneNanos = new AtomicLong(0);

        // Progress reporter: once per second, prints BOTH counters - submitted (client-side, can
        // run ahead of the server) and acknowledged (rows the server has actually committed). The
        // gap between them is the buffered backlog; during the drain, submitted is flat while
        // acknowledged keeps climbing (real work), so there is no misleading "0 rows/s".
        // Rows between flushes, i.e. how often a frame is published. Mirrors runWorker's
        // commitEveryRows so the progress line can name the threshold.
        final long flushEveryRows = protocol.equals("qwp")
                ? (long) batchSize * batchesPerTransaction
                : 0L;
        final long[] lastSfBytes = {sfStartBytes};
        final Thread reporter = new Thread(() -> {
            long lastSub = 0;
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    return;
                }
                try {
                    reportTick(totalEvents, storeForwardDir, flushEveryRows,
                            appendDoneNanos, lastSfBytes, lastSub);
                    lastSub = TOTAL_SENT.get();
                } catch (RuntimeException e) {
                    // Never let one bad tick silence progress for the rest of the run. A dead
                    // reporter looks exactly like a stalled sender, and that ambiguity cost real
                    // debugging time when an UncheckedIOException from the spill walk killed
                    // this thread mid-outage.
                    System.out.printf("[progress] reporter tick failed: %s%n", e);
                }
            }
        });
        reporter.setDaemon(true);
        reporter.start();

        // Probe (QWP/WebSocket only): a separate thread polls the latest ingested timestamp
        // over a QWP query client. Same hosts/auth as the senders; it fails over automatically.
        // Skipped for ilp and for qwpudp: UDP is ingest-only (there is no query path), so the
        // equals("qwp") guard deliberately excludes it.
        final Thread probe = (protocol.equals("qwp") && probeIntervalMs > 0)
                ? startProbe(cfg, probeIntervalMs)
                : null;

        for (int id = 0; id < numSenders; id++) {
            final long eventsForThis = base + (id < rem ? 1 : 0);
            final int senderId = id;
            //futures.add(exec.submit(() -> runWorker(senderId, eventsForThis, delayMs, timestampFromFile, rows, conf)));
            futures.add(exec.submit(() -> runWorker(senderId, eventsForThis, delayMs, timestampFromFile, secondsOffset, rows, cfg)));
        }

        // Wait for completion
        exec.shutdown();
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException ee) {
                System.err.println("Worker failed: " + ee.getCause() + upgradeHint(ee.getCause()));
                System.exit(1);
            }
        }
        reporter.interrupt();
        if (probe != null) {
            probe.interrupt();
        }
        final double elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        final double rowsPerSec = elapsedSec > 0 ? totalEvents / elapsedSec : 0;
        System.out.printf("All workers completed. protocol=%s events=%d elapsed=%.3f s throughput=%,.0f rows/s (acknowledged, end-to-end)%n",
                protocol, totalEvents, elapsedSec, rowsPerSec);
        // Split the wall time into submit vs commit-drain, so the fast "submitted" rate is not
        // mistaken for durable throughput. appendDoneNanos/commitDoneNanos are ~1s-resolution.
        final long submitNanos = appendDoneNanos.get();
        if (submitNanos > 0) {
            final double submitSec = (submitNanos - startNanos) / 1_000_000_000.0;
            final double submitRate = submitSec > 0 ? totalEvents / submitSec : 0;
            System.out.printf("  submit phase %.3f s (%,.0f rows/s submitted) + commit drain %.3f s%n",
                    submitSec, submitRate, Math.max(0.0, elapsedSec - submitSec));
        }
    }

    private static void runWorker(
            int senderId,
            long totalEvents,
            int delayMs,
            boolean timestampFromFile,
            long secondsOffset,
            List<TradeRow> rows,
            SenderCfg cfg
    ) {
        System.out.printf("Sender %d will send %d events%n", senderId, totalEvents);
        long sent = 0;
        final boolean isQwp = cfg.protocol.equals("qwp");
        final boolean isUdp = cfg.protocol.equals("qwpudp");
        // Single worker on a QWP transport (WebSocket or UDP): stamp each row with the current
        // time client-side. A single thread's timestamps are monotonic (no O3) and this avoids
        // QWP's per-batch atNow() stamping. ILP, or more than one worker, use atNow()
        // (server-side, O3-safe).
        final boolean perRowMicros = (isQwp || isUdp) && cfg.numSenders == 1;
        // Flush cadence, by transport:
        //   qwp    - transactional commit every batchSize * batchesPerTransaction rows (flush commits).
        //   qwpudp - no transactions; flush every batchSize rows to emit datagrams (bounds the buffer).
        //   ilp    - 0: no explicit mid-loop flush (the HTTP client auto-flushes).
        final long commitEveryRows = isQwp
                ? (long) cfg.batchSize * cfg.batchesPerTransaction
                : isUdp
                        ? cfg.batchSize
                        : 0L;
        // Rate limiting (when --rate > 0): pace this worker to its share of the aggregate
        // target, rate / num-senders rows/second. intervalNanos is the ideal spacing between
        // this worker's rows; we track a deadline schedule from loop entry and only sleep when
        // we are running ahead of it, so rows go out back-to-back until we get ahead. This
        // reaches high rates that a fixed per-row Thread.sleep cannot. 0 => rate disabled.
        final double intervalNanos = cfg.rate > 0
                ? 1_000_000_000.0 * cfg.numSenders / cfg.rate
                : 0.0;
        final boolean rateLimited = intervalNanos > 0.0;
        final long paceStartNanos = System.nanoTime();
        try ( Sender sender = buildSender(cfg, senderId)) { //( Sender sender = Sender.fromConfig(conf)) {
            final int n = rows.size();
            for (long i = 0; i < totalEvents; i++) {
                TradeRow r = rows.get((int) (i % n));

                // Build row. trade_id = <worker>-<1-based sequence>, monotonic per sender, so you
                // can check completeness/gaps independent of timestamps and (with dedup) get
                // idempotent replay. It is a high-cardinality VARCHAR, deliberately NOT a symbol.
                sender.table("trades")
                        .symbol("symbol", r.symbol)
                        .symbol("side", r.side)
                        .doubleColumn("price", r.price)
                        .doubleColumn("amount", r.amount)
                        .stringColumn("trade_id", senderId + "-" + (i + 1));

                if (timestampFromFile) {
                    Instant ts = Instant.parse(r.timestamp);
                    if (secondsOffset != 0) {
                        ts = ts.plusSeconds(secondsOffset);
                    }
                    atNanos(sender, ts);
                } else if (secondsOffset != 0) {
                    atNanos(sender, Instant.now().plusSeconds(secondsOffset));
                } else if (perRowMicros) {
                    atNanos(sender, Instant.now());
                } else {
                    sender.atNow();
                }

                sent++;
                TOTAL_SENT.incrementAndGet();

                // Commit a transaction every batchSize * batchesPerTransaction rows (QWP), or flush
                // every batchSize rows (UDP). For QWP, record the flush sequence and refresh this
                // worker's acknowledged-row count from the ack watermark (no extra round-trip).
                if (commitEveryRows > 0 && sent % commitEveryRows == 0) {
                    if (isQwp) {
                        // flushAndGetSequence() returns -1 when there was nothing left to
                        // flush; storing that would clobber the real published frame.
                        final long fsn = sender.flushAndGetSequence();
                        if (fsn >= 0) {
                            PUBLISHED_FSN[senderId].set(fsn);
                        }
                        ACKED_FSN[senderId].set(Math.max(0L, sender.getAckedFsn()));
                    } else {
                        sender.flush();
                    }
                }

                if (rateLimited) {
                    // Deadline for the row we have just sent (sent is 1-based here). Sleep only
                    // while ahead of schedule; if behind, fall through and keep sending. The 1ms
                    // floor coalesces many rows into one sleep so we do not burn CPU sleeping for
                    // sub-millisecond slivers at high rates.
                    final long targetNanos = paceStartNanos + Math.round(sent * intervalNanos);
                    final long sleepNanos = targetNanos - System.nanoTime();
                    if (sleepNanos > 1_000_000L) {
                        try {
                            Thread.sleep(sleepNanos / 1_000_000L, (int) (sleepNanos % 1_000_000L));
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Interrupted", ie);
                        }
                    }
                } else if (delayMs > 0) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted", ie);
                    }
                }
            }
            // Final drain, done HERE rather than inside close(), and in slices so it stays
            // observable.
            //
            // close() runs its own implicit drain bounded by close_flush_timeout_millis
            // (default 60s), which is independent of --retry-timeout: a longer outage used to
            // die with "close() drain timed out ... data may be lost" while still holding
            // minutes of unused reconnect budget. Worse, while blocked inside close() nothing
            // can refresh the acked counter or the probe, so the most interesting window of a
            // durability demo was invisible.
            //
            // Sender.drain(timeoutMillis) is the documented way to take that wait back: "the
            // same shape as the implicit drain close() runs, with the caller controlling the
            // timeout per call-site". Polling it in ~1s slices lets the reporter and the probe
            // keep working, and the counter climb, for the whole drain. The budget is
            // --retry-timeout, so there is now ONE timeout governing how long we wait for the
            // server rather than two that disagree.
            if (isQwp) {
                final long finalSeq = sender.flushAndGetSequence();
                if (finalSeq >= 0) {
                    PUBLISHED_FSN[senderId].set(finalSeq);
                }
                final long drainDeadline = System.nanoTime() + cfg.retryTimeout * 1_000_000L;
                boolean drained = false;
                while (System.nanoTime() < drainDeadline) {
                    drained = sender.drain(DRAIN_SLICE_MS);
                    ACKED_FSN[senderId].set(Math.max(0L, sender.getAckedFsn()));
                    if (drained) {
                        break;
                    }
                }
                if (!drained) {
                    // Not fatal: with store-and-forward the unacked remainder stays on disk and
                    // the next sender replays it (Sender.closeFlushTimeoutMillis javadoc:
                    // "recovered by the next sender in SF mode"). Say so plainly instead of
                    // letting close() raise "data may be lost" for data that is not lost.
                    //
                    System.out.printf("Sender %d drain budget of %,dms elapsed; frames %,d/%,d "
                                    + "acked, %s still spilled. It stays in %s and replays on "
                                    + "next start%n",
                            senderId, cfg.retryTimeout,
                            ACKED_FSN[senderId].get(), PUBLISHED_FSN[senderId].get(),
                            humanBytes(sfDirBytes(cfg.storeForwardDir)),
                            cfg.storeForwardDir + "/" + senderId);
                }
            } else {
                sender.flush();
            }
            // "finished" is deliberately NOT printed yet. The try-with-resources close() below
            // still runs (briefly, per --close-flush-timeout-ms), and announcing completion
            // before it returns would be premature.
        } catch (Exception e) {
            System.err.printf("Sender %d got error: %s%s%n", senderId, e.toString(), upgradeHint(e));
            throw new RuntimeException(e);
        }
        // Reached only after close() returned.
        System.out.printf("Sender %d finished sending %d events%n", senderId, sent);
    }

    // One progress tick. Everything here is MEASURED, nothing is inferred:
    //   submitted    - our own counter of rows handed to the client
    //   frames acked - the client's ack watermark against the highest frame it published
    //   backlog      - bytes actually on disk in the store-and-forward root
    //
    // There is deliberately no row-level "acknowledged" figure. Translating the frame watermark
    // back into rows needed a side map of fsn -> cumulative rows, and that translation produced
    // six distinct wrong answers during development: premature saturation before close(),
    // 100,000-row quantisation, a gate that silenced this reporter 7s into a 98s drain, a 0 that
    // should have been "unknown" on a resumed slot, a -1 sentinel poisoning the map so a 19,579
    // frame run reported 100,000,000 rows acked, and the same sentinel printing "/-1" as a
    // denominator. Frames and bytes have never been wrong. Rows are gone.
    private static void reportTick(long totalEvents, String storeForwardDir, long commitEveryRows,
                                   AtomicLong appendDoneNanos, long[] lastSfBytes, long lastSub) {
        final long sub = TOTAL_SENT.get();
        if (sub >= totalEvents) {
            appendDoneNanos.compareAndSet(0, System.nanoTime());
            SUBMIT_COMPLETE = true;
        }

        long ackedFsn = 0;
        long publishedFsn = 0;
        boolean anyPublished = false;
        for (int i = 0; i < ACKED_FSN.length; i++) {
            ackedFsn += ACKED_FSN[i].get();
            final long pub = PUBLISHED_FSN[i].get();
            if (pub > 0) {
                publishedFsn += pub;
                anyPublished = true;
            }
        }
        if (commitEveryRows <= 0) {
            // Not QWP: no frame sequence numbers and no acks to report (UDP is fire-and-forget,
            // ILP auto-flushes over HTTP), so the frames field would be meaningless.
            System.out.printf("[progress] submitted=%,d (+%,d/s)%n", sub, sub - lastSub);
        } else {
            System.out.printf("[progress] submitted=%,d (+%,d/s) | frames acked=%s%n",
                    sub, sub - lastSub,
                    anyPublished
                            ? String.format("%,d/%,d", ackedFsn, publishedFsn)
                            // No frame published yet, which is normal early on: a flush happens
                            // every batch-size x batches-per-transaction rows, so at a paced feed
                            // the first one can be many seconds away. Name the threshold rather
                            // than leaving a bare "pending" to be reverse-engineered.
                            : String.format("none yet, first flush at %,d rows", commitEveryRows));
        }

        // Watch the spill shrink. Reported only while there is something there, so a healthy run
        // with no backlog stays quiet.
        final long[] sfStats = sfDirStats(storeForwardDir);
        final long sfNow = sfStats[0];
        if (sfNow > 0) {
            // Bytes are ALLOCATED, not used: segments are memory-mapped and pre-allocated to
            // sf_max_segment_bytes (4 MiB by default), so a run holding a few hundred KB still
            // shows a whole segment. The file count makes that readable at demo scale, and both
            // figures stay meaningful once a real backlog builds.
            System.out.printf("[store-and-forward] spill: %s allocated in %,d segment file(s)%s%n",
                    humanBytes(sfNow), sfStats[1],
                    lastSfBytes[0] > 0 && sfNow != lastSfBytes[0]
                            ? String.format(" (%+.1f MiB/s)",
                                (sfNow - lastSfBytes[0]) / (1024.0 * 1024.0))
                            : "");
        }
        lastSfBytes[0] = sfNow;
    }

    // Bytes currently spilled under the store-and-forward root. The client exposes no replay
    // event and no backlog metric, so the filesystem is the only observable signal that a
    // restart is replaying a previous run's unacked frames. Cheap enough to sample once a
    // second: a few hundred segment files.
    // Rule width for the attention banners below. The events they wrap (a write failing over to
    // another node, reads moving to a replica, a spilled backlog replaying) are the whole point of
    // an HA demo, and as ordinary one-line output they scrolled past unnoticed among per-second
    // progress lines.
    private static final String BANNER_RULE =
            "############################################################################";

    // Rule lines above and below the message. Three each side, not one: a single rule still got
    // lost in the scroll of per-second progress and probe lines during a live demo.
    private static final int BANNER_RULE_LINES = 3;

    private static void banner(String... lines) {
        final StringBuilder sb = new StringBuilder();
        sb.append('\n');
        for (int i = 0; i < BANNER_RULE_LINES; i++) {
            sb.append(BANNER_RULE).append('\n');
        }
        for (String line : lines) {
            sb.append("###  ").append(line).append('\n');
        }
        for (int i = 0; i < BANNER_RULE_LINES; i++) {
            sb.append(BANNER_RULE).append('\n');
        }
        sb.append('\n');
        // One write, so the banner cannot be interleaved by the reporter or probe threads.
        System.out.print(sb);
    }

    // {bytes, fileCount} for the spill root, from a SINGLE walk: two separate traversals a
    // second apart could disagree with each other while segments rotate.
    private static long[] sfDirStats(String dir) {
        final Path root = Path.of(dir);
        if (!Files.exists(root)) {
            return new long[]{0L, 0L};
        }
        long bytes = 0L;
        long count = 0L;
        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            for (Path f : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
                try {
                    bytes += Files.size(f);
                    count++;
                } catch (IOException | RuntimeException ignored) {
                    // Segment reclaimed mid-walk; it simply no longer counts.
                }
            }
        } catch (IOException | RuntimeException e) {
            // Files.walk wraps a mid-iteration failure in UncheckedIOException, a RuntimeException
            // that slips past catch (IOException). Store-and-forward reclaims segments
            // continuously, so entries really do vanish under the walk, and letting that escape
            // once killed the reporter thread outright.
            return new long[]{bytes, count};
        }
        return new long[]{bytes, count};
    }

    private static long sfDirBytes(String dir) {
        return sfDirStats(dir)[0];
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024L) {
            return bytes + " B";
        }
        if (bytes < 1024L * 1024L) {
            return String.format("%.1f KiB", bytes / 1024.0);
        }
        if (bytes < 1024L * 1024L * 1024L) {
            return String.format("%.1f MiB", bytes / (1024.0 * 1024.0));
        }
        return String.format("%.2f GiB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    // Unacked rows still owed to the server, i.e. what store-and-forward has to replay. The client
    // exposes no replay event: SenderConnectionEvent.Kind carries only CONNECTED, DISCONNECTED,
    // RECONNECTED, FAILED_OVER, ENDPOINT_ATTEMPT_FAILED, ALL_ENDPOINTS_UNREACHABLE and AUTH_FAILED,
    // and bufferView() is just the unflushed local buffer, not the spilled backlog. So report the
    // spill's size on disk, which is measured rather than inferred; the backlog shrinking in the
    // per-tick lines afterwards IS the replay.
    private static String backlogNote(String storeForwardDir) {
        final long bytes = sfDirBytes(storeForwardDir);
        if (bytes <= 0) {
            return "";
        }
        return String.format(" -- REPLAYING store-and-forward backlog of %s", humanBytes(bytes));
    }

    // Send the designated timestamp at NANOSECOND resolution. The client's at(Instant) path
    // delivers only microseconds (sub-microsecond digits are dropped before the wire), so we
    // convert to epoch-nanos and use at(long, NANOS) to carry full nanosecond precision.
    // QuestDB stores at the target column's resolution: a micros TIMESTAMP column silently
    // truncates the extra digits, a TIMESTAMP_NS column keeps them. The multiply stays within
    // long range for any realistic date (epochSecond * 1e9 overflows only past year ~2262).
    private static void atNanos(Sender sender, Instant ts) {
        final long epochNanos = ts.getEpochSecond() * 1_000_000_000L + ts.getNano();
        sender.at(epochNanos, ChronoUnit.NANOS);
    }

    private static List<TradeRow> loadCsv(String path, boolean needTimestamp) throws Exception {
        List<TradeRow> out = new ArrayList<>(1024);
        try (InputStream in0 = Files.newInputStream(Path.of(path));
            InputStream in = path.endsWith(".gz") ? new GZIPInputStream(in0) : in0;
            InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(isr);
            CSVReader reader = new CSVReader(br)) {

            String[] header = reader.readNext();
            if (header == null) {
                return out;
            }
            Map<String, Integer> idx = headerIndex(
                    header,
                    new String[]{"symbol", "side", "price", "amount"},
                    needTimestamp ? new String[]{"timestamp"} : new String[]{}
            );

            String[] row;
            while ((row = reader.readNext()) != null) {
                if (row.length == 0) continue;
                TradeRow tr = new TradeRow();
                tr.symbol = row[idx.get("symbol")].trim();
                tr.side = row[idx.get("side")].trim();
                tr.price = Double.parseDouble(row[idx.get("price")].trim());
                tr.amount = Double.parseDouble(row[idx.get("amount")].trim());
                if (needTimestamp) {
                    tr.timestamp = row[idx.get("timestamp")].trim();
                }
                out.add(tr);
            }
        }
        return out;
    }

    private static Map<String, Integer> headerIndex(String[] header, String[] required, String[] requiredIfNeeded) {
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < header.length; i++) {
            idx.put(header[i].trim(), i);
        }
        for (String r : required) {
            if (!idx.containsKey(r)) {
                throw new IllegalArgumentException("CSV missing required column: " + r + " in header " + Arrays.toString(header));
            }
        }
        for (String r : requiredIfNeeded) {
            if (!idx.containsKey(r)) {
                throw new IllegalArgumentException("CSV missing required column: " + r + " in header " + Arrays.toString(header));
            }
        }
        return idx;
    }

    private static LineSenderBuilder buildBuilder(String addrsCsv, String token, String username, String password, int retryTimeout) {
        String[] addrs = Arrays.stream(addrsCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);

        boolean hasToken = token != null && !token.isEmpty();
        boolean hasBasic = username != null && !username.isEmpty() && password != null && !password.isEmpty();

        LineSenderBuilder sb = Sender.builder(Sender.Transport.HTTP);

        if ((hasToken || hasBasic)) {
            sb =  sb.enableTls().advancedTls().disableCertificateValidation();
        }

        for (String addr : addrs) {
            sb.address(addr);
        }

        if (hasToken) {
            sb.httpToken(token);
        } else if (hasBasic) {
            sb.httpUsernamePassword(username, password);
        }

        sb.retryTimeoutMillis(retryTimeout);
        //sb.maxBackoffMillis(5000);
        sb.protocolVersion(2);

        return sb;
    }

    // Builds a Sender for the configured transport. The ILP branch is the existing
    // HTTP construction, verbatim, so --protocol ilp is identical to prior behaviour.
    // The QWP branch adds store-and-forward and transactional commit; each worker gets
    // a unique senderId and spill dir.
    private static Sender buildSender(SenderCfg cfg, int workerId) {
        if ("ilp".equals(cfg.protocol)) {
            return buildBuilder(cfg.addrsCsv, cfg.token, cfg.username, cfg.password, cfg.retryTimeout).build();
        }

        if ("qwpudp".equals(cfg.protocol)) {
            // ---- QWP/UDP branch ----
            // Fire-and-forget datagrams to the UDP ingest port (:9007 by convention). The
            // transport rejects authentication and does not use TLS, store-and-forward,
            // failover, or a connection listener (there is no persistent connection). Any
            // token/basic auth on the command line was already warned about and is ignored.
            String[] udpAddrs = Arrays.stream(cfg.addrsCsv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new);

            LineSenderBuilder u = Sender.builder(Sender.Transport.UDP);
            for (String addr : udpAddrs) {
                u.address(addr);
            }
            return u.build();
        }

        // ---- QWP (WebSocket) branch ----
        String[] addrs = Arrays.stream(cfg.addrsCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);

        boolean hasToken = cfg.token != null && !cfg.token.isEmpty();
        boolean hasBasic = cfg.username != null && !cfg.username.isEmpty()
                && cfg.password != null && !cfg.password.isEmpty();

        LineSenderBuilder b = Sender.builder(Sender.Transport.WEBSOCKET);

        // TLS is decided the same way as the ILP path: on when token/basic auth is present.
        if ((hasToken || hasBasic)) {
            b = b.enableTls().advancedTls().disableCertificateValidation();
        }

        for (String addr : addrs) {
            b.address(addr);
        }

        if (hasToken) {
            b.httpToken(cfg.token);
        } else if (hasBasic) {
            b.httpUsernamePassword(cfg.username, cfg.password);
        }

        final String who = cfg.senderIdBase + "-" + workerId;   // UNIQUE per worker/server
        final String sfPath = cfg.storeForwardDir + "/" + who;
        try {
            Files.createDirectories(Path.of(sfPath));
        } catch (Exception e) {
            System.err.printf("[%s] WARN: could not pre-create store-and-forward dir %s: %s%n",
                    who, sfPath, e.getMessage());
        }

        // Narrate connection state changes so a host going down and the failover to the next
        // host in --addrs is visible. The repetitive backoff events (disconnected / endpoint
        // failed / all unreachable) are throttled to ~once every 3s so they do not bury the
        // meaningful transitions (connected / failed over / reconnected) or the query client.
        final long[] lastNoisyMs = {0L};
        final SenderConnectionListener connListener = event -> {
            final String host = event.getHost() + ":" + event.getPort();
            final String cause = event.getCause() != null
                    ? String.valueOf(event.getCause().getMessage()) : "no detail";
            final String msg;
            boolean noisy = false;
            // Wrap in a banner: a write path moving to another node is a headline event.
            boolean prominent = false;
            switch (event.getKind()) {
                case CONNECTED:
                    msg = "connected to " + host;
                    break;
                case RECONNECTED:
                    msg = "reconnected to " + host + backlogNote(cfg.storeForwardDir);
                    prominent = true;
                    break;
                case FAILED_OVER:
                    msg = "failed over " + event.getPreviousHost() + ":" + event.getPreviousPort()
                            + " -> " + host + backlogNote(cfg.storeForwardDir);
                    prominent = true;
                    break;
                case AUTH_FAILED:
                    msg = "auth failed for " + host;
                    break;
                // NOTE: no RECONNECT_BUDGET_EXHAUSTED case on purpose. That kind was dropped from
                // the client's SenderConnectionEvent.Kind in newer builds (e.g. 1.3.6-SNAPSHOT), so
                // switching on it fails to compile there. If a client still emits it, the default
                // branch below narrates it generically.
                case DISCONNECTED:
                    msg = "connection lost to " + host + " (" + cause + "), will retry";
                    noisy = true;
                    break;
                case ENDPOINT_ATTEMPT_FAILED:
                    msg = "endpoint " + host + " failed (" + cause + "), trying next" + upgradeHint(event.getCause());
                    noisy = true;
                    break;
                case ALL_ENDPOINTS_UNREACHABLE:
                    msg = "all endpoints unreachable, backing off";
                    noisy = true;
                    break;
                default:
                    msg = event.getKind() + " host=" + host;
                    noisy = true;
            }
            if (noisy) {
                final long now = System.currentTimeMillis();
                if (now - lastNoisyMs[0] < 3000L) {
                    return;   // throttle repetitive backoff spam
                }
                lastNoisyMs[0] = now;
            }
            if (prominent) {
                banner("INGESTION " + msg, "sender=" + who);
            } else {
                System.out.printf("[ingestion client %s] %s%n", who, msg);
            }
        };

        // Enterprise-only: hold spilled frames until a durable (committed) ack. OSS servers
        // reject this during the WebSocket upgrade, so it is gated behind --enterprise.
        if (cfg.enterprise) {
            b.requestDurableAck(true);
        }

        // Bound a single TCP connect so a black-holed host fails over fast (0 = OS default).
        if (cfg.connectTimeoutMs > 0) {
            b.connectTimeoutMillis(cfg.connectTimeoutMs);
        }

        // Keep close() from re-running a long implicit drain. runWorker() already drained in
        // observable slices against --retry-timeout, and whatever is still unacked is replayed by
        // the next sender from the spill directory, so a long close only delays shutdown and
        // produces a "data may be lost" error for data that is not lost. 0 opts out entirely.
        b.closeFlushTimeoutMillis(cfg.closeFlushTimeoutMs);

        return b.storeAndForwardDir(sfPath)
                .senderId(who)
                .transactional(true)
                .connectionListener(connListener)
                // reconnectMaxDurationMillis is the QWP analog of the ILP retryTimeoutMillis:
                // the overall "keep retrying" budget. Driven by --retry-timeout on both transports.
                .reconnectMaxDurationMillis(cfg.retryTimeout)
                .reconnectInitialBackoffMillis(100)
                .reconnectMaxBackoffMillis(5_000)
                .autoFlushBytes(524_288)              // 512 KiB, under the ~1MB WS frame cap
                .autoFlushRows(cfg.batchSize)         // one batch = one deferred append
                .autoFlushIntervalMillis(1_000)
                .build();
    }

    // Config string for the QWP query client: same hosts and token/auth as the senders,
    // ws/wss chosen the same way (TLS on when token/basic auth is present), failover on
    // for more than one host. Matches the reference project's queryClientConfig().
    private static String queryClientConfig(SenderCfg cfg) {
        String[] addrs = Arrays.stream(cfg.addrsCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);

        boolean hasToken = cfg.token != null && !cfg.token.isEmpty();
        boolean hasBasic = cfg.username != null && !cfg.username.isEmpty()
                && cfg.password != null && !cfg.password.isEmpty();
        boolean tls = hasToken || hasBasic;

        StringBuilder sb = new StringBuilder(tls ? "wss" : "ws")
                .append("::addr=").append(String.join(",", addrs)).append(';');
        if (hasToken) {
            sb.append("token=").append(cfg.token).append(';');
        } else if (hasBasic) {
            sb.append("username=").append(cfg.username).append(';')
              .append("password=").append(cfg.password).append(';');
        }
        if (tls) {
            sb.append("tls_verify=unsafe_off;");
        }
        // Bound the probe's TCP connect too, so it fails over as fast as the senders (0 = OS default).
        if (cfg.connectTimeoutMs > 0) {
            sb.append("connect_timeout=").append(cfg.connectTimeoutMs).append(';');
        }
        if (addrs.length > 1) {
            sb.append("failover=on;");
        }
        if (cfg.zone != null && !cfg.zone.isEmpty()) {
            sb.append("zone=").append(cfg.zone).append(';');
        }
        return sb.toString();
    }

    // Starts a daemon thread that polls the latest ingested timestamp over a QWP query
    // client every intervalMs, printing to stdout. Independent of the senders. The client
    // fails over across the configured hosts automatically; onFailoverReset reports hops.
    //
    // The client is built INSIDE the poll loop and torn down on any connection loss, because
    // a QwpQueryClient cannot recover on its own:
    //   * connect() outside the loop means a server that is down at startup kills the probe
    //     thread outright, and nothing ever restarts it;
    //   * once a connection dies, the generation listener latches the terminal failure. With
    //     failover on, execute() eventually leaves connected=false and every later execute()
    //     throws "not connected; call connect() first". With failover off (a single --addrs
    //     host) it is sneakier: connected stays TRUE, isConnected() keeps returning true, and
    //     every execute() short-circuits on the latched failure and reports it through
    //     onError() forever.
    // Neither state clears by calling connect() again (it early-returns when connected), so
    // recovery means close() + fromConfig() on a fresh instance.
    private static Thread startProbe(SenderCfg cfg, long intervalMs) {
        final Thread t = new Thread(() -> {
            QwpQueryClient client = null;
            try {
                final long[] latest = {Long.MIN_VALUE};
                final boolean[] wasDown = {false};
                // Consecutive failed ticks. The probe exists to show read HA, so an outage has
                // to stay VISIBLE: reporting the loss once and then going quiet is
                // indistinguishable from the probe thread having died, and it also hides WHY
                // the reader is not picking up a replica. target defaults to any, so a replica
                // is an acceptable endpoint and connect() walks the address list; if a read is
                // not being served during a primary outage we need the per-attempt reason.
                final long[] downTicks = {0L};
                // Wall time the last successful connect() took. A dead endpoint that blackholes
                // SYN burns the whole connect_timeout before the walk reaches the next address,
                // so this is how you tell "failover is broken" from "failover is just slow".
                final long[] connectMs = {0L};
                // Set by the probe handler when the server reports a transport-level failure.
                // Transport failures always carry STATUS_INTERNAL_ERROR; a SQL-level error
                // (e.g. "table does not exist" before ingestion creates it) carries a different
                // status and must NOT tear the connection down.
                final boolean[] transportFailed = {false};
                final QwpColumnBatchHandler handler = new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                        batch.forEachRow(row -> {
                            if (!row.isNull(0)) {
                                latest[0] = row.getLongValue(0);
                            }
                        });
                    }

                    @Override
                    public void onEnd(long totalRows) {
                    }

                    @Override
                    public void onError(byte status, String message) {
                        if (status == QWP_STATUS_INTERNAL_ERROR) {
                            transportFailed[0] = true;
                        } else {
                            System.out.printf("[query client] server error: %s%n", message);
                        }
                    }

                    @Override
                    public void onFailoverReset(QwpServerInfo info) {
                        // Reads surviving an outage by moving to another node is THE thing an HA
                        // demo is showing, so it gets a banner rather than one line among many.
                        banner("READS FAILED OVER -- now served by role=" + QwpServerInfo.roleName(info.getRole()),
                                "node=" + orNone(info.getNodeId()) + " zone=" + orNone(info.getZoneId()),
                                "queries keep being answered; only writes need a primary");
                    }
                };
                // `switch status` reports the serving node's LIVE lifecycle role: current_role, plus
                // target_role while a switch is in flight. This is authoritative -- unlike the QWP
                // handshake role from getServerInfo(), it reflects an in-place promotion/demotion that
                // never dropped the read connection. We show current_role as THE role; getServerInfo()
                // only supplies node/zone, and its handshake role is a labelled fallback used solely
                // when the status query is unavailable (e.g. missing SYSTEM ADMIN). statusDiag says why.
                final String[] currentRole = {null};
                final String[] targetRole = {null};
                final String[] statusDiag = {null};
                final long[] lastStatusDiagMs = {0L};
                final QwpColumnBatchHandler statusHandler = new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                        final int cols = batch.getColumnCount();
                        final StringBuilder names = new StringBuilder();
                        for (int c = 0; c < cols; c++) {
                            if (names.length() > 0) {
                                names.append(',');
                            }
                            names.append(batch.getColumnName(c));
                        }
                        batch.forEachRow(row -> {
                            for (int c = 0; c < cols; c++) {
                                final String name = batch.getColumnName(c);
                                if (name == null) {
                                    continue;
                                }
                                final String lower = name.toLowerCase(java.util.Locale.ROOT);
                                if (!lower.contains("role")) {
                                    continue;
                                }
                                final String value = row.isNull(c) ? null : row.getString(c);
                                if (lower.contains("current")) {
                                    currentRole[0] = value;
                                } else if (lower.contains("target")) {
                                    targetRole[0] = value;
                                } else if (currentRole[0] == null) {
                                    // A single unqualified "role" column (older servers) is the current one.
                                    currentRole[0] = value;
                                }
                            }
                        });
                        if (currentRole[0] == null) {
                            statusDiag[0] = "'" + STATUS_QUERY + "' returned no current-role column; columns=[" + names + "]";
                        }
                    }

                    @Override
                    public void onEnd(long totalRows) {
                    }

                    @Override
                    public void onError(byte status, String message) {
                        statusDiag[0] = "'" + STATUS_QUERY + "' error (status " + status + "): " + message;
                    }

                    @Override
                    public void onFailoverReset(QwpServerInfo info) {
                    }
                };
                while (!Thread.currentThread().isInterrupted()) {
                    latest[0] = Long.MIN_VALUE;
                    currentRole[0] = null;
                    targetRole[0] = null;
                    statusDiag[0] = null;
                    transportFailed[0] = false;
                    final long tickStart = System.nanoTime();
                    try {
                        // (Re)build on first pass and after every connection loss. A server that
                        // is down at startup just leaves client == null and retries next tick.
                        if (client == null) {
                            final long connectStart = System.nanoTime();
                            client = QwpQueryClient.fromConfig(queryClientConfig(cfg));
                            client.connect();
                            connectMs[0] = (System.nanoTime() - connectStart) / 1_000_000L;
                            final QwpServerInfo info = client.getServerInfo();
                            final String what = wasDown[0]
                                    ? "connection restored after " + downTicks[0] + " failed attempt(s)"
                                    : "connected";
                            downTicks[0] = 0;
                            if (info != null) {
                                System.out.printf("[query client] %s in %dms, serving node=%s role=%s zone=%s cluster=%s%n",
                                        what, connectMs[0], orNone(info.getNodeId()),
                                        QwpServerInfo.roleName(info.getRole()),
                                        orNone(info.getZoneId()), orNone(info.getClusterId()));
                            } else {
                                System.out.printf("[query client] %s in %dms%n", what, connectMs[0]);
                            }
                            wasDown[0] = false;
                        }
                        client.execute(PROBE_QUERY, handler);
                        // A transport failure is reported through onError, not by throwing, and
                        // it leaves the client permanently latched. Force the rebuild path.
                        if (transportFailed[0] || !client.isConnected()) {
                            throw new IllegalStateException("query connection lost");
                        }
                        if (latest[0] != Long.MIN_VALUE) {
                            // trades designated timestamp is microseconds by default.
                            Instant ts = Instant.EPOCH.plus(latest[0], ChronoUnit.MICROS);
                            // Ask the serving node for its live role. A status-query failure must not
                            // look like a connection loss, so swallow it here (currentRole stays null).
                            try {
                                client.execute(STATUS_QUERY, statusHandler);
                            } catch (Exception ex) {
                                statusDiag[0] = "'" + STATUS_QUERY + "' threw: " + ex;
                            }
                            if (currentRole[0] == null && statusDiag[0] == null) {
                                statusDiag[0] = "'" + STATUS_QUERY + "' produced no row batch and no error"
                                        + " (not a SELECT-style result on the read path?)";
                            }
                            // Role comes from `switch status` (the authoritative live role of the serving
                            // node). getServerInfo() only supplies node/zone here; its handshake role is a
                            // labelled fallback used only when the status query is unavailable.
                            final QwpServerInfo si = client.getServerInfo();
                            final String node = si != null ? orNone(si.getNodeId()) : "(none)";
                            final String zone = si != null ? orNone(si.getZoneId()) : "(none)";
                            final String served;
                            if (currentRole[0] != null) {
                                final boolean switching = targetRole[0] != null
                                        && !targetRole[0].equalsIgnoreCase(currentRole[0]);
                                served = " served by role=" + currentRole[0]
                                        + (switching ? " (switching -> " + targetRole[0] + ")" : "")
                                        + " node=" + node + " zone=" + zone;
                            } else {
                                final String handshake = si != null ? QwpServerInfo.roleName(si.getRole()) : "unknown";
                                served = " served by role=" + handshake + " node=" + node + " zone=" + zone
                                        + " (handshake role; live 'switch status' unavailable, may be stale)";
                            }
                            // Unconditional: a read that is still being served is exactly what a
                            // failover demo has to keep showing, including after submission ends.
                            System.out.printf("[probe] latest trades timestamp = %s (raw=%d)%s%n",
                                    ts, latest[0], served);
                            // Explain a missing live role at most once per 30s so it does not spam.
                            if (currentRole[0] == null && statusDiag[0] != null) {
                                final long now = System.currentTimeMillis();
                                if (now - lastStatusDiagMs[0] > 30_000L) {
                                    lastStatusDiagMs[0] = now;
                                    System.out.println("[probe] live role unavailable: " + statusDiag[0]);
                                }
                            }
                        } else {
                            // Connected and the query succeeded, but no row came back. Say so
                            // rather than printing nothing, which would look identical to the
                            // read being down.
                            System.out.println("[probe] read OK but query returned no rows yet");
                        }
                    } catch (Exception e) {
                        downTicks[0]++;
                        if (!wasDown[0]) {
                            System.out.printf("[query client] connection lost (%s), will retry%n",
                                    String.valueOf(e.getMessage()));
                            wasDown[0] = true;
                        }
                        // Print EVERY failed attempt, not just the first. Reads are supposed to
                        // survive a primary outage by moving to a replica, so each tick the read
                        // is still down is the thing the demo needs to show, with the reason.
                        {
                            System.out.printf("[probe] read DOWN, attempt %d failed after %dms: %s%n",
                                    downTicks[0], (System.nanoTime() - tickStart) / 1_000_000L,
                                    String.valueOf(e.getMessage()));
                        }
                        // Drop the latched client; the next tick builds a fresh one.
                        //
                        // Dispose it on a throwaway thread rather than inline. close() joins the
                        // client's I/O thread for up to shutdownJoinMs, which is a hardcoded 5s
                        // (private, no setter and no connection-string key, still true in 1.3.9),
                        // and a dead connection reliably hits that full timeout. Closing inline
                        // therefore stalls this loop ~5s per failed tick, so the reader sits idle
                        // while a live replica is right there waiting to serve -- which is exactly
                        // why read failover looked broken. connect() itself walks every endpoint
                        // and only gives up with "all QWP endpoints unreachable", so the failover
                        // was never the problem; our teardown was.
                        //
                        // Safe to hand off: execute() has already returned, no handler is running,
                        // and the next tick builds a SEPARATE instance that shares no state.
                        if (client != null) {
                            final QwpQueryClient dead = client;
                            client = null;
                            final Thread reaper = new Thread(() -> {
                                try {
                                    dead.close();
                                } catch (Exception ignored) {
                                    // A dead connection often fails to close cleanly; nothing to do.
                                }
                            }, "qwp-probe-reaper");
                            reaper.setDaemon(true);
                            reaper.start();
                        }
                    }
                    Thread.sleep(intervalMs);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("[probe] stopped: " + e.getMessage() + upgradeHint(e));
            } finally {
                if (client != null) {
                    client.close();
                }
            }
        }, "qwp-probe");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static String orNone(String s) {
        return (s == null || s.isEmpty()) ? "(none)" : s;
    }

    // Turn a failed QWP WebSocket-upgrade throwable into a human-readable hint, using the
    // client's TYPED signals rather than string-matching the message:
    //   - QwpAuthFailedException  -> a definitive auth rejection (HTTP 401/403).
    //   - WebSocketUpgradeException.isRoleMismatch() (HTTP 421) -> the endpoint is not writable
    //     (a REPLICA / PRIMARY_CATCHUP), i.e. no primary is available among --addrs to accept writes.
    //   - WebSocketUpgradeException with status 400 -> a missing/malformed auth token (e.g. an
    //     unset token env var): the server refuses the upgrade before it can return a 401.
    // Walks the cause chain (the upgrade failure is usually wrapped). Returns "" otherwise.
    private static String upgradeHint(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof QwpAuthFailedException) {
                final QwpAuthFailedException a = (QwpAuthFailedException) c;
                return " -- auth rejected (HTTP " + a.getStatusCode() + "): bad/expired credentials"
                        + " or missing permission; check --token or --user/--password";
            }
            if (c instanceof WebSocketUpgradeException) {
                final WebSocketUpgradeException w = (WebSocketUpgradeException) c;
                if (w.isRoleMismatch()) {
                    return " -- endpoint is not writable (role=" + w.getServerRole() + "): no primary"
                            + " available among --addrs to accept writes";
                }
                if (w.getStatusCode() == 400) {
                    return " -- HTTP 400 on the upgrade: likely a missing or malformed ILP auth token"
                            + " (e.g. an unset token env var); check --token or --user/--password";
                }
            }
        }
        return "";
    }

    private static String buildConf(String addrsCsv, String token, String username, String password, int retryTimeout) {
        String[] addrs = Arrays.stream(addrsCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);

        boolean hasToken = token != null && !token.isEmpty();
        boolean hasBasic = username != null && !username.isEmpty() && password != null && !password.isEmpty();

        final String protocol = (hasToken || hasBasic) ? "https" : "http";
        StringBuilder sb = new StringBuilder(protocol).append("::");

        for (String addr : addrs) {
            sb.append("addr=").append(addr).append(";");
        }

        if (hasToken) {
            sb.append("token=").append(token).append(";");
        } else if (hasBasic) {
            sb.append("username=").append(username).append(";")
              .append("password=").append(password).append(";");
        }

        // TLS verify always off when using HTTPS, as requested
        if (!protocol.equals("http")) {
            sb.append("tls_verify=unsafe_off;");
        }

        sb.append("retry_timeout=").append(retryTimeout).append(";");
        sb.append("maxBackoffMillis=5000;");
        return sb.toString();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String k = args[i];
            switch (k) {
                case "--addrs":
                case "--token":
                case "--username":
                case "--password":
                case "--total-events":
                case "--delay-ms":
                case "--rate":
                case "--num-senders":
                case "--csv":
                case "--timestamp-from-file":
                case "--seconds-offset":
                case "--retry-timeout":
                case "--protocol":
                case "--sender-id":
                case "--store-forward-dir":
                case "--batch-size":
                case "--batches-per-transaction":
                case "--probe-interval-ms":
                case "--close-flush-timeout-ms":
                case "--connect-timeout-ms":
                case "--enterprise":
                case "--zone":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("Missing value for " + k);
                    }
                    out.put(k, args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + k);
            }
        }
        return out;
    }

    private static final class TradeRow {
        String symbol;
        String side;
        double price;
        double amount;
        String timestamp; // only used when timestampFromFile = true
    }

    // Immutable transport config carried into each worker so buildSender() can construct
    // a per-worker Sender (QWP needs a unique senderId + spill dir per worker).
    private static final class SenderCfg {
        final String protocol;
        final String addrsCsv;
        final String token;
        final String username;
        final String password;
        final int retryTimeout;
        final String senderIdBase;
        final String storeForwardDir;
        final int batchSize;
        final int batchesPerTransaction;
        final int numSenders;
        final boolean enterprise;
        final String zone;
        final int connectTimeoutMs;
        // close()'s implicit drain budget. Separate from retryTimeout on purpose: our own polled
        // drain owns the waiting, so this only bounds shutdown.
        final long closeFlushTimeoutMs;
        // Target aggregate rows/second across all workers; 0 = disabled (use delayMs).
        final long rate;

        SenderCfg(String protocol, String addrsCsv, String token, String username, String password,
                  int retryTimeout, String senderIdBase, String storeForwardDir,
                  int batchSize, int batchesPerTransaction, int numSenders, boolean enterprise, String zone,
                  int connectTimeoutMs, long closeFlushTimeoutMs, long rate) {
            this.protocol = protocol;
            this.addrsCsv = addrsCsv;
            this.token = token;
            this.username = username;
            this.password = password;
            this.retryTimeout = retryTimeout;
            this.senderIdBase = senderIdBase;
            this.storeForwardDir = storeForwardDir;
            this.batchSize = batchSize;
            this.batchesPerTransaction = batchesPerTransaction;
            this.numSenders = numSenders;
            this.enterprise = enterprise;
            this.zone = zone;
            this.connectTimeoutMs = connectTimeoutMs;
            this.closeFlushTimeoutMs = closeFlushTimeoutMs;
            this.rate = rate;
        }
    }
}
