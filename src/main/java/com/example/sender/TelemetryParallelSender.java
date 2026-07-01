package com.example.sender;

import io.questdb.client.Sender;
import io.questdb.client.Sender.LineSenderBuilder;
import io.questdb.client.SenderConnectionListener;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.opencsv.CSVReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

public class TelemetryParallelSender {

    // Defaults
    private static final String DEFAULT_ADDRS = "questdb:9000";
    private static final long DEFAULT_TOTAL_EVENTS = 1_000_000L;
    private static final int DEFAULT_DELAY_MS = 50;
    private static final int DEFAULT_NUM_SENDERS = 10;
    private static final int DEFAULT_RETRY_TIMEOUT = 360000;
    // New default CSV for Cisco baseline
    private static final String DEFAULT_CSV = "./cisco_baseline_500gbps.csv";
    private static final boolean DEFAULT_TIMESTAMP_FROM_FILE = false;
    private static final long DEFAULT_SECONDS_OFFSET = 0L;

    // QWP (WebSocket) transport
    private static final String DEFAULT_PROTOCOL = "qwp";
    private static final String DEFAULT_SENDER_ID = "ha_sender";
    private static final String DEFAULT_STORE_FORWARD_DIR = "/tmp/qdb-sf";
    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final int DEFAULT_BATCHES_PER_TRANSACTION = 10;
    // Probe (QWP only): poll the latest ingested timestamp on an interval, 0 disables.
    private static final long DEFAULT_PROBE_INTERVAL_MS = 1000L;
    private static final String PROBE_QUERY = "select timestamp from cisco_baseline limit -1";
    // Zone for the query client (egress). Biases failover toward same-zone instances on
    // Enterprise; a no-op on OSS (which advertises no zone). Empty omits the key.
    private static final String DEFAULT_ZONE = "eu-west-1";

    // Rows sent (client-side) across all workers, for the once-per-second progress reporter.
    private static final AtomicLong TOTAL_SENT = new AtomicLong();

    // Column classification
    private static final String TIMESTAMP_COLUMN = "time";

    private static final Set<String> SYMBOL_COLUMNS = new HashSet<>(Arrays.asList(
            "name",
            "EncodingPath",
            "Producer",
            "af_name",
            "instance_name",
            "interface_name",
            "node_name",
            "route_table_name",
            "saf_name",
            "vrf_name"));

    private static final Set<String> BOOLEAN_COLUMNS = new HashSet<>(Arrays.asList(
            "performance_statistics__global__ipv4rib_server__is_rib_connection_up"));

    public static void main(String[] args) throws Exception {
        // Parse CLI flags
        Map<String, String> a = parseArgs(args);

        final String addrsCsv = a.getOrDefault("--addrs", DEFAULT_ADDRS);
        final String token = a.get("--token"); // optional
        final String username = a.get("--username"); // optional
        final String password = a.get("--password"); // optional
        final long totalEvents = Long.parseLong(a.getOrDefault("--total-events", String.valueOf(DEFAULT_TOTAL_EVENTS)));
        final int delayMs = Integer.parseInt(a.getOrDefault("--delay-ms", String.valueOf(DEFAULT_DELAY_MS)));
        final int numSenders = Integer.parseInt(a.getOrDefault("--num-senders", String.valueOf(DEFAULT_NUM_SENDERS)));
        final int retryTimeout = Integer
                .parseInt(a.getOrDefault("--retry-timeout", String.valueOf(DEFAULT_RETRY_TIMEOUT)));
        final String csvPath = a.getOrDefault("--csv", DEFAULT_CSV);
        final boolean timestampFromFile = Boolean.parseBoolean(a.getOrDefault(
                "--timestamp-from-file",
                String.valueOf(DEFAULT_TIMESTAMP_FROM_FILE)));
        final long secondsOffset = Long.parseLong(a.getOrDefault("--seconds-offset",
                String.valueOf(DEFAULT_SECONDS_OFFSET)));
        final String protocol = a.getOrDefault("--protocol", DEFAULT_PROTOCOL);
        final String senderIdBase = a.getOrDefault("--sender-id", DEFAULT_SENDER_ID);
        final String storeForwardDir = a.getOrDefault("--store-forward-dir", DEFAULT_STORE_FORWARD_DIR);
        final int batchSize = Integer.parseInt(a.getOrDefault("--batch-size", String.valueOf(DEFAULT_BATCH_SIZE)));
        final int batchesPerTransaction = Integer.parseInt(a.getOrDefault("--batches-per-transaction",
                String.valueOf(DEFAULT_BATCHES_PER_TRANSACTION)));
        final long probeIntervalMs = Long.parseLong(a.getOrDefault("--probe-interval-ms",
                String.valueOf(DEFAULT_PROBE_INTERVAL_MS)));
        // Enterprise-only: request durable acks (data durably uploaded). OSS servers do not
        // support it and the connection is rejected, so it is off by default.
        final boolean enterprise = Boolean.parseBoolean(a.getOrDefault("--enterprise", "false"));
        final String zone = a.getOrDefault("--zone", DEFAULT_ZONE);

        if (!protocol.equals("qwp") && !protocol.equals("ilp")) {
            System.err.println("--protocol must be 'qwp' or 'ilp', got: " + protocol);
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
        if (probeIntervalMs < 0) {
            System.err.println("--probe-interval-ms must be >= 0 (0 disables the probe)");
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

        final SenderCfg cfg = new SenderCfg(protocol, addrsCsv, token, username, password, retryTimeout,
                senderIdBase, storeForwardDir, batchSize, batchesPerTransaction, numSenders, enterprise, zone);

        final String conf = buildConf(addrsCsv, token, username, password, retryTimeout);
        System.out.println("Ingestion started. Protocol: " + protocol
                + (protocol.equals("qwp")
                        ? " (WebSocket, sender-id=" + senderIdBase + ", store-and-forward=" + storeForwardDir
                                + ", batch-size=" + batchSize + ", batches-per-transaction=" + batchesPerTransaction + ")"
                        : "")
                + " | config: " + conf.replaceAll("(token=)([^;]+)", "$1***")
                        .replaceAll("(password=)([^;]+)", "$1***"));

        final CiscoCsvData data = loadCsv(csvPath);
        final String[] header = data.header;
        final List<String[]> rows = data.rows;

        if (rows.isEmpty()) {
            System.err.println("CSV has no data rows.");
            System.exit(2);
        }

        // Find index of the time column (optional)
        int tmpTimeIndex = -1;
        for (int i = 0; i < header.length; i++) {
            if (TIMESTAMP_COLUMN.equals(header[i])) {
                tmpTimeIndex = i;
                break;
            }
        }
        if (timestampFromFile && tmpTimeIndex < 0) {
            throw new IllegalArgumentException("CSV has no 'time' column but --timestamp-from-file=true was requested");
        }

        // This one is now effectively final and safe to capture in lambdas
        final int timeIndex = tmpTimeIndex;

        final long base = totalEvents / numSenders;
        final long rem = totalEvents % numSenders;
        final ExecutorService exec = Executors.newFixedThreadPool(numSenders);
        final List<Future<?>> futures = new ArrayList<>(numSenders);

        // Time only the ingestion: start right before the workers begin sending.
        final long startNanos = System.nanoTime();

        // Progress reporter: prints rows/s (client-side, sent) once per second.
        final Thread reporter = new Thread(() -> {
            long last = 0;
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    return;
                }
                long now = TOTAL_SENT.get();
                System.out.printf("[progress] sent=%,d rate=%,d rows/s%n", now, now - last);
                last = now;
            }
        });
        reporter.setDaemon(true);
        reporter.start();

        // Probe (QWP only): a separate thread polls the latest ingested timestamp over a
        // QWP query client. Same hosts/auth as the senders; it fails over automatically.
        final Thread probe = (protocol.equals("qwp") && probeIntervalMs > 0)
                ? startProbe(cfg, probeIntervalMs)
                : null;

        for (int id = 0; id < numSenders; id++) {
            final long eventsForThis = base + (id < rem ? 1 : 0);
            final int senderId = id;
            futures.add(exec.submit(
                    () -> runWorker(senderId, eventsForThis, delayMs, timestampFromFile, secondsOffset, timeIndex,
                            header, rows, cfg)));
        }

        // Wait for completion
        exec.shutdown();
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (ExecutionException ee) {
                System.err.println("Worker failed: " + ee.getCause());
                System.exit(1);
            }
        }
        reporter.interrupt();
        if (probe != null) {
            probe.interrupt();
        }
        final double elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        final double rowsPerSec = elapsedSec > 0 ? totalEvents / elapsedSec : 0;
        System.out.printf("All workers completed. protocol=%s events=%d elapsed=%.3f s throughput=%,.0f rows/s%n",
                protocol, totalEvents, elapsedSec, rowsPerSec);
        System.exit(0);
    }

    private static void runWorker(
            int senderId,
            long totalEvents,
            int delayMs,
            boolean timestampFromFile,
            long secondsOffset,
            int timeIndex,
            String[] header,
            List<String[]> rows,
            SenderCfg cfg) {
        System.out.printf("Sender %d will send %d events%n", senderId, totalEvents);
        long sent = 0;
        final int n = rows.size();
        final boolean isQwp = cfg.protocol.equals("qwp");
        // QWP with a single worker: stamp each row with the current time client-side. A single
        // thread's timestamps are monotonic (no O3) and this avoids QWP's per-batch atNow()
        // stamping. ILP, or QWP with more than one worker, use atNow() (server-side, O3-safe).
        final boolean perRowMicros = isQwp && cfg.numSenders == 1;
        // QWP transactional commit cadence: commit every batchSize * batchesPerTransaction
        // rows via an explicit flush(). 0 disables periodic commits (ILP path is unchanged).
        final long commitEveryRows = isQwp
                ? (long) cfg.batchSize * cfg.batchesPerTransaction
                : 0L;

        try (Sender sender = buildSender(cfg, senderId)) {
            for (long i = 0; i < totalEvents; i++) {
                String[] csvRow = rows.get((int) (i % n));

                // Start a new row in cisco_baseline
                sender.table("cisco_baseline");

                // FIRST PASS: write all SYMBOL columns (ILP requires symbols before fields)
                for (int col = 0; col < header.length; col++) {
                    String colName = header[col];

                    // Skip timestamp column entirely as a field
                    if (TIMESTAMP_COLUMN.equals(colName)) {
                        continue;
                    }

                    if (!SYMBOL_COLUMNS.contains(colName)) {
                        continue;
                    }

                    String raw = csvRow[col];
                    if (isEmpty(raw)) {
                        continue;
                    }
                    raw = raw.trim();
                    sender.symbol(colName, raw);
                }

                // SECOND PASS: write all non SYMBOL columns (booleans + numerics)
                for (int col = 0; col < header.length; col++) {
                    String colName = header[col];

                    // Skip timestamp column and SYMBOLs, already handled
                    if (TIMESTAMP_COLUMN.equals(colName) || SYMBOL_COLUMNS.contains(colName)) {
                        continue;
                    }

                    String raw = csvRow[col];
                    if (isEmpty(raw)) {
                        continue;
                    }
                    raw = raw.trim();

                    if (BOOLEAN_COLUMNS.contains(colName)) {
                        boolean b = Boolean.parseBoolean(raw);
                        sender.boolColumn(colName, b);
                    } else {
                        // Numeric columns: treat as LONG, but accept scientific notation
                        long value = parseLongFlexible(raw);
                        sender.longColumn(colName, value);
                    }
                }

                // Set timestamp
                if (timestampFromFile && timeIndex >= 0) {
                    String t = csvRow[timeIndex];
                    if (!isEmpty(t)) {
                        Instant ts = Instant.parse(t.trim());
                        if (secondsOffset != 0) {
                            ts = ts.plusSeconds(secondsOffset);
                        }
                        sender.at(ts);
                    } else {
                        sender.atNow();
                    }
                } else if (secondsOffset != 0) {
                    sender.at(Instant.now().plusSeconds(secondsOffset));
                } else if (perRowMicros) {
                    // QWP single worker: per-row microsecond timestamp (see runWorker header).
                    sender.at(Instant.now());
                } else {
                    // Default: server timestamp
                    sender.atNow();
                }

                sent++;
                TOTAL_SENT.incrementAndGet();

                // QWP-only: commit a transaction every batchSize * batchesPerTransaction rows.
                if (commitEveryRows > 0 && sent % commitEveryRows == 0) {
                    sender.flush();
                }

                if (delayMs > 0) {
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted", ie);
                    }
                }
            }

            sender.flush();
            System.out.printf("Sender %d finished sending %d events%n", senderId, sent);
        } catch (Exception e) {
            System.err.printf("Sender %d got error: %s%n", senderId, e.toString());
            throw new RuntimeException(e);
        }
    }

    private static CiscoCsvData loadCsv(String path) throws Exception {
        List<String[]> rows = new ArrayList<>(1024);
        String[] header;

        try (InputStream in0 = Files.newInputStream(Path.of(path));
                InputStream in = path.endsWith(".gz") ? new GZIPInputStream(in0) : in0;
                InputStreamReader isr = new InputStreamReader(in, StandardCharsets.UTF_8);
                BufferedReader br = new BufferedReader(isr);
                CSVReader reader = new CSVReader(br)) {

            header = reader.readNext();
            if (header == null) {
                return new CiscoCsvData(new String[0], rows);
            }

            // Header cells are already unquoted by CSVReader; trim them
            for (int i = 0; i < header.length; i++) {
                if (header[i] != null) {
                    header[i] = header[i].trim();
                }
            }

            String[] row;
            while ((row = reader.readNext()) != null) {
                if (row.length == 0) {
                    continue;
                }
                rows.add(row);
            }
        }

        return new CiscoCsvData(header, rows);
    }

    private static boolean isEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

    /**
     * Parse a numeric string that may be integer or scientific notation.
     * Examples:
     * "12345" -> 12345
     * "3.92297799168e+11" -> 392297799168L (truncated)
     */
    private static long parseLongFlexible(String raw) {
        String v = raw.trim();
        if (v.isEmpty()) {
            throw new IllegalArgumentException("Cannot parse empty numeric string as long");
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            double d = Double.parseDouble(v);
            return (long) d;
        }
    }

    private static LineSenderBuilder buildBuilder(
            String addrsCsv,
            String token,
            String username,
            String password,
            int retryTimeout) {
        String[] addrs = Arrays.stream(addrsCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);

        boolean hasToken = token != null && !token.isEmpty();
        boolean hasBasic = username != null && !username.isEmpty() && password != null && !password.isEmpty();

        LineSenderBuilder sb = Sender.builder(Sender.Transport.HTTP);

        if (hasToken || hasBasic) {
            sb = sb.enableTls().advancedTls().disableCertificateValidation();
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
            switch (event.getKind()) {
                case CONNECTED:
                    msg = "connected to " + host;
                    break;
                case RECONNECTED:
                    msg = "reconnected to " + host;
                    break;
                case FAILED_OVER:
                    msg = "failed over " + event.getPreviousHost() + ":" + event.getPreviousPort() + " -> " + host;
                    break;
                case AUTH_FAILED:
                    msg = "auth failed for " + host;
                    break;
                case RECONNECT_BUDGET_EXHAUSTED:
                    msg = "reconnect budget exhausted, giving up";
                    break;
                case DISCONNECTED:
                    msg = "connection lost to " + host + " (" + cause + "), will retry";
                    noisy = true;
                    break;
                case ENDPOINT_ATTEMPT_FAILED:
                    msg = "endpoint " + host + " failed (" + cause + "), trying next";
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
            System.out.printf("[ingestion client %s] %s%n", who, msg);
        };

        // Enterprise-only: hold spilled frames until a durable (committed) ack. OSS servers
        // reject this during the WebSocket upgrade, so it is gated behind --enterprise.
        if (cfg.enterprise) {
            b.requestDurableAck(true);
        }

        return b.storeAndForwardDir(sfPath)
                .senderId(who)
                .transactional(true)
                .connectionListener(connListener)
                .reconnectMaxDurationMillis(300_000)
                .reconnectInitialBackoffMillis(100)
                .reconnectMaxBackoffMillis(5_000)
                .autoFlushBytes(524_288)              // 512 KiB, under the ~1MB WS frame cap
                .autoFlushRows(cfg.batchSize)         // one batch = one deferred append
                .autoFlushIntervalMillis(1_000)
                .build();
    }

    // Config string for the QWP query client: same hosts and token/auth as the senders,
    // ws/wss chosen the same way (TLS on when token/basic auth is present), failover on
    // for more than one host, and the preferred zone when set.
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
    private static Thread startProbe(SenderCfg cfg, long intervalMs) {
        final Thread t = new Thread(() -> {
            try (QwpQueryClient client = QwpQueryClient.fromConfig(queryClientConfig(cfg))) {
                client.connect();
                final QwpServerInfo info = client.getServerInfo();
                if (info != null) {
                    System.out.printf("[query client] connected, serving node=%s role=%s zone=%s cluster=%s%n",
                            orNone(info.getNodeId()), QwpServerInfo.roleName(info.getRole()),
                            orNone(info.getZoneId()), orNone(info.getClusterId()));
                } else {
                    System.out.println("[query client] connected");
                }
                final long[] latest = {Long.MIN_VALUE};
                final boolean[] wasDown = {false};
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
                        System.out.printf("[query client] server error: %s%n", message);
                    }

                    @Override
                    public void onFailoverReset(QwpServerInfo info) {
                        System.out.printf("[query client] failed over -> now serving node=%s role=%s zone=%s%n",
                                orNone(info.getNodeId()), QwpServerInfo.roleName(info.getRole()), orNone(info.getZoneId()));
                    }
                };
                while (!Thread.currentThread().isInterrupted()) {
                    latest[0] = Long.MIN_VALUE;
                    try {
                        client.execute(PROBE_QUERY, handler);
                        if (wasDown[0]) {
                            System.out.println("[query client] connection restored");
                            wasDown[0] = false;
                        }
                        if (latest[0] != Long.MIN_VALUE) {
                            // cisco_baseline designated timestamp is microseconds by default.
                            Instant ts = Instant.EPOCH.plus(latest[0], ChronoUnit.MICROS);
                            System.out.printf("[probe] latest cisco_baseline timestamp = %s (raw=%d)%n", ts, latest[0]);
                        }
                    } catch (Exception e) {
                        if (!wasDown[0]) {
                            System.out.printf("[query client] connection lost (%s), will retry%n",
                                    String.valueOf(e.getMessage()));
                            wasDown[0] = true;
                        }
                    }
                    Thread.sleep(intervalMs);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("[probe] stopped: " + e.getMessage());
            }
        }, "qwp-probe");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static String orNone(String s) {
        return (s == null || s.isEmpty()) ? "(none)" : s;
    }

    private static String buildConf(
            String addrsCsv,
            String token,
            String username,
            String password,
            int retryTimeout) {
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

        if (!"http".equals(protocol)) {
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

    private static final class CiscoCsvData {
        final String[] header;
        final List<String[]> rows;

        CiscoCsvData(String[] header, List<String[]> rows) {
            this.header = header;
            this.rows = rows;
        }
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

        SenderCfg(String protocol, String addrsCsv, String token, String username, String password,
                  int retryTimeout, String senderIdBase, String storeForwardDir,
                  int batchSize, int batchesPerTransaction, int numSenders, boolean enterprise, String zone) {
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
        }
    }
}
