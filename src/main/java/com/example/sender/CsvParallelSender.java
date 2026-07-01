package com.example.sender;

import io.questdb.client.Sender;
import io.questdb.client.Sender.LineSenderBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.opencsv.CSVReader;

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
    private static final int DEFAULT_NUM_SENDERS = 10;
    private static final int DEFAULT_RETRY_TIMEOUT = 360000;
    private static final String DEFAULT_CSV = "./trades20250728.csv.gz";
    private static final boolean DEFAULT_TIMESTAMP_FROM_FILE = false;
    private static final long DEFAULT_SECONDS_OFFSET = 0L;

    // QWP (WebSocket) transport
    private static final String DEFAULT_PROTOCOL = "qwp";
    private static final String DEFAULT_SENDER_ID = "ha_sender";
    private static final String DEFAULT_STORE_FORWARD_DIR = "/tmp/qdb-sf";
    // Batch = one auto-flush append (deferred, no commit under transactional mode).
    // Transaction = batches-per-transaction batches, committed atomically per table
    // by an explicit flush(). See buildSender()/runWorker().
    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final int DEFAULT_BATCHES_PER_TRANSACTION = 10;

    // Rows sent (client-side) across all workers, for the once-per-second progress reporter.
    private static final AtomicLong TOTAL_SENT = new AtomicLong();

    public static void main(String[] args) throws Exception {
        // Parse CLI flags
        Map<String, String> a = parseArgs(args);

        final String addrsCsv = a.getOrDefault("--addrs", DEFAULT_ADDRS);
        final String token = a.get("--token");               // optional
        final String username = a.get("--username");         // optional
        final String password = a.get("--password");         // optional
        final long totalEvents = Long.parseLong(a.getOrDefault("--total-events", String.valueOf(DEFAULT_TOTAL_EVENTS)));
        final int delayMs = Integer.parseInt(a.getOrDefault("--delay-ms", String.valueOf(DEFAULT_DELAY_MS)));
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
                senderIdBase, storeForwardDir, batchSize, batchesPerTransaction, numSenders);

        final String conf = buildConf(addrsCsv, token, username, password, retryTimeout);
        System.out.println("Ingestion started. Protocol: " + protocol
                + (protocol.equals("qwp")
                        ? " (WebSocket, sender-id=" + senderIdBase + ", store-and-forward=" + storeForwardDir
                                + ", batch-size=" + batchSize + ", batches-per-transaction=" + batchesPerTransaction + ")"
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
                System.err.println("Worker failed: " + ee.getCause());
                System.exit(1);
            }
        }
        reporter.interrupt();
        final double elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000.0;
        final double rowsPerSec = elapsedSec > 0 ? totalEvents / elapsedSec : 0;
        System.out.printf("All workers completed. protocol=%s events=%d elapsed=%.3f s throughput=%,.0f rows/s%n",
                protocol, totalEvents, elapsedSec, rowsPerSec);
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
        // QWP with a single worker: stamp each row with the current time client-side. A single
        // thread's timestamps are monotonic (no O3) and this avoids QWP's per-batch atNow()
        // stamping. ILP, or QWP with more than one worker, use atNow() (server-side, O3-safe).
        final boolean perRowMicros = isQwp && cfg.numSenders == 1;
        // QWP transactional commit cadence: commit every batchSize * batchesPerTransaction
        // rows via an explicit flush(). 0 disables periodic commits (ILP path is unchanged).
        final long commitEveryRows = isQwp
                ? (long) cfg.batchSize * cfg.batchesPerTransaction
                : 0L;
        try ( Sender sender = buildSender(cfg, senderId)) { //( Sender sender = Sender.fromConfig(conf)) {
            final int n = rows.size();
            for (long i = 0; i < totalEvents; i++) {
                TradeRow r = rows.get((int) (i % n));

                // Build row
                sender.table("trades")
                        .symbol("symbol", r.symbol)
                        .symbol("side", r.side)
                        .doubleColumn("price", r.price)
                        .doubleColumn("amount", r.amount);

                if (timestampFromFile) {
                    Instant ts = Instant.parse(r.timestamp);
                    if (secondsOffset != 0) {
                        ts = ts.plusSeconds(secondsOffset);
                    }
                    sender.at(ts);
                } else if (secondsOffset != 0) {
                    sender.at(Instant.now().plusSeconds(secondsOffset));
                } else if (perRowMicros) {
                    sender.at(Instant.now());
                } else {
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
            // Explicit flush at the end of this connection's work
            sender.flush();
            System.out.printf("Sender %d finished sending %d events%n", senderId, sent);
        } catch (Exception e) {
            System.err.printf("Sender %d got error: %s%n", senderId, e.toString());
            throw new RuntimeException(e);
        }
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

        return b.storeAndForwardDir(sfPath)
                .senderId(who)
                .transactional(true)
                .reconnectMaxDurationMillis(300_000)
                .reconnectInitialBackoffMillis(100)
                .reconnectMaxBackoffMillis(5_000)
                .autoFlushBytes(524_288)              // 512 KiB, under the ~1MB WS frame cap
                .autoFlushRows(cfg.batchSize)         // one batch = one deferred append
                .autoFlushIntervalMillis(1_000)
                .build();
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

        SenderCfg(String protocol, String addrsCsv, String token, String username, String password,
                  int retryTimeout, String senderIdBase, String storeForwardDir,
                  int batchSize, int batchesPerTransaction, int numSenders) {
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
        }
    }
}
