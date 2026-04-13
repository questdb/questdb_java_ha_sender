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

        final String conf = buildConf(addrsCsv, token, username, password, retryTimeout);
        final LineSenderBuilder builder = buildBuilder(addrsCsv, token, username, password, retryTimeout);
        System.out.println("Ingestion started. Connecting with config: " + conf.replaceAll("(token=)([^;]+)", "$1***")
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

        for (int id = 0; id < numSenders; id++) {
            final long eventsForThis = base + (id < rem ? 1 : 0);
            final int senderId = id;
            //futures.add(exec.submit(() -> runWorker(senderId, eventsForThis, delayMs, timestampFromFile, rows, conf)));
            futures.add(exec.submit(() -> runWorker(senderId, eventsForThis, delayMs, timestampFromFile, secondsOffset, rows, builder)));
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
        System.out.println("All workers completed.");
    }

    private static void runWorker(
            int senderId,
            long totalEvents,
            int delayMs,
            boolean timestampFromFile,
            long secondsOffset,
            List<TradeRow> rows,
            LineSenderBuilder builder
    ) {
        System.out.printf("Sender %d will send %d events%n", senderId, totalEvents);
        long sent = 0;
        try ( Sender sender = builder.build()) { //( Sender sender = Sender.fromConfig(conf)) {
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
                } else {
                    sender.atNow();
                }

                sent++;

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
}
