package com.example.sender;

import io.questdb.client.Sender;
import io.questdb.client.Sender.LineSenderBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

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
        System.out.println("Ingestion started. Connecting with config: " +
                conf.replaceAll("(token=)([^;]+)", "$1***")
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

        for (int id = 0; id < numSenders; id++) {
            final long eventsForThis = base + (id < rem ? 1 : 0);
            final int senderId = id;
            futures.add(exec.submit(
                    () -> runWorker(senderId, eventsForThis, delayMs, timestampFromFile, timeIndex, header, rows,
                            builder)));
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
            int timeIndex,
            String[] header,
            List<String[]> rows,
            LineSenderBuilder builder) {
        System.out.printf("Sender %d will send %d events%n", senderId, totalEvents);
        long sent = 0;
        final int n = rows.size();

        try (Sender sender = builder.build()) {
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
                        sender.at(ts);
                    } else {
                        sender.atNow();
                    }
                } else {
                    // Default: server timestamp
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

    private static final class CiscoCsvData {
        final String[] header;
        final List<String[]> rows;

        CiscoCsvData(String[] header, List<String[]> rows) {
            this.header = header;
            this.rows = rows;
        }
    }
}
