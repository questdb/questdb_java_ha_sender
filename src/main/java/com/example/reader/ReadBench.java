package com.example.reader;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Java port of python/read_bench.py: read the last N rows of a table as fast as the
 * QWP query client allows, reporting rows/sec and throughput live.
 * <p>
 * Records are streamed straight off the QWP column batches: the loop only counts rows
 * and payload bytes, nothing is materialised. At the end it prints the first and last
 * {@code --sample} rows of the range, rendered from the batches already received (no
 * extra query).
 * <p>
 * Throughput is the uncompressed QWP batch payload ({@code payloadLimit - payloadAddr}),
 * not wire bytes and not the Python script's decoded Arrow size, so MB/s figures are not
 * directly comparable between the two.
 * <p>
 * Failover: {@code --addr} takes a comma-separated host list
 * ({@code h1:9000,h2:9000}). The client connects to the first reachable node and fails
 * over to the next one if a connection drops. A failover mid-query discards the partial
 * result and re-runs the query on the new node, so the chunk's counts and captured rows
 * are rolled back and re-read. A chunk whose query fails outright is also rolled back and
 * retried, up to {@code --chunk-retries} times.
 * <p>
 * Splitting ({@code --split rows|time}, {@code --chunks}) follows read_bench.py exactly;
 * see its docstring for the trade-offs.
 */
public class ReadBench {

    private static final DateTimeFormatter ISO_NANOS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'").withZone(ZoneOffset.UTC);

    // ---------------------------------------------------------------- args

    static final class Args {
        String table;
        // All three cluster nodes, matching CsvParallelSender.DEFAULT_ADDRS: a bare run
        // then exercises failover. VPC-internal addresses, not reachable from outside.
        String addr = "172.31.42.41:9000,172.31.41.35:9000,10.0.0.8:9000";
        long limit = 10_000_000L;
        int readers = 1;
        String split = "rows";
        int chunks = 0;
        String timestampCol = "timestamp";
        double reportInterval = 0.5;
        int sample = 5;
        String token;
        String username;
        String password;
        boolean tls;
        String tlsVerify = "on";
        String target;
        String zone;
        String conf;
        int chunkRetries = 3;

        boolean useTls() {
            return tls || token != null || (username != null && password != null);
        }
    }

    private static void usage() {
        System.err.println(
                "usage: ReadBench TABLE [--addr host:port[,host:port...]] [--limit N] [--readers N]\n"
                        + "                 [--split rows|time] [--chunks N] [--timestamp-col COL]\n"
                        + "                 [--report-interval SEC] [--sample N]\n"
                        + "                 [--token TOK | --token-file PATH | --username U --password P] [--tls]\n"
                        + "                 [--tls-verify on|unsafe_off] [--target any|primary|replica]\n"
                        + "                 [--zone Z] [--chunk-retries N] [--conf CONNECT_STRING]\n"
                        + "\n"
                        + "  --addr accepts a comma-separated list for failover, e.g. h1:9000,h2:9000\n"
                        + "  --conf replaces the generated connect string entirely (addr/auth/tls flags ignored)");
    }

    private static Args parse(String[] argv) {
        Args a = new Args();
        for (int i = 0; i < argv.length; i++) {
            String k = argv[i];
            if (k.equals("-h") || k.equals("--help")) {
                usage();
                System.exit(0);
            }
            if (!k.startsWith("--")) {
                if (a.table != null) {
                    throw new IllegalArgumentException("unexpected argument: " + k);
                }
                a.table = k;
                continue;
            }
            if (k.equals("--tls")) {
                a.tls = true;
                continue;
            }
            if (i + 1 >= argv.length) {
                throw new IllegalArgumentException("missing value for " + k);
            }
            String v = argv[++i];
            switch (k) {
                case "--addr": a.addr = v; break;
                case "--limit": a.limit = Long.parseLong(v.replace("_", "")); break;
                case "--readers": a.readers = Integer.parseInt(v); break;
                case "--split": a.split = v; break;
                case "--chunks": a.chunks = Integer.parseInt(v); break;
                case "--timestamp-col": a.timestampCol = v; break;
                case "--report-interval": a.reportInterval = Double.parseDouble(v); break;
                case "--sample": a.sample = Integer.parseInt(v); break;
                case "--token": a.token = v.isEmpty() ? null : v; break;
                case "--token-file": a.token = readTokenFile(v); break;
                case "--username": a.username = v; break;
                case "--password": a.password = v; break;
                case "--tls-verify": a.tlsVerify = v; break;
                case "--target": a.target = v; break;
                case "--zone": a.zone = v; break;
                case "--conf": a.conf = v; break;
                case "--chunk-retries": a.chunkRetries = Integer.parseInt(v); break;
                default: throw new IllegalArgumentException("unknown option: " + k);
            }
        }
        if (a.table == null) {
            throw new IllegalArgumentException("TABLE is required");
        }
        if (a.readers < 1) {
            throw new IllegalArgumentException("--readers must be >= 1");
        }
        if (!a.split.equals("rows") && !a.split.equals("time")) {
            throw new IllegalArgumentException("--split must be rows or time");
        }
        if (!a.tlsVerify.equals("on") && !a.tlsVerify.equals("unsafe_off")) {
            throw new IllegalArgumentException("--tls-verify must be on or unsafe_off");
        }
        return a;
    }

    private static String readTokenFile(String path) {
        try {
            String p = path.startsWith("~/") ? System.getProperty("user.home") + path.substring(1) : path;
            return new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(p)),
                    java.nio.charset.StandardCharsets.UTF_8).trim();
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("cannot read --token-file " + path + ": " + e.getMessage());
        }
    }

    /** QWP connect string for the reader(s), with optional auth, TLS and failover routing. */
    static String buildConf(Args a) {
        if (a.conf != null) {
            return a.conf;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(a.useTls() ? "wss" : "ws").append("::addr=").append(a.addr).append(';');
        if (a.token != null) {
            sb.append("token=").append(a.token).append(';');
        } else if (a.username != null && a.password != null) {
            sb.append("username=").append(a.username).append(";password=").append(a.password).append(';');
        }
        if (a.useTls() && a.tlsVerify.equals("unsafe_off")) {
            sb.append("tls_verify=unsafe_off;");
        }
        if (a.target != null) {
            sb.append("target=").append(a.target).append(';');
        }
        if (a.zone != null) {
            sb.append("zone=").append(a.zone).append(';');
        }
        return sb.toString();
    }

    /** Connect string with secrets masked, safe to print. */
    static String redact(String conf) {
        return conf.replaceAll("(token|password)=[^;]*", "$1=***");
    }

    // ---------------------------------------------------------------- work split

    static final class Chunk {
        final int index;   // position in the range, 0 = oldest
        final String sql;

        Chunk(int index, String sql) {
            this.index = index;
            this.sql = sql;
        }
    }

    /**
     * {@code --chunks} SELECTs covering the last {@code --limit} rows as equal row-count
     * slices, oldest first, using {@code LIMIT -m, -n} (the half-open range [-m, -n)).
     */
    static List<String> rowChunks(Args a) {
        List<String> sqls = new ArrayList<>();
        for (int i = 0; i < a.chunks; i++) {
            long lo = a.limit - (a.limit * i) / a.chunks;         // rows from the end, inclusive
            long hi = a.limit - (a.limit * (i + 1)) / a.chunks;   // rows from the end, exclusive
            if (lo <= hi) {
                continue;
            }
            if (hi == 0) {
                sqls.add("select * from " + a.table + " limit -" + lo);
            } else {
                sqls.add("select * from " + a.table + " limit -" + lo + ", -" + hi);
            }
        }
        return sqls;
    }

    /**
     * One SELECT per reader, splitting the last-N rows' timestamp range into equal time
     * spans. Costs one preliminary query.
     */
    static List<String> timeSlices(Args a, QuestDB db) throws InterruptedException {
        String all = "select * from " + a.table + " limit -" + a.limit;
        if (a.readers <= 1) {
            return Collections.singletonList(all);
        }
        String ts = a.timestampCol;
        final long[] bounds = new long[2];
        final boolean[] found = {false};
        final long[] nanosPerUnit = {1_000L};
        try (Query q = db.borrowQuery()) {
            q.sql("select min(" + ts + ") lo, max(" + ts + ") hi from (select " + ts
                            + " from " + a.table + " limit -" + a.limit + ")")
                    .handler(new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            if (batch.getRowCount() == 0 || batch.isNull(0, 0) || batch.isNull(1, 0)) {
                                return;
                            }
                            byte t = batch.getColumnWireType(0);
                            nanosPerUnit[0] = t == QwpConstants.TYPE_TIMESTAMP_NANOS ? 1L
                                    : t == QwpConstants.TYPE_DATE ? 1_000_000L : 1_000L;
                            bounds[0] = batch.getLongValue(0, 0);
                            bounds[1] = batch.getLongValue(1, 0);
                            found[0] = true;
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                        }
                    }).submit().await();
        }
        if (!found[0]) {
            return Collections.singletonList(all);
        }
        long lo = bounds[0] * nanosPerUnit[0];
        long hi = bounds[1] * nanosPerUnit[0];
        if (hi <= lo) {
            System.err.println("[warn] timestamp range too narrow to split; using 1 reader");
            return Collections.singletonList(all);
        }
        long span = hi - lo;
        List<String> sqls = new ArrayList<>();
        for (int i = 0; i < a.readers; i++) {
            long from = lo + (long) ((double) span * i / a.readers);
            long to = i == a.readers - 1 ? hi : lo + (long) ((double) span * (i + 1) / a.readers);
            String op = i == a.readers - 1 ? "<=" : "<";   // last slice includes the max row
            sqls.add("select * from " + a.table + " where " + ts + " >= '" + nsToIso(from)
                    + "' and " + ts + " " + op + " '" + nsToIso(to) + "'");
        }
        return sqls;
    }

    static String nsToIso(long ns) {
        return ISO_NANOS.format(Instant.ofEpochSecond(Math.floorDiv(ns, 1_000_000_000L),
                Math.floorMod(ns, 1_000_000_000L)));
    }

    // ---------------------------------------------------------------- row capture

    /** First and last {@code n} rows of one chunk, rendered as strings. */
    static final class Capture {
        final List<String[]> head = new ArrayList<>();
        final ArrayDeque<String[]> tail = new ArrayDeque<>();

        void clear() {
            head.clear();
            tail.clear();
        }
    }

    static volatile String[] columnNames;

    static String render(QwpColumnBatch b, int col, int row) {
        if (b.isNull(col, row)) {
            return "null";
        }
        byte type = b.getColumnWireType(col);
        switch (type) {
            case QwpConstants.TYPE_BOOLEAN: return String.valueOf(b.getBoolValue(col, row));
            case QwpConstants.TYPE_BYTE: return String.valueOf(b.getByteValue(col, row));
            case QwpConstants.TYPE_SHORT: return String.valueOf(b.getShortValue(col, row));
            case QwpConstants.TYPE_CHAR: return String.valueOf(b.getCharValue(col, row));
            case QwpConstants.TYPE_INT: return String.valueOf(b.getIntValue(col, row));
            case QwpConstants.TYPE_LONG: return String.valueOf(b.getLongValue(col, row));
            case QwpConstants.TYPE_FLOAT: return String.valueOf(b.getFloatValue(col, row));
            case QwpConstants.TYPE_DOUBLE: return String.valueOf(b.getDoubleValue(col, row));
            case QwpConstants.TYPE_DATE: return nsToIso(b.getLongValue(col, row) * 1_000_000L);
            case QwpConstants.TYPE_TIMESTAMP: return nsToIso(b.getLongValue(col, row) * 1_000L);
            case QwpConstants.TYPE_TIMESTAMP_NANOS: return nsToIso(b.getLongValue(col, row));
            case QwpConstants.TYPE_SYMBOL: return b.getSymbol(col, row);
            case QwpConstants.TYPE_VARCHAR: return b.getString(col, row);
            case QwpConstants.TYPE_UUID:
                return new UUID(b.getUuidHi(col, row), b.getUuidLo(col, row)).toString();
            case QwpConstants.TYPE_IPv4: {
                int v = b.getIntValue(col, row);
                return ((v >>> 24) & 0xFF) + "." + ((v >>> 16) & 0xFF) + "." + ((v >>> 8) & 0xFF) + "." + (v & 0xFF);
            }
            case QwpConstants.TYPE_LONG256: {
                StringBuilder sb = new StringBuilder("0x");
                for (int w = 3; w >= 0; w--) {
                    sb.append(String.format("%016x", b.getLong256Word(col, row, w)));
                }
                return sb.toString();
            }
            case QwpConstants.TYPE_DOUBLE_ARRAY: return Arrays.toString(b.getDoubleArrayElements(col, row));
            default: return "<" + QwpConstants.getTypeName(type) + ">";
        }
    }

    static String[] renderRow(QwpColumnBatch b, int row) {
        int n = b.getColumnCount();
        String[] out = new String[n];
        for (int c = 0; c < n; c++) {
            out[c] = render(b, c, row);
        }
        return out;
    }

    // ---------------------------------------------------------------- reader

    /**
     * Result handler for one reader. It runs on the pool's query I/O thread while the
     * reader thread blocks in {@code await()}; the await provides the happens-before
     * edge for the per-chunk fields the reader thread resets between chunks.
     */
    static final class ChunkHandler implements QwpColumnBatchHandler {
        final int reader;
        final AtomicLongArray rows;
        final AtomicLongArray bytes;
        final int sample;
        Capture capture;
        long chunkRows;
        long chunkBytes;
        final List<String> failovers;

        ChunkHandler(int reader, AtomicLongArray rows, AtomicLongArray bytes, int sample, List<String> failovers) {
            this.reader = reader;
            this.rows = rows;
            this.bytes = bytes;
            this.sample = sample;
            this.failovers = failovers;
        }

        void start(Capture c) {
            capture = c;
            chunkRows = 0;
            chunkBytes = 0;
        }

        /** Undo this chunk's contribution, before a re-run. */
        void rollback() {
            rows.addAndGet(reader, -chunkRows);
            bytes.addAndGet(reader, -chunkBytes);
            chunkRows = 0;
            chunkBytes = 0;
            if (capture != null) {
                capture.clear();
            }
        }

        @Override
        public void onBatch(QwpColumnBatch batch) {
            int n = batch.getRowCount();
            long b = batch.payloadLimit() - batch.payloadAddr();
            chunkRows += n;
            chunkBytes += b;
            rows.addAndGet(reader, n);
            bytes.addAndGet(reader, b);
            if (sample > 0 && n > 0) {
                if (columnNames == null) {
                    String[] names = new String[batch.getColumnCount()];
                    for (int c = 0; c < names.length; c++) {
                        names[c] = batch.getColumnName(c);
                    }
                    columnNames = names;
                }
                // Only the rows that can end up in the sample are rendered: the head
                // until it is full, and the last `sample` rows of each batch for the tail.
                for (int r = 0; r < n && capture.head.size() < sample; r++) {
                    capture.head.add(renderRow(batch, r));
                }
                for (int r = Math.max(0, n - sample); r < n; r++) {
                    capture.tail.addLast(renderRow(batch, r));
                    if (capture.tail.size() > sample) {
                        capture.tail.removeFirst();
                    }
                }
            }
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
        }

        @Override
        public void onFailoverReset(QwpServerInfo newNode) {
            // Partial result discarded; the client re-runs the query on the new node.
            rollback();
            failovers.add("reader " + reader + " failed over to node=" + newNode.getNodeId()
                    + " zone=" + newNode.getZoneId());
        }
    }

    // ---------------------------------------------------------------- main

    public static void main(String[] argv) throws Exception {
        Args a;
        try {
            a = parse(argv);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            usage();
            System.exit(2);
            return;
        }
        System.exit(run(a));
    }

    static int run(Args a) throws Exception {
        String conf = buildConf(a);
        System.out.println("[conf]   " + redact(conf));

        try (QuestDB db = QuestDB.builder()
                .fromConfig(conf)
                .queryPoolSize(a.readers)
                .senderPoolMin(0)   // read-only tool: never open an ingest connection
                .build()) {

            List<String> sqls;
            int readers;
            if (a.split.equals("rows")) {
                a.chunks = Math.max(a.chunks > 0 ? a.chunks : a.readers * 8, a.readers);
                sqls = rowChunks(a);
                readers = Math.min(a.readers, sqls.size());
            } else {
                sqls = timeSlices(a, db);
                readers = sqls.size();
            }

            ConcurrentLinkedQueue<Chunk> work = new ConcurrentLinkedQueue<>();
            Capture[] captures = new Capture[sqls.size()];
            for (int i = 0; i < sqls.size(); i++) {
                work.add(new Chunk(i, sqls.get(i)));
                captures[i] = new Capture();
            }

            System.out.printf("[scan]   reading last %,d rows of '%s' across %d reader(s), %d %s chunk(s) ...%n",
                    a.limit, a.table, readers, sqls.size(), a.split);

            AtomicLongArray rows = new AtomicLongArray(readers);
            AtomicLongArray bytes = new AtomicLongArray(readers);
            int[] chunksDone = new int[readers];
            List<String> errors = new CopyOnWriteArrayList<>();
            List<String> failovers = new CopyOnWriteArrayList<>();

            Thread reporter = new Thread(() -> {
                long lastR = 0, lastB = 0;
                long sleepMs = (long) (a.reportInterval * 1000);
                try {
                    while (true) {
                        Thread.sleep(sleepMs);
                        long r = sum(rows), b = sum(bytes);
                        System.out.printf("[scan]   %,14d rows | %,13.0f rows/s | %,9.1f MB/s%n",
                                r, (r - lastR) / a.reportInterval, (b - lastB) / a.reportInterval / 1e6);
                        lastR = r;
                        lastB = b;
                    }
                } catch (InterruptedException ignored) {
                    // stop
                }
            }, "reporter");
            reporter.setDaemon(true);

            long t0 = System.nanoTime();
            reporter.start();
            Thread[] threads = new Thread[readers];
            for (int i = 0; i < readers; i++) {
                final int idx = i;
                threads[i] = new Thread(() -> runReader(idx, db, work, captures, rows, bytes,
                        chunksDone, errors, failovers, a), "reader-" + i);
                threads[i].start();
            }
            for (Thread t : threads) {
                t.join();
            }
            double elapsed = (System.nanoTime() - t0) / 1e9;
            reporter.interrupt();
            reporter.join();

            for (String f : failovers) {
                System.out.println("[failover] " + f);
            }
            if (!errors.isEmpty()) {
                for (String e : errors) {
                    System.err.println("reader failed: " + e);
                }
                return 1;
            }

            long total = sum(rows);
            long tbytes = sum(bytes);
            if (total == 0) {
                System.err.println("[done]   '" + a.table + "' returned 0 rows");
                return 1;
            }
            System.out.printf("[done]   %,d rows, %.2f GiB payload in %.3fs across %d reader(s)%n",
                    total, tbytes / (double) (1L << 30), elapsed, readers);
            System.out.printf("[done]   %,.0f rows/s | %,.1f MB/s | %.2f Gb/s (uncompressed QWP payload, not wire bytes)%n",
                    total / elapsed, tbytes / elapsed / 1e6, tbytes * 8 / elapsed / 1e9);
            if (readers > 1) {
                StringBuilder per = new StringBuilder();
                for (int i = 0; i < readers; i++) {
                    per.append(String.format("r%d=%,d(%dch)  ", i, rows.get(i), chunksDone[i]));
                }
                System.out.println("[done]   per-reader rows: " + per.toString().trim());
            }

            if (a.sample > 0 && columnNames != null) {
                printSample(a.sample, captures);
            }
            return 0;
        }
    }

    static void runReader(int idx, QuestDB db, ConcurrentLinkedQueue<Chunk> work, Capture[] captures,
                          AtomicLongArray rows, AtomicLongArray bytes, int[] chunksDone,
                          List<String> errors, List<String> failovers, Args a) {
        ChunkHandler handler = new ChunkHandler(idx, rows, bytes, a.sample, failovers);
        // One borrowed query handle (one connection) per reader, reused for every chunk.
        try (Query q = db.borrowQuery()) {
            Chunk chunk;
            while ((chunk = work.poll()) != null) {
                for (int attempt = 0; ; attempt++) {
                    handler.start(captures[chunk.index]);
                    try {
                        q.sql(chunk.sql).handler(handler).submit().await();
                        break;
                    } catch (QueryException e) {
                        handler.rollback();
                        if (attempt >= a.chunkRetries) {
                            throw e;
                        }
                        failovers.add(String.format("reader %d retrying chunk %d (attempt %d): status=0x%02X %s",
                                idx, chunk.index, attempt + 1, e.getStatus() & 0xFF, e.getMessage()));
                    }
                }
                chunksDone[idx]++;
            }
        } catch (Exception e) {
            errors.add("reader " + idx + ": " + e);
        }
    }

    static void printSample(int n, Capture[] captures) {
        List<String[]> first = new ArrayList<>();
        for (int i = 0; i < captures.length && first.size() < n; i++) {
            for (String[] r : captures[i].head) {
                if (first.size() >= n) {
                    break;
                }
                first.add(r);
            }
        }
        List<String[]> last = new ArrayList<>();
        for (int i = captures.length - 1; i >= 0 && last.size() < n; i--) {
            List<String[]> tail = new ArrayList<>(captures[i].tail);
            for (int j = tail.size() - 1; j >= 0 && last.size() < n; j--) {
                last.add(tail.get(j));
            }
        }
        Collections.reverse(last);

        System.out.println();
        System.out.println("[sample] first " + first.size() + " row(s) of the range:");
        printTable(columnNames, first);
        System.out.println();
        System.out.println("[sample] last " + last.size() + " row(s) of the range:");
        printTable(columnNames, last);
    }

    static void printTable(String[] header, List<String[]> body) {
        int[] w = new int[header.length];
        for (int c = 0; c < header.length; c++) {
            w[c] = header[c].length();
            for (String[] r : body) {
                w[c] = Math.max(w[c], r[c].length());
            }
        }
        StringBuilder sep = new StringBuilder("+");
        for (int c : w) {
            sep.append("-".repeat(c + 2)).append('+');
        }
        System.out.println(sep);
        System.out.println(line(header, w));
        System.out.println(sep);
        for (String[] r : body) {
            System.out.println(line(r, w));
        }
        System.out.println(sep);
    }

    private static String line(String[] cells, int[] w) {
        StringBuilder sb = new StringBuilder("|");
        for (int c = 0; c < cells.length; c++) {
            sb.append(' ').append(cells[c]).append(" ".repeat(w[c] - cells[c].length())).append(" |");
        }
        return sb.toString();
    }

    private static long sum(AtomicLongArray arr) {
        long s = 0;
        for (int i = 0; i < arr.length(); i++) {
            s += arr.get(i);
        }
        return s;
    }
}
