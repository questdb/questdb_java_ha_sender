// Parallel CSV replay sender for QuestDB, a C++ port of the Java/Rust ha_sender.
//
// Replays a CSV of trades in a loop across N worker threads over one of three transports
// (--protocol):
//   * qwp    - QWP over WebSocket: store-and-forward (un-acked frames spill to disk and
//              replay after an outage), transactional commit, multi-host failover. A probe
//              thread polls the latest ingested timestamp and the serving node's live role
//              over a QWP query client (the egress reader).
//   * qwpudp - QWP over UDP: fire-and-forget datagrams to :9007. Ingest-only,
//              unauthenticated, single-endpoint, best-effort.
//   * ilp    - the legacy ILP/HTTP transport.
//
// Like the Rust client, the C/C++ client has no auto-flush: batching is driven by explicit
// flush() calls at the same boundaries as the Java/Rust ports.

#include <questdb/ingress/line_sender.hpp>
#include <questdb/egress/line_reader.hpp>

#include <zlib.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace qi = questdb::ingress;
namespace eg = questdb::egress;
using namespace questdb::ingress::literals;

namespace {

constexpr std::string_view PROBE_QUERY = "select timestamp from trades limit -1";
// Live lifecycle role of whichever node the query client is connected to (current_role /
// target_role), unlike the QWP handshake which only refreshes on reconnect.
constexpr std::string_view STATUS_QUERY = "switch status";

struct Args {
    std::string addrs = "localhost:9000";
    std::string token, username, password;
    uint64_t total_events = 1'000'000;
    uint64_t delay_ms = 50;
    uint64_t rate = 0;  // target aggregate rows/s across all workers (0 = off, use delay_ms)
    unsigned num_senders = 10;
    uint64_t retry_timeout = 360'000;
    std::string csv = "./trades20250728.csv.gz";
    bool timestamp_from_file = false;
    int64_t seconds_offset = 0;
    std::string protocol = "qwp";
    std::string sender_id = "ha_sender";
    std::string store_forward_dir = "/tmp/qdb-sf";
    uint64_t batch_size = 10'000;
    uint64_t batches_per_transaction = 10;
    uint64_t probe_interval_ms = 1000;
    bool enterprise = false;
    std::string zone = "eu-west-1";
};

struct TradeRow {
    std::string symbol;
    std::string side;
    double price;
    double amount;
    int64_t ts_nanos;  // only used with --timestamp-from-file
};

int64_t now_nanos() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::vector<std::string> split_addrs(const std::string& csv) {
    std::vector<std::string> out;
    std::stringstream ss(csv);
    std::string item;
    while (std::getline(ss, item, ',')) {
        size_t b = item.find_first_not_of(" \t");
        size_t e = item.find_last_not_of(" \t");
        if (b != std::string::npos) out.push_back(item.substr(b, e - b + 1));
    }
    return out;
}

bool has_token(const Args& a) { return !a.token.empty(); }
bool has_basic(const Args& a) { return !a.username.empty() && !a.password.empty(); }
bool use_tls(const Args& a) { return has_token(a) || has_basic(a); }

void append_auth(std::string& c, const Args& a) {
    if (has_token(a))
        c += "token=" + a.token + ";";
    else if (has_basic(a))
        c += "username=" + a.username + ";password=" + a.password + ";";
}

std::string build_ingest_conf(const Args& a, unsigned worker_id) {
    auto addrs = split_addrs(a.addrs);
    std::string joined;
    for (size_t i = 0; i < addrs.size(); ++i) joined += (i ? "," : "") + addrs[i];

    if (a.protocol == "qwpudp") {
        return "qwpudp::addr=" + addrs[0] + ";";
    }
    if (a.protocol == "ilp") {
        std::string c = (use_tls(a) ? "https" : "http");
        c += "::addr=" + joined + ";";
        append_auth(c, a);
        if (use_tls(a)) c += "tls_verify=unsafe_off;";
        c += "retry_timeout=" + std::to_string(a.retry_timeout) + ";";
        return c;
    }
    // qwp (WebSocket). Use the spec aliases ws/wss (qwpws/qwpwss are deprecated); the client
    // maps ws -> QwpWs and wss -> QwpWss, matching the reader conf below.
    std::string who = a.sender_id + "-" + std::to_string(worker_id);
    std::string sf = a.store_forward_dir + "/" + who;
    std::error_code ec;
    std::filesystem::create_directories(sf, ec);
    std::string c = (use_tls(a) ? "wss" : "ws");
    c += "::addr=" + joined + ";";
    append_auth(c, a);
    if (use_tls(a)) c += "tls_verify=unsafe_off;";
    c += "sender_id=" + who + ";";
    c += "sf_dir=" + sf + ";";
    c += "reconnect_max_duration_millis=" + std::to_string(a.retry_timeout) + ";";
    c += "reconnect_initial_backoff_millis=100;";
    c += "reconnect_max_backoff_millis=5000;";
    if (a.enterprise) c += "request_durable_ack=true;";
    return c;
}

std::string build_reader_conf(const Args& a) {
    auto addrs = split_addrs(a.addrs);
    std::string joined;
    for (size_t i = 0; i < addrs.size(); ++i) joined += (i ? "," : "") + addrs[i];
    std::string c = (use_tls(a) ? "wss" : "ws");
    c += "::addr=" + joined + ";target=any;failover=true;";
    append_auth(c, a);
    if (use_tls(a)) c += "tls_verify=unsafe_off;";
    if (!a.zone.empty()) c += "zone=" + a.zone + ";";
    return c;
}

std::string format_micros(int64_t micros) {
    std::time_t secs = static_cast<std::time_t>(micros / 1'000'000);
    int64_t frac = micros % 1'000'000;
    std::tm tm{};
    gmtime_r(&secs, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm);
    char out[64];
    std::snprintf(out, sizeof(out), "%s.%06lldZ", buf, static_cast<long long>(frac));
    return out;
}

const char* role_name(eg::server_role r) {
    switch (r) {
        case eg::server_role::standalone: return "STANDALONE";
        case eg::server_role::primary: return "PRIMARY";
        case eg::server_role::replica: return "REPLICA";
        case eg::server_role::primary_catchup: return "PRIMARY_CATCHUP";
        default: return "UNKNOWN";
    }
}

// Read a string cell from a Symbol or Varchar column; empty for null / non-string kinds.
std::optional<std::string> col_string(const eg::column& col, size_t row) {
    std::optional<std::string> out;
    col.visit(eg::overload{
        [&](eg::varlen_view v) {
            if (auto x = v.as_string_view(row)) out = std::string(*x);
        },
        [&](eg::symbol_view v) {
            if (auto x = v.resolve(row)) out = std::string(*x);
        },
        [](auto) {},
    });
    return out;
}

void load_csv(const std::string& path, bool need_timestamp, std::vector<TradeRow>& out) {
    gzFile f = gzopen(path.c_str(), "rb");  // transparently reads .gz and plain files
    if (!f) throw std::runtime_error("cannot open CSV: " + path);
    char line[4096];
    char* header = gzgets(f, line, sizeof(line));
    if (!header) { gzclose(f); return; }

    // Minimal CSV field split: honours double-quoted fields (quotes are dropped and a
    // comma inside quotes does not split), then trims whitespace on unquoted fields.
    auto fields = [](const char* s) {
        std::vector<std::string> v;
        std::string cur;
        bool in_quotes = false;
        for (const char* p = s; *p && *p != '\n' && *p != '\r'; ++p) {
            char ch = *p;
            if (ch == '"') { in_quotes = !in_quotes; continue; }
            if (ch == ',' && !in_quotes) { v.push_back(cur); cur.clear(); }
            else cur += ch;
        }
        v.push_back(cur);
        for (auto& x : v) {
            size_t b = x.find_first_not_of(" \t");
            size_t e = x.find_last_not_of(" \t");
            x = (b == std::string::npos) ? "" : x.substr(b, e - b + 1);
        }
        return v;
    };

    auto cols = fields(header);
    auto idx = [&](const std::string& name) -> int {
        for (size_t i = 0; i < cols.size(); ++i)
            if (cols[i] == name) return static_cast<int>(i);
        throw std::runtime_error("CSV missing required column: " + name);
    };
    int i_sym = idx("symbol"), i_side = idx("side"), i_price = idx("price"),
        i_amt = idx("amount");
    int i_ts = need_timestamp ? idx("timestamp") : -1;

    while (gzgets(f, line, sizeof(line))) {
        auto r = fields(line);
        if (r.size() <= static_cast<size_t>(i_amt)) continue;
        TradeRow tr;
        tr.symbol = r[i_sym];
        tr.side = r[i_side];
        tr.price = std::stod(r[i_price]);
        tr.amount = std::stod(r[i_amt]);
        tr.ts_nanos = 0;
        if (need_timestamp) {
            // ISO-8601 like 2025-07-28T12:34:56.789012Z
            std::tm tm{};
            const std::string& s = r[i_ts];
            int y, mo, d, h, mi, se;
            long frac = 0;
            if (std::sscanf(s.c_str(), "%d-%d-%dT%d:%d:%d", &y, &mo, &d, &h, &mi, &se) == 6) {
                tm.tm_year = y - 1900; tm.tm_mon = mo - 1; tm.tm_mday = d;
                tm.tm_hour = h; tm.tm_min = mi; tm.tm_sec = se;
                std::time_t secs = timegm(&tm);
                size_t dot = s.find('.');
                if (dot != std::string::npos) {
                    std::string fs = s.substr(dot + 1);
                    while (!fs.empty() && !std::isdigit((unsigned char)fs.back())) fs.pop_back();
                    fs.resize(9, '0');  // pad to nanoseconds
                    frac = std::stol(fs.substr(0, 9));
                }
                tr.ts_nanos = static_cast<int64_t>(secs) * 1'000'000'000 + frac;
            }
        }
        out.push_back(std::move(tr));
    }
    gzclose(f);
}

void run_worker(unsigned worker_id, uint64_t total_events, const Args& args,
                const std::vector<TradeRow>& rows, std::atomic<uint64_t>& total_sent) {
    std::printf("Sender %u will send %llu events\n", worker_id,
                static_cast<unsigned long long>(total_events));
    const bool is_qwp = args.protocol == "qwp";
    const bool is_udp = args.protocol == "qwpudp";
    // Single worker on a QWP transport stamps rows client-side (monotonic, no O3).
    const bool per_row = (is_qwp || is_udp) && args.num_senders == 1;
    const uint64_t commit_every =
        is_qwp ? args.batch_size * args.batches_per_transaction : args.batch_size;

    // Rate limiting (when --rate > 0): pace this worker to its share of the aggregate target,
    // rate / num-senders rows/second. interval_nanos is the ideal spacing between this worker's
    // rows; we sleep only when running ahead of a deadline schedule, so rows go out back-to-back
    // until we get ahead. Reaches high targets a fixed per-row sleep cannot.
    const double interval_nanos =
        args.rate > 0 ? 1'000'000'000.0 * args.num_senders / static_cast<double>(args.rate) : 0.0;
    const bool rate_limited = interval_nanos > 0.0;

    auto sender = qi::line_sender::from_conf(qi::utf8_view{build_ingest_conf(args, worker_id)});
    auto buffer = sender.new_buffer();
    const size_t n = rows.size();
    uint64_t sent = 0;
    const auto pace_start = std::chrono::steady_clock::now();

    for (uint64_t i = 0; i < total_events; ++i) {
        const TradeRow& r = rows[i % n];
        std::string trade_id = std::to_string(worker_id) + "-" + std::to_string(i + 1);
        buffer.table("trades"_tn)
            .symbol("symbol"_cn, qi::utf8_view{r.symbol})
            .symbol("side"_cn, qi::utf8_view{r.side})
            .column("price"_cn, r.price)
            .column("amount"_cn, r.amount)
            .column("trade_id"_cn, qi::utf8_view{trade_id});

        if (args.timestamp_from_file)
            buffer.at(qi::timestamp_nanos{r.ts_nanos + args.seconds_offset * 1'000'000'000});
        else if (args.seconds_offset != 0)
            buffer.at(qi::timestamp_nanos{now_nanos() + args.seconds_offset * 1'000'000'000});
        else if (per_row)
            buffer.at(qi::timestamp_nanos::now());
        else
            buffer.at_now();

        ++sent;
        total_sent.fetch_add(1, std::memory_order_relaxed);
        if (sent % commit_every == 0) sender.flush(buffer);
        if (rate_limited) {
            // Deadline for the row just sent (sent is 1-based). Sleep only while ahead of
            // schedule; the 1ms floor coalesces many rows into one sleep at high rates.
            const uint64_t target_nanos = static_cast<uint64_t>(sent * interval_nanos);
            const uint64_t elapsed_nanos = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - pace_start)
                    .count());
            if (target_nanos > elapsed_nanos) {
                const uint64_t sleep_nanos = target_nanos - elapsed_nanos;
                if (sleep_nanos > 1'000'000)
                    std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_nanos));
            }
        } else if (args.delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(args.delay_ms));
        }
    }
    sender.flush(buffer);
    if (is_qwp) sender.close_drain();  // drain un-acked store-and-forward frames
    std::printf("Sender %u finished sending %llu events\n", worker_id,
                static_cast<unsigned long long>(sent));
}

// Pull current_role / target_role out of `switch status` by column name.
void read_switch_status(eg::reader& reader, std::optional<std::string>& current,
                        std::optional<std::string>& target) {
    current.reset();
    target.reset();
    auto cur = reader.execute(qi::utf8_view{STATUS_QUERY});
    while (auto batch_opt = cur.next_batch()) {
        auto& batch = *batch_opt;
        const size_t cols = batch.column_count();
        for (size_t r = 0; r < batch.row_count(); ++r) {
            for (size_t c = 0; c < cols; ++c) {
                std::string name(batch.column_name(c));
                std::string lower = name;
                for (auto& ch : lower) ch = std::tolower((unsigned char)ch);
                if (lower.find("role") == std::string::npos) continue;
                auto v = col_string(batch.column(c), r);
                if (lower.find("current") != std::string::npos) current = v;
                else if (lower.find("target") != std::string::npos) target = v;
                else if (!current) current = v;
            }
        }
    }
}

void run_probe(const Args& args, std::atomic<bool>& stop) {
    std::unique_ptr<eg::reader> reader;
    try {
        reader = std::make_unique<eg::reader>(qi::utf8_view{build_reader_conf(args)});
    } catch (const std::exception& e) {
        std::printf("[query client] connect failed: %s\n", e.what());
        return;
    }
    // server_info() is a non-owning view whose role is only populated at the handshake; it
    // reads as "other" after queries run. Capture the handshake node/role once, at connect,
    // and use it as the (labelled, possibly-stale) fallback when `switch status` is absent.
    std::string hs_node, hs_role;
    {
        auto si = reader->server_info();
        hs_node = std::string(si.node_id());
        if (hs_node.empty()) hs_node = "(none)";
        hs_role = role_name(si.role());
        std::string cluster(si.cluster_id());
        std::printf("[query client] connected, serving node=%s role=%s cluster=%s\n",
                    hs_node.c_str(), hs_role.c_str(), cluster.empty() ? "(none)" : cluster.c_str());
    }

    bool was_down = false;
    while (!stop.load(std::memory_order_relaxed)) {
        try {
            int64_t latest = 0;
            bool have = false;
            bool is_nanos = false;
            auto cur = reader->execute(qi::utf8_view{PROBE_QUERY});
            while (auto batch_opt = cur.next_batch()) {
                auto& batch = *batch_opt;
                auto col = batch.column(0);
                is_nanos = col.kind() == eg::column_kind::timestamp_nanos;
                for (size_t r = 0; r < batch.row_count(); ++r)
                    if (auto v = col.get<int64_t>(r)) { latest = *v; have = true; }
            }
            if (was_down) { std::printf("[query client] connection restored\n"); was_down = false; }
            if (have) {
                std::optional<std::string> current, target;
                try { read_switch_status(*reader, current, target); } catch (...) {}
                std::string served;
                if (current) {
                    bool switching = target && *target != *current;
                    served = " served by role=" + *current +
                             (switching ? " (switching -> " + *target + ")" : "") +
                             " node=" + hs_node;
                } else {
                    served = " served by role=" + hs_role + " node=" + hs_node +
                             " (handshake role; live 'switch status' unavailable, may be stale)";
                }
                int64_t micros = is_nanos ? latest / 1000 : latest;
                std::printf("[probe] latest trades timestamp = %s (raw=%lld)%s\n",
                            format_micros(micros).c_str(), static_cast<long long>(latest),
                            served.c_str());
            }
        } catch (const std::exception& e) {
            if (!was_down) {
                std::printf("[query client] connection lost (%s), will retry\n", e.what());
                was_down = true;
            }
        }
        for (uint64_t slept = 0; slept < args.probe_interval_ms && !stop.load(); slept += 50)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

std::string need_value(int& i, int argc, char** argv) {
    if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + argv[i]);
    return argv[++i];
}

Args parse_args(int argc, char** argv) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        std::string k = argv[i];
        if (k == "--addrs") a.addrs = need_value(i, argc, argv);
        else if (k == "--token") a.token = need_value(i, argc, argv);
        else if (k == "--username") a.username = need_value(i, argc, argv);
        else if (k == "--password") a.password = need_value(i, argc, argv);
        else if (k == "--total-events") a.total_events = std::stoull(need_value(i, argc, argv));
        else if (k == "--delay-ms") a.delay_ms = std::stoull(need_value(i, argc, argv));
        else if (k == "--rate") a.rate = std::stoull(need_value(i, argc, argv));
        else if (k == "--num-senders") a.num_senders = std::stoul(need_value(i, argc, argv));
        else if (k == "--retry-timeout") a.retry_timeout = std::stoull(need_value(i, argc, argv));
        else if (k == "--csv") a.csv = need_value(i, argc, argv);
        else if (k == "--timestamp-from-file") a.timestamp_from_file = true;
        else if (k == "--seconds-offset") a.seconds_offset = std::stoll(need_value(i, argc, argv));
        else if (k == "--protocol") a.protocol = need_value(i, argc, argv);
        else if (k == "--sender-id") a.sender_id = need_value(i, argc, argv);
        else if (k == "--store-forward-dir") a.store_forward_dir = need_value(i, argc, argv);
        else if (k == "--batch-size") a.batch_size = std::stoull(need_value(i, argc, argv));
        else if (k == "--batches-per-transaction")
            a.batches_per_transaction = std::stoull(need_value(i, argc, argv));
        else if (k == "--probe-interval-ms")
            a.probe_interval_ms = std::stoull(need_value(i, argc, argv));
        else if (k == "--enterprise") a.enterprise = true;
        else if (k == "--zone") a.zone = need_value(i, argc, argv);
        else throw std::runtime_error("unknown argument: " + k);
    }
    return a;
}

}  // namespace

int main(int argc, char** argv) {
    Args args;
    try {
        args = parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "%s\n", e.what());
        return 2;
    }

    if (args.protocol != "qwp" && args.protocol != "qwpudp" && args.protocol != "ilp") {
        std::fprintf(stderr, "--protocol must be 'qwp', 'qwpudp', or 'ilp', got: %s\n",
                     args.protocol.c_str());
        return 2;
    }
    auto addrs = split_addrs(args.addrs);
    if (addrs.empty()) { std::fprintf(stderr, "--addrs must contain a host:port\n"); return 2; }
    if (args.protocol == "qwpudp") {
        if (addrs.size() > 1) {
            std::fprintf(stderr, "--protocol qwpudp supports a single host:port only (UDP is "
                                 "connectionless, no failover); got %zu addresses\n", addrs.size());
            return 2;
        }
        if (has_token(args) || !args.username.empty() || !args.password.empty())
            std::fprintf(stderr, "[warn] --protocol qwpudp is unauthenticated (UDP accepts any "
                                 "connection); ignoring --token/--username/--password\n");
    }

    // --rate takes precedence: when set it drives pacing and --delay-ms is ignored.
    if (args.rate > 0 && args.delay_ms > 0)
        std::fprintf(stderr, "[warn] --rate %llu overrides --delay-ms %llu (rate limiting drives "
                             "pacing; the per-row delay is ignored)\n",
                     (unsigned long long)args.rate, (unsigned long long)args.delay_ms);

    std::vector<TradeRow> rows;
    try {
        load_csv(args.csv, args.timestamp_from_file, rows);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "Failed to read CSV: %s\n", e.what());
        return 2;
    }
    if (rows.empty()) { std::fprintf(stderr, "CSV has no data rows: %s\n", args.csv.c_str()); return 2; }

    if (args.rate > 0)
        std::printf("Pacing: rate=%llu rows/s (aggregate across %u workers)\n",
                    (unsigned long long)args.rate, args.num_senders);
    else
        std::printf("Pacing: delay-ms=%llu\n", (unsigned long long)args.delay_ms);

    if (args.protocol == "qwp")
        std::printf("Ingestion started. Protocol: qwp (WebSocket, sender-id=%s, "
                    "store-and-forward=%s, batch-size=%llu, batches-per-transaction=%llu, "
                    "retry-timeout-ms=%llu) | addrs: %s\n",
                    args.sender_id.c_str(), args.store_forward_dir.c_str(),
                    (unsigned long long)args.batch_size,
                    (unsigned long long)args.batches_per_transaction,
                    (unsigned long long)args.retry_timeout, args.addrs.c_str());
    else if (args.protocol == "qwpudp")
        std::printf("Ingestion started. Protocol: qwpudp (QWP/UDP datagrams, ingest-only, "
                    "unauthenticated, no store-and-forward, no failover; query client disabled, "
                    "batch-size=%llu) | addr: %s\n",
                    (unsigned long long)args.batch_size, addrs[0].c_str());
    else
        std::printf("Ingestion started. Protocol: ilp (HTTP, retry-timeout-ms=%llu) | addrs: %s\n",
                    (unsigned long long)args.retry_timeout, args.addrs.c_str());

    std::atomic<uint64_t> total_sent{0};
    std::atomic<bool> stop{false};
    auto start = std::chrono::steady_clock::now();

    std::thread reporter([&] {
        uint64_t last = 0;
        while (!stop.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            uint64_t now = total_sent.load();
            std::printf("[progress] sent=%llu rate=%llu rows/s\n",
                        (unsigned long long)now, (unsigned long long)(now - last));
            last = now;
        }
    });

    std::thread probe;
    if (args.protocol == "qwp" && args.probe_interval_ms > 0)
        probe = std::thread(run_probe, std::cref(args), std::ref(stop));

    const uint64_t base = args.total_events / args.num_senders;
    const uint64_t rem = args.total_events % args.num_senders;
    std::atomic<int> failures{0};
    std::vector<std::thread> workers;
    for (unsigned id = 0; id < args.num_senders; ++id) {
        uint64_t events = base + (id < rem ? 1 : 0);
        workers.emplace_back([&, id, events] {
            try {
                run_worker(id, events, args, rows, total_sent);
            } catch (const std::exception& e) {
                std::fprintf(stderr, "Worker failed: Sender %u: %s\n", id, e.what());
                failures.fetch_add(1);
            }
        });
    }
    for (auto& w : workers) w.join();
    stop.store(true);
    if (probe.joinable()) probe.join();
    reporter.join();

    if (failures.load() > 0) return 1;

    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    double rate = elapsed > 0 ? args.total_events / elapsed : 0;
    std::printf("All workers completed. protocol=%s events=%llu elapsed=%.3f s throughput=%.0f rows/s\n",
                args.protocol.c_str(), (unsigned long long)args.total_events, elapsed, rate);
    return 0;
}
