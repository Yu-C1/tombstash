// A thin, dependency-free HTTP server that exposes the engine's live stats as
// JSON and serves the dashboard page. The engine stays pure: this only reads
// through the DB's public accessors and drives a workload through put()/del().
//
//   stats_server [port] [web_dir]
//   routes:  GET /            -> web_dir/index.html
//            GET /app.js      -> web_dir/app.js
//            GET /stats       -> JSON snapshot of DB stats
//            POST /workload   -> run a background insert/delete workload
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <thread>

#include "lsmkv/db.h"

namespace fs = std::filesystem;
using lsmkv::DB;

namespace {

std::atomic<bool> g_workload_running{false};
std::atomic<bool> g_workload_group{false};

std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (unsigned char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += static_cast<char>(c);
                }
        }
    }
    return out;
}

std::string build_stats_json(DB& db) {
    DB::Stats s = db.stats();
    std::ostringstream j;
    j << "{";
    j << "\"memtable\":{"
      << "\"entries\":" << db.memtable_entry_count() << ","
      << "\"bytes\":" << db.memtable_bytes() << ","
      << "\"threshold\":" << db.threshold() << ","
      << "\"tombstones\":" << db.memtable_tombstones() << "},";
    j << "\"counters\":{"
      << "\"reads\":" << s.reads << ","
      << "\"writes\":" << s.writes << ","
      << "\"deletes\":" << s.deletes << ","
      << "\"compactions\":" << s.compactions << "},";
    j << "\"bloom\":{"
      << "\"checks\":" << s.bloom_checks << ","
      << "\"skips\":" << s.bloom_skips << ","
      << "\"falsePositives\":" << s.bloom_false_positives << "},";
    j << "\"writeAmp\":{"
      << "\"user\":" << s.user_bytes << ","
      << "\"wal\":" << s.wal_bytes << ","
      << "\"flush\":" << s.flush_bytes << ","
      << "\"compaction\":" << s.compaction_bytes << "},";
    j << "\"disk\":" << db.disk_bytes() << ",";
    j << "\"workload\":{\"running\":"
      << (g_workload_running.load() ? "true" : "false") << ",\"group\":"
      << (g_workload_group.load() ? "true" : "false") << "},";
    j << "\"sstables\":[";
    auto infos = db.sstable_infos();
    for (std::size_t i = 0; i < infos.size(); ++i) {
        const auto& t = infos[i];
        if (i) j << ",";
        j << "{\"size\":" << t.size_bytes << ",\"records\":" << t.records
          << ",\"tier\":" << t.tier << ",\"min\":\"" << json_escape(t.min_key)
          << "\",\"max\":\"" << json_escape(t.max_key) << "\"}";
    }
    j << "]}";
    return j.str();
}

// A fixed-duration workload of random inserts (and occasional deletes). Two
// durability modes, so the dashboard can contrast them live:
//   group = false -> fsync per write (durable each op, ~hundreds/sec)
//   group = true  -> group commit: one fsync per chunk (fast, ~tens of thousands/sec)
// Both run ~6 seconds, so the same time budget shows wildly different work done.
void run_workload(DB* db, bool group) {
    using namespace std::chrono;
    const auto start = steady_clock::now();
    std::mt19937 rng(2025);
    std::uniform_int_distribution<int> key_dist(0, 200000);
    std::string value(100, 'x');
    const int chunk = group ? 1000 : 50;
    while (steady_clock::now() - start < seconds(6)) {
        for (int i = 0; i < chunk; ++i) {
            int k = key_dist(rng);
            std::string key = "key" + std::to_string(k);
            const bool sync = !group;  // fsync mode syncs every write
            if (k % 17 == 0) {
                db->del(key, sync);
            } else {
                db->put(key, value, sync);
            }
        }
        if (group) {
            db->sync();  // one fsync for the whole chunk
            std::this_thread::sleep_for(milliseconds(12));  // smooth the chart
        }
        // fsync mode needs no sleep -- the per-write fsync already paces it.
    }
    if (group) db->sync();
    g_workload_running.store(false);
}

std::string http_response(const std::string& status, const std::string& ctype,
                          const std::string& body) {
    std::ostringstream r;
    r << "HTTP/1.1 " << status << "\r\n"
      << "Content-Type: " << ctype << "\r\n"
      << "Content-Length: " << body.size() << "\r\n"
      << "Connection: close\r\n\r\n"
      << body;
    return r.str();
}

std::string serve_file(const fs::path& path, const std::string& ctype) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return http_response("404 Not Found", "text/plain", "not found");
    std::ostringstream ss;
    ss << in.rdbuf();
    return http_response("200 OK", ctype, ss.str());
}

std::string handle(const std::string& method, const std::string& path, DB& db,
                   const fs::path& web_dir) {
    if (method == "GET" && (path == "/" || path == "/index.html")) {
        return serve_file(web_dir / "index.html", "text/html");
    }
    if (method == "GET" && path == "/app.js") {
        return serve_file(web_dir / "app.js", "application/javascript");
    }
    if (method == "GET" && path == "/stats") {
        return http_response("200 OK", "application/json", build_stats_json(db));
    }
    if (method == "POST" && path.rfind("/workload", 0) == 0) {
        const bool group = path.find("mode=group") != std::string::npos;
        bool expected = false;
        bool started = g_workload_running.compare_exchange_strong(expected, true);
        if (started) {
            g_workload_group.store(group);
            std::thread(run_workload, &db, group).detach();
        }
        return http_response("200 OK", "application/json",
                             started ? "{\"ok\":true}" : "{\"ok\":false,\"busy\":true}");
    }
    return http_response("404 Not Found", "text/plain", "not found");
}

}  // namespace

int main(int argc, char** argv) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8080;
    fs::path web_dir = (argc > 2) ? argv[2] : "dashboard/web";

    fs::path db_dir = fs::temp_directory_path() / "lsmkv_dashboard";
    fs::remove_all(db_dir);
    // Small memtable + normal tiering so flushes and compactions happen visibly.
    DB db(db_dir.string(), /*threshold=*/64 * 1024, /*min_merge=*/4);

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::perror("socket");
        return 1;
    }
    int opt = 1;
    ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);  // localhost only
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::perror("bind");
        return 1;
    }
    ::listen(server_fd, 16);
    std::printf("tombstash dashboard on http://localhost:%d  (Ctrl+C to stop)\n",
                port);

    for (;;) {
        int client = ::accept(server_fd, nullptr, nullptr);
        if (client < 0) continue;

        std::string req;
        char buf[4096];
        while (req.find("\r\n\r\n") == std::string::npos) {
            ssize_t n = ::recv(client, buf, sizeof(buf), 0);
            if (n <= 0) break;
            req.append(buf, static_cast<std::size_t>(n));
            if (req.size() > (1u << 20)) break;
        }

        std::string method, path;
        {
            std::istringstream line(req);
            line >> method >> path;
        }

        std::string resp = handle(method, path, db, web_dir);
        ::send(client, resp.data(), resp.size(), 0);
        ::close(client);
    }
}
