// Benchmark harness for the LSM key-value store (spec section 13).
//
// Produces a throughput table on a large workload and an SVG bar chart. Build in
// Release for meaningful numbers:
//   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
//   cmake --build build-release -j
//   ./build-release/benchmark [num_keys] [svg_out]
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "lsmkv/db.h"
#include "lsmkv/sstable.h"

namespace fs = std::filesystem;
using lsmkv::SSTable;
using lsmkv::DB;
using Clock = std::chrono::steady_clock;

namespace {

std::string key_of(int i) {
    char buf[24];
    std::snprintf(buf, sizeof(buf), "key%010d", i);
    return buf;
}

double seconds_since(Clock::time_point t0) {
    return std::chrono::duration<double>(Clock::now() - t0).count();
}

double per_sec(std::uint64_t ops, double secs) {
    return secs > 0 ? static_cast<double>(ops) / secs : 0.0;
}

// A random permutation of [0, n) with a fixed seed, so read order is shuffled
// but reproducible across runs.
std::vector<int> shuffled_indices(int n) {
    std::vector<int> v(n);
    std::iota(v.begin(), v.end(), 0);
    std::mt19937 rng(12345);
    std::shuffle(v.begin(), v.end(), rng);
    return v;
}

void write_svg(const std::string& path, const std::vector<std::string>& labels,
               const std::vector<double>& values) {
    const int w = 720, row_h = 46, pad_top = 56, pad_left = 250, bar_max = 400;
    const int h = pad_top + row_h * static_cast<int>(values.size()) + 20;
    double vmax = *std::max_element(values.begin(), values.end());
    if (vmax <= 0) vmax = 1;

    std::string s;
    char buf[512];
    std::snprintf(buf, sizeof(buf),
                  "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%d\" "
                  "height=\"%d\" font-family=\"sans-serif\">\n",
                  w, h);
    s += buf;
    s += "<text x=\"20\" y=\"32\" font-size=\"20\" font-weight=\"bold\">"
         "tombstash throughput (ops/sec)</text>\n";
    for (std::size_t i = 0; i < values.size(); ++i) {
        int y = pad_top + static_cast<int>(i) * row_h;
        int bw = static_cast<int>(bar_max * (values[i] / vmax));
        if (bw < 1) bw = 1;
        std::snprintf(buf, sizeof(buf),
                      "<text x=\"20\" y=\"%d\" font-size=\"14\">%s</text>\n",
                      y + 20, labels[i].c_str());
        s += buf;
        std::snprintf(buf, sizeof(buf),
                      "<rect x=\"%d\" y=\"%d\" width=\"%d\" height=\"24\" "
                      "rx=\"3\" fill=\"#3b82f6\"/>\n",
                      pad_left, y + 4, bw);
        s += buf;
        std::snprintf(buf, sizeof(buf),
                      "<text x=\"%d\" y=\"%d\" font-size=\"14\">%.0f</text>\n",
                      pad_left + bw + 8, y + 20, values[i]);
        s += buf;
    }
    s += "</svg>\n";

    std::FILE* f = std::fopen(path.c_str(), "wb");
    if (f) {
        std::fwrite(s.data(), 1, s.size(), f);
        std::fclose(f);
        std::printf("\nWrote chart: %s\n", path.c_str());
    }
}

// Totals for one write-amplification run.
struct WaResult {
    std::uint64_t user = 0, wal = 0, flush = 0, compaction = 0, vlog = 0;
    std::uint64_t sstable_disk = 0;
    std::size_t tables = 0;
};

// Write wn keys with group commit, let background compaction settle, and report
// how many bytes hit disk. sep_threshold = SIZE_MAX keeps every value inline;
// a small value separates them into the value log. Default min_merge/size_ratio
// so compaction actually runs -- that repeated rewriting is what separation cuts.
WaResult run_wa(const std::string& dir, int wn, const std::string& value,
                std::size_t sep_threshold) {
    fs::remove_all(dir);
    WaResult r;
    {
        DB db(dir, DB::kDefaultThreshold, DB::kDefaultMinMerge,
              DB::kDefaultSizeRatio, sep_threshold);
        for (int i = 0; i < wn; ++i) {
            db.put(key_of(i), value, /*sync=*/false);
            if ((i % 1000) == 999) db.sync();
        }
        db.sync();
        db.flush();
        db.wait_for_idle();  // let tiered compaction finish
        DB::Stats s = db.stats();
        r.user = s.user_bytes;
        r.wal = s.wal_bytes;
        r.flush = s.flush_bytes;
        r.compaction = s.compaction_bytes;
        r.vlog = s.vlog_bytes;
        r.sstable_disk = db.disk_bytes();
        r.tables = db.sstable_count();
    }
    fs::remove_all(dir);
    return r;
}

}  // namespace

int main(int argc, char** argv) {
    const int n = (argc > 1) ? std::atoi(argv[1]) : 1'000'000;
    const std::string svg_out = (argc > 2) ? argv[2] : "results.svg";
    const std::string value(100, 'x');  // fixed 100-byte values

    fs::path dir = fs::temp_directory_path() / "lsmkv_bench_main";
    fs::path dir_sync = fs::temp_directory_path() / "lsmkv_bench_sync";
    fs::remove_all(dir);
    fs::remove_all(dir_sync);

    std::printf("tombstash benchmark: %d keys, %zu-byte values\n\n", n,
                value.size());

    // Disable background compaction (min_merge huge) so the benchmark controls
    // when compaction happens and several SSTables remain for the Bloom test.
    const std::size_t kNoAutoCompact = 1'000'000'000;

    // --- Phase 1: sequential writes, group commit (fsync every 1000) ---------
    DB db(dir.string(), DB::kDefaultThreshold, kNoAutoCompact);
    auto t0 = Clock::now();
    for (int i = 0; i < n; ++i) {
        db.put(key_of(i), value, /*sync=*/false);
        if ((i % 1000) == 999) db.sync();
    }
    db.sync();
    double write_group = per_sec(n, seconds_since(t0));
    db.flush();  // push the tail of the memtable to an SSTable

    // --- Phase 2: sequential writes, fsync per write (smaller sample) --------
    const int m = std::min(n, 20'000);
    DB db_sync(dir_sync.string(), DB::kDefaultThreshold, kNoAutoCompact);
    t0 = Clock::now();
    for (int i = 0; i < m; ++i) db_sync.put(key_of(i), value, /*sync=*/true);
    double write_sync = per_sec(m, seconds_since(t0));

    const std::size_t tables_before = db.sstable_count();

    // --- Phase 3: random reads of existing keys, Bloom ON --------------------
    const int reads = std::min(n, 200'000);
    std::vector<int> order = shuffled_indices(n);
    db.set_bloom_enabled(true);
    SSTable::reset_block_reads();
    t0 = Clock::now();
    std::uint64_t found = 0;
    for (int i = 0; i < reads; ++i) {
        if (db.get(key_of(order[i])).has_value()) ++found;
    }
    double read_bloom_on = per_sec(reads, seconds_since(t0));
    std::uint64_t blk_present_on = SSTable::block_reads();

    // --- Phase 4: same reads, Bloom OFF --------------------------------------
    db.set_bloom_enabled(false);
    SSTable::reset_block_reads();
    t0 = Clock::now();
    for (int i = 0; i < reads; ++i) (void)db.get(key_of(order[i]));
    double read_bloom_off = per_sec(reads, seconds_since(t0));
    std::uint64_t blk_present_off = SSTable::block_reads();

    // --- Phase 5: missing-key reads, Bloom ON (where the filter earns its keep)
    db.set_bloom_enabled(true);
    auto snap_miss0 = db.stats();
    SSTable::reset_block_reads();
    t0 = Clock::now();
    for (int i = 0; i < reads; ++i) (void)db.get("missing" + std::to_string(i));
    double read_missing = per_sec(reads, seconds_since(t0));
    std::uint64_t blk_missing_on = SSTable::block_reads();
    auto snap_miss1 = db.stats();

    // --- Phase 5b: missing-key reads, Bloom OFF (read a block in every table) -
    db.set_bloom_enabled(false);
    SSTable::reset_block_reads();
    t0 = Clock::now();
    for (int i = 0; i < reads; ++i) (void)db.get("missing" + std::to_string(i));
    double read_missing_off = per_sec(reads, seconds_since(t0));
    std::uint64_t blk_missing_off = SSTable::block_reads();
    db.set_bloom_enabled(true);

    // --- Phase 6: disk usage before/after a full compaction ------------------
    std::uint64_t disk_before = db.disk_bytes();
    db.compact_all();
    std::uint64_t disk_after = db.disk_bytes();
    const std::size_t tables_after = db.sstable_count();

    // --- Report --------------------------------------------------------------
    DB::Stats s = db.stats();
    double wa = s.user_bytes ? static_cast<double>(s.wal_bytes + s.flush_bytes +
                                                   s.compaction_bytes) /
                                   static_cast<double>(s.user_bytes)
                             : 0.0;
    auto mib = [](std::uint64_t b) { return b / (1024.0 * 1024.0); };
    double miss_checks = static_cast<double>(snap_miss1.bloom_checks -
                                             snap_miss0.bloom_checks);
    double miss_skips = static_cast<double>(snap_miss1.bloom_skips -
                                            snap_miss0.bloom_skips);
    (void)found;

    std::printf("== Throughput ==\n");
    std::printf("  sequential writes (group commit) : %12.0f ops/sec\n", write_group);
    std::printf("  sequential writes (fsync/write)  : %12.0f ops/sec\n", write_sync);
    std::printf("  random reads (present), Bloom ON : %12.0f ops/sec\n", read_bloom_on);
    std::printf("  random reads (present), Bloom OFF: %12.0f ops/sec\n", read_bloom_off);
    std::printf("  missing-key reads, Bloom ON      : %12.0f ops/sec\n", read_missing);
    std::printf("  missing-key reads, Bloom OFF     : %12.0f ops/sec\n", read_missing_off);
    std::printf("  Bloom speedup on missing keys    : %12.1fx\n",
                read_missing_off > 0 ? read_missing / read_missing_off : 0.0);

    // The real payoff of the sparse index + Bloom: each avoided block read is a
    // disk read on a cold cache. This count is cache-independent.
    std::printf("\n== Data-block reads (%d reads each; lower = Bloom skipped more) ==\n",
                reads);
    std::printf("  present keys, Bloom ON           : %12llu\n",
                (unsigned long long)blk_present_on);
    std::printf("  present keys, Bloom OFF          : %12llu\n",
                (unsigned long long)blk_present_off);
    std::printf("  missing keys, Bloom ON           : %12llu\n",
                (unsigned long long)blk_missing_on);
    std::printf("  missing keys, Bloom OFF          : %12llu\n",
                (unsigned long long)blk_missing_off);
    std::printf("  block reads Bloom eliminated (missing): %llu -> %llu\n",
                (unsigned long long)blk_missing_off,
                (unsigned long long)blk_missing_on);

    std::printf("\n== Space ==\n");
    std::printf("  SSTables before compaction       : %zu (%.1f MiB)\n",
                tables_before, mib(disk_before));
    std::printf("  SSTables after compaction        : %zu (%.1f MiB)\n",
                tables_after, mib(disk_after));

    std::printf("\n== Write amplification ==\n");
    std::printf("  user data written                : %.1f MiB\n", mib(s.user_bytes));
    std::printf("  WAL bytes                        : %.1f MiB (%.2fx)\n",
                mib(s.wal_bytes), s.user_bytes ? (double)s.wal_bytes / s.user_bytes : 0);
    std::printf("  flush bytes                      : %.1f MiB (%.2fx)\n",
                mib(s.flush_bytes), s.user_bytes ? (double)s.flush_bytes / s.user_bytes : 0);
    std::printf("  compaction bytes                 : %.1f MiB (%.2fx)\n",
                mib(s.compaction_bytes),
                s.user_bytes ? (double)s.compaction_bytes / s.user_bytes : 0);
    std::printf("  total write amplification        : %.2fx\n", wa);

    // --- Key-value separation (WiscKey): write amp inline vs separated --------
    // A separate, smaller workload with background compaction ON, so values are
    // rewritten by real merges. Separation makes those merges move pointers, not
    // values. Values here (100 B) are separated at a 64-byte threshold.
    const int wn = std::min(n, 200'000);
    fs::path dir_inline = fs::temp_directory_path() / "lsmkv_bench_inline";
    fs::path dir_sep = fs::temp_directory_path() / "lsmkv_bench_sep";
    WaResult in = run_wa(dir_inline.string(), wn, value, SIZE_MAX);
    WaResult sep = run_wa(dir_sep.string(), wn, value, 64);
    auto wa_of = [](const WaResult& r) {
        return r.user ? (double)(r.wal + r.flush + r.compaction + r.vlog) / r.user
                      : 0.0;
    };

    std::printf("\n== Key-value separation (WiscKey), %d keys, %zu-byte values ==\n",
                wn, value.size());
    std::printf("  %-34s %12s %12s\n", "", "inline", "separated");
    std::printf("  %-34s %12.1f %12.1f\n", "flush bytes (MiB)",
                mib(in.flush), mib(sep.flush));
    std::printf("  %-34s %12.1f %12.1f\n", "compaction bytes (MiB)",
                mib(in.compaction), mib(sep.compaction));
    std::printf("  %-34s %12.1f %12.1f\n", "value-log bytes (MiB)",
                mib(in.vlog), mib(sep.vlog));
    std::printf("  %-34s %12.2fx %11.2fx\n", "total write amplification",
                wa_of(in), wa_of(sep));
    std::printf("  %-34s %12.1f %12.1f\n", "on-disk sstables (MiB)",
                mib(in.sstable_disk), mib(sep.sstable_disk));
    std::printf("  %-34s %12.1f %12.1f\n", "on-disk total incl. vlog (MiB)",
                mib(in.sstable_disk), mib(sep.sstable_disk + sep.vlog));
    std::printf("  compaction bytes cut by separation : %.2fx\n",
                sep.compaction ? (double)in.compaction / sep.compaction : 0.0);

    std::printf("\n== Bloom filter (missing-key phase) ==\n");
    std::printf("  checks                           : %.0f\n", miss_checks);
    std::printf("  correct skips                    : %.0f (%.2f%%)\n", miss_skips,
                miss_checks ? 100.0 * miss_skips / miss_checks : 0.0);
    std::printf("  false positives                  : %.0f (%.4f%%)\n",
                miss_checks - miss_skips,
                miss_checks ? 100.0 * (miss_checks - miss_skips) / miss_checks : 0.0);

    write_svg(svg_out,
              {"seq writes (group commit)", "seq writes (fsync/write)",
               "present reads (Bloom on)", "present reads (Bloom off)",
               "missing reads (Bloom on)", "missing reads (Bloom off)"},
              {write_group, write_sync, read_bloom_on, read_bloom_off,
               read_missing, read_missing_off});

    fs::remove_all(dir);
    fs::remove_all(dir_sync);
    return 0;
}
