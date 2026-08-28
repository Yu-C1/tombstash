#pragma once
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "lsmkv/memtable.h"
#include "lsmkv/sstable.h"
#include "lsmkv/vlog.h"
#include "lsmkv/wal.h"

namespace lsmkv {

// The key-value store. Writes go to the WAL (fsync'd) then the memtable; when
// the memtable exceeds its size threshold it is flushed to a new SSTable and the
// WAL is rotated. Reads check the memtable, then SSTables newest to oldest,
// stopping at the first match (a tombstone counts as not-found).
//
// A background thread runs size-tiered compaction: it merges similar-size
// SSTables into fewer, larger ones, dropping shadowed values and (where safe)
// tombstones. Reads take a snapshot of the SSTable list (a copy of the
// shared_ptrs) and read without holding a lock, so reads never block compaction
// and compaction never deletes a file out from under a reader.
class DB {
public:
    static constexpr std::size_t kDefaultThreshold = 4u * 1024 * 1024;  // 4 MiB
    static constexpr std::size_t kDefaultMinMerge = 4;
    static constexpr double kDefaultSizeRatio = 2.0;
    // Values at least this many bytes are stored in the value log instead of
    // inline in the SSTable (WiscKey key-value separation). SIZE_MAX disables
    // separation (everything inline). Small values stay inline so short-value
    // point reads and scans keep their single-read fast path.
    static constexpr std::size_t kDefaultValueSepThreshold = 128;

    // Cumulative counters, for benchmarking and (later) the stats dashboard.
    struct Stats {
        std::uint64_t user_bytes = 0;         // key+value bytes the caller wrote
        std::uint64_t wal_bytes = 0;          // bytes appended to the WAL
        std::uint64_t flush_bytes = 0;        // bytes written by memtable flushes
        std::uint64_t compaction_bytes = 0;   // bytes written by compaction merges
        std::uint64_t vlog_bytes = 0;          // bytes appended to the value log
        std::uint64_t writes = 0;
        std::uint64_t deletes = 0;
        std::uint64_t reads = 0;
        std::uint64_t bloom_checks = 0;           // Bloom filters consulted
        std::uint64_t bloom_skips = 0;            // filters that ruled a key out
        std::uint64_t bloom_false_positives = 0;  // filter said maybe, key absent
        std::uint64_t compactions = 0;            // merges completed
    };

    // A snapshot of one SSTable's shape, for the dashboard's layout view.
    struct SSTableInfo {
        std::uint64_t size_bytes = 0;
        std::uint64_t records = 0;
        std::string min_key;
        std::string max_key;
        int tier = 0;  // size bucket (bigger = merged further)
    };

    explicit DB(const std::string& dir,
                std::size_t memtable_threshold = kDefaultThreshold,
                std::size_t min_merge = kDefaultMinMerge,
                double size_ratio = kDefaultSizeRatio,
                std::size_t value_sep_threshold = kDefaultValueSepThreshold);
    ~DB();

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    // sync=true (default) fsyncs the WAL before returning (durable on return).
    // sync=false batches the write; call sync() to make a batch durable.
    void put(const std::string& key, const std::string& value, bool sync = true);
    void del(const std::string& key, bool sync = true);
    std::optional<std::string> get(const std::string& key) const;

    // Live key-value pairs with start <= key < end, in ascending key order.
    // Merges the memtable and every SSTable; newest value wins, tombstones are
    // excluded (deleted keys are not returned).
    std::vector<std::pair<std::string, std::string>> scan(
        const std::string& start, const std::string& end) const;

    // fsync the WAL, making all preceding unsynced writes durable (group commit).
    void sync();

    // Force the current memtable to disk as an SSTable, even if under threshold.
    void flush();

    // Merge every SSTable into one (a major compaction). Synchronous; used for
    // benchmarks and to reclaim space on demand.
    void compact_all();

    // Turn the Bloom-filter read optimization on or off (to benchmark its value).
    void set_bloom_enabled(bool on);

    // Block until no compaction is running and none is pending.
    void wait_for_idle();

    std::size_t memtable_entry_count() const;
    std::size_t memtable_bytes() const;        // current memtable byte size
    std::size_t memtable_tombstones() const;   // tombstones awaiting collection
    std::size_t threshold() const { return threshold_; }  // flush threshold
    std::size_t sstable_count() const;
    std::uint64_t disk_bytes() const;  // total size of all SSTable files
    std::vector<SSTableInfo> sstable_infos() const;  // newest first
    Stats stats() const;

private:
    void flush_locked();          // caller holds the exclusive lock
    void load_sstables();         // called once from the constructor
    void compaction_loop();       // body of the background thread
    bool compaction_pending() const;  // caller holds the lock
    // Replace inputs (matched by identity) with output at the newest input's age
    // slot, marking the replaced files obsolete. Caller holds the exclusive lock.
    void install_merge_result(const std::vector<std::shared_ptr<SSTable>>& inputs,
                              std::shared_ptr<SSTable> output);

    std::string dir_;
    std::string wal_path_;
    std::string vlog_path_;
    std::size_t threshold_;
    std::size_t min_merge_;
    double size_ratio_;
    std::size_t value_sep_threshold_;

    Wal wal_;
    ValueLog vlog_;
    Memtable memtable_;
    std::vector<std::shared_ptr<SSTable>> sstables_;  // newest first
    std::uint64_t next_seq_ = 0;
    bool bloom_enabled_ = true;

    mutable std::shared_mutex mu_;
    std::condition_variable_any cv_;
    bool stop_ = false;
    bool compacting_ = false;
    std::thread compactor_;

    // Stats counters. Atomic so reads/compaction can bump them without the lock;
    // mutable because get() is const but still counts Bloom activity.
    mutable std::atomic<std::uint64_t> stat_user_bytes_{0};
    mutable std::atomic<std::uint64_t> stat_wal_bytes_{0};
    mutable std::atomic<std::uint64_t> stat_flush_bytes_{0};
    mutable std::atomic<std::uint64_t> stat_compaction_bytes_{0};
    mutable std::atomic<std::uint64_t> stat_vlog_bytes_{0};
    mutable std::atomic<std::uint64_t> stat_writes_{0};
    mutable std::atomic<std::uint64_t> stat_deletes_{0};
    mutable std::atomic<std::uint64_t> stat_reads_{0};
    mutable std::atomic<std::uint64_t> stat_bloom_checks_{0};
    mutable std::atomic<std::uint64_t> stat_bloom_skips_{0};
    mutable std::atomic<std::uint64_t> stat_bloom_fps_{0};
    mutable std::atomic<std::uint64_t> stat_compactions_{0};
};

}  // namespace lsmkv
