#pragma once
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "lsmkv/memtable.h"
#include "lsmkv/sstable.h"
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
//
// Concurrency (spec section 4): a single std::shared_mutex. Reads take it shared
// only long enough to copy the snapshot; writes, flushes, and the compaction
// list-swap take it exclusive. The slow part of compaction (merge + file write)
// runs with no lock held.
class DB {
public:
    static constexpr std::size_t kDefaultThreshold = 4u * 1024 * 1024;  // 4 MiB
    static constexpr std::size_t kDefaultMinMerge = 4;
    static constexpr double kDefaultSizeRatio = 2.0;

    // Open the database rooted at dir (created if missing).
    // memtable_threshold: byte size at which the memtable flushes.
    // min_merge: number of similar-size SSTables that triggers a compaction.
    // size_ratio: size factor separating tiers.
    explicit DB(const std::string& dir,
                std::size_t memtable_threshold = kDefaultThreshold,
                std::size_t min_merge = kDefaultMinMerge,
                double size_ratio = kDefaultSizeRatio);
    ~DB();

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    void put(const std::string& key, const std::string& value);
    void del(const std::string& key);
    std::optional<std::string> get(const std::string& key) const;

    // Force the current memtable to disk as an SSTable, even if under threshold.
    void flush();

    // Block until no compaction is running and none is pending. For deterministic
    // tests; not needed in normal use.
    void wait_for_idle();

    std::size_t memtable_entry_count() const;
    std::size_t sstable_count() const;

private:
    void flush_locked();          // caller holds the exclusive lock
    void load_sstables();         // called once from the constructor
    void compaction_loop();       // body of the background thread
    bool compaction_pending() const;  // caller holds the lock

    std::string dir_;
    std::string wal_path_;
    std::size_t threshold_;
    std::size_t min_merge_;
    double size_ratio_;

    Wal wal_;
    Memtable memtable_;
    // Newest first: sstables_.front() is the most recently flushed.
    std::vector<std::shared_ptr<SSTable>> sstables_;
    std::uint64_t next_seq_ = 0;

    mutable std::shared_mutex mu_;
    // condition_variable_any because we wait on a shared_mutex, not a plain mutex.
    std::condition_variable_any cv_;
    bool stop_ = false;        // set in the destructor to end the thread
    bool compacting_ = false;  // true while a merge is in flight
    std::thread compactor_;
};

}  // namespace lsmkv
