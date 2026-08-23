#pragma once
#include <cstddef>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
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
// On open, existing SSTables are loaded and the WAL is replayed, so acknowledged
// writes survive a crash. Compaction arrives in Week 3.
//
// Concurrency stage 1 (spec section 4): a single std::shared_mutex -- shared for
// reads, exclusive for writes and flushes.
class DB {
public:
    static constexpr std::size_t kDefaultThreshold = 4u * 1024 * 1024;  // 4 MiB

    // Open the database rooted at dir (created if missing). memtable_threshold is
    // the byte size at which the memtable flushes to an SSTable.
    explicit DB(const std::string& dir,
                std::size_t memtable_threshold = kDefaultThreshold);

    void put(const std::string& key, const std::string& value);
    void del(const std::string& key);
    std::optional<std::string> get(const std::string& key) const;

    // Force the current memtable to disk as an SSTable, even if under threshold.
    void flush();

    std::size_t memtable_entry_count() const;
    std::size_t sstable_count() const;

private:
    void flush_locked();       // caller holds the exclusive lock
    void load_sstables();      // called once from the constructor

    std::string dir_;
    std::string wal_path_;
    std::size_t threshold_;
    Wal wal_;
    Memtable memtable_;
    // Newest first: sstables_.front() is the most recently flushed.
    std::vector<std::shared_ptr<SSTable>> sstables_;
    std::uint64_t next_seq_ = 0;
    mutable std::shared_mutex mu_;
};

}  // namespace lsmkv
