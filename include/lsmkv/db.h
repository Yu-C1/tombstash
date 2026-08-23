#pragma once
#include <cstddef>
#include <optional>
#include <shared_mutex>
#include <string>

#include "lsmkv/memtable.h"
#include "lsmkv/wal.h"

namespace lsmkv {

// Week 1 database: a durable memtable. Writes go to the WAL (fsync'd) then the
// memtable; reads hit the memtable. On open, the WAL is replayed to rebuild the
// memtable, so acknowledged writes survive a crash. SSTable flushing and
// compaction arrive in later weeks.
//
// Concurrency stage 1 (spec section 4): a single std::shared_mutex. Reads take a
// shared lock, writes an exclusive lock.
class DB {
public:
    // Open the database rooted at dir (created if missing). Replays dir/wal.log.
    explicit DB(const std::string& dir);

    void put(const std::string& key, const std::string& value);
    void del(const std::string& key);
    std::optional<std::string> get(const std::string& key) const;

    std::size_t entry_count() const;

private:
    std::string wal_path_;
    Wal wal_;
    Memtable memtable_;
    mutable std::shared_mutex mu_;
};

}  // namespace lsmkv
