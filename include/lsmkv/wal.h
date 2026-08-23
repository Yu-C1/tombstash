#pragma once
#include <string>
#include <vector>

#include "lsmkv/record.h"

namespace lsmkv {

// Append-only write-ahead log. Every write is appended and fsync'd before it is
// applied to the memtable, so an acknowledged write survives a crash.
//
// On-disk record format (little-endian lengths):
//   [op: 1 byte][key length: 4 bytes][key bytes][value length: 4 bytes][value bytes]
class Wal {
public:
    // Open (creating if needed) the log at path for appending. Existing contents
    // are preserved unless truncate is set, which starts the log empty -- used
    // after a flush, when the memtable's data is durable in an SSTable and the
    // old log is no longer needed for recovery.
    explicit Wal(std::string path, bool truncate = false);
    ~Wal();

    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;
    Wal(Wal&&) noexcept;
    Wal& operator=(Wal&&) noexcept;

    // Append one record and fsync so it is durable before returning.
    void append(const Record& rec);

    // Read every complete record from the log at path, in write order. A partial
    // trailing record (a torn write from a crash) is ignored: it was never
    // acknowledged, so dropping it loses no acknowledged write.
    static std::vector<Record> replay(const std::string& path);

private:
    void close_fd() noexcept;

    std::string path_;
    int fd_ = -1;
};

}  // namespace lsmkv
