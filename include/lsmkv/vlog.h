#pragma once
#include <cstdint>
#include <string>

#include "lsmkv/record.h"

namespace lsmkv {

// Append-only log of separated values (WiscKey key-value separation, spec's
// standout layer). Values past the DB's separation threshold are written here
// instead of inline in the SSTable, so a compaction merge rewrites a small
// fixed-size pointer instead of the whole value -- cutting write amplification.
//
// On-disk entry format: [klen:4][key][vlen:4][value]. The key is stored beside
// each value so a future garbage collector can look the key up in the LSM to
// decide whether the value is still live. GC is not implemented yet: the log is
// append-only, so overwritten and deleted values leave dead bytes behind and the
// file only grows. A ValuePtr returned by append() points straight at the value
// payload, so read() needs only the offset and length, not the key.
//
// Concurrency: append()/sync() run under the DB's exclusive lock (flush only);
// read() uses a positioned pread and needs no lock. A pointer only becomes
// visible to readers after its value was appended and the log fsync'd, so a
// reader never races an in-flight append.
class ValueLog {
public:
    explicit ValueLog(std::string path);
    ~ValueLog();

    ValueLog(const ValueLog&) = delete;
    ValueLog& operator=(const ValueLog&) = delete;

    // Append one value; returns where its payload landed. Does not fsync -- call
    // sync() before publishing any pointer into the log (e.g. before renaming the
    // SSTable that references it), or the pointer could dangle after a crash.
    ValuePtr append(const std::string& key, const std::string& value);

    // fsync the log, making every appended value durable.
    void sync();

    // The value bytes at ptr. A positioned read; safe to call concurrently.
    std::string read(const ValuePtr& ptr) const;

    std::uint64_t size_bytes() const { return append_off_; }

private:
    std::string path_;
    int fd_ = -1;                   // O_RDWR: pwrite to append, pread to read
    std::uint64_t append_off_ = 0;  // next append position (= current file size)
};

}  // namespace lsmkv
