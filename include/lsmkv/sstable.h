#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "lsmkv/bloom_filter.h"
#include "lsmkv/record.h"

namespace lsmkv {

// A sorted, immutable on-disk file created when a memtable flushes. Never
// modified after writing.
//
// File layout (see spec section 8):
//   [data block ]  sorted records: [op:1][klen:4][key][vlen:4][val]
//   [index block]  one entry per record: [klen:4][key][offset:8]
//   [bloom block]  serialized Bloom filter
//   [footer     ]  [data_size:8][index_size:8][bloom_size:8][magic:8]
//
// The index is dense (one entry per key), so a lookup is a single binary search
// plus one positioned read of the record.
class SSTable {
public:
    // Write records (which MUST be sorted ascending by key, tombstones included)
    // to path as a new SSTable, fsync'd before returning.
    static void build(const std::string& path, const std::vector<Record>& sorted);

    // Open an existing SSTable, loading its index and Bloom filter into memory.
    // The file stays open for positioned reads.
    explicit SSTable(std::string path);
    ~SSTable();

    SSTable(const SSTable&) = delete;
    SSTable& operator=(const SSTable&) = delete;

    // The stored record for key (op distinguishes a value from a tombstone), or
    // nullopt if this SSTable does not contain the key.
    std::optional<Record> get(const std::string& key) const;

    const std::string& path() const { return path_; }
    std::size_t key_count() const { return index_.size(); }

private:
    struct IndexEntry {
        std::string key;
        std::uint64_t offset;  // byte offset of the record in the data block
    };

    std::string path_;
    int fd_ = -1;
    std::uint64_t data_size_ = 0;  // end offset of the data block
    std::vector<IndexEntry> index_;
    BloomFilter bloom_;
};

}  // namespace lsmkv
