#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "lsmkv/bloom_filter.h"
#include "lsmkv/record.h"

namespace lsmkv {

// A sorted, immutable on-disk file created when a memtable flushes.
//
// Records are grouped into ~4 KiB data blocks. The index is SPARSE: one entry
// per block (its first key + the block's byte offset and length), not one entry
// per key. This keeps the in-memory index small enough to scale to far more keys
// than a dense per-key index would -- at the cost of reading and scanning a whole
// block per lookup. A per-SSTable Bloom filter lets a read skip the block read
// entirely for keys the file does not contain.
//
// A value past the DB's separation threshold is stored out-of-line in the value
// log (WiscKey); its record holds a pointer instead of the bytes, flagged by the
// high bit of the op byte. Compaction carries such a record through by pointer,
// so a large value is not rewritten on every merge.
//
// File layout:
//   [data region ]  concatenated blocks; each block is sorted records
//                   inline:    [op:1][klen:4][key][vlen:4][val]
//                   separated: [op|0x80:1][klen:4][key][vlen:4][vlog_gen:4][vlog_offset:8]
//   [index block ]  one entry per block: [klen:4][first_key][offset:8][length:8]
//   [bloom block ]  serialized Bloom filter
//   [footer      ]  [data_size:8][index_size:8][bloom_size:8][num_records:8][magic:8]
class SSTable {
public:
    // ~4 KiB target block size (a block may exceed it by one record).
    static constexpr std::size_t kBlockSize = 4096;

    // Process-wide count of data-block reads (one disk read each on a cold cache).
    // A benchmarking instrument: it makes the Bloom filter's savings visible
    // independent of the page cache. Not part of normal operation.
    static std::uint64_t block_reads();
    static void reset_block_reads();

    // Write records (which MUST be sorted ascending by key, tombstones included)
    // to path as a new SSTable, fsync'd before returning. Returns bytes written.
    static std::uint64_t build(const std::string& path,
                               const std::vector<Record>& sorted);

    explicit SSTable(std::string path);
    ~SSTable();

    SSTable(const SSTable&) = delete;
    SSTable& operator=(const SSTable&) = delete;

    // The stored record for key, or nullopt if absent. Bloom filter first, then
    // sparse-index binary search to the one candidate block, then scan the block.
    std::optional<Record> get(const std::string& key) const;

    // The Bloom filter's answer alone: false means definitely absent.
    bool may_contain(const std::string& key) const;

    // Sparse-index lookup + block scan, without consulting the Bloom filter.
    std::optional<Record> get_no_bloom(const std::string& key) const;

    // Records with start <= key < end, ascending, tombstones included. Seeks to
    // the start block via the sparse index, then reads blocks until end. For scans.
    std::vector<Record> range(const std::string& start,
                              const std::string& end) const;

    const std::string& path() const { return path_; }
    std::size_t key_count() const { return num_records_; }
    std::size_t block_count() const { return index_.size(); }
    std::uint64_t size_bytes() const { return file_size_; }
    const std::string& min_key() const { return min_key_; }  // smallest key in file
    const std::string& max_key() const { return max_key_; }  // largest key in file
    void mark_obsolete() { obsolete_ = true; }

    // Forward cursor over every record in key order, reading one block at a time.
    // Used by compaction's k-way merge.
    class Iterator {
    public:
        explicit Iterator(const SSTable* sst);
        bool valid() const;
        const std::string& key() const { return block_[pos_].key; }
        const Record& record() const { return block_[pos_]; }
        void next();

    private:
        void load_block(std::size_t i);
        const SSTable* sst_;
        std::size_t block_idx_ = 0;
        std::size_t pos_ = 0;
        std::vector<Record> block_;
    };

    Iterator iterator() const { return Iterator(this); }

private:
    struct IndexEntry {
        std::string first_key;  // smallest key in the block
        std::uint64_t offset;   // byte offset of the block in the data region
        std::uint64_t length;   // block size in bytes
    };

    // Read block i off disk and parse its records (sorted, key order).
    std::vector<Record> read_block_records(std::size_t i) const;

    std::string path_;
    int fd_ = -1;
    std::uint64_t data_size_ = 0;
    std::uint64_t file_size_ = 0;
    std::uint64_t num_records_ = 0;
    bool obsolete_ = false;
    std::string min_key_;
    std::string max_key_;
    std::vector<IndexEntry> index_;  // one entry per block, sorted by first_key
    BloomFilter bloom_;
};

}  // namespace lsmkv
