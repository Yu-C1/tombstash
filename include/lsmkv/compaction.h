#pragma once
#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "lsmkv/record.h"
#include "lsmkv/sstable.h"

namespace lsmkv {

// The result of choosing what to compact: which SSTables (as indices into the
// newest-first list) to merge, and whether tombstones may be dropped.
struct CompactionPick {
    std::vector<std::size_t> indices;  // into the newest-first sstable list
    bool drop_tombstones;
};

// Size-tiered selection (spec section 6). Groups SSTables into size buckets
// (consecutive buckets differ by size_ratio). If some bucket holds at least
// min_merge tables, returns those to be merged, preferring the smallest such
// bucket so small tables are compacted first. Returns nullopt if no bucket
// qualifies.
//
// drop_tombstones is set only when the chosen tables are age-contiguous down to
// the oldest table -- i.e. no un-chosen, older SSTable could still hold a
// shadowed value for a deleted key. Otherwise tombstones must be kept.
std::optional<CompactionPick> pick_compaction(
    const std::vector<std::shared_ptr<SSTable>>& tables,
    std::size_t min_merge, double size_ratio);

// k-way merge of inputs given newest-first. For each key the newest value wins
// and older duplicates are dropped. Tombstones are omitted iff drop_tombstones.
// Output is sorted ascending by key.
std::vector<Record> merge_records(
    const std::vector<std::shared_ptr<SSTable>>& inputs_newest_first,
    bool drop_tombstones);

}  // namespace lsmkv
