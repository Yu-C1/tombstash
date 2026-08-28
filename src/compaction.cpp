#include "lsmkv/compaction.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>

namespace lsmkv {

namespace {

// Size bucket a table falls into: consecutive buckets differ by size_ratio, so
// two tables in the same bucket are within a factor of size_ratio in size.
long size_bucket(std::uint64_t size, double ratio) {
    double s = static_cast<double>(size < 1 ? 1 : size);
    return static_cast<long>(std::floor(std::log(s) / std::log(ratio)));
}

// True when indices (sorted ascending, into a newest-first list of `total`
// tables) run contiguously all the way to the oldest table. Then every table
// older than the newest chosen one is also chosen, so no un-chosen older table
// can hold a shadowed value -- tombstones are safe to drop.
bool contiguous_to_oldest(const std::vector<std::size_t>& sorted_indices,
                          std::size_t total) {
    if (sorted_indices.empty()) return false;
    if (sorted_indices.back() != total - 1) return false;  // must reach oldest
    for (std::size_t k = 0; k < sorted_indices.size(); ++k) {
        if (sorted_indices[k] != sorted_indices.front() + k) return false;
    }
    return true;
}

}  // namespace

std::optional<CompactionPick> pick_compaction(
    const std::vector<std::shared_ptr<SSTable>>& tables, std::size_t min_merge,
    double size_ratio) {
    if (min_merge < 2 || tables.size() < min_merge) return std::nullopt;
    const std::size_t n = tables.size();

    // A merge must be age-contiguous. `tables` is newest-first, so a set of
    // *consecutive* indices has no other table interleaved in age; a set with a
    // gap does -- and if that skipped table holds a newer value for a key an
    // older merged table also holds, the merge would keep the older value and,
    // placed at the newest input's slot, shadow the newer one. So restrict a pick
    // to a maximal run of consecutive tables in the same size bucket, and prefer
    // the smallest such bucket (compact small, newer tables before big ones).
    std::optional<CompactionPick> best;
    long best_bucket = 0;
    std::size_t i = 0;
    while (i < n) {
        long b = size_bucket(tables[i]->size_bytes(), size_ratio);
        std::size_t j = i;
        while (j < n && size_bucket(tables[j]->size_bytes(), size_ratio) == b) ++j;
        if (j - i >= min_merge && (!best || b < best_bucket)) {
            CompactionPick pick;
            for (std::size_t k = i; k < j; ++k) pick.indices.push_back(k);
            pick.drop_tombstones = contiguous_to_oldest(pick.indices, n);
            best = std::move(pick);
            best_bucket = b;
        }
        i = j;
    }
    return best;
}

std::vector<Record> merge_records(
    const std::vector<std::shared_ptr<SSTable>>& inputs, bool drop_tombstones) {
    const std::size_t n = inputs.size();
    // One streaming cursor per input; each reads its SSTable one block at a time.
    std::vector<SSTable::Iterator> cursor;
    cursor.reserve(n);
    for (const auto& in : inputs) cursor.push_back(in->iterator());
    std::vector<Record> out;

    for (;;) {
        // Smallest key currently under any cursor.
        bool any = false;
        std::string min_key;
        for (std::size_t j = 0; j < n; ++j) {
            if (!cursor[j].valid()) continue;
            const std::string& k = cursor[j].key();
            if (!any || k < min_key) {
                min_key = k;
                any = true;
            }
        }
        if (!any) break;

        // Inputs are newest-first, so the smallest index holding min_key is the
        // newest copy -- that record wins.
        std::size_t newest = std::numeric_limits<std::size_t>::max();
        for (std::size_t j = 0; j < n; ++j) {
            if (cursor[j].valid() && cursor[j].key() == min_key) {
                newest = j;
                break;
            }
        }
        Record rec = cursor[newest].record();

        // Consume this key from every input (drop the shadowed older copies).
        for (std::size_t j = 0; j < n; ++j) {
            if (cursor[j].valid() && cursor[j].key() == min_key) cursor[j].next();
        }

        if (!(drop_tombstones && rec.op == Op::Delete)) {
            out.push_back(std::move(rec));
        }
    }
    return out;
}

}  // namespace lsmkv
