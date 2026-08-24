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

    // Group table indices by size bucket. std::map keeps buckets ordered from
    // smallest size to largest.
    std::map<long, std::vector<std::size_t>> buckets;
    for (std::size_t i = 0; i < tables.size(); ++i) {
        buckets[size_bucket(tables[i]->size_bytes(), size_ratio)].push_back(i);
    }

    // Smallest qualifying bucket first: compact small tables before big ones.
    for (const auto& [bucket, indices] : buckets) {
        (void)bucket;
        if (indices.size() >= min_merge) {
            CompactionPick pick;
            pick.indices = indices;  // already ascending
            pick.drop_tombstones = contiguous_to_oldest(indices, tables.size());
            return pick;
        }
    }
    return std::nullopt;
}

std::vector<Record> merge_records(
    const std::vector<std::shared_ptr<SSTable>>& inputs, bool drop_tombstones) {
    const std::size_t n = inputs.size();
    std::vector<std::size_t> cursor(n, 0);
    std::vector<Record> out;

    auto valid = [&](std::size_t j) { return cursor[j] < inputs[j]->key_count(); };

    for (;;) {
        // Smallest key currently under any cursor.
        bool any = false;
        std::string min_key;
        for (std::size_t j = 0; j < n; ++j) {
            if (!valid(j)) continue;
            const std::string& k = inputs[j]->key_at(cursor[j]);
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
            if (valid(j) && inputs[j]->key_at(cursor[j]) == min_key) {
                newest = j;
                break;
            }
        }
        Record rec = inputs[newest]->record_at(cursor[newest]);

        // Consume this key from every input (drop the shadowed older copies).
        for (std::size_t j = 0; j < n; ++j) {
            if (valid(j) && inputs[j]->key_at(cursor[j]) == min_key) ++cursor[j];
        }

        if (!(drop_tombstones && rec.op == Op::Delete)) {
            out.push_back(std::move(rec));
        }
    }
    return out;
}

}  // namespace lsmkv
