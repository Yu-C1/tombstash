#pragma once
#include <cstddef>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "lsmkv/record.h"

namespace lsmkv {

// In-memory sorted table of recent writes. Backed by std::map for Week 1; a skip
// list is a later optimization. Not thread-safe on its own -- the DB layer owns
// synchronization.
class Memtable {
public:
    // Insert or overwrite a live value.
    void put(const std::string& key, const std::string& value);

    // Insert a tombstone marking the key deleted. Shadows any older value.
    void del(const std::string& key);

    // The value if the key is present and live. nullopt if the key is absent or
    // tombstoned.
    std::optional<std::string> get(const std::string& key) const;

    // Whether the key is present as a tombstone in this memtable. Needed later
    // so a memtable delete can shadow a value living in an older SSTable.
    bool is_tombstone(const std::string& key) const;

    // All entries in ascending key order, tombstones included, as records ready
    // to write to an SSTable on flush.
    std::vector<Record> snapshot() const;

    std::size_t entry_count() const { return entries_.size(); }

    // Number of entries currently held as tombstones (awaiting collection).
    std::size_t tombstone_count() const;

    // Approximate in-memory byte size of all keys and values held. Drives the
    // flush threshold in later weeks.
    std::size_t size_bytes() const { return size_bytes_; }

    bool empty() const { return entries_.empty(); }

private:
    struct Entry {
        std::string value;
        bool tombstone = false;
    };

    std::map<std::string, Entry> entries_;
    std::size_t size_bytes_ = 0;
};

}  // namespace lsmkv
