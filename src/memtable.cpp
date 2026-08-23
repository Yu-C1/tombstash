#include "lsmkv/memtable.h"

namespace lsmkv {

namespace {
// Byte accounting for one entry: key plus value payload.
std::size_t entry_bytes(const std::string& key, const std::string& value) {
    return key.size() + value.size();
}
}  // namespace

void Memtable::put(const std::string& key, const std::string& value) {
    auto it = entries_.find(key);
    if (it != entries_.end()) {
        size_bytes_ -= entry_bytes(key, it->second.value);
        it->second.value = value;
        it->second.tombstone = false;
    } else {
        it = entries_.emplace(key, Entry{value, false}).first;
    }
    size_bytes_ += entry_bytes(key, it->second.value);
}

void Memtable::del(const std::string& key) {
    auto it = entries_.find(key);
    if (it != entries_.end()) {
        size_bytes_ -= entry_bytes(key, it->second.value);
        it->second.value.clear();
        it->second.tombstone = true;
    } else {
        it = entries_.emplace(key, Entry{"", true}).first;
    }
    // A tombstone's value is empty, so it contributes only its key bytes.
    size_bytes_ += key.size();
}

std::optional<std::string> Memtable::get(const std::string& key) const {
    auto it = entries_.find(key);
    if (it == entries_.end() || it->second.tombstone) {
        return std::nullopt;
    }
    return it->second.value;
}

bool Memtable::is_tombstone(const std::string& key) const {
    auto it = entries_.find(key);
    return it != entries_.end() && it->second.tombstone;
}

}  // namespace lsmkv
