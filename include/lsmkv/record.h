#pragma once
#include <cstdint>
#include <string>

namespace lsmkv {

// Operation type stored in the WAL and memtable.
enum class Op : uint8_t {
    Put = 0,
    Delete = 1,  // tombstone; value is empty
};

// One logical write. Keys and values are arbitrary byte strings, so std::string
// is used as a byte container, not as text.
struct Record {
    Op op;
    std::string key;
    std::string value;
};

}  // namespace lsmkv
