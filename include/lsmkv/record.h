#pragma once
#include <cstdint>
#include <string>

namespace lsmkv {

// Operation type stored in the WAL and memtable.
enum class Op : uint8_t {
    Put = 0,
    Delete = 1,  // tombstone; value is empty
};

// Locates a value stored out-of-line in the value log (WiscKey key-value
// separation). offset points at the value's payload bytes; len is its length.
struct ValuePtr {
    std::uint64_t offset = 0;
    std::uint32_t len = 0;
};

// One logical write. Keys and values are arbitrary byte strings, so std::string
// is used as a byte container, not as text.
//
// A value is stored one of two ways. Inline (the default, and always so in the
// WAL and memtable): the bytes live in `value`. Separated (only in an SSTable,
// for values past the DB's separation threshold): `separated` is true, the bytes
// live in the value log at `vptr`, and `value` is empty. Compaction carries a
// separated record through by its pointer without ever touching the value bytes.
struct Record {
    Op op;
    std::string key;
    std::string value;
    bool separated = false;  // true: value is in the value log at vptr
    ValuePtr vptr{};         // valid when separated
};

}  // namespace lsmkv
