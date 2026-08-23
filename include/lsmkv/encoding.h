#pragma once
#include <cstdint>
#include <string>

// Little-endian fixed-width integer encoding shared by the WAL and SSTable
// on-disk formats. Little-endian is fixed regardless of host so files are
// portable.
namespace lsmkv::enc {

inline void put_u32_le(std::string& buf, uint32_t v) {
    buf.push_back(static_cast<char>(v & 0xff));
    buf.push_back(static_cast<char>((v >> 8) & 0xff));
    buf.push_back(static_cast<char>((v >> 16) & 0xff));
    buf.push_back(static_cast<char>((v >> 24) & 0xff));
}

inline void put_u64_le(std::string& buf, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        buf.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
}

inline uint32_t read_u32_le(const char* p) {
    return static_cast<uint32_t>(static_cast<unsigned char>(p[0])) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

inline uint64_t read_u64_le(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
    }
    return v;
}

}  // namespace lsmkv::enc
