#pragma once
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace lsmkv {

// A Bloom filter answers "is this key definitely not present?" with no false
// negatives and a tunable false-positive rate. One is stored per SSTable so a
// read can skip files that cannot hold the key.
//
// Uses k hash functions synthesized from two base hashes (double hashing):
// bit_i = (h1 + i*h2) mod m. Sized from the expected key count and target FPR.
class BloomFilter {
public:
    // Build an empty filter sized for expected_keys at target_fpr (e.g. 0.01).
    BloomFilter(std::size_t expected_keys, double target_fpr);

    void add(const std::string& key);

    // False means the key is definitely absent. True means probably present.
    bool maybe_contains(const std::string& key) const;

    // Serialized form: [num_bits:8][k:4][bit bytes]. Round-trips via deserialize.
    std::string serialize() const;
    static BloomFilter deserialize(const std::string& data);

    std::uint64_t num_bits() const { return num_bits_; }
    std::uint32_t num_hashes() const { return k_; }

private:
    BloomFilter(std::uint64_t num_bits, std::uint32_t k);  // used by deserialize

    void set_bit(std::uint64_t i);
    bool get_bit(std::uint64_t i) const;

    std::uint64_t num_bits_;
    std::uint32_t k_;
    std::vector<std::uint8_t> bits_;
};

}  // namespace lsmkv
