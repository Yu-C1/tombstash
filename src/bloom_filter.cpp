#include "lsmkv/bloom_filter.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

#include "lsmkv/encoding.h"

namespace lsmkv {

namespace {

// FNV-1a 64-bit hash over the raw bytes, with a seed so two independent hashes
// can be drawn from one key for double hashing.
std::uint64_t fnv1a(const std::string& s, std::uint64_t seed) {
    std::uint64_t h = 1469598103934665603ULL ^ seed;
    for (unsigned char c : s) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    return h;
}

// Optimal bit count m = -n ln p / (ln 2)^2, rounded up and floored at 1.
std::uint64_t optimal_bits(std::size_t n, double p) {
    if (n == 0) n = 1;
    if (!(p > 0.0) || !(p < 1.0)) p = 0.01;
    double m = -static_cast<double>(n) * std::log(p) / (M_LN2 * M_LN2);
    if (m < 1.0) m = 1.0;
    return static_cast<std::uint64_t>(std::ceil(m));
}

// Optimal hash count k = (m/n) ln 2, clamped to a sane range.
std::uint32_t optimal_hashes(std::uint64_t m, std::size_t n) {
    if (n == 0) n = 1;
    double k = static_cast<double>(m) / static_cast<double>(n) * M_LN2;
    long r = std::lround(k);
    if (r < 1) r = 1;
    if (r > 30) r = 30;
    return static_cast<std::uint32_t>(r);
}

}  // namespace

BloomFilter::BloomFilter(std::size_t expected_keys, double target_fpr)
    : num_bits_(optimal_bits(expected_keys, target_fpr)),
      k_(optimal_hashes(num_bits_, expected_keys)),
      bits_((num_bits_ + 7) / 8, 0) {}

BloomFilter::BloomFilter(std::uint64_t num_bits, std::uint32_t k)
    : num_bits_(num_bits == 0 ? 1 : num_bits),
      k_(k == 0 ? 1 : k),
      bits_((num_bits_ + 7) / 8, 0) {}

void BloomFilter::set_bit(std::uint64_t i) { bits_[i >> 3] |= (1u << (i & 7)); }

bool BloomFilter::get_bit(std::uint64_t i) const {
    return (bits_[i >> 3] >> (i & 7)) & 1u;
}

void BloomFilter::add(const std::string& key) {
    std::uint64_t h1 = fnv1a(key, 0);
    std::uint64_t h2 = fnv1a(key, 0x9e3779b97f4a7c15ULL) | 1ULL;  // keep h2 odd
    for (std::uint32_t i = 0; i < k_; ++i) {
        set_bit((h1 + static_cast<std::uint64_t>(i) * h2) % num_bits_);
    }
}

bool BloomFilter::maybe_contains(const std::string& key) const {
    std::uint64_t h1 = fnv1a(key, 0);
    std::uint64_t h2 = fnv1a(key, 0x9e3779b97f4a7c15ULL) | 1ULL;
    for (std::uint32_t i = 0; i < k_; ++i) {
        if (!get_bit((h1 + static_cast<std::uint64_t>(i) * h2) % num_bits_)) {
            return false;  // a clear bit proves the key was never added
        }
    }
    return true;
}

std::string BloomFilter::serialize() const {
    std::string out;
    out.reserve(8 + 4 + bits_.size());
    enc::put_u64_le(out, num_bits_);
    enc::put_u32_le(out, k_);
    out.append(reinterpret_cast<const char*>(bits_.data()), bits_.size());
    return out;
}

BloomFilter BloomFilter::deserialize(const std::string& data) {
    if (data.size() < 12) {
        throw std::runtime_error("bloom: truncated header");
    }
    std::uint64_t num_bits = enc::read_u64_le(data.data());
    std::uint32_t k = enc::read_u32_le(data.data() + 8);
    BloomFilter bf(num_bits, k);
    std::size_t want = bf.bits_.size();
    if (data.size() - 12 != want) {
        throw std::runtime_error("bloom: bit-array size mismatch");
    }
    std::copy(data.begin() + 12, data.end(),
              reinterpret_cast<char*>(bf.bits_.data()));
    return bf;
}

}  // namespace lsmkv
