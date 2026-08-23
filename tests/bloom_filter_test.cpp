#include <gtest/gtest.h>

#include <string>

#include "lsmkv/bloom_filter.h"

using lsmkv::BloomFilter;

TEST(BloomFilterTest, NoFalseNegatives) {
    BloomFilter bf(1000, 0.01);
    for (int i = 0; i < 1000; ++i) {
        bf.add("key" + std::to_string(i));
    }
    // Every added key must report as maybe-present. A false negative would be a
    // correctness bug: a read would wrongly skip an SSTable that holds the key.
    for (int i = 0; i < 1000; ++i) {
        EXPECT_TRUE(bf.maybe_contains("key" + std::to_string(i)))
            << "false negative on key" << i;
    }
}

TEST(BloomFilterTest, FalsePositiveRateNearTarget) {
    const int n = 10000;
    const double target = 0.01;
    BloomFilter bf(n, target);
    for (int i = 0; i < n; ++i) bf.add("present" + std::to_string(i));

    int false_positives = 0;
    const int trials = 10000;
    for (int i = 0; i < trials; ++i) {
        // Keys never inserted; any hit is a false positive.
        if (bf.maybe_contains("absent" + std::to_string(i))) ++false_positives;
    }
    double rate = static_cast<double>(false_positives) / trials;
    // Allow generous headroom over the 1% target to keep the test stable.
    EXPECT_LT(rate, 0.03) << "observed FPR " << rate;
}

TEST(BloomFilterTest, SerializeRoundTripsMembership) {
    BloomFilter bf(500, 0.01);
    for (int i = 0; i < 500; ++i) bf.add("k" + std::to_string(i));

    std::string bytes = bf.serialize();
    BloomFilter restored = BloomFilter::deserialize(bytes);

    EXPECT_EQ(restored.num_bits(), bf.num_bits());
    EXPECT_EQ(restored.num_hashes(), bf.num_hashes());
    for (int i = 0; i < 500; ++i) {
        EXPECT_TRUE(restored.maybe_contains("k" + std::to_string(i)));
    }
    EXPECT_FALSE(restored.maybe_contains("definitely-not-inserted-xyz"));
}

TEST(BloomFilterTest, HandlesArbitraryBytes) {
    BloomFilter bf(4, 0.01);
    std::string key("a\0b\0c", 5);
    bf.add(key);
    EXPECT_TRUE(bf.maybe_contains(key));
}
