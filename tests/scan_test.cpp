#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include "lsmkv/db.h"

namespace fs = std::filesystem;
using lsmkv::DB;
using KV = std::pair<std::string, std::string>;

namespace {

class ScanTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_scan_" +
                std::string(::testing::UnitTest::GetInstance()
                                ->current_test_info()
                                ->name()));
        fs::remove_all(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }
    std::string dir() const { return dir_.string(); }
    fs::path dir_;
};

std::vector<std::string> keys_of(const std::vector<KV>& kvs) {
    std::vector<std::string> ks;
    for (const auto& kv : kvs) ks.push_back(kv.first);
    return ks;
}

}  // namespace

TEST_F(ScanTest, MemtableOnlyReturnsSortedRange) {
    DB db(dir());
    db.put("d", "4");
    db.put("a", "1");
    db.put("c", "3");
    db.put("b", "2");
    auto r = db.scan("b", "d");  // [b, d): b, c   (d excluded)
    ASSERT_EQ(r.size(), 2u);
    EXPECT_EQ(r[0], (KV{"b", "2"}));
    EXPECT_EQ(r[1], (KV{"c", "3"}));
}

TEST_F(ScanTest, HalfOpenBounds) {
    DB db(dir());
    for (char c = 'a'; c <= 'e'; ++c) db.put(std::string(1, c), "v");
    auto r = db.scan("b", "d");
    EXPECT_EQ(keys_of(r), (std::vector<std::string>{"b", "c"}));  // start incl, end excl
}

TEST_F(ScanTest, MergesMemtableAndSSTables) {
    DB db(dir(), DB::kDefaultThreshold, /*min_merge=*/1000);
    db.put("a", "1"); db.put("c", "3"); db.flush();  // in an SSTable
    db.put("b", "2"); db.put("d", "4");              // in the memtable
    auto r = db.scan("a", "z");
    EXPECT_EQ(keys_of(r), (std::vector<std::string>{"a", "b", "c", "d"}));
    EXPECT_EQ(r[1].second, "2");
    EXPECT_EQ(r[2].second, "3");
}

TEST_F(ScanTest, NewestValueWins) {
    DB db(dir(), DB::kDefaultThreshold, /*min_merge=*/1000);
    db.put("k", "old"); db.flush();
    db.put("k", "new");  // shadows the flushed value
    auto r = db.scan("k", "k~");
    ASSERT_EQ(r.size(), 1u);
    EXPECT_EQ(r[0], (KV{"k", "new"}));
}

TEST_F(ScanTest, TombstonesAreExcluded) {
    DB db(dir(), DB::kDefaultThreshold, /*min_merge=*/1000);
    db.put("a", "1"); db.put("b", "2"); db.put("c", "3"); db.flush();
    db.del("b");  // tombstone in memtable shadows the flushed b
    auto r = db.scan("a", "z");
    EXPECT_EQ(keys_of(r), (std::vector<std::string>{"a", "c"}));  // b is deleted
}

TEST_F(ScanTest, EmptyRange) {
    DB db(dir());
    db.put("a", "1");
    EXPECT_TRUE(db.scan("x", "z").empty());
    EXPECT_TRUE(db.scan("a", "a").empty());  // start == end -> empty
}

TEST_F(ScanTest, SpansBlocksAndTablesInOrder) {
    DB db(dir(), /*threshold=*/8 * 1024, /*min_merge=*/1000);  // many flushes + blocks
    const int n = 5000;
    for (int i = 0; i < n; ++i) {
        char k[16];
        std::snprintf(k, sizeof(k), "key%05d", i);
        db.put(k, "v" + std::to_string(i));
    }
    auto r = db.scan("key01000", "key02000");
    ASSERT_EQ(r.size(), 1000u);
    EXPECT_EQ(r.front().first, "key01000");
    EXPECT_EQ(r.back().first, "key01999");
    for (std::size_t i = 1; i < r.size(); ++i) {
        EXPECT_LT(r[i - 1].first, r[i].first);  // strictly ascending, no dups
    }
}
