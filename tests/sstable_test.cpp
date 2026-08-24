#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

#include "lsmkv/record.h"
#include "lsmkv/sstable.h"

namespace fs = std::filesystem;
using lsmkv::Op;
using lsmkv::Record;
using lsmkv::SSTable;

namespace {

class SSTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_sst_" +
                std::string(::testing::UnitTest::GetInstance()
                                ->current_test_info()
                                ->name()));
        fs::remove_all(dir_);
        fs::create_directories(dir_);
        path_ = (dir_ / "t.sst").string();
    }
    void TearDown() override { fs::remove_all(dir_); }

    fs::path dir_;
    std::string path_;
};

}  // namespace

TEST_F(SSTableTest, BuildThenGetHitsAndMisses) {
    std::vector<Record> recs = {
        {Op::Put, "alpha", "1"},
        {Op::Put, "bravo", "2"},
        {Op::Put, "charlie", "3"},
    };
    SSTable::build(path_, recs);
    SSTable sst(path_);

    ASSERT_TRUE(sst.get("alpha").has_value());
    EXPECT_EQ(sst.get("alpha")->value, "1");
    EXPECT_EQ(sst.get("charlie")->value, "3");
    EXPECT_FALSE(sst.get("delta").has_value());  // absent
    EXPECT_EQ(sst.key_count(), 3u);
}

TEST_F(SSTableTest, StoresTombstones) {
    std::vector<Record> recs = {
        {Op::Delete, "gone", ""},
        {Op::Put, "here", "v"},
    };
    SSTable::build(path_, recs);
    SSTable sst(path_);

    auto rec = sst.get("gone");
    ASSERT_TRUE(rec.has_value());
    EXPECT_EQ(rec->op, Op::Delete);  // tombstone recovered, shadows older data
    EXPECT_EQ(sst.get("here")->op, Op::Put);
}

TEST_F(SSTableTest, PreservesArbitraryBytes) {
    std::string key("k\0y", 3);
    std::string val("\0\1\2\3", 4);
    std::vector<Record> recs = {{Op::Put, key, val}};
    SSTable::build(path_, recs);
    SSTable sst(path_);

    auto rec = sst.get(key);
    ASSERT_TRUE(rec.has_value());
    EXPECT_EQ(rec->value, val);
    EXPECT_EQ(rec->value.size(), 4u);
}

TEST_F(SSTableTest, ReopenReadsSameData) {
    std::vector<Record> recs;
    for (int i = 0; i < 200; ++i) {
        char k[16];
        std::snprintf(k, sizeof(k), "key%04d", i);
        recs.push_back({Op::Put, k, "val" + std::to_string(i)});
    }
    SSTable::build(path_, recs);
    {
        SSTable sst(path_);
        EXPECT_EQ(sst.get("key0100")->value, "val100");
    }
    // A fresh handle on the same immutable file reads identical data.
    SSTable sst2(path_);
    EXPECT_EQ(sst2.get("key0000")->value, "val0");
    EXPECT_EQ(sst2.get("key0199")->value, "val199");
    EXPECT_FALSE(sst2.get("key0200").has_value());
}

TEST_F(SSTableTest, RejectsBadMagic) {
    // A file that is not an SSTable must be refused, not misread.
    std::string bad = (dir_ / "bad.sst").string();
    {
        std::FILE* f = std::fopen(bad.c_str(), "wb");
        ASSERT_NE(f, nullptr);
        std::string junk(64, 'x');
        std::fwrite(junk.data(), 1, junk.size(), f);
        std::fclose(f);
    }
    EXPECT_THROW(SSTable{bad}, std::exception);
}

TEST_F(SSTableTest, SpansMultipleBlocks) {
    // Enough records to exceed the ~4 KiB block size several times over, so the
    // sparse index has more than one entry and lookups cross blocks.
    std::vector<Record> recs;
    const int n = 5000;
    for (int i = 0; i < n; ++i) {
        char k[16];
        std::snprintf(k, sizeof(k), "key%06d", i);
        recs.push_back({Op::Put, k, "value-" + std::to_string(i)});
    }
    SSTable::build(path_, recs);
    SSTable sst(path_);

    EXPECT_EQ(sst.key_count(), static_cast<std::size_t>(n));
    EXPECT_GT(sst.block_count(), 1u);  // genuinely sparse: many blocks

    // Hits at the start, middle, end, and block boundaries.
    EXPECT_EQ(sst.get("key000000")->value, "value-0");
    EXPECT_EQ(sst.get("key002500")->value, "value-2500");
    EXPECT_EQ(sst.get("key004999")->value, "value-4999");
    // Misses: before all keys, after all keys, and inside the range.
    EXPECT_FALSE(sst.get("aaa").has_value());
    EXPECT_FALSE(sst.get("key999999").has_value());
    EXPECT_FALSE(sst.get("key002500x").has_value());
}

TEST_F(SSTableTest, IteratorVisitsEveryRecordInOrder) {
    std::vector<Record> recs;
    const int n = 3000;
    for (int i = 0; i < n; ++i) {
        char k[16];
        std::snprintf(k, sizeof(k), "k%06d", i);
        recs.push_back({Op::Put, k, "v" + std::to_string(i)});
    }
    SSTable::build(path_, recs);
    SSTable sst(path_);

    int seen = 0;
    std::string prev;
    for (auto it = sst.iterator(); it.valid(); it.next()) {
        if (seen > 0) EXPECT_LT(prev, it.key());  // strictly ascending
        prev = it.key();
        ++seen;
    }
    EXPECT_EQ(seen, n);  // every record, once
}
