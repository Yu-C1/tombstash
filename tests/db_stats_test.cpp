#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "lsmkv/db.h"

namespace fs = std::filesystem;
using lsmkv::DB;

namespace {

class DBStatsTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_stats_" +
                std::string(::testing::UnitTest::GetInstance()
                                ->current_test_info()
                                ->name()));
        fs::remove_all(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }
    std::string dir() const { return dir_.string(); }
    fs::path dir_;
};

}  // namespace

TEST_F(DBStatsTest, GroupCommitThenSyncRecovers) {
    {
        DB db(dir());
        for (int i = 0; i < 50; ++i) {
            db.put("k" + std::to_string(i), "v" + std::to_string(i), /*sync=*/false);
        }
        db.sync();  // one fsync for the whole batch
    }
    DB db2(dir());
    for (int i = 0; i < 50; ++i) {
        ASSERT_TRUE(db2.get("k" + std::to_string(i)).has_value()) << "lost k" << i;
        EXPECT_EQ(db2.get("k" + std::to_string(i)).value(), "v" + std::to_string(i));
    }
}

TEST_F(DBStatsTest, CompactAllMergesToOne) {
    DB db(dir(), DB::kDefaultThreshold, /*min_merge=*/1000);  // no auto-compaction
    for (int i = 0; i < 10; ++i) {
        db.put("k" + std::to_string(i), "v" + std::to_string(i));
        db.flush();
    }
    db.del("k3");
    db.flush();
    EXPECT_GT(db.sstable_count(), 1u);

    db.compact_all();
    EXPECT_EQ(db.sstable_count(), 1u);
    EXPECT_FALSE(db.get("k3").has_value());  // deletion preserved
    for (int i = 0; i < 10; ++i) {
        if (i == 3) continue;
        EXPECT_EQ(db.get("k" + std::to_string(i)).value(), "v" + std::to_string(i));
    }
}

TEST_F(DBStatsTest, BloomOnAndOffReturnSameResults) {
    DB db(dir(), DB::kDefaultThreshold, /*min_merge=*/1000);
    for (int i = 0; i < 100; ++i) {
        db.put("k" + std::to_string(i), "v" + std::to_string(i));
        if (i % 10 == 9) db.flush();
    }
    db.flush();

    for (int i = 0; i < 100; ++i) {
        std::string k = "k" + std::to_string(i);
        db.set_bloom_enabled(true);
        auto on = db.get(k);
        db.set_bloom_enabled(false);
        auto off = db.get(k);
        EXPECT_EQ(on, off) << "mismatch on " << k;
    }
    // Absent keys agree too.
    db.set_bloom_enabled(true);
    EXPECT_FALSE(db.get("nope").has_value());
    db.set_bloom_enabled(false);
    EXPECT_FALSE(db.get("nope").has_value());
}

TEST_F(DBStatsTest, CountersTrackWork) {
    DB db(dir(), DB::kDefaultThreshold, /*min_merge=*/1000);
    db.put("a", "111");
    db.put("b", "22");
    db.del("a");
    DB::Stats s = db.stats();
    EXPECT_EQ(s.writes, 2u);
    EXPECT_EQ(s.deletes, 1u);
    // user_bytes = a+111 (1+3) + b+22 (1+2) + a-tombstone (1) = 8
    EXPECT_EQ(s.user_bytes, 8u);
    EXPECT_GT(s.wal_bytes, s.user_bytes);  // includes per-record framing overhead
    EXPECT_EQ(s.flush_bytes, 0u);

    db.flush();
    EXPECT_GT(db.stats().flush_bytes, 0u);
}
