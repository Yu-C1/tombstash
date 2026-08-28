#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "lsmkv/db.h"

namespace fs = std::filesystem;
using lsmkv::DB;

namespace {

class LeveledTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_lvl_" + std::string(::testing::UnitTest::GetInstance()
                                               ->current_test_info()
                                               ->name()));
        fs::remove_all(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }
    std::string dir() const { return dir_.string(); }
    fs::path dir_;

    static constexpr std::size_t kThresh = 4 * 1024;
    static std::string key(int i) {
        char b[16];
        std::snprintf(b, sizeof(b), "k%07d", i);
        return b;
    }
    static std::string val(int i) { return "v" + std::to_string(i) + std::string(180, 'x'); }

    // Every level >= 1 must hold non-overlapping key ranges (the leveled invariant).
    static void assertNonOverlapping(const DB& db) {
        auto infos = db.sstable_infos();
        std::map<int, std::vector<std::pair<std::string, std::string>>> byLevel;
        for (const auto& in : infos) {
            if (in.tier >= 1) byLevel[in.tier].emplace_back(in.min_key, in.max_key);
        }
        for (auto& [lv, ranges] : byLevel) {
            std::sort(ranges.begin(), ranges.end());
            for (std::size_t i = 1; i < ranges.size(); ++i) {
                EXPECT_LT(ranges[i - 1].second, ranges[i].first)
                    << "overlap in level " << lv;
            }
        }
    }
};

}  // namespace

TEST_F(LeveledTest, WritesReadBackAndBuildLevels) {
    DB db(dir(), kThresh, 4, 2.0, DB::kDefaultValueSepThreshold, DB::Compaction::Leveled);
    const int n = 2000;
    for (int i = 0; i < n; ++i) db.put(key(i), val(i));
    db.wait_for_idle();

    int max_level = 0;
    for (const auto& in : db.sstable_infos()) max_level = std::max(max_level, in.tier);
    EXPECT_GE(max_level, 1);  // compaction pushed data below L0
    assertNonOverlapping(db);
    for (int i = 0; i < n; ++i)
        EXPECT_EQ(db.get(key(i)), std::optional<std::string>(val(i))) << "i=" << i;
}

TEST_F(LeveledTest, OverwritesNewestWins) {
    DB db(dir(), kThresh, 4, 2.0, DB::kDefaultValueSepThreshold, DB::Compaction::Leveled);
    const int n = 800;
    for (int i = 0; i < n; ++i) db.put(key(i), "old");
    for (int i = 0; i < n; ++i) db.put(key(i), val(i));  // overwrite each
    db.wait_for_idle();
    assertNonOverlapping(db);
    for (int i = 0; i < n; ++i)
        EXPECT_EQ(db.get(key(i)), std::optional<std::string>(val(i))) << "i=" << i;
}

TEST_F(LeveledTest, DeletesRemoveKeys) {
    DB db(dir(), kThresh, 4, 2.0, DB::kDefaultValueSepThreshold, DB::Compaction::Leveled);
    const int n = 800;
    for (int i = 0; i < n; ++i) db.put(key(i), val(i));
    for (int i = 0; i < n; i += 3) db.del(key(i));
    db.wait_for_idle();
    assertNonOverlapping(db);
    for (int i = 0; i < n; ++i) {
        auto v = db.get(key(i));
        if (i % 3 == 0)
            EXPECT_EQ(v, std::nullopt) << "i=" << i;
        else
            EXPECT_EQ(v, std::optional<std::string>(val(i))) << "i=" << i;
    }
}

TEST_F(LeveledTest, SurvivesReopen) {
    {
        DB db(dir(), kThresh, 4, 2.0, DB::kDefaultValueSepThreshold, DB::Compaction::Leveled);
        for (int i = 0; i < 1500; ++i) db.put(key(i), val(i));
        db.wait_for_idle();
    }
    // Reopen: the manifest records each table's level, so the level structure and
    // read order are restored.
    DB db(dir(), kThresh, 4, 2.0, DB::kDefaultValueSepThreshold, DB::Compaction::Leveled);
    assertNonOverlapping(db);
    for (int i = 0; i < 1500; ++i)
        EXPECT_EQ(db.get(key(i)), std::optional<std::string>(val(i))) << "i=" << i;
}

TEST_F(LeveledTest, ScanAcrossLevels) {
    DB db(dir(), kThresh, 4, 2.0, DB::kDefaultValueSepThreshold, DB::Compaction::Leveled);
    const int n = 1500;
    for (int i = 0; i < n; ++i) db.put(key(i), val(i));
    db.wait_for_idle();
    auto r = db.scan(key(100), key(200));
    ASSERT_EQ(r.size(), 100u);
    for (std::size_t i = 0; i < r.size(); ++i) {
        EXPECT_EQ(r[i].first, key(100 + static_cast<int>(i)));
        if (i) EXPECT_LT(r[i - 1].first, r[i].first);  // sorted
    }
}

TEST_F(LeveledTest, ReopenConsistencyFuzz) {
    std::mt19937 rng(424242);
    std::map<std::string, std::optional<std::string>> expected;
    auto kn = [](int i) { return "key" + std::to_string(i); };
    {
        DB db(dir(), kThresh, 4, 2.0, /*sep=*/64, DB::Compaction::Leveled);
        for (int step = 0; step < 3000; ++step) {
            int i = static_cast<int>(rng() % 150);
            std::string k = kn(i);
            int op = static_cast<int>(rng() % 10);
            if (op < 8) {
                std::string v = "v" + std::to_string(step) + std::string(rng() % 150, 'x');
                db.put(k, v);
                expected[k] = v;
            } else {
                db.del(k);
                expected[k] = std::nullopt;
            }
            if (step % 2500 == 2499) db.gc_value_log();
        }
        db.wait_for_idle();
        assertNonOverlapping(db);
        for (const auto& [k, v] : expected) EXPECT_EQ(db.get(k), v) << k;
    }
    DB db(dir(), kThresh, 4, 2.0, /*sep=*/64, DB::Compaction::Leveled);
    for (const auto& [k, v] : expected) EXPECT_EQ(db.get(k), v) << "reopen: " << k;
}
