#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <random>
#include <string>

#include "lsmkv/db.h"

namespace fs = std::filesystem;
using lsmkv::DB;

namespace {

class ManifestTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_man_" + std::string(::testing::UnitTest::GetInstance()
                                               ->current_test_info()
                                               ->name()));
        fs::remove_all(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }
    std::string dir() const { return dir_.string(); }
    fs::path dir_;

    // Build the age-non-contiguous scenario in `db`: an old big table holds
    // k=OLD, a tiny newer table overwrites k=NEW, then two more big tables arrive.
    // The big tables share a size bucket but the tiny one sits between them by age.
    // A correct compaction must NOT merge across that gap (it would resurrect OLD).
    static void fillNonContiguous(DB& db) {
        std::string big(500, 'x');
        db.put("k", "OLD");
        for (int i = 0; i < 100; ++i) db.put("A" + std::to_string(i), big);
        db.flush();  // big, holds k=OLD
        db.put("k", "NEW");
        db.flush();  // tiny, holds k=NEW (newer)
        for (int i = 0; i < 100; ++i) db.put("B" + std::to_string(i), big);
        db.flush();  // big
        for (int i = 0; i < 100; ++i) db.put("C" + std::to_string(i), big);
        db.flush();  // big -> big bucket has 3 but they are not age-adjacent
        db.wait_for_idle();
    }
};

}  // namespace

TEST_F(ManifestTest, AgeNonContiguousMergeKeepsNewestInMemory) {
    DB db(dir(), 64u * 1024 * 1024, /*min_merge=*/3, 2.0, /*sep=*/SIZE_MAX);
    fillNonContiguous(db);
    EXPECT_EQ(db.get("k"), std::optional<std::string>("NEW"));  // not resurrected OLD
}

TEST_F(ManifestTest, AgeNonContiguousMergeSurvivesReopen) {
    {
        DB db(dir(), 64u * 1024 * 1024, 3, 2.0, SIZE_MAX);
        fillNonContiguous(db);
    }
    DB db(dir(), 64u * 1024 * 1024, 3, 2.0, SIZE_MAX);  // reopen (manifest order)
    EXPECT_EQ(db.get("k"), std::optional<std::string>("NEW"));
}

TEST_F(ManifestTest, ManifestWrittenAndReopenPreservesData) {
    {
        DB db(dir(), 8 * 1024, 4);
        for (int i = 0; i < 400; ++i) db.put("k" + std::to_string(i), "v" + std::to_string(i));
        db.wait_for_idle();
    }
    EXPECT_TRUE(fs::exists((dir_ / "MANIFEST").string()));
    DB db(dir(), 8 * 1024, 4);
    for (int i = 0; i < 400; ++i)
        EXPECT_EQ(db.get("k" + std::to_string(i)),
                  std::optional<std::string>("v" + std::to_string(i)));
}

TEST_F(ManifestTest, OrphanFilesCleanedOnOpen) {
    {
        DB db(dir(), 8 * 1024, 1000);
        for (int i = 0; i < 50; ++i) db.put("k" + std::to_string(i), std::string(200, 'v'));
        db.wait_for_idle();
    }
    // Drop leftovers an interrupted operation might have left behind.
    auto touch = [&](const std::string& name, const std::string& body) {
        std::ofstream(dir_ / name, std::ios::binary) << body;
    };
    touch("sst-999999.sst", "garbage-not-in-manifest");
    touch("vlog-000099.log", "orphan-generation");
    touch("stale.tmp", "interrupted");
    ASSERT_TRUE(fs::exists(dir_ / "sst-999999.sst"));

    DB db(dir(), 8 * 1024, 1000);  // reopen: cleanup_orphans runs
    EXPECT_FALSE(fs::exists(dir_ / "sst-999999.sst"));
    EXPECT_FALSE(fs::exists(dir_ / "vlog-000099.log"));
    EXPECT_FALSE(fs::exists(dir_ / "stale.tmp"));
    for (int i = 0; i < 50; ++i)  // real data intact
        EXPECT_EQ(db.get("k" + std::to_string(i)),
                  std::optional<std::string>(std::string(200, 'v')));
}

TEST_F(ManifestTest, BootstrapWhenManifestMissing) {
    {
        DB db(dir(), 8 * 1024, 4);
        for (int i = 0; i < 200; ++i) db.put("k" + std::to_string(i), "v" + std::to_string(i));
        db.wait_for_idle();
    }
    fs::remove(dir_ / "MANIFEST");  // simulate a store from before manifests
    DB db(dir(), 8 * 1024, 4);      // must bootstrap from a directory scan
    for (int i = 0; i < 200; ++i)
        EXPECT_EQ(db.get("k" + std::to_string(i)),
                  std::optional<std::string>("v" + std::to_string(i)));
    EXPECT_TRUE(fs::exists((dir_ / "MANIFEST").string()));  // and re-establish one
}

TEST_F(ManifestTest, ReopenConsistencyFuzz) {
    std::mt19937 rng(20260827);
    std::map<std::string, std::optional<std::string>> expected;
    auto keyname = [](int i) { return "key" + std::to_string(i); };

    {
        DB db(dir(), 16 * 1024, 4, 2.0, /*sep=*/64);
        for (int step = 0; step < 4000; ++step) {
            int i = static_cast<int>(rng() % 120);
            std::string k = keyname(i);
            int op = static_cast<int>(rng() % 10);
            if (op < 7) {
                std::string v = "v" + std::to_string(step) + std::string(rng() % 200, 'x');
                db.put(k, v);
                expected[k] = v;
            } else if (op < 9) {
                db.del(k);
                expected[k] = std::nullopt;
            } else {
                db.flush();
            }
            if (step % 1500 == 1499) db.compact_all();
            if (step % 2000 == 1999) db.gc_value_log();
        }
        db.wait_for_idle();
        // Values match before reopen.
        for (const auto& [k, v] : expected) EXPECT_EQ(db.get(k), v) << k;
    }
    // ... and identically after reopen (manifest order + age-contiguous merges).
    DB db(dir(), 16 * 1024, 4, 2.0, /*sep=*/64);
    for (const auto& [k, v] : expected) EXPECT_EQ(db.get(k), v) << "after reopen: " << k;
}
