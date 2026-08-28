#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "lsmkv/db.h"

namespace fs = std::filesystem;
using lsmkv::DB;

namespace {

class GcTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_gc_" + std::string(::testing::UnitTest::GetInstance()
                                              ->current_test_info()
                                              ->name()));
        fs::remove_all(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }
    std::string dir() const { return dir_.string(); }
    fs::path dir_;

    static constexpr std::size_t kSep = 64;
    static constexpr std::size_t kNoCompact = 1000;
    static std::string big(char c) { return std::string(300, c); }

    // Total bytes of all value-log files currently on disk in the DB dir.
    std::uint64_t vlog_dir_bytes() const {
        std::uint64_t total = 0;
        for (const auto& e : fs::directory_iterator(dir_)) {
            std::string n = e.path().filename().string();
            if (n.rfind("vlog-", 0) == 0 && e.is_regular_file()) {
                total += fs::file_size(e.path());
            }
        }
        return total;
    }
};

}  // namespace

TEST_F(GcTest, ReclaimsOverwrittenValues) {
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    // Same key overwritten many times, each flushed -> many dead copies in the log.
    for (int i = 0; i < 50; ++i) {
        db.put("k", big(static_cast<char>('a' + i % 26)));
        db.flush();
    }
    std::uint64_t before = vlog_dir_bytes();
    db.gc_value_log();
    std::uint64_t after = vlog_dir_bytes();

    EXPECT_LT(after, before);              // dead copies reclaimed
    EXPECT_LT(after, before / 10);         // ~50 copies -> 1
    EXPECT_EQ(db.get("k"), std::optional<std::string>(big('a' + 49 % 26)));
    EXPECT_EQ(db.sstable_count(), 1u);     // merged to one table
}

TEST_F(GcTest, LiveKeysCorrectAfterGc) {
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    const int n = 100;
    for (int i = 0; i < n; ++i) db.put("key" + std::to_string(i), big('x'));
    db.flush();
    for (int i = 0; i < n; i += 2) db.put("key" + std::to_string(i), big('y'));  // overwrite evens
    db.flush();
    for (int i = 0; i < n; i += 5) db.del("key" + std::to_string(i));            // delete /5
    db.flush();

    db.gc_value_log();

    for (int i = 0; i < n; ++i) {
        auto v = db.get("key" + std::to_string(i));
        if (i % 5 == 0) {
            EXPECT_EQ(v, std::nullopt) << "i=" << i << " should be deleted";
        } else if (i % 2 == 0) {
            EXPECT_EQ(v, std::optional<std::string>(big('y'))) << "i=" << i;
        } else {
            EXPECT_EQ(v, std::optional<std::string>(big('x'))) << "i=" << i;
        }
    }
}

TEST_F(GcTest, SurvivesReopenAfterGc) {
    {
        DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
        db.put("a", big('a'));
        db.put("b", big('b'));
        db.flush();
        db.put("a", big('A'));  // overwrite
        db.flush();
        db.gc_value_log();
    }
    // Reopen: old generation is gone; pointers must resolve against the new one.
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    EXPECT_EQ(db.get("a"), std::optional<std::string>(big('A')));
    EXPECT_EQ(db.get("b"), std::optional<std::string>(big('b')));
}

TEST_F(GcTest, RepeatableAndEmptyAndInlineOnly) {
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    db.gc_value_log();  // empty DB: no crash, nothing to do

    db.put("s", "tiny");           // inline (below threshold)
    db.put("b", big('b'));         // separated
    db.flush();
    db.gc_value_log();
    db.gc_value_log();             // twice in a row
    EXPECT_EQ(db.get("s"), std::optional<std::string>("tiny"));
    EXPECT_EQ(db.get("b"), std::optional<std::string>(big('b')));
}

TEST_F(GcTest, ConcurrentReadsDuringGc) {
    DB db(dir(), 16 * 1024, kNoCompact, DB::kDefaultSizeRatio, kSep);
    const int n = 200;
    auto val = [](int i) { return std::string(300, static_cast<char>('A' + i % 26)); };
    for (int i = 0; i < n; ++i) db.put("key" + std::to_string(i), val(i));
    db.flush();

    std::atomic<bool> stop{false};
    std::atomic<bool> mismatch{false};
    std::vector<std::thread> readers;
    for (int t = 0; t < 4; ++t) {
        readers.emplace_back([&] {
            while (!stop.load()) {
                for (int i = 0; i < n; ++i) {
                    auto v = db.get("key" + std::to_string(i));
                    if (!v || *v != val(i)) mismatch.store(true);
                }
            }
        });
    }
    for (int g = 0; g < 3; ++g) db.gc_value_log();  // swap generations under readers
    stop.store(true);
    for (auto& th : readers) th.join();

    EXPECT_FALSE(mismatch.load());
    for (int i = 0; i < n; ++i)
        EXPECT_EQ(db.get("key" + std::to_string(i)),
                  std::optional<std::string>(val(i)));
}
