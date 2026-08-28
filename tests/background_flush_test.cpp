#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "lsmkv/db.h"
#include "lsmkv/wal.h"

namespace fs = std::filesystem;
using lsmkv::DB;
using lsmkv::Op;
using lsmkv::Record;
using lsmkv::Wal;

namespace {

class BgFlushTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_bgflush_" + std::string(::testing::UnitTest::GetInstance()
                                                   ->current_test_info()
                                                   ->name()));
        fs::remove_all(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }
    std::string dir() const { return dir_.string(); }
    fs::path dir_;

    static constexpr std::size_t kSmall = 4 * 1024;  // tiny threshold -> many flushes
    static constexpr std::size_t kNoCompact = 1000;
    static std::string key(int i) {
        char b[16];
        std::snprintf(b, sizeof(b), "k%06d", i);
        return b;
    }
    static std::string val(int i) { return std::string(300, static_cast<char>('A' + i % 26)); }
};

}  // namespace

TEST_F(BgFlushTest, BackgroundFlushProducesTablesAndKeepsEveryKey) {
    DB db(dir(), kSmall, kNoCompact);
    const int n = 500;
    for (int i = 0; i < n; ++i) db.put(key(i), val(i));  // crosses threshold many times
    db.wait_for_idle();
    EXPECT_GT(db.sstable_count(), 1u);  // flushes ran in the background
    for (int i = 0; i < n; ++i)
        EXPECT_EQ(db.get(key(i)), std::optional<std::string>(val(i))) << "i=" << i;
}

TEST_F(BgFlushTest, ReadsSeeInFlightDataWithoutWaiting) {
    DB db(dir(), kSmall, kNoCompact);
    const int n = 500;
    for (int i = 0; i < n; ++i) db.put(key(i), val(i));
    // Deliberately no wait_for_idle: some data is still in the active or sealed
    // memtable, or an in-flight flush. Every written key must still be found.
    for (int i = 0; i < n; ++i)
        EXPECT_EQ(db.get(key(i)), std::optional<std::string>(val(i))) << "i=" << i;
}

TEST_F(BgFlushTest, RecoversLeftoverFlushingWal) {
    // Simulate a crash after a seal but before the flush finished: both a
    // wal-flushing.log (older data) and a wal.log (newer) are present at open.
    fs::create_directories(dir());
    {
        Wal w((dir_ / "wal-flushing.log").string(), /*truncate=*/true);
        w.append(Record{Op::Put, "a", "1"});
        w.append(Record{Op::Put, "b", "2"});
        w.sync();
    }
    {
        Wal w((dir_ / "wal.log").string(), /*truncate=*/true);
        w.append(Record{Op::Put, "b", "3"});  // newer overwrite of b
        w.append(Record{Op::Put, "c", "4"});
        w.sync();
    }
    DB db(dir(), kSmall, kNoCompact);
    EXPECT_EQ(db.get("a"), std::optional<std::string>("1"));
    EXPECT_EQ(db.get("b"), std::optional<std::string>("3"));  // active WAL wins
    EXPECT_EQ(db.get("c"), std::optional<std::string>("4"));
    EXPECT_FALSE(fs::exists((dir_ / "wal-flushing.log").string()));  // consumed
}

TEST_F(BgFlushTest, FlushedDataSurvivesReopen) {
    {
        DB db(dir(), kSmall, kNoCompact);
        for (int i = 0; i < 300; ++i) db.put(key(i), val(i));
        db.wait_for_idle();
    }
    DB db(dir(), kSmall, kNoCompact);  // reopen
    for (int i = 0; i < 300; ++i)
        EXPECT_EQ(db.get(key(i)), std::optional<std::string>(val(i))) << "i=" << i;
}

TEST_F(BgFlushTest, ConcurrentReadsDuringBackgroundFlush) {
    DB db(dir(), kSmall, kNoCompact);
    const int base = 100;
    for (int i = 0; i < base; ++i) db.put(key(i), val(i));  // stable keys readers check
    db.wait_for_idle();

    std::atomic<bool> stop{false};
    std::atomic<bool> mismatch{false};
    std::vector<std::thread> readers;
    for (int t = 0; t < 3; ++t) {
        readers.emplace_back([&] {
            while (!stop.load()) {
                for (int i = 0; i < base; ++i) {
                    auto v = db.get(key(i));
                    if (!v || *v != val(i)) mismatch.store(true);
                }
            }
        });
    }
    // Add many new keys: forces seals + background flushes under the readers.
    for (int i = base; i < base + 800; ++i) db.put(key(i), val(i));
    db.wait_for_idle();
    stop.store(true);
    for (auto& th : readers) th.join();

    EXPECT_FALSE(mismatch.load());
    for (int i = 0; i < base + 800; ++i)
        EXPECT_EQ(db.get(key(i)), std::optional<std::string>(val(i))) << "i=" << i;
}
