#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "lsmkv/compaction.h"
#include "lsmkv/db.h"
#include "lsmkv/record.h"
#include "lsmkv/sstable.h"

namespace fs = std::filesystem;
using lsmkv::CompactionPick;
using lsmkv::DB;
using lsmkv::merge_records;
using lsmkv::Op;
using lsmkv::pick_compaction;
using lsmkv::Record;
using lsmkv::SSTable;

namespace {

class CompactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_comp_" +
                std::string(::testing::UnitTest::GetInstance()
                                ->current_test_info()
                                ->name()));
        fs::remove_all(dir_);
        fs::create_directories(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }

    // Build an SSTable from records (must be sorted by key) and open it.
    std::shared_ptr<SSTable> make_sst(int id, const std::vector<Record>& recs) {
        std::string p = (dir_ / ("in-" + std::to_string(id) + ".sst")).string();
        SSTable::build(p, recs);
        return std::make_shared<SSTable>(p);
    }

    std::string dir() const { return dir_.string(); }
    fs::path dir_;
};

}  // namespace

TEST_F(CompactionTest, MergeKeepsNewestValuePerKey) {
    auto newer = make_sst(1, {{Op::Put, "a", "1"}, {Op::Put, "c", "3"}});
    auto older = make_sst(2, {{Op::Put, "a", "9"}, {Op::Put, "b", "2"}});

    // inputs newest-first: newer shadows older for shared key "a".
    auto out = merge_records({newer, older}, /*drop_tombstones=*/false);
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0].key, "a");
    EXPECT_EQ(out[0].value, "1");  // from the newer table
    EXPECT_EQ(out[1].key, "b");
    EXPECT_EQ(out[1].value, "2");
    EXPECT_EQ(out[2].key, "c");
    EXPECT_EQ(out[2].value, "3");
}

TEST_F(CompactionTest, MergeDropsTombstonesOnlyWhenAsked) {
    auto newer = make_sst(1, {{Op::Delete, "gone", ""}});
    auto older = make_sst(2, {{Op::Put, "gone", "was-here"}, {Op::Put, "k", "v"}});

    auto kept = merge_records({newer, older}, /*drop_tombstones=*/false);
    ASSERT_EQ(kept.size(), 2u);
    EXPECT_EQ(kept[0].key, "gone");
    EXPECT_EQ(kept[0].op, Op::Delete);  // tombstone retained to shadow older data

    auto dropped = merge_records({newer, older}, /*drop_tombstones=*/true);
    ASSERT_EQ(dropped.size(), 1u);  // tombstone and shadowed value both gone
    EXPECT_EQ(dropped[0].key, "k");
}

TEST_F(CompactionTest, PickFiresOnMinMergeSimilarSizedTables) {
    std::vector<std::shared_ptr<SSTable>> tables;
    for (int i = 0; i < 4; ++i) {
        tables.push_back(make_sst(i, {{Op::Put, "k" + std::to_string(i), "v"}}));
    }
    auto pick = pick_compaction(tables, /*min_merge=*/4, /*size_ratio=*/2.0);
    ASSERT_TRUE(pick.has_value());
    EXPECT_EQ(pick->indices.size(), 4u);
    // Whole list chosen and it reaches the oldest -> tombstones may be dropped.
    EXPECT_TRUE(pick->drop_tombstones);

    // One fewer table: nothing to do.
    tables.pop_back();
    EXPECT_FALSE(pick_compaction(tables, 4, 2.0).has_value());
}

TEST_F(CompactionTest, BackgroundCompactionReducesTableCount) {
    // Large threshold so only explicit flush() creates tables -- deterministic.
    DB db(dir(), /*memtable_threshold=*/DB::kDefaultThreshold, /*min_merge=*/4);
    const int n = 32;
    for (int i = 0; i < n; ++i) {
        db.put("k" + std::to_string(i), "v" + std::to_string(i));
        db.flush();  // one tiny SSTable each
    }
    db.wait_for_idle();

    EXPECT_LT(db.sstable_count(), static_cast<std::size_t>(n));  // merged
    for (int i = 0; i < n; ++i) {
        ASSERT_TRUE(db.get("k" + std::to_string(i)).has_value()) << "lost k" << i;
        EXPECT_EQ(db.get("k" + std::to_string(i)).value(), "v" + std::to_string(i));
    }
}

TEST_F(CompactionTest, CompactionPreservesNewestAndDeletes) {
    DB db(dir(), DB::kDefaultThreshold, /*min_merge=*/4);
    db.put("x", "1"); db.flush();   // oldest
    db.put("x", "2"); db.flush();
    db.put("y", "a"); db.flush();
    db.del("y");      db.flush();   // newest; 4 tables -> compaction fires
    db.wait_for_idle();

    EXPECT_EQ(db.get("x").value(), "2");    // newest value survives
    EXPECT_FALSE(db.get("y").has_value());  // deletion survives
}

TEST_F(CompactionTest, StateSurvivesReopenAfterCompaction) {
    {
        DB db(dir(), DB::kDefaultThreshold, /*min_merge=*/4);
        for (int i = 0; i < 20; ++i) {
            db.put("k" + std::to_string(i), "v" + std::to_string(i));
            db.flush();
        }
        db.del("k5");
        db.flush();
        db.wait_for_idle();
    }
    // Reopen: only the compacted SSTable(s) remain on disk.
    DB db2(dir(), DB::kDefaultThreshold, /*min_merge=*/4);
    EXPECT_FALSE(db2.get("k5").has_value());
    for (int i = 0; i < 20; ++i) {
        if (i == 5) continue;
        ASSERT_TRUE(db2.get("k" + std::to_string(i)).has_value()) << "lost k" << i;
        EXPECT_EQ(db2.get("k" + std::to_string(i)).value(), "v" + std::to_string(i));
    }
}

// Reads must stay correct while compaction runs concurrently. Meaningful under
// ThreadSanitizer (-DLSMKV_TSAN=ON), which flags data races on the shared state.
TEST_F(CompactionTest, ConcurrentReadsDuringCompaction) {
    DB db(dir(), /*memtable_threshold=*/256, /*min_merge=*/4);
    const int keys = 100;
    for (int i = 0; i < keys; ++i) {
        db.put("k" + std::to_string(i), "v" + std::to_string(i));
    }

    std::atomic<bool> failed{false};
    std::atomic<bool> stop{false};
    std::vector<std::thread> readers;
    for (int r = 0; r < 4; ++r) {
        readers.emplace_back([&] {
            for (int it = 0; !stop.load() && it < 5000; ++it) {
                int i = it % keys;
                auto v = db.get("k" + std::to_string(i));
                // These keys are never deleted or overwritten: always present.
                if (!v || *v != "v" + std::to_string(i)) failed.store(true);
            }
        });
    }
    // Keep writing to drive flushes and compaction while readers run.
    for (int i = keys; i < keys + 400; ++i) {
        db.put("extra" + std::to_string(i), "e" + std::to_string(i));
    }
    db.wait_for_idle();
    stop.store(true);
    for (auto& t : readers) t.join();

    EXPECT_FALSE(failed.load());
}
