#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "lsmkv/db.h"

namespace fs = std::filesystem;
using lsmkv::DB;

namespace {

class DBSSTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_dbsst_" +
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

TEST_F(DBSSTableTest, FlushMovesMemtableToSSTable) {
    DB db(dir());
    db.put("a", "1");
    db.put("b", "2");
    db.flush();

    EXPECT_EQ(db.sstable_count(), 1u);
    EXPECT_EQ(db.memtable_entry_count(), 0u);
    // Reads now come from the SSTable, not the memtable.
    EXPECT_EQ(db.get("a").value(), "1");
    EXPECT_EQ(db.get("b").value(), "2");
    EXPECT_FALSE(db.get("missing").has_value());
}

TEST_F(DBSSTableTest, AutoFlushOnThreshold) {
    DB db(dir(), /*memtable_threshold=*/64);  // tiny, forces flushes
    for (int i = 0; i < 200; ++i) {
        db.put("key" + std::to_string(i), "value" + std::to_string(i));
    }
    EXPECT_GT(db.sstable_count(), 0u);
    // All keys still readable across whichever tier they landed in.
    for (int i = 0; i < 200; ++i) {
        EXPECT_EQ(db.get("key" + std::to_string(i)).value(),
                  "value" + std::to_string(i));
    }
}

TEST_F(DBSSTableTest, NewestValueWinsAcrossSSTables) {
    DB db(dir());
    db.put("k", "old");
    db.flush();
    db.put("k", "new");
    db.flush();
    EXPECT_EQ(db.sstable_count(), 2u);
    EXPECT_EQ(db.get("k").value(), "new");  // newer SSTable shadows older
}

TEST_F(DBSSTableTest, DeleteInNewerTierShadowsFlushedValue) {
    DB db(dir());
    db.put("k", "v");
    db.flush();               // value now in an SSTable
    EXPECT_EQ(db.get("k").value(), "v");
    db.del("k");              // tombstone in the memtable
    EXPECT_FALSE(db.get("k").has_value());
    db.flush();               // tombstone now in a newer SSTable
    EXPECT_FALSE(db.get("k").has_value());
}

TEST_F(DBSSTableTest, RecoveryLoadsSSTablesAndReplaysWAL) {
    {
        DB db(dir());
        db.put("flushed1", "a");
        db.put("flushed2", "b");
        db.flush();               // these live in an SSTable
        db.put("in_wal", "c");    // this only in the WAL (not yet flushed)
        db.del("flushed1");       // tombstone only in the WAL
    }
    // Reopen: rebuild from SSTables + WAL replay.
    DB db2(dir());
    EXPECT_GE(db2.sstable_count(), 1u);
    EXPECT_FALSE(db2.get("flushed1").has_value());     // WAL tombstone wins
    EXPECT_EQ(db2.get("flushed2").value(), "b");       // from SSTable
    EXPECT_EQ(db2.get("in_wal").value(), "c");         // from replayed WAL
}

TEST_F(DBSSTableTest, ManyKeysSurviveReopenWithMultipleFlushes) {
    {
        DB db(dir(), /*memtable_threshold=*/128);
        for (int i = 0; i < 500; ++i) {
            db.put("k" + std::to_string(i), "v" + std::to_string(i));
        }
    }
    DB db2(dir(), /*memtable_threshold=*/128);
    for (int i = 0; i < 500; ++i) {
        ASSERT_TRUE(db2.get("k" + std::to_string(i)).has_value())
            << "lost k" << i;
        EXPECT_EQ(db2.get("k" + std::to_string(i)).value(),
                  "v" + std::to_string(i));
    }
}
