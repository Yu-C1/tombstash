#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "lsmkv/db.h"

namespace fs = std::filesystem;
using lsmkv::DB;

namespace {

class DBTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_db_" +
                std::string(::testing::UnitTest::GetInstance()
                                ->current_test_info()
                                ->name()));
        fs::remove_all(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }
    fs::path dir_;
};

}  // namespace

TEST_F(DBTest, PutGetDelete) {
    DB db(dir_.string());
    db.put("a", "1");
    db.put("b", "2");
    EXPECT_EQ(db.get("a").value(), "1");
    EXPECT_EQ(db.get("b").value(), "2");
    db.del("a");
    EXPECT_FALSE(db.get("a").has_value());
    EXPECT_EQ(db.get("b").value(), "2");
}

// Recovery: reopening a DB rebuilds state from the WAL alone. This stands in for
// a crash, since every write was fsync'd before the process exited.
TEST_F(DBTest, RecoversAcknowledgedWritesAfterReopen) {
    {
        DB db(dir_.string());
        db.put("k1", "v1");
        db.put("k2", "v2");
        db.del("k1");
    }
    DB db2(dir_.string());  // fresh in-memory state, rebuilt from the WAL
    EXPECT_FALSE(db2.get("k1").has_value());  // tombstone recovered
    ASSERT_TRUE(db2.get("k2").has_value());
    EXPECT_EQ(db2.get("k2").value(), "v2");
}

TEST_F(DBTest, LatestValueWinsAfterRecovery) {
    {
        DB db(dir_.string());
        db.put("k", "first");
        db.put("k", "second");
    }
    DB db2(dir_.string());
    EXPECT_EQ(db2.get("k").value(), "second");
}

TEST_F(DBTest, EmptyDirStartsEmpty) {
    DB db(dir_.string());
    EXPECT_EQ(db.entry_count(), 0u);
    EXPECT_FALSE(db.get("anything").has_value());
}
