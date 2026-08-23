#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <string>

#include "lsmkv/record.h"
#include "lsmkv/wal.h"

namespace fs = std::filesystem;
using lsmkv::Op;
using lsmkv::Record;
using lsmkv::Wal;

namespace {

// Each test gets its own temp directory named after the test case.
class WalTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_wal_" +
                std::string(::testing::UnitTest::GetInstance()
                                ->current_test_info()
                                ->name()));
        fs::remove_all(dir_);
        fs::create_directories(dir_);
        path_ = (dir_ / "wal.log").string();
    }
    void TearDown() override { fs::remove_all(dir_); }

    fs::path dir_;
    std::string path_;
};

}  // namespace

TEST_F(WalTest, AppendThenReplayRoundTrips) {
    {
        Wal wal(path_);
        wal.append(Record{Op::Put, "k1", "v1"});
        wal.append(Record{Op::Put, "k2", "v2"});
        wal.append(Record{Op::Delete, "k1", ""});
    }
    auto recs = Wal::replay(path_);
    ASSERT_EQ(recs.size(), 3u);
    EXPECT_EQ(recs[0].op, Op::Put);
    EXPECT_EQ(recs[0].key, "k1");
    EXPECT_EQ(recs[0].value, "v1");
    EXPECT_EQ(recs[1].key, "k2");
    EXPECT_EQ(recs[2].op, Op::Delete);
    EXPECT_EQ(recs[2].key, "k1");
    EXPECT_TRUE(recs[2].value.empty());
}

TEST_F(WalTest, ReplayMissingFileIsEmpty) {
    auto recs = Wal::replay((dir_ / "does_not_exist.log").string());
    EXPECT_TRUE(recs.empty());
}

TEST_F(WalTest, AppendsAcrossReopen) {
    { Wal wal(path_); wal.append(Record{Op::Put, "a", "1"}); }
    { Wal wal(path_); wal.append(Record{Op::Put, "b", "2"}); }
    auto recs = Wal::replay(path_);
    ASSERT_EQ(recs.size(), 2u);
    EXPECT_EQ(recs[0].key, "a");
    EXPECT_EQ(recs[1].key, "b");
}

TEST_F(WalTest, PreservesArbitraryBytes) {
    std::string key("a\0b", 3);
    std::string val("\0\1\2", 3);
    { Wal wal(path_); wal.append(Record{Op::Put, key, val}); }
    auto recs = Wal::replay(path_);
    ASSERT_EQ(recs.size(), 1u);
    EXPECT_EQ(recs[0].key, key);
    EXPECT_EQ(recs[0].value, val);
}

TEST_F(WalTest, TornTrailingRecordIsIgnored) {
    { Wal wal(path_); wal.append(Record{Op::Put, "good", "value"}); }
    // Simulate a crash mid-write: op byte plus a truncated key length.
    {
        std::FILE* f = std::fopen(path_.c_str(), "ab");
        ASSERT_NE(f, nullptr);
        const char junk[3] = {0x00, 0x05, 0x00};
        std::fwrite(junk, 1, sizeof(junk), f);
        std::fclose(f);
    }
    auto recs = Wal::replay(path_);
    ASSERT_EQ(recs.size(), 1u);  // the torn tail is dropped, the good record stays
    EXPECT_EQ(recs[0].key, "good");
    EXPECT_EQ(recs[0].value, "value");
}
