#include <gtest/gtest.h>

#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "lsmkv/db.h"
#include "lsmkv/vlog.h"

namespace fs = std::filesystem;
using lsmkv::DB;
using lsmkv::ValueLog;
using lsmkv::ValuePtr;

namespace {

// A DB dir under the temp directory, unique per test, removed around the test.
class KvSepTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("lsmkv_kvsep_" +
                std::string(::testing::UnitTest::GetInstance()
                                ->current_test_info()
                                ->name()));
        fs::remove_all(dir_);
    }
    void TearDown() override { fs::remove_all(dir_); }
    std::string dir() const { return dir_.string(); }
    fs::path dir_;

    // Big enough to separate at a small threshold; small stays inline.
    static std::string big(char c = 'x') { return std::string(200, c); }
    static constexpr std::size_t kSep = 64;  // separation threshold for these tests
    static constexpr std::size_t kNoCompact = 1000;
};

}  // namespace

// --- ValueLog in isolation ---------------------------------------------------

TEST_F(KvSepTest, ValueLogRoundTrip) {
    fs::create_directories(dir());
    std::string path = (dir_ / "vlog.log").string();
    ValueLog vlog(path, /*gen=*/0);
    ValuePtr a = vlog.append("k1", "hello");
    ValuePtr b = vlog.append("k2", "world!!");
    vlog.sync();
    EXPECT_EQ(vlog.read(a), "hello");
    EXPECT_EQ(vlog.read(b), "world!!");
    EXPECT_NE(a.offset, b.offset);
    EXPECT_EQ(a.len, 5u);
    EXPECT_EQ(b.len, 7u);
}

TEST_F(KvSepTest, ValueLogReopenReadsPriorEntries) {
    fs::create_directories(dir());
    std::string path = (dir_ / "vlog.log").string();
    ValuePtr a, b;
    {
        ValueLog vlog(path, /*gen=*/0);
        a = vlog.append("k1", "first");
        vlog.sync();
    }
    {
        ValueLog vlog(path, /*gen=*/0);  // reopen: appends continue after existing bytes
        b = vlog.append("k2", "second");
        vlog.sync();
        EXPECT_EQ(vlog.read(a), "first");   // prior entry still readable
        EXPECT_EQ(vlog.read(b), "second");
        EXPECT_GT(b.offset, a.offset);      // appended after the first
    }
}

// --- DB integration ----------------------------------------------------------

TEST_F(KvSepTest, LargeValuesSeparatedOnFlushSmallInline) {
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    db.put("big", big());
    db.put("small", "tiny");
    EXPECT_EQ(db.stats().vlog_bytes, 0u);  // nothing separated until flush
    db.flush();
    EXPECT_GT(db.stats().vlog_bytes, 0u);  // the big value went to the value log
    EXPECT_EQ(db.get("big"), std::optional<std::string>(big()));
    EXPECT_EQ(db.get("small"), std::optional<std::string>("tiny"));
}

TEST_F(KvSepTest, ThresholdIsInclusiveLowerBound) {
    {
        DB at(dir() + "_at", DB::kDefaultThreshold, kNoCompact,
              DB::kDefaultSizeRatio, /*sep=*/100);
        at.put("k", std::string(100, 'a'));  // size == threshold -> separated
        at.flush();
        EXPECT_GT(at.stats().vlog_bytes, 0u);
        EXPECT_EQ(at.get("k"), std::optional<std::string>(std::string(100, 'a')));
        fs::remove_all(dir() + "_at");
    }
    {
        DB below(dir() + "_below", DB::kDefaultThreshold, kNoCompact,
                 DB::kDefaultSizeRatio, /*sep=*/100);
        below.put("k", std::string(99, 'a'));  // size < threshold -> inline
        below.flush();
        EXPECT_EQ(below.stats().vlog_bytes, 0u);
        EXPECT_EQ(below.get("k"), std::optional<std::string>(std::string(99, 'a')));
        fs::remove_all(dir() + "_below");
    }
}

TEST_F(KvSepTest, SeparationDisabledWhenThresholdMax) {
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, SIZE_MAX);
    db.put("big", big());
    db.flush();
    EXPECT_EQ(db.stats().vlog_bytes, 0u);  // SIZE_MAX -> everything inline
    EXPECT_EQ(db.get("big"), std::optional<std::string>(big()));
}

TEST_F(KvSepTest, SeparatedValuesSurviveCompaction) {
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    db.put("a", big('a')); db.put("c", big('c')); db.flush();
    db.put("b", big('b')); db.put("d", big('d')); db.flush();
    std::uint64_t vlog_before = db.stats().vlog_bytes;

    db.compact_all();  // merge carries pointers through; must not touch the vlog

    EXPECT_EQ(db.stats().vlog_bytes, vlog_before);  // compaction wrote no vlog bytes
    EXPECT_EQ(db.sstable_count(), 1u);
    EXPECT_EQ(db.get("a"), std::optional<std::string>(big('a')));
    EXPECT_EQ(db.get("b"), std::optional<std::string>(big('b')));
    EXPECT_EQ(db.get("c"), std::optional<std::string>(big('c')));
    EXPECT_EQ(db.get("d"), std::optional<std::string>(big('d')));
}

TEST_F(KvSepTest, SeparatedValuesSurviveReopen) {
    {
        DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
        db.put("a", big('a')); db.put("b", big('b'));
        db.flush();
    }
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    EXPECT_EQ(db.get("a"), std::optional<std::string>(big('a')));  // read from vlog
    EXPECT_EQ(db.get("b"), std::optional<std::string>(big('b')));
}

TEST_F(KvSepTest, OverwriteOfSeparatedKeyNewestWins) {
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    db.put("k", big('o')); db.flush();   // old, separated in an SSTable
    db.put("k", big('n'));               // new, in the memtable
    EXPECT_EQ(db.get("k"), std::optional<std::string>(big('n')));
    db.flush();                          // new copy now also separated
    EXPECT_EQ(db.get("k"), std::optional<std::string>(big('n')));
    db.compact_all();                    // drops the shadowed old copy
    EXPECT_EQ(db.get("k"), std::optional<std::string>(big('n')));
}

TEST_F(KvSepTest, DeleteHidesSeparatedValue) {
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    db.put("k", big()); db.flush();
    db.del("k");
    EXPECT_EQ(db.get("k"), std::nullopt);
    EXPECT_TRUE(db.scan("a", "z").empty());  // deleted key absent from a scan
    db.flush();
    db.compact_all();
    EXPECT_EQ(db.get("k"), std::nullopt);
}

TEST_F(KvSepTest, ScanResolvesSeparatedValues) {
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    db.put("a", big('a')); db.put("c", big('c')); db.flush();  // separated, on disk
    db.put("b", big('b'));  // inline in the memtable (also >= threshold; separated on flush)
    auto r = db.scan("a", "z");
    ASSERT_EQ(r.size(), 3u);
    EXPECT_EQ(r[0], (std::pair<std::string, std::string>{"a", big('a')}));
    EXPECT_EQ(r[1], (std::pair<std::string, std::string>{"b", big('b')}));
    EXPECT_EQ(r[2], (std::pair<std::string, std::string>{"c", big('c')}));
}

TEST_F(KvSepTest, MixedInlineAndSeparatedInOneFlush) {
    DB db(dir(), DB::kDefaultThreshold, kNoCompact, DB::kDefaultSizeRatio, kSep);
    db.put("big1", big('1'));
    db.put("small1", "s1");
    db.put("big2", big('2'));
    db.put("small2", "s2");
    db.flush();
    EXPECT_EQ(db.get("big1"), std::optional<std::string>(big('1')));
    EXPECT_EQ(db.get("small1"), std::optional<std::string>("s1"));
    EXPECT_EQ(db.get("big2"), std::optional<std::string>(big('2')));
    EXPECT_EQ(db.get("small2"), std::optional<std::string>("s2"));
    EXPECT_EQ(db.get("absent"), std::nullopt);
}
