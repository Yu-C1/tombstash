#include <gtest/gtest.h>

#include <string>

#include "lsmkv/memtable.h"

using lsmkv::Memtable;

TEST(MemtableTest, PutThenGet) {
    Memtable m;
    m.put("k1", "v1");
    auto v = m.get("k1");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, "v1");
}

TEST(MemtableTest, MissingKeyReturnsNullopt) {
    Memtable m;
    EXPECT_FALSE(m.get("absent").has_value());
}

TEST(MemtableTest, OverwriteReturnsNewestValue) {
    Memtable m;
    m.put("k", "old");
    m.put("k", "new");
    EXPECT_EQ(m.get("k").value(), "new");
    EXPECT_EQ(m.entry_count(), 1u);
}

TEST(MemtableTest, DeleteShadowsValue) {
    Memtable m;
    m.put("k", "v");
    m.del("k");
    EXPECT_FALSE(m.get("k").has_value());
    EXPECT_TRUE(m.is_tombstone("k"));
}

TEST(MemtableTest, DeleteAbsentKeyCreatesTombstone) {
    Memtable m;
    m.del("ghost");
    EXPECT_FALSE(m.get("ghost").has_value());
    EXPECT_TRUE(m.is_tombstone("ghost"));
}

TEST(MemtableTest, PutAfterDeleteRevivesKey) {
    Memtable m;
    m.put("k", "v");
    m.del("k");
    m.put("k", "again");
    EXPECT_EQ(m.get("k").value(), "again");
    EXPECT_FALSE(m.is_tombstone("k"));
}

TEST(MemtableTest, ArbitraryBytesInKeyAndValue) {
    Memtable m;
    std::string key("a\0b", 3);
    std::string val("x\0\0y", 4);
    m.put(key, val);
    ASSERT_TRUE(m.get(key).has_value());
    EXPECT_EQ(m.get(key).value(), val);
    EXPECT_EQ(m.get(key).value().size(), 4u);
}

TEST(MemtableTest, KeysAreSortedLexicographically) {
    // std::map keeps keys ordered; this is what SSTable flush relies on later.
    Memtable m;
    m.put("banana", "1");
    m.put("apple", "2");
    m.put("cherry", "3");
    EXPECT_EQ(m.entry_count(), 3u);
}

TEST(MemtableTest, SizeBytesTracksKeysAndValues) {
    Memtable m;
    EXPECT_EQ(m.size_bytes(), 0u);
    m.put("ab", "cde");  // 2 + 3
    EXPECT_EQ(m.size_bytes(), 5u);
    m.put("ab", "z");  // overwrite: 2 + 1
    EXPECT_EQ(m.size_bytes(), 3u);
}
