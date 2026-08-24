# tombstash

A persistent key-value store in C++ built on a Log-Structured Merge (LSM) tree —
write-ahead log, SSTables with a sparse block index and Bloom filters, size-tiered
compaction, and lock-free concurrent reads. This is the design behind RocksDB and
LevelDB.

## The problem it solves

A naive on-disk store keeps keys sorted in a file and updates in place, forcing a
random disk write (and on SSDs, write amplification from erase-before-write) on
every insert. An LSM tree only ever appends — to a log, then to whole immutable
files — turning random writes into sequential ones, which is fast for
write-heavy workloads. The cost it trades for that is read and space
amplification, which the sparse index, Bloom filters, and compaction keep in check.

## Architecture

```
     put(k,v) / del(k)                         get(k)
          |                                       |
          v                                       v
   +--------------+                        check memtable  --hit--> value
   | 1. WAL append|  (fsync = durable)           |
   +--------------+                          (miss / not tombstone)
          |                                       |
          v                                 for each SSTable, newest -> oldest:
   +--------------+                            Bloom filter says "no"? skip file
   | 2. memtable  |  (std::map, in RAM)        else sparse index -> block,
   +--------------+                            read that block, scan it
          | (when full)                            |
          v                                        v
   flush -> new SSTable  ----- background ---> size-tiered compaction merges
   (sorted, immutable)        compaction       similar-size SSTables, keeps the
                                                newest value per key, drops
                                                shadowed values + tombstones
```

- **Write path:** append to WAL → `fsync` → insert into memtable → return. WAL
  first: a crash after the WAL write is recoverable by replay; a crash before it
  means the caller never saw success, so nothing is lost.
- **Read path:** memtable first (newest), then SSTables newest → oldest. A Bloom
  filter per SSTable skips files that cannot hold the key; a sparse index locates
  the one ~4 KiB block that could, which is then read and scanned. First match
  wins; a tombstone counts as not-found.

## SSTable layout

```
[data region]  records grouped into ~4 KiB blocks (sorted): [op][klen][key][vlen][val]...
[index block]  SPARSE: one entry per block -> [first_key][block offset][block length]
[bloom block]  serialized Bloom filter (all of this file's keys)
[footer]       [data_size][index_size][bloom_size][num_records][magic]
```

The index is **sparse** — one entry per block, not per key — so it stays small
enough to scale to far more keys than a dense per-key index would fit in RAM. A
lookup binary-searches the sparse index to a block, then reads and scans that
block. The Bloom filter is what makes this cheap for absent keys: it skips the
block read entirely when the key cannot be in the file.

## Build and run

Requires a Linux toolchain (or WSL): g++ 13+, CMake 3.16+, GoogleTest.

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure     # 46 tests
```

CLI:

```bash
./build/lsmkv /tmp/mydb put foo bar
./build/lsmkv /tmp/mydb get foo      # -> bar
./build/lsmkv /tmp/mydb del foo
```

Benchmark (build Release, or the numbers are meaningless):

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j
./build-release/benchmark 1000000 bench/results.svg
```

Observability dashboard — a live view of the engine's internals in the browser:

```bash
./build/stats_server 8080 dashboard/web
# open http://localhost:8080  and click "Run workload"
```

A dependency-free C++ HTTP server exposes the engine's stats as JSON at `/stats`;
the page polls it and renders the memtable filling, SSTables appearing on flush
and merging on compaction, live read/write throughput, and the counters. The
engine stays pure — the dashboard only reads through the DB's public accessors.

ThreadSanitizer build (validates the concurrent reads + background compaction):

```bash
cmake -S . -B build-tsan -DLSMKV_TSAN=ON
cmake --build build-tsan -j
setarch "$(uname -m)" -R ./build-tsan/unit_tests
```

`setarch -R` disables ASLR for the run: on newer kernels (e.g. WSL2) the default
high-entropy address layout trips ThreadSanitizer's fixed mappings.

## Design decisions and tradeoffs

- **LSM vs B-tree.** A B-tree does one `O(log n)` lookup but updates in place
  (random writes). The LSM makes writes sequential and `O(log k)` in the memtable
  size `k`, at the cost of reads possibly checking several SSTables. The sparse
  index, Bloom filters, and compaction bound that cost.
- **Sparse index + Bloom filter.** A dense per-key index (every key + offset in
  RAM) is simplest but its memory grows with the total key count — at a billion
  keys that is tens of GB. A sparse index stores one entry per ~4 KiB block
  (~100× less memory) but can only narrow a key to a block, not confirm it — so a
  lookup must read the block. The Bloom filter cancels exactly the wasteful block
  reads that creates: it answers "definitely not in this file" in RAM, so absent
  keys never touch the disk. Sparse index + Bloom recreate the dense index's fast
  "skip absent keys" behavior at a fraction of the RAM.
- **Size-tiered vs leveled compaction.** Size-tiered: merge several similar-size
  SSTables into one larger table. Simpler and lower write amplification than
  leveled, at the cost of higher read/space amplification.
- **Tombstone dropping.** A delete writes a tombstone that shadows older data. It
  is dropped during compaction only when the merge set reaches the oldest table —
  otherwise an un-merged older file could resurrect the key. The merged table
  takes the newest input's age slot, keeping newest-wins correct across
  non-adjacent size tiers.
- **Concurrency: snapshot reads.** A single `std::shared_mutex`. A read copies the
  list of SSTable `shared_ptr`s under a brief shared lock, then reads with no lock
  held. Background compaction does its slow merge and file write unlocked, and
  only swaps the list under a brief exclusive lock. A merged-away file is unlinked
  when the last reader releases it, so a reader never has a file pulled out from
  under it. Verified race-free under ThreadSanitizer.
- **Group commit.** `put(..., sync=false)` batches writes; one `sync()` fsyncs the
  batch, amortizing fsync latency against a window of reduced durability.

## Benchmarks

1M keys, 100-byte values, WSL2 (Ubuntu 24.04) on ext4, Release (`-O2`). Single
run — order-of-magnitude, not precise.

![throughput](bench/results.svg)

| Operation | Throughput | Notes |
|---|---:|---|
| Sequential writes, group commit | ~198,000 ops/sec | one fsync per 1000 writes |
| Sequential writes, fsync per write | ~420 ops/sec | fully durable each write |
| Missing-key reads, Bloom on | ~675,000 ops/sec | filter skips the files |
| Missing-key reads, Bloom off | ~27,000 ops/sec | reads a block in every SSTable |
| Present-key reads, Bloom on | ~53,000 ops/sec | |
| Present-key reads, Bloom off | ~213,000 ops/sec | |

Write amplification: **3.28×** total — WAL 1.08× + flush 1.10× + compaction 1.10×.
Bloom filter: **0.47%** false-positive rate (1% target).

### What the Bloom filter buys (block reads, 200k reads each)

| | Bloom OFF | Bloom ON |
|---|---:|---:|
| Missing-key data-block reads | **5,400,000** | **25,450** |

This is the point of the sparse index + Bloom design. Without the filter, a
missing key forces a block read in **every** SSTable (27 files → 5.4M reads); with
it, only the 0.47% false positives touch a block (25,450). That is a **212×**
reduction in disk reads — **24.6× throughput** even with everything in the page
cache. On a dataset larger than RAM, each avoided block read is a real disk seek,
so the gap is far larger. The block-read count is reported because it proves the
filter's value independent of caching.

**Honest nuance:** present-key reads don't benefit here — the keys were written in
sorted order, so each SSTable holds a contiguous key range and the sparse index
already prunes files whose range can't cover the key. Bloom's probe is then pure
overhead. The filter wins precisely where the range check can't prune: missing
keys, and randomly-distributed present keys.

## What I learned / future work

- Durability is fsync latency: group commit is ~480× faster than a fsync per write
  because it shares one fsync across a batch.
- A Bloom filter is only useful once the index is sparse. With a dense per-key
  index the index already answers "is it here?", so the filter is redundant.
  Switching to a sparse index made the filter matter — 212× fewer block reads on
  missing keys — and measuring block reads (not just wall-clock) proved it without
  fighting the page cache.
- Future: block compression, a streaming SSTable builder (merge without holding a
  whole table in memory), leveled compaction with a comparison, range scans, and
  key-value separation (WiscKey) for large values.

Built on Linux; `fsync` provides durability. On Windows, develop inside WSL.
