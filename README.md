# tombstash

A persistent key-value store in C++ built on a Log-Structured Merge (LSM) tree —
write-ahead log, SSTables, size-tiered compaction, Bloom filters, and lock-free
concurrent reads. This is the design behind RocksDB and LevelDB.

## The problem it solves

A naive on-disk store keeps keys sorted in a file and updates in place, forcing a
random disk write (and on SSDs, write amplification from erase-before-write) on
every insert. An LSM tree only ever appends — to a log, then to whole immutable
files — turning random writes into sequential ones, which is fast for
write-heavy workloads. The cost it trades for that is read and space
amplification, which Bloom filters and compaction keep in check.

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
   +--------------+                             Bloom filter says "no"? skip
   | 2. memtable  |  (std::map, in RAM)         else binary-search the index,
   +--------------+                             read the record; first match wins
          | (when full)                             |
          v                                         v
   flush -> new SSTable  ----- background ----> size-tiered compaction merges
   (sorted, immutable)        compaction        similar-size SSTables, keeps the
                                                 newest value per key, drops
                                                 shadowed values + tombstones
```

- **Write path:** append to WAL → `fsync` → insert into memtable → return. WAL
  first: a crash after the WAL write is recoverable by replay; a crash before it
  means the caller never saw success, so nothing is lost.
- **Read path:** memtable first (newest), then SSTables newest → oldest. A Bloom
  filter per SSTable skips files that cannot hold the key. First match wins; a
  tombstone counts as not-found.

## Build and run

Requires a Linux toolchain (or WSL): g++ 13+, CMake 3.16+, GoogleTest.

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure     # 44 tests
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
  size `k`, at the cost of reads possibly checking several SSTables. Bloom filters
  and compaction bound that cost.
- **Size-tiered vs leveled compaction.** This uses size-tiered: merge several
  similar-size SSTables into one larger table. It is simpler and has lower write
  amplification than leveled, at the cost of higher read/space amplification.
- **Tombstone dropping.** A delete writes a tombstone that shadows older data. A
  tombstone is dropped during compaction only when the merge set reaches the
  oldest table — otherwise an un-merged older file could resurrect the key. The
  merged table takes the newest input's age slot, keeping newest-wins correct
  even across non-adjacent size tiers.
- **Concurrency: snapshot reads.** A single `std::shared_mutex`. A read copies the
  list of SSTable `shared_ptr`s under a brief shared lock, then reads with no lock
  held. The background compaction thread does its slow merge and file write
  unlocked, and only swaps the list under a brief exclusive lock. A merged-away
  file is unlinked when the last reader releases it, so a reader never has a file
  pulled out from under it. Verified race-free under ThreadSanitizer.
- **Group commit.** `put(..., sync=false)` batches writes; one `sync()` fsyncs the
  batch. Amortizes fsync latency (see the benchmark — ~480× on this machine)
  against a window of reduced durability.

## Benchmarks

1M keys, 100-byte values, WSL2 (Ubuntu 24.04) on ext4, Release (`-O2`). Single
run — treat as order-of-magnitude, not precise; timing on a shared machine
varies run to run.

![throughput](bench/results.svg)

| Operation | Throughput | Notes |
|---|---:|---|
| Sequential writes, group commit | ~191,000 ops/sec | one fsync per 1000 writes |
| Sequential writes, fsync per write | ~400 ops/sec | fully durable each write |
| Random reads, present key, Bloom on | ~267,000 ops/sec | |
| Random reads, present key, Bloom off | ~290,000 ops/sec | |
| Missing-key reads, Bloom on | ~807,000 ops/sec | worst case for reads |
| Missing-key reads, Bloom off | ~647,000 ops/sec | |

Write amplification: **3.70×** total — WAL 1.08× + flush 1.31× + compaction 1.31×.
Bloom filter: **0.47%** false-positive rate (1% target), 99.5% of probes on
missing keys correctly skip the SSTable.

**What the numbers show (and a caveat):** group commit is ~480× faster than
per-write fsync — fsync latency dominates durable writes. Bloom's benefit here is
modest (~1.2× on missing keys) because the entire 141 MB dataset fits in the OS
page cache: there are no disk seeks to save, and a Bloom probe (hashing + random
bit lookups) is not much cheaper than binary-searching an in-memory index. Bloom
filters earn their 10–100× on datasets larger than RAM, where a probe avoids a
real disk read. This benchmark is CPU-bound, not I/O-bound, so it understates
them. (The workload also has no overwrites or deletes, so compaction reduces the
file count 27 → 1 but reclaims no bytes; a workload with updates would.)

## What I learned / future work

- Durability is fsync latency: the difference between a correct log and a fast one
  is entirely in when you call fsync. Group commit is the standard answer.
- Bloom filters are a disk-I/O optimization; their value doesn't show in a
  fully-cached benchmark. Measuring that honestly mattered more than a big number.
- Future: streaming SSTable builder (merge without holding all records in memory),
  a sparse block index instead of a dense per-key index, leveled compaction as an
  alternative, range scans, and key-value separation (WiscKey) for large values.

Built on Linux; `fsync` provides durability. On Windows, develop inside WSL.
