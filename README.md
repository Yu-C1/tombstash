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
  means the caller never saw success, so nothing is lost. When the memtable fills,
  the write only *seals* it (a pointer swap + WAL rotation); a background thread
  builds the SSTable, so writes never stall on a flush.
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

A large value (past the separation threshold) is not stored in the block at all —
it lives in a separate append-only value log (`vlog.log`), and the record holds a
pointer to it. This keeps values out of compaction; see the key-value separation
note under Design decisions.

## Build and run

Requires a Linux toolchain (or WSL): g++ 13+, CMake 3.16+, GoogleTest.

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure     # 86 tests
```

CLI:

```bash
./build/lsmkv /tmp/mydb put foo bar
./build/lsmkv /tmp/mydb get foo          # -> bar
./build/lsmkv /tmp/mydb del foo
./build/lsmkv /tmp/mydb scan aaa zzz     # sorted key<TAB>value in [aaa, zzz)
./build/lsmkv /tmp/mydb gc               # reclaim dead value-log space
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
- **Size-tiered vs leveled compaction.** Both are implemented and selectable
  (`DB::Compaction::SizeTiered`, the default, or `Leveled`). Size-tiered merges
  several similar-size SSTables into one larger table — simpler, lower write
  amplification, higher read/space. Leveled keeps each level past L0 as
  non-overlapping sorted runs split into ~target-size files: a key lives in at
  most one file per level, so reads and space stay tight, at the cost of higher
  write amplification (data is rewritten as it sinks level by level). A flush
  lands in L0; when L0 fills it merges into L1; when a level exceeds its byte
  budget one of its files merges into the overlapping files of the next level,
  preserving non-overlap. The benchmark below measures the tradeoff. A table's
  level is recorded in the manifest, so the structure survives a reopen.
- **Age-contiguous merges (size-tiered).** A size-tiered merge only ever combines
  tables adjacent in the newest→oldest list. Grouping purely by size can skip a
  differently-sized table that sits between them in age (say a tiny table from a
  single-key overwrite between two big ones); merging across that gap would keep
  the older value and, placed at the newest input's age, shadow the newer one.
  Restricting a pick to a consecutive run rules that out.
- **Crash safety: the manifest.** Which SSTables make up the store, and in what
  age order, is recorded in a `MANIFEST` file — not inferred from filenames. A
  filename's sequence number can't encode age after a compaction, whose output
  gets a fresh high number but holds older data; sorting by it on reload would
  order tables wrong and surface a stale value. The manifest stores the age order
  explicitly, so a reopen reconstructs exactly the in-memory order. It is written
  atomically (temp + fsync + rename + dir fsync) as the single commit point of
  every flush, compaction, and GC: the new file set is durable in the manifest
  *before* any superseded file is unlinked, so a file the live manifest names
  always exists. On open, any sst/vlog file the manifest does not reference is a
  leftover from an interrupted operation and is deleted.
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
- **Concurrency: background flush.** A write that fills the memtable does not build
  the SSTable itself. Instead it *seals* the full memtable into an immutable slot,
  starts a fresh active memtable, and rotates the WAL — all a few pointer and
  rename operations under the lock — and a background thread builds the SSTable
  with no write lock held. So a flush never freezes the write path. A read checks
  three layers newest-first: active memtable, the sealed memtable being flushed,
  then the SSTables. Only one memtable seals at a time; a writer that fills the
  next one while a flush is still running waits (bounded memory, i.e. write
  backpressure). The sealed memtable's WAL is renamed aside as `wal-flushing.log`
  and dropped only once its SSTable is durable; a crash mid-flush is recovered by
  replaying it on the next open. Verified race-free under ThreadSanitizer with
  readers running across the flush.
- **Group commit.** `put(..., sync=false)` batches writes; one `sync()` fsyncs the
  batch, amortizing fsync latency against a window of reduced durability.
- **Range scan.** `scan(start, end)` returns the live key-value pairs in
  `[start, end)` in sorted order. It seeks each source (memtable + every SSTable)
  to `start` — a binary search on each sparse index — then k-way merges them
  forward, newest value winning and tombstones excluded. The merge is entirely at
  read time: nothing is written, and Bloom filters do not apply (a range spans
  keys, not one key).
- **Key-value separation (WiscKey).** Most of an LSM's write amplification is
  values being copied forward on every compaction. A value at least
  `value_sep_threshold` bytes (default 128) is instead appended to a separate
  value log, and the SSTable stores only a fixed-size pointer. Compaction then
  merges pointers, never re-touching the value — cutting the bytes compaction
  writes ~4× in the benchmark below. Small values stay inline so short-value point
  reads and scans keep their single-read fast path. The costs are real and
  deliberately measured: a point read does one extra positioned read to fetch the
  value, a range scan does one random value-log read per key (values are scattered
  in write order, not laid beside their keys), and the append-only log has no
  value log is fsync'd before the SSTable that points into it, so a pointer never
  dangles after a crash.
- **Value-log garbage collection.** The log is append-only, so overwrites and
  deletes leave dead values behind; `gc_value_log()` reclaims them. It is a *full*
  GC: merge every SSTable to the live record set (the merge already drops shadowed
  values and tombstones — so whatever it keeps is exactly what's live), copy those
  values into a fresh value-log generation while rewriting their pointers, build
  one new SSTable, then atomically swap in the new files and drop the old
  generation and tables. Correctness falls out of doing it single-threaded over a
  consistent snapshot: no live-pointer races, because the merge *is* the liveness
  check. Pointers name their generation, so a reader that snapshotted the old
  tables and old log (both captured together) keeps resolving against them until
  it releases — the old files are unlinked only then, exactly as obsolete SSTables
  are. The tradeoff is that it is stop-the-world and rewrites everything; the
  production answer is incremental *segmented* GC (pick the highest-garbage
  segment, relocate just its live values, refcount segments for reclaim), which
  trades that pause for real concurrency hazards — GC racing a concurrent
  overwrite, and reclaiming a segment a reader still points into.

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

### Key-value separation (WiscKey)

200k keys, 100-byte values, background compaction on, values separated at a
64-byte threshold so the 100-byte values move to the value log.

| | Inline | Separated |
|---|---:|---:|
| Flush bytes | 23.7 MiB | 6.0 MiB |
| Compaction bytes | 17.6 MiB | **4.5 MiB** |
| Value-log bytes | 0 | 23.1 MiB |
| Total write amplification | 2.99× | 2.64× |
| On-disk, SSTables | 23.7 MiB | 6.0 MiB |
| On-disk, total incl. value log | 23.7 MiB | 29.1 MiB |

The point is the **compaction bytes: 17.6 → 4.5 MiB, a 3.95× cut.** With values in
the log, a merge rewrites 6-byte-ish pointers instead of 100-byte values, so
repeated compaction stops re-copying the data. SSTables shrink ~4×, which also
means more of them fit in cache.

**Honest nuance:** total write amplification barely moves (2.99× → 2.64×) at this
size — the WAL still logs every full value for durability, and 200k keys form only
a few size tiers, so compaction wasn't the dominant cost to begin with. The
compaction-bytes ratio is what separation actually targets, and it widens with
dataset size and overwrite rate. Note too that total on-disk bytes *grow* (29.1 vs
23.7 MiB): for unique keys the pointers are pure overhead, and with no garbage
collector the log never reclaims dead values. Separation trades space and read
cost for write cost — the right trade only for large, write-heavy, point-lookup
values, which is exactly why it is threshold-gated.

### Value-log garbage collection

50k keys overwritten 4 times (so the log holds 4 copies of each value, only the
newest live), then `gc_value_log()`.

| Value log on disk | Bytes |
|---|---:|
| Before GC | 23.1 MiB |
| After GC | 5.8 MiB |
| **Reclaimed** | **4.00×** |

Four copies collapse to one — GC keeps only the live value per key and drops the
old generation. Verified race-free under ThreadSanitizer with reader threads
running throughout the generation swap.

### Size-tiered vs leveled compaction

Same workload — 50k keys, each written twice (so compaction has real work) —
under each strategy, background compaction on.

| | Size-tiered | Leveled |
|---|---:|---:|
| Write amplification | 4.01× | **6.57×** |
| On-disk size (space) | 11.8 MiB | **6.5 MiB** |
| Block reads / lookup (read) | 1.07 | 1.05 |

The classic trade, in the store's own numbers: **leveled writes ~1.6× more** (it
rewrites data as it sinks level by level) **to hold ~1.8× less on disk** (a key
lives in one file per level, not duplicated across tiers). Read amplification is
close here — Bloom filters plus the sparse-index range check already prune a
lookup to about one block under either strategy — so the honest headline is the
**write-vs-space** trade: pick leveled when reads and space matter, size-tiered
when writes dominate.

## What I learned / future work

- Durability is fsync latency: group commit is ~480× faster than a fsync per write
  because it shares one fsync across a batch.
- A Bloom filter is only useful once the index is sparse. With a dense per-key
  index the index already answers "is it here?", so the filter is redundant.
  Switching to a sparse index made the filter matter — 212× fewer block reads on
  missing keys — and measuring block reads (not just wall-clock) proved it without
  fighting the page cache.
- Key-value separation moves write cost off the compaction path but doesn't come
  free — it adds a read per lookup, turns range scans into random I/O, and needs a
  garbage collector to reclaim the log. Measuring compaction bytes separately from
  total write amplification showed where the win actually lands.
- A merge that already keeps newest-per-key and drops tombstones *is* a liveness
  oracle — full value-log GC reused it directly instead of writing a separate
  "is this value still referenced?" check. Reusing existing invariants beat adding
  new machinery. The hard part is purely concurrency, which the full-GC design
  sidesteps by being stop-the-world; generational pointers let a reader keep using
  the old log until it releases, so the swap needs no reader coordination.
- Moving the flush off the write path (seal + background thread) is the same
  immutability trick as snapshot reads: a sealed memtable never changes, so a
  reader and the flusher can touch it at once without locking. The subtlety is
  durability, not speed — the sealed memtable needs its own WAL until its SSTable
  lands, which meant rotating the log and handling a leftover one on recovery.
- A single sequence number can't do two jobs. Using it as both a unique filename
  and the age order breaks after a compaction, whose output is newer by number but
  older by data — reads went stale after a restart. Reasoning through that turned
  up a second, worse bug hiding behind it: size-tiered compaction grouped tables
  by size alone, so it could merge an age-non-contiguous set and resurrect an
  overwritten value *without any restart at all*. The lesson was to derive age
  from something that actually records it (a manifest) and to keep every merge
  age-contiguous — and that a reopen-consistency fuzz test is worth more than any
  number of hand-picked cases for catching this class of ordering bug.
- Building leveled compaction next to size-tiered made the amplification triangle
  concrete: the same code path (merge, then split the output into fixed-size
  files) produces a completely different write/space profile just by choosing
  which tables to merge. Measuring both with one harness beat reasoning about it.
- Future: incremental segmented value-log GC (reclaim without the stop-the-world
  pause), block compression, and a streaming SSTable builder (merge without
  holding a whole table in memory).

Built on Linux; `fsync` provides durability. On Windows, develop inside WSL.
