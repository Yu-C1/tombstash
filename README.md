# lsm-kv-store

A persistent key-value store in C++ using a Log-Structured Merge (LSM) tree —
write-ahead log, SSTables, size-tiered compaction, Bloom filters, and concurrent
reads. This is the design behind RocksDB and LevelDB: it turns random disk writes
into sequential ones, which is fast for write-heavy workloads.

## Status

- [x] Memtable — `put` / `get` / `del` over `std::map`, tombstones, byte accounting
- [x] Write-ahead log — length-prefixed records, `fsync` per append, replay on open
- [x] Durable DB — WAL + memtable, crash recovery via replay
- [x] SSTable flush — sorted immutable files (data / index / bloom / footer)
- [x] Bloom filters — per SSTable, sized by target FPR, double hashing
- [x] Read path — memtable → SSTables newest→oldest, Bloom skips, binary search
- [x] Recovery — load SSTables + replay WAL; atomic flush (tmp+rename+dir fsync)
- [x] 33 GoogleTest cases, all passing
- [ ] Size-tiered compaction (Week 3)
- [ ] Benchmarks + CLI (Week 4)

## Build and run

Requires a Linux toolchain (or WSL): g++ 13+, CMake 3.16+, GoogleTest.

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

ThreadSanitizer build (for the concurrency work in later weeks):

```bash
cmake -S . -B build-tsan -DLSMKV_TSAN=ON
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

## Design notes

- **Write path:** append to WAL → `fsync` → insert into memtable → return. WAL
  first, memtable second: a crash after the WAL write is recoverable by replay;
  a crash before it means the caller never saw success, so nothing is lost.
- **WAL record format:** `[op:1][key len:4 LE][key][value len:4 LE][value]`.
  A torn trailing record from a crash is ignored on replay — it was never
  acknowledged.
- **Concurrency:** stage 1 is a single `std::shared_mutex` (many readers, one
  writer). Immutable-memtable swap and snapshot reads come in later weeks.

Built on Linux; `fsync` provides durability. On Windows, develop inside WSL.
