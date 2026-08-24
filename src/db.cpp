#include "lsmkv/db.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lsmkv/compaction.h"

namespace lsmkv {

namespace fs = std::filesystem;

namespace {

// Create the database directory if needed and return the WAL path inside it.
std::string prepare_wal_path(const std::string& dir) {
    fs::create_directories(dir);
    fs::path p(dir);
    p /= "wal.log";
    return p.string();
}

// SSTable file name for a sequence number, zero-padded so lexical and numeric
// order agree: sst-000042.sst.
std::string sst_name(std::uint64_t seq) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "sst-%06llu.sst",
                  static_cast<unsigned long long>(seq));
    return buf;
}

// Parse the sequence number from an SSTable file name, or nullopt if the name is
// not of the form sst-<digits>.sst.
std::optional<std::uint64_t> parse_seq(const std::string& name) {
    const std::string prefix = "sst-";
    const std::string suffix = ".sst";
    if (name.size() <= prefix.size() + suffix.size()) return std::nullopt;
    if (name.compare(0, prefix.size(), prefix) != 0) return std::nullopt;
    if (name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
        return std::nullopt;
    }
    std::string digits =
        name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
    if (digits.empty()) return std::nullopt;
    for (char c : digits) {
        if (c < '0' || c > '9') return std::nullopt;
    }
    return static_cast<std::uint64_t>(std::stoull(digits));
}

// fsync the directory so a newly created file's name is itself durable.
void fsync_dir(const std::string& dir) {
    int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(),
                                "fsync_dir open: " + dir);
    }
    if (::fsync(fd) != 0) {
        int e = errno;
        ::close(fd);
        throw std::system_error(e, std::generic_category(), "fsync_dir: " + dir);
    }
    ::close(fd);
}

}  // namespace

DB::DB(const std::string& dir, std::size_t memtable_threshold,
       std::size_t min_merge, double size_ratio)
    : dir_(dir),
      wal_path_(prepare_wal_path(dir)),
      threshold_(memtable_threshold),
      min_merge_(min_merge),
      size_ratio_(size_ratio),
      wal_(wal_path_) {
    load_sstables();
    // Replay the WAL (writes made since the last flush) back into the memtable.
    for (const Record& rec : Wal::replay(wal_path_)) {
        if (rec.op == Op::Put) {
            memtable_.put(rec.key, rec.value);
        } else {
            memtable_.del(rec.key);
        }
    }
    // Start the background compactor. If the loaded SSTables already qualify, it
    // will pick that up on its first wait (the predicate is checked immediately).
    compactor_ = std::thread(&DB::compaction_loop, this);
}

DB::~DB() {
    {
        std::unique_lock lock(mu_);
        stop_ = true;
    }
    cv_.notify_all();
    if (compactor_.joinable()) compactor_.join();
}

void DB::load_sstables() {
    std::vector<std::pair<std::uint64_t, std::string>> found;
    std::uint64_t max_seq = 0;
    bool any = false;
    for (const auto& entry : fs::directory_iterator(dir_)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        // Remove leftover temp files from a flush/compaction cut short by a crash.
        if (name.size() >= 4 && name.compare(name.size() - 4, 4, ".tmp") == 0) {
            fs::remove(entry.path());
            continue;
        }
        auto seq = parse_seq(name);
        if (!seq) continue;
        found.emplace_back(*seq, entry.path().string());
        max_seq = std::max(max_seq, *seq);
        any = true;
    }
    // Sort newest first so sstables_.front() is the most recent.
    std::sort(found.begin(), found.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });
    for (auto& [seq, path] : found) {
        sstables_.push_back(std::make_shared<SSTable>(path));
    }
    next_seq_ = any ? max_seq + 1 : 0;
}

void DB::flush_locked() {
    if (memtable_.empty()) return;

    std::uint64_t seq = next_seq_++;
    std::string final_path = (fs::path(dir_) / sst_name(seq)).string();
    std::string tmp_path = final_path + ".tmp";

    // 1. Write and fsync the SSTable under a temp name.
    SSTable::build(tmp_path, memtable_.snapshot());
    // 2. Atomically rename into place, then fsync the directory so the name is
    //    durable. Only now is the flushed data safely on disk.
    if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "SSTable rename: " + tmp_path);
    }
    fsync_dir(dir_);

    // 3. Publish the new SSTable and reset the memtable.
    sstables_.insert(sstables_.begin(), std::make_shared<SSTable>(final_path));
    memtable_ = Memtable{};

    // 4. The flushed data is durable in the SSTable, so the old WAL is no longer
    //    needed for recovery -- truncate it. (A crash before this point simply
    //    replays the WAL on top of the SSTable, which is harmless.)
    wal_ = Wal(wal_path_, /*truncate=*/true);

    // A new table may have completed a size tier -- wake the compactor (and any
    // wait_for_idle waiter). notify_all so a waiter can't consume the wakeup
    // meant for the compactor and stall progress.
    cv_.notify_all();
}

bool DB::compaction_pending() const {
    return pick_compaction(sstables_, min_merge_, size_ratio_).has_value();
}

void DB::compaction_loop() {
    for (;;) {
        std::vector<std::shared_ptr<SSTable>> inputs;
        bool drop_tombstones = false;
        std::string final_path;
        std::string tmp_path;

        {
            std::unique_lock lock(mu_);
            cv_.wait(lock, [this] { return stop_ || compaction_pending(); });
            if (stop_) return;

            auto pick = pick_compaction(sstables_, min_merge_, size_ratio_);
            if (!pick) continue;  // spurious wakeup

            for (std::size_t idx : pick->indices) inputs.push_back(sstables_[idx]);
            drop_tombstones = pick->drop_tombstones;

            std::uint64_t seq = next_seq_++;
            final_path = (fs::path(dir_) / sst_name(seq)).string();
            tmp_path = final_path + ".tmp";
            compacting_ = true;
        }

        // Slow work with NO lock held: merge the inputs and write the new file.
        std::vector<Record> merged = merge_records(inputs, drop_tombstones);
        SSTable::build(tmp_path, merged);
        if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
            throw std::system_error(errno, std::generic_category(),
                                    "compaction rename: " + tmp_path);
        }
        fsync_dir(dir_);
        auto output = std::make_shared<SSTable>(final_path);

        {
            std::unique_lock lock(mu_);
            // Replace the inputs with the single merged output, placed at the age
            // slot of the newest input so newest-wins stays correct. Inputs are
            // matched by pointer identity, since the list may have gained newer
            // tables from flushes while we merged.
            std::unordered_set<const SSTable*> input_set;
            for (const auto& sp : inputs) input_set.insert(sp.get());

            std::vector<std::shared_ptr<SSTable>> next;
            next.reserve(sstables_.size());
            bool inserted = false;
            for (const auto& sp : sstables_) {
                if (input_set.count(sp.get())) {
                    sp->mark_obsolete();  // unlinked when the last reader releases
                    if (!inserted) {
                        next.push_back(output);
                        inserted = true;
                    }
                } else {
                    next.push_back(sp);
                }
            }
            if (!inserted) next.push_back(output);  // defensive: inputs all gone
            sstables_ = std::move(next);

            compacting_ = false;
            // Drop our own references so obsolete files can be unlinked promptly
            // once any outstanding reader snapshots release them.
            inputs.clear();
            cv_.notify_all();  // wake wait_for_idle and re-check for more work
        }
    }
}

void DB::put(const std::string& key, const std::string& value) {
    std::unique_lock lock(mu_);
    // WAL first, then memtable: a crash after the WAL append is recoverable by
    // replay; a crash before it means the caller never saw success.
    wal_.append(Record{Op::Put, key, value});
    memtable_.put(key, value);
    if (memtable_.size_bytes() >= threshold_) flush_locked();
}

void DB::del(const std::string& key) {
    std::unique_lock lock(mu_);
    wal_.append(Record{Op::Delete, key, ""});
    memtable_.del(key);
    if (memtable_.size_bytes() >= threshold_) flush_locked();
}

std::optional<std::string> DB::get(const std::string& key) const {
    std::vector<std::shared_ptr<SSTable>> snapshot;
    {
        std::shared_lock lock(mu_);
        // Memtable holds the newest writes.
        if (auto v = memtable_.get(key)) return v;
        if (memtable_.is_tombstone(key)) return std::nullopt;  // deleted, stop
        snapshot = sstables_;  // copy the shared_ptrs, then read lock-free
    }
    // SSTables newest to oldest, first match wins. No lock held here, so
    // compaction can run concurrently; the snapshot keeps these files alive.
    for (const auto& sst : snapshot) {
        if (auto rec = sst->get(key)) {
            if (rec->op == Op::Delete) return std::nullopt;  // tombstone shadows
            return rec->value;
        }
    }
    return std::nullopt;
}

void DB::flush() {
    std::unique_lock lock(mu_);
    flush_locked();
}

void DB::wait_for_idle() {
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] { return !compacting_ && !compaction_pending(); });
}

std::size_t DB::memtable_entry_count() const {
    std::shared_lock lock(mu_);
    return memtable_.entry_count();
}

std::size_t DB::sstable_count() const {
    std::shared_lock lock(mu_);
    return sstables_.size();
}

}  // namespace lsmkv
