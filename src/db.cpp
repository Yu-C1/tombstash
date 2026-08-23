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
#include <utility>
#include <vector>

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

// fsync the directory so a newly created file's name is itself durable. Without
// this a crash could lose the rename even though the file's bytes are on disk.
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

DB::DB(const std::string& dir, std::size_t memtable_threshold)
    : dir_(dir),
      wal_path_(prepare_wal_path(dir)),
      threshold_(memtable_threshold),
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
}

void DB::load_sstables() {
    std::vector<std::pair<std::uint64_t, std::string>> found;
    std::uint64_t max_seq = 0;
    bool any = false;
    for (const auto& entry : fs::directory_iterator(dir_)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        // Remove leftover temp files from a flush interrupted by a crash.
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
    std::shared_lock lock(mu_);
    // Memtable holds the newest writes.
    if (auto v = memtable_.get(key)) return v;
    if (memtable_.is_tombstone(key)) return std::nullopt;  // deleted, stop here
    // Then SSTables, newest to oldest. First match wins.
    for (const auto& sst : sstables_) {
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

std::size_t DB::memtable_entry_count() const {
    std::shared_lock lock(mu_);
    return memtable_.entry_count();
}

std::size_t DB::sstable_count() const {
    std::shared_lock lock(mu_);
    return sstables_.size();
}

}  // namespace lsmkv
