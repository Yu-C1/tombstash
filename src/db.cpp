#include "lsmkv/db.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cmath>
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

std::string prepare_wal_path(const std::string& dir) {
    fs::create_directories(dir);
    fs::path p(dir);
    p /= "wal.log";
    return p.string();
}

std::string vlog_file_path(const std::string& dir) {
    // The directory already exists by now (prepare_wal_path created it).
    return (fs::path(dir) / "vlog.log").string();
}

// Zero-padded so lexical and numeric order agree: sst-000042.sst.
std::string sst_name(std::uint64_t seq) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "sst-%06llu.sst",
                  static_cast<unsigned long long>(seq));
    return buf;
}

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

// On-disk size of one WAL record: op byte + two 4-byte lengths + payloads.
std::uint64_t wal_record_bytes(const std::string& key, const std::string& value) {
    return 1 + 4 + key.size() + 4 + value.size();
}

}  // namespace

DB::DB(const std::string& dir, std::size_t memtable_threshold,
       std::size_t min_merge, double size_ratio,
       std::size_t value_sep_threshold)
    : dir_(dir),
      wal_path_(prepare_wal_path(dir)),
      vlog_path_(vlog_file_path(dir)),
      threshold_(memtable_threshold),
      min_merge_(min_merge),
      size_ratio_(size_ratio),
      value_sep_threshold_(value_sep_threshold),
      wal_(wal_path_),
      vlog_(vlog_path_) {
    load_sstables();
    for (const Record& rec : Wal::replay(wal_path_)) {
        if (rec.op == Op::Put) {
            memtable_.put(rec.key, rec.value);
        } else {
            memtable_.del(rec.key);
        }
    }
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
        if (name.size() >= 4 && name.compare(name.size() - 4, 4, ".tmp") == 0) {
            fs::remove(entry.path());  // leftover from an interrupted write
            continue;
        }
        auto seq = parse_seq(name);
        if (!seq) continue;
        found.emplace_back(*seq, entry.path().string());
        max_seq = std::max(max_seq, *seq);
        any = true;
    }
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

    // WiscKey separation: values at least the threshold go to the value log; the
    // SSTable keeps only a pointer, so later compactions never rewrite the value.
    std::vector<Record> recs = memtable_.snapshot();
    std::uint64_t vlog_written = 0;
    for (Record& r : recs) {
        if (r.op == Op::Put && r.value.size() >= value_sep_threshold_) {
            ValuePtr p = vlog_.append(r.key, r.value);
            vlog_written += 4 + r.key.size() + 4 + r.value.size();
            r.separated = true;
            r.vptr = p;
            r.value.clear();
        }
    }
    // The value log must be durable before an SSTable pointer into it becomes
    // durable, or a crash could leave a pointer to bytes that were never written.
    if (vlog_written) vlog_.sync();

    std::uint64_t bytes = SSTable::build(tmp_path, recs);
    if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "SSTable rename: " + tmp_path);
    }
    fsync_dir(dir_);
    stat_flush_bytes_ += bytes;
    stat_vlog_bytes_ += vlog_written;

    sstables_.insert(sstables_.begin(), std::make_shared<SSTable>(final_path));
    memtable_ = Memtable{};
    // Flushed data is durable in the SSTable, so the old WAL can be discarded.
    wal_ = Wal(wal_path_, /*truncate=*/true);

    cv_.notify_all();  // a new table may complete a size tier; wake the compactor
}

bool DB::compaction_pending() const {
    return pick_compaction(sstables_, min_merge_, size_ratio_).has_value();
}

void DB::install_merge_result(
    const std::vector<std::shared_ptr<SSTable>>& inputs,
    std::shared_ptr<SSTable> output) {
    std::unordered_set<const SSTable*> input_set;
    for (const auto& sp : inputs) input_set.insert(sp.get());

    std::vector<std::shared_ptr<SSTable>> next;
    next.reserve(sstables_.size());
    bool inserted = false;
    for (const auto& sp : sstables_) {
        if (input_set.count(sp.get())) {
            sp->mark_obsolete();  // unlinked when the last reader releases it
            if (!inserted) {
                next.push_back(output);  // output takes the newest input's slot
                inserted = true;
            }
        } else {
            next.push_back(sp);
        }
    }
    if (!inserted) next.push_back(output);  // defensive: inputs all gone
    sstables_ = std::move(next);
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
            if (!pick) continue;

            for (std::size_t idx : pick->indices) inputs.push_back(sstables_[idx]);
            drop_tombstones = pick->drop_tombstones;
            std::uint64_t seq = next_seq_++;
            final_path = (fs::path(dir_) / sst_name(seq)).string();
            tmp_path = final_path + ".tmp";
            compacting_ = true;
        }

        // Slow work with NO lock held.
        std::vector<Record> merged = merge_records(inputs, drop_tombstones);
        std::uint64_t bytes = SSTable::build(tmp_path, merged);
        if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
            throw std::system_error(errno, std::generic_category(),
                                    "compaction rename: " + tmp_path);
        }
        fsync_dir(dir_);
        auto output = std::make_shared<SSTable>(final_path);

        {
            std::unique_lock lock(mu_);
            stat_compaction_bytes_ += bytes;
            stat_compactions_ += 1;
            install_merge_result(inputs, output);
            compacting_ = false;
            inputs.clear();  // let obsolete files unlink once readers release them
            cv_.notify_all();
        }
    }
}

void DB::put(const std::string& key, const std::string& value, bool sync) {
    std::unique_lock lock(mu_);
    wal_.append(Record{Op::Put, key, value}, sync);
    memtable_.put(key, value);
    stat_writes_ += 1;
    stat_user_bytes_ += key.size() + value.size();
    stat_wal_bytes_ += wal_record_bytes(key, value);
    if (memtable_.size_bytes() >= threshold_) flush_locked();
}

void DB::del(const std::string& key, bool sync) {
    std::unique_lock lock(mu_);
    wal_.append(Record{Op::Delete, key, ""}, sync);
    memtable_.del(key);
    stat_deletes_ += 1;
    stat_user_bytes_ += key.size();
    stat_wal_bytes_ += wal_record_bytes(key, "");
    if (memtable_.size_bytes() >= threshold_) flush_locked();
}

std::optional<std::string> DB::get(const std::string& key) const {
    stat_reads_ += 1;
    std::vector<std::shared_ptr<SSTable>> snapshot;
    bool use_bloom;
    {
        std::shared_lock lock(mu_);
        if (auto v = memtable_.get(key)) return v;
        if (memtable_.is_tombstone(key)) return std::nullopt;
        snapshot = sstables_;
        use_bloom = bloom_enabled_;
    }
    // No lock held: compaction may run concurrently; the snapshot keeps files alive.
    for (const auto& sst : snapshot) {
        if (use_bloom) {
            stat_bloom_checks_ += 1;
            if (!sst->may_contain(key)) {
                stat_bloom_skips_ += 1;
                continue;  // Bloom proves the key is not in this file
            }
        }
        auto rec = sst->get_no_bloom(key);
        if (!rec) {
            if (use_bloom) stat_bloom_fps_ += 1;  // filter said maybe, but absent
            continue;
        }
        if (rec->op == Op::Delete) return std::nullopt;  // tombstone shadows
        // A separated value lives in the value log; one extra positioned read.
        if (rec->separated) return vlog_.read(rec->vptr);
        return rec->value;
    }
    return std::nullopt;
}

std::vector<std::pair<std::string, std::string>> DB::scan(
    const std::string& start, const std::string& end) const {
    stat_reads_ += 1;
    // Each source is a sorted run of records in [start, end): the memtable
    // (newest) first, then SSTables newest to oldest.
    std::vector<std::vector<Record>> sources;
    std::vector<std::shared_ptr<SSTable>> snapshot;
    {
        std::shared_lock lock(mu_);
        sources.push_back(memtable_.range(start, end));
        snapshot = sstables_;
    }
    for (const auto& sst : snapshot) sources.push_back(sst->range(start, end));

    // k-way merge: emit the smallest key across sources; the newest source
    // holding it wins; a tombstone means the key is deleted, so it is skipped.
    std::vector<std::pair<std::string, std::string>> out;
    std::vector<std::size_t> cur(sources.size(), 0);
    auto valid = [&](std::size_t j) { return cur[j] < sources[j].size(); };

    for (;;) {
        bool any = false;
        std::string min_key;
        for (std::size_t j = 0; j < sources.size(); ++j) {
            if (!valid(j)) continue;
            const std::string& k = sources[j][cur[j]].key;
            if (!any || k < min_key) {
                min_key = k;
                any = true;
            }
        }
        if (!any) break;

        std::size_t newest = 0;
        for (std::size_t j = 0; j < sources.size(); ++j) {
            if (valid(j) && sources[j][cur[j]].key == min_key) {
                newest = j;
                break;
            }
        }
        const Record& rec = sources[newest][cur[newest]];
        bool live = rec.op == Op::Put;
        // A separated value costs one random value-log read per key -- the known
        // cost of key-value separation for range scans (values are scattered in
        // write order, not laid out next to their keys in the block).
        std::string value;
        if (live) value = rec.separated ? vlog_.read(rec.vptr) : rec.value;

        for (std::size_t j = 0; j < sources.size(); ++j) {
            if (valid(j) && sources[j][cur[j]].key == min_key) ++cur[j];
        }
        if (live) out.emplace_back(min_key, std::move(value));
    }
    return out;
}

void DB::sync() {
    std::unique_lock lock(mu_);
    wal_.sync();
}

void DB::flush() {
    std::unique_lock lock(mu_);
    flush_locked();
}

void DB::compact_all() {
    std::vector<std::shared_ptr<SSTable>> inputs;
    std::string final_path;
    std::string tmp_path;
    {
        std::unique_lock lock(mu_);
        if (sstables_.size() <= 1) return;  // nothing to merge
        inputs = sstables_;                 // all tables, newest-first
        std::uint64_t seq = next_seq_++;
        final_path = (fs::path(dir_) / sst_name(seq)).string();
        tmp_path = final_path + ".tmp";
        compacting_ = true;
    }
    // Merging everything reaches the oldest table, so tombstones can be dropped.
    std::vector<Record> merged = merge_records(inputs, /*drop_tombstones=*/true);
    std::uint64_t bytes = SSTable::build(tmp_path, merged);
    if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "compact_all rename: " + tmp_path);
    }
    fsync_dir(dir_);
    auto output = std::make_shared<SSTable>(final_path);
    {
        std::unique_lock lock(mu_);
        stat_compaction_bytes_ += bytes;
        stat_compactions_ += 1;
        install_merge_result(inputs, output);
        compacting_ = false;
        inputs.clear();
        cv_.notify_all();
    }
}

void DB::set_bloom_enabled(bool on) {
    std::unique_lock lock(mu_);
    bloom_enabled_ = on;
}

void DB::wait_for_idle() {
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] { return !compacting_ && !compaction_pending(); });
}

std::size_t DB::memtable_entry_count() const {
    std::shared_lock lock(mu_);
    return memtable_.entry_count();
}

std::size_t DB::memtable_bytes() const {
    std::shared_lock lock(mu_);
    return memtable_.size_bytes();
}

std::size_t DB::memtable_tombstones() const {
    std::shared_lock lock(mu_);
    return memtable_.tombstone_count();
}

std::size_t DB::sstable_count() const {
    std::shared_lock lock(mu_);
    return sstables_.size();
}

std::uint64_t DB::disk_bytes() const {
    std::shared_lock lock(mu_);
    std::uint64_t total = 0;
    for (const auto& t : sstables_) total += t->size_bytes();
    return total;
}

std::vector<DB::SSTableInfo> DB::sstable_infos() const {
    std::shared_lock lock(mu_);
    std::vector<SSTableInfo> out;
    out.reserve(sstables_.size());
    for (const auto& t : sstables_) {
        SSTableInfo info;
        info.size_bytes = t->size_bytes();
        info.records = t->key_count();
        info.min_key = t->min_key();
        info.max_key = t->max_key();
        double s = static_cast<double>(std::max<std::uint64_t>(t->size_bytes(), 1));
        info.tier = static_cast<int>(std::floor(std::log(s) / std::log(size_ratio_)));
        out.push_back(std::move(info));
    }
    return out;
}

DB::Stats DB::stats() const {
    Stats s;
    s.user_bytes = stat_user_bytes_.load();
    s.wal_bytes = stat_wal_bytes_.load();
    s.flush_bytes = stat_flush_bytes_.load();
    s.compaction_bytes = stat_compaction_bytes_.load();
    s.vlog_bytes = stat_vlog_bytes_.load();
    s.writes = stat_writes_.load();
    s.deletes = stat_deletes_.load();
    s.reads = stat_reads_.load();
    s.bloom_checks = stat_bloom_checks_.load();
    s.bloom_skips = stat_bloom_skips_.load();
    s.bloom_false_positives = stat_bloom_fps_.load();
    s.compactions = stat_compactions_.load();
    return s;
}

}  // namespace lsmkv
