#include "lsmkv/db.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lsmkv/compaction.h"
#include "lsmkv/encoding.h"

namespace lsmkv {

namespace fs = std::filesystem;

namespace {

std::string prepare_wal_path(const std::string& dir) {
    fs::create_directories(dir);
    fs::path p(dir);
    p /= "wal.log";
    return p.string();
}

// Zero-padded so lexical and numeric order agree: vlog-000042.log.
std::string vlog_name(std::uint32_t gen) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "vlog-%06lu.log",
                  static_cast<unsigned long>(gen));
    return buf;
}

std::optional<std::uint32_t> parse_vlog_gen(const std::string& name) {
    const std::string prefix = "vlog-";
    const std::string suffix = ".log";
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
    return static_cast<std::uint32_t>(std::stoul(digits));
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

std::uint64_t seq_of_path(const std::string& path) {
    auto s = parse_seq(fs::path(path).filename().string());
    return s ? *s : 0;
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

// --- MANIFEST ---------------------------------------------------------------
// The manifest is the authoritative record of the live table set and its age
// order. Reload reads it instead of inferring order from filename sequence
// numbers (which diverge from age after a compaction, whose output gets a fresh
// high sequence but holds older data). It also carries next_seq and the current
// value-log generation, and names every live SSTable newest-first. Any sst/vlog
// file on disk not referenced by it is a leftover from an interrupted operation.
//
// Format: [magic:8 "LSMKVMN2"][next_seq:8][current_gen:4][num_tables:4]
//         then num_tables x [namelen:4][name][level:4]. Names are basenames;
//         level is the compaction level (0 for size-tiered / L0).
constexpr char kManMagic[8] = {'L', 'S', 'M', 'K', 'V', 'M', 'N', '2'};

struct ManifestEntry {
    std::string name;
    std::int32_t level = 0;
};

struct ManifestData {
    std::uint64_t next_seq = 0;
    std::uint32_t current_gen = 0;
    std::vector<ManifestEntry> tables;  // newest first
};

void write_file_sync(const std::string& path, const std::string& contents) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(),
                                "manifest open: " + path);
    }
    std::size_t written = 0;
    while (written < contents.size()) {
        ssize_t n = ::write(fd, contents.data() + written, contents.size() - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            int e = errno;
            ::close(fd);
            throw std::system_error(e, std::generic_category(), "manifest write");
        }
        written += static_cast<std::size_t>(n);
    }
    if (::fsync(fd) != 0) {
        int e = errno;
        ::close(fd);
        throw std::system_error(e, std::generic_category(), "manifest fsync");
    }
    ::close(fd);
}

// Atomically replace the manifest: write a temp copy, fsync it, rename over the
// old one, then fsync the directory so the rename is durable. The rename is the
// commit point at which the DB's table set officially changes.
void manifest_write(const std::string& dir, const ManifestData& m) {
    std::string buf;
    buf.append(kManMagic, sizeof(kManMagic));
    enc::put_u64_le(buf, m.next_seq);
    enc::put_u32_le(buf, m.current_gen);
    enc::put_u32_le(buf, static_cast<std::uint32_t>(m.tables.size()));
    for (const ManifestEntry& e : m.tables) {
        enc::put_u32_le(buf, static_cast<std::uint32_t>(e.name.size()));
        buf.append(e.name);
        enc::put_u32_le(buf, static_cast<std::uint32_t>(e.level));
    }
    std::string tmp = (fs::path(dir) / "MANIFEST.tmp").string();
    std::string final = (fs::path(dir) / "MANIFEST").string();
    write_file_sync(tmp, buf);
    if (std::rename(tmp.c_str(), final.c_str()) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "manifest rename: " + tmp);
    }
    fsync_dir(dir);
}

std::optional<ManifestData> manifest_read(const std::string& dir) {
    std::string path = (fs::path(dir) / "MANIFEST").string();
    std::error_code ec;
    if (!fs::exists(path, ec)) return std::nullopt;

    std::string b;
    {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::system_error(errno, std::generic_category(),
                                    "manifest open: " + path);
        }
        char tmp[4096];
        for (;;) {
            ssize_t n = ::read(fd, tmp, sizeof(tmp));
            if (n < 0) {
                if (errno == EINTR) continue;
                int e = errno;
                ::close(fd);
                throw std::system_error(e, std::generic_category(), "manifest read");
            }
            if (n == 0) break;
            b.append(tmp, static_cast<std::size_t>(n));
        }
        ::close(fd);
    }

    std::size_t p = 0;
    auto need = [&](std::size_t n) {
        if (p + n > b.size()) throw std::runtime_error("manifest: truncated");
    };
    need(8);
    // A missing/foreign/older-format manifest is treated as "none" so the store
    // bootstraps from a directory scan rather than failing to open.
    if (std::memcmp(b.data(), kManMagic, sizeof(kManMagic)) != 0) return std::nullopt;
    p += 8;
    ManifestData m;
    need(8);
    m.next_seq = enc::read_u64_le(b.data() + p);
    p += 8;
    need(4);
    m.current_gen = enc::read_u32_le(b.data() + p);
    p += 4;
    need(4);
    std::uint32_t count = enc::read_u32_le(b.data() + p);
    p += 4;
    for (std::uint32_t i = 0; i < count; ++i) {
        need(4);
        std::uint32_t len = enc::read_u32_le(b.data() + p);
        p += 4;
        need(len);
        ManifestEntry e;
        e.name.assign(b, p, len);
        p += len;
        need(4);
        e.level = static_cast<std::int32_t>(enc::read_u32_le(b.data() + p));
        p += 4;
        m.tables.push_back(std::move(e));
    }
    return m;
}

}  // namespace

DB::DB(const std::string& dir, std::size_t memtable_threshold,
       std::size_t min_merge, double size_ratio,
       std::size_t value_sep_threshold, Compaction strategy)
    : dir_(dir),
      wal_path_(prepare_wal_path(dir)),
      threshold_(memtable_threshold),
      min_merge_(min_merge),
      size_ratio_(size_ratio),
      value_sep_threshold_(value_sep_threshold),
      leveled_(strategy == Compaction::Leveled),
      leveled_target_bytes_(2 * memtable_threshold),
      leveled_l1_bytes_(16 * memtable_threshold),
      wal_(wal_path_) {
    // Load the committed table set. The manifest is authoritative when present;
    // otherwise (a fresh store, or one from before manifests) fall back to a
    // directory scan and write a manifest once recovery is done.
    std::optional<ManifestData> man = manifest_read(dir_);
    if (man) {
        next_seq_ = man->next_seq;
        current_gen_ = man->current_gen;
        std::vector<std::string> names;
        for (const ManifestEntry& e : man->tables) {
            auto sst = std::make_shared<SSTable>((fs::path(dir_) / e.name).string());
            level_of_[sst.get()] = e.level;
            sstables_.push_back(std::move(sst));
            names.push_back(e.name);
        }
        std::string vp = (fs::path(dir_) / vlog_name(current_gen_)).string();
        vlogs_[current_gen_] = std::make_shared<ValueLog>(vp, current_gen_);
        cleanup_orphans(names);  // drop files no committed manifest references
    } else {
        load_sstables();  // bootstrap: order by sequence (best effort), all level 0
        load_vlogs();
        for (const auto& t : sstables_) level_of_[t.get()] = 0;
    }

    // Recovery. A leftover wal-flushing.log means a crash after a memtable was
    // sealed but before its flush finished; its data is older than the active WAL.
    // Replay it first, then the active WAL on top (newest wins). If one was found,
    // flush the recovered memtable to an SSTable synchronously and drop it, so the
    // store starts from a clean single active WAL.
    auto apply = [this](const Record& rec) {
        if (rec.op == Op::Put) {
            memtable_.put(rec.key, rec.value);
        } else {
            memtable_.del(rec.key);
        }
    };
    std::string flushing_wal = flushing_wal_path();
    bool had_flushing = fs::exists(flushing_wal);
    if (had_flushing) {
        for (const Record& rec : Wal::replay(flushing_wal)) apply(rec);
    }
    for (const Record& rec : Wal::replay(wal_path_)) apply(rec);
    if (had_flushing) {
        flush_locked();  // durably persist the recovered data (also writes a manifest)
        fs::remove(flushing_wal);
        fsync_dir(dir_);
    }
    // Establish a manifest for a store that had none (fresh or pre-manifest).
    if (!man) write_manifest_locked();

    compactor_ = std::thread(&DB::compaction_loop, this);
    flusher_ = std::thread(&DB::flush_loop, this);
}

void DB::write_manifest_locked() {
    ManifestData m;
    m.next_seq = next_seq_;
    m.current_gen = current_gen_;
    m.tables.reserve(sstables_.size());
    for (const auto& t : sstables_) {
        ManifestEntry e;
        e.name = fs::path(t->path()).filename().string();
        auto it = level_of_.find(t.get());
        e.level = (it == level_of_.end()) ? 0 : it->second;
        m.tables.push_back(std::move(e));
    }
    manifest_write(dir_, m);
}

void DB::cleanup_orphans(const std::vector<std::string>& live) {
    std::unordered_set<std::string> keep(live.begin(), live.end());
    for (const auto& entry : fs::directory_iterator(dir_)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        if (name.size() >= 4 && name.compare(name.size() - 4, 4, ".tmp") == 0) {
            fs::remove(entry.path());  // interrupted write (incl. MANIFEST.tmp)
            continue;
        }
        if (parse_seq(name)) {  // an SSTable file
            if (!keep.count(name)) fs::remove(entry.path());  // not in the manifest
        } else if (auto g = parse_vlog_gen(name)) {  // a value-log file
            if (*g != current_gen_) fs::remove(entry.path());  // superseded/orphaned
        }
    }
}

DB::~DB() {
    {
        std::unique_lock lock(mu_);
        stop_ = true;
    }
    cv_.notify_all();
    // A sealed-but-unflushed memtable at shutdown stays in wal-flushing.log and is
    // recovered on the next open, so the flusher may exit without flushing it.
    if (flusher_.joinable()) flusher_.join();
    if (compactor_.joinable()) compactor_.join();
}

std::string DB::flushing_wal_path() const {
    return (fs::path(dir_) / "wal-flushing.log").string();
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

void DB::load_vlogs() {
    std::uint32_t max_gen = 0;
    bool any = false;
    for (const auto& entry : fs::directory_iterator(dir_)) {
        if (!entry.is_regular_file()) continue;
        auto gen = parse_vlog_gen(entry.path().filename().string());
        if (!gen) continue;
        vlogs_[*gen] = std::make_shared<ValueLog>(entry.path().string(), *gen);
        max_gen = std::max(max_gen, *gen);
        any = true;
    }
    // Append to the highest generation present (its tail, if any, is written past).
    current_gen_ = any ? max_gen : 0;
    if (vlogs_.find(current_gen_) == vlogs_.end()) {
        std::string p = (fs::path(dir_) / vlog_name(current_gen_)).string();
        vlogs_[current_gen_] = std::make_shared<ValueLog>(p, current_gen_);
    }
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
            ValuePtr p = vlogs_[current_gen_]->append(r.key, r.value);
            vlog_written += 4 + r.key.size() + 4 + r.value.size();
            r.separated = true;
            r.vptr = p;
            r.value.clear();
        }
    }
    // The value log must be durable before an SSTable pointer into it becomes
    // durable, or a crash could leave a pointer to bytes that were never written.
    if (vlog_written) vlogs_[current_gen_]->sync();

    std::uint64_t bytes = SSTable::build(tmp_path, recs);
    if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "SSTable rename: " + tmp_path);
    }
    fsync_dir(dir_);
    stat_flush_bytes_ += bytes;
    stat_vlog_bytes_ += vlog_written;

    auto flushed = std::make_shared<SSTable>(final_path);
    level_of_[flushed.get()] = 0;  // fresh flushes land in L0
    sstables_.insert(sstables_.begin(), std::move(flushed));
    write_manifest_locked();  // commit the new table set before dropping the WAL
    memtable_ = Memtable{};
    // Flushed data is durable in the SSTable, so the old WAL can be discarded.
    wal_ = Wal(wal_path_, /*truncate=*/true);

    cv_.notify_all();  // a new table may complete a size tier; wake the compactor
}

void DB::seal_locked() {
    if (memtable_.empty()) return;
    // The sealed memtable's writes are all in the current wal.log. Make them
    // durable, then move that log aside as wal-flushing.log and start a fresh
    // active wal.log. On a crash before the flush completes, recovery replays
    // wal-flushing.log. The directory fsync makes the rename durable before the
    // fresh wal.log (a new inode at the old name) is created.
    wal_.sync();
    std::string fw = flushing_wal_path();
    if (std::rename(wal_path_.c_str(), fw.c_str()) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "seal WAL rename: " + wal_path_);
    }
    fsync_dir(dir_);
    wal_ = Wal(wal_path_, /*truncate=*/true);  // old fd closed on assignment

    // Reserve the sequence now, at seal time, so the flushed table's age (its
    // filename sequence) reflects when its data became newest.
    flushing_seq_ = next_seq_++;
    flushing_memtable_ = std::exchange(memtable_, Memtable{});
    has_flushing_ = true;
    cv_.notify_all();  // wake the flusher (and any writer waiting for the slot)
}

void DB::maybe_seal(std::unique_lock<std::shared_mutex>& lock) {
    if (memtable_.size_bytes() < threshold_) return;
    // Only one memtable may be in the flushing slot at a time. If the previous
    // flush has not finished, wait for it -- backpressure when writes outrun the
    // flusher (better than growing unbounded memory).
    cv_.wait(lock, [this] { return !has_flushing_ || stop_; });
    if (stop_) return;
    seal_locked();
}

void DB::flush_loop() {
    for (;;) {
        std::vector<Record> records;
        std::shared_ptr<ValueLog> gen_log;
        std::uint64_t seq;
        {
            std::unique_lock lock(mu_);
            cv_.wait(lock, [this] { return stop_ || has_flushing_; });
            if (stop_) return;  // a pending sealed memtable is left for recovery
            // Snapshot the sealed (immutable) memtable and capture the current
            // value-log generation under the lock; then work with no lock held.
            records = flushing_memtable_.snapshot();
            gen_log = vlogs_[current_gen_];
            seq = flushing_seq_;
        }

        // Off-lock: WiscKey separation + build the SSTable. This flusher is the
        // only writer of the value log (a second flush can't start until this one
        // clears the slot, and GC waits for has_flushing_), so appending unlocked
        // is safe; readers only pread already-published (older) offsets.
        std::uint64_t vlog_written = 0;
        for (Record& r : records) {
            if (r.op == Op::Put && r.value.size() >= value_sep_threshold_) {
                r.vptr = gen_log->append(r.key, r.value);
                vlog_written += 4 + r.key.size() + 4 + r.value.size();
                r.separated = true;
                r.value.clear();
            }
        }
        if (vlog_written) gen_log->sync();

        std::string final_path = (fs::path(dir_) / sst_name(seq)).string();
        std::string tmp_path = final_path + ".tmp";
        std::uint64_t bytes = SSTable::build(tmp_path, records);
        if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
            throw std::system_error(errno, std::generic_category(),
                                    "flush rename: " + tmp_path);
        }
        fsync_dir(dir_);
        auto output = std::make_shared<SSTable>(final_path);

        {
            std::unique_lock lock(mu_);
            level_of_[output.get()] = 0;  // fresh flushes land in L0
            sstables_.insert(sstables_.begin(), output);  // newest
            write_manifest_locked();  // commit before dropping the sealed WAL
            stat_flush_bytes_ += bytes;
            stat_vlog_bytes_ += vlog_written;
            flushing_memtable_ = Memtable{};
            has_flushing_ = false;
            // The data is durable in a manifest-listed SSTable; drop its WAL.
            std::error_code ec;
            fs::remove(flushing_wal_path(), ec);
            cv_.notify_all();  // wake waiting writers, the compactor, GC
        }
    }
}

bool DB::compaction_pending() const {
    if (leveled_) return leveled_pending_locked();
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

    // Size-tiered keeps everything at level 0; the level map just tracks liveness.
    for (const auto& sp : inputs) level_of_.erase(sp.get());
    level_of_[output.get()] = 0;

    // Commit the new set BEFORE unlinking any input, so every file the live
    // manifest names always exists on disk. The inputs are unlinked when the
    // last reader that snapshotted them releases them.
    write_manifest_locked();
    for (const auto& sp : inputs) sp->mark_obsolete();
}

void DB::compaction_loop() {
    for (;;) {
        {
            std::unique_lock lock(mu_);
            cv_.wait(lock, [this] { return stop_ || compaction_pending(); });
            if (stop_) return;
        }
        if (leveled_)
            do_leveled_compaction();
        else
            do_size_tiered_compaction();
    }
}

void DB::do_size_tiered_compaction() {
    std::vector<std::shared_ptr<SSTable>> inputs;
    bool drop_tombstones = false;
    std::string final_path;
    std::string tmp_path;
    {
        std::unique_lock lock(mu_);
        auto pick = pick_compaction(sstables_, min_merge_, size_ratio_);
        if (!pick) return;
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

std::uint64_t DB::level_budget(int level) const {
    // L1 gets leveled_l1_bytes_; each deeper level is size_ratio_ times larger.
    double b = static_cast<double>(leveled_l1_bytes_);
    for (int i = 1; i < level; ++i) b *= size_ratio_;
    return static_cast<std::uint64_t>(b);
}

bool DB::leveled_pending_locked() const {
    int l0 = 0;
    int max_level = 0;
    std::map<int, std::uint64_t> level_bytes;
    for (const auto& t : sstables_) {
        int lv = level_of_.at(t.get());
        max_level = std::max(max_level, lv);
        if (lv == 0)
            ++l0;
        else
            level_bytes[lv] += t->size_bytes();
    }
    if (l0 >= static_cast<int>(min_merge_)) return true;  // L0 has too many tables
    for (const auto& [lv, bytes] : level_bytes) {
        if (bytes > level_budget(lv)) return true;  // a level is over budget
    }
    return false;
}

void DB::reorder_sstables_locked() {
    // Read order: L0 newest (highest sequence) first, then each deeper level by
    // key range. L0 tables are only ever flushes, so their sequence is their age.
    std::stable_sort(
        sstables_.begin(), sstables_.end(),
        [this](const std::shared_ptr<SSTable>& a, const std::shared_ptr<SSTable>& b) {
            int la = level_of_.at(a.get()), lb = level_of_.at(b.get());
            if (la != lb) return la < lb;
            if (la == 0) return seq_of_path(a->path()) > seq_of_path(b->path());
            return a->min_key() < b->min_key();
        });
}

void DB::do_leveled_compaction() {
    std::vector<std::shared_ptr<SSTable>> inputs;  // newest-first: source then dest
    int dest_level = 0;
    bool drop_tombstones = false;
    {
        std::unique_lock lock(mu_);
        if (!leveled_pending_locked()) return;

        std::map<int, std::vector<std::shared_ptr<SSTable>>> by_level;
        int max_level = 0;
        for (const auto& t : sstables_) {
            int lv = level_of_.at(t.get());
            by_level[lv].push_back(t);
            max_level = std::max(max_level, lv);
        }

        // Choose the source level: L0 if it has too many tables, else the
        // shallowest level over its byte budget.
        int src_level = -1;
        if (static_cast<int>(by_level[0].size()) >= static_cast<int>(min_merge_)) {
            src_level = 0;
        } else {
            for (int lv = 1; lv <= max_level; ++lv) {
                std::uint64_t bytes = 0;
                for (const auto& t : by_level[lv]) bytes += t->size_bytes();
                if (bytes > level_budget(lv)) { src_level = lv; break; }
            }
        }
        if (src_level < 0) return;
        dest_level = src_level + 1;

        // Source: all of L0, or the single lowest-range table of a deeper level.
        std::vector<std::shared_ptr<SSTable>> src_tables;
        if (src_level == 0) {
            src_tables = by_level[0];  // already newest-first in sstables_ order
        } else {
            auto& lv = by_level[src_level];
            src_tables.push_back(*std::min_element(
                lv.begin(), lv.end(), [](const auto& a, const auto& b) {
                    return a->min_key() < b->min_key();
                }));
        }

        // Combined key range of the source tables.
        std::string lo = src_tables.front()->min_key();
        std::string hi = src_tables.front()->max_key();
        for (const auto& t : src_tables) {
            if (t->min_key() < lo) lo = t->min_key();
            if (t->max_key() > hi) hi = t->max_key();
        }
        // Destination tables whose range overlaps [lo, hi].
        std::vector<std::shared_ptr<SSTable>> dest_tables;
        for (const auto& t : by_level[dest_level]) {
            if (!(t->max_key() < lo || t->min_key() > hi)) dest_tables.push_back(t);
        }

        inputs = src_tables;  // newer
        inputs.insert(inputs.end(), dest_tables.begin(), dest_tables.end());  // older
        // Tombstones may be dropped only if nothing lives below the destination.
        drop_tombstones = (dest_level >= max_level);
        compacting_ = true;
    }

    // Off-lock: merge, then split into ~target-size files for the destination.
    std::vector<Record> merged = merge_records(inputs, drop_tombstones);
    std::vector<std::vector<Record>> groups;
    {
        std::vector<Record> cur;
        std::uint64_t cur_bytes = 0;
        for (Record& r : merged) {
            cur_bytes += 1 + 4 + r.key.size() + 4 + (r.separated ? 12 : r.value.size());
            cur.push_back(std::move(r));
            if (cur_bytes >= leveled_target_bytes_) {
                groups.push_back(std::move(cur));
                cur.clear();
                cur_bytes = 0;
            }
        }
        if (!cur.empty()) groups.push_back(std::move(cur));
    }

    // Reserve a sequence per output file.
    std::vector<std::uint64_t> seqs;
    {
        std::unique_lock lock(mu_);
        for (std::size_t i = 0; i < groups.size(); ++i) seqs.push_back(next_seq_++);
    }
    std::vector<std::shared_ptr<SSTable>> outputs;
    std::uint64_t total_bytes = 0;
    for (std::size_t i = 0; i < groups.size(); ++i) {
        std::string final_path = (fs::path(dir_) / sst_name(seqs[i])).string();
        std::string tmp_path = final_path + ".tmp";
        total_bytes += SSTable::build(tmp_path, groups[i]);
        if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
            throw std::system_error(errno, std::generic_category(),
                                    "leveled rename: " + tmp_path);
        }
        outputs.push_back(std::make_shared<SSTable>(final_path));
    }
    fsync_dir(dir_);

    {
        std::unique_lock lock(mu_);
        std::unordered_set<const SSTable*> in_set;
        for (const auto& t : inputs) in_set.insert(t.get());
        std::vector<std::shared_ptr<SSTable>> next;
        for (const auto& t : sstables_) {
            if (!in_set.count(t.get())) next.push_back(t);
        }
        for (const auto& t : inputs) level_of_.erase(t.get());
        for (const auto& o : outputs) {
            level_of_[o.get()] = dest_level;
            next.push_back(o);
        }
        sstables_ = std::move(next);
        reorder_sstables_locked();

        stat_compaction_bytes_ += total_bytes;
        stat_compactions_ += 1;
        write_manifest_locked();  // commit before unlinking inputs
        for (const auto& t : inputs) t->mark_obsolete();
        compacting_ = false;
        inputs.clear();
        cv_.notify_all();
    }
}

void DB::put(const std::string& key, const std::string& value, bool sync) {
    std::unique_lock lock(mu_);
    wal_.append(Record{Op::Put, key, value}, sync);
    memtable_.put(key, value);
    stat_writes_ += 1;
    stat_user_bytes_ += key.size() + value.size();
    stat_wal_bytes_ += wal_record_bytes(key, value);
    maybe_seal(lock);  // seal + background flush when full (off the write path)
}

void DB::del(const std::string& key, bool sync) {
    std::unique_lock lock(mu_);
    wal_.append(Record{Op::Delete, key, ""}, sync);
    memtable_.del(key);
    stat_deletes_ += 1;
    stat_user_bytes_ += key.size();
    stat_wal_bytes_ += wal_record_bytes(key, "");
    maybe_seal(lock);  // seal + background flush when full (off the write path)
}

std::optional<std::string> DB::get(const std::string& key) const {
    stat_reads_ += 1;
    std::vector<std::shared_ptr<SSTable>> snapshot;
    std::map<std::uint32_t, std::shared_ptr<ValueLog>> vlog_snap;
    bool use_bloom;
    {
        std::shared_lock lock(mu_);
        if (auto v = memtable_.get(key)) return v;
        if (memtable_.is_tombstone(key)) return std::nullopt;
        // Then the sealed memtable being flushed (newer than any SSTable). Its
        // values are inline -- separation happens only when it is written out.
        if (has_flushing_) {
            if (auto v = flushing_memtable_.get(key)) return v;
            if (flushing_memtable_.is_tombstone(key)) return std::nullopt;
        }
        snapshot = sstables_;
        vlog_snap = vlogs_;  // consistent with `snapshot`: GC swaps both at once
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
        if (rec->separated) return vlog_snap.at(rec->vptr.gen)->read(rec->vptr);
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
    std::map<std::uint32_t, std::shared_ptr<ValueLog>> vlog_snap;
    {
        std::shared_lock lock(mu_);
        sources.push_back(memtable_.range(start, end));
        // The sealed memtable ranks between the active memtable and the SSTables.
        if (has_flushing_) sources.push_back(flushing_memtable_.range(start, end));
        snapshot = sstables_;
        vlog_snap = vlogs_;
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
        if (live)
            value = rec.separated ? vlog_snap.at(rec.vptr.gen)->read(rec.vptr)
                                  : rec.value;

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
    // Let any in-flight background flush finish first, then flush the active
    // memtable synchronously, so on return everything written so far is on disk.
    cv_.wait(lock, [this] { return !has_flushing_ || stop_; });
    if (stop_) return;
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

void DB::gc_value_log() {
    std::unique_lock lock(mu_);
    // Wait out any in-flight compaction and background flush so the SSTable set is
    // stable, then hold compacting_ so the compactor starts no new work while GC
    // runs. A new flush cannot seal meanwhile because seal needs this lock.
    cv_.wait(lock, [this] { return !compacting_ && !has_flushing_; });
    compacting_ = true;

    flush_locked();  // move the active memtable into an SSTable: all data on disk
    if (sstables_.empty()) {
        compacting_ = false;
        cv_.notify_all();
        return;
    }
    std::vector<std::shared_ptr<SSTable>> inputs = sstables_;  // all, newest-first

    // The merge picks the live records (newest per key, tombstones dropped since
    // it reaches the oldest table). Any value whose pointer does not survive the
    // merge is dead and is simply never copied to the new generation.
    std::vector<Record> merged = merge_records(inputs, /*drop_tombstones=*/true);

    std::uint32_t new_gen = current_gen_ + 1;
    std::string vpath = (fs::path(dir_) / vlog_name(new_gen)).string();
    auto new_vlog = std::make_shared<ValueLog>(vpath, new_gen);

    // Relocate each surviving value into the new generation and repoint it.
    std::uint64_t relocated = 0;
    for (Record& rec : merged) {
        if (rec.separated) {
            std::string v = vlogs_.at(rec.vptr.gen)->read(rec.vptr);
            rec.vptr = new_vlog->append(rec.key, v);
            relocated += 4 + rec.key.size() + 4 + v.size();
        }
    }
    new_vlog->sync();  // durable before the SSTable that points into it

    std::uint64_t seq = next_seq_++;
    std::string final_path = (fs::path(dir_) / sst_name(seq)).string();
    std::string tmp_path = final_path + ".tmp";
    std::uint64_t bytes = SSTable::build(tmp_path, merged);
    if (std::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "gc rename: " + tmp_path);
    }
    fsync_dir(dir_);
    auto output = std::make_shared<SSTable>(final_path);

    // Set the new state, then commit it to the manifest, then unlink the old
    // files -- never before, so every file the live manifest names exists. The
    // old tables and generations are unlinked once the last reader that
    // snapshotted them releases them (the SSTable lifetime rule, reused).
    sstables_ = {output};
    level_of_.clear();            // one table now; back to L0
    level_of_[output.get()] = 0;
    vlogs_[new_gen] = new_vlog;
    current_gen_ = new_gen;
    write_manifest_locked();  // commit: [output], generation = new_gen

    for (auto& in : inputs) in->mark_obsolete();
    for (auto it = vlogs_.begin(); it != vlogs_.end();) {
        if (it->first != new_gen) {
            it->second->mark_obsolete();
            it = vlogs_.erase(it);
        } else {
            ++it;
        }
    }

    stat_compaction_bytes_ += bytes;
    stat_vlog_bytes_ += relocated;
    stat_compactions_ += 1;

    compacting_ = false;
    inputs.clear();  // let obsolete files unlink once readers release them
    cv_.notify_all();
}

void DB::set_bloom_enabled(bool on) {
    std::unique_lock lock(mu_);
    bloom_enabled_ = on;
}

void DB::wait_for_idle() {
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] {
        return !compacting_ && !compaction_pending() && !has_flushing_;
    });
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
        if (leveled_) {
            auto it = level_of_.find(t.get());
            info.tier = (it == level_of_.end()) ? 0 : it->second;  // compaction level
        } else {
            double s = static_cast<double>(std::max<std::uint64_t>(t->size_bytes(), 1));
            info.tier =
                static_cast<int>(std::floor(std::log(s) / std::log(size_ratio_)));
        }
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
