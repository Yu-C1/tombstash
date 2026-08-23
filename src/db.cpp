#include "lsmkv/db.h"

#include <filesystem>
#include <mutex>
#include <shared_mutex>

namespace lsmkv {

namespace {
// Create the database directory if needed and return the WAL path inside it.
std::string prepare_wal_path(const std::string& dir) {
    std::filesystem::create_directories(dir);
    std::filesystem::path p(dir);
    p /= "wal.log";
    return p.string();
}
}  // namespace

DB::DB(const std::string& dir)
    : wal_path_(prepare_wal_path(dir)), wal_(wal_path_) {
    // Rebuild the memtable from the log written before the last shutdown or
    // crash. Records are applied in write order, so the newest write wins.
    for (const Record& rec : Wal::replay(wal_path_)) {
        if (rec.op == Op::Put) {
            memtable_.put(rec.key, rec.value);
        } else {
            memtable_.del(rec.key);
        }
    }
}

void DB::put(const std::string& key, const std::string& value) {
    std::unique_lock lock(mu_);
    // WAL first, then memtable: a crash after the WAL append is recoverable by
    // replay; a crash before it means the caller never saw success.
    wal_.append(Record{Op::Put, key, value});
    memtable_.put(key, value);
}

void DB::del(const std::string& key) {
    std::unique_lock lock(mu_);
    wal_.append(Record{Op::Delete, key, ""});
    memtable_.del(key);
}

std::optional<std::string> DB::get(const std::string& key) const {
    std::shared_lock lock(mu_);
    return memtable_.get(key);
}

std::size_t DB::entry_count() const {
    std::shared_lock lock(mu_);
    return memtable_.entry_count();
}

}  // namespace lsmkv
