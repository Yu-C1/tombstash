#include "lsmkv/sstable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "lsmkv/encoding.h"

namespace lsmkv {

namespace {

std::atomic<std::uint64_t> g_block_reads{0};


constexpr std::size_t kFooterSize = 40;  // 4 x u64 + 8-byte magic
constexpr char kMagic[8] = {'L', 'S', 'M', 'K', 'V', 'S', 'S', 'T'};

void write_file_sync(const std::string& path, const std::string& contents) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(),
                                "SSTable open for write: " + path);
    }
    std::size_t written = 0;
    while (written < contents.size()) {
        ssize_t n = ::write(fd, contents.data() + written,
                            contents.size() - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            int e = errno;
            ::close(fd);
            throw std::system_error(e, std::generic_category(), "SSTable write");
        }
        written += static_cast<std::size_t>(n);
    }
    if (::fsync(fd) != 0) {
        int e = errno;
        ::close(fd);
        throw std::system_error(e, std::generic_category(), "SSTable fsync");
    }
    ::close(fd);
}

std::string pread_exact(int fd, std::uint64_t off, std::size_t len) {
    std::string buf(len, '\0');
    std::size_t got = 0;
    while (got < len) {
        ssize_t n = ::pread(fd, buf.data() + got, len - got, off + got);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "SSTable pread");
        }
        if (n == 0) throw std::runtime_error("SSTable pread: unexpected EOF");
        got += static_cast<std::size_t>(n);
    }
    return buf;
}

void append_record(std::string& buf, const Record& rec) {
    buf.push_back(static_cast<char>(rec.op));
    enc::put_u32_le(buf, static_cast<std::uint32_t>(rec.key.size()));
    buf.append(rec.key);
    enc::put_u32_le(buf, static_cast<std::uint32_t>(rec.value.size()));
    buf.append(rec.value);
}

// Parse one record from bytes starting at p; fill out, return the next position.
std::size_t parse_one(const std::string& b, std::size_t p, Record& out) {
    if (p + 1 + 4 > b.size()) throw std::runtime_error("SSTable: short record header");
    out.op = static_cast<Op>(static_cast<unsigned char>(b[p]));
    p += 1;
    std::uint32_t klen = enc::read_u32_le(b.data() + p);
    p += 4;
    if (p + klen + 4 > b.size()) throw std::runtime_error("SSTable: bad key length");
    out.key.assign(b, p, klen);
    p += klen;
    std::uint32_t vlen = enc::read_u32_le(b.data() + p);
    p += 4;
    if (p + vlen > b.size()) throw std::runtime_error("SSTable: bad value length");
    out.value.assign(b, p, vlen);
    p += vlen;
    return p;
}

}  // namespace

std::uint64_t SSTable::build(const std::string& path,
                             const std::vector<Record>& sorted) {
    BloomFilter bloom(sorted.empty() ? 1 : sorted.size(), 0.01);
    std::string data;
    std::string index;
    std::uint64_t num_records = 0;

    std::size_t block_start = 0;
    std::string first_key;
    bool block_open = false;

    auto close_block = [&](const std::string& fkey, std::uint64_t start,
                           std::uint64_t end) {
        enc::put_u32_le(index, static_cast<std::uint32_t>(fkey.size()));
        index.append(fkey);
        enc::put_u64_le(index, start);
        enc::put_u64_le(index, end - start);
    };

    for (const Record& rec : sorted) {
        if (rec.key.size() > UINT32_MAX || rec.value.size() > UINT32_MAX) {
            throw std::length_error("SSTable record field exceeds 4 GiB");
        }
        if (!block_open) {
            block_start = data.size();
            first_key = rec.key;
            block_open = true;
        }
        append_record(data, rec);
        bloom.add(rec.key);
        ++num_records;
        // Close the block once it reaches the target size (by one record over).
        if (data.size() - block_start >= kBlockSize) {
            close_block(first_key, block_start, data.size());
            block_open = false;
        }
    }
    if (block_open) close_block(first_key, block_start, data.size());

    std::string bloom_bytes = bloom.serialize();

    std::string footer;
    enc::put_u64_le(footer, data.size());
    enc::put_u64_le(footer, index.size());
    enc::put_u64_le(footer, bloom_bytes.size());
    enc::put_u64_le(footer, num_records);
    footer.append(kMagic, sizeof(kMagic));

    std::string file;
    file.reserve(data.size() + index.size() + bloom_bytes.size() + footer.size());
    file += data;
    file += index;
    file += bloom_bytes;
    file += footer;
    write_file_sync(path, file);
    return file.size();
}

SSTable::SSTable(std::string path) : path_(std::move(path)), bloom_(1, 0.01) {
    fd_ = ::open(path_.c_str(), O_RDONLY);
    if (fd_ < 0) {
        throw std::system_error(errno, std::generic_category(),
                                "SSTable open: " + path_);
    }
    struct stat st{};
    if (::fstat(fd_, &st) != 0) {
        int e = errno;
        ::close(fd_);
        fd_ = -1;
        throw std::system_error(e, std::generic_category(), "SSTable fstat");
    }
    auto file_size = static_cast<std::uint64_t>(st.st_size);
    if (file_size < kFooterSize) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("SSTable: file smaller than footer: " + path_);
    }

    std::string footer = pread_exact(fd_, file_size - kFooterSize, kFooterSize);
    if (footer.compare(32, 8, kMagic, sizeof(kMagic)) != 0) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("SSTable: bad magic: " + path_);
    }
    std::uint64_t data_size = enc::read_u64_le(footer.data());
    std::uint64_t index_size = enc::read_u64_le(footer.data() + 8);
    std::uint64_t bloom_size = enc::read_u64_le(footer.data() + 16);
    std::uint64_t num_records = enc::read_u64_le(footer.data() + 24);
    if (data_size + index_size + bloom_size + kFooterSize != file_size) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("SSTable: block sizes disagree with file: " + path_);
    }
    data_size_ = data_size;
    file_size_ = file_size;
    num_records_ = num_records;

    // Sparse index: one entry per block, already sorted by first_key on disk.
    std::string idx = pread_exact(fd_, data_size, index_size);
    std::size_t p = 0;
    while (p < idx.size()) {
        if (p + 4 > idx.size()) throw std::runtime_error("SSTable: torn index");
        std::uint32_t klen = enc::read_u32_le(idx.data() + p);
        p += 4;
        if (p + klen + 16 > idx.size()) throw std::runtime_error("SSTable: torn index");
        IndexEntry e;
        e.first_key.assign(idx, p, klen);
        p += klen;
        e.offset = enc::read_u64_le(idx.data() + p);
        p += 8;
        e.length = enc::read_u64_le(idx.data() + p);
        p += 8;
        index_.push_back(std::move(e));
    }

    std::string bloom_bytes = pread_exact(fd_, data_size + index_size, bloom_size);
    bloom_ = BloomFilter::deserialize(bloom_bytes);
}

SSTable::~SSTable() {
    if (fd_ >= 0) ::close(fd_);
    if (obsolete_) ::unlink(path_.c_str());
}

std::uint64_t SSTable::block_reads() { return g_block_reads.load(); }
void SSTable::reset_block_reads() { g_block_reads.store(0); }

std::vector<Record> SSTable::read_block_records(std::size_t i) const {
    const IndexEntry& e = index_[i];
    g_block_reads.fetch_add(1, std::memory_order_relaxed);  // one data-block read
    std::string bytes = pread_exact(fd_, e.offset, static_cast<std::size_t>(e.length));
    std::vector<Record> recs;
    std::size_t p = 0;
    while (p < bytes.size()) {
        Record r;
        p = parse_one(bytes, p, r);
        recs.push_back(std::move(r));
    }
    return recs;
}

bool SSTable::may_contain(const std::string& key) const {
    return bloom_.maybe_contains(key);
}

std::optional<Record> SSTable::get_no_bloom(const std::string& key) const {
    if (index_.empty()) return std::nullopt;
    // Candidate block = the last block whose first_key <= key.
    auto it = std::upper_bound(
        index_.begin(), index_.end(), key,
        [](const std::string& k, const IndexEntry& e) { return k < e.first_key; });
    if (it == index_.begin()) return std::nullopt;  // key precedes the first block
    --it;
    std::vector<Record> recs = read_block_records(
        static_cast<std::size_t>(it - index_.begin()));
    for (const Record& r : recs) {
        if (r.key == key) return r;
        if (r.key > key) break;  // records are sorted; we've passed where it would be
    }
    return std::nullopt;
}

std::optional<Record> SSTable::get(const std::string& key) const {
    if (!may_contain(key)) return std::nullopt;
    return get_no_bloom(key);
}

SSTable::Iterator::Iterator(const SSTable* sst) : sst_(sst) {
    if (!sst_->index_.empty()) load_block(0);
}

void SSTable::Iterator::load_block(std::size_t i) {
    block_idx_ = i;
    block_ = sst_->read_block_records(i);
    pos_ = 0;
}

bool SSTable::Iterator::valid() const {
    return block_idx_ < sst_->index_.size() && pos_ < block_.size();
}

void SSTable::Iterator::next() {
    ++pos_;
    if (pos_ >= block_.size()) {
        if (block_idx_ + 1 < sst_->index_.size()) {
            load_block(block_idx_ + 1);
        } else {
            block_idx_ = sst_->index_.size();  // exhausted -> invalid
        }
    }
}

}  // namespace lsmkv
