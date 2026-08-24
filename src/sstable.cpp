#include "lsmkv/sstable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "lsmkv/encoding.h"

namespace lsmkv {

namespace {

constexpr std::size_t kFooterSize = 32;  // 3 x u64 sizes + 8-byte magic
constexpr char kMagic[8] = {'L', 'S', 'M', 'K', 'V', 'S', 'S', 'T'};

// Write the whole buffer to a new file and fsync it durable before returning.
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

// Read exactly len bytes at absolute offset off. pread does not move a shared
// file offset, so concurrent reads on one fd are safe.
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

// Parse one data-block record from a slice that begins exactly at the record.
Record parse_record(const std::string& s) {
    if (s.size() < 1 + 4) throw std::runtime_error("SSTable: short record header");
    Record rec;
    rec.op = static_cast<Op>(static_cast<unsigned char>(s[0]));
    std::size_t p = 1;
    std::uint32_t klen = enc::read_u32_le(s.data() + p);
    p += 4;
    if (p + klen + 4 > s.size()) throw std::runtime_error("SSTable: bad key length");
    rec.key = s.substr(p, klen);
    p += klen;
    std::uint32_t vlen = enc::read_u32_le(s.data() + p);
    p += 4;
    if (p + vlen > s.size()) throw std::runtime_error("SSTable: bad value length");
    rec.value = s.substr(p, vlen);
    return rec;
}

}  // namespace

std::uint64_t SSTable::build(const std::string& path,
                             const std::vector<Record>& sorted) {
    BloomFilter bloom(sorted.empty() ? 1 : sorted.size(), 0.01);
    std::string data;
    std::string index;
    for (const Record& rec : sorted) {
        if (rec.key.size() > UINT32_MAX || rec.value.size() > UINT32_MAX) {
            throw std::length_error("SSTable record field exceeds 4 GiB");
        }
        std::uint64_t off = data.size();
        data.push_back(static_cast<char>(rec.op));
        enc::put_u32_le(data, static_cast<std::uint32_t>(rec.key.size()));
        data.append(rec.key);
        enc::put_u32_le(data, static_cast<std::uint32_t>(rec.value.size()));
        data.append(rec.value);

        enc::put_u32_le(index, static_cast<std::uint32_t>(rec.key.size()));
        index.append(rec.key);
        enc::put_u64_le(index, off);

        bloom.add(rec.key);
    }
    std::string bloom_bytes = bloom.serialize();

    std::string footer;
    enc::put_u64_le(footer, data.size());
    enc::put_u64_le(footer, index.size());
    enc::put_u64_le(footer, bloom_bytes.size());
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

SSTable::SSTable(std::string path)
    : path_(std::move(path)), bloom_(1, 0.01) {
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
    if (footer.compare(24, 8, kMagic, sizeof(kMagic)) != 0) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("SSTable: bad magic: " + path_);
    }
    std::uint64_t data_size = enc::read_u64_le(footer.data());
    std::uint64_t index_size = enc::read_u64_le(footer.data() + 8);
    std::uint64_t bloom_size = enc::read_u64_le(footer.data() + 16);
    if (data_size + index_size + bloom_size + kFooterSize != file_size) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("SSTable: block sizes disagree with file: " + path_);
    }
    data_size_ = data_size;
    file_size_ = file_size;

    // Load the dense index into memory (already sorted by key on disk).
    std::string idx = pread_exact(fd_, data_size, index_size);
    std::size_t p = 0;
    while (p < idx.size()) {
        if (p + 4 > idx.size()) throw std::runtime_error("SSTable: torn index");
        std::uint32_t klen = enc::read_u32_le(idx.data() + p);
        p += 4;
        if (p + klen + 8 > idx.size()) throw std::runtime_error("SSTable: torn index");
        IndexEntry e;
        e.key = idx.substr(p, klen);
        p += klen;
        e.offset = enc::read_u64_le(idx.data() + p);
        p += 8;
        index_.push_back(std::move(e));
    }

    std::string bloom_bytes = pread_exact(fd_, data_size + index_size, bloom_size);
    bloom_ = BloomFilter::deserialize(bloom_bytes);
}

SSTable::~SSTable() {
    if (fd_ >= 0) ::close(fd_);
    // Deleted only after the last handle is gone, so a reader holding this
    // SSTable via a snapshot never has the file pulled out from under it.
    if (obsolete_) ::unlink(path_.c_str());
}

Record SSTable::record_at(std::size_t i) const {
    std::uint64_t start = index_[i].offset;
    std::uint64_t end = (i + 1 < index_.size()) ? index_[i + 1].offset : data_size_;
    std::string slice = pread_exact(fd_, start, static_cast<std::size_t>(end - start));
    return parse_record(slice);
}

bool SSTable::may_contain(const std::string& key) const {
    return bloom_.maybe_contains(key);
}

std::optional<Record> SSTable::get_no_bloom(const std::string& key) const {
    auto it = std::lower_bound(
        index_.begin(), index_.end(), key,
        [](const IndexEntry& e, const std::string& k) { return e.key < k; });
    if (it == index_.end() || it->key != key) {
        return std::nullopt;
    }
    return record_at(static_cast<std::size_t>(it - index_.begin()));
}

std::optional<Record> SSTable::get(const std::string& key) const {
    if (!may_contain(key)) {
        return std::nullopt;  // Bloom proves absence: skip the file entirely
    }
    return get_no_bloom(key);
}

}  // namespace lsmkv
