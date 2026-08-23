#include "lsmkv/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "lsmkv/encoding.h"

namespace lsmkv {

namespace {

constexpr std::size_t kLenSize = sizeof(uint32_t);

// Write the whole buffer, retrying short and interrupted writes.
void write_all(int fd, const char* data, std::size_t len) {
    std::size_t written = 0;
    while (written < len) {
        ssize_t n = ::write(fd, data + written, len - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "WAL write");
        }
        written += static_cast<std::size_t>(n);
    }
}

}  // namespace

Wal::Wal(std::string path, bool truncate) : path_(std::move(path)) {
    int flags = O_WRONLY | O_CREAT | O_APPEND | (truncate ? O_TRUNC : 0);
    fd_ = ::open(path_.c_str(), flags, 0644);
    if (fd_ < 0) {
        throw std::system_error(errno, std::generic_category(),
                                "WAL open: " + path_);
    }
}

Wal::~Wal() { close_fd(); }

Wal::Wal(Wal&& other) noexcept : path_(std::move(other.path_)), fd_(other.fd_) {
    other.fd_ = -1;
}

Wal& Wal::operator=(Wal&& other) noexcept {
    if (this != &other) {
        close_fd();
        path_ = std::move(other.path_);
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

void Wal::close_fd() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

void Wal::append(const Record& rec) {
    if (rec.key.size() > UINT32_MAX || rec.value.size() > UINT32_MAX) {
        throw std::length_error("WAL record field exceeds 4 GiB");
    }
    std::string buf;
    buf.reserve(1 + kLenSize + rec.key.size() + kLenSize + rec.value.size());
    buf.push_back(static_cast<char>(rec.op));
    enc::put_u32_le(buf, static_cast<uint32_t>(rec.key.size()));
    buf.append(rec.key);
    enc::put_u32_le(buf, static_cast<uint32_t>(rec.value.size()));
    buf.append(rec.value);

    write_all(fd_, buf.data(), buf.size());
    // fsync makes the appended bytes durable before the caller sees success.
    if (::fsync(fd_) != 0) {
        throw std::system_error(errno, std::generic_category(), "WAL fsync");
    }
}

std::vector<Record> Wal::replay(const std::string& path) {
    std::vector<Record> records;
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) return records;  // no log yet
        throw std::system_error(errno, std::generic_category(),
                                "WAL replay open: " + path);
    }

    // Read the whole log into memory. A log is bounded by the memtable flush
    // threshold, so it stays small.
    std::string data;
    char chunk[64 * 1024];
    for (;;) {
        ssize_t n = ::read(fd, chunk, sizeof(chunk));
        if (n < 0) {
            if (errno == EINTR) continue;
            int e = errno;
            ::close(fd);
            throw std::system_error(e, std::generic_category(), "WAL read");
        }
        if (n == 0) break;
        data.append(chunk, static_cast<std::size_t>(n));
    }
    ::close(fd);

    std::size_t pos = 0;
    const std::size_t size = data.size();
    while (pos < size) {
        // op byte + key length.
        if (pos + 1 + kLenSize > size) break;  // torn record
        Op op = static_cast<Op>(static_cast<unsigned char>(data[pos]));
        std::size_t p = pos + 1;
        uint32_t klen = enc::read_u32_le(data.data() + p);
        p += kLenSize;
        if (p + klen > size) break;  // torn
        std::string key = data.substr(p, klen);
        p += klen;
        if (p + kLenSize > size) break;  // torn
        uint32_t vlen = enc::read_u32_le(data.data() + p);
        p += kLenSize;
        if (p + vlen > size) break;  // torn
        std::string value = data.substr(p, vlen);
        p += vlen;

        records.push_back(Record{op, std::move(key), std::move(value)});
        pos = p;
    }
    return records;
}

}  // namespace lsmkv
