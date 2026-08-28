#include "lsmkv/vlog.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <system_error>
#include <utility>

#include "lsmkv/encoding.h"

namespace lsmkv {

namespace {

// Write the whole buffer at off, retrying short writes and EINTR.
void pwrite_all(int fd, const std::string& buf, std::uint64_t off) {
    std::size_t written = 0;
    while (written < buf.size()) {
        ssize_t n = ::pwrite(fd, buf.data() + written, buf.size() - written,
                             off + written);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "vlog pwrite");
        }
        written += static_cast<std::size_t>(n);
    }
}

std::string pread_exact(int fd, std::uint64_t off, std::size_t len) {
    std::string buf(len, '\0');
    std::size_t got = 0;
    while (got < len) {
        ssize_t n = ::pread(fd, buf.data() + got, len - got, off + got);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::system_error(errno, std::generic_category(), "vlog pread");
        }
        if (n == 0) throw std::runtime_error("vlog pread: unexpected EOF");
        got += static_cast<std::size_t>(n);
    }
    return buf;
}

}  // namespace

ValueLog::ValueLog(std::string path) : path_(std::move(path)) {
    fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        throw std::system_error(errno, std::generic_category(),
                                "vlog open: " + path_);
    }
    struct stat st{};
    if (::fstat(fd_, &st) != 0) {
        int e = errno;
        ::close(fd_);
        fd_ = -1;
        throw std::system_error(e, std::generic_category(), "vlog fstat");
    }
    // Append after whatever is already there. Any unreferenced tail left by an
    // interrupted flush is inert -- no SSTable points at it -- and is simply
    // written past. (Reclaiming it is the job of a future garbage collector.)
    append_off_ = static_cast<std::uint64_t>(st.st_size);
}

ValueLog::~ValueLog() {
    if (fd_ >= 0) ::close(fd_);
}

ValuePtr ValueLog::append(const std::string& key, const std::string& value) {
    if (key.size() > UINT32_MAX || value.size() > UINT32_MAX) {
        throw std::length_error("vlog entry field exceeds 4 GiB");
    }
    std::string rec;
    rec.reserve(4 + key.size() + 4 + value.size());
    enc::put_u32_le(rec, static_cast<std::uint32_t>(key.size()));
    rec.append(key);
    enc::put_u32_le(rec, static_cast<std::uint32_t>(value.size()));
    rec.append(value);

    std::uint64_t rec_off = append_off_;
    pwrite_all(fd_, rec, rec_off);
    append_off_ += rec.size();

    // Point straight at the value payload, past [klen:4][key][vlen:4].
    ValuePtr ptr;
    ptr.offset = rec_off + 4 + key.size() + 4;
    ptr.len = static_cast<std::uint32_t>(value.size());
    return ptr;
}

void ValueLog::sync() {
    if (::fsync(fd_) != 0) {
        throw std::system_error(errno, std::generic_category(),
                                "vlog fsync: " + path_);
    }
}

std::string ValueLog::read(const ValuePtr& ptr) const {
    return pread_exact(fd_, ptr.offset, ptr.len);
}

}  // namespace lsmkv
