// Minimal command-line interface to the LSM key-value store.
//
//   lsmkv <dir> put <key> <value>
//   lsmkv <dir> get <key>
//   lsmkv <dir> del <key>
//
// Each invocation opens the store at <dir>, runs one operation, and exits.
#include <cstdio>
#include <string>

#include "lsmkv/db.h"

namespace {

int usage() {
    std::fprintf(stderr,
                 "usage:\n"
                 "  lsmkv <dir> put <key> <value>\n"
                 "  lsmkv <dir> get <key>\n"
                 "  lsmkv <dir> del <key>\n"
                 "  lsmkv <dir> scan <start> <end>\n");
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) return usage();
    std::string dir = argv[1];
    std::string cmd = argv[2];

    lsmkv::DB db(dir);

    if (cmd == "put") {
        if (argc != 5) return usage();
        db.put(argv[3], argv[4]);
        std::printf("OK\n");
        return 0;
    }
    if (cmd == "get") {
        if (argc != 4) return usage();
        auto v = db.get(argv[3]);
        if (!v) {
            std::fprintf(stderr, "(not found)\n");
            return 1;
        }
        std::fwrite(v->data(), 1, v->size(), stdout);
        std::printf("\n");
        return 0;
    }
    if (cmd == "del") {
        if (argc != 4) return usage();
        db.del(argv[3]);
        std::printf("OK\n");
        return 0;
    }
    if (cmd == "scan") {
        if (argc != 5) return usage();
        for (const auto& [key, value] : db.scan(argv[3], argv[4])) {
            std::fwrite(key.data(), 1, key.size(), stdout);
            std::fputc('\t', stdout);
            std::fwrite(value.data(), 1, value.size(), stdout);
            std::fputc('\n', stdout);
        }
        return 0;
    }
    return usage();
}
