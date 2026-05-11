// Standalone C++ SLK bidirectional compatibility tester.
//
// Uses the same deterministic LCG as the Rust mgslk test to generate
// Vec<Option<(i64, String)>> structures and verifies that Rust and C++
// produce byte-for-byte identical SLK output.
//
// Compile: g++ -std=c++17 -O2 cpp_slk_compat.cpp -o cpp_slk_compat
//
// Usage:
//   ./cpp_slk_compat write <count> <seed> <out.bin>
//   ./cpp_slk_compat read  <count> <seed> <in.bin>
//   ./cpp_slk_compat verify <count> <seed> <in.bin>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <optional>

// ─── LCG (must match Rust exactly) ──────────────────────────────────────────

static uint64_t lcg(uint64_t *seed) {
    const uint64_t A = 6364136223846793005ULL;
    const uint64_t C = 1442695040888963407ULL;
    *seed = (*seed) * A + C;
    return *seed;
}

// ─── SLK raw payload writer ─────────────────────────────────────────────────

struct SlkPayloadWriter {
    std::vector<uint8_t> buf;

    void write_u8(uint8_t v) { buf.push_back(v); }

    void write_u32(uint32_t v) {
        for (int i = 0; i < 4; ++i)
            buf.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
    }

    void write_u64(uint64_t v) {
        for (int i = 0; i < 8; ++i)
            buf.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFF));
    }

    void write_i64(int64_t v) {
        write_u64(static_cast<uint64_t>(v));
    }

    void write_bool(bool v) { write_u8(v ? 1 : 0); }

    void write_string(const std::string &s) {
        write_u64(static_cast<uint64_t>(s.size()));
        buf.insert(buf.end(), s.begin(), s.end());
    }

    void write_option_tuple_i64_string(
        const std::optional<std::pair<int64_t, std::string>> &opt
    ) {
        if (opt.has_value()) {
            write_bool(true);
            write_i64(opt.value().first);
            write_string(opt.value().second);
        } else {
            write_bool(false);
        }
    }

    void write_vec_option_tuple_i64_string(
        const std::vector<std::optional<std::pair<int64_t, std::string>>> &vec
    ) {
        write_u64(static_cast<uint64_t>(vec.size()));
        for (const auto &item : vec) {
            write_option_tuple_i64_string(item);
        }
    }
};

// ─── SLK framed writer (segments + footer) ──────────────────────────────────

static constexpr uint32_t K_SEGMENT_MAX_DATA_SIZE = 262144;
static constexpr uint32_t K_FOOTER = 0;

static std::vector<uint8_t> build_slk_frame(const std::vector<uint8_t> &payload) {
    std::vector<uint8_t> out;
    out.reserve(payload.size() + 8);
    size_t offset = 0;
    while (offset < payload.size()) {
        size_t chunk = payload.size() - offset;
        if (chunk > K_SEGMENT_MAX_DATA_SIZE) chunk = K_SEGMENT_MAX_DATA_SIZE;
        // segment header: u32 LE size
        for (int i = 0; i < 4; ++i)
            out.push_back(static_cast<uint8_t>((chunk >> (8 * i)) & 0xFF));
        out.insert(out.end(), payload.begin() + offset,
                   payload.begin() + offset + chunk);
        offset += chunk;
    }
    // footer
    for (int i = 0; i < 4; ++i)
        out.push_back(0);
    return out;
}

// ─── SLK payload reader (reads from a contiguous byte buffer) ───────────────

struct SlkPayloadReader {
    const uint8_t *data;
    size_t len;
    size_t pos = 0;

    bool can_read(size_t n) const { return pos + n <= len; }

    uint8_t read_u8() {
        if (!can_read(1)) {
            std::fprintf(stderr, "SLK read overflow at pos %zu\n", pos);
            std::exit(1);
        }
        return data[pos++];
    }

    uint32_t read_u32() {
        if (!can_read(4)) {
            std::fprintf(stderr, "SLK read overflow (u32) at pos %zu\n", pos);
            std::exit(1);
        }
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i)
            v |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
        pos += 4;
        return v;
    }

    uint64_t read_u64() {
        if (!can_read(8)) {
            std::fprintf(stderr, "SLK read overflow (u64) at pos %zu\n", pos);
            std::exit(1);
        }
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(data[pos + i]) << (8 * i);
        pos += 8;
        return v;
    }

    int64_t read_i64() {
        return static_cast<int64_t>(read_u64());
    }

    bool read_bool() {
        return read_u8() != 0;
    }

    std::string read_string() {
        uint64_t size = read_u64();
        if (!can_read(size)) {
            std::fprintf(stderr, "SLK read overflow (string, size=%lu) at pos %zu\n",
                         static_cast<unsigned long>(size), pos);
            std::exit(1);
        }
        std::string s;
        s.reserve(size);
        for (uint64_t i = 0; i < size; ++i) s.push_back(static_cast<char>(data[pos + i]));
        pos += size;
        return s;
    }

    std::optional<std::pair<int64_t, std::string>> read_option_tuple_i64_string() {
        bool exists = read_bool();
        if (!exists) return std::nullopt;
        int64_t i = read_i64();
        std::string s = read_string();
        return std::make_optional(std::make_pair(i, s));
    }

    std::vector<std::optional<std::pair<int64_t, std::string>>>
    read_vec_option_tuple_i64_string() {
        uint64_t size = read_u64();
        std::vector<std::optional<std::pair<int64_t, std::string>>> vec;
        vec.reserve(size);
        for (uint64_t i = 0; i < size; ++i) {
            vec.push_back(read_option_tuple_i64_string());
        }
        return vec;
    }
};

// ─── SLK framed reader ──────────────────────────────────────────────────────

// Read all segment payloads into a single contiguous buffer.
static std::vector<uint8_t> read_slk_frame(const std::vector<uint8_t> &framed) {
    std::vector<uint8_t> payload;
    size_t offset = 0;
    while (offset + 4 <= framed.size()) {
        uint32_t seg_size = 0;
        for (int i = 0; i < 4; ++i)
            seg_size |= static_cast<uint32_t>(framed[offset + i]) << (8 * i);
        offset += 4;
        if (seg_size == 0) break; // footer
        if (offset + seg_size > framed.size()) {
            std::fprintf(stderr, "SLK segment extends past end of file\n");
            std::exit(1);
        }
        payload.insert(payload.end(), framed.begin() + offset,
                       framed.begin() + offset + seg_size);
        offset += seg_size;
    }
    return payload;
}

// ─── Data generation (same as Rust) ─────────────────────────────────────────

static std::vector<std::optional<std::pair<int64_t, std::string>>>
generate_one(uint64_t *seed) {
    uint64_t len = lcg(seed) % 20;
    std::vector<std::optional<std::pair<int64_t, std::string>>> vec;
    vec.reserve(len);
    for (uint64_t i = 0; i < len; ++i) {
        if (lcg(seed) % 3 == 0) {
            vec.push_back(std::nullopt);
        } else {
            int64_t iv = static_cast<int64_t>(lcg(seed));
            std::string s = "v" + std::to_string(lcg(seed));
            vec.push_back(std::make_optional(std::make_pair(iv, s)));
        }
    }
    return vec;
}

// ─── File I/O helpers ───────────────────────────────────────────────────────

static std::vector<uint8_t> read_file(const char *path) {
    FILE *f = std::fopen(path, "rb");
    if (!f) {
        std::perror(path);
        std::exit(1);
    }
    std::fseek(f, 0, SEEK_END);
    long sz = std::ftell(f);
    std::fseek(f, 0, SEEK_SET);
    std::vector<uint8_t> data(sz);
    if (std::fread(data.data(), 1, sz, f) != static_cast<size_t>(sz)) {
        std::perror("fread");
        std::exit(1);
    }
    std::fclose(f);
    return data;
}

static void write_file(const char *path, const std::vector<uint8_t> &data) {
    FILE *f = std::fopen(path, "wb");
    if (!f) {
        std::perror(path);
        std::exit(1);
    }
    if (std::fwrite(data.data(), 1, data.size(), f) != data.size()) {
        std::perror("fwrite");
        std::exit(1);
    }
    std::fclose(f);
}

// ─── Commands ───────────────────────────────────────────────────────────────

static int cmd_write(int argc, char **argv) {
    if (argc < 5) {
        std::fprintf(stderr, "Usage: %s write <count> <seed> <out.bin>\n", argv[0]);
        return 1;
    }
    uint64_t count = std::strtoull(argv[2], nullptr, 10);
    uint64_t seed  = std::strtoull(argv[3], nullptr, 10);
    const char *out_path = argv[4];

    SlkPayloadWriter pw;
    for (uint64_t n = 0; n < count; ++n) {
        auto val = generate_one(&seed);
        pw.write_vec_option_tuple_i64_string(val);
    }

    auto framed = build_slk_frame(pw.buf);
    write_file(out_path, framed);
    std::printf("Wrote %lu structures (%lu bytes framed) to %s\n",
                static_cast<unsigned long>(count),
                static_cast<unsigned long>(framed.size()), out_path);
    return 0;
}

static int cmd_read(int argc, char **argv) {
    if (argc < 5) {
        std::fprintf(stderr, "Usage: %s read <count> <seed> <in.bin>\n", argv[0]);
        return 1;
    }
    uint64_t count = std::strtoull(argv[2], nullptr, 10);
    uint64_t seed  = std::strtoull(argv[3], nullptr, 10);
    const char *in_path = argv[4];

    auto framed = read_file(in_path);
    auto payload = read_slk_frame(framed);

    SlkPayloadReader reader;
    reader.data = payload.data();
    reader.len = payload.size();

    uint64_t ok = 0;
    for (uint64_t n = 0; n < count; ++n) {
        auto expected = generate_one(&seed);
        auto actual = reader.read_vec_option_tuple_i64_string();
        if (expected.size() != actual.size()) {
            std::printf("FAIL iteration %lu: size mismatch %lu vs %lu\n",
                        static_cast<unsigned long>(n),
                        static_cast<unsigned long>(expected.size()),
                        static_cast<unsigned long>(actual.size()));
            return 1;
        }
        bool match = true;
        for (size_t i = 0; i < expected.size(); ++i) {
            if (expected[i].has_value() != actual[i].has_value()) {
                match = false; break;
            }
            if (expected[i].has_value()) {
                if (expected[i]->first != actual[i]->first ||
                    expected[i]->second != actual[i]->second) {
                    match = false; break;
                }
            }
        }
        if (!match) {
            std::printf("FAIL iteration %lu: value mismatch\n",
                        static_cast<unsigned long>(n));
            return 1;
        }
        ++ok;
    }
    std::printf("OK: %lu/%lu structures verified\n",
                static_cast<unsigned long>(ok),
                static_cast<unsigned long>(count));
    return 0;
}

static int cmd_verify(int argc, char **argv) {
    if (argc < 5) {
        std::fprintf(stderr, "Usage: %s verify <count> <seed> <in.bin>\n", argv[0]);
        return 1;
    }
    uint64_t count = std::strtoull(argv[2], nullptr, 10);
    uint64_t seed  = std::strtoull(argv[3], nullptr, 10);
    const char *in_path = argv[4];

    auto framed = read_file(in_path);
    auto payload = read_slk_frame(framed);

    // Also write our own copy and compare byte-for-byte
    SlkPayloadWriter pw;
    for (uint64_t n = 0; n < count; ++n) {
        auto val = generate_one(&seed);
        pw.write_vec_option_tuple_i64_string(val);
    }
    auto our_framed = build_slk_frame(pw.buf);

    if (framed != our_framed) {
        std::printf("FAIL: byte-level mismatch. Expected %lu bytes, got %lu bytes\n",
                    static_cast<unsigned long>(our_framed.size()),
                    static_cast<unsigned long>(framed.size()));
        // Find first differing byte
        size_t limit = framed.size() < our_framed.size() ? framed.size() : our_framed.size();
        for (size_t i = 0; i < limit; ++i) {
            if (framed[i] != our_framed[i]) {
                std::printf("First difference at offset %zu: expected 0x%02x, got 0x%02x\n",
                            i, our_framed[i], framed[i]);
                break;
            }
        }
        return 1;
    }
    std::printf("OK: %lu bytes byte-for-byte identical\n",
                static_cast<unsigned long>(framed.size()));
    return 0;
}

// ─── Main ───────────────────────────────────────────────────────────────────

int main(int argc, char **argv) {
    if (argc < 2) {
        std::fprintf(stderr,
            "Usage:\n"
            "  %s write  <count> <seed> <out.bin>   -- write SLK file\n"
            "  %s read   <count> <seed> <in.bin>    -- read and verify values\n"
            "  %s verify <count> <seed> <in.bin>    -- byte-for-byte compare\n",
            argv[0], argv[0], argv[0]);
        return 1;
    }

    const char *cmd = argv[1];
    if (std::strcmp(cmd, "write") == 0) return cmd_write(argc, argv);
    if (std::strcmp(cmd, "read") == 0)  return cmd_read(argc, argv);
    if (std::strcmp(cmd, "verify") == 0) return cmd_verify(argc, argv);

    std::fprintf(stderr, "Unknown command: %s\n", cmd);
    return 1;
}
