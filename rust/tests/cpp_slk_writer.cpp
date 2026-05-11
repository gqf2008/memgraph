// Minimal standalone C++ SLK writer for cross-language compatibility testing.
// This program produces SLK-framed bytes that match the Rust mgslk crate's
// wire format exactly.  It uses only standard headers and does NOT depend on
// the Memgraph C++ codebase or /opt/toolchain-v7.
//
// Wire format (little-endian):
//   [u32 LE segment_size][payload bytes...][u32 LE footer=0]
//
// Payload layout for this test:
//   u64     value  (42)
//   String  value  (u64 len + utf8 bytes)  ("hello")
//   Vec<u32> value (u64 len + u32 items LE) ([1, 2, 3])

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

// Write a little-endian value of any integer type to a byte vector.
template <typename T>
static void write_le(std::vector<uint8_t> &out, T value) {
    static_assert(std::is_integral<T>::value, "T must be integral");
    for (size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFF));
    }
}

// SLK segment framing constants.
static constexpr uint32_t K_SEGMENT_MAX_DATA_SIZE = 262144;
static constexpr uint32_t K_FOOTER = 0;

// Build an SLK-framed byte stream from raw payload bytes.
static std::vector<uint8_t> build_slk_frame(const std::vector<uint8_t> &payload) {
    std::vector<uint8_t> out;
    out.reserve(payload.size() + 8);

    if (payload.size() <= K_SEGMENT_MAX_DATA_SIZE) {
        // Single segment.
        write_le(out, static_cast<uint32_t>(payload.size()));
        out.insert(out.end(), payload.begin(), payload.end());
        write_le(out, K_FOOTER);
    } else {
        // Multi-segment (not needed for this small test, but kept for completeness).
        size_t offset = 0;
        while (offset < payload.size()) {
            size_t chunk = std::min<size_t>(K_SEGMENT_MAX_DATA_SIZE, payload.size() - offset);
            write_le(out, static_cast<uint32_t>(chunk));
            out.insert(out.end(), payload.begin() + offset, payload.begin() + offset + chunk);
            offset += chunk;
        }
        write_le(out, K_FOOTER);
    }
    return out;
}

int main(int argc, char **argv) {
    const char *out_path = "cpp_slk_output.bin";
    if (argc >= 2) {
        out_path = argv[1];
    }

    std::vector<uint8_t> payload;

    // 1. u64 value = 42
    write_le(payload, static_cast<uint64_t>(42));

    // 2. String value = "hello"
    const std::string hello = "hello";
    write_le(payload, static_cast<uint64_t>(hello.size()));
    payload.insert(payload.end(), hello.begin(), hello.end());

    // 3. Vec<u32> = [1, 2, 3]
    write_le(payload, static_cast<uint64_t>(3)); // length
    write_le(payload, static_cast<uint32_t>(1));
    write_le(payload, static_cast<uint32_t>(2));
    write_le(payload, static_cast<uint32_t>(3));

    // Frame with SLK segments and write to file.
    std::vector<uint8_t> framed = build_slk_frame(payload);

    FILE *f = std::fopen(out_path, "wb");
    if (!f) {
        std::perror("fopen");
        return 1;
    }
    if (std::fwrite(framed.data(), 1, framed.size(), f) != framed.size()) {
        std::perror("fwrite");
        std::fclose(f);
        return 1;
    }
    std::fclose(f);

    std::printf("Wrote %zu bytes to %s\n", framed.size(), out_path);
    return 0;
}
