// Standalone C++ PropertyValue SLK writer for cross-language compatibility.
//
// Writes a sequence of PropertyValue objects in the exact SLK format that
// the Rust mgcore crate expects:
//   [u8 tag][type-specific payload...]
//
// Tags (matching mgp_value_type):
//   0 = Null
//   1 = Bool     [u8 0/1]
//   2 = Int      [i64 LE]
//   3 = Double   [f64 bits as u64 LE]
//   4 = String   [u64 len LE][utf8 bytes...]
//   5 = List     [u64 len LE][items...]
//   6 = Map      [u64 len LE][(String key, PropertyValue value)...]
//
// Compile: g++ -std=c++17 -O2 cpp_property_value_writer.cpp -o cpp_property_value_writer
// Usage:   ./cpp_property_value_writer <out.bin>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

// ─── Little-endian writers ──────────────────────────────────────────────────

template <typename T>
static void write_le(std::vector<uint8_t> &out, T value) {
    static_assert(std::is_integral<T>::value, "T must be integral");
    for (size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xFF));
    }
}

static void write_u8(std::vector<uint8_t> &out, uint8_t v) { out.push_back(v); }
static void write_u64(std::vector<uint8_t> &out, uint64_t v) { write_le(out, v); }
static void write_i64(std::vector<uint8_t> &out, int64_t v) {
    write_u64(out, static_cast<uint64_t>(v));
}
static void write_f64(std::vector<uint8_t> &out, double v) {
    union { double d; uint64_t u; } x;
    x.d = v;
    write_u64(out, x.u);
}
static void write_bool(std::vector<uint8_t> &out, bool v) { write_u8(out, v ? 1 : 0); }
static void write_string(std::vector<uint8_t> &out, const std::string &s) {
    write_u64(out, static_cast<uint64_t>(s.size()));
    out.insert(out.end(), s.begin(), s.end());
}

// ─── SLK framing ────────────────────────────────────────────────────────────

static constexpr uint32_t K_SEGMENT_MAX_DATA_SIZE = 262144;

static std::vector<uint8_t> build_slk_frame(const std::vector<uint8_t> &payload) {
    std::vector<uint8_t> out;
    out.reserve(payload.size() + 8);
    size_t offset = 0;
    while (offset < payload.size()) {
        size_t chunk = payload.size() - offset;
        if (chunk > K_SEGMENT_MAX_DATA_SIZE) chunk = K_SEGMENT_MAX_DATA_SIZE;
        write_le(out, static_cast<uint32_t>(chunk));
        out.insert(out.end(), payload.begin() + offset, payload.begin() + offset + chunk);
        offset += chunk;
    }
    write_le(out, static_cast<uint32_t>(0)); // footer
    return out;
}

// ─── PropertyValue SLK encoding ─────────────────────────────────────────────

// Forward declaration for recursive types
static void write_property_value(std::vector<uint8_t> &out, int tag, const std::string &s);
static void write_property_value(std::vector<uint8_t> &out, int tag,
                                 const std::vector<std::pair<int, std::string>> &list_items);
static void write_property_value(std::vector<uint8_t> &out, int tag,
                                 const std::vector<std::pair<std::string, std::pair<int, std::string>>> &map_entries);

static void write_property_value(std::vector<uint8_t> &out, int tag) {
    write_u8(out, static_cast<uint8_t>(tag));
}

static void write_property_value(std::vector<uint8_t> &out, int tag, bool v) {
    write_u8(out, static_cast<uint8_t>(tag));
    write_bool(out, v);
}

static void write_property_value(std::vector<uint8_t> &out, int tag, int64_t v) {
    write_u8(out, static_cast<uint8_t>(tag));
    write_i64(out, v);
}

static void write_property_value(std::vector<uint8_t> &out, int tag, double v) {
    write_u8(out, static_cast<uint8_t>(tag));
    write_f64(out, v);
}

static void write_property_value(std::vector<uint8_t> &out, int tag, const std::string &s) {
    write_u8(out, static_cast<uint8_t>(tag));
    write_string(out, s);
}

// List of (tag, string_value) pairs. For scalar types only.
static void write_property_value(std::vector<uint8_t> &out, int tag,
                                 const std::vector<std::pair<int, std::string>> &items) {
    write_u8(out, static_cast<uint8_t>(tag));
    write_u64(out, static_cast<uint64_t>(items.size()));
    for (const auto &item : items) {
        // Each item is a PropertyValue - write its tag then value
        write_u8(out, static_cast<uint8_t>(item.first));
        // For simplicity, this test only uses String items in lists
        write_string(out, item.second);
    }
}

// Map: vector of (key, (tag, value_string)) pairs
static void write_property_value(std::vector<uint8_t> &out, int tag,
                                 const std::vector<std::pair<std::string, std::pair<int, std::string>>> &entries) {
    write_u8(out, static_cast<uint8_t>(tag));
    write_u64(out, static_cast<uint64_t>(entries.size()));
    for (const auto &entry : entries) {
        write_string(out, entry.first);
        write_u8(out, static_cast<uint8_t>(entry.second.first));
        write_string(out, entry.second.second);
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

int main(int argc, char **argv) {
    const char *out_path = "cpp_property_values.bin";
    if (argc >= 2) {
        out_path = argv[1];
    }

    std::vector<uint8_t> payload;

    // 1. Null (tag 0, no payload)
    write_property_value(payload, 0);

    // 2. Bool true (tag 1)
    write_property_value(payload, 1, true);

    // 3. Bool false (tag 1)
    write_property_value(payload, 1, false);

    // 4. Int 42 (tag 2)
    write_property_value(payload, 2, static_cast<int64_t>(42));

    // 5. Int -1 (tag 2)
    write_property_value(payload, 2, static_cast<int64_t>(-1));

    // 6. Int i64::MAX (tag 2)
    write_property_value(payload, 2, static_cast<int64_t>(9223372036854775807LL));

    // 7. Double 3.14 (tag 3)
    write_property_value(payload, 3, 3.14);

    // 8. Double -0.0 (tag 3)
    write_property_value(payload, 3, -0.0);

    // 9. String "hello" (tag 4)
    write_property_value(payload, 4, std::string("hello"));

    // 10. String empty (tag 4)
    write_property_value(payload, 4, std::string(""));

    // 11. String unicode (tag 4)
    write_property_value(payload, 4, std::string("🦀🚀"));

    // 12. List [String("a"), String("b")] (tag 5)
    write_property_value(payload, 5,
                         std::vector<std::pair<int, std::string>>{
                             {4, "a"},
                             {4, "b"},
                         });

    // 13. List empty (tag 5)
    write_property_value(payload, 5,
                         std::vector<std::pair<int, std::string>>{});

    // 14. Map {"key1": String("value1"), "key2": String("value2")} (tag 6)
    write_property_value(payload, 6,
                         std::vector<std::pair<std::string, std::pair<int, std::string>>>{
                             {"key1", {4, "value1"}},
                             {"key2", {4, "value2"}},
                         });

    // 15. Map empty (tag 6)
    write_property_value(payload, 6,
                         std::vector<std::pair<std::string, std::pair<int, std::string>>>{});

    // Frame and write
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
