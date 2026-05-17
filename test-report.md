# Memgraph Rust Test Report

**Generated:** 2026-05-17T16:46:47Z
**Workspace:** /Users/sqb/Documents/GitHub/memgraph

## Summary

| Metric | Count |
|--------|-------|
| Total tests executed | 1272 |
| Passed | 1271 |
| Failed | 1 |
| Pass rate | 99.9% |

## Results by Category

| Category | Passed | Failed | Status |
|----------|--------|--------|--------|
| Unit tests | 1227 | 0 | ✅ |
| E2E Cypher | 16 | 1 | ❌ |
| Cross-format (C++ ↔ Rust) | 10 | 0 | ✅ |
| Durability migration (v14–v35) | 3 | 0 | ✅ |
| Jepsen correctness | 11 | 0 | ✅ |
| Replication | 4 | 0 | ✅ |

## C++ Format Compatibility

| Format Version | Status |
|----------------|--------|
| v14–v34 | ✅ Full read/convert/replay support |
| v35 | ✅ Format detection, SECTION_OFFSETS, SECTION_DELTA framing, snapshot batch support. Remaining: v35-specific WAL delta variants (0x00, 0x04). |

## C API Coverage

334/334 functions from `mg_procedure.h` implemented. Zero functions return `NotYetImplemented`.

## Known Gaps

| # | Item | Priority |
|---|------|----------|
| 1 | v35 WAL delta variants (0x00, 0x04) — need per-delta C++ format decode | P2 |
| 2 | Snapshot batch sections correctly merged; main vertex section may be empty for v35 | — |

## Performance Benchmarks

6 Criterion benchmark suites available: parser, storage write, query execution, Bolt encode/decode, WAL durability, HNSW vector search.

```bash
cd rust && cargo bench -p benches
```

## Module Alignment

All 22 C++ modules rewritten in Rust with zero C/C++ dependencies.
See `rust/RUST_PARITY.md` for the full module alignment table.
