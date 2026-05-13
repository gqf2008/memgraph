# Jepsen Tests for Rust Memgraph

This directory contains the Jepsen distributed consistency testing setup for the Rust rewrite of Memgraph.

## Background

The original C++ Memgraph has Jepsen tests in `../../tests/jepsen/`. This directory provides the bridge to run those same tests (and new ones) against the Rust binary.

## Prerequisites

- Docker + Docker Compose
- Clojure/Leiningen (for Jepsen)
- Rust toolchain (for building the binary)

## Quick Start

```bash
# Build the Rust binary
cd ../..
cargo build --release -p mgserver

# Run a single-node Memgraph for quick testing
cd rust/jepsen
docker-compose up -d memgraph

# Run a basic Jepsen test (requires Leiningen installed)
cd ../../tests/jepsen
lein run test --workload bank --nodes n1 --time-limit 60
```

## Workloads

Jepsen tests verify the following properties under fault injection:

| Workload | Property Tested | Description |
|----------|----------------|-------------|
| Bank | Consistency | Transfer money between accounts; total must remain constant |
| Counter | Linearizability | Increment a shared counter; reads must be monotonic |
| Set | Set semantics | Add elements to a set; membership must be consistent |
| Register | Linearizability | Read/write a single register; last-write-wins |

## Fault Injection (Nemesis)

Jepsen injects the following faults during tests:

- **Partition**: Network partitions between nodes
- **Kill**: SIGKILL the memgraph process
- **Pause**: SIGSTOP/SIGCONT to freeze the process
- **Clock skew**: Adjust system clock

## Running Full Suite

```bash
# Start 5-node cluster
cd rust/jepsen
docker-compose up -d

# Run bank test with partition nemesis
cd ../../tests/jepsen
lein run test \
  --workload bank \
  --nodes n1,n2,n3,n4,n5 \
  --nemesis partition \
  --time-limit 300 \
  --concurrency 50
```

## Rust-Native Consistency Tests

For faster feedback during development, run the Rust-native consistency tests:

```bash
cd rust
cargo test -p mgstorage consistency
cargo test -p integration-tests consistency
```

These test the storage engine directly without requiring Docker or Clojure.

## Architecture

The Rust Memgraph Jepsen setup uses the same protocol as C++ Memgraph:

- **Bolt protocol** for Cypher queries (same port 7687)
- **Same persistence format** (snapshots + WAL)
- **Same replication protocol** (mgrepl)

This means the existing Jepsen clients and workloads should work with minimal or no changes.

## CI Integration

The `rust.yml` GitHub Actions workflow builds the Rust binary. A future workflow will run Jepsen tests against it on every PR.
