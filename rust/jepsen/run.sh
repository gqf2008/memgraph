#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

usage() {
    echo "Usage: $0 [build|up|test|down|clean]"
    echo "  build  - Build Rust mgserver binary"
    echo "  up     - Start Jepsen cluster via docker-compose"
    echo "  test   - Run Jepsen tests against Rust binary"
    echo "  down   - Stop Jepsen cluster"
    echo "  clean  - Stop cluster and remove volumes"
    exit 1
}

cmd="${1:-}"

build() {
    echo "=== Building Rust mgserver ==="
    cd "$REPO_ROOT/rust"
    cargo build --release -p mgserver
    echo "Binary: $REPO_ROOT/rust/target/release/mgserver"
}

up() {
    echo "=== Starting Jepsen cluster ==="
    cd "$SCRIPT_DIR"
    docker-compose up -d n1 n2 n3 n4 n5
    echo "Waiting for nodes to start..."
    sleep 5
    docker-compose ps
}

test_jepsen() {
    echo "=== Running Jepsen tests ==="
    cd "$REPO_ROOT/tests/jepsen"
    lein run test \
        --workload bank \
        --nodes n1,n2,n3,n4,n5 \
        --time-limit 60 \
        --concurrency 10
}

down() {
    echo "=== Stopping Jepsen cluster ==="
    cd "$SCRIPT_DIR"
    docker-compose down
}

clean() {
    echo "=== Cleaning Jepsen cluster ==="
    cd "$SCRIPT_DIR"
    docker-compose down -v
    docker system prune -f
}

case "$cmd" in
    build) build ;;
    up) up ;;
    test) test_jepsen ;;
    down) down ;;
    clean) clean ;;
    *) usage ;;
esac
