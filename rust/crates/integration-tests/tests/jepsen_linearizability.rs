//! Linearizability tests for memgraph-rs.
//!
//! These tests verify single-node consistency properties: data persistence,
//! no fabricated values, and monotonic reads. Full distributed linearizability
//! across main+replica requires snapshot replication, which is not yet
//! implemented; new replicas only receive deltas from post-connection commits.

use std::collections::HashMap;
use std::time::{Duration, Instant};

mod nemesis;
use nemesis::{memgraph_binary, run_query_cli};

// ─── History Recording (reserved for future distributed linearizability tests) ──

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpType {
    Write,
    Read,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Operation {
    op_type: OpType,
    client_id: usize,
    key: String,
    value: i64,
    start: Instant,
    end: Instant,
}

#[allow(dead_code)]
struct History {
    ops: Vec<Operation>,
}

#[allow(dead_code)]
impl History {
    fn new() -> Self {
        Self { ops: Vec::new() }
    }

    fn record(&mut self, op: Operation) {
        self.ops.push(op);
    }
}

// ─── Linearizability Checker (reserved for future use) ─────────────────────

#[allow(dead_code)]
fn check_linearizable(history: &[Operation]) -> Result<(), String> {
    let mut by_key: HashMap<String, Vec<&Operation>> = HashMap::new();
    for op in history {
        by_key.entry(op.key.clone()).or_default().push(op);
    }

    for (key, ops) in by_key {
        let mut sorted: Vec<&Operation> = ops.iter().copied().collect();
        if sorted.len() > 8 {
            sorted.sort_by_key(|op| op.end);
            if let Err(e) = verify_register_order(&key, &sorted) {
                return Err(e);
            }
        } else {
            let mut found = false;
            let permutations = generate_permutations(&sorted);
            for perm in permutations {
                if is_valid_linearization(&perm) {
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(format!(
                    "no linearization found for key '{}' with {} ops",
                    key,
                    sorted.len()
                ));
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
fn verify_register_order(key: &str, ops: &[&Operation]) -> Result<(), String> {
    let mut current_value: i64 = 0;
    let mut last_write_end: Option<Instant> = None;

    for op in ops {
        match op.op_type {
            OpType::Write => {
                current_value = op.value;
                last_write_end = Some(op.end);
            }
            OpType::Read => {
                if op.value != current_value {
                    if let Some(lwe) = last_write_end {
                        if op.end > lwe {
                            return Err(format!(
                                "linearizability violation on key '{}': read saw {} but current is {} (read ended after last write)",
                                key, op.value, current_value
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn is_valid_linearization(ops: &[&Operation]) -> bool {
    let mut current_value: i64 = 0;
    for op in ops {
        match op.op_type {
            OpType::Write => {
                current_value = op.value;
            }
            OpType::Read => {
                if op.value != current_value {
                    return false;
                }
            }
        }
    }
    true
}

#[allow(dead_code)]
fn generate_permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.is_empty() {
        return vec![vec![]];
    }
    let mut result = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        rest.remove(i);
        for mut perm in generate_permutations(&rest) {
            perm.insert(0, items[i].clone());
            result.push(perm);
        }
    }
    result
}

// ─── Deterministic LCG ─────────────────────────────────────────────────────

fn lcg(seed: &mut u64) -> u64 {
    const A: u64 = 6364136223846793005;
    const C: u64 = 1442695040888963407;
    *seed = seed.wrapping_mul(A).wrapping_add(C);
    *seed
}

// ─── Test: No Fabricated Values ────────────────────────────────────────────

/// Write values via CLI, then verify that reads only return values that were
/// actually written (no fabricated values).
#[test]
fn test_no_fabricated_values() {
    let bin = memgraph_binary();
    if !bin.exists() {
        eprintln!("memgraph-rs binary not found at {:?}, skipping", bin);
        return;
    }

    let main_data = tempfile::tempdir().unwrap();

    // Seed and write all data via CLI (no daemon running)
    let _ = run_query_cli(main_data.path(), "CREATE (:Register {id: 0, val: 0})");
    let mut written_values = Vec::new();
    let mut seed = 0x1234_5678_9abc_def0u64;
    for _ in 0..30 {
        let val = (lcg(&mut seed) % 100) as i64 + 1;
        let query = format!(
            "MATCH (n:Register {{id: 0}}) SET n.val = {} RETURN n.val AS v",
            val
        );
        if run_query_cli(main_data.path(), &query).map(|o| o.status.success()).unwrap_or(false) {
            written_values.push(val);
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    // Read multiple times
    let mut read_values = Vec::new();
    for _ in 0..20 {
        let query = "MATCH (n:Register {id: 0}) RETURN n.val AS v";
        if let Ok(out) = run_query_cli(main_data.path(), query) {
            if out.status.success() {
                let stdout = String::from_utf8_lossy(&out.stdout);
                if let Some(v) = parse_val_from_stdout(&stdout) {
                    read_values.push(v);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Every read value must be either the initial value (0) or one of the written values
    let write_set: std::collections::HashSet<i64> = written_values.iter().copied().collect();
    for r in &read_values {
        if *r != 0 && !write_set.contains(r) {
            panic!(
                "read value {} was never written. writes: {:?}",
                r, written_values
            );
        }
    }

    // Final read must match the last written value
    let final_val = written_values.last().copied().unwrap_or(0);
    let out = run_query_cli(main_data.path(), "MATCH (n:Register {id: 0}) RETURN n.val AS v")
        .expect("final read failed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let last_read = parse_val_from_stdout(&stdout).unwrap_or(-1);
    assert_eq!(
        last_read, final_val,
        "final read should be {}, got {}",
        final_val, last_read
    );
}

// ─── Test: Monotonic Reads ─────────────────────────────────────────────────

/// Write a sequence of values, then read repeatedly and verify reads are
/// monotonic (non-decreasing) for a monotonically increasing write pattern.
#[test]
fn test_monotonic_reads() {
    let bin = memgraph_binary();
    if !bin.exists() {
        eprintln!("memgraph-rs binary not found at {:?}, skipping", bin);
        return;
    }

    let main_data = tempfile::tempdir().unwrap();

    // Seed
    let _ = run_query_cli(main_data.path(), "CREATE (:Counter {id: 0, val: 0})");

    // Write increasing values
    let n_writes = 10;
    for i in 1..=n_writes {
        let query = format!(
            "MATCH (n:Counter {{id: 0}}) SET n.val = {} RETURN n.val AS v",
            i
        );
        let out = run_query_cli(main_data.path(), &query).expect("write failed");
        assert!(out.status.success(), "write {} failed", i);
        std::thread::sleep(Duration::from_millis(5));
    }

    // Read repeatedly; all reads should see the final value
    let mut reads = Vec::new();
    for _ in 0..10 {
        std::thread::sleep(Duration::from_millis(30));
        let out = run_query_cli(main_data.path(), "MATCH (n:Counter {id: 0}) RETURN n.val AS v")
            .expect("read failed");
        let stdout = String::from_utf8_lossy(&out.stdout);
        if let Some(v) = parse_val_from_stdout(&stdout) {
            reads.push(v);
        }
    }

    // All reads should be non-decreasing and end at n_writes
    for window in reads.windows(2) {
        assert!(
            window[1] >= window[0],
            "reads non-monotonic: {:?}",
            reads
        );
    }

    let final_read = reads.last().copied().unwrap_or(-1);
    assert_eq!(
        final_read, n_writes as i64,
        "final read should be {}, got {}. reads: {:?}",
        n_writes, final_read, reads
    );
}

// ─── Test: Write Persistence Across Restart ────────────────────────────────

/// Write values, start daemon, verify data is loaded, kill daemon, write more,
/// restart, verify new data is loaded.
#[test]
fn test_persistence_across_restart() {
    let bin = memgraph_binary();
    if !bin.exists() {
        eprintln!("memgraph-rs binary not found at {:?}, skipping", bin);
        return;
    }

    let main_data = tempfile::tempdir().unwrap();

    // Write batch1
    let _ = run_query_cli(main_data.path(), "CREATE (:Persist {id: 0, val: 0})");
    for i in 1..=5 {
        let query = format!(
            "MATCH (n:Persist {{id: 0}}) SET n.val = {} RETURN n.val AS v",
            i
        );
        let out = run_query_cli(main_data.path(), &query).expect("batch1 write failed");
        assert!(out.status.success(), "batch1 write {} failed", i);
    }

    // Start daemon (loads snapshot)
    let mut main_proc = nemesis::spawn_main(main_data.path(), 10100).expect("spawn main failed");
    nemesis::wait_for_startup();

    // Verify batch1 visible
    let out = run_query_cli(main_data.path(), "MATCH (n:Persist {id: 0}) RETURN n.val AS v")
        .expect("read failed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let val1 = parse_val_from_stdout(&stdout).expect("no value for batch1");
    assert_eq!(val1, 5, "main should see batch1 value 5, got {}", val1);

    // Kill main
    nemesis::kill_main(&mut main_proc);
    std::thread::sleep(Duration::from_millis(500));

    // Write batch2 while main is dead
    for i in 6..=10 {
        let query = format!(
            "MATCH (n:Persist {{id: 0}}) SET n.val = {} RETURN n.val AS v",
            i
        );
        let out = run_query_cli(main_data.path(), &query).expect("batch2 write failed");
        assert!(out.status.success(), "batch2 write {} failed", i);
    }

    // Restart main
    let mut main_proc = nemesis::restart_main(main_data.path(), 10100).expect("restart failed");
    nemesis::wait_for_startup();

    // Verify batch2 visible
    let out = run_query_cli(main_data.path(), "MATCH (n:Persist {id: 0}) RETURN n.val AS v")
        .expect("read failed");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let final_val = parse_val_from_stdout(&stdout).expect("no value for batch2");
    assert_eq!(
        final_val, 10,
        "main should see batch2 value 10, got {}",
        final_val
    );

    main_proc.kill_and_wait();
}

// ─── Helpers ───────────────────────────────────────────────────────────────

/// Parse an integer value from query CLI stdout.
/// The CLI prints a table with a `---` separator; the value is on the line
/// immediately after `---`. We ignore everything else (banner, logs).
fn parse_val_from_stdout(stdout: &str) -> Option<i64> {
    let mut lines = stdout.lines().peekable();
    while let Some(line) = lines.next() {
        if line.trim() == "---" {
            // The value line follows the separator.
            if let Some(value_line) = lines.next() {
                let trimmed = value_line.trim();
                if trimmed == "(empty result)" {
                    return None;
                }
                // The value may be surrounded by table borders; strip non-numeric
                // prefix/suffix and try to parse.
                let numeric: String = trimmed
                    .chars()
                    .skip_while(|c| !c.is_ascii_digit() && *c != '-')
                    .collect::<String>()
                    .chars()
                    .take_while(|c| c.is_ascii_digit() || *c == '-')
                    .collect();
                if let Ok(n) = numeric.parse::<i64>() {
                    return Some(n);
                }
                // Fallback: try parsing the whole trimmed line.
                if let Ok(n) = trimmed.parse::<i64>() {
                    return Some(n);
                }
            }
        }
    }
    None
}
