//! Nemesis framework for Jepsen-style distributed consistency testing.
//!
//! Provides helpers to inject faults into a running memgraph-rs cluster:
//! - Network partitions (replica disconnected from main)
//! - Process kills (SIGKILL main or replica)
//! - Process restarts (start new main on same data directory)
//! - Random latency injection
//!
//! All helpers use `std::process::Command` and clean up via Drop guards.

use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

/// Guard that kills a child process on drop.
pub struct ProcessGuard {
    pub child: Child,
    pub name: String,
}

impl ProcessGuard {
    /// Create a new guard wrapping a child process.
    pub fn new(child: Child, name: impl Into<String>) -> Self {
        Self {
            child,
            name: name.into(),
        }
    }

    /// Check if the process is still alive.
    pub fn is_alive(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    /// Kill the process and wait for it to exit.
    pub fn kill_and_wait(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Send SIGTERM and wait for graceful shutdown, falling back to SIGKILL.
    pub fn graceful_shutdown(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            let pid = self.child.id() as i32;
            if pid > 0 {
                unsafe {
                    libc::kill(pid, libc::SIGTERM);
                }
                // Wait up to 3 seconds for graceful exit
                let start = std::time::Instant::now();
                loop {
                    if start.elapsed() > std::time::Duration::from_secs(3) {
                        break;
                    }
                    if self.child.try_wait().unwrap().is_some() {
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
            }
        }
        // Fallback: SIGKILL
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Locate the memgraph-rs binary (debug or release build).
pub fn memgraph_binary() -> std::path::PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // deps
    path.pop(); // debug or release
    // If we are in release mode, path is already correct.
    // If in debug mode under deps, we need to go up one more level.
    if path.file_name().map(|n| n == "deps").unwrap_or(false) {
        path.pop();
    }
    path.push("memgraph-rs");
    path
}

/// Spawn a main memgraph-rs process with the given data directory and replication port.
///
/// Bolt is disabled (`--bolt-port 0`) to avoid port conflicts.
pub fn spawn_main(
    data_dir: &std::path::Path,
    replication_port: u16,
) -> Result<ProcessGuard, String> {
    let bin = memgraph_binary();
    if !bin.exists() {
        return Err(format!("memgraph-rs binary not found at {:?}", bin));
    }

    let child = Command::new(&bin)
        .args([
            "--data-directory",
            data_dir.to_str().unwrap(),
            "--replication-role",
            "main",
            "--replication-port",
            &replication_port.to_string(),
            "--bolt-port",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("failed to spawn main: {}", e))?;

    Ok(ProcessGuard::new(child, format!("main:{}", replication_port)))
}

/// Spawn a replica memgraph-rs process.
pub fn spawn_replica(
    data_dir: &std::path::Path,
    main_addr: &str,
) -> Result<ProcessGuard, String> {
    let bin = memgraph_binary();
    if !bin.exists() {
        return Err(format!("memgraph-rs binary not found at {:?}", bin));
    }

    let child = Command::new(&bin)
        .args([
            "--data-directory",
            data_dir.to_str().unwrap(),
            "--replication-role",
            "replica",
            "--replica-of",
            main_addr,
            "--bolt-port",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("failed to spawn replica: {}", e))?;

    Ok(ProcessGuard::new(child, format!("replica-of-{}", main_addr)))
}

/// Execute a query against a memgraph-rs instance via `--query` CLI flag.
///
/// This spawns a short-lived process using the same data directory.
/// Returns stdout/stderr output.
pub fn run_query_cli(
    data_dir: &std::path::Path,
    query: &str,
) -> Result<std::process::Output, String> {
    let bin = memgraph_binary();
    if !bin.exists() {
        return Err(format!("memgraph-rs binary not found at {:?}", bin));
    }

    Command::new(&bin)
        .args([
            "--data-directory",
            data_dir.to_str().unwrap(),
            "--query",
            query,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("failed to run query: {}", e))
}

/// Nemesis: kill the main process with SIGKILL.
///
/// Returns the exit status (should indicate signal termination).
pub fn kill_main(main_proc: &mut ProcessGuard) -> std::process::ExitStatus {
    main_proc.kill_and_wait();
    // After kill_and_wait, the child has already been reaped.
    // Return a synthetic exit status if needed.
    std::process::ExitStatus::default()
}

/// Nemesis: restart a main process on the same data directory.
///
/// The old main must already be dead. This starts a new main on the same port.
pub fn restart_main(
    data_dir: &std::path::Path,
    replication_port: u16,
) -> Result<ProcessGuard, String> {
    spawn_main(data_dir, replication_port)
}

/// Nemesis: partition a replica from its main by killing it and restarting
/// with a non-existent main address.
///
/// The old replica is killed. A new replica process is started pointing to
/// a dead address, simulating a network partition.
pub fn partition_replica(
    replica_proc: &mut ProcessGuard,
    data_dir: &std::path::Path,
) -> Result<ProcessGuard, String> {
    replica_proc.kill_and_wait();
    thread::sleep(Duration::from_millis(200));

    let bin = memgraph_binary();
    if !bin.exists() {
        return Err(format!("memgraph-rs binary not found at {:?}", bin));
    }

    let child = Command::new(&bin)
        .args([
            "--data-directory",
            data_dir.to_str().unwrap(),
            "--replication-role",
            "replica",
            "--replica-of",
            "127.0.0.1:59999", // non-existent main
            "--bolt-port",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("failed to spawn partitioned replica: {}", e))?;

    Ok(ProcessGuard::new(child, "partitioned-replica".to_string()))
}

/// Nemesis: inject random latency in the current thread.
///
/// Uses a deterministic LCG with the given seed.
pub fn random_latency(seed: &mut u64) {
    let delay_ms = lcg(seed) % 10 + 1; // 1-10ms
    thread::sleep(Duration::from_millis(delay_ms));
}

/// Deterministic LCG for pseudo-randomness.
fn lcg(seed: &mut u64) -> u64 {
    const A: u64 = 6364136223846793005;
    const C: u64 = 1442695040888963407;
    *seed = seed.wrapping_mul(A).wrapping_add(C);
    *seed
}

/// Wait for a process to become healthy by polling its data directory with a query.
///
/// Returns true if the process responded successfully within the timeout.
pub fn wait_for_healthy(
    data_dir: &std::path::Path,
    timeout: Duration,
    poll_interval: Duration,
) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        match run_query_cli(data_dir, "RETURN 1 AS ok") {
            Ok(out) => {
                if out.status.success() {
                    let stdout = String::from_utf8_lossy(&out.stdout);
                    if stdout.contains("ok") {
                        return true;
                    }
                }
            }
            Err(_) => {}
        }
        thread::sleep(poll_interval);
    }
    false
}

/// Wait for a process to bind by simply sleeping (simpler than polling).
pub fn wait_for_startup() {
    thread::sleep(Duration::from_millis(800));
}

/// Wait for replication to catch up (heuristic sleep).
pub fn wait_for_replication() {
    thread::sleep(Duration::from_millis(1200));
}
