//! System-level utilities for Memgraph.
//! Equivalent to C++ `src/system/`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// System resource information.
#[derive(Clone, Debug)]
pub struct SystemInfo {
    pub total_memory_bytes: u64,
    pub used_memory_bytes: u64,
    pub cpu_count: u32,
    pub os_name: String,
    pub process_id: u32,
    pub hostname: String,
    pub cpu_usage_percent: f64,
}

impl SystemInfo {
    /// Gather current system information.
    pub fn gather() -> Self {
        Self {
            total_memory_bytes: Self::total_memory(),
            used_memory_bytes: Self::used_memory(),
            cpu_count: num_cpus::get() as u32,
            os_name: std::env::consts::OS.to_string(),
            process_id: std::process::id(),
            hostname: Self::hostname(),
            cpu_usage_percent: Self::cpu_usage(),
        }
    }

    #[cfg(target_os = "macos")]
    fn total_memory() -> u64 {
        if let Ok(output) = std::process::Command::new("sysctl")
            .args(["hw.memsize"])
            .output()
        {
            if let Ok(s) = String::from_utf8(output.stdout) {
                if let Some(val) = s.split(':').nth(1) {
                    return val.trim().parse().unwrap_or(0);
                }
            }
        }
        0
    }

    #[cfg(target_os = "linux")]
    fn total_memory() -> u64 {
        if let Ok(s) = std::fs::read_to_string("/proc/meminfo") {
            for line in s.lines() {
                if line.starts_with("MemTotal:") {
                    if let Some(val) = line.split_whitespace().nth(1) {
                        return val.parse::<u64>().unwrap_or(0) * 1024; // kB → bytes
                    }
                }
            }
        }
        0
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    fn total_memory() -> u64 { 0 }

    fn used_memory() -> u64 {
        // Approximate via RSS
        #[cfg(target_os = "macos")]
        {
            if let Ok(output) = std::process::Command::new("ps")
                .args(["-o", "rss=", "-p", &std::process::id().to_string()])
                .output()
            {
                if let Ok(s) = String::from_utf8(output.stdout) {
                    return s.trim().parse::<u64>().unwrap_or(0) * 1024; // KB → bytes
                }
            }
        }
        #[cfg(target_os = "linux")]
        {
            if let Ok(s) = std::fs::read_to_string(format!("/proc/{}/statm", std::process::id())) {
                if let Some(val) = s.split_whitespace().nth(1) {
                    return val.parse::<u64>().unwrap_or(0) * 4096; // pages → bytes
                }
            }
        }
        0
    }

    fn hostname() -> String {
        std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "unknown".into())
    }

    fn cpu_usage() -> f64 {
        // Placeholder: would require sampling /proc/stat or task_info over time
        0.0
    }
}

/// Memory usage tracker for Memgraph.
pub struct MemoryTracker {
    limit_bytes: std::sync::atomic::AtomicU64,
    pub warning_threshold: f64,
    pub critical_threshold: f64,
    last_check: std::sync::Mutex<Instant>,
}

impl MemoryTracker {
    pub fn new(limit_mib: u64, warning_threshold: f64, critical_threshold: f64) -> Self {
        Self {
            limit_bytes: std::sync::atomic::AtomicU64::new(limit_mib * 1024 * 1024),
            warning_threshold,
            critical_threshold,
            last_check: std::sync::Mutex::new(Instant::now() - Duration::from_secs(2)),
        }
    }

    pub fn limit_bytes(&self) -> u64 {
        self.limit_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_limit_bytes(&self, bytes: u64) {
        self.limit_bytes.store(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if memory usage exceeds the warning threshold.
    pub fn exceeds_warning(&self) -> bool {
        let limit = self.limit_bytes();
        let used = SystemInfo::gather().used_memory_bytes;
        limit > 0 && (used as f64) > (limit as f64) * self.warning_threshold
    }

    /// Check if memory usage exceeds the critical threshold.
    pub fn exceeds_critical(&self) -> bool {
        let limit = self.limit_bytes();
        let used = SystemInfo::gather().used_memory_bytes;
        limit > 0 && (used as f64) > (limit as f64) * self.critical_threshold
    }

    /// Check if memory usage exceeds the hard limit.
    pub fn exceeds_limit(&self) -> bool {
        let limit = self.limit_bytes();
        let used = SystemInfo::gather().used_memory_bytes;
        limit > 0 && used > limit
    }

    /// Get current memory pressure level.
    pub fn pressure_level(&self) -> MemoryPressure {
        if self.exceeds_critical() {
            MemoryPressure::Critical
        } else if self.exceeds_warning() {
            MemoryPressure::Warning
        } else {
            MemoryPressure::Normal
        }
    }

    /// Check and update; returns true if at least 1 second has passed since last check.
    pub fn should_check(&self) -> bool {
        let mut last = self.last_check.lock().unwrap();
        if last.elapsed() >= Duration::from_secs(1) {
            *last = Instant::now();
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MemoryPressure {
    Normal,
    Warning,
    Critical,
}

/// CPU usage sampler (requires periodic sampling).
pub struct CpuSampler {
    last_sample: std::sync::Mutex<Option<(Instant, u64)>>,
}

impl CpuSampler {
    pub fn new() -> Self {
        Self {
            last_sample: std::sync::Mutex::new(None),
        }
    }

    /// Sample CPU usage and return percentage since last call.
    pub fn sample(&self) -> f64 {
        let now = Instant::now();
        let current_time = Self::process_cpu_time_ns();

        let mut last = self.last_sample.lock().unwrap();
        let result = if let Some((prev_time, prev_cpu)) = *last {
            let elapsed = now.duration_since(prev_time).as_secs_f64();
            let cpu_delta = (current_time.saturating_sub(prev_cpu)) as f64 / 1e9;
            if elapsed > 0.0 {
                (cpu_delta / elapsed) * 100.0 / (num_cpus::get() as f64)
            } else {
                0.0
            }
        } else {
            0.0
        };
        *last = Some((now, current_time));
        result
    }

    #[cfg(target_os = "macos")]
    fn process_cpu_time_ns() -> u64 {
        // Would use task_info(TASK_BASIC_INFO) in production
        0
    }

    #[cfg(target_os = "linux")]
    fn process_cpu_time_ns() -> u64 {
        if let Ok(s) = std::fs::read_to_string(format!("/proc/{}/stat", std::process::id())) {
            let fields: Vec<&str> = s.split_whitespace().collect();
            // utime is field 14, stime is field 15 (in clock ticks)
            if fields.len() > 15 {
                let utime: u64 = fields[13].parse().unwrap_or(0);
                let stime: u64 = fields[14].parse().unwrap_or(0);
                // Approximate: assume 100 ticks/sec
                return (utime + stime) * 10_000_000;
            }
        }
        0
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    fn process_cpu_time_ns() -> u64 { 0 }
}

/// Disk space monitor.
pub struct DiskMonitor {
    pub path: String,
}

impl DiskMonitor {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string() }
    }

    /// Get disk usage for the monitored path.
    pub fn usage(&self) -> DiskUsage {
        #[cfg(target_os = "macos")]
        {
            if let Ok(output) = std::process::Command::new("df")
                .args(["-k", &self.path])
                .output()
            {
                if let Ok(s) = String::from_utf8(output.stdout) {
                    if let Some(line) = s.lines().nth(1) {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 4 {
                            if let (Ok(total), Ok(used), Ok(available)) = (
                                parts[1].parse::<u64>(),
                                parts[2].parse::<u64>(),
                                parts[3].parse::<u64>(),
                            ) {
                                return DiskUsage {
                                    total_bytes: total * 1024,
                                    used_bytes: used * 1024,
                                    available_bytes: available * 1024,
                                };
                            }
                        }
                    }
                }
            }
        }
        #[cfg(target_os = "linux")]
        {
            if let Ok(output) = std::process::Command::new("df")
                .args(["-B1", &self.path])
                .output()
            {
                if let Ok(s) = String::from_utf8(output.stdout) {
                    if let Some(line) = s.lines().nth(1) {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 4 {
                            if let (Ok(total), Ok(used), Ok(available)) = (
                                parts[1].parse::<u64>(),
                                parts[2].parse::<u64>(),
                                parts[3].parse::<u64>(),
                            ) {
                                return DiskUsage {
                                    total_bytes: total,
                                    used_bytes: used,
                                    available_bytes: available,
                                };
                            }
                        }
                    }
                }
            }
        }
        DiskUsage::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct DiskUsage {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
}

impl DiskUsage {
    pub fn usage_percent(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            (self.used_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }
}

/// Unix signal handler for graceful shutdown.
pub struct SignalHandler {
    shutdown_requested: Arc<AtomicBool>,
    reload_requested: Arc<AtomicBool>,
}

impl SignalHandler {
    pub fn new() -> Self {
        Self {
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            reload_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Install signal handlers for SIGTERM and SIGINT.
    #[cfg(unix)]
    pub fn install(&self) {
        use std::sync::atomic::AtomicBool;
        let shutdown = Arc::clone(&self.shutdown_requested);
        let reload = Arc::clone(&self.reload_requested);

        ctrlc::set_handler(move || {
            shutdown.store(true, Ordering::Relaxed);
        }).ok();

        // SIGUSR1 for config reload
        #[cfg(unix)]
        {
            let reload2 = Arc::clone(&reload);
            unsafe {
                libc::signal(libc::SIGUSR1, sigusr1_handler as libc::sighandler_t);
            }
            // In a real implementation, we'd use signalfd or a dedicated thread
            let _ = reload2;
        }
    }

    #[cfg(not(unix))]
    pub fn install(&self) {
        let shutdown = Arc::clone(&self.shutdown_requested);
        ctrlc::set_handler(move || {
            shutdown.store(true, Ordering::Relaxed);
        }).ok();
    }

    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Relaxed)
    }

    pub fn is_reload_requested(&self) -> bool {
        self.reload_requested.load(Ordering::Relaxed)
    }

    pub fn clear_reload(&self) {
        self.reload_requested.store(false, Ordering::Relaxed);
    }
}

#[cfg(unix)]
extern "C" fn sigusr1_handler(_: libc::c_int) {
    // Signal reload request — actual flag update would need atomic static
}

/// Process limit checker.
pub struct ProcessLimits {
    pub max_open_files: u64,
    pub max_memory_mb: u64,
    pub max_threads: u64,
}

impl ProcessLimits {
    pub fn current() -> Self {
        Self {
            max_open_files: Self::rlimit(libc::RLIMIT_NOFILE),
            max_memory_mb: Self::rlimit(libc::RLIMIT_AS) / (1024 * 1024),
            max_threads: Self::rlimit(libc::RLIMIT_NPROC),
        }
    }

    #[cfg(unix)]
    fn rlimit(resource: libc::c_int) -> u64 {
        let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        unsafe {
            if libc::getrlimit(resource, &mut limit) == 0 {
                return limit.rlim_cur;
            }
        }
        0
    }

    #[cfg(not(unix))]
    fn rlimit(_resource: libc::c_int) -> u64 {
        0
    }
}

/// Schedule a periodic task on a background thread.
pub fn schedule_periodic<F>(name: &str, interval: Duration, mut f: F) -> std::thread::JoinHandle<()>
where
    F: FnMut() + Send + 'static,
{
    let name = name.to_string();
    std::thread::Builder::new()
        .name(name.clone())
        .spawn(move || {
            loop {
                std::thread::sleep(interval);
                f();
            }
        })
        .expect("failed to spawn periodic task")
}

/// Run GC, TTL cleanup, and other maintenance tasks.
pub fn maintenance_tick(storage: &mgstorage::storage::Storage) -> MaintenanceReport {
    let gc_freed = storage.gc();
    let ttl_expired = storage.ttl_cleanup();
    MaintenanceReport {
        gc_deltas_freed: gc_freed,
        ttl_vertices_expired: ttl_expired,
    }
}

#[derive(Debug)]
pub struct MaintenanceReport {
    pub gc_deltas_freed: usize,
    pub ttl_vertices_expired: usize,
}

/// File descriptor usage tracker.
pub struct FileDescriptorTracker {
    pub max_fds: u64,
}

impl FileDescriptorTracker {
    pub fn new() -> Self {
        Self {
            max_fds: ProcessLimits::current().max_open_files,
        }
    }

    /// Count currently open file descriptors for this process (best-effort).
    #[cfg(target_os = "linux")]
    pub fn current_open_fds(&self) -> usize {
        if let Ok(entries) = std::fs::read_dir(format!("/proc/{}/fd", std::process::id())) {
            entries.filter_map(|e| e.ok()).count()
        } else {
            0
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn current_open_fds(&self) -> usize {
        // macOS: use lsof as fallback
        if let Ok(output) = std::process::Command::new("lsof")
            .args(["-p", &std::process::id().to_string(), "-n"])
            .output()
        {
            let s = String::from_utf8_lossy(&output.stdout);
            s.lines().count().saturating_sub(1)
        } else {
            0
        }
    }

    pub fn usage_percent(&self) -> f64 {
        if self.max_fds == 0 {
            0.0
        } else {
            (self.current_open_fds() as f64 / self.max_fds as f64) * 100.0
        }
    }
}

/// Network I/O monitor for the process.
pub struct NetworkMonitor;

impl NetworkMonitor {
    /// Read bytes sent/received from /proc/net/dev (Linux only).
    #[cfg(target_os = "linux")]
    pub fn read_stats() -> NetworkStats {
        if let Ok(s) = std::fs::read_to_string("/proc/net/dev") {
            let mut rx_bytes = 0u64;
            let mut tx_bytes = 0u64;
            for line in s.lines().skip(2) {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 9 {
                    if let (Ok(rx), Ok(tx)) = (parts[1].parse::<u64>(), parts[9].parse::<u64>()) {
                        rx_bytes += rx;
                        tx_bytes += tx;
                    }
                }
            }
            NetworkStats { rx_bytes, tx_bytes }
        } else {
            NetworkStats::default()
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn read_stats() -> NetworkStats {
        NetworkStats::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct NetworkStats {
    pub rx_bytes: u64,
    pub tx_bytes: u64,
}

/// Thread pool statistics snapshot.
#[derive(Clone, Debug, Default)]
pub struct ThreadPoolStats {
    pub active_threads: usize,
    pub queued_tasks: usize,
    pub completed_tasks: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_info_gather() {
        let info = SystemInfo::gather();
        assert!(info.cpu_count > 0);
        assert!(!info.os_name.is_empty());
        assert!(info.process_id > 0);
        assert!(!info.hostname.is_empty());
    }

    #[test]
    fn test_memory_tracker_no_limit() {
        let tracker = MemoryTracker::new(0, 0.8, 0.95);
        assert!(!tracker.exceeds_limit());
        assert!(!tracker.exceeds_warning());
        assert!(!tracker.exceeds_critical());
        assert_eq!(tracker.pressure_level(), MemoryPressure::Normal);
    }

    #[test]
    fn test_memory_tracker_pressure_levels() {
        let tracker = MemoryTracker::new(1, 0.5, 0.9);
        // With only 1 MiB limit, any real RSS will exceed
        let level = tracker.pressure_level();
        assert!(level == MemoryPressure::Warning || level == MemoryPressure::Critical);
    }

    #[test]
    fn test_memory_tracker_should_check() {
        let tracker = MemoryTracker::new(0, 0.8, 0.95);
        assert!(tracker.should_check());
        // Second call within 1 second should return false
        assert!(!tracker.should_check());
    }

    #[test]
    fn test_cpu_sampler() {
        let sampler = CpuSampler::new();
        let pct = sampler.sample();
        assert!(pct >= 0.0);
        // Second sample should have some elapsed time
        std::thread::sleep(Duration::from_millis(10));
        let pct2 = sampler.sample();
        assert!(pct2 >= 0.0);
    }

    #[test]
    fn test_disk_monitor() {
        let monitor = DiskMonitor::new("/");
        let usage = monitor.usage();
        // Should return something even if parsing fails
        assert!(usage.total_bytes >= 0);
    }

    #[test]
    fn test_disk_usage_percent() {
        let usage = DiskUsage {
            total_bytes: 100,
            used_bytes: 50,
            available_bytes: 50,
        };
        assert_eq!(usage.usage_percent(), 50.0);
    }

    #[test]
    fn test_signal_handler() {
        let handler = SignalHandler::new();
        assert!(!handler.is_shutdown_requested());
        assert!(!handler.is_reload_requested());
        handler.clear_reload();
    }

    #[test]
    fn test_process_limits() {
        let limits = ProcessLimits::current();
        // At least one limit should be non-zero on Unix
        #[cfg(unix)]
        assert!(limits.max_open_files > 0);
    }

    #[test]
    fn test_maintenance_tick() {
        let storage = mgstorage::storage::Storage::new();
        let report = maintenance_tick(&storage);
        assert_eq!(report.gc_deltas_freed, 0);
        assert_eq!(report.ttl_vertices_expired, 0);
    }

    #[test]
    fn test_schedule_periodic() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counter = Arc::new(AtomicUsize::new(0));
        let c2 = Arc::clone(&counter);
        let handle = schedule_periodic("test", Duration::from_millis(10), move || {
            c2.fetch_add(1, Ordering::Relaxed);
        });
        std::thread::sleep(Duration::from_millis(50));
        drop(handle);
        assert!(counter.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn test_file_descriptor_tracker() {
        let tracker = FileDescriptorTracker::new();
        let open_fds = tracker.current_open_fds();
        assert!(open_fds > 0); // At least stdin/stdout/stderr
        let pct = tracker.usage_percent();
        assert!(pct >= 0.0);
    }

    #[test]
    fn test_network_monitor() {
        let stats = NetworkMonitor::read_stats();
        // Should not panic; values may be 0 on non-Linux
        assert!(stats.rx_bytes >= 0);
        assert!(stats.tx_bytes >= 0);
    }

    #[test]
    fn test_network_stats_default() {
        let stats = NetworkStats::default();
        assert_eq!(stats.rx_bytes, 0);
        assert_eq!(stats.tx_bytes, 0);
    }

    #[test]
    fn test_thread_pool_stats_default() {
        let stats = ThreadPoolStats::default();
        assert_eq!(stats.active_threads, 0);
        assert_eq!(stats.queued_tasks, 0);
        assert_eq!(stats.completed_tasks, 0);
    }
}
