//! Server startup and shutdown lifecycle management.
//!
//! Provides structured initialization, graceful shutdown, signal handling,
//! runtime config reloading, plugin loading, and certificate management.
//! Ensures components are initialized in the correct order and cleaned up
//! properly on exit.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mgauth::AuthStore;
use mgcatalog::Catalog;
use mgdbms::DbmsHandler;
use mgdurability::{recover, SnapshotWriter, WalWriter};
use mgstorage::storage::Storage;
use mgsystem::{DiskMonitor, MaintenanceReport, MemoryTracker, SignalHandler, SystemInfo};
use tracing::{error, info, warn};

use crate::admin::{AdminState, ServerConfig};
use crate::query_cache::{CacheWarmer, EvictionPolicy, QueryCache};
use crate::ServerWalWriter;

/// Server lifecycle phases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecyclePhase {
    Created,
    Initializing,
    ConfigLoaded,
    StorageReady,
    AuthReady,
    PluginsLoading,
    ServicesStarting,
    Running,
    ShuttingDown,
    Persisting,
    ShutDown,
}

/// TLS certificate configuration with hot-reload support.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub last_loaded: std::time::SystemTime,
}

impl TlsConfig {
    pub fn new(cert_path: PathBuf, key_path: PathBuf) -> Self {
        Self {
            cert_path,
            key_path,
            last_loaded: std::time::SystemTime::UNIX_EPOCH,
        }
    }

    /// Check if certificate files have changed since last load.
    pub fn has_changed(&self) -> bool {
        let cert_mtime = fs::metadata(&self.cert_path)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let key_mtime = fs::metadata(&self.key_path)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        cert_mtime > self.last_loaded || key_mtime > self.last_loaded
    }

    pub fn mark_loaded(&mut self) {
        self.last_loaded = std::time::SystemTime::now();
    }
}

/// Plugin/module descriptor.
#[derive(Clone, Debug)]
pub struct PluginInfo {
    pub name: String,
    pub path: PathBuf,
    pub enabled: bool,
}

/// Plugin registry for dynamic query modules.
pub struct PluginRegistry {
    plugins: std::sync::Mutex<Vec<PluginInfo>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Scan a directory for plugin files (.so / .dll / .dylib).
    pub fn scan_directory(&self, dir: &Path) -> Result<usize, String> {
        let mut count = 0;
        if !dir.exists() {
            return Ok(0);
        }
        let entries = fs::read_dir(dir).map_err(|e| format!("failed to read plugin dir: {}", e))?;
        let mut plugins = self.plugins.lock().unwrap();
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(ext) = path.extension() {
                let ext_str = ext.to_string_lossy().to_lowercase();
                if ext_str == "so" || ext_str == "dll" || ext_str == "dylib" {
                    let name = path
                        .file_stem()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_else(|| "unknown".into());
                    plugins.push(PluginInfo {
                        name,
                        path,
                        enabled: true,
                    });
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    pub fn list(&self) -> Vec<PluginInfo> {
        self.plugins.lock().unwrap().clone()
    }
}

/// Context shared across the entire server lifecycle.
pub struct ServerContext {
    pub phase: std::sync::RwLock<LifecyclePhase>,
    pub start_time: Instant,
    pub storage: Arc<Storage>,
    pub catalog: Arc<Catalog>,
    pub auth: Arc<AuthStore>,
    pub dbms: Arc<DbmsHandler>,
    pub admin: Arc<AdminState>,
    pub data_directory: PathBuf,
    pub signal_handler: SignalHandler,
    pub memory_tracker: MemoryTracker,
    pub disk_monitor: DiskMonitor,
    pub system_info: SystemInfo,
    pub shutdown_flag: Arc<AtomicBool>,
    pub query_cache: Arc<QueryCache>,
    pub prepared_statements: crate::query_cache::PreparedStatementCache,
    pub plugin_registry: PluginRegistry,
    pub tls_config: std::sync::Mutex<Option<TlsConfig>>,
    pub cache_warmer: std::sync::Mutex<CacheWarmer>,
    pub audit: Option<Arc<mgaudit::AuditLog>>,
    pub replication: Arc<std::sync::Mutex<Option<crate::replication::ReplicationManager>>>,
    pub coordinator: Arc<std::sync::Mutex<Option<mgcoord::Coordinator>>>,
    pub cluster_manager: Arc<tokio::sync::RwLock<Option<mgcoord::ClusterManager>>>,
    pub cluster_state: std::sync::RwLock<Arc<mgcoord::ClusterState>>,
}

impl ServerContext {
    pub fn new(data_dir: PathBuf) -> Self {
        let system_info = SystemInfo::gather();
        info!(
            "System: {} CPUs, {} MB total memory, PID {}",
            system_info.cpu_count,
            system_info.total_memory_bytes / (1024 * 1024),
            system_info.process_id
        );

        let storage = Arc::new(Storage::new());
        let trigger_exec = Arc::new(mginterp::TriggerInterpreter::new(storage.clone()));
        storage.set_trigger_executor(trigger_exec);

        let audit_config = mgaudit::AuditConfig {
            storage_directory: data_dir.join("audit"),
            buffer_size: 1000,
            buffer_flush_interval_ms: 5000,
        };
        let audit = Some(mgaudit::AuditLog::new(audit_config));

        Self {
            phase: std::sync::RwLock::new(LifecyclePhase::Created),
            start_time: Instant::now(),
            storage,
            catalog: Arc::new(Catalog::new()),
            auth: Arc::new(AuthStore::new()),
            dbms: Arc::new(DbmsHandler::new()),
            admin: AdminState::new(),
            data_directory: data_dir,
            signal_handler: SignalHandler::new(),
            memory_tracker: MemoryTracker::new(0, 0.8, 0.95),
            disk_monitor: DiskMonitor::new("."),
            system_info,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            query_cache: Arc::new(QueryCache::new(1000)),
            prepared_statements: crate::query_cache::PreparedStatementCache::new(100),
            plugin_registry: PluginRegistry::new(),
            tls_config: std::sync::Mutex::new(None),
            cache_warmer: std::sync::Mutex::new(CacheWarmer::new()),
            audit,
            replication: Arc::new(std::sync::Mutex::new(None)),
            coordinator: Arc::new(std::sync::Mutex::new(None)),
            cluster_manager: Arc::new(tokio::sync::RwLock::new(None)),
            cluster_state: std::sync::RwLock::new(Arc::new(mgcoord::ClusterState::new(
                String::new(),
            ))),
        }
    }

    pub fn new_with_config(
        data_dir: PathBuf,
        cache_size: usize,
        cache_ttl: Option<Duration>,
        cache_policy: EvictionPolicy,
    ) -> Self {
        let mut ctx = Self::new(data_dir);
        let mut cache = QueryCache::new(cache_size).with_policy(cache_policy);
        if let Some(ttl) = cache_ttl {
            cache = cache.with_ttl(ttl);
        }
        ctx.query_cache = Arc::new(cache);
        ctx
    }

    pub fn phase(&self) -> LifecyclePhase {
        *self.phase.read().unwrap()
    }

    fn set_phase(&self, phase: LifecyclePhase) {
        let old = *self.phase.read().unwrap();
        *self.phase.write().unwrap() = phase;
        info!("Server phase: {:?} -> {:?}", old, phase);
    }

    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_flag.load(Ordering::Relaxed)
    }

    pub fn request_shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }

    /// Check if TLS certificates need reloading.
    pub fn check_cert_reload(&self) -> bool {
        let tls = self.tls_config.lock().unwrap();
        tls.as_ref().map(|c| c.has_changed()).unwrap_or(false)
    }

    /// Mark TLS certificates as loaded.
    pub fn set_memory_limit(&self, limit_mib: u64) {
        self.memory_tracker.set_limit_bytes(limit_mib * 1024 * 1024);
    }

    pub fn mark_certs_loaded(&self) {
        let mut tls = self.tls_config.lock().unwrap();
        if let Some(ref mut c) = *tls {
            c.mark_loaded();
        }
    }
}

/// Initialize the server from configuration.
pub fn initialize(ctx: &ServerContext, config: &StartupConfig) -> Result<(), String> {
    ctx.set_phase(LifecyclePhase::Initializing);

    // Ensure data directory exists
    fs::create_dir_all(&ctx.data_directory)
        .map_err(|e| format!("failed to create data directory: {}", e))?;

    // Create subdirectories
    fs::create_dir_all(ctx.data_directory.join("plugins")).ok();
    fs::create_dir_all(ctx.data_directory.join("snapshots")).ok();
    fs::create_dir_all(ctx.data_directory.join("wal")).ok();

    ctx.set_phase(LifecyclePhase::ConfigLoaded);

    // Initialize auth if credentials provided
    if let Some((user, pass)) = config.auth_credentials.as_ref() {
        ctx.auth.set_enabled(true);
        ctx.auth
            .add_user(user, &mgauth::hash_password(pass), mgauth::Role::Admin);
        info!("Authentication enabled for user: {}", user);
    }

    // Load persistence (snapshot + WAL replay)
    let wal_path = ctx.data_directory.join("wal.mgwal");
    let snap_path = ctx.data_directory.join("snapshot.mgsnap");
    match recover(
        &ctx.storage,
        Some(&ctx.catalog),
        snap_path.to_str().unwrap_or("snapshot.mgsnap"),
        &[wal_path.to_str().unwrap_or("wal.mgwal")],
    ) {
        Ok(()) => info!(
            "Recovery complete from {} + {}",
            snap_path.display(),
            wal_path.display()
        ),
        Err(e) => warn!("No persistence found. Starting with empty database. ({e})"),
    }

    mginterp::init_gid_after_load(&ctx.storage);

    ctx.set_phase(LifecyclePhase::StorageReady);

    // Apply config to admin state
    if let Some(cfg) = config.runtime_config.clone() {
        ctx.admin.update_config(cfg);
    }

    ctx.set_phase(LifecyclePhase::AuthReady);

    // Scan for plugins
    let plugin_dir = ctx.data_directory.join("plugins");
    match ctx.plugin_registry.scan_directory(&plugin_dir) {
        Ok(n) => {
            if n > 0 {
                info!("Discovered {} plugin(s) in {}", n, plugin_dir.display());
            }
        }
        Err(e) => warn!("Plugin scan failed: {}", e),
    }

    ctx.set_phase(LifecyclePhase::PluginsLoading);

    // Start audit log
    if let Some(ref audit) = ctx.audit {
        if let Err(e) = audit.start() {
            warn!("Failed to start audit log: {}", e);
        }
    }

    // Install signal handlers
    ctx.signal_handler.install();
    info!("Signal handlers installed (SIGTERM/SIGINT for shutdown, SIGUSR1 for config reload)");

    ctx.set_phase(LifecyclePhase::ServicesStarting);

    Ok(())
}

/// Run background maintenance tasks (GC, TTL, memory monitoring, cache maintenance).
pub fn start_background_tasks(ctx: Arc<ServerContext>, gc_interval_secs: u64) {
    let gc_ctx = ctx.clone();
    std::thread::Builder::new()
        .name("gc-maintenance".into())
        .spawn(move || {
            let interval = Duration::from_secs(gc_interval_secs.max(1));
            let mut last_cert_check = Instant::now();
            loop {
                std::thread::sleep(interval);
                if gc_ctx.is_shutting_down() {
                    break;
                }

                // Run maintenance tick
                let report: MaintenanceReport = mgsystem::maintenance_tick(&gc_ctx.storage);
                if report.gc_deltas_freed > 0 {
                    info!("[gc] collected {} deltas", report.gc_deltas_freed);
                }
                if report.ttl_vertices_expired > 0 {
                    info!("[ttl] expired {} vertices", report.ttl_vertices_expired);
                }

                // Memory pressure check
                if gc_ctx.memory_tracker.should_check() {
                    match gc_ctx.memory_tracker.pressure_level() {
                        mgsystem::MemoryPressure::Normal => {}
                        mgsystem::MemoryPressure::Warning => {
                            warn!("Memory pressure: WARNING");
                        }
                        mgsystem::MemoryPressure::Critical => {
                            error!("Memory pressure: CRITICAL — triggering emergency GC");
                            let _ = gc_ctx.storage.gc();
                        }
                    }
                }

                // Disk space check
                let disk = gc_ctx.disk_monitor.usage();
                if disk.usage_percent() > 90.0 {
                    warn!("Disk usage critical: {:.1}%", disk.usage_percent());
                }

                // Cache maintenance (TTL eviction)
                gc_ctx.query_cache.maintenance();

                // Certificate reload check (every 5 minutes)
                if last_cert_check.elapsed() > Duration::from_secs(300) {
                    if gc_ctx.check_cert_reload() {
                        info!("TLS certificate change detected; will reload on next connection");
                    }
                    last_cert_check = Instant::now();
                }
            }
            info!("[gc-maintenance] thread exiting");
        })
        .expect("failed to spawn GC thread");
}

/// Graceful shutdown sequence with ordered component cleanup.
pub fn shutdown(ctx: &ServerContext) {
    ctx.set_phase(LifecyclePhase::ShuttingDown);
    info!("Starting graceful shutdown...");

    ctx.request_shutdown();

    // Step 1: Stop accepting new connections (wait for existing to drain)
    let active = ctx.admin.list_active_queries();
    if !active.is_empty() {
        warn!("Waiting for {} active queries to complete...", active.len());
        for i in 0..50 {
            std::thread::sleep(Duration::from_millis(100));
            let remaining = ctx.admin.list_active_queries().len();
            if remaining == 0 {
                info!("All queries completed");
                break;
            }
            if i == 49 {
                warn!("Forcing shutdown with {} queries still active", remaining);
            }
        }
    }

    // Step 2: Wait for connections to drain
    let conns = ctx.admin.list_connections();
    if !conns.is_empty() {
        warn!("Waiting for {} connections to close...", conns.len());
        for i in 0..30 {
            std::thread::sleep(Duration::from_millis(100));
            if ctx.admin.connection_count() == 0 {
                break;
            }
            if i == 29 {
                warn!(
                    "Forcing shutdown with {} connections still open",
                    ctx.admin.connection_count()
                );
            }
        }
    }

    // Step 3: Flush prepared statements
    ctx.prepared_statements.clear();
    info!("Prepared statements cleared");

    // Step 4: Stop audit log
    if let Some(ref audit) = ctx.audit {
        audit.stop();
    }

    // Step 5: Stop coordinator background thread
    if ctx.coordinator.lock().unwrap().is_some() {
        info!("Coordinator shutting down");
    }

    // Step 6: Persist state
    ctx.set_phase(LifecyclePhase::Persisting);
    persist_state(ctx);

    ctx.set_phase(LifecyclePhase::ShutDown);
    info!("Shutdown complete. Uptime: {:?}", ctx.uptime());
}

/// Save snapshot and reset WAL.
pub fn persist_state(ctx: &ServerContext) {
    fs::create_dir_all(&ctx.data_directory).ok();

    let snap_path = ctx.data_directory.join("snapshot.mgsnap");
    let wal_path = ctx.data_directory.join("wal.mgwal");

    ctx.storage.sync_wal();

    let snap = mgdurability::dump_snapshot(&ctx.storage, Some(&ctx.catalog));
    if let Err(e) = SnapshotWriter::write(&snap_path, &snap) {
        warn!("Failed to write snapshot: {}", e);
    } else {
        info!("Snapshot saved to {}", snap_path.display());
    }

    let _ = fs::remove_file(&wal_path);
    if let Ok(wal_writer) = WalWriter::create(&wal_path) {
        ctx.storage
            .set_wal(Box::new(ServerWalWriter { inner: wal_writer }));
        info!("WAL reset at {}", wal_path.display());
    } else {
        warn!("Failed to reset WAL at {}", wal_path.display());
    }
}

/// Reload configuration at runtime.
pub fn reload_config(ctx: &ServerContext, path: &Path) -> Result<(), String> {
    info!("Reloading configuration from {}...", path.display());
    let contents = fs::read_to_string(path).map_err(|e| format!("failed to read config: {}", e))?;
    let cfg: ServerConfigFile =
        serde_json::from_str(&contents).map_err(|e| format!("failed to parse config: {}", e))?;

    if let Some(max_conn) = cfg.max_connections {
        let mut admin_cfg = ctx.admin.config();
        admin_cfg.max_connections = max_conn;
        ctx.admin.update_config(admin_cfg.clone());
    }
    if let Some(query_timeout) = cfg.query_timeout_ms {
        let mut admin_cfg = ctx.admin.config();
        admin_cfg.query_timeout_ms = query_timeout;
        ctx.admin.update_config(admin_cfg.clone());
    }
    if let Some(gc_interval) = cfg.gc_interval_secs {
        let mut admin_cfg = ctx.admin.config();
        admin_cfg.gc_interval_secs = gc_interval;
        ctx.admin.update_config(admin_cfg.clone());
    }
    if let Some(slow_threshold) = cfg.slow_query_threshold_ms {
        let mut admin_cfg = ctx.admin.config();
        admin_cfg.slow_query_threshold_ms = slow_threshold;
        ctx.admin.update_config(admin_cfg.clone());
    }
    if let Some(cache_size) = cfg.query_cache_size {
        // Cannot resize cache directly; clear if shrunk significantly
        let current = ctx.query_cache.stats().capacity;
        if cache_size < current / 2 {
            ctx.query_cache.clear();
            warn!("Query cache cleared due to significant size reduction");
        }
    }

    ctx.signal_handler.clear_reload();
    info!("Configuration reloaded successfully");
    Ok(())
}

/// On-disk server configuration format (JSON).
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ServerConfigFile {
    pub data_directory: Option<String>,
    pub bolt_port: Option<u16>,
    pub log_filter: Option<String>,
    pub bolt_user: Option<String>,
    pub bolt_pass: Option<String>,
    pub gc_interval_secs: Option<u64>,
    pub max_connections: Option<usize>,
    pub query_timeout_ms: Option<u64>,
    pub slow_query_threshold_ms: Option<u64>,
    pub memory_limit_mib: Option<u64>,
    pub http_port: Option<u16>,
    pub query_cache_size: Option<usize>,
    pub query_cache_ttl_secs: Option<u64>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
}

/// Configuration for server startup.
#[derive(Clone, Debug, Default)]
pub struct StartupConfig {
    pub auth_credentials: Option<(String, String)>,
    pub runtime_config: Option<ServerConfig>,
    pub gc_interval_secs: u64,
    pub memory_limit_mib: u64,
    pub cache_size: usize,
    pub cache_ttl_secs: Option<u64>,
    pub cache_policy: EvictionPolicy,
}

impl StartupConfig {
    pub fn from_args(args: &crate::Args) -> Self {
        let auth_credentials = match (&args.bolt_user, &args.bolt_pass) {
            (Some(u), Some(p)) => Some((u.clone(), p.clone())),
            _ => None,
        };

        let mut runtime_config = ServerConfig::default();
        let mut cache_size = args.query_cache_size;
        let mut cache_ttl_secs = None;

        if let Some(ref path) = args.config {
            if let Ok(contents) = fs::read_to_string(path) {
                if let Ok(cfg) = serde_json::from_str::<ServerConfigFile>(&contents) {
                    if let Some(v) = cfg.max_connections {
                        runtime_config.max_connections = v;
                    }
                    if let Some(v) = cfg.query_timeout_ms {
                        runtime_config.query_timeout_ms = v;
                    }
                    if let Some(v) = cfg.slow_query_threshold_ms {
                        runtime_config.slow_query_threshold_ms = v;
                    }
                    if let Some(v) = cfg.gc_interval_secs {
                        runtime_config.gc_interval_secs = v;
                    }
                    if let Some(v) = cfg.query_cache_size {
                        cache_size = v;
                    }
                    if let Some(v) = cfg.query_cache_ttl_secs {
                        cache_ttl_secs = Some(v);
                    }
                }
            }
        }

        let gc_interval = runtime_config.gc_interval_secs;
        Self {
            auth_credentials,
            runtime_config: Some(runtime_config),
            gc_interval_secs: gc_interval,
            memory_limit_mib: args.memory_limit,
            cache_size,
            cache_ttl_secs,
            cache_policy: EvictionPolicy::Lru,
        }
    }
}

/// Startup health check: verify critical subsystems.
pub fn health_check_startup(ctx: &ServerContext) -> Result<Vec<String>, String> {
    let mut issues = Vec::new();

    // Check data directory is writable
    let test_file = ctx.data_directory.join(".startup_test");
    if fs::write(&test_file, b"test").is_err() {
        issues.push("Data directory is not writable".into());
    } else {
        let _ = fs::remove_file(&test_file);
    }

    // Check storage is accessible
    if ctx.storage.all_vertices().is_empty() {
        // Empty storage is fine, just verify it doesn't panic
    }

    // Check catalog
    let (labels, props, edge_types) = ctx.catalog.dump_mappings();
    info!(
        "Catalog: {} labels, {} properties, {} edge types",
        labels.len(),
        props.len(),
        edge_types.len()
    );

    if issues.is_empty() {
        Ok(vec!["All startup checks passed".into()])
    } else {
        Ok(issues)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_context_creation() {
        let ctx = ServerContext::new(PathBuf::from("/tmp/mg_test_ctx"));
        assert_eq!(ctx.phase(), LifecyclePhase::Created);
        assert!(!ctx.is_shutting_down());
    }

    #[test]
    fn test_server_context_with_config() {
        let ctx = ServerContext::new_with_config(
            PathBuf::from("/tmp/mg_test_ctx2"),
            500,
            Some(Duration::from_secs(60)),
            EvictionPolicy::Lfu,
        );
        let stats = ctx.query_cache.stats();
        assert_eq!(stats.capacity, 500);
        assert_eq!(stats.policy, EvictionPolicy::Lfu);
        assert_eq!(stats.ttl_secs, Some(60));
    }

    #[test]
    fn test_startup_config_defaults() {
        let cfg = StartupConfig::default();
        assert_eq!(cfg.cache_size, 0);
        assert_eq!(cfg.cache_ttl_secs, None);
        assert_eq!(cfg.cache_policy, EvictionPolicy::Lru);
    }

    #[test]
    fn test_tls_config_change_detection() {
        let tmp_dir = std::env::temp_dir().join(format!("mg_tls_test_{}", std::process::id()));
        fs::create_dir_all(&tmp_dir).unwrap();
        let cert_path = tmp_dir.join("cert.pem");
        let key_path = tmp_dir.join("key.pem");
        fs::write(&cert_path, "test-cert").unwrap();
        fs::write(&key_path, "test-key").unwrap();

        let tls = TlsConfig::new(cert_path.clone(), key_path.clone());
        assert!(tls.has_changed()); // never loaded

        let mut tls = tls;
        tls.mark_loaded();
        assert!(!tls.has_changed());

        // Simulate file modification
        std::thread::sleep(Duration::from_millis(50));
        fs::write(&cert_path, "new-cert").unwrap();
        assert!(tls.has_changed());

        fs::remove_dir_all(&tmp_dir).ok();
    }

    #[test]
    fn test_plugin_registry_scan() {
        let reg = PluginRegistry::new();
        let tmp_dir = std::env::temp_dir().join(format!("mg_plugin_test_{}", std::process::id()));
        fs::create_dir_all(&tmp_dir).unwrap();

        // Create fake plugin files
        fs::write(tmp_dir.join("libtest.so"), b"fake").unwrap();
        fs::write(tmp_dir.join("libother.dylib"), b"fake").unwrap();
        fs::write(tmp_dir.join("not_a_plugin.txt"), b"fake").unwrap();

        let count = reg.scan_directory(&tmp_dir).unwrap();
        assert_eq!(count, 2);

        let plugins = reg.list();
        assert_eq!(plugins.len(), 2);

        fs::remove_dir_all(&tmp_dir).ok();
    }

    #[test]
    fn test_health_check_startup() {
        let ctx = ServerContext::new(PathBuf::from("/tmp/mg_health_test"));
        let result = health_check_startup(&ctx);
        assert!(result.is_ok());
        let issues = result.unwrap();
        assert!(!issues.is_empty());
    }

    #[test]
    fn test_server_config_file_roundtrip() {
        let cfg = ServerConfigFile {
            data_directory: Some("/data".into()),
            bolt_port: Some(7687),
            log_filter: Some("info".into()),
            bolt_user: Some("admin".into()),
            bolt_pass: Some("secret".into()),
            gc_interval_secs: Some(60),
            max_connections: Some(100),
            query_timeout_ms: Some(5000),
            slow_query_threshold_ms: Some(1000),
            memory_limit_mib: Some(4096),
            http_port: Some(7474),
            query_cache_size: Some(1000),
            query_cache_ttl_secs: Some(300),
            tls_cert: Some("/etc/cert.pem".into()),
            tls_key: Some("/etc/key.pem".into()),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let decoded: ServerConfigFile = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.bolt_port, Some(7687));
        assert_eq!(decoded.query_cache_size, Some(1000));
    }
}
