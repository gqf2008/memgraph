//! Memgraph server configuration.
//! Equivalent to C++ `src/flags/` + `config/flags.yaml`.
//!
//! Uses `clap` for CLI argument parsing with YAML config file support.

use clap::Parser;
use serde::{Deserialize, Serialize};

/// Memgraph server configuration — mirrors C++ flags.yaml categories.
#[derive(Parser, Clone, Debug, Serialize, Deserialize)]
#[command(name = "memgraph", version, about = "Memgraph - in-memory graph database")]
pub struct Flags {
    // ─── General ───────────────────────────────────────────────────────
    /// Path to data directory for durability (snapshot + WAL).
    #[arg(long = "data-directory", default_value = "")]
    pub data_directory: String,

    /// Path to a YAML config file.
    #[arg(long = "config-file", short = 'c')]
    pub config_file: Option<String>,

    /// Storage mode: IN_MEMORY_TRANSACTIONAL, IN_MEMORY_ANALYTICAL.
    #[arg(long = "storage-mode", default_value = "IN_MEMORY_TRANSACTIONAL")]
    pub storage_mode: String,

    /// Transaction isolation level: SNAPSHOT_ISOLATION, READ_COMMITTED, READ_UNCOMMITTED.
    #[arg(long = "isolation-level", default_value = "SNAPSHOT_ISOLATION")]
    pub isolation_level: String,

    /// Memory limit in MiB (0 = no limit).
    #[arg(long = "memory-limit", default_value = "0")]
    pub memory_limit: u64,

    /// Memory warning threshold as fraction (0.0-1.0).
    #[arg(long = "memory-warning-threshold", default_value = "0.8")]
    pub memory_warning_threshold: f64,

    // ─── Bolt ──────────────────────────────────────────────────────────
    /// Bolt server bind address.
    #[arg(long = "bolt-server-address", default_value = "0.0.0.0")]
    pub bolt_server_address: String,

    /// Bolt server port.
    #[arg(long = "bolt-port", default_value = "7687")]
    pub bolt_port: u16,

    /// Bolt server name for HELLO negotiation.
    #[arg(long = "bolt-server-name", default_value = "MemgraphRust/0.1")]
    pub bolt_server_name: String,

    /// Max concurrent Bolt connections.
    #[arg(long = "bolt-max-connections", default_value = "100")]
    pub bolt_max_connections: usize,

    /// Bolt certificate file for TLS.
    #[arg(long = "bolt-cert-file")]
    pub bolt_cert_file: Option<String>,

    /// Bolt key file for TLS.
    #[arg(long = "bolt-key-file")]
    pub bolt_key_file: Option<String>,

    // ─── Authentication ────────────────────────────────────────────────
    /// Enable authentication.
    #[arg(long = "auth-enabled")]
    pub auth_enabled: bool,

    /// Default Bolt user.
    #[arg(long = "bolt-user")]
    pub bolt_user: Option<String>,

    /// Default Bolt password.
    #[arg(long = "bolt-pass")]
    pub bolt_pass: Option<String>,

    /// Auth module path for custom auth.
    #[arg(long = "auth-module")]
    pub auth_module: Option<String>,

    /// LDAP server URL.
    #[arg(long = "auth-ldap-server")]
    pub auth_ldap_server: Option<String>,

    // ─── Durability ────────────────────────────────────────────────────
    /// Enable WAL durability.
    #[arg(long = "storage-wal-enabled")]
    pub wal_enabled: bool,

    /// Snapshot interval in seconds (0 = disabled).
    #[arg(long = "storage-snapshot-interval", default_value = "300")]
    pub snapshot_interval_secs: u64,

    /// Snapshot on exit.
    #[arg(long = "storage-snapshot-on-exit")]
    pub snapshot_on_exit: bool,

    /// WAL file directory.
    #[arg(long = "storage-wal-directory")]
    pub wal_directory: Option<String>,

    // ─── GC ────────────────────────────────────────────────────────────
    /// GC interval in seconds.
    #[arg(long = "storage-gc-interval", default_value = "60")]
    pub gc_interval_secs: u64,

    /// TTL cleanup interval in seconds.
    #[arg(long = "storage-ttl-interval", default_value = "60")]
    pub ttl_interval_secs: u64,

    // ─── Replication ───────────────────────────────────────────────────
    /// Replication role: MAIN, REPLICA.
    #[arg(long = "replication-role", default_value = "MAIN")]
    pub replication_role: String,

    /// Replication server address.
    #[arg(long = "replication-server-address", default_value = "0.0.0.0")]
    pub replication_server_address: String,

    /// Replication server port.
    #[arg(long = "replication-port", default_value = "10000")]
    pub replication_port: u16,

    /// Main instance address (for replica).
    #[arg(long = "replication-main-address")]
    pub replication_main_address: Option<String>,

    /// Main instance port (for replica).
    #[arg(long = "replication-main-port")]
    pub replication_main_port: Option<u16>,

    // ─── Coordination ──────────────────────────────────────────────────
    /// Coordinator server address.
    #[arg(long = "coordinator-server-address", default_value = "0.0.0.0")]
    pub coordinator_server_address: String,

    /// Coordinator server port.
    #[arg(long = "coordinator-port", default_value = "12000")]
    pub coordinator_port: u16,

    /// Raft node ID.
    #[arg(long = "raft-node-id", default_value = "1")]
    pub raft_node_id: u64,

    // ─── Logging ───────────────────────────────────────────────────────
    /// Log level: TRACE, DEBUG, INFO, WARN, ERROR.
    #[arg(long = "log-level", default_value = "INFO")]
    pub log_level: String,

    /// Log to stderr instead of file.
    #[arg(long = "log-to-stderr")]
    pub log_to_stderr: bool,

    // ─── Telemetry ─────────────────────────────────────────────────────
    /// Enable telemetry.
    #[arg(long = "telemetry-enabled")]
    pub telemetry_enabled: bool,

    /// Telemetry server URL.
    #[arg(long = "telemetry-server")]
    pub telemetry_server: Option<String>,

    // ─── Query ─────────────────────────────────────────────────────────
    /// Max query execution time in seconds.
    #[arg(long = "query-execution-timeout-sec", default_value = "600")]
    pub query_execution_timeout_secs: u64,

    /// Enable query profiling.
    #[arg(long = "query-profiling")]
    pub query_profiling: bool,

    /// Enable query plan caching.
    #[arg(long = "query-plan-cache-enabled")]
    pub query_plan_cache_enabled: bool,

    /// Query plan cache size.
    #[arg(long = "query-plan-cache-size", default_value = "1024")]
    pub query_plan_cache_size: usize,

    // ─── Text Search ───────────────────────────────────────────────────
    /// Text index directory.
    #[arg(long = "text-index-directory")]
    pub text_index_directory: Option<String>,

    // ─── Metrics ───────────────────────────────────────────────────────
    /// Metrics bind address.
    #[arg(long = "metrics-address", default_value = "0.0.0.0")]
    pub metrics_address: String,

    /// Metrics port (0 = disabled).
    #[arg(long = "metrics-port", default_value = "9091")]
    pub metrics_port: u16,

    // ─── WebSocket ─────────────────────────────────────────────────────
    /// Enable WebSocket log streaming server.
    #[arg(long = "websocket-enabled")]
    pub websocket_enabled: bool,

    /// WebSocket server bind address.
    #[arg(long = "websocket-address", default_value = "0.0.0.0")]
    pub websocket_address: String,

    /// WebSocket server port (0 = disabled).
    #[arg(long = "websocket-port", default_value = "7444")]
    pub websocket_port: u16,
}

impl Flags {
    /// Parse from CLI args. If `--config-file` is set, merge YAML values.
    pub fn parse() -> Self {
        let mut flags = <Flags as Parser>::parse();

        // Load config file if specified, merge into flags
        if let Some(ref path) = flags.config_file {
            if let Ok(contents) = std::fs::read_to_string(path) {
                if let Ok(file_flags) = serde_yaml::from_str::<Flags>(&contents) {
                    flags.merge(file_flags);
                }
            }
        }

        // Load YAML config from default locations
        for default_path in &["memgraph.yaml", "/etc/memgraph/memgraph.yaml"] {
            if flags.config_file.is_none() {
                if let Ok(contents) = std::fs::read_to_string(default_path) {
                    if let Ok(file_flags) = serde_yaml::from_str::<Flags>(&contents) {
                        flags.merge(file_flags);
                        break;
                    }
                }
            }
        }

        // Setup tracing
        if flags.log_to_stderr {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(flags.log_level.clone())
                .try_init();
        }

        flags
    }

    /// Merge non-default values from another Flags into self.
    fn merge(&mut self, other: Flags) {
        // Only override if the other value differs from its default.
        // Simple strategy: use serde to detect non-default.
        // For now, just merge non-empty strings and non-zero numerics.
        if !other.data_directory.is_empty() { self.data_directory = other.data_directory; }
        if let Some(cf) = other.config_file { self.config_file = Some(cf); }
        if other.storage_mode != "IN_MEMORY_TRANSACTIONAL" { self.storage_mode = other.storage_mode; }
        if other.bolt_port != 7687 { self.bolt_port = other.bolt_port; }
        if other.bolt_server_address != "0.0.0.0" { self.bolt_server_address = other.bolt_server_address; }
        if other.bolt_max_connections != 100 { self.bolt_max_connections = other.bolt_max_connections; }
        if let Some(cert) = other.bolt_cert_file { self.bolt_cert_file = Some(cert); }
        if let Some(key) = other.bolt_key_file { self.bolt_key_file = Some(key); }
        if other.auth_enabled { self.auth_enabled = true; }
        if let Some(user) = other.bolt_user { self.bolt_user = Some(user); }
        if let Some(pass) = other.bolt_pass { self.bolt_pass = Some(pass); }
        if other.wal_enabled { self.wal_enabled = true; }
        if other.snapshot_interval_secs != 300 { self.snapshot_interval_secs = other.snapshot_interval_secs; }
        if other.snapshot_on_exit { self.snapshot_on_exit = true; }
        if other.replication_role != "MAIN" { self.replication_role = other.replication_role; }
        if other.replication_port != 10000 { self.replication_port = other.replication_port; }
        if other.coordinator_port != 12000 { self.coordinator_port = other.coordinator_port; }
        if other.log_level != "INFO" { self.log_level = other.log_level; }
        if other.log_to_stderr { self.log_to_stderr = true; }
        if other.telemetry_enabled { self.telemetry_enabled = true; }
        if other.metrics_port != 9091 { self.metrics_port = other.metrics_port; }
        if other.query_execution_timeout_secs != 600 { self.query_execution_timeout_secs = other.query_execution_timeout_secs; }
        if other.memory_limit != 0 { self.memory_limit = other.memory_limit; }
        if other.gc_interval_secs != 60 { self.gc_interval_secs = other.gc_interval_secs; }
        if other.ttl_interval_secs != 60 { self.ttl_interval_secs = other.ttl_interval_secs; }
        if other.query_plan_cache_size != 1024 { self.query_plan_cache_size = other.query_plan_cache_size; }
        if other.websocket_enabled { self.websocket_enabled = true; }
        if other.websocket_port != 7444 { self.websocket_port = other.websocket_port; }
        if other.websocket_address != "0.0.0.0" { self.websocket_address = other.websocket_address; }
    }

    pub fn bolt_addr(&self) -> String {
        format!("{}:{}", self.bolt_server_address, self.bolt_port)
    }

    pub fn is_tls_enabled(&self) -> bool {
        self.bolt_cert_file.is_some() && self.bolt_key_file.is_some()
    }

    pub fn metrics_addr(&self) -> String {
        format!("{}:{}", self.metrics_address, self.metrics_port)
    }

    /// Get a single setting value by name.
    pub fn get_setting(&self, name: &str) -> String {
        match name.to_ascii_lowercase().as_str() {
            "bolt-port" => self.bolt_port.to_string(),
            "bolt-server-address" => self.bolt_server_address.clone(),
            "bolt-max-connections" => self.bolt_max_connections.to_string(),
            "data-directory" => self.data_directory.clone(),
            "storage-mode" => self.storage_mode.clone(),
            "isolation-level" => self.isolation_level.clone(),
            "memory-limit" => self.memory_limit.to_string(),
            "memory-warning-threshold" => self.memory_warning_threshold.to_string(),
            "auth-enabled" => self.auth_enabled.to_string(),
            "wal-enabled" => self.wal_enabled.to_string(),
            "snapshot-interval-secs" => self.snapshot_interval_secs.to_string(),
            "snapshot-on-exit" => self.snapshot_on_exit.to_string(),
            "gc-interval-secs" => self.gc_interval_secs.to_string(),
            "ttl-interval-secs" => self.ttl_interval_secs.to_string(),
            "replication-role" => self.replication_role.clone(),
            "replication-port" => self.replication_port.to_string(),
            "coordinator-port" => self.coordinator_port.to_string(),
            "log-level" => self.log_level.clone(),
            "log-to-stderr" => self.log_to_stderr.to_string(),
            "telemetry-enabled" => self.telemetry_enabled.to_string(),
            "query-execution-timeout-secs" => self.query_execution_timeout_secs.to_string(),
            "query-profiling" => self.query_profiling.to_string(),
            "query-plan-cache-enabled" => self.query_plan_cache_enabled.to_string(),
            "query-plan-cache-size" => self.query_plan_cache_size.to_string(),
            "metrics-address" => self.metrics_address.clone(),
            "metrics-port" => self.metrics_port.to_string(),
            "websocket-enabled" => self.websocket_enabled.to_string(),
            "websocket-address" => self.websocket_address.clone(),
            "websocket-port" => self.websocket_port.to_string(),
            _ => format!("<unknown setting: {}>", name),
        }
    }

    /// List all settings as (name, value) pairs.
    pub fn list_settings(&self) -> Vec<(String, String)> {
        vec![
            ("bolt-port".into(), self.bolt_port.to_string()),
            ("bolt-server-address".into(), self.bolt_server_address.clone()),
            ("bolt-max-connections".into(), self.bolt_max_connections.to_string()),
            ("data-directory".into(), self.data_directory.clone()),
            ("storage-mode".into(), self.storage_mode.clone()),
            ("isolation-level".into(), self.isolation_level.clone()),
            ("memory-limit".into(), self.memory_limit.to_string()),
            ("memory-warning-threshold".into(), self.memory_warning_threshold.to_string()),
            ("auth-enabled".into(), self.auth_enabled.to_string()),
            ("wal-enabled".into(), self.wal_enabled.to_string()),
            ("snapshot-interval-secs".into(), self.snapshot_interval_secs.to_string()),
            ("snapshot-on-exit".into(), self.snapshot_on_exit.to_string()),
            ("gc-interval-secs".into(), self.gc_interval_secs.to_string()),
            ("ttl-interval-secs".into(), self.ttl_interval_secs.to_string()),
            ("replication-role".into(), self.replication_role.clone()),
            ("replication-port".into(), self.replication_port.to_string()),
            ("coordinator-port".into(), self.coordinator_port.to_string()),
            ("log-level".into(), self.log_level.clone()),
            ("log-to-stderr".into(), self.log_to_stderr.to_string()),
            ("telemetry-enabled".into(), self.telemetry_enabled.to_string()),
            ("query-execution-timeout-secs".into(), self.query_execution_timeout_secs.to_string()),
            ("query-profiling".into(), self.query_profiling.to_string()),
            ("query-plan-cache-enabled".into(), self.query_plan_cache_enabled.to_string()),
            ("query-plan-cache-size".into(), self.query_plan_cache_size.to_string()),
            ("metrics-address".into(), self.metrics_address.clone()),
            ("metrics-port".into(), self.metrics_port.to_string()),
            ("websocket-enabled".into(), self.websocket_enabled.to_string()),
            ("websocket-address".into(), self.websocket_address.clone()),
            ("websocket-port".into(), self.websocket_port.to_string()),
        ]
    }

    /// Set a runtime-modifiable setting. Returns Err for unknown or immutable settings.
    pub fn set_setting(&mut self, name: &str, value: &str) -> Result<(), String> {
        match name.to_ascii_lowercase().as_str() {
            "bolt-port" => {
                self.bolt_port = value.parse().map_err(|e| format!("invalid port: {}", e))?;
            }
            "bolt-max-connections" => {
                self.bolt_max_connections = value.parse().map_err(|e| format!("invalid count: {}", e))?;
            }
            "memory-limit" => {
                self.memory_limit = value.parse().map_err(|e| format!("invalid limit: {}", e))?;
            }
            "memory-warning-threshold" => {
                self.memory_warning_threshold = value.parse().map_err(|e| format!("invalid threshold: {}", e))?;
            }
            "wal-enabled" => {
                self.wal_enabled = value.parse().map_err(|e| format!("invalid bool: {}", e))?;
            }
            "snapshot-interval-secs" => {
                self.snapshot_interval_secs = value.parse().map_err(|e| format!("invalid seconds: {}", e))?;
            }
            "snapshot-on-exit" => {
                self.snapshot_on_exit = value.parse().map_err(|e| format!("invalid bool: {}", e))?;
            }
            "gc-interval-secs" => {
                self.gc_interval_secs = value.parse().map_err(|e| format!("invalid seconds: {}", e))?;
            }
            "ttl-interval-secs" => {
                self.ttl_interval_secs = value.parse().map_err(|e| format!("invalid seconds: {}", e))?;
            }
            "log-level" => {
                self.log_level = value.to_string();
            }
            "query-execution-timeout-secs" => {
                self.query_execution_timeout_secs = value.parse().map_err(|e| format!("invalid seconds: {}", e))?;
            }
            "query-profiling" => {
                self.query_profiling = value.parse().map_err(|e| format!("invalid bool: {}", e))?;
            }
            "query-plan-cache-enabled" => {
                self.query_plan_cache_enabled = value.parse().map_err(|e| format!("invalid bool: {}", e))?;
            }
            "query-plan-cache-size" => {
                self.query_plan_cache_size = value.parse().map_err(|e| format!("invalid size: {}", e))?;
            }
            "metrics-port" => {
                self.metrics_port = value.parse().map_err(|e| format!("invalid port: {}", e))?;
            }
            "websocket-enabled" => {
                self.websocket_enabled = value.parse().map_err(|e| format!("invalid bool: {}", e))?;
            }
            "websocket-port" => {
                self.websocket_port = value.parse().map_err(|e| format!("invalid port: {}", e))?;
            }
            _ => return Err(format!("setting '{}' is unknown or not mutable at runtime", name)),
        }
        Ok(())
    }
}

impl Default for Flags {
    fn default() -> Self {
        Flags {
            data_directory: String::new(),
            config_file: None,
            storage_mode: "IN_MEMORY_TRANSACTIONAL".into(),
            isolation_level: "SNAPSHOT_ISOLATION".into(),
            memory_limit: 0,
            memory_warning_threshold: 0.8,
            bolt_server_address: "0.0.0.0".into(),
            bolt_port: 7687,
            bolt_server_name: "MemgraphRust/0.1".into(),
            bolt_max_connections: 100,
            bolt_cert_file: None,
            bolt_key_file: None,
            auth_enabled: false,
            bolt_user: None,
            bolt_pass: None,
            auth_module: None,
            auth_ldap_server: None,
            wal_enabled: false,
            snapshot_interval_secs: 300,
            snapshot_on_exit: false,
            wal_directory: None,
            gc_interval_secs: 60,
            ttl_interval_secs: 60,
            replication_role: "MAIN".into(),
            replication_server_address: "0.0.0.0".into(),
            replication_port: 10000,
            replication_main_address: None,
            replication_main_port: None,
            coordinator_server_address: "0.0.0.0".into(),
            coordinator_port: 12000,
            raft_node_id: 1,
            log_level: "INFO".into(),
            log_to_stderr: false,
            telemetry_enabled: false,
            telemetry_server: None,
            query_execution_timeout_secs: 600,
            query_profiling: false,
            query_plan_cache_enabled: false,
            query_plan_cache_size: 1024,
            text_index_directory: None,
            metrics_address: "0.0.0.0".into(),
            metrics_port: 9091,
            websocket_enabled: false,
            websocket_address: "0.0.0.0".into(),
            websocket_port: 7444,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_flags() {
        let flags = Flags::default();
        assert_eq!(flags.bolt_port, 7687);
        assert_eq!(flags.storage_mode, "IN_MEMORY_TRANSACTIONAL");
        assert!(!flags.auth_enabled);
    }

    #[test]
    fn test_bolt_addr() {
        let flags = Flags::default();
        assert_eq!(flags.bolt_addr(), "0.0.0.0:7687");
    }

    #[test]
    fn test_parse_empty() {
        let flags = Flags::parse_from(std::iter::empty::<&str>());
        assert_eq!(flags.bolt_port, 7687);
    }

    #[test]
    fn test_parse_with_args() {
        let flags = Flags::parse_from([
            "memgraph",
            "--bolt-port", "8000",
            "--auth-enabled",
            "--log-level", "DEBUG",
        ]);
        assert_eq!(flags.bolt_port, 8000);
        assert!(flags.auth_enabled);
        assert_eq!(flags.log_level, "DEBUG");
    }
}
