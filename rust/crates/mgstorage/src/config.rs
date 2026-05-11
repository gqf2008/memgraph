//! Storage engine configuration.
//!
//! Tunable parameters for the in-memory storage engine:
//! memory limits, GC intervals, index settings, and isolation defaults.

/// Storage mode: in-memory or on-disk backed by `mgdisk::DiskKv`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum StorageMode {
    #[default]
    InMemory,
    OnDisk {
        path: String,
    },
}

/// Configuration for the storage engine.
#[derive(Clone, Debug)]
pub struct StorageConfig {
    /// Storage mode.
    pub mode: StorageMode,
    /// Maximum number of vertices before write rejection (0 = unlimited).
    pub max_vertices: usize,
    /// Maximum number of edges before write rejection (0 = unlimited).
    pub max_edges: usize,
    /// Memory limit in MiB (0 = unlimited).
    pub memory_limit_mib: u64,
    /// GC interval in milliseconds.
    pub gc_interval_ms: u64,
    /// TTL check interval in seconds.
    pub ttl_check_interval_secs: u64,
    /// Default isolation level for new transactions.
    pub default_isolation: mgcore::delta::IsolationLevel,
    /// Whether label-property indices are built automatically.
    pub auto_index_label_property: bool,
    /// Whether edge type-property indices are built automatically.
    pub auto_index_edge_type_property: bool,
    /// Maximum delta chain length before eager GC.
    pub max_delta_chain_length: usize,
    /// Snapshot interval in seconds (0 = disabled).
    pub snapshot_interval_sec: u64,
    /// WAL enabled.
    pub wal_enabled: bool,
    /// WAL sync on every commit.
    pub wal_sync_on_commit: bool,
    /// LRU cache size for hot vertices in on-disk mode.
    pub lru_cache_size: usize,
    /// Maximum query execution time in milliseconds (0 = unlimited).
    pub query_timeout_ms: u64,
    /// Enable parallel query execution.
    pub parallel_query_execution: bool,
    /// Number of worker threads for parallel execution.
    pub query_worker_threads: usize,
    /// Enable query result caching.
    pub query_cache_enabled: bool,
    /// Query cache capacity in number of entries.
    pub query_cache_capacity: usize,
    /// Enable automatic index creation on frequently queried properties.
    pub auto_index_creation: bool,
    /// Minimum query frequency before auto-indexing (queries per minute).
    pub auto_index_threshold: u64,
    /// Replication role: standalone, primary, or replica.
    pub replication_role: ReplicationRole,
    /// Enable analytics collection.
    pub analytics_enabled: bool,
    /// Analytics collection interval in seconds.
    pub analytics_interval_secs: u64,
}

/// Replication role for the storage engine.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum ReplicationRole {
    #[default]
    Standalone,
    Primary,
    Replica {
        primary_address: String,
    },
}

impl StorageConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config for on-disk mode at the given path.
    pub fn on_disk(path: impl Into<String>) -> Self {
        Self {
            mode: StorageMode::OnDisk { path: path.into() },
            ..Default::default()
        }
    }

    /// Validate configuration and return errors.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if self.gc_interval_ms == 0 {
            errors.push("gc_interval_ms must be > 0".into());
        }
        if self.max_delta_chain_length == 0 {
            errors.push("max_delta_chain_length must be > 0".into());
        }
        if let StorageMode::OnDisk { ref path } = self.mode {
            if path.is_empty() {
                errors.push("on-disk path must not be empty".into());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            mode: StorageMode::InMemory,
            max_vertices: 0,
            max_edges: 0,
            memory_limit_mib: 0,
            gc_interval_ms: 60_000,
            ttl_check_interval_secs: 60,
            default_isolation: mgcore::delta::IsolationLevel::SnapshotIsolation,
            auto_index_label_property: false,
            auto_index_edge_type_property: false,
            max_delta_chain_length: 100,
            snapshot_interval_sec: 0,
            wal_enabled: true,
            wal_sync_on_commit: true,
            lru_cache_size: 10_000,
            query_timeout_ms: 0,
            parallel_query_execution: false,
            query_worker_threads: num_cpus::get(),
            query_cache_enabled: true,
            query_cache_capacity: 1_000,
            auto_index_creation: false,
            auto_index_threshold: 100,
            replication_role: ReplicationRole::Standalone,
            analytics_enabled: false,
            analytics_interval_secs: 300,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = StorageConfig::default();
        assert_eq!(cfg.gc_interval_ms, 60_000);
        assert_eq!(cfg.max_delta_chain_length, 100);
        assert_eq!(cfg.mode, StorageMode::InMemory);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_invalid_config() {
        let mut cfg = StorageConfig::default();
        cfg.gc_interval_ms = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_on_disk_config() {
        let cfg = StorageConfig::on_disk("/tmp/mgstorage_test");
        assert!(matches!(cfg.mode, StorageMode::OnDisk { .. }));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_on_disk_empty_path_invalid() {
        let cfg = StorageConfig::on_disk("");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_replication_role() {
        let cfg = StorageConfig::default();
        assert_eq!(cfg.replication_role, ReplicationRole::Standalone);

        let mut cfg2 = cfg.clone();
        cfg2.replication_role = ReplicationRole::Primary;
        assert!(cfg2.validate().is_ok());

        let mut cfg3 = cfg.clone();
        cfg3.replication_role = ReplicationRole::Replica {
            primary_address: "127.0.0.1:10000".into(),
        };
        assert!(cfg3.validate().is_ok());
    }

    #[test]
    fn test_query_timeout_config() {
        let mut cfg = StorageConfig::default();
        cfg.query_timeout_ms = 5000;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_parallel_execution_config() {
        let mut cfg = StorageConfig::default();
        cfg.parallel_query_execution = true;
        cfg.query_worker_threads = 4;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_analytics_config() {
        let mut cfg = StorageConfig::default();
        cfg.analytics_enabled = true;
        cfg.analytics_interval_secs = 60;
        assert!(cfg.validate().is_ok());
    }
}
