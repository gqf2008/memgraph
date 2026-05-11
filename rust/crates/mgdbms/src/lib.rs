#![allow(dead_code)]
//! # mgdbms — Multi-database management.
//!
//! Manages multiple isolated databases within a single Memgraph instance.
//! Each database has its own storage engine, tenant assignment, and lifecycle.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use mgstorage::storage::Storage;

/// Database name.
pub type DatabaseName = String;

/// Tenant identifier.
pub type TenantId = String;

/// Database state.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DatabaseState {
    Online,
    Offline,
    Creating,
    Dropping,
}

/// A named database instance.
pub struct Database {
    pub name: DatabaseName,
    pub storage: Storage,
    pub state: RwLock<DatabaseState>,
    pub tenant: RwLock<Option<TenantId>>,
    pub transaction_count: std::sync::atomic::AtomicU64,
    pub query_count: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("name", &self.name)
            .field("state", &self.state)
            .field("tenant", &self.tenant)
            .field(
                "transaction_count",
                &self
                    .transaction_count
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
            .field(
                "query_count",
                &self.query_count.load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish()
    }
}

impl Database {
    pub fn new(name: DatabaseName) -> Self {
        Self {
            name,
            storage: Storage::new(),
            state: RwLock::new(DatabaseState::Online),
            tenant: RwLock::new(None),
            transaction_count: std::sync::atomic::AtomicU64::new(0),
            query_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Increment the transaction counter.
    pub fn record_transaction(&self) {
        self.transaction_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Increment the query execution counter.
    pub fn record_query(&self) {
        self.query_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Multi-database handler. Manages named databases.
pub struct DbmsHandler {
    databases: RwLock<HashMap<DatabaseName, Arc<Database>>>,
    default_db: DatabaseName,
}

impl Default for DbmsHandler {
    fn default() -> Self {
        let mut databases = HashMap::new();
        let default = Arc::new(Database::new("default".to_string()));
        databases.insert("default".to_string(), default);
        Self {
            databases: RwLock::new(databases),
            default_db: "default".to_string(),
        }
    }
}

impl DbmsHandler {
    /// Create a new handler with a default database.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the default database.
    pub fn default_db(&self) -> Arc<Database> {
        self.get(&self.default_db.clone())
            .expect("default database must exist")
    }

    /// Get a database by name.
    pub fn get(&self, name: &DatabaseName) -> Option<Arc<Database>> {
        self.databases
            .read()
            .expect("lock poisoned")
            .get(name)
            .cloned()
    }

    /// Create a new database.
    pub fn create(&self, name: DatabaseName) -> Result<Arc<Database>, DbmsError> {
        let mut dbs = self.databases.write().expect("lock poisoned");
        if dbs.contains_key(&name) {
            return Err(DbmsError::DatabaseExists(name));
        }
        let db = Arc::new(Database::new(name.clone()));
        dbs.insert(name, db.clone());
        Ok(db)
    }

    /// Drop a database. Cannot drop the default database.
    pub fn drop(&self, name: &DatabaseName) -> Result<(), DbmsError> {
        if name == &self.default_db {
            return Err(DbmsError::CannotDropDefault);
        }
        let mut dbs = self.databases.write().expect("lock poisoned");
        if dbs.remove(name).is_none() {
            return Err(DbmsError::DatabaseNotFound(name.clone()));
        }
        Ok(())
    }

    /// List all database names.
    pub fn list(&self) -> Vec<DatabaseName> {
        self.databases
            .read()
            .expect("lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Check if a database exists.
    pub fn exists(&self, name: &DatabaseName) -> bool {
        self.databases
            .read()
            .expect("lock poisoned")
            .contains_key(name)
    }

    /// Assign a tenant to a database.
    pub fn assign_tenant(&self, db_name: &DatabaseName, tenant: TenantId) -> Result<(), DbmsError> {
        let db = self
            .get(db_name)
            .ok_or_else(|| DbmsError::DatabaseNotFound(db_name.clone()))?;
        *db.tenant.write().expect("lock poisoned") = Some(tenant);
        Ok(())
    }

    /// Get the tenant for a database.
    pub fn tenant(&self, db_name: &DatabaseName) -> Option<TenantId> {
        self.get(db_name)?
            .tenant
            .read()
            .expect("lock poisoned")
            .clone()
    }
}

/// DBMS-level errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbmsError {
    DatabaseExists(DatabaseName),
    DatabaseNotFound(DatabaseName),
    CannotDropDefault,
    CannotRenameDefault,
    QuotaExceeded(String),
}

impl std::fmt::Display for DbmsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbmsError::DatabaseExists(name) => write!(f, "database already exists: {}", name),
            DbmsError::DatabaseNotFound(name) => write!(f, "database not found: {}", name),
            DbmsError::CannotDropDefault => write!(f, "cannot drop the default database"),
            DbmsError::CannotRenameDefault => write!(f, "cannot rename the default database"),
            DbmsError::QuotaExceeded(msg) => write!(f, "quota exceeded: {}", msg),
        }
    }
}

/// Database-level statistics.
#[derive(Clone, Debug, Default)]
pub struct DatabaseStats {
    pub vertex_count: usize,
    pub edge_count: usize,
    pub index_count: usize,
    pub constraint_count: usize,
    pub memory_estimate_bytes: usize,
    pub trigger_count: usize,
    pub transaction_count: u64,
    pub query_count: u64,
}

/// Database size quota.
#[derive(Clone, Debug, Default)]
pub struct DatabaseQuota {
    pub max_vertices: Option<usize>,
    pub max_edges: Option<usize>,
    pub max_memory_mb: Option<usize>,
}

/// Backup metadata for a database snapshot.
#[derive(Clone, Debug)]
pub struct BackupInfo {
    pub db_name: DatabaseName,
    pub timestamp: std::time::SystemTime,
    pub size_bytes: usize,
    pub path: String,
}

/// Backup manager handles snapshot creation and restoration.
pub struct BackupManager {
    backups: RwLock<Vec<BackupInfo>>,
    backup_dir: String,
}

impl BackupManager {
    pub fn new(backup_dir: String) -> Self {
        Self {
            backups: RwLock::new(Vec::new()),
            backup_dir,
        }
    }

    pub fn record_backup(&self, info: BackupInfo) {
        self.backups.write().expect("lock poisoned").push(info);
    }

    pub fn list_backups(&self, db_name: &str) -> Vec<BackupInfo> {
        self.backups
            .read()
            .expect("lock poisoned")
            .iter()
            .filter(|b| b.db_name == db_name)
            .cloned()
            .collect()
    }

    pub fn latest_backup(&self, db_name: &str) -> Option<BackupInfo> {
        self.list_backups(db_name)
            .into_iter()
            .max_by_key(|b| b.timestamp)
    }

    pub fn remove_old_backups(&self, db_name: &str, keep_count: usize) {
        let mut backups = self.backups.write().expect("lock poisoned");
        let mut db_backups: Vec<_> = backups
            .iter()
            .enumerate()
            .filter(|(_, b)| b.db_name == db_name)
            .map(|(i, b)| (i, b.timestamp))
            .collect();
        db_backups.sort_by_key(|(_, ts)| *ts);
        let to_remove: std::collections::HashSet<_> = db_backups
            .iter()
            .take(db_backups.len().saturating_sub(keep_count))
            .map(|(i, _)| *i)
            .collect();
        let mut i = 0;
        backups.retain(|_| {
            let keep = !to_remove.contains(&i);
            i += 1;
            keep
        });
    }
}

impl Database {
    /// Collect statistics from the storage engine.
    pub fn stats(&self) -> DatabaseStats {
        let vertices = self.storage.all_vertices();
        let edges = self.storage.all_edges();
        let indices = self.storage.active_label_indices.read().unwrap().len()
            + self
                .storage
                .active_label_property_indices
                .read()
                .unwrap()
                .len();
        let constraints = self.storage.constraints.list().len();
        // Rough estimate: ~128 bytes per vertex, ~64 bytes per edge
        let mem = vertices.len() * 128 + edges.len() * 64;
        DatabaseStats {
            vertex_count: vertices.len(),
            edge_count: edges.len(),
            index_count: indices,
            constraint_count: constraints,
            memory_estimate_bytes: mem,
            trigger_count: self.storage.triggers.list().len(),
            transaction_count: self
                .transaction_count
                .load(std::sync::atomic::Ordering::Relaxed),
            query_count: self.query_count.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    /// Check if the database is within its quota.
    pub fn check_quota(&self, quota: &DatabaseQuota) -> Result<(), DbmsError> {
        let stats = self.stats();
        if let Some(max_v) = quota.max_vertices {
            if stats.vertex_count > max_v {
                return Err(DbmsError::QuotaExceeded(format!(
                    "vertex count {} exceeds quota {}",
                    stats.vertex_count, max_v
                )));
            }
        }
        if let Some(max_e) = quota.max_edges {
            if stats.edge_count > max_e {
                return Err(DbmsError::QuotaExceeded(format!(
                    "edge count {} exceeds quota {}",
                    stats.edge_count, max_e
                )));
            }
        }
        if let Some(max_mem) = quota.max_memory_mb {
            let mem_mb = stats.memory_estimate_bytes / (1024 * 1024);
            if mem_mb > max_mem {
                return Err(DbmsError::QuotaExceeded(format!(
                    "memory {}MB exceeds quota {}MB",
                    mem_mb, max_mem
                )));
            }
        }
        Ok(())
    }
}

impl DbmsHandler {
    /// Get statistics for all databases.
    pub fn all_stats(&self) -> HashMap<DatabaseName, DatabaseStats> {
        let dbs = self.databases.read().expect("lock poisoned");
        dbs.iter()
            .map(|(name, db)| (name.clone(), db.stats()))
            .collect()
    }

    /// Get statistics for a single database.
    pub fn db_stats(&self, name: &DatabaseName) -> Option<DatabaseStats> {
        self.get(name).map(|db| db.stats())
    }

    /// Set a database's state.
    pub fn set_state(&self, name: &DatabaseName, state: DatabaseState) -> Result<(), DbmsError> {
        let db = self
            .get(name)
            .ok_or_else(|| DbmsError::DatabaseNotFound(name.clone()))?;
        *db.state.write().expect("lock poisoned") = state;
        Ok(())
    }

    /// Get a database's state.
    pub fn get_state(&self, name: &DatabaseName) -> Option<DatabaseState> {
        self.get(name)
            .map(|db| *db.state.read().expect("lock poisoned"))
    }

    /// Rename a database (not allowed for default).
    pub fn rename(&self, old_name: &DatabaseName, new_name: DatabaseName) -> Result<(), DbmsError> {
        if old_name == &self.default_db {
            return Err(DbmsError::CannotRenameDefault);
        }
        let mut dbs = self.databases.write().expect("lock poisoned");
        if dbs.contains_key(&new_name) {
            return Err(DbmsError::DatabaseExists(new_name));
        }
        let db = dbs
            .remove(old_name)
            .ok_or_else(|| DbmsError::DatabaseNotFound(old_name.clone()))?;
        // Arc::make_mut to get mutable access — this is safe because we're the only reference holder
        // in practice. For full correctness, Database fields should use interior mutability.
        dbs.insert(new_name, db);
        Ok(())
    }
}

/// Database access role for RBAC within a database.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DatabaseRole {
    Owner,
    Admin,
    ReadWrite,
    ReadOnly,
}

/// User account within a database.
pub struct DbUser {
    pub username: String,
    pub role: DatabaseRole,
    pub created_at: std::time::SystemTime,
}

/// Connection pool limit for a database.
#[derive(Clone, Debug)]
pub struct ConnectionLimits {
    pub max_connections: usize,
    pub max_queries_per_second: Option<u64>,
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            max_queries_per_second: None,
        }
    }
}

/// Query execution tracker for a single database.
pub struct QueryTracker {
    total_queries: std::sync::atomic::AtomicU64,
    failed_queries: std::sync::atomic::AtomicU64,
    total_latency_ms: std::sync::atomic::AtomicU64,
}

impl Default for QueryTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryTracker {
    pub fn new() -> Self {
        Self {
            total_queries: std::sync::atomic::AtomicU64::new(0),
            failed_queries: std::sync::atomic::AtomicU64::new(0),
            total_latency_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn record_query(&self, latency_ms: u64, success: bool) {
        self.total_queries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.total_latency_ms
            .fetch_add(latency_ms, std::sync::atomic::Ordering::Relaxed);
        if !success {
            self.failed_queries
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn total_queries(&self) -> u64 {
        self.total_queries
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn failed_queries(&self) -> u64 {
        self.failed_queries
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn avg_latency_ms(&self) -> u64 {
        let total = self
            .total_queries
            .load(std::sync::atomic::Ordering::Relaxed);
        if total == 0 {
            0
        } else {
            self.total_latency_ms
                .load(std::sync::atomic::Ordering::Relaxed)
                / total
        }
    }
}

/// Replication configuration for a database.
#[derive(Clone, Debug)]
pub struct ReplicationConfig {
    pub role: ReplicationRole,
    pub primary_addr: Option<String>,
    pub replica_addrs: Vec<String>,
    pub sync_mode: SyncMode,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplicationRole {
    Primary,
    Replica,
    Standalone,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SyncMode {
    Sync,
    Async,
    StrictSync,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            role: ReplicationRole::Standalone,
            primary_addr: None,
            replica_addrs: vec![],
            sync_mode: SyncMode::Async,
        }
    }
}

/// Extended database with runtime configuration.
pub struct DatabaseRuntime {
    pub db: Arc<Database>,
    pub quota: RwLock<DatabaseQuota>,
    pub limits: RwLock<ConnectionLimits>,
    pub tracker: QueryTracker,
    pub replication: RwLock<ReplicationConfig>,
    pub users: RwLock<HashMap<String, DbUser>>,
}

impl DatabaseRuntime {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            quota: RwLock::new(DatabaseQuota::default()),
            limits: RwLock::new(ConnectionLimits::default()),
            tracker: QueryTracker::new(),
            replication: RwLock::new(ReplicationConfig::default()),
            users: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_user(&self, username: String, role: DatabaseRole) {
        let user = DbUser {
            username: username.clone(),
            role,
            created_at: std::time::SystemTime::now(),
        };
        self.users
            .write()
            .expect("lock poisoned")
            .insert(username, user);
    }

    pub fn remove_user(&self, username: &str) -> bool {
        self.users
            .write()
            .expect("lock poisoned")
            .remove(username)
            .is_some()
    }

    pub fn get_user(&self, username: &str) -> Option<DbUser> {
        self.users
            .read()
            .expect("lock poisoned")
            .get(username)
            .cloned()
    }

    pub fn set_quota(&self, quota: DatabaseQuota) {
        *self.quota.write().expect("lock poisoned") = quota;
    }

    pub fn set_limits(&self, limits: ConnectionLimits) {
        *self.limits.write().expect("lock poisoned") = limits;
    }

    pub fn set_replication(&self, config: ReplicationConfig) {
        *self.replication.write().expect("lock poisoned") = config;
    }
}

impl Clone for DbUser {
    fn clone(&self) -> Self {
        Self {
            username: self.username.clone(),
            role: self.role,
            created_at: self.created_at,
        }
    }
}

/// Extended DBMS handler with runtime state.
pub struct DbmsRuntime {
    handler: DbmsHandler,
    runtimes: RwLock<HashMap<DatabaseName, Arc<DatabaseRuntime>>>,
    global_limits: RwLock<ConnectionLimits>,
}

impl Default for DbmsRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl DbmsRuntime {
    pub fn new() -> Self {
        let handler = DbmsHandler::new();
        let mut runtimes = HashMap::new();
        let default_rt = Arc::new(DatabaseRuntime::new(handler.default_db()));
        runtimes.insert("default".to_string(), default_rt);
        Self {
            handler,
            runtimes: RwLock::new(runtimes),
            global_limits: RwLock::new(ConnectionLimits::default()),
        }
    }

    pub fn handler(&self) -> &DbmsHandler {
        &self.handler
    }

    pub fn create_db(&self, name: DatabaseName) -> Result<Arc<DatabaseRuntime>, DbmsError> {
        let db = self.handler.create(name.clone())?;
        let rt = Arc::new(DatabaseRuntime::new(db));
        self.runtimes
            .write()
            .expect("lock poisoned")
            .insert(name, rt.clone());
        Ok(rt)
    }

    pub fn drop_db(&self, name: &DatabaseName) -> Result<(), DbmsError> {
        self.handler.drop(name)?;
        self.runtimes.write().expect("lock poisoned").remove(name);
        Ok(())
    }

    pub fn get_runtime(&self, name: &DatabaseName) -> Option<Arc<DatabaseRuntime>> {
        self.runtimes
            .read()
            .expect("lock poisoned")
            .get(name)
            .cloned()
    }

    pub fn default_runtime(&self) -> Arc<DatabaseRuntime> {
        self.get_runtime(&"default".to_string())
            .expect("default runtime must exist")
    }

    pub fn all_runtimes(&self) -> Vec<(DatabaseName, Arc<DatabaseRuntime>)> {
        self.runtimes
            .read()
            .expect("lock poisoned")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn set_global_limits(&self, limits: ConnectionLimits) {
        *self.global_limits.write().expect("lock poisoned") = limits;
    }

    pub fn global_limits(&self) -> ConnectionLimits {
        self.global_limits.read().expect("lock poisoned").clone()
    }

    /// Health check for a specific database.
    pub fn check_health(&self, name: &DatabaseName) -> Result<DatabaseHealth, DbmsError> {
        let rt = self
            .get_runtime(name)
            .ok_or_else(|| DbmsError::DatabaseNotFound(name.clone()))?;
        let stats = rt.db.stats();
        let quota = rt.quota.read().expect("lock poisoned").clone();
        let quota_ok = rt.db.check_quota(&quota).is_ok();
        let state = *rt.db.state.read().expect("lock poisoned");
        let query_rate = rt.tracker.total_queries();
        let avg_latency_ms = rt.tracker.avg_latency_ms();

        Ok(DatabaseHealth {
            db_name: name.clone(),
            state,
            vertex_count: stats.vertex_count,
            edge_count: stats.edge_count,
            memory_estimate_mb: stats.memory_estimate_bytes / (1024 * 1024),
            quota_ok,
            query_rate,
            avg_latency_ms,
        })
    }

    /// Health check for all databases.
    pub fn all_health(&self) -> Vec<DatabaseHealth> {
        self.all_runtimes()
            .into_iter()
            .filter_map(|(name, _)| self.check_health(&name).ok())
            .collect()
    }

    /// Total queries across all databases.
    pub fn total_queries(&self) -> u64 {
        self.all_runtimes()
            .into_iter()
            .map(|(_, rt)| rt.tracker.total_queries())
            .sum()
    }

    /// Total failed queries across all databases.
    pub fn total_failed_queries(&self) -> u64 {
        self.all_runtimes()
            .into_iter()
            .map(|(_, rt)| rt.tracker.failed_queries())
            .sum()
    }
}

/// Health status for a single database.
#[derive(Clone, Debug)]
pub struct DatabaseHealth {
    pub db_name: DatabaseName,
    pub state: DatabaseState,
    pub vertex_count: usize,
    pub edge_count: usize,
    pub memory_estimate_mb: usize,
    pub quota_ok: bool,
    pub query_rate: u64,
    pub avg_latency_ms: u64,
}

/// Connection pool tracker per database.
pub struct ConnectionPool {
    pub active: std::sync::atomic::AtomicU64,
    pub max: usize,
    pub waiting: std::sync::atomic::AtomicU64,
}

impl ConnectionPool {
    pub fn new(max: usize) -> Self {
        Self {
            active: std::sync::atomic::AtomicU64::new(0),
            max,
            waiting: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn try_acquire(&self) -> bool {
        let current = self
            .active
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if current as usize >= self.max {
            self.active
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    pub fn release(&self) {
        self.active
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn active(&self) -> u64 {
        self.active.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn waiting(&self) -> u64 {
        self.waiting.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Query routing strategy for multi-database setups.
pub enum RoutingStrategy {
    RoundRobin,
    LeastLoaded,
    Random,
}

/// Simple query router for distributing reads across databases.
pub struct QueryRouter {
    strategy: RoutingStrategy,
    round_robin_index: std::sync::atomic::AtomicU64,
}

impl QueryRouter {
    pub fn new(strategy: RoutingStrategy) -> Self {
        Self {
            strategy,
            round_robin_index: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Select a database for a read query.
    pub fn select_read_db(&self, candidates: &[DatabaseName]) -> Option<DatabaseName> {
        if candidates.is_empty() {
            return None;
        }
        match self.strategy {
            RoutingStrategy::RoundRobin => {
                let idx = self
                    .round_robin_index
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    as usize
                    % candidates.len();
                Some(candidates[idx].clone())
            }
            RoutingStrategy::Random => {
                use rand::Rng;
                let idx = rand::thread_rng().gen_range(0..candidates.len());
                Some(candidates[idx].clone())
            }
            RoutingStrategy::LeastLoaded => {
                // Simplified: just pick first for now
                Some(candidates[0].clone())
            }
        }
    }
}

/// Lifecycle hooks for database startup/shutdown.
pub trait DatabaseLifecycleHook: Send + Sync {
    fn on_startup(&self, db_name: &DatabaseName);
    fn on_shutdown(&self, db_name: &DatabaseName);
}

/// Registry of lifecycle hooks.
pub struct LifecycleRegistry {
    hooks: RwLock<Vec<Box<dyn DatabaseLifecycleHook>>>,
}

impl Default for LifecycleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleRegistry {
    pub fn new() -> Self {
        Self {
            hooks: RwLock::new(Vec::new()),
        }
    }

    pub fn register(&self, hook: Box<dyn DatabaseLifecycleHook>) {
        self.hooks.write().expect("lock poisoned").push(hook);
    }

    pub fn startup(&self, db_name: &DatabaseName) {
        for hook in self.hooks.read().expect("lock poisoned").iter() {
            hook.on_startup(db_name);
        }
    }

    pub fn shutdown(&self, db_name: &DatabaseName) {
        for hook in self.hooks.read().expect("lock poisoned").iter() {
            hook.on_shutdown(db_name);
        }
    }
}

/// Instance-wide metadata.
#[derive(Clone, Debug)]
pub struct InstanceInfo {
    pub instance_id: String,
    pub version: String,
    pub start_time: std::time::SystemTime,
    pub config_path: Option<String>,
}

impl InstanceInfo {
    pub fn new() -> Self {
        Self {
            instance_id: uuid::Uuid::new_v4().to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            start_time: std::time::SystemTime::now(),
            config_path: None,
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().unwrap_or_default().as_secs()
    }
}

impl Default for InstanceInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_database() {
        let handler = DbmsHandler::new();
        let db = handler.default_db();
        assert_eq!(db.name, "default");
    }

    #[test]
    fn test_create_and_drop_database() {
        let handler = DbmsHandler::new();
        let db = handler.create("testdb".into()).unwrap();
        assert_eq!(db.name, "testdb");
        assert!(handler.exists(&"testdb".into()));

        handler.drop(&"testdb".into()).unwrap();
        assert!(!handler.exists(&"testdb".into()));
    }

    #[test]
    fn test_cannot_drop_default() {
        let handler = DbmsHandler::new();
        let err = handler.drop(&"default".into()).unwrap_err();
        assert_eq!(err, DbmsError::CannotDropDefault);
    }

    #[test]
    fn test_duplicate_create() {
        let handler = DbmsHandler::new();
        handler.create("dup".into()).unwrap();
        let err = handler.create("dup".into()).unwrap_err();
        assert_eq!(err, DbmsError::DatabaseExists("dup".into()));
    }

    #[test]
    fn test_tenant_assignment() {
        let handler = DbmsHandler::new();
        handler
            .assign_tenant(&"default".into(), "tenant-1".into())
            .unwrap();
        assert_eq!(handler.tenant(&"default".into()), Some("tenant-1".into()));
    }

    #[test]
    fn test_db_stats_empty() {
        let handler = DbmsHandler::new();
        let stats = handler.db_stats(&"default".into()).unwrap();
        assert_eq!(stats.vertex_count, 0);
        assert_eq!(stats.edge_count, 0);
    }

    #[test]
    fn test_database_state() {
        let handler = DbmsHandler::new();
        assert_eq!(
            handler.get_state(&"default".into()),
            Some(DatabaseState::Online)
        );
        handler
            .set_state(&"default".into(), DatabaseState::Offline)
            .unwrap();
        assert_eq!(
            handler.get_state(&"default".into()),
            Some(DatabaseState::Offline)
        );
    }

    #[test]
    fn test_rename_database() {
        let handler = DbmsHandler::new();
        handler.create("old".into()).unwrap();
        handler.rename(&"old".into(), "new".into()).unwrap();
        assert!(!handler.exists(&"old".into()));
        assert!(handler.exists(&"new".into()));
    }

    #[test]
    fn test_cannot_rename_default() {
        let handler = DbmsHandler::new();
        assert!(matches!(
            handler.rename(&"default".into(), "other".into()),
            Err(DbmsError::CannotRenameDefault)
        ));
    }

    #[test]
    fn test_quota_check() {
        let db = Database::new("test".into());
        let quota = DatabaseQuota {
            max_vertices: Some(100),
            max_edges: Some(100),
            max_memory_mb: Some(1024),
        };
        assert!(db.check_quota(&quota).is_ok());
    }

    #[test]
    fn test_dbms_runtime_create_and_drop() {
        let runtime = DbmsRuntime::new();
        let rt = runtime.create_db("test_rt".into()).unwrap();
        assert_eq!(rt.db.name, "test_rt");
        assert!(runtime.get_runtime(&"test_rt".to_string()).is_some());
        runtime.drop_db(&"test_rt".to_string()).unwrap();
        assert!(runtime.get_runtime(&"test_rt".to_string()).is_none());
    }

    #[test]
    fn test_dbms_health_check() {
        let runtime = DbmsRuntime::new();
        let health = runtime.check_health(&"default".to_string()).unwrap();
        assert_eq!(health.db_name, "default");
        assert!(health.quota_ok);
    }

    #[test]
    fn test_dbms_all_health() {
        let runtime = DbmsRuntime::new();
        runtime.create_db("db2".into()).unwrap();
        let all = runtime.all_health();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_connection_pool() {
        let pool = ConnectionPool::new(2);
        assert!(pool.try_acquire());
        assert!(pool.try_acquire());
        assert!(!pool.try_acquire());
        pool.release();
        assert!(pool.try_acquire());
    }

    #[test]
    fn test_query_router_round_robin() {
        let router = QueryRouter::new(RoutingStrategy::RoundRobin);
        let candidates = vec!["db1".into(), "db2".into(), "db3".into()];
        let r1 = router.select_read_db(&candidates).unwrap();
        let _r2 = router.select_read_db(&candidates).unwrap();
        let r3 = router.select_read_db(&candidates).unwrap();
        assert_ne!(r1, r3); // round-robin cycles through
    }

    #[test]
    fn test_query_router_empty() {
        let router = QueryRouter::new(RoutingStrategy::Random);
        assert!(router.select_read_db(&[]).is_none());
    }

    #[test]
    fn test_lifecycle_registry() {
        struct TestHook;
        impl DatabaseLifecycleHook for TestHook {
            fn on_startup(&self, _db_name: &DatabaseName) {}
            fn on_shutdown(&self, _db_name: &DatabaseName) {}
        }
        let registry = LifecycleRegistry::new();
        registry.register(Box::new(TestHook));
        registry.startup(&"default".to_string());
        registry.shutdown(&"default".to_string());
    }

    #[test]
    fn test_instance_info() {
        let info = InstanceInfo::new();
        assert!(!info.instance_id.is_empty());
        assert!(!info.version.is_empty());
        assert!(info.uptime_secs() < 10);
    }

    #[test]
    fn test_query_tracker() {
        let tracker = QueryTracker::new();
        tracker.record_query(100, true);
        tracker.record_query(200, false);
        assert_eq!(tracker.total_queries(), 2);
        assert_eq!(tracker.failed_queries(), 1);
        assert_eq!(tracker.avg_latency_ms(), 150);
    }
}
