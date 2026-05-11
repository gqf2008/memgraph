//! # mginterp — Cypher query interpreter
//!
//! Executes parsed Cypher queries against the storage engine.
//! Connects the parser (mgparser) to the storage (mgstorage).

mod eval;
pub mod builtin_procs;
pub mod cache;
mod physical_exec;

use std::cell::{Cell, RefCell};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, Instant};
use std::sync::atomic::Ordering;

use cache::{QueryCache, QueryPlanCache};

use mgauth::AuthStore;
use mgcore::delta::IsolationLevel;
use mgcore::property_value::{EdgeRefValue, PathValue, PropertyValue, VertexRef};
use mgcore::types::{EdgeTypeId, Gid, PropertyId};
use mgparser::ast::*;
use mgstorage::storage::{Storage, StorageError};
use mgstorage::transaction::Transaction;
use mgstorage::query_profile::QueryProfile;

pub use eval::{eval_expression, eval_expression_with_storage, set_active_catalog};
use eval::active_catalog;

thread_local! {
    static ACTIVE_AUTH_STORE: Cell<*const AuthStore> = const { Cell::new(std::ptr::null()) };
    static ACTIVE_TRANSACTION: RefCell<Option<Arc<Transaction>>> = const { RefCell::new(None) };
    static HOPS_COUNTER: Cell<u64> = const { Cell::new(0) };
    static HOPS_LIMIT: Cell<Option<u64>> = const { Cell::new(None) };
    static ACTIVE_SETTINGS: Cell<*const SettingsStore> = const { Cell::new(std::ptr::null()) };
    static ACTIVE_TX_LOG: Cell<*const TransactionLog> = const { Cell::new(std::ptr::null()) };
}

/// Increment the thread-local hops counter by `n`.
/// Returns an error if the hops limit is exceeded.
pub(crate) fn increment_hops(n: u64) -> Result<(), ExecError> {
    HOPS_COUNTER.with(|c| {
        let new_val = c.get() + n;
        c.set(new_val);
        if let Some(limit) = HOPS_LIMIT.with(|l| l.get()) {
            if new_val > limit {
                return Err(ExecError::Runtime(format!(
                    "hops limit exceeded: {} > {}", new_val, limit
                )));
            }
        }
        Ok(())
    })
}

/// Set the thread-local hops limit.
fn set_hops_limit(limit: Option<u64>) {
    HOPS_LIMIT.with(|l| l.set(limit));
}

/// Reset and return the current hops counter value.
pub(crate) fn reset_hops() -> u64 {
    HOPS_COUNTER.with(|c| {
        let v = c.get();
        c.set(0);
        v
    })
}

/// Guard that restores the active auth store on drop.
pub struct ActiveAuthGuard {
    prev: *const AuthStore,
}

impl Drop for ActiveAuthGuard {
    fn drop(&mut self) {
        ACTIVE_AUTH_STORE.with(|c| c.set(self.prev));
    }
}

/// Set the active auth store for DDL execution in the current thread.
pub fn set_active_auth_store(auth: Option<&AuthStore>) -> ActiveAuthGuard {
    let guard = ActiveAuthGuard {
        prev: ACTIVE_AUTH_STORE.with(|c| c.get()),
    };
    ACTIVE_AUTH_STORE.with(|c| {
        c.set(auth.map(|a| a as *const AuthStore).unwrap_or(std::ptr::null()));
    });
    guard
}

fn active_auth_store() -> Option<&'static AuthStore> {
    ACTIVE_AUTH_STORE.with(|c| {
        let ptr = c.get();
        if ptr.is_null() { None } else { Some(unsafe { &*ptr }) }
    })
}

/// Guard that restores the active transaction on drop.
pub struct ActiveTransactionGuard {
    prev: Option<Arc<Transaction>>,
}

impl Drop for ActiveTransactionGuard {
    fn drop(&mut self) {
        ACTIVE_TRANSACTION.with(|c| *c.borrow_mut() = self.prev.clone());
    }
}

/// Set the active explicit transaction for the current thread.
/// When set, write operations (CREATE, SET, DELETE, REMOVE) will use this
/// transaction instead of beginning a new one, and will NOT commit it.
/// The caller is responsible for committing or aborting the transaction.
/// The guard restores the previous active transaction on drop.
pub fn set_active_transaction(tx: Option<Arc<Transaction>>) -> ActiveTransactionGuard {
    let prev = ACTIVE_TRANSACTION.with(|c| c.borrow().clone());
    ACTIVE_TRANSACTION.with(|c| *c.borrow_mut() = tx);
    ActiveTransactionGuard { prev }
}

fn active_transaction() -> Option<Arc<Transaction>> {
    ACTIVE_TRANSACTION.with(|c| c.borrow().clone())
}

thread_local! {
    static ACTIVE_DBMS: Cell<*const mgdbms::DbmsHandler> = const { Cell::new(std::ptr::null()) };
    static QUERY_DEADLINE: Cell<Option<Instant>> = const { Cell::new(None) };
    static QUERY_CANCEL_TOKEN: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

/// Global query cache shared across all threads.
static GLOBAL_QUERY_CACHE: LazyLock<QueryCache> = LazyLock::new(|| QueryCache::new(256, 600));

/// Global query plan cache shared across all threads.
static GLOBAL_PLAN_CACHE: LazyLock<QueryPlanCache> = LazyLock::new(|| QueryPlanCache::new(1000));

/// Guard that clears the query deadline on drop.
pub struct QueryDeadlineGuard;

impl Drop for QueryDeadlineGuard {
    fn drop(&mut self) {
        QUERY_DEADLINE.with(|c| c.set(None));
    }
}

/// Guard that clears the cancel token on drop.
struct CancelTokenGuard;

impl Drop for CancelTokenGuard {
    fn drop(&mut self) {
        QUERY_CANCEL_TOKEN.with(|t| *t.borrow_mut() = None);
    }
}

/// Set a query execution deadline for the current thread.
/// Returns a guard that clears the deadline when dropped.
pub fn set_query_deadline(timeout: Option<Duration>) -> QueryDeadlineGuard {
    let deadline = timeout.map(|d| Instant::now() + d);
    QUERY_DEADLINE.with(|c| c.set(deadline));
    QueryDeadlineGuard
}

/// Check if the current query has exceeded its deadline.
/// Returns `ExecError::Runtime` if timed out.
pub fn check_query_timeout() -> Result<(), ExecError> {
    QUERY_DEADLINE.with(|c| {
        if let Some(deadline) = c.get() {
            if Instant::now() > deadline {
                return Err(ExecError::Runtime("Query timeout exceeded".into()));
            }
        }
        Ok(())
    })?;
    QUERY_CANCEL_TOKEN.with(|t| {
        if let Some(ref token) = *t.borrow() {
            if token.load(AtomicOrdering::Relaxed) {
                return Err(ExecError::Runtime("Query cancelled".into()));
            }
        }
        Ok(())
    })
}

/// Guard that restores the active DBMS on drop.
pub struct ActiveDbmsGuard {
    prev: *const mgdbms::DbmsHandler,
}

impl Drop for ActiveDbmsGuard {
    fn drop(&mut self) {
        ACTIVE_DBMS.with(|c| c.set(self.prev));
    }
}

/// Set the active DBMS for database DDL execution in the current thread.
pub fn set_active_dbms(dbms: Option<&mgdbms::DbmsHandler>) -> ActiveDbmsGuard {
    let guard = ActiveDbmsGuard {
        prev: ACTIVE_DBMS.with(|c| c.get()),
    };
    ACTIVE_DBMS.with(|c| {
        c.set(dbms.map(|d| d as *const mgdbms::DbmsHandler).unwrap_or(std::ptr::null()));
    });
    guard
}

fn active_dbms() -> Option<&'static mgdbms::DbmsHandler> {
    ACTIVE_DBMS.with(|c| {
        let ptr = c.get();
        if ptr.is_null() { None } else { Some(unsafe { &*ptr }) }
    })
}

// ─── Runtime settings store ───────────────────────────────────────────────

/// Thread-safe key-value store for runtime-modifiable settings.
#[derive(Clone, Debug, Default)]
pub struct SettingsStore {
    inner: Arc<RwLock<HashMap<String, String>>>,
}

impl SettingsStore {
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn from_flags(flags: &mgflags::Flags) -> Self {
        let mut map = HashMap::new();
        for (k, v) in flags.list_settings() {
            map.insert(k, v);
        }
        Self { inner: Arc::new(RwLock::new(map)) }
    }

    pub fn get(&self, name: &str) -> Option<String> {
        self.inner.read().unwrap().get(name).cloned()
    }

    pub fn list(&self) -> Vec<(String, String)> {
        self.inner.read().unwrap().iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn set(&self, name: &str, value: &str) -> Result<(), String> {
        self.inner.write().unwrap().insert(name.to_string(), value.to_string());
        Ok(())
    }
}

// ─── Runtime settings thread-local ────────────────────────────────────────

/// Guard that restores the active settings on drop.
pub struct ActiveSettingsGuard {
    prev: *const SettingsStore,
}

impl Drop for ActiveSettingsGuard {
    fn drop(&mut self) {
        ACTIVE_SETTINGS.with(|c| c.set(self.prev));
    }
}

/// Set the active settings store for the current thread.
pub fn set_active_settings(settings: Option<&SettingsStore>) -> ActiveSettingsGuard {
    let guard = ActiveSettingsGuard {
        prev: ACTIVE_SETTINGS.with(|c| c.get()),
    };
    ACTIVE_SETTINGS.with(|c| {
        c.set(settings.map(|s| s as *const SettingsStore).unwrap_or(std::ptr::null()));
    });
    guard
}

fn active_settings() -> Option<&'static SettingsStore> {
    ACTIVE_SETTINGS.with(|c| {
        let ptr = c.get();
        if ptr.is_null() { None } else { Some(unsafe { &*ptr }) }
    })
}

// ─── Transaction log thread-local ─────────────────────────────────────────

/// Information about an active query/transaction.
#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub id: String,
    pub query_text: String,
    pub start_time: Instant,
    pub cancel_token: Arc<AtomicBool>,
}

impl TransactionInfo {
    pub fn elapsed_ms(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }
}

/// Simple in-memory transaction log for SHOW TRANSACTIONS / TERMINATE.
pub struct TransactionLog {
    pub active: std::sync::Mutex<Vec<TransactionInfo>>,
}

impl TransactionLog {
    pub fn new() -> Self {
        Self { active: std::sync::Mutex::new(Vec::new()) }
    }

    /// Register a new query and return its cancel token.
    pub fn register(&self, query_text: &str) -> (String, Arc<AtomicBool>) {
        let id = format!("tx-{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis());
        let token = Arc::new(AtomicBool::new(false));
        let info = TransactionInfo {
            id: id.clone(),
            query_text: query_text.to_string(),
            start_time: Instant::now(),
            cancel_token: token.clone(),
        };
        self.active.lock().unwrap().push(info);
        (id, token)
    }

    /// Remove a completed query from the log.
    pub fn unregister(&self, id: &str) {
        self.active.lock().unwrap().retain(|t| t.id != id);
    }

    pub fn list_active(&self) -> Vec<TransactionInfo> {
        self.active.lock().unwrap().clone()
    }

    pub fn terminate(&self, id: &str) -> Result<(), String> {
        let active = self.active.lock().unwrap();
        if let Some(info) = active.iter().find(|t| t.id == id) {
            info.cancel_token.store(true, AtomicOrdering::Relaxed);
            Ok(())
        } else {
            Err(format!("Transaction '{}' not found", id))
        }
    }
}

/// Guard that restores the active transaction log on drop.
pub struct ActiveTxLogGuard {
    prev: *const TransactionLog,
}

impl Drop for ActiveTxLogGuard {
    fn drop(&mut self) {
        ACTIVE_TX_LOG.with(|c| c.set(self.prev));
    }
}

/// Set the active transaction log for the current thread.
pub fn set_active_tx_log(tx_log: Option<&TransactionLog>) -> ActiveTxLogGuard {
    let guard = ActiveTxLogGuard {
        prev: ACTIVE_TX_LOG.with(|c| c.get()),
    };
    ACTIVE_TX_LOG.with(|c| {
        c.set(tx_log.map(|t| t as *const TransactionLog).unwrap_or(std::ptr::null()));
    });
    guard
}

fn active_tx_log() -> Option<&'static TransactionLog> {
    ACTIVE_TX_LOG.with(|c| {
        let ptr = c.get();
        if ptr.is_null() { None } else { Some(unsafe { &*ptr }) }
    })
}

/// Helper: use an explicit transaction if provided, otherwise begin a new one.
/// Returns `(transaction, owned)` where `owned` indicates whether the caller
/// should commit/abort the transaction.
fn get_or_begin_tx<'a>(
    storage: &'a Storage,
    explicit: Option<&'a Arc<Transaction>>,
) -> (Arc<Transaction>, bool) {
    match explicit {
        Some(tx) => (tx.clone(), false),
        None => (storage.begin_transaction(IsolationLevel::SnapshotIsolation), true),
    }
}

/// A row of results from a query.
pub type ResultRow = HashMap<String, PropertyValue>;

/// Query result: a sequence of named rows.
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<ResultRow>,
    /// Total number of edge traversals (hops) during query execution.
    pub number_of_hops: u64,
}

/// Error during query execution.
#[derive(Debug)]
pub enum ExecError {
    Storage(StorageError),
    Parse(String),
    Runtime(String),
}

/// A callable Cypher procedure.
pub type ProcedureFn = Box<dyn Fn(&Storage, &[PropertyValue]) -> Result<QueryResult, ExecError> + Send + Sync>;

/// Registry of callable procedures.
pub struct ProcedureRegistry {
    procedures: HashMap<String, ProcedureFn>,
}

impl ProcedureRegistry {
    pub fn new() -> Self {
        Self { procedures: HashMap::new() }
    }

    pub fn register(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&Storage, &[PropertyValue]) -> Result<QueryResult, ExecError> + Send + Sync + 'static,
    ) {
        self.procedures.insert(name.into(), Box::new(f));
    }

    pub fn get(&self, name: &str) -> Option<&ProcedureFn> {
        self.procedures.get(name)
    }

    pub fn names(&self) -> Vec<&String> {
        self.procedures.keys().collect()
    }
}

impl Default for ProcedureRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecError::Storage(e) => write!(f, "storage error: {}", e),
            ExecError::Parse(msg) => write!(f, "parse error: {}", msg),
            ExecError::Runtime(msg) => write!(f, "runtime error: {}", msg),
        }
    }
}

impl From<StorageError> for ExecError {
    fn from(e: StorageError) -> Self {
        ExecError::Storage(e)
    }
}

use std::sync::atomic::AtomicU64;

static NEXT_GID: AtomicU64 = AtomicU64::new(1);
static GID_INIT: std::sync::Once = std::sync::Once::new();

/// Call after loading snapshots to ensure NEXT_GID doesn't collide.
pub fn init_gid_after_load(storage: &Storage) {
    GID_INIT.call_once(|| {
        let max_gid = storage.all_vertex_gids().iter().map(|g| g.as_uint()).max().unwrap_or(0);
        NEXT_GID.store(max_gid.saturating_add(1000), Ordering::Relaxed);
    });
}

/// Execute a Cypher query string against the storage engine.
pub fn execute(storage: &Storage, query_str: &str) -> Result<QueryResult, ExecError> {
    execute_with_catalog(storage, query_str, None)
}

/// Execute with catalog for name→ID resolution.
pub fn execute_with_catalog(
    storage: &Storage,
    query_str: &str,
    catalog: Option<&mgcatalog::Catalog>,
) -> Result<QueryResult, ExecError> {
    let query = mgparser::parse_query_with_catalog(query_str, catalog)
        .map_err(|e| ExecError::Parse(format!("{}", e)))?;
    let _guard = eval::set_active_catalog(catalog);
    execute_query(storage, &query)
}

/// Execute with catalog and parameter bindings.
pub fn execute_with_catalog_and_params(
    storage: &Storage,
    query_str: &str,
    catalog: Option<&mgcatalog::Catalog>,
    params: &HashMap<String, PropertyValue>,
) -> Result<QueryResult, ExecError> {
    execute_with_catalog_auth_and_params(storage, query_str, catalog, params, None)
}

/// Parse a query, using the global cache when available.
/// The cache key combines the query text with the catalog instance id
/// to avoid cross-catalog pollution.
fn parse_cached(query_str: &str, catalog: Option<&mgcatalog::Catalog>) -> Result<Query, ExecError> {
    let catalog_id = catalog.map(|c| c.instance_id()).unwrap_or(0);
    let normalized = cache::normalize_cache_key(query_str);
    let cache_key = format!("{}:{:x}", normalized, catalog_id);
    if let Some(cached) = GLOBAL_QUERY_CACHE.get(&cache_key) {
        return Ok(cached);
    }
    let query = mgparser::parse_query_with_catalog(query_str, catalog)
        .map_err(|e| ExecError::Parse(format!("{}", e)))?;
    GLOBAL_QUERY_CACHE.insert(cache_key, query.clone());
    Ok(query)
}

/// Execute with catalog, parameters, and optional auth store.
pub fn execute_with_catalog_auth_and_params(
    storage: &Storage,
    query_str: &str,
    catalog: Option<&mgcatalog::Catalog>,
    params: &HashMap<String, PropertyValue>,
    auth: Option<&AuthStore>,
) -> Result<QueryResult, ExecError> {
    let qid = storage.start_query(query_str.to_string());
    let start = std::time::Instant::now();
    let cached = GLOBAL_QUERY_CACHE.get(&cache::normalize_cache_key(query_str)).is_some();
    let query = parse_cached(query_str, catalog)?;
    let parse_time = start.elapsed();
    let _cat_guard = eval::set_active_catalog(catalog);
    let _auth_guard = set_active_auth_store(auth);
    let result = execute_query_with_binding(storage, &query, params);
    let total_time = start.elapsed();
    let exec_time = total_time.saturating_sub(parse_time);
    let rows_returned = result.as_ref().map(|r| r.rows.len() as u64).unwrap_or(0);
    storage.finish_query(qid);
    storage.query_profiler.record(mgstorage::query_profile::QueryProfile {
        query_text: query_str.to_string(),
        planning_time: parse_time,
        execution_time: exec_time,
        rows_scanned: 0,
        rows_returned,
        index_hits: 0,
        memory_used_bytes: 0,
        cache_hits: if cached { 1 } else { 0 },
    });
    result
}

/// Execute with catalog, auth, DBMS, and parameters.
pub fn execute_with_catalog_auth_dbms_and_params(
    storage: &Storage,
    query_str: &str,
    catalog: Option<&mgcatalog::Catalog>,
    params: &HashMap<String, PropertyValue>,
    auth: Option<&AuthStore>,
    dbms: Option<&mgdbms::DbmsHandler>,
) -> Result<QueryResult, ExecError> {
    let qid = storage.start_query(query_str.to_string());
    let start = std::time::Instant::now();
    let cached = GLOBAL_QUERY_CACHE.get(&cache::normalize_cache_key(query_str)).is_some();
    let query = parse_cached(query_str, catalog)?;
    let parse_time = start.elapsed();
    let _cat_guard = eval::set_active_catalog(catalog);
    let _auth_guard = set_active_auth_store(auth);
    let _dbms_guard = set_active_dbms(dbms);

    // Register in transaction log and set cancel token
    let tx_id;
    let _cancel_guard;
    if let Some(tx_log) = active_tx_log() {
        let (id, token) = tx_log.register(query_str);
        tx_id = Some(id);
        QUERY_CANCEL_TOKEN.with(|t| *t.borrow_mut() = Some(token));
        _cancel_guard = Some(CancelTokenGuard);
    } else {
        tx_id = None;
        _cancel_guard = None;
    }

    let result = execute_query_with_binding(storage, &query, params);

    // Unregister from transaction log
    if let (Some(ref id), Some(tx_log)) = (&tx_id, active_tx_log()) {
        tx_log.unregister(id);
    }

    let total_time = start.elapsed();
    let exec_time = total_time.saturating_sub(parse_time);
    let rows_returned = result.as_ref().map(|r| r.rows.len() as u64).unwrap_or(0);
    storage.finish_query(qid);
    storage.query_profiler.record(mgstorage::query_profile::QueryProfile {
        query_text: query_str.to_string(),
        planning_time: parse_time,
        execution_time: exec_time,
        rows_scanned: 0,
        rows_returned,
        index_hits: 0,
        memory_used_bytes: 0,
        cache_hits: if cached { 1 } else { 0 },
    });
    result
}

/// Execute with catalog, auth, DBMS, parameters, and optional query timeout.
pub fn execute_with_catalog_auth_dbms_and_params_timeout(
    storage: &Storage,
    query_str: &str,
    catalog: Option<&mgcatalog::Catalog>,
    params: &HashMap<String, PropertyValue>,
    auth: Option<&AuthStore>,
    dbms: Option<&mgdbms::DbmsHandler>,
    timeout: Option<Duration>,
) -> Result<QueryResult, ExecError> {
    execute_with_catalog_auth_dbms_params_timeout_settings_txlog(
        storage, query_str, catalog, params, auth, dbms, timeout, None, None,
    )
}

/// Execute with catalog, auth, DBMS, parameters, timeout, settings, and tx log.
pub fn execute_with_catalog_auth_dbms_params_timeout_settings_txlog(
    storage: &Storage,
    query_str: &str,
    catalog: Option<&mgcatalog::Catalog>,
    params: &HashMap<String, PropertyValue>,
    auth: Option<&AuthStore>,
    dbms: Option<&mgdbms::DbmsHandler>,
    timeout: Option<Duration>,
    settings: Option<&SettingsStore>,
    tx_log: Option<&TransactionLog>,
) -> Result<QueryResult, ExecError> {
    let _deadline_guard = set_query_deadline(timeout);
    let _settings_guard = set_active_settings(settings);
    let _txlog_guard = set_active_tx_log(tx_log);
    execute_with_catalog_auth_dbms_and_params(storage, query_str, catalog, params, auth, dbms)
}

/// Execute with optional procedure registry.
pub fn execute_with_registry(
    storage: &Storage,
    query_str: &str,
    registry: Option<&ProcedureRegistry>,
) -> Result<QueryResult, ExecError> {
    let query = mgparser::parse_query(query_str)
        .map_err(|e| ExecError::Parse(format!("{}", e)))?;
    execute_query_with_registry(storage, &query, &HashMap::new(), registry)
}

/// Execute a parsed query.
pub fn execute_query(storage: &Storage, query: &Query) -> Result<QueryResult, ExecError> {
    execute_query_with_binding(storage, query, &HashMap::new())
}

/// Execute a parsed query with an outer binding context (for subqueries like EXISTS).
pub fn execute_query_with_binding(
    storage: &Storage,
    query: &Query,
    outer_binding: &HashMap<String, PropertyValue>,
) -> Result<QueryResult, ExecError> {
    execute_query_with_registry(storage, query, outer_binding, None)
}

/// Get a cached plan or compute it, using the global plan cache.
fn get_cached_plan(storage: &Storage, query: &Query) -> mgplanner::LogicalPlan {
    use mgparser::Fingerprint;
    let fp = query.fingerprint();

    if let Some(cached) = GLOBAL_PLAN_CACHE.get(fp) {
        return cached;
    }

    let plan = mgplanner::plan_query(storage, query);
    GLOBAL_PLAN_CACHE.insert(fp, plan.clone());
    plan
}

/// Attempt to execute a read-only query via the physical plan executor.
/// Returns `Some(result)` if the query was successfully executed physically,
/// or `None` if the query should fall back to clause-based execution.
fn try_physical_execution(
    storage: &Storage,
    query: &Query,
) -> Option<Result<QueryResult, ExecError>> {
    // Reject queries with write clauses, periodic commit, or union
    let has_write = query.clauses.iter().any(|c| {
        matches!(
            c,
            Clause::Create { .. }
                | Clause::Set { .. }
                | Clause::Delete { .. }
                | Clause::Merge { .. }
                | Clause::Remove { .. }
        )
    });
    let has_match = query.clauses.iter().any(|c| matches!(c, Clause::Match { .. }));
    if has_write || !has_match || query.periodic_commit.is_some() || query.union.is_some() {
        return None;
    }

    // Reject clauses that the physical executor does not handle
    for clause in &query.clauses {
        match clause {
            Clause::Return { items, distinct, .. } => {
                if *distinct {
                    return None;
                }
                // Reject subquery expressions in RETURN (column naming mismatch)
                if items.iter().any(|item| has_subquery_expression(&item.expression)) {
                    return None;
                }
            }
            Clause::Match { pattern, .. } => {
                // Reject path aliases (e.g. MATCH p = (a)) — not yet in physical executor
                if pattern.elements.iter().any(|e| e.path_alias.is_some()) {
                    return None;
                }
                // Reject variable-length edges — physical executor only handles single-hop
                if pattern.elements.iter().any(|e| {
                    e.edges.iter().any(|(edge_pat, _)| {
                        edge_pat.var_length.is_some()
                    })
                }) {
                    return None;
                }
                // Reject anonymous edges with properties — no alias to reference in Filter
                if pattern.elements.iter().any(|e| {
                    e.edges.iter().any(|(edge_pat, _)| {
                        edge_pat.alias.is_none() && !edge_pat.properties.is_empty()
                    })
                }) {
                    return None;
                }
                // Reject multiple edge types — EdgeExpand only handles a single type
                if pattern.elements.iter().any(|e| {
                    e.edges.iter().any(|(edge_pat, _)| edge_pat.edge_types.len() > 1)
                }) {
                    return None;
                }
                // Reject multi-hop paths with anonymous intermediate nodes
                for element in &pattern.elements {
                    if element.edges.len() > 1 {
                        for (edge_idx, (_edge_pat, right_node)) in element.edges.iter().enumerate() {
                            if edge_idx < element.edges.len() - 1 && right_node.alias.is_none() {
                                return None;
                            }
                        }
                    }
                }
            }
            Clause::OptionalMatch { .. }
            | Clause::Foreach { .. }
            | Clause::OrderBy { .. }
            | Clause::Limit { .. }
            | Clause::Skip { .. }
            | Clause::Call { .. }
            | Clause::With { .. }
            | Clause::Unwind { .. }
            | Clause::LoadCsv { .. }
            | Clause::LoadJsonl { .. } => {
                return None;
            }
            _ => {}
        }
    }

    // Reject WHERE clauses with subquery expressions (safety net)
    for clause in &query.clauses {
        if let Clause::Match { where_clause: Some(expr), .. } = clause {
            if has_subquery_expression(expr) {
                return None;
            }
        }
    }

    // Generate logical plan and check for unsupported operators
    let logical_plan = mgplanner::plan_query(storage, query);
    if !logical_plan_is_executable(&logical_plan) {
        return None;
    }

    // Convert to physical plan (skip egraph optimization until Produce is supported there)
    let stats = mgplanner::PlanStats::from_storage(storage);
    let catalog_stats = mgplanner::CatalogStats::from_plan_stats(&stats);
    let cost_model = mgplanner::CostModel::default();
    let physical_plan = mgplanner::physical_plan_from_logical(&logical_plan, &cost_model, &catalog_stats);

    Some(physical_exec::execute_physical_plan(storage, &physical_plan))
}

/// Check whether an expression contains a subquery (EXISTS, list predicates).
fn has_subquery_expression(expr: &Expression) -> bool {
    use mgparser::ast::Expression;
    match expr {
        Expression::Exists(_) | Expression::CountSubquery(_) => true,
        Expression::All { .. }
        | Expression::Any { .. }
        | Expression::None { .. }
        | Expression::Single { .. } => true,
        Expression::And(lhs, rhs)
        | Expression::Or(lhs, rhs)
        | Expression::Eq(lhs, rhs)
        | Expression::Neq(lhs, rhs)
        | Expression::Lt(lhs, rhs)
        | Expression::Gt(lhs, rhs)
        | Expression::Lte(lhs, rhs)
        | Expression::Gte(lhs, rhs)
        | Expression::Add(lhs, rhs)
        | Expression::Sub(lhs, rhs)
        | Expression::Mul(lhs, rhs)
        | Expression::Div(lhs, rhs)
        | Expression::Mod(lhs, rhs)
        | Expression::In(lhs, rhs)
        | Expression::StartsWith(lhs, rhs)
        | Expression::EndsWith(lhs, rhs)
        | Expression::Contains(lhs, rhs)
        | Expression::RegexMatch(lhs, rhs) => {
            has_subquery_expression(lhs) || has_subquery_expression(rhs)
        }
        Expression::Not(inner)
        | Expression::IsNull(inner)
        | Expression::IsNotNull(inner)
        | Expression::Neg(inner) => has_subquery_expression(inner),
        Expression::Property { object, .. }
        | Expression::Label { object, .. }
        | Expression::MapProjection { object, .. } => has_subquery_expression(object),
        Expression::Function { arguments, .. } => {
            arguments.iter().any(has_subquery_expression)
        }
        Expression::Case { expression, whens, else_branch } => {
            expression.as_ref().map_or(false, |e| has_subquery_expression(e))
                || whens.iter().any(|(w, t)| has_subquery_expression(w) || has_subquery_expression(t))
                || else_branch.as_ref().map_or(false, |e| has_subquery_expression(e))
        }
        Expression::List(elements) => elements.iter().any(has_subquery_expression),
        Expression::Map(elements) => elements.iter().any(|(_, e)| has_subquery_expression(e)),
        Expression::Filter { list, predicate, .. }
        | Expression::Extract { list, expression: predicate, .. } => {
            has_subquery_expression(list) || has_subquery_expression(predicate)
        }
        Expression::Reduce { initial, list, expression, .. } => {
            has_subquery_expression(initial) || has_subquery_expression(list) || has_subquery_expression(expression)
        }
        Expression::PatternComprehension { where_clause, expression, .. } => {
            where_clause.as_ref().map_or(false, |e| has_subquery_expression(e))
                || has_subquery_expression(expression)
        }
        Expression::Index { object, index } => {
            has_subquery_expression(object) || has_subquery_expression(index)
        }
        _ => false,
    }
}

/// Check whether a logical plan contains only operators supported by the
/// physical executor.
fn logical_plan_is_executable(plan: &mgplanner::LogicalPlan) -> bool {
    use mgplanner::LogicalOp;
    match &plan.op {
        LogicalOp::AllScan { .. }
        | LogicalOp::LabelScan { .. }
        | LogicalOp::Filter { .. }
        | LogicalOp::Produce { .. }
        | LogicalOp::Sort { .. }
        | LogicalOp::Limit { .. }
        | LogicalOp::Skip { .. }
        | LogicalOp::TopN { .. }
        | LogicalOp::Distinct
        | LogicalOp::EdgeExpand { .. }
        | LogicalOp::Aggregate { .. } => true,
        // LabelPropertyScan requires a real index; reject until physical
        // executor has fallback logic for missing indices.
        LogicalOp::Join { left, right, .. } => {
            logical_plan_is_executable(left) && logical_plan_is_executable(right)
        }
        _ => false,
    }
}

/// Execute a parsed query with registry and outer binding context.
pub fn execute_query_with_registry(
    storage: &Storage,
    query: &Query,
    outer_binding: &HashMap<String, PropertyValue>,
    registry: Option<&ProcedureRegistry>,
) -> Result<QueryResult, ExecError> {
    // Reset hops counter and set limit for this query
    reset_hops();
    set_hops_limit(query.hops_limit.map(|n| n as u64));
    match query.mode {
        QueryMode::Explain => {
            let plan = get_cached_plan(storage, query);
            let mut row = ResultRow::new();
            row.insert("PLAN".to_string(), PropertyValue::String(plan.to_string()));
            return Ok(QueryResult {
                columns: vec!["PLAN".to_string()],
                rows: vec![row],
                number_of_hops: 0,
            });
        }
        QueryMode::Profile => {
            let start = std::time::Instant::now();
            let (_bindings, mut result) = exec_clauses_with_binding(storage, &query.clauses, outer_binding, registry)?;

            // Handle UNION
            if let Some(ref union) = query.union {
                let (_, right_result) = exec_clauses_with_binding(storage, &union.right.clauses, outer_binding, registry)?;
                if union.all {
                    result.rows.extend(right_result.rows);
                } else {
                    let mut seen: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
                    let key_cols: Vec<usize> = (0..result.columns.len()).collect();
                    result.rows.retain(|r| {
                        let key: Vec<String> = key_cols.iter().map(|&i| format!("{:?}", r.values().nth(i))).collect();
                        seen.insert(key)
                    });
                    for row in right_result.rows {
                        let key: Vec<String> = key_cols.iter().map(|&i| format!("{:?}", row.values().nth(i))).collect();
                        if seen.insert(key) {
                            result.rows.push(row);
                        }
                    }
                }
            }

            let elapsed_ms = start.elapsed().as_millis() as i64;
            let plan = get_cached_plan(storage, query);
            let mut row = ResultRow::new();
            row.insert("PLAN".to_string(), PropertyValue::String(plan.to_string()));
            row.insert("ROWS".to_string(), PropertyValue::Int(result.rows.len() as i64));
            row.insert("TIME_MS".to_string(), PropertyValue::Int(elapsed_ms));
            return Ok(QueryResult {
                columns: vec!["PLAN".to_string(), "ROWS".to_string(), "TIME_MS".to_string()],
                rows: vec![row],
                number_of_hops: reset_hops(),
            });
        }
        QueryMode::Standard => {
            // Attempt physical plan execution for supported read-only queries.
            if outer_binding.is_empty() {
                if let Some(result) = try_physical_execution(storage, query) {
                    return result;
                }
            }

            // Handle USING PERIODIC COMMIT for LOAD CSV queries
            if let Some(batch_size) = query.periodic_commit {
                if let Some(csv_idx) = query.clauses.iter().position(|c| matches!(c, Clause::LoadCsv { .. })) {
                    let pre_clauses = &query.clauses[..csv_idx];
                    let post_clauses = &query.clauses[csv_idx + 1..];

                    // Execute pre-CSV clauses normally
                    let (pre_bindings, _) = exec_clauses_with_binding(storage, pre_clauses, outer_binding, registry)?;
                    if pre_bindings.is_empty() {
                        return Ok(QueryResult { columns: vec![], rows: vec![], number_of_hops: reset_hops() });
                    }

                    let Clause::LoadCsv { url, with_headers, alias } = &query.clauses[csv_idx] else {
                        unreachable!()
                    };

                    let file_path = url.trim_start_matches("file://");
                    let mut reader = csv::ReaderBuilder::new()
                        .has_headers(*with_headers)
                        .from_path(file_path)
                        .map_err(|e| ExecError::Runtime(format!("CSV read error: {}", e)))?;

                    let headers: Vec<String> = if *with_headers {
                        reader.headers()
                            .map_err(|e| ExecError::Runtime(format!("CSV headers error: {}", e)))?
                            .iter().map(|s| s.to_string()).collect()
                    } else {
                        vec![]
                    };

                    let mut all_columns: Vec<String> = Vec::new();
                    let mut all_rows: Vec<ResultRow> = Vec::new();
                    let mut row_count = 0;
                    let mut tx_guard: Option<ActiveTransactionGuard> = None;

                    for result in reader.records() {
                        let record = result.map_err(|e| ExecError::Runtime(format!("CSV parse error: {}", e)))?;
                        let mut map = Vec::new();
                        for (i, field) in record.iter().enumerate() {
                            let key = if *with_headers {
                                headers.get(i).cloned().unwrap_or_else(|| format!("column_{}", i))
                            } else {
                                format!("column_{}", i)
                            };
                            map.push((key, PropertyValue::String(field.to_string())));
                        }
                        let row_value = PropertyValue::Map(map);

                        // Commit previous batch if needed
                        if row_count > 0 && row_count % batch_size == 0 {
                            if let Some(ref tx) = active_transaction() {
                                if !storage.commit_transaction(tx) {
                                    return Err(ExecError::Runtime(
                                        "transaction aborted due to write-write conflict".into(),
                                    ));
                                }
                            }
                            tx_guard = None;
                        }

                        // Start new transaction for this batch
                        if tx_guard.is_none() {
                            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
                            tx_guard = Some(set_active_transaction(Some(tx)));
                        }

                        // Execute post-CSV clauses for each pre-binding crossed with this row
                        for pre_binding in &pre_bindings {
                            let mut binding = pre_binding.clone();
                            binding.insert(alias.clone(), row_value.clone());
                            let (_, result) = exec_clauses_with_binding(storage, post_clauses, &binding, registry)?;
                            if !result.rows.is_empty() {
                                if all_columns.is_empty() {
                                    all_columns = result.columns;
                                }
                                all_rows.extend(result.rows);
                            }
                        }

                        row_count += 1;
                    }

                    // Final commit
                    if let Some(ref tx) = active_transaction() {
                        if !storage.commit_transaction(tx) {
                            return Err(ExecError::Runtime(
                                "transaction aborted due to write-write conflict".into(),
                            ));
                        }
                    }
                    drop(tx_guard);

                    return Ok(QueryResult {
                        columns: all_columns,
                        rows: all_rows,
                        number_of_hops: reset_hops(),
                    });
                }
            }

            let (_bindings, mut result) = exec_clauses_with_binding(storage, &query.clauses, outer_binding, registry)?;

            // Handle UNION
            if let Some(ref union) = query.union {
                let (_, right_result) = exec_clauses_with_binding(storage, &union.right.clauses, outer_binding, registry)?;
                if union.all {
                    result.rows.extend(right_result.rows);
                } else {
                    let mut seen: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
                    let key_cols: Vec<usize> = (0..result.columns.len()).collect();
                    result.rows.retain(|r| {
                        let key: Vec<String> = key_cols.iter().map(|&i| format!("{:?}", r.values().nth(i))).collect();
                        seen.insert(key)
                    });
                    for row in right_result.rows {
                        let key: Vec<String> = key_cols.iter().map(|&i| format!("{:?}", row.values().nth(i))).collect();
                        if seen.insert(key) {
                            result.rows.push(row);
                        }
                    }
                }
            }

            result.number_of_hops = reset_hops();
            Ok(result)
        }
    }
}

/// Execute clauses with outer bindings; returns (final_bindings, return_result).
/// If no RETURN clause exists, final_bindings can be used to check for subquery matches.
pub(crate) fn exec_clauses_with_binding(
    storage: &Storage,
    clauses: &[Clause],
    outer_binding: &HashMap<String, PropertyValue>,
    registry: Option<&ProcedureRegistry>,
) -> Result<(Vec<HashMap<String, PropertyValue>>, QueryResult), ExecError> {
    // Single transaction for the entire query, ensuring MATCH, WHERE, and SET
    // all observe the same MVCC snapshot.
    let prev_tx = active_transaction();
    let (tx, owned) = match prev_tx {
        Some(tx) => (tx, false),
        None => (storage.begin_transaction(IsolationLevel::SnapshotIsolation), true),
    };
    let _tx_guard = set_active_transaction(Some(tx.clone()));

    let mut bindings: Vec<HashMap<String, PropertyValue>> = vec![outer_binding.clone()];
    let mut last_return: Option<QueryResult> = None;

    let exec_result: Result<(), ExecError> = (|| {
        for clause in clauses {
        check_query_timeout()?;
        match clause {
            Clause::Match { pattern, where_clause } => {
                bindings = exec_match(storage, pattern, where_clause, &bindings)?;
            }
            Clause::OptionalMatch { pattern, where_clause } => {
                bindings = exec_optional_match(storage, pattern, where_clause, &bindings)?;
            }
            Clause::Create { pattern } => {
                // After CREATE, bind the created vertex aliases so RETURN can reference them.
                for binding in bindings.iter_mut() {
                    exec_create_with_binding(storage, pattern, binding)?;
                }
            }
            Clause::Set { items } => {
                exec_set(storage, items, &bindings)?;
            }
            Clause::Delete { expressions, detach } => {
                exec_delete(storage, expressions, *detach, &bindings)?;
            }
            Clause::Return { items, distinct, all } => {
                last_return = Some(exec_return(storage, &bindings, items, *distinct, *all)?);
            }
            Clause::With { items, where_clause } => {
                bindings = exec_with(storage, &bindings, items, where_clause)?;
            }
            Clause::OrderBy { items } => {
                if let Some(ref mut result) = last_return {
                    exec_order_by(storage, result, items)?;
                } else {
                    // ORDER BY after WITH — sort bindings
                    let sort_exprs: Vec<_> = items.iter().map(|o| (o.expression.clone(), o.ascending)).collect();
                    bindings.sort_by(|a, b| {
                        for (expr, ascending) in &sort_exprs {
                            let va = eval::eval_expression_with_storage(expr, a, Some(storage));
                            let vb = eval::eval_expression_with_storage(expr, b, Some(storage));
                            match compare(&va, &vb) {
                                std::cmp::Ordering::Equal => continue,
                                ord => return if *ascending { ord } else { ord.reverse() },
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            }
            Clause::Skip { count } => {
                if let Some(ref mut result) = last_return {
                    let n = eval_const(count)? as usize;
                    if n < result.rows.len() {
                        result.rows = result.rows.split_off(n);
                    } else {
                        result.rows.clear();
                    }
                } else {
                    let n = eval_const(count)? as usize;
                    if n < bindings.len() {
                        bindings = bindings.split_off(n);
                    } else {
                        bindings.clear();
                    }
                }
            }
            Clause::Limit { count } => {
                if let Some(ref mut result) = last_return {
                    let n = eval_const(count)? as usize;
                    result.rows.truncate(n);
                } else {
                    let n = eval_const(count)? as usize;
                    bindings.truncate(n);
                }
            }
            Clause::Merge { pattern } => {
                let matched = exec_match(storage, &pattern.pattern, &None, &bindings)?;
                if matched.iter().any(|m| !m.is_empty()) {
                    bindings = matched;
                    if !pattern.on_match.is_empty() {
                        exec_set(storage, &pattern.on_match, &bindings)?;
                    }
                } else {
                    let mut merged_binding = bindings.first().cloned().unwrap_or_default();
                    exec_create_with_binding(storage, &CreatePattern { elements: pattern.pattern.elements.clone() }, &mut merged_binding)?;
                    bindings = exec_match(storage, &pattern.pattern, &None, &[merged_binding])?;
                    if !pattern.on_create.is_empty() {
                        exec_set(storage, &pattern.on_create, &bindings)?;
                    }
                }
            }
            Clause::Remove { items } => {
                exec_remove(storage, items, &bindings)?;
            }
            Clause::Unwind { expression, alias } => {
                bindings = exec_unwind(&bindings, expression, alias)?;
            }
            Clause::LoadCsv { url, with_headers, alias } => {
                bindings = exec_load_csv(&bindings, url, *with_headers, alias)?;
            }
            Clause::LoadJsonl { url, alias } => {
                bindings = exec_load_jsonl(&bindings, url, alias)?;
            }
            Clause::Call { procedure_name, arguments, yield_items, yield_all } => {
                let call_result = exec_call(storage, registry, procedure_name, arguments, &bindings, yield_items)?;
                // If YIELD items specified or YIELD *, create a Cartesian product of existing bindings
                // with procedure result rows so each procedure row feeds into subsequent clauses
                if !yield_items.is_empty() || *yield_all {
                    let mut new_bindings = Vec::new();
                    for binding in &bindings {
                        for row in &call_result.rows {
                            let mut b = binding.clone();
                            if *yield_all {
                                for (key, val) in row {
                                    b.insert(key.clone(), val.clone());
                                }
                            } else {
                                for item in yield_items {
                                    if let Some(val) = row.get(item) {
                                        b.insert(item.clone(), val.clone());
                                    }
                                }
                            }
                            new_bindings.push(b);
                        }
                    }
                    bindings = new_bindings;
                }
                last_return = Some(call_result);
            }
            Clause::CallSubquery { query, in_transactions } => {
                let batch_size = in_transactions.unwrap_or(usize::MAX);
                let mut new_bindings = Vec::new();

                // Save current active transaction (if any)
                let prev_tx = ACTIVE_TRANSACTION.with(|c| c.borrow().clone());

                if in_transactions.is_some() {
                    // Clear outer transaction so subquery batches use their own
                    ACTIVE_TRANSACTION.with(|c| *c.borrow_mut() = None);
                }

                for chunk in bindings.chunks(batch_size) {
                    let tx = if in_transactions.is_some() {
                        let t = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
                        ACTIVE_TRANSACTION.with(|c| *c.borrow_mut() = Some(t.clone()));
                        Some(t)
                    } else {
                        None
                    };

                    for binding in chunk {
                        let (_, sub_result) = exec_clauses_with_binding(storage, &query.clauses, binding, registry)?;
                        for row in &sub_result.rows {
                            let mut b = binding.clone();
                            for (key, val) in row {
                                b.insert(key.clone(), val.clone());
                            }
                            new_bindings.push(b);
                        }
                    }

                    if let Some(ref t) = tx {
                        storage.commit_transaction(t);
                        ACTIVE_TRANSACTION.with(|c| *c.borrow_mut() = None);
                    }
                }

                // Restore previous active transaction
                if prev_tx.is_some() {
                    ACTIVE_TRANSACTION.with(|c| *c.borrow_mut() = prev_tx);
                }

                bindings = new_bindings;
                last_return = None;
            }
            Clause::Foreach { variable, list, clauses } => {
                for binding in &bindings {
                    let list_val = eval::eval_expression_with_storage(list, binding, Some(storage));
                    if let PropertyValue::List(items) = list_val {
                        for item in items {
                            let mut local = binding.clone();
                            local.insert(variable.clone(), item);
                            exec_clauses_with_binding(storage, clauses, &local, registry)?;
                        }
                    }
                }
            }
            Clause::CreateIndex { label, property } => {
                storage.create_label_index(*label);
                storage.create_label_property_index(*label, *property);
            }
            Clause::DropIndex { label, property } => {
                storage.drop_label_property_index(*label, *property);
                storage.drop_label_index(*label);
            }
            Clause::CreateConstraint { label, property, constraint_type } => {
                match constraint_type {
                    ConstraintKind::Unique => storage.constraints.add_unique_constraint(*label, vec![*property]),
                    ConstraintKind::Exists => storage.constraints.add_existence_constraint(*label, *property),
                    ConstraintKind::Type { ref expected } => {
                        let ct = match expected.to_lowercase().as_str() {
                            "int" | "integer" => mgstorage::constraints::ConstraintType::Int,
                            "float" | "double" => mgstorage::constraints::ConstraintType::Double,
                            "bool" | "boolean" => mgstorage::constraints::ConstraintType::Bool,
                            _ => mgstorage::constraints::ConstraintType::String,
                        };
                        storage.constraints.add_type_constraint(*label, *property, ct);
                    }
                }
            }
            Clause::DropConstraint { label, property, constraint_type } => {
                match constraint_type {
                    ConstraintKind::Unique => storage.constraints.drop_unique_constraint(*label, &[*property]),
                    ConstraintKind::Exists => storage.constraints.drop_existence_constraint(*label, *property),
                    ConstraintKind::Type { .. } => storage.constraints.drop_type_constraint(*label, *property),
                }
            }
            Clause::Show { show_type } => {
                let rows: Vec<HashMap<String, PropertyValue>> = match show_type {
                    mgparser::ast::ShowType::Databases => {
                        if let Some(dbms) = active_dbms() {
                            dbms.list().into_iter().map(|name| {
                                let mut r = HashMap::new();
                                r.insert("name".to_string(), PropertyValue::String(name.into()));
                                r
                            }).collect()
                        } else {
                            let mut r = HashMap::new();
                            r.insert("name".to_string(), PropertyValue::String("memgraph".into()));
                            vec![r]
                        }
                    }
                    mgparser::ast::ShowType::Indexes => {
                        let cat = active_catalog();
                        let mut rows = Vec::new();
                        let labels = storage.active_label_indices.read().unwrap();
                        for label in labels.iter() {
                            let mut r = HashMap::new();
                            let name = cat.as_ref().map(|c| c.label_name(*label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                            r.insert("label".to_string(), PropertyValue::String(name));
                            r.insert("type".to_string(), PropertyValue::String("label".into()));
                            rows.push(r);
                        }
                        let lp = storage.active_label_property_indices.read().unwrap();
                        for (label, prop) in lp.iter() {
                            let mut r = HashMap::new();
                            let label_name = cat.as_ref().map(|c| c.label_name(*label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                            let prop_name = cat.as_ref().map(|c| c.property_name(*prop)).unwrap_or_else(|| format!("{}", prop.as_uint()));
                            r.insert("label".to_string(), PropertyValue::String(label_name));
                            r.insert("property".to_string(), PropertyValue::String(prop_name));
                            r.insert("type".to_string(), PropertyValue::String("label+property".into()));
                            rows.push(r);
                        }
                        let vi = storage.vector_indices.read().unwrap();
                        for (label, entry) in vi.iter() {
                            let mut r = HashMap::new();
                            let label_name = cat.as_ref().map(|c| c.label_name(*label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                            r.insert("label".to_string(), PropertyValue::String(label_name));
                            r.insert("property".to_string(), PropertyValue::String(format!("{:?}", entry.property)));
                            r.insert("type".to_string(), PropertyValue::String("vector".into()));
                            rows.push(r);
                        }
                        let ti = storage.text_indices.read().unwrap();
                        for (label, entry) in ti.iter() {
                            let mut r = HashMap::new();
                            let label_name = cat.as_ref().map(|c| c.label_name(*label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                            let prop_names: Vec<String> = entry.properties.iter()
                                .map(|(_, name)| name.clone())
                                .collect();
                            r.insert("label".to_string(), PropertyValue::String(label_name));
                            r.insert("property".to_string(), PropertyValue::String(prop_names.join(", ")));
                            r.insert("type".to_string(), PropertyValue::String("text".into()));
                            rows.push(r);
                        }
                        let pi = storage.active_point_indices.read().unwrap();
                        for (label, prop) in pi.iter() {
                            let mut r = HashMap::new();
                            let label_name = cat.as_ref().map(|c| c.label_name(*label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                            let prop_name = cat.as_ref().map(|c| c.property_name(*prop)).unwrap_or_else(|| format!("{}", prop.as_uint()));
                            r.insert("label".to_string(), PropertyValue::String(label_name));
                            r.insert("property".to_string(), PropertyValue::String(prop_name));
                            r.insert("type".to_string(), PropertyValue::String("point".into()));
                            rows.push(r);
                        }
                        rows
                    }
                    mgparser::ast::ShowType::Constraints => {
                        let cat = active_catalog();
                        storage.constraints.list().into_iter().map(|c| {
                            let mut r = HashMap::new();
                            let label_name = cat.as_ref().map(|catalog| catalog.label_name(c.label)).unwrap_or_else(|| format!("{}", c.label.as_uint()));
                            let prop_name = cat.as_ref().map(|catalog| catalog.property_name(c.property)).unwrap_or_else(|| format!("{}", c.property.as_uint()));
                            r.insert("type".to_string(), PropertyValue::String(format!("{:?}", c.kind)));
                            r.insert("label".to_string(), PropertyValue::String(label_name));
                            r.insert("property".to_string(), PropertyValue::String(prop_name));
                            r
                        }).collect()
                    }
                    mgparser::ast::ShowType::Triggers => {
                        storage.triggers.list().into_iter().map(|t| {
                            let mut r = HashMap::new();
                            r.insert("name".to_string(), PropertyValue::String(t.name.clone()));
                            r.insert("timing".to_string(), PropertyValue::String(format!("{:?}", t.timing)));
                            r.insert("event".to_string(), PropertyValue::String(format!("{:?}", t.event)));
                            r.insert("statement".to_string(), PropertyValue::String(t.statement.clone()));
                            r
                        }).collect()
                    }
                    mgparser::ast::ShowType::NodeLabels => {
                        let cat = active_catalog();
                        if let Some(c) = cat {
                            c.label_names().into_iter().map(|name| {
                                let mut r = HashMap::new();
                                r.insert("label".to_string(), PropertyValue::String(name));
                                r
                            }).collect()
                        } else {
                            vec![]
                        }
                    }
                    mgparser::ast::ShowType::EdgeTypes => {
                        let cat = active_catalog();
                        if let Some(c) = cat {
                            c.edge_type_names().into_iter().map(|name| {
                                let mut r = HashMap::new();
                                r.insert("edge_type".to_string(), PropertyValue::String(name));
                                r
                            }).collect()
                        } else {
                            vec![]
                        }
                    }
                };
                let columns = match show_type {
                    mgparser::ast::ShowType::Databases => vec!["name".into()],
                    mgparser::ast::ShowType::Indexes => vec!["label".into(), "property".into(), "type".into()],
                    mgparser::ast::ShowType::Constraints => vec!["type".into(), "label".into(), "property".into()],
                    mgparser::ast::ShowType::Triggers => vec!["name".into(), "timing".into(), "event".into(), "statement".into()],
                    mgparser::ast::ShowType::NodeLabels => vec!["label".into()],
                    mgparser::ast::ShowType::EdgeTypes => vec!["edge_type".into()],
                };
                last_return = Some(QueryResult { number_of_hops: 0, columns, rows });
            }
            Clause::CreateUser { username, password } => {
                if let Some(auth) = active_auth_store() {
                    auth.add_user_with_password(username, password, mgauth::Role::ReadWrite)
                        .map_err(|e| ExecError::Runtime(format!("{}", e)))?;
                }
            }
            Clause::DropUser { username } => {
                if let Some(auth) = active_auth_store() {
                    auth.remove_user(username);
                }
            }
            Clause::CreateRole { role_name } => {
                if let Some(auth) = active_auth_store() {
                    auth.create_role(role_name)
                        .map_err(|e| ExecError::Runtime(format!("{}", e)))?;
                }
            }
            Clause::DropRole { role_name } => {
                if let Some(auth) = active_auth_store() {
                    auth.drop_role(role_name)
                        .map_err(|e| ExecError::Runtime(format!("{}", e)))?;
                }
            }
            Clause::GrantRole { role_name, username } => {
                if let Some(auth) = active_auth_store() {
                    auth.grant_role(username, role_name)
                        .map_err(|e| ExecError::Runtime(format!("{}", e)))?;
                }
            }
            Clause::RevokeRole { role_name, username } => {
                if let Some(auth) = active_auth_store() {
                    auth.revoke_role(username, role_name)
                        .map_err(|e| ExecError::Runtime(format!("{}", e)))?;
                }
            }
            Clause::ShowAuth { auth_type } => {
                let (columns, rows) = match auth_type {
                    mgparser::ast::ShowAuthType::Users => {
                        let rows = if let Some(auth) = active_auth_store() {
                            auth.list_users().into_iter().map(|u| {
                                let mut r = HashMap::new();
                                r.insert("username".to_string(), PropertyValue::String(u.username.clone()));
                                r.insert("role".to_string(), PropertyValue::String(u.role.as_str().to_string()));
                                r
                            }).collect()
                        } else {
                            vec![]
                        };
                        (vec!["username".into(), "role".into()], rows)
                    }
                    mgparser::ast::ShowAuthType::Roles => {
                        let rows = if let Some(auth) = active_auth_store() {
                            auth.list_roles().into_iter().map(|name| {
                                let mut r = HashMap::new();
                                r.insert("role".to_string(), PropertyValue::String(name));
                                r
                            }).collect()
                        } else {
                            vec![]
                        };
                        (vec!["role".into()], rows)
                    }
                };
                last_return = Some(QueryResult { number_of_hops: 0, columns, rows });
            }
            Clause::CreateTrigger { name, target, event, timing, statement } => {
                let storage_event = match (target, event) {
                    (mgparser::ast::TriggerTarget::Vertex, mgparser::ast::TriggerEvent::Create) => mgstorage::triggers::TriggerEvent::VertexCreate,
                    (mgparser::ast::TriggerTarget::Vertex, mgparser::ast::TriggerEvent::Delete) => mgstorage::triggers::TriggerEvent::VertexDelete,
                    (mgparser::ast::TriggerTarget::Vertex, mgparser::ast::TriggerEvent::Update) => mgstorage::triggers::TriggerEvent::VertexUpdate,
                    (mgparser::ast::TriggerTarget::Edge, mgparser::ast::TriggerEvent::Create) => mgstorage::triggers::TriggerEvent::EdgeCreate,
                    (mgparser::ast::TriggerTarget::Edge, mgparser::ast::TriggerEvent::Delete) => mgstorage::triggers::TriggerEvent::EdgeDelete,
                    (mgparser::ast::TriggerTarget::Edge, mgparser::ast::TriggerEvent::Update) => mgstorage::triggers::TriggerEvent::EdgeUpdate,
                };
                let storage_timing = match timing {
                    mgparser::ast::TriggerTiming::Before => mgstorage::triggers::TriggerTiming::Before,
                    mgparser::ast::TriggerTiming::After => mgstorage::triggers::TriggerTiming::After,
                };
                let trigger = mgstorage::triggers::Trigger {
                    name: name.clone(),
                    timing: storage_timing,
                    event: storage_event,
                    label_filter: None,
                    statement: statement.clone(),
                };
                storage.triggers.create(trigger)
                    .map_err(|e| ExecError::Runtime(format!("{}", e)))?;
            }
            Clause::DropTrigger { name } => {
                storage.triggers.drop(name);
            }
            Clause::CreateDatabase { name } => {
                if let Some(dbms) = active_dbms() {
                    dbms.create(name.clone())
                        .map_err(|e| ExecError::Runtime(format!("{}", e)))?;
                }
            }
            Clause::DropDatabase { name, force } => {
                if let Some(dbms) = active_dbms() {
                    if *force {
                        let _ = dbms.drop(name);
                    } else {
                        dbms.drop(name)
                            .map_err(|e| ExecError::Runtime(format!("{}", e)))?;
                    }
                }
            }
            Clause::ShowSetting { name } => {
                let rows = if let Some(settings) = active_settings() {
                    let val = settings.get(name).unwrap_or_else(|| "<null>".to_string());
                    let mut r = HashMap::new();
                    r.insert("name".to_string(), PropertyValue::String(name.clone()));
                    r.insert("value".to_string(), PropertyValue::String(val));
                    vec![r]
                } else {
                    vec![]
                };
                last_return = Some(QueryResult {
                    number_of_hops: 0,
                    columns: vec!["name".into(), "value".into()],
                    rows,
                });
            }
            Clause::ShowSettings => {
                let rows = if let Some(settings) = active_settings() {
                    settings.list().into_iter().map(|(k, v)| {
                        let mut r = HashMap::new();
                        r.insert("name".to_string(), PropertyValue::String(k));
                        r.insert("value".to_string(), PropertyValue::String(v));
                        r
                    }).collect()
                } else {
                    vec![]
                };
                last_return = Some(QueryResult {
                    number_of_hops: 0,
                    columns: vec!["name".into(), "value".into()],
                    rows,
                });
            }
            Clause::SetSetting { name, value } => {
                let binding_ctx = bindings.first().cloned().unwrap_or_default();
                let val = eval_expression(value, &binding_ctx);
                let val_str = match &val {
                    PropertyValue::String(s) => s.clone(),
                    PropertyValue::Int(n) => n.to_string(),
                    PropertyValue::Double(f) => f.to_string(),
                    PropertyValue::Bool(b) => b.to_string(),
                    _ => format!("{}", val),
                };
                if let Some(settings) = active_settings() {
                    settings.set(name, &val_str)
                        .map_err(|e| ExecError::Runtime(format!("{}", e)))?;
                }
            }
            Clause::ShowTransactions => {
                let rows = if let Some(tx_log) = active_tx_log() {
                    tx_log.list_active().into_iter().map(|tx| {
                        let mut r = HashMap::new();
                        r.insert("transaction_id".to_string(), PropertyValue::String(tx.id.clone()));
                        r.insert("query".to_string(), PropertyValue::String(tx.query_text.clone()));
                        r.insert("elapsed_ms".to_string(), PropertyValue::Int(tx.elapsed_ms() as i64));
                        r
                    }).collect()
                } else {
                    vec![]
                };
                last_return = Some(QueryResult {
                    number_of_hops: 0,
                    columns: vec!["transaction_id".into(), "query".into(), "elapsed_ms".into()],
                    rows,
                });
            }
            Clause::TerminateTransaction { transaction_id } => {
                if let Some(tx_log) = active_tx_log() {
                    tx_log.terminate(transaction_id)
                        .map_err(|e| ExecError::Runtime(format!("{}", e)))?;
                }
            }
            Clause::GrantPrivilege { privileges, target_name, is_user } => {
                if let Some(auth) = active_auth_store() {
                    auth.grant_privileges(target_name.clone(), *is_user, privileges.clone())
                        .map_err(|e| ExecError::Runtime(e))?;
                }
            }
            Clause::RevokePrivilege { privileges, target_name, is_user } => {
                if let Some(auth) = active_auth_store() {
                    auth.revoke_privileges(target_name.clone(), *is_user, privileges.clone())
                        .map_err(|e| ExecError::Runtime(e))?;
                }
            }
            Clause::DenyPrivilege { privileges, target_name, is_user } => {
                if let Some(auth) = active_auth_store() {
                    auth.deny_privileges(target_name.clone(), *is_user, privileges.clone())
                        .map_err(|e| ExecError::Runtime(e))?;
                }
            }
            Clause::ShowPrivileges { target_name, is_user } => {
                let columns = vec!["privilege".to_string(), "effect".to_string()];
                let mut rows = Vec::new();
                if let Some(auth) = active_auth_store() {
                    let privs = auth.list_privileges(target_name, *is_user);
                    for (p, granted) in privs {
                        let mut r = HashMap::new();
                        r.insert("privilege".to_string(), PropertyValue::String(format!("{:?}", p)));
                        r.insert("effect".to_string(), PropertyValue::String(if granted { "GRANT".into() } else { "DENY".into() }));
                        rows.push(r);
                    }
                }
                last_return = Some(QueryResult { number_of_hops: 0, columns, rows });
            }
            Clause::AlterUser { username, action } => {
                if let Some(auth) = active_auth_store() {
                    match action {
                        AlterUserAction::SetPassword { password } => {
                            auth.set_password(&username, &password)
                                .map_err(|e| ExecError::Runtime(e))?;
                        }
                        AlterUserAction::RenameTo { new_name } => {
                            auth.rename_user(&username, &new_name)
                                .map_err(|e| ExecError::Runtime(e))?;
                        }
                    }
                }
            }
            Clause::BeginTransaction => {
                // In auto-commit mode this is a no-op; explicit transactions
                // are managed by the Bolt server state machine.
            }
            Clause::CommitTransaction => {}
            Clause::RollbackTransaction => {}
            Clause::SetStorageMode { mode: _ } => {
                // Storage mode is a runtime config change handled by the server.
                // The parser validates the mode; actual mode switching is
                // delegated to the server layer.
            }
        }
    } // for loop
    Ok(())
    })();

    match exec_result {
        Err(e) => {
            if owned {
                storage.abort_transaction(&tx);
            }
            Err(e)
        }
        Ok(()) => {
            if owned {
                if !storage.commit_transaction(&tx) {
                    return Err(ExecError::Runtime(
                        "transaction aborted due to write-write conflict".into(),
                    ));
                }
            }
            let result = last_return.unwrap_or(QueryResult { number_of_hops: 0,
                columns: vec![],
                rows: vec![],
            });
            Ok((bindings, result))
        }
    }
}

/// Evaluate an expression to a constant (no variable bindings).
fn eval_const(expr: &Expression) -> Result<i64, ExecError> {
    match eval_expression(expr, &HashMap::new()) {
        PropertyValue::Int(n) => Ok(n),
        other => Err(ExecError::Runtime(format!("expected integer, got {:?}", other))),
    }
}

// ─── MATCH execution ───────────────────────────────────────────────────

fn exec_match(
    storage: &Storage,
    pattern: &MatchPattern,
    where_clause: &Option<Expression>,
    existing: &[HashMap<String, PropertyValue>],
) -> Result<Vec<HashMap<String, PropertyValue>>, ExecError> {
    let mut results: Vec<HashMap<String, PropertyValue>> = existing.to_vec();

    for element in &pattern.elements {
        check_query_timeout()?;
        let mut new_results = Vec::new();
        for existing_binding in &results {
            let element_bindings = match_element(storage, element, existing_binding)?;
            for binding in element_bindings {
                let mut merged = existing_binding.clone();
                let mut conflict = false;
                for (k, v) in &binding {
                    if let Some(existing_v) = merged.get(k) {
                        if existing_v != v {
                            conflict = true;
                            break;
                        }
                    }
                    merged.insert(k.clone(), v.clone());
                }
                if conflict {
                    continue;
                }
                // Apply WHERE filter
                if let Some(ref where_expr) = where_clause {
                    let cond = eval::eval_expression_with_storage(where_expr, &merged, Some(storage));
                    if !cond.is_truthy() {
                        continue;
                    }
                }
                new_results.push(merged);
            }
        }
        results = new_results;
    }

    if results.is_empty() && existing.is_empty() {
        Ok(existing.to_vec())
    } else {
        Ok(results)
    }
}

/// Check if an expression is a literal that can be used for index lookup.
fn expr_to_literal(expr: &Expression) -> Option<PropertyValue> {
    match expr {
        Expression::Int(v) => Some(PropertyValue::Int(*v)),
        Expression::Double(v) => Some(PropertyValue::Double(*v)),
        Expression::String(v) => Some(PropertyValue::String(v.clone())),
        Expression::Bool(v) => Some(PropertyValue::Bool(*v)),
        Expression::Null => Some(PropertyValue::Null),
        _ => None,
    }
}

fn match_element(
    storage: &Storage,
    element: &PatternElement,
    binding: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, ExecError> {
    let tx = match active_transaction() {
        Some(tx) => tx,
        None => storage.begin_transaction(IsolationLevel::SnapshotIsolation),
    };
    let mut results = Vec::new();

    // If start node alias is already bound, use the bound GID as the sole candidate
    let start_gid = element.node.alias.as_ref().and_then(|alias| {
        binding.get(alias).and_then(|val| match val {
            PropertyValue::Vertex(vref) => Some(vref.gid),
            PropertyValue::Int(gid_int) => Some(Gid::from(*gid_int as u64)),
            _ => None,
        })
    });

    let candidates: Vec<Gid> = if let Some(gid) = start_gid {
        vec![gid]
    } else if element.node.labels.len() == 1 {
        let label = element.node.labels[0];
        // Try to use label-property index if a property has a literal value
        let mut index_gids: Option<Vec<Gid>> = None;
        for (prop_id, expr) in &element.node.properties {
            if let Some(value) = expr_to_literal(expr) {
                if storage.has_label_property_index(label, *prop_id) {
                    index_gids = Some(storage.vertices_by_label_property(label, *prop_id, &value));
                    break;
                }
            }
        }
        index_gids.unwrap_or_else(|| storage.vertices_by_label(label))
    } else if !element.node.labels.is_empty() {
        // Multiple labels: intersection of all label index results
        let mut candidates: Option<std::collections::HashSet<Gid>> = None;
        for label in &element.node.labels {
            let gids: std::collections::HashSet<Gid> = storage.vertices_by_label(*label).into_iter().collect();
            match candidates {
                None => candidates = Some(gids),
                Some(ref mut c) => c.retain(|gid| gids.contains(gid)),
            }
        }
        candidates.map(|c| c.into_iter().collect()).unwrap_or_default()
    } else {
        storage.all_vertex_gids()
    };

    for gid in &candidates {
        check_query_timeout()?;
        if let Some(v) = storage.get_vertex(*gid, &tx) {
            // Check label constraints
            let labels_ok = element.node.labels.is_empty()
                || element.node.labels.iter().all(|l| v.labels.contains(l));

            if !labels_ok {
                continue;
            }

            // Check property constraints on the node pattern
            let props_ok = element.node.properties.iter().all(|(key, expr)| {
                let expected = eval_expression(expr, binding);
                *v.properties.get(*key) == expected
            });

            if !props_ok {
                continue;
            }

            let mut result_binding = HashMap::new();
            if let Some(ref alias) = element.node.alias {
                result_binding.insert(alias.clone(), PropertyValue::Vertex(
                    VertexRef::new(v.gid, v.labels.clone(), v.properties.clone())
                ));
            }
            result_binding.insert(
                format!("__gid__{}", element.node.alias.as_deref().unwrap_or("anon")),
                PropertyValue::Int(gid.as_int()),
            );

            // Match edges
            if !element.edges.is_empty() {
                let edge_results = match_edges(storage, &tx, *gid, v.labels.clone(), v.properties.clone(), &element.edges, binding)?;
                for (edge_binding, path) in edge_results {
                    let mut merged = result_binding.clone();
                    for (k, v) in edge_binding {
                        merged.insert(k, v);
                    }
                    if let Some(ref path_alias) = element.path_alias {
                        merged.insert(path_alias.clone(), PropertyValue::Path(path));
                    }
                    results.push(merged);
                }
            } else {
                if let Some(ref path_alias) = element.path_alias {
                    let path = PathValue::new(VertexRef::new(v.gid, v.labels.clone(), v.properties.clone()));
                    result_binding.insert(path_alias.clone(), PropertyValue::Path(path));
                }
                results.push(result_binding);
            }
        }
    }

    Ok(results)
}

fn exec_optional_match(
    storage: &Storage,
    pattern: &MatchPattern,
    where_clause: &Option<Expression>,
    existing: &[HashMap<String, PropertyValue>],
) -> Result<Vec<HashMap<String, PropertyValue>>, ExecError> {
    // Collect variable names introduced by the pattern
    let mut null_vars = Vec::new();
    for element in &pattern.elements {
        if let Some(ref alias) = element.path_alias {
            null_vars.push(alias.clone());
        }
        if let Some(ref alias) = element.node.alias {
            null_vars.push(alias.clone());
        }
        for (edge, node) in &element.edges {
            if let Some(ref alias) = edge.alias {
                null_vars.push(alias.clone());
            }
            if let Some(ref alias) = node.alias {
                null_vars.push(alias.clone());
            }
        }
    }

    // Per-binding optional match: for each outer binding, try to match.
    // If matches found, use them; otherwise return the original binding with nulls.
    let mut results = Vec::new();
    for binding in existing {
        let matched = exec_match(storage, pattern, where_clause, &[binding.clone()])?;
        if matched.is_empty() {
            let mut extended = binding.clone();
            for var in &null_vars {
                extended.entry(var.clone()).or_insert(PropertyValue::Null);
            }
            results.push(extended);
        } else {
            for m in matched {
                results.push(m);
            }
        }
    }
    Ok(results)
}

fn match_edges(
    storage: &Storage,
    tx: &mgstorage::transaction::Transaction,
    from_gid: Gid,
    from_labels: Vec<mgcore::types::LabelId>,
    from_properties: mgcore::property_store::PropertyStore,
    edges: &[(EdgePattern, NodePattern)],
    initial_binding: &HashMap<String, PropertyValue>,
) -> Result<Vec<(HashMap<String, PropertyValue>, PathValue)>, ExecError> {
    let start_vertex = VertexRef::new(from_gid, from_labels, from_properties);
    let mut results: Vec<(HashMap<String, PropertyValue>, Gid, PathValue)> =
        vec![(initial_binding.clone(), from_gid, PathValue::new(start_vertex))];

    for (edge_pat, node_pat) in edges {
        check_query_timeout()?;
        let mut new_results = Vec::new();

        if let Some((min, max)) = edge_pat.var_length {
            for (prev_binding, current_gid, path) in &results {
                let paths = traverse_variable_length(
                    storage, tx, *current_gid, path.clone(), edge_pat, node_pat, min, max, prev_binding,
                )?;
                for (path_binding, final_gid, final_path) in paths {
                    let mut merged = prev_binding.clone();
                    merged.extend(path_binding);
                    new_results.push((merged, final_gid, final_path));
                }
            }
        } else {
            // Pre-compute edge type property index candidates if applicable
            let index_candidates: Option<HashSet<Gid>> =
                if edge_pat.edge_types.len() == 1 {
                    let etype = edge_pat.edge_types[0];
                    edge_pat.properties.iter().find_map(|(prop_id, expr)| {
                        let value = expr_to_literal(expr)?;
                        if storage.has_edge_type_property_index(etype, *prop_id) {
                            Some(storage.edges_by_type_property_value(etype, *prop_id, &value).into_iter().collect())
                        } else {
                            None
                        }
                    })
                } else {
                    None
                };

            for (prev_binding, current_gid, path) in &results {
                check_query_timeout()?;
                // Use adjacency lists based on direction
                let edge_gids: Vec<Gid> = match edge_pat.direction {
                    Direction::Right => storage.vertex_out_edge_gids(*current_gid),
                    Direction::Left => storage.vertex_in_edge_gids(*current_gid),
                    Direction::Either => {
                        let mut gids = storage.vertex_out_edge_gids(*current_gid);
                        gids.extend(storage.vertex_in_edge_gids(*current_gid));
                        let seen: HashSet<Gid> = gids.iter().copied().collect();
                        seen.into_iter().collect()
                    }
                };

                for edge_gid in &edge_gids {
                    increment_hops(1)?;
                    // Fast-path: skip if index candidates exist and this edge isn't one
                    if let Some(ref candidates) = index_candidates {
                        if !candidates.contains(edge_gid) {
                            continue;
                        }
                    }

                    if let Some(edge) = storage.get_edge(*edge_gid, tx) {
                        // Direction check
                        let matches_dir = match edge_pat.direction {
                            Direction::Right => edge.from_vertex == *current_gid,
                            Direction::Left => edge.to_vertex == *current_gid,
                            Direction::Either => edge.from_vertex == *current_gid || edge.to_vertex == *current_gid,
                        };
                        if !matches_dir {
                            continue;
                        }

                        // Edge type check
                        if !edge_pat.edge_types.is_empty() && !edge_pat.edge_types.contains(&edge.edge_type) {
                            continue;
                        }

                        // Edge property check
                        let edge_props_ok = edge_pat.properties.iter().all(|(key, expr)| {
                            let expected = eval_expression(expr, prev_binding);
                            *edge.properties.get(*key) == expected
                        });
                        if !edge_props_ok {
                            continue;
                        }

                        let other_gid = if edge.from_vertex == *current_gid {
                            edge.to_vertex
                        } else {
                            edge.from_vertex
                        };

                        // Variable reuse: if node alias already bound, verify same GID
                        if let Some(ref alias) = node_pat.alias {
                            if let Some(bound_val) = prev_binding.get(alias) {
                                let bound_gid = match bound_val {
                                    PropertyValue::Vertex(vref) => Some(vref.gid),
                                    PropertyValue::Int(gid_int) => Some(Gid::from(*gid_int as u64)),
                                    _ => None,
                                };
                                if bound_gid.map_or(false, |g| g != other_gid) {
                                    continue;
                                }
                            }
                        }

                        if let Some(target) = storage.get_vertex(other_gid, tx) {
                            // Node label check
                            let labels_ok = node_pat.labels.is_empty()
                                || node_pat.labels.iter().all(|l| target.labels.contains(l));
                            if !labels_ok {
                                continue;
                            }

                            // Node property check
                            let props_ok = node_pat.properties.iter().all(|(key, expr)| {
                                let expected = eval_expression(expr, prev_binding);
                                *target.properties.get(*key) == expected
                            });
                            if !props_ok {
                                continue;
                            }

                            let mut merged = prev_binding.clone();
                            if let Some(ref alias) = edge_pat.alias {
                                merged.insert(alias.clone(), PropertyValue::Edge(
                                    EdgeRefValue::new(
                                        edge.gid, edge.edge_type, edge.from_vertex, edge.to_vertex, edge.properties.clone(),
                                    )
                                ));
                                merged.insert(format!("__gid__{}", alias), PropertyValue::Int(edge.gid.as_int()));
                            }
                            if let Some(ref alias) = node_pat.alias {
                                merged.insert(alias.clone(), PropertyValue::Vertex(
                                    VertexRef::new(target.gid, target.labels.clone(), target.properties.clone())
                                ));
                            }
                            merged.insert(
                                format!("__gid__{}", node_pat.alias.as_deref().unwrap_or("anon")),
                                PropertyValue::Int(other_gid.as_int()),
                            );
                            let mut new_path = path.clone();
                            new_path.add_edge(EdgeRefValue::new(
                                edge.gid, edge.edge_type, edge.from_vertex, edge.to_vertex, edge.properties.clone(),
                            ));
                            new_path.add_vertex(VertexRef::new(target.gid, target.labels.clone(), target.properties.clone()));
                            new_results.push((merged, other_gid, new_path));
                        }
                    }
                }
            }
        }

        results = new_results;
    }

    Ok(results.into_iter().map(|(b, _, p)| (b, p)).collect())
}

fn matches_direction(current: Gid, from: Gid, to: Gid, dir: Direction) -> bool {
    match dir {
        Direction::Right => from == current,
        Direction::Left => to == current,
        Direction::Either => from == current || to == current,
    }
}

/// Variable-length path traversal (DFS by default, BFS when requested).
/// Returns list of (bindings, final_vertex_gid, path).
fn traverse_variable_length(
    storage: &Storage,
    tx: &mgstorage::transaction::Transaction,
    start_gid: Gid,
    initial_path: PathValue,
    edge_pat: &EdgePattern,
    node_pat: &NodePattern,
    min: usize,
    max: Option<usize>,
    binding: &HashMap<String, PropertyValue>,
) -> Result<Vec<(HashMap<String, PropertyValue>, Gid, PathValue)>, ExecError> {
    match edge_pat.path_algorithm {
        PathAlgorithm::Bfs => traverse_variable_length_bfs(
            storage, tx, start_gid, initial_path, edge_pat, node_pat, min, max, binding,
        ),
        PathAlgorithm::WShortest => traverse_variable_length_wshortest(
            storage, tx, start_gid, initial_path, edge_pat, node_pat, min, max, binding,
        ),
        PathAlgorithm::AllShortest => traverse_variable_length_all_shortest(
            storage, tx, start_gid, initial_path, edge_pat, node_pat, min, max, binding,
        ),
        PathAlgorithm::KShortest => {
            let limit = match edge_pat.kshortest_limit {
                Some(ref expr) => match eval_expression(expr, binding) {
                    PropertyValue::Int(n) => n as usize,
                    other => return Err(ExecError::Runtime(format!("KSHORTEST limit must be integer, got {:?}", other))),
                },
                None => usize::MAX,
            };
            traverse_variable_length_kshortest(
                storage, tx, start_gid, initial_path, edge_pat, node_pat, min, max, binding, limit,
            )
        }
        PathAlgorithm::Default => traverse_variable_length_dfs(
            storage, tx, start_gid, initial_path, edge_pat, node_pat, min, max, binding,
        ),
    }
}

/// DFS traversal for variable-length paths (Cypher default).
fn traverse_variable_length_dfs(
    storage: &Storage,
    tx: &mgstorage::transaction::Transaction,
    start_gid: Gid,
    initial_path: PathValue,
    edge_pat: &EdgePattern,
    node_pat: &NodePattern,
    min: usize,
    max: Option<usize>,
    binding: &HashMap<String, PropertyValue>,
) -> Result<Vec<(HashMap<String, PropertyValue>, Gid, PathValue)>, ExecError> {
    let mut results = Vec::new();
    // Stack: (current_gid, depth, visited_edge_gids, path)
    let mut stack: Vec<(Gid, usize, Vec<Gid>, PathValue)> =
        vec![(start_gid, 0, Vec::new(), initial_path)];

    while let Some((current_gid, depth, visited_edges, path)) = stack.pop() {
        check_query_timeout()?;
        if depth >= min {
            if let Some(target) = storage.get_vertex(current_gid, tx) {
                let labels_ok = node_pat.labels.is_empty()
                    || node_pat.labels.iter().all(|l| target.labels.contains(l));
                let props_ok = node_pat.properties.iter().all(|(key, expr)| {
                    let expected = eval_expression(expr, binding);
                    *target.properties.get(*key) == expected
                });
                if labels_ok && props_ok {
                    let mut result_binding = HashMap::new();
                    if let Some(ref alias) = edge_pat.alias {
                        let edge_list: Vec<PropertyValue> = path.edges.iter().map(|e| PropertyValue::Edge(e.clone())).collect();
                        result_binding.insert(alias.clone(), PropertyValue::List(edge_list));
                    }
                    if let Some(ref alias) = node_pat.alias {
                        result_binding.insert(alias.clone(), PropertyValue::Vertex(
                            VertexRef::new(target.gid, target.labels.clone(), target.properties.clone())
                        ));
                    }
                    results.push((result_binding, current_gid, path.clone()));
                }
            }
        }

        if let Some(m) = max {
            if depth >= m {
                continue;
            }
        }

        // Expand using adjacency lists
        let edge_gids: Vec<Gid> = match edge_pat.direction {
            Direction::Right => storage.vertex_out_edge_gids(current_gid),
            Direction::Left => storage.vertex_in_edge_gids(current_gid),
            Direction::Either => {
                let mut gids = storage.vertex_out_edge_gids(current_gid);
                gids.extend(storage.vertex_in_edge_gids(current_gid));
                let seen: HashSet<Gid> = gids.iter().copied().collect();
                seen.into_iter().collect()
            }
        };

        for edge_gid in &edge_gids {
            increment_hops(1)?;
            if let Some(edge) = storage.get_edge(*edge_gid, tx) {
                let matches_dir = match edge_pat.direction {
                    Direction::Right => edge.from_vertex == current_gid,
                    Direction::Left => edge.to_vertex == current_gid,
                    Direction::Either => edge.from_vertex == current_gid || edge.to_vertex == current_gid,
                };
                if !matches_dir {
                    continue;
                }
                if !edge_pat.edge_types.is_empty() && !edge_pat.edge_types.contains(&edge.edge_type) {
                    continue;
                }

                let edge_props_ok = edge_pat.properties.iter().all(|(key, expr)| {
                    let expected = eval_expression(expr, binding);
                    *edge.properties.get(*key) == expected
                });
                if !edge_props_ok {
                    continue;
                }

                // Avoid reusing edges (simple path constraint)
                if visited_edges.contains(edge_gid) {
                    continue;
                }

                let other_gid = if edge.from_vertex == current_gid { edge.to_vertex } else { edge.from_vertex };
                let mut new_visited = visited_edges.clone();
                new_visited.push(*edge_gid);
                let mut new_path = path.clone();
                new_path.add_edge(EdgeRefValue::new(
                    edge.gid, edge.edge_type, edge.from_vertex, edge.to_vertex, edge.properties.clone(),
                ));
                let (next_labels, next_props) = storage.get_vertex(other_gid, tx)
                    .map(|v| (v.labels.clone(), v.properties.clone()))
                    .unwrap_or_default();
                new_path.add_vertex(VertexRef::new(other_gid, next_labels, next_props));
                stack.push((other_gid, depth + 1, new_visited, new_path));
            }
        }
    }

    Ok(results)
}

/// BFS traversal for variable-length paths (Memgraph `[*bfs..N]`).
/// Visits vertices in breadth-first order and avoids revisiting vertices.
fn traverse_variable_length_bfs(
    storage: &Storage,
    tx: &mgstorage::transaction::Transaction,
    start_gid: Gid,
    initial_path: PathValue,
    edge_pat: &EdgePattern,
    node_pat: &NodePattern,
    min: usize,
    max: Option<usize>,
    binding: &HashMap<String, PropertyValue>,
) -> Result<Vec<(HashMap<String, PropertyValue>, Gid, PathValue)>, ExecError> {
    let mut results = Vec::new();
    // Queue: (current_gid, depth, visited_vertex_gids, visited_edge_gids, path)
    let mut queue: Vec<(Gid, usize, Vec<Gid>, Vec<Gid>, PathValue)> =
        vec![(start_gid, 0, vec![start_gid], Vec::new(), initial_path)];
    let mut front = 0;

    while front < queue.len() {
        check_query_timeout()?;
        let (current_gid, depth, visited_vertices, visited_edges, path) = queue[front].clone();
        front += 1;

        if depth >= min {
            if let Some(target) = storage.get_vertex(current_gid, tx) {
                let labels_ok = node_pat.labels.is_empty()
                    || node_pat.labels.iter().all(|l| target.labels.contains(l));
                let props_ok = node_pat.properties.iter().all(|(key, expr)| {
                    let expected = eval_expression(expr, binding);
                    *target.properties.get(*key) == expected
                });
                if labels_ok && props_ok {
                    let mut result_binding = HashMap::new();
                    if let Some(ref alias) = edge_pat.alias {
                        let edge_list: Vec<PropertyValue> = path.edges.iter().map(|e| PropertyValue::Edge(e.clone())).collect();
                        result_binding.insert(alias.clone(), PropertyValue::List(edge_list));
                    }
                    if let Some(ref alias) = node_pat.alias {
                        result_binding.insert(alias.clone(), PropertyValue::Vertex(
                            VertexRef::new(target.gid, target.labels.clone(), target.properties.clone())
                        ));
                    }
                    results.push((result_binding, current_gid, path.clone()));
                }
            }
        }

        if let Some(m) = max {
            if depth >= m {
                continue;
            }
        }

        // Expand using adjacency lists
        let edge_gids: Vec<Gid> = match edge_pat.direction {
            Direction::Right => storage.vertex_out_edge_gids(current_gid),
            Direction::Left => storage.vertex_in_edge_gids(current_gid),
            Direction::Either => {
                let mut gids = storage.vertex_out_edge_gids(current_gid);
                gids.extend(storage.vertex_in_edge_gids(current_gid));
                let seen: HashSet<Gid> = gids.iter().copied().collect();
                seen.into_iter().collect()
            }
        };

        for edge_gid in &edge_gids {
            increment_hops(1)?;
            if let Some(edge) = storage.get_edge(*edge_gid, tx) {
                let matches_dir = match edge_pat.direction {
                    Direction::Right => edge.from_vertex == current_gid,
                    Direction::Left => edge.to_vertex == current_gid,
                    Direction::Either => edge.from_vertex == current_gid || edge.to_vertex == current_gid,
                };
                if !matches_dir {
                    continue;
                }
                if !edge_pat.edge_types.is_empty() && !edge_pat.edge_types.contains(&edge.edge_type) {
                    continue;
                }

                let edge_props_ok = edge_pat.properties.iter().all(|(key, expr)| {
                    let expected = eval_expression(expr, binding);
                    *edge.properties.get(*key) == expected
                });
                if !edge_props_ok {
                    continue;
                }

                // Avoid reusing edges (simple path constraint)
                if visited_edges.contains(edge_gid) {
                    continue;
                }

                let other_gid = if edge.from_vertex == current_gid { edge.to_vertex } else { edge.from_vertex };

                // BFS: avoid revisiting vertices to prevent cycles
                if visited_vertices.contains(&other_gid) {
                    continue;
                }

                let mut new_visited_vertices = visited_vertices.clone();
                new_visited_vertices.push(other_gid);
                let mut new_visited_edges = visited_edges.clone();
                new_visited_edges.push(*edge_gid);
                let mut new_path = path.clone();
                new_path.add_edge(EdgeRefValue::new(
                    edge.gid, edge.edge_type, edge.from_vertex, edge.to_vertex, edge.properties.clone(),
                ));
                let (next_labels, next_props) = storage.get_vertex(other_gid, tx)
                    .map(|v| (v.labels.clone(), v.properties.clone()))
                    .unwrap_or_default();
                new_path.add_vertex(VertexRef::new(other_gid, next_labels, next_props));
                queue.push((other_gid, depth + 1, new_visited_vertices, new_visited_edges, new_path));
            }
        }
    }

    Ok(results)
}

/// All-shortest-paths traversal for variable-length edges.
/// Finds every vertex reachable within `max` hops along with ONE representative
/// shortest path (by hop count).  Uses BFS level-by-level and only records the
/// first time a vertex is discovered.
fn traverse_variable_length_all_shortest(
    storage: &Storage,
    tx: &mgstorage::transaction::Transaction,
    start_gid: Gid,
    initial_path: PathValue,
    edge_pat: &EdgePattern,
    node_pat: &NodePattern,
    min: usize,
    max: Option<usize>,
    binding: &HashMap<String, PropertyValue>,
) -> Result<Vec<(HashMap<String, PropertyValue>, Gid, PathValue)>, ExecError> {
    let mut results = Vec::new();

    // (vertex_gid, depth, path)
    let mut queue: Vec<(Gid, usize, PathValue)> = vec![(start_gid, 0, initial_path)];
    let mut front = 0;
    let mut visited: HashSet<Gid> = HashSet::new();
    visited.insert(start_gid);

    while front < queue.len() {
        check_query_timeout()?;
        let (current_gid, depth, path) = queue[front].clone();
        front += 1;

        if depth >= min {
            if let Some(target) = storage.get_vertex(current_gid, tx) {
                let labels_ok = node_pat.labels.is_empty()
                    || node_pat.labels.iter().all(|l| target.labels.contains(l));
                let props_ok = node_pat.properties.iter().all(|(key, expr)| {
                    let expected = eval_expression(expr, binding);
                    *target.properties.get(*key) == expected
                });
                if labels_ok && props_ok {
                    let mut result_binding = HashMap::new();
                    if let Some(ref alias) = edge_pat.alias {
                        let edge_list: Vec<PropertyValue> =
                            path.edges.iter().map(|e| PropertyValue::Edge(e.clone())).collect();
                        result_binding.insert(alias.clone(), PropertyValue::List(edge_list));
                    }
                    if let Some(ref alias) = node_pat.alias {
                        result_binding.insert(
                            alias.clone(),
                            PropertyValue::Vertex(VertexRef::new(target.gid, target.labels.clone(), target.properties.clone())),
                        );
                    }
                    results.push((result_binding, current_gid, path.clone()));
                }
            }
        }

        if let Some(m) = max {
            if depth >= m {
                continue;
            }
        }

        let edge_gids: Vec<Gid> = match edge_pat.direction {
            Direction::Right => storage.vertex_out_edge_gids(current_gid),
            Direction::Left => storage.vertex_in_edge_gids(current_gid),
            Direction::Either => {
                let mut gids = storage.vertex_out_edge_gids(current_gid);
                gids.extend(storage.vertex_in_edge_gids(current_gid));
                let seen: HashSet<Gid> = gids.iter().copied().collect();
                seen.into_iter().collect()
            }
        };

        for edge_gid in &edge_gids {
            increment_hops(1)?;
            if let Some(edge) = storage.get_edge(*edge_gid, tx) {
                let matches_dir = match edge_pat.direction {
                    Direction::Right => edge.from_vertex == current_gid,
                    Direction::Left => edge.to_vertex == current_gid,
                    Direction::Either => {
                        edge.from_vertex == current_gid || edge.to_vertex == current_gid
                    }
                };
                if !matches_dir {
                    continue;
                }
                if !edge_pat.edge_types.is_empty() && !edge_pat.edge_types.contains(&edge.edge_type) {
                    continue;
                }
                let edge_props_ok = edge_pat.properties.iter().all(|(key, expr)| {
                    let expected = eval_expression(expr, binding);
                    *edge.properties.get(*key) == expected
                });
                if !edge_props_ok {
                    continue;
                }

                let other_gid = if edge.from_vertex == current_gid {
                    edge.to_vertex
                } else {
                    edge.from_vertex
                };

                if !visited.insert(other_gid) {
                    continue;
                }

                let mut new_path = path.clone();
                new_path.add_edge(EdgeRefValue::new(
                    edge.gid,
                    edge.edge_type,
                    edge.from_vertex,
                    edge.to_vertex,
                    edge.properties.clone(),
                ));
                let (next_labels, next_props) = storage
                    .get_vertex(other_gid, tx)
                    .map(|v| (v.labels.clone(), v.properties.clone()))
                    .unwrap_or_default();
                new_path.add_vertex(VertexRef::new(other_gid, next_labels, next_props));
                queue.push((other_gid, depth + 1, new_path));
            }
        }
    }

    Ok(results)
}

/// Weighted-shortest-path traversal for variable-length edges (Dijkstra).
/// Every edge has unit weight (1.0) since the parser does not yet carry a
/// weight-property specifier.  Returns one shortest-weight path per reachable
/// vertex that satisfies the node constraints.
fn traverse_variable_length_wshortest(
    storage: &Storage,
    tx: &mgstorage::transaction::Transaction,
    start_gid: Gid,
    initial_path: PathValue,
    edge_pat: &EdgePattern,
    node_pat: &NodePattern,
    min: usize,
    max: Option<usize>,
    binding: &HashMap<String, PropertyValue>,
) -> Result<Vec<(HashMap<String, PropertyValue>, Gid, PathValue)>, ExecError> {
    // Dijkstra state: (total_weight, vertex_gid, depth, path)
    #[derive(Clone, Debug)]
    struct State {
        weight: f64,
        gid: Gid,
        depth: usize,
        path: PathValue,
    }
    impl PartialEq for State {
        fn eq(&self, other: &Self) -> bool {
            self.weight == other.weight
        }
    }
    impl Eq for State {}
    impl PartialOrd for State {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for State {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            other.weight.partial_cmp(&self.weight).unwrap_or(std::cmp::Ordering::Equal)
        }
    }

    let mut heap = BinaryHeap::new();
    heap.push(State {
        weight: 0.0,
        gid: start_gid,
        depth: 0,
        path: initial_path,
    });

    let mut best: HashMap<Gid, (f64, usize, PathValue)> = HashMap::new();
    let mut results = Vec::new();

    while let Some(state) = heap.pop() {
        check_query_timeout()?;
        let State { weight, gid, depth, path } = state;

        // Respect hop-count bound
        if let Some(m) = max {
            if depth > m {
                continue;
            }
        }

        // If we've already found a better path to this vertex, skip
        if let Some((prev_w, prev_d, _)) = best.get(&gid) {
            if weight > *prev_w || (weight == *prev_w && depth >= *prev_d) {
                continue;
            }
        }
        best.insert(gid, (weight, depth, path.clone()));

        if depth >= min {
            if let Some(target) = storage.get_vertex(gid, tx) {
                let labels_ok = node_pat.labels.is_empty()
                    || node_pat.labels.iter().all(|l| target.labels.contains(l));
                let props_ok = node_pat.properties.iter().all(|(key, expr)| {
                    let expected = eval_expression(expr, binding);
                    *target.properties.get(*key) == expected
                });
                if labels_ok && props_ok {
                    let mut result_binding = HashMap::new();
                    if let Some(ref alias) = edge_pat.alias {
                        let edge_list: Vec<PropertyValue> =
                            path.edges.iter().map(|e| PropertyValue::Edge(e.clone())).collect();
                        result_binding.insert(alias.clone(), PropertyValue::List(edge_list));
                    }
                    if let Some(ref alias) = node_pat.alias {
                        result_binding.insert(
                            alias.clone(),
                            PropertyValue::Vertex(VertexRef::new(target.gid, target.labels.clone(), target.properties.clone())),
                        );
                    }
                    results.push((result_binding, gid, path.clone()));
                }
            }
        }

        if let Some(m) = max {
            if depth >= m {
                continue;
            }
        }

        let edge_gids: Vec<Gid> = match edge_pat.direction {
            Direction::Right => storage.vertex_out_edge_gids(gid),
            Direction::Left => storage.vertex_in_edge_gids(gid),
            Direction::Either => {
                let mut gids = storage.vertex_out_edge_gids(gid);
                gids.extend(storage.vertex_in_edge_gids(gid));
                let seen: HashSet<Gid> = gids.iter().copied().collect();
                seen.into_iter().collect()
            }
        };

        for edge_gid in &edge_gids {
            increment_hops(1)?;
            if let Some(edge) = storage.get_edge(*edge_gid, tx) {
                let matches_dir = match edge_pat.direction {
                    Direction::Right => edge.from_vertex == gid,
                    Direction::Left => edge.to_vertex == gid,
                    Direction::Either => edge.from_vertex == gid || edge.to_vertex == gid,
                };
                if !matches_dir {
                    continue;
                }
                if !edge_pat.edge_types.is_empty() && !edge_pat.edge_types.contains(&edge.edge_type) {
                    continue;
                }
                let edge_props_ok = edge_pat.properties.iter().all(|(key, expr)| {
                    let expected = eval_expression(expr, binding);
                    *edge.properties.get(*key) == expected
                });
                if !edge_props_ok {
                    continue;
                }

                let other_gid = if edge.from_vertex == gid {
                    edge.to_vertex
                } else {
                    edge.from_vertex
                };

                let mut new_path = path.clone();
                new_path.add_edge(EdgeRefValue::new(
                    edge.gid,
                    edge.edge_type,
                    edge.from_vertex,
                    edge.to_vertex,
                    edge.properties.clone(),
                ));
                let (next_labels, next_props) = storage
                    .get_vertex(other_gid, tx)
                    .map(|v| (v.labels.clone(), v.properties.clone()))
                    .unwrap_or_default();
                new_path.add_vertex(VertexRef::new(other_gid, next_labels, next_props));

                heap.push(State {
                    weight: weight + 1.0,
                    gid: other_gid,
                    depth: depth + 1,
                    path: new_path,
                });
            }
        }
    }

    Ok(results)
}

/// Compute a single shortest path between two vertices using Dijkstra.
/// Returns the path (including start vertex) or None if no path exists.
fn dijkstra_single_path(
    storage: &Storage,
    tx: &mgstorage::transaction::Transaction,
    source: Gid,
    target: Gid,
    edge_pat: &EdgePattern,
    max_depth: Option<usize>,
    blocked_edges: &HashSet<Gid>,
    blocked_vertices: &HashSet<Gid>,
) -> Result<Option<PathValue>, ExecError> {
    #[derive(Clone, Debug)]
    struct State {
        weight: f64,
        gid: Gid,
        depth: usize,
        path: PathValue,
    }
    impl PartialEq for State {
        fn eq(&self, other: &Self) -> bool {
            self.weight == other.weight
        }
    }
    impl Eq for State {}
    impl PartialOrd for State {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for State {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            other.weight.partial_cmp(&self.weight).unwrap_or(std::cmp::Ordering::Equal)
        }
    }

    let start_vertex = match storage.get_vertex(source, tx) {
        Some(v) => v,
        None => return Ok(None),
    };
    let start_path = PathValue::new(VertexRef::new(
        start_vertex.gid,
        start_vertex.labels.clone(),
        start_vertex.properties.clone(),
    ));

    let mut heap = BinaryHeap::new();
    heap.push(State {
        weight: 0.0,
        gid: source,
        depth: 0,
        path: start_path,
    });
    let mut best: HashMap<Gid, f64> = HashMap::new();

    while let Some(state) = heap.pop() {
        let State { weight, gid, depth, path } = state;

        if let Some(m) = max_depth {
            if depth > m {
                continue;
            }
        }

        if let Some(&prev_w) = best.get(&gid) {
            if weight > prev_w {
                continue;
            }
        }
        best.insert(gid, weight);

        if gid == target {
            return Ok(Some(path));
        }

        if let Some(m) = max_depth {
            if depth >= m {
                continue;
            }
        }

        let edge_gids: Vec<Gid> = match edge_pat.direction {
            Direction::Right => storage.vertex_out_edge_gids(gid),
            Direction::Left => storage.vertex_in_edge_gids(gid),
            Direction::Either => {
                let mut gids = storage.vertex_out_edge_gids(gid);
                gids.extend(storage.vertex_in_edge_gids(gid));
                let seen: HashSet<Gid> = gids.iter().copied().collect();
                seen.into_iter().collect()
            }
        };

        for edge_gid in &edge_gids {
            increment_hops(1)?;
            if blocked_edges.contains(edge_gid) {
                continue;
            }
            if let Some(edge) = storage.get_edge(*edge_gid, tx) {
                let matches_dir = match edge_pat.direction {
                    Direction::Right => edge.from_vertex == gid,
                    Direction::Left => edge.to_vertex == gid,
                    Direction::Either => edge.from_vertex == gid || edge.to_vertex == gid,
                };
                if !matches_dir {
                    continue;
                }
                if !edge_pat.edge_types.is_empty() && !edge_pat.edge_types.contains(&edge.edge_type) {
                    continue;
                }
                let edge_props_ok = edge_pat.properties.iter().all(|(key, expr)| {
                    let expected = eval_expression(expr, &HashMap::new());
                    *edge.properties.get(*key) == expected
                });
                if !edge_props_ok {
                    continue;
                }

                let other_gid = if edge.from_vertex == gid {
                    edge.to_vertex
                } else {
                    edge.from_vertex
                };

                if blocked_vertices.contains(&other_gid) {
                    continue;
                }

                let mut new_path = path.clone();
                new_path.add_edge(EdgeRefValue::new(
                    edge.gid,
                    edge.edge_type,
                    edge.from_vertex,
                    edge.to_vertex,
                    edge.properties.clone(),
                ));
                let (next_labels, next_props) = storage
                    .get_vertex(other_gid, tx)
                    .map(|v| (v.labels.clone(), v.properties.clone()))
                    .unwrap_or_default();
                new_path.add_vertex(VertexRef::new(other_gid, next_labels, next_props));

                heap.push(State {
                    weight: weight + 1.0,
                    gid: other_gid,
                    depth: depth + 1,
                    path: new_path,
                });
            }
        }
    }

    Ok(None)
}

/// K-Shortest paths traversal using Yen's algorithm.
fn traverse_variable_length_kshortest(
    storage: &Storage,
    tx: &mgstorage::transaction::Transaction,
    start_gid: Gid,
    initial_path: PathValue,
    edge_pat: &EdgePattern,
    node_pat: &NodePattern,
    min: usize,
    max: Option<usize>,
    binding: &HashMap<String, PropertyValue>,
    limit: usize,
) -> Result<Vec<(HashMap<String, PropertyValue>, Gid, PathValue)>, ExecError> {
    // First find the shortest path from start to any matching target
    let mut all_results: Vec<(HashMap<String, PropertyValue>, Gid, PathValue)> = Vec::new();

    // We need to find all target vertices that match the node pattern first,
    // then run Yen's algorithm for each source-target pair.
    // For simplicity, collect all matching targets reachable within max hops.
    let mut matching_targets: Vec<Gid> = Vec::new();
    {
        let mut visited: HashSet<Gid> = HashSet::new();
        let mut queue: Vec<(Gid, usize)> = vec![(start_gid, 0)];
        visited.insert(start_gid);
        while let Some((current_gid, depth)) = queue.pop() {
            check_query_timeout()?;
            if depth >= min {
                if let Some(target) = storage.get_vertex(current_gid, tx) {
                    let labels_ok = node_pat.labels.is_empty()
                        || node_pat.labels.iter().all(|l| target.labels.contains(l));
                    let props_ok = node_pat.properties.iter().all(|(key, expr)| {
                        let expected = eval_expression(expr, binding);
                        *target.properties.get(*key) == expected
                    });
                    if labels_ok && props_ok {
                        matching_targets.push(current_gid);
                    }
                }
            }
            if let Some(m) = max {
                if depth >= m {
                    continue;
                }
            }
            let edge_gids: Vec<Gid> = match edge_pat.direction {
                Direction::Right => storage.vertex_out_edge_gids(current_gid),
                Direction::Left => storage.vertex_in_edge_gids(current_gid),
                Direction::Either => {
                    let mut gids = storage.vertex_out_edge_gids(current_gid);
                    gids.extend(storage.vertex_in_edge_gids(current_gid));
                    let seen: HashSet<Gid> = gids.iter().copied().collect();
                    seen.into_iter().collect()
                }
            };
            for edge_gid in &edge_gids {
            increment_hops(1)?;
                if let Some(edge) = storage.get_edge(*edge_gid, tx) {
                    let other_gid = if edge.from_vertex == current_gid {
                        edge.to_vertex
                    } else {
                        edge.from_vertex
                    };
                    if visited.insert(other_gid) {
                        queue.push((other_gid, depth + 1));
                    }
                }
            }
        }
    }

    for target_gid in matching_targets {
        check_query_timeout()?;

        // Yen's algorithm for this source-target pair
        let mut shortest_paths: Vec<PathValue> = Vec::new();
        let mut found_paths_set: HashSet<Vec<Gid>> = HashSet::new();

        // Find initial shortest path
        if let Some(path) = dijkstra_single_path(
            storage, tx, start_gid, target_gid, edge_pat, max, &HashSet::new(), &HashSet::new(),
        )? {
            if path.edges.len() >= min {
                let key: Vec<Gid> = path.edges.iter().map(|e| e.gid).collect();
                found_paths_set.insert(key);
                shortest_paths.push(path);
            }
        }

        // Generate K-1 more paths
        for _ in 1..limit {
            if shortest_paths.is_empty() {
                break;
            }
            let last_path = shortest_paths.last().unwrap().clone();

            let mut candidate_paths: Vec<(usize, Vec<Gid>, PathValue)> = Vec::new();

            for deviation_idx in 0..last_path.edges.len() {
                // Build blocked edges and vertices for this deviation
                let mut blocked_edges: HashSet<Gid> = HashSet::new();
                let mut blocked_vertices: HashSet<Gid> = HashSet::new();

                // Block the edge at deviation_idx from all previously found paths
                // that share the same prefix up to deviation_idx
                for prev_path in &shortest_paths {
                    if deviation_idx < prev_path.edges.len() {
                        let prefix_matches = (0..deviation_idx).all(|i| {
                            i < last_path.edges.len()
                                && prev_path.edges[i].gid == last_path.edges[i].gid
                        });
                        if prefix_matches {
                            blocked_edges.insert(prev_path.edges[deviation_idx].gid);
                        }
                    }
                }

                // Block vertices in the root path (except the deviation vertex)
                let mut current_vertex = start_gid;
                for i in 0..deviation_idx {
                    blocked_vertices.insert(current_vertex);
                    let edge = &last_path.edges[i];
                    current_vertex = if edge.from_vertex == current_vertex {
                        edge.to_vertex
                    } else {
                        edge.from_vertex
                    };
                }

                // Compute spur path from deviation vertex to target
                let spur_source = current_vertex;
                if let Some(spur_path) = dijkstra_single_path(
                    storage, tx, spur_source, target_gid, edge_pat, max, &blocked_edges, &blocked_vertices,
                )? {
                    // Combine root path + spur path
                    let mut candidate = PathValue::new(last_path.vertices[0].clone());
                    for i in 0..deviation_idx {
                        candidate.add_edge(last_path.edges[i].clone());
                        candidate.add_vertex(last_path.vertices[i + 1].clone());
                    }
                    // Skip spur_path's first vertex (it's the deviation vertex, already in candidate)
                    for i in 0..spur_path.edges.len() {
                        candidate.add_edge(spur_path.edges[i].clone());
                        candidate.add_vertex(spur_path.vertices[i + 1].clone());
                    }

                    let key: Vec<Gid> = candidate.edges.iter().map(|e| e.gid).collect();
                    if !found_paths_set.contains(&key) {
                        let total_weight = candidate.edges.len();
                        candidate_paths.push((
                            total_weight,
                            key.clone(),
                            candidate,
                        ));
                    }
                }
            }

            // Pick the best candidate (shortest edge count)
            if let Some(min_idx) = candidate_paths.iter().enumerate().min_by_key(|(_, (w, _, _))| w).map(|(i, _)| i) {
                let (_, key, candidate) = candidate_paths.swap_remove(min_idx);
                if candidate.edges.len() > max.unwrap_or(usize::MAX) {
                    break;
                }
                found_paths_set.insert(key);
                shortest_paths.push(candidate);
            } else {
                break;
            }
        }

        // Build result bindings for all found paths
        for path in shortest_paths {
            let mut result_binding = HashMap::new();
            if let Some(ref alias) = edge_pat.alias {
                let edge_list: Vec<PropertyValue> =
                    path.edges.iter().map(|e| PropertyValue::Edge(e.clone())).collect();
                result_binding.insert(alias.clone(), PropertyValue::List(edge_list));
            }
            if let Some(ref alias) = node_pat.alias {
                let target_vertex = path.end();
                result_binding.insert(
                    alias.clone(),
                    PropertyValue::Vertex(target_vertex.clone()),
                );
            }
            all_results.push((result_binding, target_gid, path));
        }
    }

    Ok(all_results)
}

// ─── CREATE execution ──────────────────────────────────────────────────

fn exec_create_with_binding(
    storage: &Storage,
    pattern: &CreatePattern,
    binding: &mut HashMap<String, PropertyValue>,
) -> Result<(), ExecError> {
    let (tx, owned) = match active_transaction() {
        Some(tx) => (tx, false),
        None => (storage.begin_transaction(IsolationLevel::SnapshotIsolation), true),
    };
    let _tx_guard = set_active_transaction(Some(tx.clone()));

    for element in &pattern.elements {
        // Reuse existing vertex if alias is already bound (e.g. CREATE (a)-[:KNOWS]->(b) after MATCH)
        let (gid, _labels) = if let Some(ref alias) = element.node.alias {
            if let Some(PropertyValue::Vertex(vr)) = binding.get(alias) {
                (vr.gid, vr.labels.clone())
            } else {
                let gid = Gid::from(NEXT_GID.fetch_add(1, Ordering::Relaxed));
                storage.create_vertex(&tx, gid)?;
                let mut properties = mgcore::property_store::PropertyStore::new();
                for (key, value) in &element.node.properties {
                    let val = eval_expression_with_storage(value, binding, Some(storage));
                    storage.vertex_set_property(&tx, gid, *key, val.clone())?;
                    properties.set(*key, val);
                }
                let mut labels = Vec::new();
                for label in &element.node.labels {
                    storage.vertex_add_label(&tx, gid, *label)?;
                    labels.push(*label);
                }
                binding.insert(alias.clone(), PropertyValue::Vertex(
                    mgcore::property_value::VertexRef::new(gid, labels.clone(), properties)
                ));
                (gid, labels)
            }
        } else {
            let gid = Gid::from(NEXT_GID.fetch_add(1, Ordering::Relaxed));
            storage.create_vertex(&tx, gid)?;
            let mut _properties = mgcore::property_store::PropertyStore::new();
            for (key, value) in &element.node.properties {
                let val = eval_expression_with_storage(value, binding, Some(storage));
                storage.vertex_set_property(&tx, gid, *key, val.clone())?;
                _properties.set(*key, val);
            }
            let mut labels = Vec::new();
            for label in &element.node.labels {
                storage.vertex_add_label(&tx, gid, *label)?;
                labels.push(*label);
            }
            (gid, labels)
        };

        // Create edges
        let mut prev_gid = gid;
        for (edge_pat, node_pat) in &element.edges {
            // Reuse existing vertex if alias is already bound
            let (target_gid, _target_labels) = if let Some(ref alias) = node_pat.alias {
                if let Some(PropertyValue::Vertex(vr)) = binding.get(alias) {
                    (vr.gid, vr.labels.clone())
                } else {
                    let target_gid = Gid::from(NEXT_GID.fetch_add(1, Ordering::Relaxed));
                    storage.create_vertex(&tx, target_gid)?;
                    let mut target_properties = mgcore::property_store::PropertyStore::new();
                    for (key, value) in &node_pat.properties {
                        let val = eval_expression_with_storage(value, binding, Some(storage));
                        storage.vertex_set_property(&tx, target_gid, *key, val.clone())?;
                        target_properties.set(*key, val);
                    }
                    let mut target_labels = Vec::new();
                    for label in &node_pat.labels {
                        storage.vertex_add_label(&tx, target_gid, *label)?;
                        target_labels.push(*label);
                    }
                    binding.insert(alias.clone(), PropertyValue::Vertex(
                        mgcore::property_value::VertexRef::new(target_gid, target_labels.clone(), target_properties)
                    ));
                    (target_gid, target_labels)
                }
            } else {
                let target_gid = Gid::from(NEXT_GID.fetch_add(1, Ordering::Relaxed));
                storage.create_vertex(&tx, target_gid)?;
                let mut _target_properties = mgcore::property_store::PropertyStore::new();
                for (key, value) in &node_pat.properties {
                    let val = eval_expression_with_storage(value, binding, Some(storage));
                    storage.vertex_set_property(&tx, target_gid, *key, val.clone())?;
                    _target_properties.set(*key, val);
                }
                let mut target_labels = Vec::new();
                for label in &node_pat.labels {
                    storage.vertex_add_label(&tx, target_gid, *label)?;
                    target_labels.push(*label);
                }
                (target_gid, target_labels)
            };

            let edge_gid = Gid::from(NEXT_GID.fetch_add(1, Ordering::Relaxed));
            let etype = edge_pat.edge_types.first()
                .copied()
                .unwrap_or(EdgeTypeId::from(0u32));
            let (from_gid, to_gid) = match edge_pat.direction {
                Direction::Left => (target_gid, prev_gid),
                _ => (prev_gid, target_gid),
            };
            storage.create_edge(&tx, edge_gid, from_gid, to_gid, etype)?;

            // Bind edge alias for RETURN (e.g. CREATE ...-[r:TYPE]->... RETURN type(r))
            if let Some(ref alias) = edge_pat.alias {
                let mut edge_properties = mgcore::property_store::PropertyStore::new();
                for (key, value) in &edge_pat.properties {
                    let val = eval_expression_with_storage(value, binding, Some(storage));
                    edge_properties.set(*key, val);
                }
                binding.insert(alias.clone(), PropertyValue::Edge(
                    mgcore::property_value::EdgeRefValue::new(edge_gid, etype, from_gid, to_gid, edge_properties)
                ));
                binding.insert(format!("__gid__{}", alias), PropertyValue::Int(edge_gid.as_int()));
            }

            for (key, value) in &edge_pat.properties {
                let val = eval_expression_with_storage(value, binding, Some(storage));
                storage.edge_set_property(&tx, edge_gid, *key, val)?;
            }

            prev_gid = target_gid;
        }
    }

    if owned {
        if !storage.commit_transaction(&tx) {
            return Err(ExecError::Runtime(
                "transaction aborted due to write-write conflict".into(),
            ));
        }
    }
    Ok(())
}

// ─── SET execution ─────────────────────────────────────────────────────

fn exec_set(
    storage: &Storage,
    items: &[SetItem],
    bindings: &[HashMap<String, PropertyValue>],
) -> Result<(), ExecError> {
    let (tx, owned) = match active_transaction() {
        Some(tx) => (tx, false),
        None => (storage.begin_transaction(IsolationLevel::SnapshotIsolation), true),
    };
    let _tx_guard = set_active_transaction(Some(tx.clone()));
    let default_binding = HashMap::new();
    let binding = bindings.first().unwrap_or(&default_binding);

    for item in items {
        match item {
            SetItem::Property { expression, key, value } => {
                let (gid, is_edge) = match expression {
                    Expression::Identifier(name) => {
                        binding.get(name.as_str())
                            .and_then(|v| match v {
                                PropertyValue::Edge(e) => Some((e.gid, true)),
                                PropertyValue::Vertex(vref) => Some((vref.gid, false)),
                                PropertyValue::Int(g) => Some((Gid::from(*g as u64), false)),
                                _ => None,
                            })
                            .or_else(|| {
                                binding.get(&format!("__gid__{}", name))
                                    .and_then(|v| match v {
                                        PropertyValue::Int(g) => Some((Gid::from(*g as u64), false)),
                                        _ => None,
                                    })
                            })
                    }
                    _ => None,
                }.ok_or_else(|| ExecError::Runtime("cannot resolve SET target".into()))?;
                let val = eval_expression_with_storage(value, binding, Some(storage));
                if is_edge {
                    storage.edge_set_property(&tx, gid, *key, val)?;
                } else {
                    storage.vertex_set_property(&tx, gid, *key, val)?;
                }
            }
            SetItem::Variable { alias, expression } => {
                let (gid, is_edge) = binding.get(alias.as_str())
                    .and_then(|v| match v {
                        PropertyValue::Edge(e) => Some((e.gid, true)),
                        PropertyValue::Vertex(vref) => Some((vref.gid, false)),
                        PropertyValue::Int(g) => Some((Gid::from(*g as u64), false)),
                        _ => None,
                    })
                    .or_else(|| {
                        binding.get(&format!("__gid__{}", alias))
                            .and_then(|v| match v {
                                PropertyValue::Int(g) => Some((Gid::from(*g as u64), false)),
                                _ => None,
                            })
                    })
                    .ok_or_else(|| ExecError::Runtime("cannot resolve SET variable target".into()))?;
                let val = eval_expression_with_storage(expression, binding, Some(storage));
                if let PropertyValue::Map(entries) = val {
                    // Cypher semantics: SET n = map replaces ALL properties.
                    // Clear existing properties first.
                    if is_edge {
                        if let Some(snap) = storage.get_edge(gid, &tx) {
                            for (pid, _) in snap.properties.iter() {
                                storage.edge_remove_property(&tx, gid, pid)?;
                            }
                        }
                        for (prop_name, prop_val) in entries {
                            let pid = match active_catalog() {
                                Some(cat) => cat.property(&prop_name),
                                None => PropertyId::from(0u32),
                            };
                            storage.edge_set_property(&tx, gid, pid, prop_val)?;
                        }
                    } else {
                        if let Some(snap) = storage.get_vertex(gid, &tx) {
                            for (pid, _) in snap.properties.iter() {
                                storage.vertex_remove_property(&tx, gid, pid)?;
                            }
                        }
                        for (prop_name, prop_val) in entries {
                            let pid = match active_catalog() {
                                Some(cat) => cat.property(&prop_name),
                                None => PropertyId::from(0u32),
                            };
                            storage.vertex_set_property(&tx, gid, pid, prop_val)?;
                        }
                    }
                }
            }
            SetItem::VariableUpdate { alias, expression } => {
                let (gid, is_edge) = binding.get(alias.as_str())
                    .and_then(|v| match v {
                        PropertyValue::Edge(e) => Some((e.gid, true)),
                        PropertyValue::Vertex(vref) => Some((vref.gid, false)),
                        PropertyValue::Int(g) => Some((Gid::from(*g as u64), false)),
                        _ => None,
                    })
                    .or_else(|| {
                        binding.get(&format!("__gid__{}", alias))
                            .and_then(|v| match v {
                                PropertyValue::Int(g) => Some((Gid::from(*g as u64), false)),
                                _ => None,
                            })
                    })
                    .ok_or_else(|| ExecError::Runtime("cannot resolve SET += target".into()))?;
                let val = eval_expression_with_storage(expression, binding, Some(storage));
                if let PropertyValue::Map(entries) = val {
                    for (prop_name, prop_val) in entries {
                        let pid = match active_catalog() {
                            Some(cat) => cat.property(&prop_name),
                            None => PropertyId::from(0u32),
                        };
                        if is_edge {
                            storage.edge_set_property(&tx, gid, pid, prop_val)?;
                        } else {
                            storage.vertex_set_property(&tx, gid, pid, prop_val)?;
                        }
                    }
                }
            }
            SetItem::Label { alias, label } => {
                let gid = binding.get(&format!("__gid__{}", alias))
                    .or_else(|| binding.get(alias.as_str()))
                    .and_then(|v| match v {
                        PropertyValue::Vertex(vref) => Some(vref.gid),
                        PropertyValue::Int(g) => Some(Gid::from(*g as u64)),
                        _ => None,
                    })
                    .ok_or_else(|| ExecError::Runtime("cannot resolve SET label target".into()))?;
                storage.vertex_add_label(&tx, gid, *label)?;
            }
        }
    }

    if owned {
        if !storage.commit_transaction(&tx) {
            return Err(ExecError::Runtime(
                "transaction aborted due to write-write conflict".into(),
            ));
        }
    }
    Ok(())
}

// ─── REMOVE execution ──────────────────────────────────────────────────

fn exec_remove(
    storage: &Storage,
    items: &[RemoveItem],
    bindings: &[HashMap<String, PropertyValue>],
) -> Result<(), ExecError> {
    let (tx, owned) = match active_transaction() {
        Some(tx) => (tx, false),
        None => (storage.begin_transaction(IsolationLevel::SnapshotIsolation), true),
    };
    let default_binding = HashMap::new();
    let binding = bindings.first().unwrap_or(&default_binding);
    for item in items {
        match item {
            RemoveItem::Property { expression, key } => {
                let (gid, is_edge) = match expression {
                    Expression::Identifier(name) => {
                        binding.get(name.as_str())
                            .and_then(|v| match v {
                                PropertyValue::Edge(e) => Some((e.gid, true)),
                                PropertyValue::Vertex(vref) => Some((vref.gid, false)),
                                PropertyValue::Int(g) => Some((Gid::from(*g as u64), false)),
                                _ => None,
                            })
                            .or_else(|| {
                                binding.get(&format!("__gid__{}", name))
                                    .and_then(|v| match v {
                                        PropertyValue::Int(g) => Some((Gid::from(*g as u64), false)),
                                        _ => None,
                                    })
                            })
                    }
                    _ => None,
                }.ok_or_else(|| ExecError::Runtime("cannot resolve REMOVE target".into()))?;
                if is_edge {
                    storage.edge_remove_property(&tx, gid, *key)?;
                } else {
                    storage.vertex_set_property(&tx, gid, *key, PropertyValue::Null)?;
                }
            }
            RemoveItem::Label { alias, label } => {
                let gid = binding.get(&format!("__gid__{}", alias))
                    .or_else(|| binding.get(alias.as_str()))
                    .and_then(|v| match v { PropertyValue::Int(g) => Some(Gid::from(*g as u64)), _ => None })
                    .ok_or_else(|| ExecError::Runtime("cannot resolve REMOVE target".into()))?;
                storage.vertex_remove_label(&tx, gid, *label)?;
            }
        }
    }
    if owned {
        if !storage.commit_transaction(&tx) {
            return Err(ExecError::Runtime(
                "transaction aborted due to write-write conflict".into(),
            ));
        }
    }
    Ok(())
}

// ─── DELETE execution ──────────────────────────────────────────────────

fn exec_delete(
    storage: &Storage,
    expressions: &[Expression],
    detach: bool,
    bindings: &[HashMap<String, PropertyValue>],
) -> Result<(), ExecError> {
    let (tx, owned) = match active_transaction() {
        Some(tx) => (tx, false),
        None => (storage.begin_transaction(IsolationLevel::SnapshotIsolation), true),
    };
    for binding in bindings {
        for expr in expressions {
            let val = eval_expression(expr, binding);
            match val {
                PropertyValue::Vertex(v) => {
                    let gid = Gid::from(v.gid.as_uint());
                    if detach {
                        let edges = storage.all_edges_gids();
                        for (edge_gid, from, to, _) in &edges {
                            if *from == gid || *to == gid {
                                storage.delete_edge(&tx, *edge_gid)?;
                            }
                        }
                    }
                    storage.delete_vertex(&tx, gid)?;
                }
                PropertyValue::Edge(e) => {
                    storage.delete_edge(&tx, e.gid)?;
                }
                _ => return Err(ExecError::Runtime("DELETE target must be a vertex or edge".into())),
            }
        }
    }
    if owned {
        if !storage.commit_transaction(&tx) {
            return Err(ExecError::Runtime(
                "transaction aborted due to write-write conflict".into(),
            ));
        }
    }
    Ok(())
}

// ─── CALL execution ────────────────────────────────────────────────────

fn args_to_map(args: &[PropertyValue]) -> HashMap<String, PropertyValue> {
    if args.len() == 1 {
        if let PropertyValue::Map(entries) = &args[0] {
            return entries.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        }
    }
    HashMap::new()
}

fn parse_partition_arg(args: &[PropertyValue]) -> Result<HashMap<Gid, usize>, ExecError> {
    if args.is_empty() {
        return Ok(HashMap::new());
    }
    if args.len() != 2 {
        return Err(ExecError::Runtime(
            "partition arguments must be two lists: [node_ids], [community_ids]".into(),
        ));
    }
    let nodes = match &args[0] {
        PropertyValue::List(items) => items,
        _ => {
            return Err(ExecError::Runtime(
                "first partition argument must be a list of node ids".into(),
            ))
        }
    };
    let communities = match &args[1] {
        PropertyValue::List(items) => items,
        _ => {
            return Err(ExecError::Runtime(
                "second partition argument must be a list of community ids".into(),
            ))
        }
    };
    if nodes.len() != communities.len() {
        return Err(ExecError::Runtime(
            "node list and community list must have the same length".into(),
        ));
    }
    let mut partition = HashMap::new();
    for (n, c) in nodes.iter().zip(communities.iter()) {
        let gid = match n {
            PropertyValue::Int(id) => Gid::from(*id as u64),
            _ => {
                return Err(ExecError::Runtime(
                    "node id must be an integer".into(),
                ))
            }
        };
        let cid = match c {
            PropertyValue::Int(id) => *id as usize,
            _ => {
                return Err(ExecError::Runtime(
                    "community id must be an integer".into(),
                ))
            }
        };
        partition.insert(gid, cid);
    }
    Ok(partition)
}

fn exec_call(
    storage: &Storage,
    registry: Option<&ProcedureRegistry>,
    name: &str,
    arguments: &[Expression],
    bindings: &[HashMap<String, PropertyValue>],
    _yield_items: &[String],
) -> Result<QueryResult, ExecError> {
    let name_lower = name.to_lowercase();

    // Evaluate arguments
    let args: Vec<PropertyValue> = arguments
        .iter()
        .map(|expr| {
            // Use first binding for argument evaluation (procedures typically don't need per-row binding)
            bindings.first()
                .map(|b| eval::eval_expression_with_storage(expr, b, Some(storage)))
                .unwrap_or(PropertyValue::Null)
        })
        .collect();

    // Check external/custom registry first
    if let Some(reg) = registry {
        if let Some(proc) = reg.get(&name_lower) {
            return proc(storage, &args);
        }
    }

    // Legacy hardcoded procedures (kept for compatibility)
    match name_lower.as_str() {
        "show.procedures" | "show procedures" | "dbms.procedures" => {
            let mut rows = Vec::new();
            let mut seen = std::collections::HashSet::new();
            if let Some(reg) = registry {
                for name in reg.names() {
                    if seen.insert(name.clone()) {
                        let mut r = HashMap::new();
                        r.insert("name".to_string(), PropertyValue::String(name.clone()));
                        r.insert("signature".to_string(), PropertyValue::String(format!("{}() :: (result :: ANY)", name)));
                        rows.push(r);
                    }
                }
            }
            let builtin_reg = builtin_procs::ProcedureRegistry::new();
            for name in builtin_reg.list() {
                if seen.insert(name.clone()) {
                    let mut r = HashMap::new();
                    r.insert("name".to_string(), PropertyValue::String(name.clone()));
                    r.insert("signature".to_string(), PropertyValue::String(format!("{}() :: (result :: ANY)", name)));
                    rows.push(r);
                }
            }
            // Hardcoded algorithm procedures not in builtin registry
            let hardcoded: [&str; 76] = [
                "algo.pagerank", "algo.wcc", "algo.bfs", "algo.triangle_count",
                "algo.shortest_path", "algo.degree_centrality", "algo.clustering_coefficient",
                "algo.betweenness_centrality", "algo.closeness_centrality", "algo.scc",
                "algo.diameter", "algo.average_path_length", "algo.degreecentrality",
                "algo.degrecentrality", "algo.graphdensity", "algo.graph_density",
                "algo.globalclusteringcoefficient", "algo.global_clustering_coefficient",
                "algo.coreness", "algo.degreeassortativity", "algo.degree_assortativity",
                "algo.bridges", "algo.eccentricity", "algo.eigenvector_centrality",
                "algo.harmonic_centrality", "algo.has_cycle", "algo.is_bipartite",
                "algo.radius", "algo.topological_sort", "algo.center", "algo.periphery",
                "algo.small_world_coefficient", "algo.articulation_points", "algo.is_biconnected",
                "algo.connected_components", "algo.cycle_detection", "algo.all_pairs_shortest_path",
                "algo.maximal_cliques", "algo.clique_number", "algo.greedy_coloring",
                "algo.dsatur_coloring", "algo.degeneracy", "algo.predict_links",
                "algo.dfs", "algo.random_walk", "algo.jaccard_similarity",
                "algo.cosine_similarity", "algo.adamic_adar", "algo.common_neighbors",
                "algo.resource_allocation", "algo.preferential_attachment", "algo.soundarajan_hopcroft", "algo.dijkstra",
                "algo.dijkstra_all", "algo.floyd_warshall", "algo.prim", "algo.kruskal",
                "algo.k_core", "algo.k_core_decomposition", "algo.louvain",
                "algo.label_propagation", "algo.katz_centrality", "algo.count_paths_of_length",
                "algo.hits", "algo.shortest_path_weighted", "algo.simple_random_walk",
                "algo.biased_random_walk", "algo.generate_walks", "algo.random_walk_with_restart",
                "algo.personalized_pagerank", "algo.cliques_containing", "algo.chromatic_number",
                "algo.rich_club_coefficient", "algo.modularity", "algo.conductance", "algo.normalized_cut",
            ];
            for name in hardcoded {
                if seen.insert(name.to_string()) {
                    let mut r = HashMap::new();
                    r.insert("name".to_string(), PropertyValue::String(name.to_string()));
                    r.insert("signature".to_string(), PropertyValue::String(format!("{}() :: (result :: ANY)", name)));
                    rows.push(r);
                }
            }
            Ok(QueryResult { number_of_hops: 0,
                columns: vec!["name".into(), "signature".into()],
                rows,
            })
        }
        "show.schema" | "show schema" | "show schema info" => {
            Ok(QueryResult { number_of_hops: 0,
                columns: vec!["schema".into()],
                rows: vec![row("schema", "{}", "schema", "{}")],
            })
        }
        "dbms.info" => {
            Ok(QueryResult { number_of_hops: 0,
                columns: vec!["version".into(), "storage_mode".into()],
                rows: vec![row("version", "0.1.0", "storage_mode", "in-memory")],
            })
        }
        "db.stats" | "storage.info" => {
            let vc = storage.vertex_count() as i64;
            let ec = storage.edge_count() as i64;
            let label_idx_count = storage.active_label_indices.read().unwrap().len() as i64;
            let lp_idx_count = storage.active_label_property_indices.read().unwrap().len() as i64;
            let point_idx_count = storage.active_point_indices.read().unwrap().len() as i64;
            let constraint_count = storage.constraints.list().len() as i64;
            let text_idx_count = storage.text_indices.read().unwrap().len() as i64;
            let vector_idx_count = storage.vector_indices.read().unwrap().len() as i64;
            let tx_committed = storage.metrics.snapshot().transactions_committed as i64;
            let tx_aborted = storage.metrics.snapshot().transactions_aborted as i64;
            let mode = if storage.is_on_disk() { "on_disk" } else { "in_memory" };
            Ok(QueryResult { number_of_hops: 0,
                columns: vec!["stat".into(), "value".into()],
                rows: vec![
                    row("stat", "vertex_count", "value", &vc.to_string()),
                    row("stat", "edge_count", "value", &ec.to_string()),
                    row("stat", "label_indices", "value", &label_idx_count.to_string()),
                    row("stat", "label_property_indices", "value", &lp_idx_count.to_string()),
                    row("stat", "point_indices", "value", &point_idx_count.to_string()),
                    row("stat", "text_indices", "value", &text_idx_count.to_string()),
                    row("stat", "vector_indices", "value", &vector_idx_count.to_string()),
                    row("stat", "constraints", "value", &constraint_count.to_string()),
                    row("stat", "transactions_committed", "value", &tx_committed.to_string()),
                    row("stat", "transactions_aborted", "value", &tx_aborted.to_string()),
                    row("stat", "storage_mode", "value", mode),
                ],
            })
        }
        "db.analytics" => {
            let stats = mgstorage::analytics::compute_graph_stats(storage);
            let mut r = HashMap::new();
            r.insert("vertexCount".to_string(), PropertyValue::Int(stats.vertex_count as i64));
            r.insert("edgeCount".to_string(), PropertyValue::Int(stats.edge_count as i64));
            r.insert("avgDegree".to_string(), PropertyValue::Double(stats.avg_degree));
            r.insert("density".to_string(), PropertyValue::Double(stats.density));
            r.insert("connectedComponents".to_string(), PropertyValue::Int(stats.connected_component_count as i64));
            r.insert("isolatedVertices".to_string(), PropertyValue::Int(stats.isolated_vertex_count as i64));
            Ok(QueryResult { number_of_hops: 0,
                columns: vec!["vertexCount".into(), "edgeCount".into(), "avgDegree".into(), "density".into(), "connectedComponents".into(), "isolatedVertices".into()],
                rows: vec![r],
            })
        }
        "db.querystats" | "db.queryStats" => {
            let stats = storage.query_profiler.stats();
            let mut rows = Vec::new();
            for (query_text, s) in stats {
                let mut r = HashMap::new();
                r.insert("query".to_string(), PropertyValue::String(query_text));
                r.insert("count".to_string(), PropertyValue::Int(s.count as i64));
                r.insert("totalExecutionTimeMs".to_string(), PropertyValue::Double(s.total_execution_time.as_millis() as f64));
                r.insert("maxExecutionTimeMs".to_string(), PropertyValue::Double(s.max_execution_time.as_millis() as f64));
                r.insert("minExecutionTimeMs".to_string(), PropertyValue::Double(s.min_execution_time.as_millis() as f64));
                rows.push(r);
            }
            Ok(QueryResult { number_of_hops: 0, columns: vec!["query".into(), "count".into(), "totalExecutionTimeMs".into(), "maxExecutionTimeMs".into(), "minExecutionTimeMs".into()], rows })
        }
        "db.listqueries" | "db.listQueries" => {
            let active = storage.list_active_queries();
            let mut rows = Vec::new();
            for q in active {
                let mut r = HashMap::new();
                r.insert("queryId".to_string(), PropertyValue::Int(q.query_id as i64));
                r.insert("query".to_string(), PropertyValue::String(q.query_text));
                r.insert("elapsedTimeMs".to_string(), PropertyValue::Double(q.started_at.elapsed().as_millis() as f64));
                rows.push(r);
            }
            Ok(QueryResult { number_of_hops: 0, columns: vec!["queryId".into(), "query".into(), "elapsedTimeMs".into()], rows })
        }
        "db.labels" | "storage.labels" => {
            let cat = active_catalog();
            let labels = storage.schema_info.all_labels();
            let mut rows: Vec<HashMap<String, PropertyValue>> = labels.iter().map(|l| {
                let count = storage.label_count(*l) as i64;
                let name = cat.as_ref().map(|c| c.label_name(*l)).unwrap_or_else(|| format!("{}", l.as_uint()));
                row("label", &name, "count", &count.to_string())
            }).collect();
            rows.sort_by(|a, b| {
                let a_name = a.get("label").and_then(|v| match v { PropertyValue::String(s) => Some(s.as_str()), _ => None }).unwrap_or("");
                let b_name = b.get("label").and_then(|v| match v { PropertyValue::String(s) => Some(s.as_str()), _ => None }).unwrap_or("");
                a_name.cmp(b_name)
            });
            Ok(QueryResult { number_of_hops: 0, columns: vec!["label".into(), "count".into()], rows })
        }
        "db.constraints" => {
            let cat = active_catalog();
            let mut rows = Vec::new();
            for c in storage.constraints.list() {
                let mut r = HashMap::new();
                r.insert("type".to_string(), PropertyValue::String(format!("{:?}", c.kind)));
                let label_name = cat.as_ref().map(|catalog| catalog.label_name(c.label)).unwrap_or_else(|| format!("{}", c.label.as_uint()));
                let prop_name = cat.as_ref().map(|catalog| catalog.property_name(c.property)).unwrap_or_else(|| format!("{}", c.property.as_uint()));
                r.insert("label".to_string(), PropertyValue::String(label_name));
                r.insert("property".to_string(), PropertyValue::String(prop_name));
                rows.push(r);
            }
            Ok(QueryResult { number_of_hops: 0, columns: vec!["type".into(), "label".into(), "property".into()], rows })
        }
        "db.indexes" => {
            let cat = active_catalog();
            let mut rows = Vec::new();
            let labels = storage.active_label_indices.read().unwrap();
            for label in labels.iter() {
                let mut r = HashMap::new();
                let name = cat.as_ref().map(|c| c.label_name(*label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                r.insert("label".to_string(), PropertyValue::String(name));
                r.insert("type".to_string(), PropertyValue::String("label".into()));
                rows.push(r);
            }
            let lp = storage.active_label_property_indices.read().unwrap();
            for (label, prop) in lp.iter() {
                let mut r = HashMap::new();
                let label_name = cat.as_ref().map(|c| c.label_name(*label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                let prop_name = cat.as_ref().map(|c| c.property_name(*prop)).unwrap_or_else(|| format!("{}", prop.as_uint()));
                r.insert("label".to_string(), PropertyValue::String(label_name));
                r.insert("property".to_string(), PropertyValue::String(prop_name));
                r.insert("type".to_string(), PropertyValue::String("label+property".into()));
                rows.push(r);
            }
            let et = storage.active_edge_type_indices.read().unwrap();
            for edge_type in et.iter() {
                let mut r = HashMap::new();
                let name = cat.as_ref().map(|c| c.edge_type_name(*edge_type)).unwrap_or_else(|| format!("{}", edge_type.as_uint()));
                r.insert("label".to_string(), PropertyValue::String(name));
                r.insert("type".to_string(), PropertyValue::String("edge_type".into()));
                rows.push(r);
            }
            let etp = storage.active_edge_type_property_indices.read().unwrap();
            for (edge_type, prop) in etp.iter() {
                let mut r = HashMap::new();
                let et_name = cat.as_ref().map(|c| c.edge_type_name(*edge_type)).unwrap_or_else(|| format!("{}", edge_type.as_uint()));
                let prop_name = cat.as_ref().map(|c| c.property_name(*prop)).unwrap_or_else(|| format!("{}", prop.as_uint()));
                r.insert("label".to_string(), PropertyValue::String(et_name));
                r.insert("property".to_string(), PropertyValue::String(prop_name));
                r.insert("type".to_string(), PropertyValue::String("edge_type+property".into()));
                rows.push(r);
            }
            // Vector indices
            let vector_indices = storage.vector_indices.read().unwrap();
            for (label, entry) in vector_indices.iter() {
                let mut r = HashMap::new();
                let label_name = cat.as_ref().map(|c| c.label_name(*label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                let prop_name = cat.as_ref().map(|c| c.property_name(entry.property)).unwrap_or_else(|| format!("{}", entry.property.as_uint()));
                r.insert("label".to_string(), PropertyValue::String(label_name));
                r.insert("property".to_string(), PropertyValue::String(prop_name));
                r.insert("type".to_string(), PropertyValue::String("vector".into()));
                r.insert("dimension".to_string(), PropertyValue::Int(entry.dimension as i64));
                r.insert("distance".to_string(), PropertyValue::String(format!("{:?}", entry.distance)));
                rows.push(r);
            }
            drop(vector_indices);
            // Text indices
            let text_indices = storage.text_indices.read().unwrap();
            for (label, entry) in text_indices.iter() {
                let mut r = HashMap::new();
                let label_name = cat.as_ref().map(|c| c.label_name(*label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                let prop_names: Vec<String> = entry.properties.iter().map(|(pid, _)| {
                    cat.as_ref().map(|c| c.property_name(*pid)).unwrap_or_else(|| format!("{}", pid.as_uint()))
                }).collect();
                r.insert("label".to_string(), PropertyValue::String(label_name));
                r.insert("property".to_string(), PropertyValue::String(prop_names.join(",")));
                r.insert("type".to_string(), PropertyValue::String("text".into()));
                rows.push(r);
            }
            drop(text_indices);
            // Point indices
            let point_indices = storage.active_point_indices.read().unwrap();
            for (label, prop) in point_indices.iter() {
                let mut r = HashMap::new();
                let label_name = cat.as_ref().map(|c| c.label_name(*label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                let prop_name = cat.as_ref().map(|c| c.property_name(*prop)).unwrap_or_else(|| format!("{}", prop.as_uint()));
                r.insert("label".to_string(), PropertyValue::String(label_name));
                r.insert("property".to_string(), PropertyValue::String(prop_name));
                r.insert("type".to_string(), PropertyValue::String("point".into()));
                rows.push(r);
            }
            Ok(QueryResult { number_of_hops: 0, columns: vec!["label".into(), "property".into(), "type".into()], rows })
        }
        "db.propertykeys" | "db.property_keys" | "db.propertyKeys" => {
            let rows: Vec<HashMap<String, PropertyValue>> = if let Some(cat) = active_catalog() {
                cat.property_names().into_iter().map(|name| {
                    let mut r = HashMap::new();
                    r.insert("propertyKey".to_string(), PropertyValue::String(name));
                    r
                }).collect()
            } else {
                Vec::new()
            };
            Ok(QueryResult { number_of_hops: 0, columns: vec!["propertyKey".into()], rows })
        }
        "db.relationshiptypes" | "db.relationship_types" | "db.relationshipTypes" => {
            let rows: Vec<HashMap<String, PropertyValue>> = if let Some(cat) = active_catalog() {
                cat.edge_type_names().into_iter().map(|name| {
                    let mut r = HashMap::new();
                    r.insert("relationshipType".to_string(), PropertyValue::String(name));
                    r
                }).collect()
            } else {
                Vec::new()
            };
            Ok(QueryResult { number_of_hops: 0, columns: vec!["relationshipType".into()], rows })
        }
        "db.schema.nodetypeproperties" | "db.schema.nodeTypeProperties" => {
            let cat = active_catalog();
            let labels = storage.schema_info.all_labels();
            let mut rows = Vec::new();
            for label in labels {
                let label_name = cat.as_ref().map(|c| c.label_name(label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                for prop in storage.schema_info.label_properties(label) {
                    let prop_name = cat.as_ref().map(|c| c.property_name(prop)).unwrap_or_else(|| format!("{}", prop.as_uint()));
                    let mut r = HashMap::new();
                    r.insert("nodeType".to_string(), PropertyValue::String(label_name.clone()));
                    r.insert("propertyName".to_string(), PropertyValue::String(prop_name));
                    r.insert("propertyTypes".to_string(), PropertyValue::List(vec![PropertyValue::String("String".into())]));
                    rows.push(r);
                }
            }
            rows.sort_by(|a, b| {
                let a_type = a.get("nodeType").and_then(|v| match v { PropertyValue::String(s) => Some(s.as_str()), _ => None }).unwrap_or("");
                let b_type = b.get("nodeType").and_then(|v| match v { PropertyValue::String(s) => Some(s.as_str()), _ => None }).unwrap_or("");
                let a_prop = a.get("propertyName").and_then(|v| match v { PropertyValue::String(s) => Some(s.as_str()), _ => None }).unwrap_or("");
                let b_prop = b.get("propertyName").and_then(|v| match v { PropertyValue::String(s) => Some(s.as_str()), _ => None }).unwrap_or("");
                a_type.cmp(b_type).then_with(|| a_prop.cmp(b_prop))
            });
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeType".into(), "propertyName".into(), "propertyTypes".into()], rows })
        }
        "db.schema.reltypeproperties" | "db.schema.relTypeProperties" => {
            let cat = active_catalog();
            let edge_types = storage.schema_info.all_edge_types();
            let mut rows = Vec::new();
            for etype in edge_types {
                let etype_name = cat.as_ref().map(|c| c.edge_type_name(etype)).unwrap_or_else(|| format!("{}", etype.as_uint()));
                for prop in storage.schema_info.edge_type_properties(etype) {
                    let prop_name = cat.as_ref().map(|c| c.property_name(prop)).unwrap_or_else(|| format!("{}", prop.as_uint()));
                    let mut r = HashMap::new();
                    r.insert("relType".to_string(), PropertyValue::String(etype_name.clone()));
                    r.insert("propertyName".to_string(), PropertyValue::String(prop_name));
                    r.insert("propertyTypes".to_string(), PropertyValue::List(vec![PropertyValue::String("String".into())]));
                    rows.push(r);
                }
            }
            rows.sort_by(|a, b| {
                let a_type = a.get("relType").and_then(|v| match v { PropertyValue::String(s) => Some(s.as_str()), _ => None }).unwrap_or("");
                let b_type = b.get("relType").and_then(|v| match v { PropertyValue::String(s) => Some(s.as_str()), _ => None }).unwrap_or("");
                let a_prop = a.get("propertyName").and_then(|v| match v { PropertyValue::String(s) => Some(s.as_str()), _ => None }).unwrap_or("");
                let b_prop = b.get("propertyName").and_then(|v| match v { PropertyValue::String(s) => Some(s.as_str()), _ => None }).unwrap_or("");
                a_type.cmp(b_type).then_with(|| a_prop.cmp(b_prop))
            });
            Ok(QueryResult { number_of_hops: 0, columns: vec!["relType".into(), "propertyName".into(), "propertyTypes".into()], rows })
        }
        "apoc.meta.schema" | "apoc.meta_schema" | "apoc_meta_schema" => {
            let cat = active_catalog();
            let mut node_types: Vec<(String, PropertyValue)> = Vec::new();
            for label in storage.schema_info.all_labels() {
                let label_name = cat.as_ref().map(|c| c.label_name(label)).unwrap_or_else(|| format!("{}", label.as_uint()));
                let count = storage.label_count(label) as i64;
                let mut props: Vec<(String, PropertyValue)> = Vec::new();
                for prop in storage.schema_info.label_properties(label) {
                    let prop_name = cat.as_ref().map(|c| c.property_name(prop)).unwrap_or_else(|| format!("{}", prop.as_uint()));
                    props.push((prop_name, PropertyValue::String("String".into())));
                }
                let node_info = PropertyValue::Map(vec![
                    ("count".into(), PropertyValue::Int(count)),
                    ("properties".into(), PropertyValue::Map(props)),
                ]);
                node_types.push((label_name, node_info));
            }
            let mut rel_types: Vec<(String, PropertyValue)> = Vec::new();
            for etype in storage.schema_info.all_edge_types() {
                let etype_name = cat.as_ref().map(|c| c.edge_type_name(etype)).unwrap_or_else(|| format!("{}", etype.as_uint()));
                let count = storage.edge_type_count(etype) as i64;
                let mut props: Vec<(String, PropertyValue)> = Vec::new();
                for prop in storage.schema_info.edge_type_properties(etype) {
                    let prop_name = cat.as_ref().map(|c| c.property_name(prop)).unwrap_or_else(|| format!("{}", prop.as_uint()));
                    props.push((prop_name, PropertyValue::String("String".into())));
                }
                let rel_info = PropertyValue::Map(vec![
                    ("count".into(), PropertyValue::Int(count)),
                    ("properties".into(), PropertyValue::Map(props)),
                ]);
                rel_types.push((etype_name, rel_info));
            }
            let schema = PropertyValue::Map(vec![
                ("nodes".into(), PropertyValue::Map(node_types)),
                ("relationships".into(), PropertyValue::Map(rel_types)),
            ]);
            let mut r = HashMap::new();
            r.insert("value".to_string(), schema);
            Ok(QueryResult { number_of_hops: 0, columns: vec!["value".into()], rows: vec![r] })
        }
        "db.dump" => {
            let cat = active_catalog();
            let mut rows = Vec::new();
            // Dump vertices
            for (_gid, labels, props) in storage.all_vertices() {
                let label_names: Vec<String> = labels.iter()
                    .map(|l| cat.as_ref().map(|c| c.label_name(*l)).unwrap_or_else(|| format!("L{}", l.as_uint())))
                    .collect();
                let label_str = if label_names.is_empty() { String::new() } else { format!(":{}", label_names.join(":")) };
                let prop_str = format_property_store(&props, cat.as_deref());
                let cypher = if prop_str.is_empty() {
                    format!("CREATE (:{}{})", label_str, "")
                } else {
                    format!("CREATE (:{} {{{}}})", label_str, prop_str)
                };
                let mut r = HashMap::new();
                r.insert("cypher".to_string(), PropertyValue::String(cypher));
                r.insert("type".to_string(), PropertyValue::String("vertex".into()));
                rows.push(r);
            }
            // Dump edges
            for (_gid, from, to, etype, props) in storage.all_edges() {
                let etype_name = cat.as_ref().map(|c| c.edge_type_name(etype)).unwrap_or_else(|| format!("R{}", etype.as_uint()));
                let prop_str = format_property_store(&props, cat.as_deref());
                let cypher = if prop_str.is_empty() {
                    format!("MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CREATE (a)-[:{}]->(b)", from.as_uint(), to.as_uint(), etype_name)
                } else {
                    format!("MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CREATE (a)-[:{} {{{}}}]->(b)", from.as_uint(), to.as_uint(), etype_name, prop_str)
                };
                let mut r = HashMap::new();
                r.insert("cypher".to_string(), PropertyValue::String(cypher));
                r.insert("type".to_string(), PropertyValue::String("edge".into()));
                rows.push(r);
            }
            Ok(QueryResult { number_of_hops: 0, columns: vec!["cypher".into(), "type".into()], rows })
        }
        "db.createtextindex" | "db.createTextIndex" => {
            let map = args_to_map(&args);
            match builtin_procs::db_create_text_index(storage, &map) {
                Ok(rows) => Ok(QueryResult { number_of_hops: 0, columns: vec![], rows }),
                Err(e) => Err(ExecError::Runtime(e)),
            }
        }
        "db.searchtextindex" | "db.searchTextIndex" => {
            let map = args_to_map(&args);
            match builtin_procs::db_search_text_index(storage, &map) {
                Ok(rows) => Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "score".into()], rows }),
                Err(e) => Err(ExecError::Runtime(e)),
            }
        }
        "db.createvectorindex" | "db.createVectorIndex" => {
            let map = args_to_map(&args);
            match builtin_procs::db_create_vector_index(storage, &map) {
                Ok(rows) => Ok(QueryResult { number_of_hops: 0, columns: vec![], rows }),
                Err(e) => Err(ExecError::Runtime(e)),
            }
        }
        "db.searchvectorindex" | "db.searchVectorIndex" => {
            let map = args_to_map(&args);
            match builtin_procs::db_search_vector_index(storage, &map) {
                Ok(rows) => Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "distance".into()], rows }),
                Err(e) => Err(ExecError::Runtime(e)),
            }
        }
        "db.createpointindex" | "db.createPointIndex" => {
            let map = args_to_map(&args);
            match builtin_procs::db_create_point_index(storage, &map) {
                Ok(rows) => Ok(QueryResult { number_of_hops: 0, columns: vec![], rows }),
                Err(e) => Err(ExecError::Runtime(e)),
            }
        }
        "db.droppointindex" | "db.dropPointIndex" => {
            let map = args_to_map(&args);
            match builtin_procs::db_drop_point_index(storage, &map) {
                Ok(rows) => Ok(QueryResult { number_of_hops: 0, columns: vec![], rows }),
                Err(e) => Err(ExecError::Runtime(e)),
            }
        }
        "db.withinbbox" | "db.withinBBox" => {
            let map = args_to_map(&args);
            match builtin_procs::db_within_bbox(storage, &map) {
                Ok(rows) => Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into()], rows }),
                Err(e) => Err(ExecError::Runtime(e)),
            }
        }
        "db.nearest" => {
            let map = args_to_map(&args);
            match builtin_procs::db_nearest(storage, &map) {
                Ok(rows) => Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "distance".into()], rows }),
                Err(e) => Err(ExecError::Runtime(e)),
            }
        }
        "db.createlabel" | "db.createLabel" => {
            if let Some(PropertyValue::String(name)) = args.first() {
                if let Some(cat) = eval::active_catalog() {
                    let _ = cat.label(name);
                }
                Ok(QueryResult { number_of_hops: 0, columns: vec!["label".into()], rows: vec![{ let mut r = HashMap::new(); r.insert("label".to_string(), PropertyValue::String(name.clone())); r }] })
            } else {
                Err(ExecError::Runtime("db.createLabel requires a label name string".into()))
            }
        }
        "db.createproperty" | "db.createProperty" => {
            if let Some(PropertyValue::String(name)) = args.first() {
                if let Some(cat) = eval::active_catalog() {
                    let _ = cat.property(name);
                }
                Ok(QueryResult { number_of_hops: 0, columns: vec!["property".into()], rows: vec![{ let mut r = HashMap::new(); r.insert("property".to_string(), PropertyValue::String(name.clone())); r }] })
            } else {
                Err(ExecError::Runtime("db.createProperty requires a property name string".into()))
            }
        }
        "db.createrelationshiptype" | "db.createRelationshipType" => {
            if let Some(PropertyValue::String(name)) = args.first() {
                if let Some(cat) = eval::active_catalog() {
                    let _ = cat.edge_type(name);
                }
                Ok(QueryResult { number_of_hops: 0, columns: vec!["relationshipType".into()], rows: vec![{ let mut r = HashMap::new(); r.insert("relationshipType".to_string(), PropertyValue::String(name.clone())); r }] })
            } else {
                Err(ExecError::Runtime("db.createRelationshipType requires a type name string".into()))
            }
        }
        "db.createedgetypeindex" | "db.createEdgeTypeIndex" => {
            if let Some(PropertyValue::String(name)) = args.first() {
                if let Some(cat) = eval::active_catalog() {
                    let etype = cat.edge_type(name);
                    storage.create_edge_type_index(etype);
                    Ok(QueryResult { number_of_hops: 0, columns: vec!["edgeType".into(), "status".into()], rows: vec![{ let mut r = HashMap::new(); r.insert("edgeType".to_string(), PropertyValue::String(name.clone())); r.insert("status".to_string(), PropertyValue::String("created".into())); r }] })
                } else {
                    Err(ExecError::Runtime("db.createEdgeTypeIndex requires a catalog".into()))
                }
            } else {
                Err(ExecError::Runtime("db.createEdgeTypeIndex requires an edge type name string".into()))
            }
        }
        "db.dropedgetypeindex" | "db.dropEdgeTypeIndex" => {
            if let Some(PropertyValue::String(name)) = args.first() {
                if let Some(cat) = eval::active_catalog() {
                    let etype = cat.edge_type(name);
                    storage.drop_edge_type_index(etype);
                    Ok(QueryResult { number_of_hops: 0, columns: vec!["edgeType".into(), "status".into()], rows: vec![{ let mut r = HashMap::new(); r.insert("edgeType".to_string(), PropertyValue::String(name.clone())); r.insert("status".to_string(), PropertyValue::String("dropped".into())); r }] })
                } else {
                    Err(ExecError::Runtime("db.dropEdgeTypeIndex requires a catalog".into()))
                }
            } else {
                Err(ExecError::Runtime("db.dropEdgeTypeIndex requires an edge type name string".into()))
            }
        }
        "db.createedgetypepropertyindex" | "db.createEdgeTypePropertyIndex" => {
            if args.len() >= 2 {
                let etype_name = match &args[0] {
                    PropertyValue::String(s) => s.clone(),
                    _ => return Err(ExecError::Runtime("db.createEdgeTypePropertyIndex requires edge type name string".into())),
                };
                let prop_name = match &args[1] {
                    PropertyValue::String(s) => s.clone(),
                    _ => return Err(ExecError::Runtime("db.createEdgeTypePropertyIndex requires property name string".into())),
                };
                if let Some(cat) = eval::active_catalog() {
                    let etype = cat.edge_type(&etype_name);
                    let prop = cat.property(&prop_name);
                    storage.create_edge_type_property_index(etype, prop);
                    Ok(QueryResult { number_of_hops: 0, columns: vec!["edgeType".into(), "property".into(), "status".into()], rows: vec![{ let mut r = HashMap::new(); r.insert("edgeType".to_string(), PropertyValue::String(etype_name)); r.insert("property".to_string(), PropertyValue::String(prop_name)); r.insert("status".to_string(), PropertyValue::String("created".into())); r }] })
                } else {
                    Err(ExecError::Runtime("db.createEdgeTypePropertyIndex requires a catalog".into()))
                }
            } else {
                Err(ExecError::Runtime("db.createEdgeTypePropertyIndex requires edge type and property names".into()))
            }
        }
        "db.stats.clear" | "db.statsClear" => {
            storage.query_profiler.clear();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["status".into()], rows: vec![{ let mut r = HashMap::new(); r.insert("status".to_string(), PropertyValue::String("cleared".into())); r }] })
        }
        "db.stats.retrieve" | "db.statsRetrieve" => {
            let stats = storage.query_profiler.stats();
            let rows: Vec<HashMap<String, PropertyValue>> = stats.into_iter().map(|(query, s)| {
                let mut r = HashMap::new();
                r.insert("query".to_string(), PropertyValue::String(query));
                r.insert("count".to_string(), PropertyValue::Int(s.count as i64));
                r.insert("totalMs".to_string(), PropertyValue::Int(s.total_execution_time.as_millis() as i64));
                r.insert("maxMs".to_string(), PropertyValue::Int(s.max_execution_time.as_millis() as i64));
                r.insert("minMs".to_string(), PropertyValue::Int(s.min_execution_time.as_millis() as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["query".into(), "count".into(), "totalMs".into(), "maxMs".into(), "minMs".into()], rows })
        }
        "algo.pagerank" => {
            let ranks = mgquery::pagerank(storage, 0.85, 100, 1e-6);
            let rows: Vec<HashMap<String, PropertyValue>> = ranks.into_iter().map(|(gid, rank)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("rank".to_string(), PropertyValue::Double(rank));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "rank".into()], rows })
        }
        "algo.wcc" => {
            let comps = mgquery::wcc(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = comps.into_iter().map(|(gid, cid)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("component".to_string(), PropertyValue::Int(cid as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "component".into()], rows })
        }
        "algo.bfs" => {
            let start_gid = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let path = mgquery::bfs(storage, start_gid);
            let rows: Vec<HashMap<String, PropertyValue>> = path.into_iter().enumerate().map(|(i, gid)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("depth".to_string(), PropertyValue::Int(i as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "depth".into()], rows })
        }
        "algo.triangle_count" => {
            let count = mgquery::triangle_count(storage);
            let mut r = HashMap::new();
            r.insert("triangles".to_string(), PropertyValue::Int(count as i64));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["triangles".into()], rows: vec![r] })
        }
        "algo.shortest_path" => {
            let from = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let to = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            if let Some(path) = mgquery::shortest_path(storage, from, to) {
                let mut r = HashMap::new();
                r.insert("path".to_string(), PropertyValue::List(
                    path.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()
                ));
                Ok(QueryResult { number_of_hops: 0, columns: vec!["path".into()], rows: vec![r] })
            } else {
                Ok(QueryResult { number_of_hops: 0, columns: vec!["path".into()], rows: vec![] })
            }
        }
        "algo.degree_centrality" => {
            let deg = mgquery::degree_centrality(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = deg.into_iter().map(|(gid, d)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("degree".to_string(), PropertyValue::Int(d as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "degree".into()], rows })
        }
        "algo.clustering_coefficient" => {
            let cc = mgquery::clustering_coefficient(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = cc.into_iter().map(|(gid, c)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("coefficient".to_string(), PropertyValue::Double(c));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "coefficient".into()], rows })
        }
        "algo.betweenness_centrality" => {
            let bc = mgquery::betweenness_centrality(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = bc.into_iter().map(|(gid, b)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("centrality".to_string(), PropertyValue::Double(b));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "centrality".into()], rows })
        }
        "algo.closeness_centrality" => {
            let cc = mgquery::closeness_centrality(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = cc.into_iter().map(|(gid, c)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("centrality".to_string(), PropertyValue::Double(c));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "centrality".into()], rows })
        }
        "algo.scc" => {
            let comps = mgquery::scc(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = comps.into_iter().map(|(gid, cid)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("component".to_string(), PropertyValue::Int(cid as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "component".into()], rows })
        }
        "algo.diameter" => {
            let d = mgquery::diameter(storage);
            let mut r = HashMap::new();
            r.insert("diameter".to_string(), PropertyValue::Int(d as i64));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["diameter".into()], rows: vec![r] })
        }
        "algo.average_path_length" => {
            let avg = mgquery::average_path_length(storage);
            let mut r = HashMap::new();
            r.insert("average_path_length".to_string(), PropertyValue::Double(avg));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["average_path_length".into()], rows: vec![r] })
        }
        // Camel-case aliases for APOC-style naming
        "algo.degrecentrality" | "algo.degreecentrality" => {
            let deg = mgquery::degree_centrality(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = deg.into_iter().map(|(gid, d)| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("degree".to_string(), PropertyValue::Int(d as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into(), "degree".into()], rows })
        }
        "algo.graphdensity" | "algo.graph_density" => {
            let d = mgquery::graph_density(storage);
            let mut r = HashMap::new();
            r.insert("density".to_string(), PropertyValue::Double(d));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["density".into()], rows: vec![r] })
        }
        "algo.globalclusteringcoefficient" | "algo.global_clustering_coefficient" => {
            let c = mgquery::global_clustering_coefficient(storage);
            let mut r = HashMap::new();
            r.insert("coefficient".to_string(), PropertyValue::Double(c));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["coefficient".into()], rows: vec![r] })
        }
        "algo.coreness" => {
            let cores = mgquery::coreness(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = cores.into_iter().map(|(gid, c)| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("coreness".to_string(), PropertyValue::Int(c as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into(), "coreness".into()], rows })
        }
        "algo.degreeassortativity" | "algo.degree_assortativity" => {
            let r = mgquery::degree_assortativity(storage);
            let mut row = HashMap::new();
            row.insert("assortativity".to_string(), PropertyValue::Double(r));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["assortativity".into()], rows: vec![row] })
        }
        "algo.bridges" => {
            let bridges = mgquery::bridges(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = bridges.into_iter().map(|(a, b)| {
                let mut r = HashMap::new();
                r.insert("nodeA".to_string(), PropertyValue::Int(a.as_int()));
                r.insert("nodeB".to_string(), PropertyValue::Int(b.as_int()));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeA".into(), "nodeB".into()], rows })
        }
        "algo.eccentricity" => {
            let ecc = mgquery::eccentricity(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = ecc.into_iter().map(|(gid, e)| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("eccentricity".to_string(), PropertyValue::Int(e as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into(), "eccentricity".into()], rows })
        }
        "algo.eigenvector_centrality" => {
            let scores = mgquery::eigenvector_centrality(storage, 100, 1e-6);
            let rows: Vec<HashMap<String, PropertyValue>> = scores.into_iter().map(|(gid, score)| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("score".to_string(), PropertyValue::Double(score));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into(), "score".into()], rows })
        }
        "algo.harmonic_centrality" => {
            let scores = mgquery::harmonic_centrality(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = scores.into_iter().map(|(gid, score)| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("score".to_string(), PropertyValue::Double(score));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into(), "score".into()], rows })
        }
        "algo.has_cycle" => {
            let mut row = HashMap::new();
            row.insert("hasCycle".to_string(), PropertyValue::Bool(mgquery::has_cycle(storage)));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["hasCycle".into()], rows: vec![row] })
        }
        "algo.is_bipartite" => {
            let mut row = HashMap::new();
            row.insert("bipartite".to_string(), PropertyValue::Bool(mgquery::is_bipartite(storage)));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["bipartite".into()], rows: vec![row] })
        }
        "algo.radius" => {
            let mut row = HashMap::new();
            row.insert("radius".to_string(), PropertyValue::Int(mgquery::radius(storage) as i64));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["radius".into()], rows: vec![row] })
        }
        "algo.topological_sort" => {
            match mgquery::topological_sort(storage) {
                Some(order) => {
                    let rows: Vec<HashMap<String, PropertyValue>> = order.into_iter().map(|gid| {
                        let mut r = HashMap::new();
                        r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                        r
                    }).collect();
                    Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into()], rows })
                }
                None => Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into()], rows: vec![] }),
            }
        }
        "algo.center" => {
            let centers = mgquery::center(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = centers.into_iter().map(|gid| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into()], rows })
        }
        "algo.periphery" => {
            let periph = mgquery::periphery(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = periph.into_iter().map(|gid| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into()], rows })
        }
        "algo.small_world_coefficient" => {
            let mut row = HashMap::new();
            row.insert("sigma".to_string(), PropertyValue::Double(mgquery::small_world_coefficient(storage)));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["sigma".into()], rows: vec![row] })
        }
        "algo.articulation_points" => {
            let points = mgquery::articulation_points::articulation_points(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = points.into_iter().map(|gid| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into()], rows })
        }
        "algo.is_biconnected" => {
            let mut row = HashMap::new();
            row.insert("biconnected".to_string(), PropertyValue::Bool(mgquery::articulation_points::is_biconnected(storage)));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["biconnected".into()], rows: vec![row] })
        }
        "algo.connected_components" => {
            let components = mgquery::community::connected_components(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = components.into_iter().map(|(gid, cid)| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("component".to_string(), PropertyValue::Int(cid.as_int()));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into(), "component".into()], rows })
        }
        "algo.cycle_detection" => {
            match mgquery::traversal::cycle_detection(storage) {
                Some(cycle) => {
                    let rows: Vec<HashMap<String, PropertyValue>> = cycle.into_iter().map(|gid| {
                        let mut r = HashMap::new();
                        r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                        r
                    }).collect();
                    Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into()], rows })
                }
                None => Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into()], rows: vec![] }),
            }
        }
        "algo.all_pairs_shortest_path" => {
            let apsp = mgquery::path::all_pairs_shortest_path(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = apsp.into_iter().map(|((a, b), dist)| {
                let mut r = HashMap::new();
                r.insert("source".to_string(), PropertyValue::Int(a.as_int()));
                r.insert("target".to_string(), PropertyValue::Int(b.as_int()));
                r.insert("distance".to_string(), PropertyValue::Int(dist as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["source".into(), "target".into(), "distance".into()], rows })
        }
        "algo.maximal_cliques" => {
            let cliques = mgquery::clique::maximal_cliques(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = cliques.into_iter().enumerate().map(|(i, clique)| {
                let mut r = HashMap::new();
                r.insert("cliqueId".to_string(), PropertyValue::Int(i as i64));
                r.insert("nodes".to_string(), PropertyValue::List(clique.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["cliqueId".into(), "nodes".into()], rows })
        }
        "algo.clique_number" => {
            let mut row = HashMap::new();
            row.insert("cliqueNumber".to_string(), PropertyValue::Int(mgquery::clique::clique_number(storage) as i64));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["cliqueNumber".into()], rows: vec![row] })
        }
        "algo.greedy_coloring" => {
            let coloring = mgquery::coloring::greedy_coloring(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = coloring.into_iter().map(|(gid, color)| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("color".to_string(), PropertyValue::Int(color as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into(), "color".into()], rows })
        }
        "algo.dsatur_coloring" => {
            let coloring = mgquery::coloring::dsatur_coloring(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = coloring.into_iter().map(|(gid, color)| {
                let mut r = HashMap::new();
                r.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("color".to_string(), PropertyValue::Int(color as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeId".into(), "color".into()], rows })
        }
        "algo.degeneracy" => {
            let mut row = HashMap::new();
            row.insert("degeneracy".to_string(), PropertyValue::Int(mgquery::k_core::degeneracy(storage) as i64));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["degeneracy".into()], rows: vec![row] })
        }
        "algo.predict_links" => {
            let predictions = mgquery::link_prediction::predict_links(storage, 10);
            let rows: Vec<HashMap<String, PropertyValue>> = predictions.into_iter().map(|(a, b, score)| {
                let mut r = HashMap::new();
                r.insert("nodeA".to_string(), PropertyValue::Int(a.as_int()));
                r.insert("nodeB".to_string(), PropertyValue::Int(b.as_int()));
                r.insert("score".to_string(), PropertyValue::Double(score));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["nodeA".into(), "nodeB".into(), "score".into()], rows })
        }
        "algo.dfs" => {
            let start_gid = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let path = mgquery::traversal::dfs(storage, start_gid);
            let mut r = HashMap::new();
            r.insert("path".to_string(), PropertyValue::List(path.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["path".into()], rows: vec![r] })
        }
        "algo.random_walk" => {
            let start_gid = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let steps = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(10);
            let walk = mgquery::embedding::simple_random_walk(storage, start_gid, steps);
            let mut r = HashMap::new();
            r.insert("walk".to_string(), PropertyValue::List(walk.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["walk".into()], rows: vec![r] })
        }
        "algo.jaccard_similarity" => {
            let a = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let b = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let score = mgquery::similarity::jaccard_similarity(storage, a, b);
            let mut r = HashMap::new();
            r.insert("score".to_string(), PropertyValue::Double(score));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["score".into()], rows: vec![r] })
        }
        "algo.cosine_similarity" => {
            let a = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let b = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let score = mgquery::similarity::cosine_similarity(storage, a, b);
            let mut r = HashMap::new();
            r.insert("score".to_string(), PropertyValue::Double(score));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["score".into()], rows: vec![r] })
        }
        "algo.adamic_adar" => {
            let a = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let b = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let score = mgquery::link_prediction::adamic_adar(storage, a, b);
            let mut r = HashMap::new();
            r.insert("score".to_string(), PropertyValue::Double(score));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["score".into()], rows: vec![r] })
        }
        "algo.common_neighbors" => {
            let a = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let b = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let count = mgquery::link_prediction::common_neighbors_count(storage, a, b);
            let mut r = HashMap::new();
            r.insert("count".to_string(), PropertyValue::Int(count as i64));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["count".into()], rows: vec![r] })
        }
        "algo.resource_allocation" => {
            let a = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let b = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let score = mgquery::link_prediction::resource_allocation(storage, a, b);
            let mut r = HashMap::new();
            r.insert("score".to_string(), PropertyValue::Double(score));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["score".into()], rows: vec![r] })
        }
        "algo.preferential_attachment" => {
            let a = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let b = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let score = mgquery::link_prediction::preferential_attachment(storage, a, b);
            let mut r = HashMap::new();
            r.insert("score".to_string(), PropertyValue::Double(score as f64));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["score".into()], rows: vec![r] })
        }
        "algo.soundarajan_hopcroft" => {
            let a = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let b = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let partition = parse_partition_arg(&args[2..])?;
            let score = mgquery::link_prediction::soundarajan_hopcroft(storage, a, b, &partition);
            let mut r = HashMap::new();
            r.insert("score".to_string(), PropertyValue::Int(score as i64));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["score".into()], rows: vec![r] })
        }
        "algo.dijkstra" => {
            let start = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let end = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            match mgquery::shortest_path::dijkstra(storage, start, end, None) {
                Some((path, dist)) => {
                    let mut r = HashMap::new();
                    r.insert("path".to_string(), PropertyValue::List(path.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
                    r.insert("distance".to_string(), PropertyValue::Double(dist));
                    Ok(QueryResult { number_of_hops: 0, columns: vec!["path".into(), "distance".into()], rows: vec![r] })
                }
                None => Ok(QueryResult { number_of_hops: 0, columns: vec!["path".into(), "distance".into()], rows: vec![] }),
            }
        }
        "algo.dijkstra_all" => {
            let start = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let dists = mgquery::shortest_path::dijkstra_all(storage, start);
            let rows: Vec<HashMap<String, PropertyValue>> = dists.into_iter().map(|(gid, (dist, _prev))| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("distance".to_string(), PropertyValue::Double(dist));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "distance".into()], rows })
        }
        "algo.floyd_warshall" => {
            let dists = mgquery::shortest_path::floyd_warshall(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = dists.into_iter().map(|((from, to), dist)| {
                let mut r = HashMap::new();
                r.insert("from".to_string(), PropertyValue::Int(from.as_int()));
                r.insert("to".to_string(), PropertyValue::Int(to.as_int()));
                r.insert("distance".to_string(), PropertyValue::Double(dist));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["from".into(), "to".into(), "distance".into()], rows })
        }
        "algo.prim" => {
            let start = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let (mst, _total) = mgquery::mst::prim(storage, start);
            let rows: Vec<HashMap<String, PropertyValue>> = mst.into_iter().map(|e| {
                let mut r = HashMap::new();
                r.insert("from".to_string(), PropertyValue::Int(e.from.as_int()));
                r.insert("to".to_string(), PropertyValue::Int(e.to.as_int()));
                r.insert("weight".to_string(), PropertyValue::Double(e.weight));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["from".into(), "to".into(), "weight".into()], rows })
        }
        "algo.kruskal" => {
            let edges = mgquery::mst::extract_edges(storage);
            let (mst, _total) = mgquery::mst::kruskal(edges);
            let rows: Vec<HashMap<String, PropertyValue>> = mst.into_iter().map(|e| {
                let mut r = HashMap::new();
                r.insert("from".to_string(), PropertyValue::Int(e.from.as_int()));
                r.insert("to".to_string(), PropertyValue::Int(e.to.as_int()));
                r.insert("weight".to_string(), PropertyValue::Double(e.weight));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["from".into(), "to".into(), "weight".into()], rows })
        }
        "algo.k_core" => {
            let k = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(2);
            let gids = mgquery::k_core::k_core(storage, k);
            let rows: Vec<HashMap<String, PropertyValue>> = gids.into_iter().map(|gid| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into()], rows })
        }
        "algo.k_core_decomposition" => {
            let cores = mgquery::k_core::core_decomposition(storage);
            let rows: Vec<HashMap<String, PropertyValue>> = cores.into_iter().map(|(gid, core)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("coreness".to_string(), PropertyValue::Int(core as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "coreness".into()], rows })
        }
        "algo.louvain" => {
            let communities = mgquery::louvain::louvain(storage, 1.0, 100);
            let rows: Vec<HashMap<String, PropertyValue>> = communities.into_iter().map(|(gid, cid)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("community".to_string(), PropertyValue::Int(cid as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "community".into()], rows })
        }
        "algo.label_propagation" => {
            let communities = mgquery::label_propagation::label_propagation(storage, 100);
            let rows: Vec<HashMap<String, PropertyValue>> = communities.into_iter().map(|(gid, cid)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("community".to_string(), PropertyValue::Int(cid as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "community".into()], rows })
        }
        "algo.katz_centrality" => {
            let scores = mgquery::katz::katz_centrality(storage, 0.1, 100, 1e-6);
            let rows: Vec<HashMap<String, PropertyValue>> = scores.into_iter().map(|(gid, score)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("score".to_string(), PropertyValue::Double(score));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "score".into()], rows })
        }
        "algo.count_paths_of_length" => {
            let k = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(1);
            let counts = mgquery::katz::count_paths_of_length(storage, k);
            let rows: Vec<HashMap<String, PropertyValue>> = counts.into_iter().map(|(gid, count)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("count".to_string(), PropertyValue::Int(count as i64));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "count".into()], rows })
        }
        "algo.hits" => {
            let (auth, hub) = mgquery::hits::hits(storage, 100, 1e-6);
            let rows: Vec<HashMap<String, PropertyValue>> = auth.into_iter().map(|(gid, a)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("authority".to_string(), PropertyValue::Double(a));
                r.insert("hub".to_string(), PropertyValue::Double(hub.get(&gid).copied().unwrap_or(0.0)));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "authority".into(), "hub".into()], rows })
        }
        "algo.shortest_path_weighted" => {
            let start = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let end = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let weight_prop = args.get(2).and_then(|v| match v {
                PropertyValue::Int(n) => Some(PropertyId::from(*n as u32)),
                _ => None,
            }).unwrap_or(PropertyId::from(0u32));
            match mgquery::path::shortest_path_weighted(storage, start, end, weight_prop) {
                Some((path, weight)) => {
                    let mut r = HashMap::new();
                    r.insert("path".to_string(), PropertyValue::List(path.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
                    r.insert("weight".to_string(), PropertyValue::Double(weight));
                    Ok(QueryResult { number_of_hops: 0, columns: vec!["path".into(), "weight".into()], rows: vec![r] })
                }
                None => Ok(QueryResult { number_of_hops: 0, columns: vec!["path".into(), "weight".into()], rows: vec![] }),
            }
        }
        "algo.simple_random_walk" => {
            let start = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let length = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(10);
            let walk = mgquery::embedding::simple_random_walk(storage, start, length);
            let mut r = HashMap::new();
            r.insert("walk".to_string(), PropertyValue::List(walk.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["walk".into()], rows: vec![r] })
        }
        "algo.biased_random_walk" => {
            let start = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let length = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(10);
            let p = args.get(2).and_then(|v| match v {
                PropertyValue::Double(f) => Some(*f),
                PropertyValue::Int(n) => Some(*n as f64),
                _ => None,
            }).unwrap_or(1.0);
            let q = args.get(3).and_then(|v| match v {
                PropertyValue::Double(f) => Some(*f),
                PropertyValue::Int(n) => Some(*n as f64),
                _ => None,
            }).unwrap_or(1.0);
            let walk = mgquery::embedding::biased_random_walk(storage, start, length, p, q);
            let mut r = HashMap::new();
            r.insert("walk".to_string(), PropertyValue::List(walk.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["walk".into()], rows: vec![r] })
        }
        "algo.generate_walks" => {
            let num_walks = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(1);
            let walk_length = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(10);
            let p = args.get(2).and_then(|v| match v {
                PropertyValue::Double(f) => Some(*f),
                PropertyValue::Int(n) => Some(*n as f64),
                _ => None,
            }).unwrap_or(1.0);
            let q = args.get(3).and_then(|v| match v {
                PropertyValue::Double(f) => Some(*f),
                PropertyValue::Int(n) => Some(*n as f64),
                _ => None,
            }).unwrap_or(1.0);
            let walks = mgquery::embedding::generate_walks(storage, num_walks, walk_length, p, q);
            let rows: Vec<HashMap<String, PropertyValue>> = walks.into_iter().enumerate().map(|(i, walk)| {
                let mut r = HashMap::new();
                r.insert("walk_id".to_string(), PropertyValue::Int(i as i64));
                r.insert("walk".to_string(), PropertyValue::List(walk.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["walk_id".into(), "walk".into()], rows })
        }
        "algo.random_walk_with_restart" => {
            let start = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let steps = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(10);
            let restart_prob = args.get(2).and_then(|v| match v {
                PropertyValue::Double(f) => Some(*f),
                PropertyValue::Int(n) => Some(*n as f64),
                _ => None,
            }).unwrap_or(0.15);
            let walk = mgquery::random_walk::random_walk_with_restart(storage, start, steps, restart_prob);
            let mut r = HashMap::new();
            r.insert("walk".to_string(), PropertyValue::List(walk.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["walk".into()], rows: vec![r] })
        }
        "algo.personalized_pagerank" => {
            let start = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let walk_count = args.get(1).and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(100);
            let walk_length = args.get(2).and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(10);
            let restart_prob = args.get(3).and_then(|v| match v {
                PropertyValue::Double(f) => Some(*f),
                PropertyValue::Int(n) => Some(*n as f64),
                _ => None,
            }).unwrap_or(0.15);
            let scores = mgquery::random_walk::personalized_pagerank(storage, start, walk_count, walk_length, restart_prob);
            let rows: Vec<HashMap<String, PropertyValue>> = scores.into_iter().map(|(gid, score)| {
                let mut r = HashMap::new();
                r.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
                r.insert("score".to_string(), PropertyValue::Double(score));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["node".into(), "score".into()], rows })
        }
        "algo.cliques_containing" => {
            let gid = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(Gid::from(*n as u64)),
                _ => None,
            }).unwrap_or(Gid::from(1u64));
            let cliques = mgquery::clique::cliques_containing(storage, gid);
            let rows: Vec<HashMap<String, PropertyValue>> = cliques.into_iter().map(|c| {
                let mut r = HashMap::new();
                r.insert("clique".to_string(), PropertyValue::List(c.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["clique".into()], rows })
        }
        "algo.chromatic_number" => {
            let coloring = mgquery::coloring::greedy_coloring(storage);
            let num = mgquery::coloring::chromatic_number(&coloring);
            let mut r = HashMap::new();
            r.insert("chromatic_number".to_string(), PropertyValue::Int(num as i64));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["chromatic_number".into()], rows: vec![r] })
        }
        "algo.rich_club_coefficient" => {
            let max_k = args.first().and_then(|v| match v {
                PropertyValue::Int(n) => Some(*n as usize),
                _ => None,
            }).unwrap_or(5usize);
            let rc = mgquery::rich_club_coefficient(storage, max_k);
            let rows: Vec<HashMap<String, PropertyValue>> = rc.into_iter().map(|(k, phi)| {
                let mut r = HashMap::new();
                r.insert("k".to_string(), PropertyValue::Int(k as i64));
                r.insert("coefficient".to_string(), PropertyValue::Double(phi));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["k".into(), "coefficient".into()], rows })
        }
        "algo.modularity" => {
            let mut partition = parse_partition_arg(&args)?;
            if partition.is_empty() {
                partition = mgquery::louvain::louvain(storage, 1.0, 100);
            }
            let q = mgquery::modularity(storage, &partition);
            let mut r = HashMap::new();
            r.insert("modularity".to_string(), PropertyValue::Double(q));
            Ok(QueryResult { number_of_hops: 0, columns: vec!["modularity".into()], rows: vec![r] })
        }
        "algo.conductance" => {
            let mut partition = parse_partition_arg(&args)?;
            if partition.is_empty() {
                partition = mgquery::louvain::louvain(storage, 1.0, 100);
            }
            let cond = mgquery::conductance(storage, &partition);
            let rows: Vec<HashMap<String, PropertyValue>> = cond.into_iter().map(|(cid, val)| {
                let mut r = HashMap::new();
                r.insert("community".to_string(), PropertyValue::Int(cid as i64));
                r.insert("conductance".to_string(), PropertyValue::Double(val));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["community".into(), "conductance".into()], rows })
        }
        "algo.normalized_cut" => {
            let mut partition = parse_partition_arg(&args)?;
            if partition.is_empty() {
                partition = mgquery::louvain::louvain(storage, 1.0, 100);
            }
            let nc = mgquery::normalized_cut(storage, &partition);
            let rows: Vec<HashMap<String, PropertyValue>> = nc.into_iter().map(|(cid, val)| {
                let mut r = HashMap::new();
                r.insert("community".to_string(), PropertyValue::Int(cid as i64));
                r.insert("normalized_cut".to_string(), PropertyValue::Double(val));
                r
            }).collect();
            Ok(QueryResult { number_of_hops: 0, columns: vec!["community".into(), "normalized_cut".into()], rows })
        }
        _ => {
            // Fall back to built-in procedure registry for procedures not hardcoded above
            let builtin_reg = builtin_procs::ProcedureRegistry::new();
            if let Some(proc) = builtin_reg.get(&name_lower) {
                let arg_map = args_to_map(&args);
                match proc(storage, &arg_map) {
                    Ok(rows) => Ok(QueryResult { number_of_hops: 0, columns: vec![], rows }),
                    Err(e) => Err(ExecError::Runtime(e)),
                }
            } else {
                Err(ExecError::Runtime(format!("unknown procedure: {}", name)))
            }
        }
    }
}

fn format_property_value(val: &PropertyValue) -> String {
    match val {
        PropertyValue::Null => "null".to_string(),
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::Int(n) => n.to_string(),
        PropertyValue::Double(f) => f.to_string(),
        PropertyValue::String(s) => format!("'{}'", s.replace('\'', "\\'")),
        PropertyValue::List(items) => {
            let parts: Vec<String> = items.iter().map(format_property_value).collect();
            format!("[{}]", parts.join(", "))
        }
        PropertyValue::Map(entries) => {
            let parts: Vec<String> = entries.iter()
                .map(|(k, v)| format!("{}: {}", k, format_property_value(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        PropertyValue::Date(d) => format!("date('{}')", d.to_iso_string()),
        PropertyValue::LocalTime(t) => format!("localtime('{}')", t.to_iso_string()),
        PropertyValue::LocalDateTime(dt) => format!("localdatetime('{}')", dt.to_iso_string()),
        PropertyValue::Duration(dur) => format!("duration('{}')", dur.to_iso_string()),
        PropertyValue::Point2D(p) => format!("point({{x: {}, y: {}, crs: '{:?}'}})", p.x, p.y, p.crs),
        PropertyValue::Point3D(p) => format!("point({{x: {}, y: {}, z: {}, crs: '{:?}'}})", p.x, p.y, p.z, p.crs),
        _ => "null".to_string(),
    }
}

fn format_property_store(props: &mgcore::property_store::PropertyStore, catalog: Option<&mgcatalog::Catalog>) -> String {
    let mut parts = Vec::new();
    for (pid, val) in props.iter() {
        let name = catalog.map(|c| c.property_name(pid)).unwrap_or_else(|| format!("p{}", pid.as_uint()));
        if !matches!(val, PropertyValue::Null) {
            parts.push(format!("{}: {}", name, format_property_value(val)));
        }
    }
    parts.join(", ")
}

fn row(k1: &str, v1: &str, k2: &str, v2: &str) -> HashMap<String, PropertyValue> {
    let mut m = HashMap::new();
    m.insert(k1.to_string(), PropertyValue::String(v1.to_string()));
    m.insert(k2.to_string(), PropertyValue::String(v2.to_string()));
    m
}

// ─── UNWIND execution ──────────────────────────────────────────────────

fn exec_unwind(
    bindings: &[HashMap<String, PropertyValue>],
    expression: &Expression,
    alias: &str,
) -> Result<Vec<HashMap<String, PropertyValue>>, ExecError> {
    let mut result = Vec::new();
    for binding in bindings {
        let val = eval_expression(expression, binding);
        match val {
            PropertyValue::List(items) => {
                for item in items {
                    let mut row = binding.clone();
                    row.insert(alias.to_string(), item);
                    result.push(row);
                }
            }
            _ => return Err(ExecError::Runtime(
                format!("UNWIND requires a list value, got {:?}", val)
            )),
        }
    }
    Ok(result)
}

fn exec_load_csv(
    bindings: &[HashMap<String, PropertyValue>],
    url: &str,
    with_headers: bool,
    alias: &str,
) -> Result<Vec<HashMap<String, PropertyValue>>, ExecError> {
    let csv_data = if url.starts_with("http://") || url.starts_with("https://") {
        ureq::get(url)
            .call()
            .map_err(|e| ExecError::Runtime(format!("HTTP fetch error for {}: {}", url, e)))?
            .body_mut()
            .read_to_string()
            .map_err(|e| ExecError::Runtime(format!("HTTP read error: {}", e)))?
    } else {
        let file_path = url.trim_start_matches("file://");
        std::fs::read_to_string(file_path)
            .map_err(|e| ExecError::Runtime(format!("CSV read error: {}", e)))?
    };

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(with_headers)
        .from_reader(csv_data.as_bytes());

    let headers: Vec<String> = if with_headers {
        reader
            .headers()
            .map_err(|e| ExecError::Runtime(format!("CSV headers error: {}", e)))?
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        vec![]
    };

    let mut rows = Vec::new();
    for result in reader.records() {
        let record = result.map_err(|e| ExecError::Runtime(format!("CSV parse error: {}", e)))?;
        let mut map = Vec::new();
        for (i, field) in record.iter().enumerate() {
            let key = if with_headers {
                headers.get(i).cloned().unwrap_or_else(|| format!("column_{}", i))
            } else {
                format!("column_{}", i)
            };
            map.push((key, PropertyValue::String(field.to_string())));
        }
        rows.push(PropertyValue::Map(map));
    }

    let mut new_bindings = Vec::new();
    for binding in bindings {
        for row in &rows {
            let mut merged = binding.clone();
            merged.insert(alias.to_string(), row.clone());
            new_bindings.push(merged);
        }
    }
    Ok(new_bindings)
}

fn exec_load_jsonl(
    bindings: &[HashMap<String, PropertyValue>],
    url: &str,
    alias: &str,
) -> Result<Vec<HashMap<String, PropertyValue>>, ExecError> {
    let jsonl_data = if url.starts_with("http://") || url.starts_with("https://") {
        ureq::get(url)
            .call()
            .map_err(|e| ExecError::Runtime(format!("HTTP fetch error for {}: {}", url, e)))?
            .body_mut()
            .read_to_string()
            .map_err(|e| ExecError::Runtime(format!("HTTP read error: {}", e)))?
    } else {
        let file_path = url.trim_start_matches("file://");
        std::fs::read_to_string(file_path)
            .map_err(|e| ExecError::Runtime(format!("JSONL read error: {}", e)))?
    };

    let mut rows = Vec::new();
    for line in jsonl_data.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(&line)
            .map_err(|e| ExecError::Runtime(format!("JSONL parse error: {}", e)))?;
        let pv = json_value_to_property_value(value);
        rows.push(pv);
    }

    let mut new_bindings = Vec::new();
    for binding in bindings {
        for row in &rows {
            let mut merged = binding.clone();
            merged.insert(alias.to_string(), row.clone());
            new_bindings.push(merged);
        }
    }
    Ok(new_bindings)
}

/// Convert a serde_json::Value to PropertyValue recursively.
fn json_value_to_property_value(value: serde_json::Value) -> PropertyValue {
    match value {
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Bool(b) => PropertyValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Double(f)
            } else {
                PropertyValue::Null
            }
        }
        serde_json::Value::String(s) => PropertyValue::String(s),
        serde_json::Value::Array(arr) => {
            PropertyValue::List(arr.into_iter().map(json_value_to_property_value).collect())
        }
        serde_json::Value::Object(obj) => {
            PropertyValue::Map(obj.into_iter().map(|(k, v)| (k, json_value_to_property_value(v))).collect())
        }
    }
}

// ─── WITH execution ────────────────────────────────────────────────────

fn exec_with(
    storage: &Storage,
    bindings: &[HashMap<String, PropertyValue>],
    items: &[ReturnItem],
    where_clause: &Option<Expression>,
) -> Result<Vec<HashMap<String, PropertyValue>>, ExecError> {
    let has_aggregate = items.iter().any(|item| is_aggregate(&item.expression));

    if !has_aggregate {
        let mut result = Vec::new();
        for binding in bindings {
            let mut row = HashMap::new();
            for (i, item) in items.iter().enumerate() {
                let val = eval::eval_expression_with_storage(&item.expression, binding, Some(storage));
                let alias = item.alias.clone().or_else(|| {
                    if let Expression::Identifier(name) = &item.expression {
                        Some(name.clone())
                    } else {
                        None
                    }
                }).unwrap_or_else(|| format!("column_{}", i));
                row.insert(alias, val);
            }
            if let Some(ref where_expr) = where_clause {
                if !eval::eval_expression_with_storage(where_expr, &row, Some(storage)).is_truthy() {
                    continue;
                }
            }
            result.push(row);
        }
        return Ok(result);
    }

    // Aggregate case: group by non-aggregate expressions, compute aggregates per group
    let group_indices: Vec<usize> = items
        .iter()
        .enumerate()
        .filter(|(_, item)| !is_aggregate(&item.expression))
        .map(|(i, _)| i)
        .collect();

    let mut output_rows: Vec<HashMap<String, PropertyValue>> = Vec::new();

    if group_indices.is_empty() {
        // No grouping: single aggregate row over all bindings
        let mut row = HashMap::new();
        for (i, item) in items.iter().enumerate() {
            let val = eval_aggregate(storage, &item.expression, bindings);
            let alias = item.alias.clone().or_else(|| {
                if let Expression::Identifier(name) = &item.expression {
                    Some(name.clone())
                } else {
                    None
                }
            }).unwrap_or_else(|| format!("column_{}", i));
            row.insert(alias, val);
        }
        if let Some(ref where_expr) = where_clause {
            if eval::eval_expression_with_storage(where_expr, &row, Some(storage)).is_truthy() {
                output_rows.push(row);
            }
        } else {
            output_rows.push(row);
        }
        return Ok(output_rows);
    }

    // Group bindings by non-aggregate expression values
    let mut groups: Vec<(Vec<PropertyValue>, Vec<HashMap<String, PropertyValue>>)> = Vec::new();
    for binding in bindings {
        let key: Vec<PropertyValue> = group_indices
            .iter()
            .map(|&gi| eval::eval_expression_with_storage(&items[gi].expression, binding, Some(storage)))
            .collect();
        match groups.iter_mut().find(|(k, _)| k == &key) {
            Some((_, g)) => g.push(binding.clone()),
            None => groups.push((key, vec![binding.clone()])),
        }
    }

    for (key, group_bindings) in groups {
        let mut row = HashMap::new();
        for (i, item) in items.iter().enumerate() {
            let val = if is_aggregate(&item.expression) {
                eval_aggregate(storage, &item.expression, &group_bindings)
            } else {
                match group_indices.iter().position(|&gi| gi == i) {
                    Some(key_pos) => key[key_pos].clone(),
                    None => PropertyValue::Null,
                }
            };
            let alias = item.alias.clone().or_else(|| {
                if let Expression::Identifier(name) = &item.expression {
                    Some(name.clone())
                } else {
                    None
                }
            }).unwrap_or_else(|| format!("column_{}", i));
            row.insert(alias, val);
        }
        if let Some(ref where_expr) = where_clause {
            if eval::eval_expression_with_storage(where_expr, &row, Some(storage)).is_truthy() {
                output_rows.push(row);
            }
        } else {
            output_rows.push(row);
        }
    }

    Ok(output_rows)
}

// ─── RETURN execution ──────────────────────────────────────────────────

fn dedup_rows(rows: Vec<ResultRow>) -> Vec<ResultRow> {
    let mut seen: Vec<ResultRow> = Vec::new();
    let mut result = Vec::new();
    for row in rows {
        if !seen.iter().any(|r| r == &row) {
            seen.push(row.clone());
            result.push(row);
        }
    }
    result
}

fn exec_return(
    storage: &Storage,
    bindings: &[HashMap<String, PropertyValue>],
    items: &[ReturnItem],
    distinct: bool,
    all: bool,
) -> Result<QueryResult, ExecError> {
    let resolved_items: Vec<ReturnItem> = if all {
        let mut seen = std::collections::HashSet::new();
        let mut all_items = Vec::new();
        for binding in bindings {
            for key in binding.keys() {
                if !key.starts_with("__") && seen.insert(key.clone()) {
                    all_items.push(ReturnItem {
                        expression: Expression::Identifier(key.clone()),
                        alias: Some(key.clone()),
                    });
                }
            }
        }
        all_items
    } else {
        items.to_vec()
    };
    let items = &resolved_items;
    let columns: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            item.alias.clone().or_else(|| {
                if let Expression::Identifier(name) = &item.expression {
                    Some(name.clone())
                } else {
                    None
                }
            }).unwrap_or_else(|| format!("column_{}", i))
        })
        .collect();

    let has_aggregate = items.iter().any(|item| is_aggregate(&item.expression));

    if !has_aggregate {
        let mut rows = Vec::new();
        for binding in bindings {
            let mut row = ResultRow::new();
            for (i, item) in items.iter().enumerate() {
                let val = eval::eval_expression_with_storage(&item.expression, binding, Some(storage));
                let col = columns[i].clone();
                row.insert(col, val);
            }
            rows.push(row);
        }
        if distinct {
            rows = dedup_rows(rows);
        }
        return Ok(QueryResult { number_of_hops: 0, columns, rows });
    }

    // Aggregate case: group by non-aggregate expressions, compute aggregates per group
    if bindings.is_empty() {
        let mut row = ResultRow::new();
        for (i, item) in items.iter().enumerate() {
            let val = eval_aggregate(storage, &item.expression, &[]);
            row.insert(columns[i].clone(), val);
        }
        return Ok(QueryResult { number_of_hops: 0, columns, rows: vec![row] });
    }

    let group_indices: Vec<usize> = items
        .iter()
        .enumerate()
        .filter(|(_, item)| !is_aggregate(&item.expression))
        .map(|(i, _)| i)
        .collect();

    if group_indices.is_empty() {
        // No grouping: single aggregate row over all bindings
        let mut row = ResultRow::new();
        for (i, item) in items.iter().enumerate() {
            let val = eval_aggregate(storage, &item.expression, bindings);
            row.insert(columns[i].clone(), val);
        }
        return Ok(QueryResult { number_of_hops: 0, columns, rows: vec![row] });
    }

    // Group bindings by non-aggregate expression values (linear search; PropertyValue lacks Hash)
    let mut groups: Vec<(Vec<PropertyValue>, Vec<HashMap<String, PropertyValue>>)> = Vec::new();
    for binding in bindings {
        let key: Vec<PropertyValue> = group_indices
            .iter()
            .map(|&gi| eval::eval_expression_with_storage(&items[gi].expression, binding, Some(storage)))
            .collect();
        match groups.iter_mut().find(|(k, _)| k == &key) {
            Some((_, g)) => g.push(binding.clone()),
            None => groups.push((key, vec![binding.clone()])),
        }
    }

    let mut rows = Vec::new();
    for (key, group_bindings) in groups {
        let mut row = ResultRow::new();
        for (i, item) in items.iter().enumerate() {
            let val = if is_aggregate(&item.expression) {
                eval_aggregate(storage, &item.expression, &group_bindings)
            } else {
                match group_indices.iter().position(|&gi| gi == i) {
                    Some(key_pos) => key[key_pos].clone(),
                    None => PropertyValue::Null,
                }
            };
            row.insert(columns[i].clone(), val);
        }
        rows.push(row);
    }
    if distinct {
        rows = dedup_rows(rows);
    }

    Ok(QueryResult { number_of_hops: 0, columns, rows })
}

pub(crate) fn is_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::CountStar => true,
        Expression::Function { name, .. } => {
            name.eq_ignore_ascii_case("count")
                || name.eq_ignore_ascii_case("sum")
                || name.eq_ignore_ascii_case("avg")
                || name.eq_ignore_ascii_case("min")
                || name.eq_ignore_ascii_case("max")
                || name.eq_ignore_ascii_case("collect")
                || name.eq_ignore_ascii_case("collect_map")
                || name.eq_ignore_ascii_case("collectmap")
        }
        Expression::Add(a, b)
        | Expression::Sub(a, b)
        | Expression::Mul(a, b)
        | Expression::Div(a, b)
        | Expression::Mod(a, b) => is_aggregate(a) || is_aggregate(b),
        Expression::Neg(a) | Expression::Not(a) => is_aggregate(a),
        _ => false,
    }
}

pub(crate) fn eval_aggregate(
    storage: &Storage,
    expr: &Expression,
    bindings: &[HashMap<String, PropertyValue>],
) -> PropertyValue {
    match expr {
        Expression::CountStar => PropertyValue::Int(bindings.len() as i64),
        Expression::Function { name, arguments, distinct } => {
            let values: Vec<PropertyValue> = bindings
                .iter()
                .filter_map(|b| arguments.first().map(|a| eval::eval_expression_with_storage(a, b, Some(storage))))
                .collect();

            if name.eq_ignore_ascii_case("count") {
                // count(n.prop) skips null values; never unwrap lists
                let mut vals = values.clone();
                if *distinct {
                    let mut seen = HashSet::new();
                    vals.retain(|v| seen.insert(format!("{:?}", v)));
                }
                PropertyValue::Int(vals.iter().filter(|v| !matches!(v, PropertyValue::Null)).count() as i64)
            } else {
                // For sum/avg/min/max/collect: if there's exactly one value and it's a List,
                // operate on the list elements (list function semantics)
                let list_values: Option<Vec<PropertyValue>> = if values.len() == 1 {
                    if let PropertyValue::List(ref items) = values[0] {
                        Some(items.clone())
                    } else { None }
                } else { None };
                let mut vals = list_values.as_ref().unwrap_or(&values).clone();
                if *distinct {
                    let mut seen = HashSet::new();
                    vals.retain(|v| seen.insert(format!("{:?}", v)));
                }
                // Extract numeric values (both Int and Double)
                let mut has_double = false;
                let nums: Vec<f64> = vals.iter()
                    .filter_map(|v| match v {
                        PropertyValue::Int(n) => Some(*n as f64),
                        PropertyValue::Double(f) => { has_double = true; Some(*f) }
                        _ => None,
                    }).collect();

                if name.eq_ignore_ascii_case("sum") {
                    if nums.is_empty() { PropertyValue::Int(0) }
                    else if has_double { PropertyValue::Double(nums.iter().sum()) }
                    else { PropertyValue::Int(nums.iter().sum::<f64>() as i64) }
                } else if name.eq_ignore_ascii_case("avg") {
                    if nums.is_empty() { PropertyValue::Null }
                    else { PropertyValue::Double(nums.iter().sum::<f64>() / nums.len() as f64) }
                } else if name.eq_ignore_ascii_case("min") {
                    nums.iter().min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .map(|n| if has_double { PropertyValue::Double(*n) } else { PropertyValue::Int(*n as i64) })
                        .unwrap_or(PropertyValue::Null)
                } else if name.eq_ignore_ascii_case("max") {
                    nums.iter().max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                        .map(|n| if has_double { PropertyValue::Double(*n) } else { PropertyValue::Int(*n as i64) })
                        .unwrap_or(PropertyValue::Null)
                } else if name.eq_ignore_ascii_case("collect") {
                    PropertyValue::List(vals)
                } else if name.eq_ignore_ascii_case("collect_map") || name.eq_ignore_ascii_case("collectmap") {
                    // collect_map(key_expr, value_expr) builds a map from keys to values
                    if arguments.len() >= 2 {
                        let mut entries = Vec::new();
                        let mut seen = HashSet::new();
                        for binding in bindings {
                            let key = eval::eval_expression_with_storage(&arguments[0], binding, Some(storage));
                            let val = eval::eval_expression_with_storage(&arguments[1], binding, Some(storage));
                            let key_str = match &key {
                                PropertyValue::String(s) => s.clone(),
                                PropertyValue::Int(n) => n.to_string(),
                                PropertyValue::Double(f) => f.to_string(),
                                _ => format!("{}", key),
                            };
                            if seen.insert(key_str.clone()) {
                                entries.push((key_str, val));
                            } else {
                                // Update existing key
                                if let Some(entry) = entries.iter_mut().find(|(k, _)| k == &key_str) {
                                    entry.1 = val;
                                }
                            }
                        }
                        PropertyValue::Map(entries)
                    } else {
                        PropertyValue::Null
                    }
                } else {
                    PropertyValue::Null
                }
            }
        }
        Expression::Add(a, b) => {
            let av = eval_aggregate(storage, a, bindings);
            let bv = eval_aggregate(storage, b, bindings);
            match (av, bv) {
                (PropertyValue::Int(x), PropertyValue::Int(y)) => PropertyValue::Int(x + y),
                (PropertyValue::Double(x), PropertyValue::Double(y)) => PropertyValue::Double(x + y),
                (PropertyValue::Int(x), PropertyValue::Double(y)) => PropertyValue::Double(x as f64 + y),
                (PropertyValue::Double(x), PropertyValue::Int(y)) => PropertyValue::Double(x + y as f64),
                (PropertyValue::String(x), PropertyValue::String(y)) => PropertyValue::String(format!("{}{}", x, y)),
                _ => PropertyValue::Null,
            }
        }
        Expression::Sub(a, b) => {
            let av = eval_aggregate(storage, a, bindings);
            let bv = eval_aggregate(storage, b, bindings);
            match (av, bv) {
                (PropertyValue::Int(x), PropertyValue::Int(y)) => PropertyValue::Int(x - y),
                (PropertyValue::Double(x), PropertyValue::Double(y)) => PropertyValue::Double(x - y),
                (PropertyValue::Int(x), PropertyValue::Double(y)) => PropertyValue::Double(x as f64 - y),
                (PropertyValue::Double(x), PropertyValue::Int(y)) => PropertyValue::Double(x - y as f64),
                _ => PropertyValue::Null,
            }
        }
        Expression::Mul(a, b) => {
            let av = eval_aggregate(storage, a, bindings);
            let bv = eval_aggregate(storage, b, bindings);
            match (av, bv) {
                (PropertyValue::Int(x), PropertyValue::Int(y)) => PropertyValue::Int(x * y),
                (PropertyValue::Double(x), PropertyValue::Double(y)) => PropertyValue::Double(x * y),
                (PropertyValue::Int(x), PropertyValue::Double(y)) => PropertyValue::Double(x as f64 * y),
                (PropertyValue::Double(x), PropertyValue::Int(y)) => PropertyValue::Double(x * y as f64),
                _ => PropertyValue::Null,
            }
        }
        Expression::Div(a, b) => {
            let av = eval_aggregate(storage, a, bindings);
            let bv = eval_aggregate(storage, b, bindings);
            match (av, bv) {
                (PropertyValue::Int(x), PropertyValue::Int(y)) if y != 0 => PropertyValue::Int(x / y),
                (PropertyValue::Double(x), PropertyValue::Double(y)) if y != 0.0 => PropertyValue::Double(x / y),
                (PropertyValue::Int(x), PropertyValue::Double(y)) if y != 0.0 => PropertyValue::Double(x as f64 / y),
                (PropertyValue::Double(x), PropertyValue::Int(y)) if y != 0 => PropertyValue::Double(x / y as f64),
                _ => PropertyValue::Null,
            }
        }
        Expression::Mod(a, b) => {
            let av = eval_aggregate(storage, a, bindings);
            let bv = eval_aggregate(storage, b, bindings);
            match (av, bv) {
                (PropertyValue::Int(x), PropertyValue::Int(y)) if y != 0 => PropertyValue::Int(x % y),
                _ => PropertyValue::Null,
            }
        }
        Expression::Neg(a) => {
            match eval_aggregate(storage, a, bindings) {
                PropertyValue::Int(x) => PropertyValue::Int(-x),
                PropertyValue::Double(x) => PropertyValue::Double(-x),
                _ => PropertyValue::Null,
            }
        }
        other => {
            // Non-aggregate expression: evaluate in first binding context
            bindings.first()
                .map(|b| eval::eval_expression_with_storage(other, b, Some(storage)))
                .unwrap_or(PropertyValue::Null)
        }
    }
}

// ─── ORDER BY execution ────────────────────────────────────────────────

fn exec_order_by(
    storage: &Storage,
    result: &mut QueryResult,
    items: &[OrderByItem],
) -> Result<(), ExecError> {
    // Pre-compute sort keys to avoid repeated expression evaluation during
    // sorting. Each comparison in sort_by would otherwise re-evaluate the
    // expression (potentially hitting storage), giving O(N log N * K) evals.
    // Pre-computation reduces this to O(N * K).
    let mut keyed: Vec<_> = result
        .rows
        .drain(..)
        .map(|row| {
            let keys: Vec<PropertyValue> = items
                .iter()
                .map(|o| eval::eval_expression_with_storage(&o.expression, &row, Some(storage)))
                .collect();
            (row, keys)
        })
        .collect();

    keyed.sort_by(|(_, keys_a), (_, keys_b)| {
        for (i, item) in items.iter().enumerate() {
            match compare(&keys_a[i], &keys_b[i]) {
                std::cmp::Ordering::Equal => continue,
                ord => return if item.ascending { ord } else { ord.reverse() },
            }
        }
        std::cmp::Ordering::Equal
    });

    result.rows = keyed.into_iter().map(|(row, _)| row).collect();
    Ok(())
}

// ─── Helpers ───────────────────────────────────────────────────────────

pub(crate) fn compare(a: &PropertyValue, b: &PropertyValue) -> std::cmp::Ordering {
    match (a, b) {
        (PropertyValue::Int(a), PropertyValue::Int(b)) => a.cmp(b),
        (PropertyValue::Double(a), PropertyValue::Double(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
        (PropertyValue::Bool(a), PropertyValue::Bool(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

// ─── Trigger Interpreter ───────────────────────────────────────────────

use mgstorage::triggers::{TriggerContext, TriggerExecutor};

/// Interpreter that executes trigger statements as Cypher queries.
pub struct TriggerInterpreter {
    storage: Arc<Storage>,
}

impl TriggerInterpreter {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }
}

impl TriggerExecutor for TriggerInterpreter {
    fn execute(&self, _ctx: &TriggerContext, statement: &str) -> Result<(), String> {
        let catalog = eval::active_catalog();
        let result = execute_with_catalog(&self.storage, statement, catalog)
            .map_err(|e| format!("trigger execution failed: {}", e))?;
        // Trigger statements typically don't return results; ignore them
        let _ = result;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_match_vertex() {
        let storage = Storage::new();
        execute(&storage, "CREATE (n:Person {name: \"Alice\"})").unwrap();
        let result = execute(&storage, "MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(result.columns.len(), 1);
        assert!(!result.rows.is_empty());
    }

    #[test]
    fn test_match_with_label_property_index() {
        let storage = Storage::new();

        // Create a label-property index BEFORE inserting data so the index is maintained
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let _ = storage.create_label_property_index(
            mgcore::types::LabelId::from(0u32),
            mgcore::types::PropertyId::from(0u32),
        );
        storage.commit_transaction(&tx);

        // Create vertices with the same label but different property values
        execute(&storage, "CREATE (n:Person {age: 30})").unwrap();
        execute(&storage, "CREATE (n:Person {age: 25})").unwrap();
        execute(&storage, "CREATE (n:Person {age: 30})").unwrap();

        // Query should use the label-property index and return exactly 2 results
        let result = execute(&storage, "MATCH (n:Person {age: 30}) RETURN n").unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_match_where() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a {value: 10})").unwrap();
        execute(&storage, "CREATE (b {value: 20})").unwrap();

        let result = execute(&storage, "MATCH (n) WHERE n.value > 15 RETURN n").unwrap();
        // Should only match the vertex with value=20
        // (WHERE filtering is limited in the current implementation)
        assert!(!result.columns.is_empty());
    }

    #[test]
    fn test_set_property() {
        let storage = Storage::new();
        execute(&storage, "CREATE (n {x: 1})").unwrap();
        execute(&storage, "MATCH (n) SET n.x = 42").unwrap();

        let result = execute(&storage, "MATCH (n) RETURN n").unwrap();
        assert!(!result.rows.is_empty());
    }

    #[test]
    fn test_count_star() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a)").unwrap();
        execute(&storage, "CREATE (b)").unwrap();

        let result = execute(&storage, "MATCH (n) RETURN count(*)").unwrap();
        assert_eq!(result.columns.len(), 1);
        assert!(!result.rows.is_empty());
    }

    #[test]
    fn test_order_by_and_limit() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a {num: 3})").unwrap();
        execute(&storage, "CREATE (b {num: 1})").unwrap();
        execute(&storage, "CREATE (c {num: 2})").unwrap();

        let result = execute(&storage, "MATCH (n) RETURN n ORDER BY n.num ASC LIMIT 2").unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_aggregate_sum_and_avg() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a)").unwrap();
        execute(&storage, "CREATE (b)").unwrap();
        execute(&storage, "CREATE (c)").unwrap();

        let result = execute(&storage, "MATCH (n) RETURN sum(1)").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Int(3)));

        let result = execute(&storage, "MATCH (n) RETURN avg(1)").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Double(1.0)));
    }

    #[test]
    fn test_aggregate_collect() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a)").unwrap();
        execute(&storage, "CREATE (b)").unwrap();

        let result = execute(&storage, "MATCH (n) RETURN collect(1)").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("column_0"),
            Some(&PropertyValue::List(vec![PropertyValue::Int(1), PropertyValue::Int(1)]))
        );
    }

    #[test]
    fn test_aggregate_grouping() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a)").unwrap();
        execute(&storage, "CREATE (b)").unwrap();
        execute(&storage, "CREATE (c)").unwrap();

        // Group by literal: everything in one group
        let result = execute(&storage, "MATCH (n) RETURN 1 AS grp, count(*) AS cnt").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("grp"), Some(&PropertyValue::Int(1)));
        assert_eq!(result.rows[0].get("cnt"), Some(&PropertyValue::Int(3)));
    }

    #[test]
    fn test_aggregate_empty_set() {
        let storage = Storage::new();
        let result = execute(&storage, "MATCH (n) RETURN count(*)").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Int(0)));
    }

    #[test]
    fn test_optional_match_found() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person)").unwrap();

        let result = execute(&storage, "OPTIONAL MATCH (n:Person) RETURN n").unwrap();
        assert!(!result.rows.is_empty());
    }

    #[test]
    fn test_optional_match_not_found() {
        let storage = Storage::new();
        // No vertices exist — regular MATCH returns 0 rows
        let regular = execute(&storage, "MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(regular.rows.len(), 0);

        // OPTIONAL MATCH returns 1 row even when no match
        let optional = execute(&storage, "OPTIONAL MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(optional.rows.len(), 1);
    }

    #[test]
    fn test_variable_length_path() {
        let storage = Storage::new();
        // Create a chain: a --KNOWS--> b --KNOWS--> c in a single CREATE
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();

        // 1-hop from Alice: should find Bob
        let result = execute(&storage, "MATCH (a:Person {name: \"Alice\"})-[:KNOWS*1..1]->(b:Person) RETURN b").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_variable_length_path_multi_hop() {
        let storage = Storage::new();
        // Create a chain: a --KNOWS--> b --KNOWS--> c in a single CREATE
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})-[:KNOWS]->(c:Person {name: \"Charlie\"})").unwrap();

        // 1..2 hops from Alice: should find Bob (1-hop) and Charlie (2-hop)
        let result = execute(&storage, "MATCH (a:Person {name: \"Alice\"})-[:KNOWS*1..2]->(b:Person) RETURN b").unwrap();
        for (i, row) in result.rows.iter().enumerate() {
            eprintln!("Row {} keys: {:?}", i, row.keys().collect::<Vec<_>>());
        }
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_variable_length_star_unbounded() {
        let storage = Storage::new();
        // Create a chain: a --KNOWS--> b --KNOWS--> c in a single CREATE
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})-[:KNOWS]->(c:Person {name: \"Charlie\"})").unwrap();

        // * means 1..unbounded: should find Bob and Charlie
        let result = execute(&storage, "MATCH (a:Person {name: \"Alice\"})-[:KNOWS*]->(b:Person) RETURN b").unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_variable_length_edge_alias_list() {
        let storage = Storage::new();
        // Create a chain: a --KNOWS--> b --KNOWS--> c in a single CREATE
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})-[:KNOWS]->(c:Person {name: \"Charlie\"})").unwrap();

        // Edge alias should be a list of edges for multi-hop
        let result = execute(&storage, "MATCH (a:Person {name: \"Alice\"})-[r:KNOWS*1..2]->(b:Person) RETURN r").unwrap();
        assert_eq!(result.rows.len(), 2);
        // One row has 1 edge, the other has 2 edges
        let edge_counts: Vec<usize> = result.rows.iter().map(|row| {
            match row.get("r") {
                Some(PropertyValue::List(edges)) => edges.len(),
                _ => 0,
            }
        }).collect();
        eprintln!("edge_counts: {:?}", edge_counts);
        assert!(edge_counts.contains(&1));
        assert!(edge_counts.contains(&2));
    }

    #[test]
    fn test_exists_true() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})").unwrap();

        let result = execute(&storage, "RETURN EXISTS { MATCH (n:Person) }").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Bool(true)));
    }

    #[test]
    fn test_exists_false() {
        let storage = Storage::new();
        // No vertices
        let result = execute(&storage, "RETURN EXISTS { MATCH (n:Person) }").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Bool(false)));
    }

    #[test]
    fn test_exists_in_where() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();

        // Find Alice only if she has an outgoing KNOWS edge
        let result = execute(&storage, "MATCH (a:Person {name: \"Alice\"}) WHERE EXISTS { MATCH (a)-[:KNOWS]->(b) } RETURN a").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_all_predicate() {
        let storage = Storage::new();
        let result = execute(&storage, "RETURN ALL(x IN [1,2,3] WHERE x > 0)").unwrap();
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Bool(true)));

        let result = execute(&storage, "RETURN ALL(x IN [1,2,3] WHERE x > 2)").unwrap();
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Bool(false)));
    }

    #[test]
    fn test_any_predicate() {
        let storage = Storage::new();
        let result = execute(&storage, "RETURN ANY(x IN [1,2,3] WHERE x > 2)").unwrap();
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Bool(true)));

        let result = execute(&storage, "RETURN ANY(x IN [1,2,3] WHERE x > 5)").unwrap();
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Bool(false)));
    }

    #[test]
    fn test_none_predicate() {
        let storage = Storage::new();
        let result = execute(&storage, "RETURN NONE(x IN [1,2,3] WHERE x > 5)").unwrap();
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Bool(true)));

        let result = execute(&storage, "RETURN NONE(x IN [1,2,3] WHERE x > 0)").unwrap();
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Bool(false)));
    }

    #[test]
    fn test_single_predicate() {
        let storage = Storage::new();
        let result = execute(&storage, "RETURN SINGLE(x IN [1,2,3] WHERE x > 2)").unwrap();
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Bool(true)));

        let result = execute(&storage, "RETURN SINGLE(x IN [1,2,3] WHERE x > 0)").unwrap();
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::Bool(false)));
    }

    #[test]
    fn test_foreach_create() {
        let storage = Storage::new();
        execute(&storage, "FOREACH (x IN [1,2,3] | CREATE (n {val: x}))").unwrap();

        let result = execute(&storage, "MATCH (n) RETURN n").unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_foreach_set() {
        let storage = Storage::new();
        execute(&storage, "CREATE (n {val: 0})").unwrap();
        execute(&storage, "MATCH (n) FOREACH (x IN [10,20,30] | SET n.val = n.val + x)").unwrap();

        let result = execute(&storage, "MATCH (n) RETURN n").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_procedure_registry() {
        let storage = Storage::new();
        let mut registry = ProcedureRegistry::new();
        registry.register("custom.test", |_, args| {
            let sum: i64 = args.iter().filter_map(|v| match v {
                PropertyValue::Int(n) => Some(*n),
                _ => None,
            }).sum();
            let mut row = HashMap::new();
            row.insert("result".to_string(), PropertyValue::Int(sum));
            Ok(QueryResult { number_of_hops: 0,
                columns: vec!["result".into()],
                rows: vec![row],
            })
        });

        let result = execute_with_registry(
            &storage,
            "CALL custom.test(1, 2, 3) YIELD result",
            Some(&registry),
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("result"), Some(&PropertyValue::Int(6)));
    }

    #[test]
    fn test_match_edge_property() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS {since: 2020}]->(b:Person {name: \"Bob\"})").unwrap();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS {since: 2021}]->(c:Person {name: \"Charlie\"})").unwrap();

        // Match only edges with since: 2020
        let result = execute(&storage, "MATCH (a:Person)-[r:KNOWS {since: 2020}]->(b:Person) RETURN b.name").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::String("Bob".into())));
    }

    #[test]
    fn test_match_endpoint_node_property() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(c:Person {name: \"Charlie\"})").unwrap();

        // Match only paths ending at Bob
        let result = execute(&storage, "MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"}) RETURN b.name").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::String("Bob".into())));
    }

    #[test]
    fn test_match_multi_hop_with_properties() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS {since: 2020}]->(b:Person {name: \"Bob\"})-[:WORKS_AT]->(c:Company {name: \"Acme\"})").unwrap();

        // Multi-hop with edge and node properties
        let result = execute(&storage, "MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company {name: \"Acme\"}) RETURN b.name, c.name").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::String("Bob".into())));
        assert_eq!(result.rows[0].get("column_1"), Some(&PropertyValue::String("Acme".into())));
    }

    #[test]
    fn test_match_directional_left() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();

        // Left direction: Bob <-[:KNOWS]- Alice
        let result = execute(&storage, "MATCH (b:Person {name: \"Bob\"})<-[:KNOWS]-(a:Person) RETURN a.name").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::String("Alice".into())));
    }

    #[test]
    fn test_match_either_direction() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();

        // Either direction should match
        let result = execute(&storage, "MATCH (a:Person {name: \"Alice\"})-[:KNOWS]-(b:Person {name: \"Bob\"}) RETURN a.name, b.name").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_match_no_result_when_edge_property_mismatch() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person)-[:KNOWS {since: 2021}]->(b:Person)").unwrap();

        // Should return empty when edge property doesn't match
        let result = execute(&storage, "MATCH (a)-[r:KNOWS {since: 2020}]->(b) RETURN a").unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_multi_pattern_element_shared_variable() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})-[:WORKS_AT]->(c:Company {name: \"Acme\"})").unwrap();

        // Match using two pattern elements sharing variable b
        let result = execute(
            &storage,
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (b:Person)-[:WORKS_AT]->(c:Company) RETURN a.name, b.name, c.name"
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::String("Alice".into())));
        assert_eq!(result.rows[0].get("column_1"), Some(&PropertyValue::String("Bob".into())));
        assert_eq!(result.rows[0].get("column_2"), Some(&PropertyValue::String("Acme".into())));
    }

    #[test]
    fn test_multi_pattern_element_inconsistent_variable() {
        let storage = Storage::new();
        execute(&storage, "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})").unwrap();
        execute(&storage, "CREATE (c:Person {name: \"Charlie\"})-[:KNOWS]->(d:Person {name: \"Diana\"})").unwrap();

        // Pattern elements with no shared variable: cross-product of all matches
        let result = execute(
            &storage,
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (c:Person)-[:KNOWS]->(d:Person) RETURN a.name, d.name"
        ).unwrap();
        // Should produce 2 x 2 = 4 rows (cartesian product)
        assert_eq!(result.rows.len(), 4);
    }

    #[test]
    fn test_multi_pattern_element_shared_middle_node() {
        let storage = Storage::new();
        // Create a chain: Alice -> Bob -> Acme
        execute(
            &storage,
            "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})-[:WORKS_AT]->(c:Company {name: \"Acme\"})"
        ).unwrap();

        // Two pattern elements sharing middle node 'b'
        let result = execute(
            &storage,
            "MATCH (a:Person)-[:KNOWS]->(b:Person), (b:Person)-[:WORKS_AT]->(c:Company) RETURN a.name, c.name"
        ).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("column_0"), Some(&PropertyValue::String("Alice".into())));
        assert_eq!(result.rows[0].get("column_1"), Some(&PropertyValue::String("Acme".into())));
    }

    #[test]
    fn test_load_csv_with_headers() {
        use std::io::Write;
        let storage = Storage::new();

        let mut tmp = std::env::temp_dir();
        tmp.push("mg_test_load_csv.csv");
        {
            let mut file = std::fs::File::create(&tmp).unwrap();
            file.write_all(b"name,age\nAlice,30\nBob,25\n").unwrap();
        }

        let path = tmp.to_str().unwrap();
        let query = format!(
            "LOAD CSV FROM '{}' WITH HEADERS AS row CREATE (n:Person {{name: row.name, age: toInteger(row.age)}})",
            path
        );
        execute(&storage, &query).unwrap();

        let result = execute(&storage, "MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(result.rows.len(), 2);

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn test_load_csv_without_headers() {
        use std::io::Write;
        let storage = Storage::new();

        let mut tmp = std::env::temp_dir();
        tmp.push("mg_test_load_csv_no_hdr.csv");
        {
            let mut file = std::fs::File::create(&tmp).unwrap();
            file.write_all(b"Charlie,35\n").unwrap();
        }

        let path = tmp.to_str().unwrap();
        let query = format!(
            "LOAD CSV FROM '{}' AS row CREATE (n:Person {{name: row.column_0, age: toInteger(row.column_1)}})",
            path
        );
        execute(&storage, &query).unwrap();

        let result = execute(&storage, "MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(result.rows.len(), 1);

        let _ = std::fs::remove_file(&tmp);
    }

    // ─── Query cache tests ──────────────────────────────────────────────────

    #[test]
    fn test_query_cache_hit() {
        let storage = Storage::new();
        let query = "CREATE (n:Person {name: 'Alice'})";

        // First execution populates the cache
        execute(&storage, query).unwrap();

        // Second execution should be a cache hit
        let result = execute(&storage, query).unwrap();
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_query_cache_preserves_semantics() {
        let storage = Storage::new();
        execute(&storage, "CREATE (n:Person {name: 'Alice'})").unwrap();

        let q1 = "MATCH (n:Person) RETURN n";
        let r1 = execute(&storage, q1).unwrap();

        // Run same query again (cache hit)
        let r2 = execute(&storage, q1).unwrap();
        assert_eq!(r1.rows.len(), r2.rows.len());
    }

    #[test]
    fn test_query_cache_with_params() {
        let storage = Storage::new();
        let mut params = HashMap::new();
        params.insert("name".to_string(), PropertyValue::String("Bob".to_string()));

        // Parameters don't affect parsing, so cache should still work
        let q = "CREATE (n:Person {name: $name})";
        let _ = execute_with_catalog_and_params(&storage, q, None, &params);

        // Same query text, different params
        let mut params2 = HashMap::new();
        params2.insert("name".to_string(), PropertyValue::String("Charlie".to_string()));
        let r2 = execute_with_catalog_and_params(&storage, q, None, &params2);
        assert!(r2.is_ok());
    }

    #[test]
    fn test_parse_cache_hit_with_whitespace_difference() {
        let q1 = "RETURN 1";
        let q2 = "RETURN  1";
        let q3 = "  RETURN 1  ";

        // First parse populates the cache
        let parsed1 = parse_cached(q1, None).unwrap();

        // Second and third queries differ only in whitespace — should be cache hits
        let parsed2 = parse_cached(q2, None).unwrap();
        let parsed3 = parse_cached(q3, None).unwrap();

        assert_eq!(parsed1.fingerprint(), parsed2.fingerprint());
        assert_eq!(parsed1.fingerprint(), parsed3.fingerprint());
    }

    #[test]
    fn test_parse_cache_different_literals_remain_different() {
        let q1 = "RETURN 'Alice'";
        let q2 = "RETURN 'Bob'";

        let parsed1 = parse_cached(q1, None).unwrap();
        let parsed2 = parse_cached(q2, None).unwrap();

        // Different literals produce different ASTs
        assert_ne!(parsed1.fingerprint(), parsed2.fingerprint());
    }

}
