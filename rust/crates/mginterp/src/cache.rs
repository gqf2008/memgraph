//! Query plan cache and prepared statements.
//!
//! The cache uses read-optimized lookups: `get()` takes a read lock and
//! uses atomic counters for hit tracking, avoiding write-lock contention
//! on the hot path. Query normalization replaces inline literals with
//! placeholders so that structurally identical queries share cache entries.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use mgparser::ast::Query;
use mgplanner::LogicalPlan;

/// A cached parsed query with atomic metadata for read-lock access.
#[allow(dead_code)]
struct CacheEntry {
    parsed: Query,
    hit_count: AtomicU64,
    last_used: Mutex<Instant>,
    created_at: Instant,
}

/// LRU query cache with size limit and TTL.
pub struct QueryCache {
    entries: RwLock<HashMap<String, Arc<CacheEntry>>>,
    /// Generation counter for LRU approximation. Incremented on every access.
    generation: AtomicU64,
    max_entries: usize,
    ttl: Duration,
}

impl QueryCache {
    pub fn new(max_entries: usize, ttl_secs: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            generation: AtomicU64::new(0),
            max_entries,
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    /// Look up a query in the cache. Uses a read lock for low contention.
    /// Hit count and access time are tracked via atomic/interior mutability.
    pub fn get(&self, query_text: &str) -> Option<Query> {
        let entries = self.entries.read().unwrap();
        let entry = entries.get(query_text)?;
        let mut lu = entry.last_used.lock().unwrap();
        if lu.elapsed() < self.ttl {
            entry.hit_count.fetch_add(1, Ordering::Relaxed);
            *lu = Instant::now();
            self.generation.fetch_add(1, Ordering::Relaxed);
            return Some(entry.parsed.clone());
        }
        None
    }

    /// Insert a parsed query into the cache. Takes a write lock and evicts
    /// the entry with the lowest generation counter when over capacity.
    pub fn insert(&self, query_text: String, parsed: Query) {
        let mut entries = self.entries.write().unwrap();
        // Check for raced insert
        if entries.contains_key(&query_text) {
            return;
        }
        // Evict if needed: remove the entry with oldest last_used.
        // Use a sampled approach to avoid O(n) iteration every time.
        if entries.len() >= self.max_entries {
            self.evict_one(&mut entries);
        }
        let entry = Arc::new(CacheEntry {
            parsed,
            hit_count: AtomicU64::new(1),
            last_used: Mutex::new(Instant::now()),
            created_at: Instant::now(),
        });
        entries.insert(query_text, entry);
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Evict one entry: remove the entry with the oldest last_used timestamp.
    fn evict_one(&self, entries: &mut HashMap<String, Arc<CacheEntry>>) {
        let sample_keys: Vec<String> = entries.iter().take(16).map(|(k, _)| k.clone()).collect();
        // Find oldest by last_used, then release refs before mutation.
        let mut oldest_key: Option<String> = None;
        let mut oldest_time: Option<Duration> = None;
        for k in &sample_keys {
            if let Some(e) = entries.get(k.as_str()) {
                let elapsed = e.last_used.lock().unwrap().elapsed();
                if oldest_time.is_none() || elapsed > oldest_time.unwrap() {
                    oldest_time = Some(elapsed);
                    oldest_key = Some(k.clone());
                }
            }
        }
        if let Some(k) = oldest_key {
            entries.remove(k.as_str());
        }
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.entries.write().unwrap().clear();
        self.generation.store(0, Ordering::Relaxed);
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }

    /// Get hit count for a cached query.
    pub fn hit_count(&self, query_text: &str) -> Option<u64> {
        self.entries
            .read()
            .unwrap()
            .get(query_text)
            .map(|e| e.hit_count.load(Ordering::Relaxed))
    }

    /// Remove expired entries. O(n) — call periodically, not on hot path.
    pub fn remove_expired(&self) {
        let mut entries = self.entries.write().unwrap();
        entries.retain(|_, v| v.last_used.lock().unwrap().elapsed() < self.ttl);
    }

    /// Approximate generation (access counter). Useful for monitoring.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }
}

/// Normalize a query string by replacing inline literals with `$`.
/// This allows `MATCH (n) WHERE n.id = 1` and `MATCH (n) WHERE n.id = 2`
/// to share the same cache entry.
///
/// Handles:
/// - integer literals (standalone, not inside identifiers)
/// - decimal/float literals
/// - single-quoted strings
pub fn normalize_query(query: &str) -> String {
    let bytes = query.as_bytes();
    let mut out = String::with_capacity(query.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'\'' => {
                // Skip string literal content
                out.push('\'');
                out.push('$');
                out.push('\'');
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' && (i + 1 >= bytes.len() || bytes[i + 1] != b'\'') {
                        break;
                    }
                    if bytes[i] == b'\'' {
                        i += 1; // escaped quote
                    }
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1; // closing quote
                }
            }
            b'0'..=b'9' | b'-' => {
                // Check context: only normalize if preceded by whitespace/paren/operator
                let is_standalone = i == 0
                    || matches!(
                        bytes[i - 1],
                        b' ' | b'\t'
                            | b'\n'
                            | b'('
                            | b'='
                            | b'<'
                            | b'>'
                            | b'+'
                            | b'-'
                            | b'*'
                            | b'/'
                            | b','
                            | b'{'
                    );
                if is_standalone && b != b'-'
                    || (b == b'-' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit())
                {
                    // Check it's actually a number (digits, optional dot+digits)
                    let start = i;
                    if bytes[i] == b'-' {
                        i += 1;
                    }
                    let mut has_digit = false;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        has_digit = true;
                        i += 1;
                    }
                    if i < bytes.len() && bytes[i] == b'.' {
                        i += 1;
                        while i < bytes.len() && bytes[i].is_ascii_digit() {
                            has_digit = true;
                            i += 1;
                        }
                    }
                    if has_digit {
                        out.push('$');
                        continue;
                    }
                    // Not a number, backtrack
                    for byte in bytes.iter().take(i).skip(start) {
                        out.push(*byte as char);
                    }
                    continue;
                }
                out.push(b as char);
                i += 1;
            }
            _ => {
                out.push(b as char);
                i += 1;
            }
        }
    }
    out
}

/// Normalize a query string for use as a cache key.
///
/// - Trims leading/trailing whitespace
/// - Collapses consecutive whitespace to a single space
/// - Preserves string literals exactly (does not modify content inside quotes)
///
/// This makes `MATCH (n) RETURN n` and `MATCH  (n)  RETURN  n` share the
/// same cache entry without affecting the parsed AST.
pub fn normalize_cache_key(query: &str) -> String {
    let bytes = query.as_bytes();
    let mut out = String::with_capacity(query.len());
    let mut in_whitespace = false;
    let mut i = 0;

    // Trim leading whitespace by skipping initial whitespace chars
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }

    while i < bytes.len() {
        let b = bytes[i];

        if b == b'\'' {
            // Preserve string literal exactly
            out.push('\'');
            i += 1;
            while i < bytes.len() {
                out.push(bytes[i] as char);
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        // Escaped quote — include it and continue
                        i += 1;
                        out.push('\'');
                    } else {
                        break;
                    }
                }
                i += 1;
            }
            i += 1; // skip closing quote
            in_whitespace = false;
            continue;
        }

        if b.is_ascii_whitespace() {
            if !in_whitespace {
                out.push(' ');
                in_whitespace = true;
            }
            i += 1;
        } else {
            out.push(b as char);
            in_whitespace = false;
            i += 1;
        }
    }

    // Remove trailing whitespace (single space we may have appended)
    if out.ends_with(' ') {
        out.pop();
    }

    out
}

impl Default for QueryCache {
    fn default() -> Self {
        Self::new(128, 300)
    }
}

// ─── Query Plan Cache ──────────────────────────────────────────────────────

/// A cached logical plan with schema generation tracking for invalidation.
struct PlanCacheEntry {
    plan: LogicalPlan,
    schema_generation: u64,
    hit_count: AtomicU64,
    created_at: Instant,
}

/// Thread-safe LRU query plan cache with schema-change invalidation.
///
/// Maps query fingerprints (u64) to compiled `LogicalPlan`s. When the
/// storage schema changes (indices created/dropped), the generation
/// counter bumps and stale entries are rejected on lookup.
pub struct QueryPlanCache {
    entries: Mutex<lru::LruCache<u64, Arc<PlanCacheEntry>>>,
    schema_generation: AtomicU64,
    max_entries: usize,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
}

impl QueryPlanCache {
    pub fn new(max_entries: usize) -> Self {
        let cache_size = std::num::NonZeroUsize::new(max_entries.max(1))
            .unwrap_or(std::num::NonZeroUsize::new(1).unwrap());
        Self {
            entries: Mutex::new(lru::LruCache::new(cache_size)),
            schema_generation: AtomicU64::new(0),
            max_entries,
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }

    /// Look up a plan by fingerprint. Returns `None` if the entry is stale
    /// (schema has changed since the plan was cached).
    pub fn get(&self, fingerprint: u64) -> Option<LogicalPlan> {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.get(&fingerprint)?;
        let current_gen = self.schema_generation.load(Ordering::Relaxed);
        if entry.schema_generation != current_gen {
            // Stale entry — schema changed since caching. Drop it.
            return None;
        }
        entry.hit_count.fetch_add(1, Ordering::Relaxed);
        self.hit_count.fetch_add(1, Ordering::Relaxed);
        Some(entry.plan.clone())
    }

    /// Insert a plan into the cache, tagged with the current schema generation.
    pub fn insert(&self, fingerprint: u64, plan: LogicalPlan) {
        let mut entries = self.entries.lock().unwrap();
        let gen = self.schema_generation.load(Ordering::Relaxed);
        let entry = Arc::new(PlanCacheEntry {
            plan,
            schema_generation: gen,
            hit_count: AtomicU64::new(1),
            created_at: Instant::now(),
        });
        entries.put(fingerprint, entry);
    }

    /// Bump the schema generation counter. Call after any schema mutation
    /// (create/drop index, constraint, etc.) to invalidate cached plans.
    pub fn bump_schema_generation(&self) {
        self.schema_generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Current schema generation.
    pub fn schema_generation(&self) -> u64 {
        self.schema_generation.load(Ordering::Relaxed)
    }

    /// Total cache hits.
    pub fn hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    /// Total cache misses.
    pub fn miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }

    /// Number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().unwrap().len() == 0
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.entries.lock().unwrap().clear();
        self.hit_count.store(0, Ordering::Relaxed);
        self.miss_count.store(0, Ordering::Relaxed);
    }

    /// Record a cache miss (for metrics).
    pub fn record_miss(&self) {
        self.miss_count.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for QueryPlanCache {
    fn default() -> Self {
        Self::new(1000)
    }
}

/// Prepared statement with parameter placeholders.
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    pub id: u64,
    pub query_text: String,
    pub parsed: Query,
    pub param_names: Vec<String>,
    pub created_at: Instant,
}

/// Prepared statement registry.
pub struct PreparedStatementRegistry {
    statements: Mutex<HashMap<u64, PreparedStatement>>,
    next_id: Mutex<u64>,
    ttl: Duration,
}

impl PreparedStatementRegistry {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            statements: Mutex::new(HashMap::new()),
            next_id: Mutex::new(1),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    pub fn prepare(&self, query_text: String, parsed: Query, param_names: Vec<String>) -> u64 {
        let id = {
            let mut next = self.next_id.lock().unwrap();
            let id = *next;
            *next += 1;
            id
        };
        let stmt = PreparedStatement {
            id,
            query_text,
            parsed,
            param_names,
            created_at: Instant::now(),
        };
        self.statements.lock().unwrap().insert(id, stmt);
        id
    }

    pub fn get(&self, id: u64) -> Option<PreparedStatement> {
        let mut stmts = self.statements.lock().unwrap();
        let stmt = stmts.get(&id)?;
        if stmt.created_at.elapsed() > self.ttl {
            stmts.remove(&id);
            return None;
        }
        Some(stmt.clone())
    }

    pub fn remove(&self, id: u64) {
        self.statements.lock().unwrap().remove(&id);
    }

    pub fn list(&self) -> Vec<u64> {
        let mut stmts = self.statements.lock().unwrap();
        stmts.retain(|_, s| s.created_at.elapsed() <= self.ttl);
        stmts.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgparser::ast::QueryMode;

    #[test]
    fn test_cache_insert_and_get() {
        let cache = QueryCache::new(10, 60);
        let query = Query {
            clauses: vec![],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: Vec::new(),
        };
        cache.insert("MATCH (n) RETURN n".into(), query.clone());
        assert_eq!(cache.len(), 1);
        let got = cache.get("MATCH (n) RETURN n");
        assert!(got.is_some());
    }

    #[test]
    fn test_cache_miss() {
        let cache = QueryCache::new(10, 60);
        assert!(cache.get("UNKNOWN").is_none());
    }

    #[test]
    fn test_cache_eviction() {
        let cache = QueryCache::new(2, 60);
        let q = Query {
            clauses: vec![],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: Vec::new(),
        };
        cache.insert("q1".into(), q.clone());
        cache.insert("q2".into(), q.clone());
        cache.insert("q3".into(), q.clone());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_cache_ttl_expiration() {
        let cache = QueryCache::new(10, 0); // 0 second TTL
        let q = Query {
            clauses: vec![],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: Vec::new(),
        };
        cache.insert("q1".into(), q);
        std::thread::sleep(Duration::from_millis(50));
        assert!(cache.get("q1").is_none());
    }

    #[test]
    fn test_cache_hit_count() {
        let cache = QueryCache::new(10, 60);
        let q = Query {
            clauses: vec![],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: Vec::new(),
        };
        cache.insert("q1".into(), q);
        let _ = cache.get("q1");
        let _ = cache.get("q1");
        assert_eq!(cache.hit_count("q1"), Some(3)); // 1 insert + 2 gets
    }

    #[test]
    fn test_prepared_statement() {
        let reg = PreparedStatementRegistry::new(60);
        let q = Query {
            clauses: vec![],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: Vec::new(),
        };
        let id = reg.prepare(
            "MATCH (n) WHERE n.id = $id RETURN n".into(),
            q,
            vec!["id".into()],
        );
        let stmt = reg.get(id);
        assert!(stmt.is_some());
        assert_eq!(stmt.unwrap().param_names, vec!["id"]);
    }

    #[test]
    fn test_prepared_statement_remove() {
        let reg = PreparedStatementRegistry::new(60);
        let q = Query {
            clauses: vec![],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: Vec::new(),
        };
        let id = reg.prepare("q".into(), q, vec![]);
        reg.remove(id);
        assert!(reg.get(id).is_none());
    }

    #[test]
    fn test_prepared_statement_ttl() {
        let reg = PreparedStatementRegistry::new(0);
        let q = Query {
            clauses: vec![],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: Vec::new(),
        };
        let id = reg.prepare("q".into(), q, vec![]);
        std::thread::sleep(Duration::from_millis(50));
        assert!(reg.get(id).is_none());
    }

    #[test]
    fn test_prepared_list() {
        let reg = PreparedStatementRegistry::new(60);
        let q = Query {
            clauses: vec![],
            union: None,
            mode: QueryMode::Standard,
            periodic_commit: None,
            hops_limit: None,
            index_hints: Vec::new(),
        };
        let id1 = reg.prepare("q1".into(), q.clone(), vec![]);
        let id2 = reg.prepare("q2".into(), q, vec![]);
        let list = reg.list();
        assert!(list.contains(&id1));
        assert!(list.contains(&id2));
    }

    // ─── normalize_cache_key tests ────────────────────────────────────────

    #[test]
    fn test_normalize_cache_key_whitespace() {
        assert_eq!(
            normalize_cache_key("MATCH (n) RETURN n"),
            "MATCH (n) RETURN n"
        );
        assert_eq!(
            normalize_cache_key("MATCH  (n)  RETURN  n"),
            "MATCH (n) RETURN n"
        );
        assert_eq!(
            normalize_cache_key("  MATCH (n) RETURN n  "),
            "MATCH (n) RETURN n"
        );
        assert_eq!(
            normalize_cache_key("MATCH\t(n)\nRETURN\nn"),
            "MATCH (n) RETURN n"
        );
    }

    #[test]
    fn test_normalize_cache_key_preserves_string_literals() {
        assert_eq!(
            normalize_cache_key("MATCH (n {name: 'Alice'}) RETURN n"),
            "MATCH (n {name: 'Alice'}) RETURN n"
        );
        assert_eq!(
            normalize_cache_key("MATCH  (n {name:  'Alice'})  RETURN  n"),
            "MATCH (n {name: 'Alice'}) RETURN n"
        );
        // Escaped quotes inside string literal
        assert_eq!(
            normalize_cache_key("RETURN 'It''s ok'"),
            "RETURN 'It''s ok'"
        );
    }

    #[test]
    fn test_normalize_cache_key_empty_and_whitespace_only() {
        assert_eq!(normalize_cache_key(""), "");
        assert_eq!(normalize_cache_key("   "), "");
        assert_eq!(normalize_cache_key("\t\n  "), "");
    }

    // ─── QueryPlanCache tests ─────────────────────────────────────────────

    #[test]
    fn test_plan_cache_insert_and_get() {
        let cache = QueryPlanCache::new(100);
        let plan = LogicalPlan {
            op: mgplanner::LogicalOp::AllScan { alias: None },
            cost: mgplanner::PlanCost::default(),
            cardinality: 1.0,
        };
        cache.insert(42, plan.clone());
        assert_eq!(cache.len(), 1);
        let got = cache.get(42);
        assert!(got.is_some());
    }

    #[test]
    fn test_plan_cache_miss() {
        let cache = QueryPlanCache::new(100);
        assert!(cache.get(999).is_none());
    }

    #[test]
    fn test_plan_cache_schema_invalidation() {
        let cache = QueryPlanCache::new(100);
        let plan = LogicalPlan {
            op: mgplanner::LogicalOp::AllScan { alias: None },
            cost: mgplanner::PlanCost::default(),
            cardinality: 1.0,
        };
        cache.insert(1, plan);
        assert!(cache.get(1).is_some());

        // Bump schema generation — plan should now be stale
        cache.bump_schema_generation();
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_plan_cache_hit_count() {
        let cache = QueryPlanCache::new(100);
        let plan = LogicalPlan {
            op: mgplanner::LogicalOp::AllScan { alias: None },
            cost: mgplanner::PlanCost::default(),
            cardinality: 1.0,
        };
        cache.insert(1, plan);
        let _ = cache.get(1);
        let _ = cache.get(1);
        assert_eq!(cache.hit_count(), 2);
    }

    #[test]
    fn test_plan_cache_lru_eviction() {
        let cache = QueryPlanCache::new(2);
        let plan = LogicalPlan {
            op: mgplanner::LogicalOp::AllScan { alias: None },
            cost: mgplanner::PlanCost::default(),
            cardinality: 1.0,
        };
        cache.insert(1, plan.clone());
        cache.insert(2, plan.clone());
        cache.insert(3, plan.clone());
        assert_eq!(cache.len(), 2);
    }
}
