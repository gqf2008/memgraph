//! Query cache and prepared statement cache for the server.
//!
//! Provides LRU caching of parsed queries, execution plans, and query results,
//! plus prepared statement storage for parameterized queries.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mgcore::property_value::PropertyValue;
use mgparser::ast::Query;
use mgplanner::LogicalPlan;

/// Parsed query entry in the cache.
#[derive(Clone, Debug)]
pub struct CachedQuery {
    pub query_text: String,
    pub parsed: Query,
    pub plan: Option<LogicalPlan>,
    pub hit_count: u64,
    pub last_accessed: Instant,
    pub created_at: Instant,
    /// Cached result (for deterministic, read-only queries).
    pub cached_result: Option<mginterp::QueryResult>,
}

impl CachedQuery {
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn idle_time(&self) -> Duration {
        self.last_accessed.elapsed()
    }
}

/// Cache eviction policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// Evict least-recently-used entry.
    #[default]
    Lru,
    /// Evict least-frequently-used entry.
    Lfu,
    /// Evict oldest entry by creation time.
    Fifo,
}

/// Query cache with configurable eviction and TTL.
pub struct QueryCache {
    inner: Mutex<HashMap<String, CachedQuery>>,
    capacity: usize,
    /// Maximum idle time before eviction (None = no TTL).
    ttl: Option<Duration>,
    policy: EvictionPolicy,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    evictions: std::sync::atomic::AtomicU64,
    /// Total approximate size of cached entries in bytes.
    total_bytes: std::sync::atomic::AtomicUsize,
}

impl QueryCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(HashMap::with_capacity(capacity)),
            capacity,
            ttl: None,
            policy: EvictionPolicy::Lru,
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
            evictions: std::sync::atomic::AtomicU64::new(0),
            total_bytes: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    pub fn with_policy(mut self, policy: EvictionPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Look up a parsed query by its text.
    pub fn get(&self, query_text: &str) -> Option<CachedQuery> {
        let mut inner = self.inner.lock().unwrap();

        // Check TTL eviction first
        if let Some(entry) = inner.get(query_text) {
            if let Some(ttl) = self.ttl {
                if entry.idle_time() > ttl {
                    inner.remove(query_text);
                    self.evictions
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.misses
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return None;
                }
            }
        }

        if let Some(entry) = inner.get_mut(query_text) {
            entry.hit_count += 1;
            entry.last_accessed = Instant::now();
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(entry.clone())
        } else {
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }

    /// Insert a parsed query into the cache.
    pub fn put(&self, query_text: String, parsed: Query, plan: Option<LogicalPlan>) {
        self.put_with_result(query_text, parsed, plan, None);
    }

    /// Insert a parsed query with an optional cached result.
    pub fn put_with_result(
        &self,
        query_text: String,
        parsed: Query,
        plan: Option<LogicalPlan>,
        result: Option<mginterp::QueryResult>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        if inner.len() >= self.capacity {
            self.evict_one(&mut inner);
        }
        let now = Instant::now();
        let entry = CachedQuery {
            query_text: query_text.clone(),
            parsed,
            plan,
            hit_count: 1,
            last_accessed: now,
            created_at: now,
            cached_result: result,
        };
        inner.insert(query_text, entry);
    }

    fn evict_one(&self, inner: &mut HashMap<String, CachedQuery>) {
        let key = match self.policy {
            EvictionPolicy::Lru => inner
                .iter()
                .min_by_key(|(_, v)| v.last_accessed)
                .map(|(k, _)| k.clone()),
            EvictionPolicy::Lfu => inner
                .iter()
                .min_by_key(|(_, v)| v.hit_count)
                .map(|(k, _)| k.clone()),
            EvictionPolicy::Fifo => inner
                .iter()
                .min_by_key(|(_, v)| v.created_at)
                .map(|(k, _)| k.clone()),
        };
        if let Some(k) = key {
            inner.remove(&k);
            self.evictions
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Run maintenance: evict expired entries, enforce capacity.
    pub fn maintenance(&self) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(ttl) = self.ttl {
            let expired: Vec<String> = inner
                .iter()
                .filter(|(_, v)| v.idle_time() > ttl)
                .map(|(k, _)| k.clone())
                .collect();
            for k in expired {
                inner.remove(&k);
                self.evictions
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        while inner.len() > self.capacity {
            self.evict_one(&mut inner);
        }
    }

    /// Clear all cached entries.
    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
        self.evictions
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock().unwrap();
        CacheStats {
            entries: inner.len(),
            hits: self.hits.load(std::sync::atomic::Ordering::Relaxed),
            misses: self.misses.load(std::sync::atomic::Ordering::Relaxed),
            evictions: self.evictions.load(std::sync::atomic::Ordering::Relaxed),
            capacity: self.capacity,
            policy: self.policy,
            ttl_secs: self.ttl.map(|d| d.as_secs()),
        }
    }
}

/// Prepared statement cache for parameterized queries.
pub struct PreparedStatementCache {
    inner: Mutex<HashMap<u64, PreparedStatement>>,
    by_text: Mutex<HashMap<String, u64>>,
    capacity: usize,
    next_id: std::sync::atomic::AtomicU64,
}

/// A prepared statement with bound parameters.
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    pub id: u64,
    pub query_text: String,
    pub parsed: Query,
    pub parameter_names: Vec<String>,
    /// Number of times executed.
    pub execution_count: u64,
    /// Average execution time in ms (EWMA).
    pub avg_duration_ms: f64,
}

impl PreparedStatementCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(HashMap::with_capacity(capacity)),
            by_text: Mutex::new(HashMap::with_capacity(capacity)),
            capacity,
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Prepare a query and store it. Returns existing statement if already prepared.
    pub fn prepare(&self, query_text: String) -> Result<PreparedStatement, String> {
        // Check if already prepared
        {
            let by_text = self.by_text.lock().unwrap();
            if let Some(&id) = by_text.get(&query_text) {
                let inner = self.inner.lock().unwrap();
                if let Some(stmt) = inner.get(&id) {
                    return Ok(stmt.clone());
                }
            }
        }

        let parsed = mgparser::parse_query(&query_text).map_err(|e| format!("{}", e))?;
        let parameter_names = extract_parameters(&query_text);

        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stmt = PreparedStatement {
            id,
            query_text: query_text.clone(),
            parsed,
            parameter_names,
            execution_count: 0,
            avg_duration_ms: 0.0,
        };

        let mut inner = self.inner.lock().unwrap();
        let mut by_text = self.by_text.lock().unwrap();

        if inner.len() >= self.capacity {
            if let Some((&oldest_id, _)) = inner.iter().min_by_key(|(_, v)| v.execution_count) {
                let oldest = inner.remove(&oldest_id);
                if let Some(o) = oldest {
                    by_text.remove(&o.query_text);
                }
            }
        }

        by_text.insert(query_text, id);
        inner.insert(id, stmt.clone());
        Ok(stmt)
    }

    /// Retrieve a prepared statement by ID.
    pub fn get(&self, id: u64) -> Option<PreparedStatement> {
        self.inner.lock().unwrap().get(&id).cloned()
    }

    /// Record execution statistics for a prepared statement.
    pub fn record_execution(&self, id: u64, duration_ms: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(stmt) = inner.get_mut(&id) {
            stmt.execution_count += 1;
            // EWMA with alpha=0.1
            let alpha = 0.1;
            stmt.avg_duration_ms =
                alpha * duration_ms as f64 + (1.0 - alpha) * stmt.avg_duration_ms;
        }
    }

    /// Remove a prepared statement.
    pub fn remove(&self, id: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let mut by_text = self.by_text.lock().unwrap();
        if let Some(stmt) = inner.remove(&id) {
            by_text.remove(&stmt.query_text);
            true
        } else {
            false
        }
    }

    /// List all prepared statements.
    pub fn list(&self) -> Vec<PreparedStatement> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
        self.by_text.lock().unwrap().clear();
    }
}

/// Extract parameter names from query text (e.g., `$name` or `{name}`).
fn extract_parameters(query: &str) -> Vec<String> {
    let mut params = Vec::new();
    for word in query.split_whitespace() {
        if word.starts_with('$') {
            let name: String = word
                .chars()
                .skip(1)
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() && !params.contains(&name) {
                params.push(name);
            }
        }
    }
    params
}

/// Substitute parameters into a query text.
pub fn bind_parameters(query_text: &str, params: &HashMap<String, PropertyValue>) -> String {
    let mut result = query_text.to_string();
    for (name, value) in params {
        let placeholder = format!("${}", name);
        let replacement = format!("{}", value);
        result = result.replace(&placeholder, &replacement);
    }
    result
}

/// Cache statistics.
#[derive(Clone, Debug, Default)]
pub struct CacheStats {
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub capacity: usize,
    pub policy: EvictionPolicy,
    pub ttl_secs: Option<u64>,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        }
    }
}

/// Cache warmer: pre-populates cache with common query patterns.
pub struct CacheWarmer {
    patterns: Vec<String>,
}

impl CacheWarmer {
    pub fn new() -> Self {
        Self {
            patterns: Vec::new(),
        }
    }

    pub fn add_pattern(&mut self, pattern: String) {
        self.patterns.push(pattern);
    }

    pub fn warm(
        &self,
        cache: &QueryCache,
        storage: &mgstorage::storage::Storage,
        _catalog: &mgcatalog::Catalog,
    ) -> usize {
        let mut warmed = 0;
        for pattern in &self.patterns {
            if cache.get(pattern).is_some() {
                continue;
            }
            match mgparser::parse_query(pattern) {
                Ok(parsed) => {
                    let plan = mgplanner::plan_query(storage, &parsed);
                    cache.put(pattern.clone(), parsed, Some(plan));
                    warmed += 1;
                }
                Err(_) => continue,
            }
        }
        warmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_cache_basic() {
        let cache = QueryCache::new(10);
        assert!(cache.get("MATCH (n) RETURN n").is_none());

        let parsed = mgparser::parse_query("MATCH (n) RETURN n").unwrap();
        cache.put("MATCH (n) RETURN n".into(), parsed.clone(), None);

        let entry = cache.get("MATCH (n) RETURN n").unwrap();
        assert_eq!(entry.query_text, "MATCH (n) RETURN n");
        assert_eq!(entry.hit_count, 2); // 1 from put + 1 from get
    }

    #[test]
    fn test_query_cache_lru_eviction() {
        let cache = QueryCache::new(2).with_policy(EvictionPolicy::Lru);
        cache.put(
            "q1".into(),
            mgparser::parse_query("RETURN 1").unwrap(),
            None,
        );
        cache.put(
            "q2".into(),
            mgparser::parse_query("RETURN 2").unwrap(),
            None,
        );
        // Access q1 to make it most-recently-used
        cache.get("q1");
        cache.put(
            "q3".into(),
            mgparser::parse_query("RETURN 3").unwrap(),
            None,
        );

        // q2 should be evicted (least recently used)
        assert!(cache.get("q2").is_none());
        assert!(cache.get("q1").is_some());
        assert!(cache.get("q3").is_some());
    }

    #[test]
    fn test_query_cache_lfu_eviction() {
        let cache = QueryCache::new(2).with_policy(EvictionPolicy::Lfu);
        cache.put(
            "q1".into(),
            mgparser::parse_query("RETURN 1").unwrap(),
            None,
        );
        cache.put(
            "q2".into(),
            mgparser::parse_query("RETURN 2").unwrap(),
            None,
        );
        // Access q1 multiple times to boost its frequency
        cache.get("q1");
        cache.get("q1");
        cache.put(
            "q3".into(),
            mgparser::parse_query("RETURN 3").unwrap(),
            None,
        );

        // q2 should be evicted (lowest frequency)
        assert!(cache.get("q2").is_none());
        assert!(cache.get("q1").is_some());
    }

    #[test]
    fn test_query_cache_ttl() {
        let cache = QueryCache::new(10).with_ttl(Duration::from_millis(50));
        let parsed = mgparser::parse_query("RETURN 1").unwrap();
        cache.put("q1".into(), parsed, None);
        assert!(cache.get("q1").is_some());

        std::thread::sleep(Duration::from_millis(60));
        assert!(cache.get("q1").is_none()); // expired
    }

    #[test]
    fn test_query_cache_maintenance() {
        let cache = QueryCache::new(1).with_ttl(Duration::from_millis(1));
        cache.put(
            "q1".into(),
            mgparser::parse_query("RETURN 1").unwrap(),
            None,
        );
        std::thread::sleep(Duration::from_millis(10));
        cache.put(
            "q2".into(),
            mgparser::parse_query("RETURN 2").unwrap(),
            None,
        );
        // q1 should be expired by maintenance during put (capacity exceeded triggers eviction)
        cache.maintenance();
        assert!(cache.get("q1").is_none());
    }

    #[test]
    fn test_prepared_statement() {
        let cache = PreparedStatementCache::new(10);
        let stmt = cache
            .prepare("MATCH (n {name: $name}) RETURN n".into())
            .unwrap();
        assert_eq!(stmt.id, 1);
        assert!(stmt.parameter_names.contains(&"name".into()));

        let retrieved = cache.get(stmt.id).unwrap();
        assert_eq!(retrieved.query_text, stmt.query_text);
    }

    #[test]
    fn test_prepared_statement_deduplication() {
        let cache = PreparedStatementCache::new(10);
        let stmt1 = cache.prepare("RETURN 1".into()).unwrap();
        let stmt2 = cache.prepare("RETURN 1".into()).unwrap();
        assert_eq!(stmt1.id, stmt2.id); // same query, same ID
    }

    #[test]
    fn test_prepared_statement_execution_stats() {
        let cache = PreparedStatementCache::new(10);
        let stmt = cache.prepare("RETURN 1".into()).unwrap();
        cache.record_execution(stmt.id, 100);
        cache.record_execution(stmt.id, 200);
        let updated = cache.get(stmt.id).unwrap();
        assert_eq!(updated.execution_count, 2);
        assert!(updated.avg_duration_ms > 0.0);
    }

    #[test]
    fn test_extract_parameters() {
        assert_eq!(
            extract_parameters("MATCH (n {name: $name, age: $age}) RETURN n"),
            vec!["name", "age"]
        );
        assert!(extract_parameters("MATCH (n) RETURN n").is_empty());
    }

    #[test]
    fn test_bind_parameters() {
        let mut params = HashMap::new();
        params.insert("name".into(), PropertyValue::String("Alice".into()));
        params.insert("age".into(), PropertyValue::Int(30));
        let result = bind_parameters("MATCH (n {name: $name, age: $age}) RETURN n", &params);
        assert!(result.contains("Alice"));
        assert!(result.contains("30"));
    }

    #[test]
    fn test_cache_stats() {
        let cache = QueryCache::new(10);
        let stats = cache.stats();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.hit_rate(), 0.0);
        assert_eq!(stats.policy, EvictionPolicy::Lru);
    }

    #[test]
    fn test_cache_warmer() {
        let warmer = CacheWarmer::new();
        // Just verify it doesn't panic with empty patterns
        let cache = QueryCache::new(10);
        let warmed = warmer.warm(
            &cache,
            &mgstorage::storage::Storage::new(),
            &mgcatalog::Catalog::new(),
        );
        assert_eq!(warmed, 0);
    }
}
