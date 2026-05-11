//! Query result and vertex cache for the storage layer.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// Simple LRU cache with a fixed capacity.
pub struct LruCache<K, V> {
    map: HashMap<K, (V, usize)>,
    order: Vec<K>,
    capacity: usize,
    next_seq: usize,
}

impl<K: Clone + Eq + Hash, V: Clone> LruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            order: Vec::with_capacity(capacity),
            capacity,
            next_seq: 0,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        let found = self.map.contains_key(key);
        if found {
            let v = self.map.get(key).unwrap().0.clone();
            self.touch(key);
            Some(v)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if self.map.contains_key(&key) {
            self.map.insert(key.clone(), (value, self.next_seq));
            self.next_seq += 1;
            self.touch(&key);
            return;
        }

        if self.map.len() >= self.capacity {
            // Evict oldest
            if let Some(oldest) = self.order.first().cloned() {
                self.map.remove(&oldest);
                self.order.remove(0);
            }
        }

        self.map.insert(key.clone(), (value, self.next_seq));
        self.next_seq += 1;
        self.order.push(key);
    }

    fn touch(&mut self, key: &K) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            let k = self.order.remove(pos);
            self.order.push(k);
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }
}

/// Thread-safe query result cache.
pub struct QueryCache {
    inner: Mutex<LruCache<String, Arc<Vec<HashMap<String, mgcore::property_value::PropertyValue>>>>>,
    hits: Mutex<u64>,
    misses: Mutex<u64>,
}

impl QueryCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(LruCache::new(capacity)),
            hits: Mutex::new(0),
            misses: Mutex::new(0),
        }
    }

    pub fn get(&self, query: &str) -> Option<Arc<Vec<HashMap<String, mgcore::property_value::PropertyValue>>>> {
        let mut cache = self.inner.lock().unwrap();
        let result = cache.get(&query.to_string());
        drop(cache);
        if result.is_some() {
            *self.hits.lock().unwrap() += 1;
        } else {
            *self.misses.lock().unwrap() += 1;
        }
        result
    }

    pub fn put(&self, query: &str, result: Arc<Vec<HashMap<String, mgcore::property_value::PropertyValue>>>) {
        self.inner.lock().unwrap().put(query.to_string(), result);
    }

    pub fn invalidate(&self, _query: &str) {
        let mut cache = self.inner.lock().unwrap();
        // LruCache doesn't support removal by key; clear all for simplicity
        cache.clear();
        drop(cache);
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = *self.hits.lock().unwrap();
        let misses = *self.misses.lock().unwrap();
        let total = hits + misses;
        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }
}

/// Prepared statement cache for parameterized queries.
pub struct PreparedStatementCache {
    statements: Mutex<HashMap<String, PreparedStatement>>,
}

#[derive(Clone, Debug)]
pub struct PreparedStatement {
    pub query_text: String,
    pub param_count: usize,
    pub created_at: std::time::Instant,
    pub use_count: Arc<Mutex<u64>>,
}

impl PreparedStatementCache {
    pub fn new() -> Self {
        Self {
            statements: Mutex::new(HashMap::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<PreparedStatement> {
        let stmts = self.statements.lock().unwrap();
        stmts.get(key).cloned().map(|stmt| {
            *stmt.use_count.lock().unwrap() += 1;
            stmt
        })
    }

    pub fn put(&self, key: &str, stmt: PreparedStatement) {
        self.statements.lock().unwrap().insert(key.to_string(), stmt);
    }

    pub fn remove(&self, key: &str) {
        self.statements.lock().unwrap().remove(key);
    }

    pub fn len(&self) -> usize {
        self.statements.lock().unwrap().len()
    }

    pub fn clear(&self) {
        self.statements.lock().unwrap().clear();
    }

    /// Evict least-used statements to keep cache under max_size.
    pub fn evict_if_needed(&self, max_size: usize) {
        let mut stmts = self.statements.lock().unwrap();
        while stmts.len() > max_size {
            let to_remove = stmts.iter()
                .min_by_key(|(_, stmt)| *stmt.use_count.lock().unwrap())
                .map(|(k, _)| k.clone());
            if let Some(k) = to_remove {
                stmts.remove(&k);
            } else {
                break;
            }
        }
    }
}

impl Default for PreparedStatementCache {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let mut cache = LruCache::new(2);
        cache.put("a", 1);
        cache.put("b", 2);
        assert_eq!(cache.get(&"a"), Some(1));
        cache.put("c", 3); // evicts "b"
        assert_eq!(cache.get(&"b"), None);
        assert_eq!(cache.get(&"c"), Some(3));
    }

    #[test]
    fn test_query_cache_hit_rate() {
        let cache = QueryCache::new(10);
        assert_eq!(cache.hit_rate(), 0.0);
        cache.get("SELECT *"); // miss
        assert!(cache.hit_rate() < 1.0);
    }

    #[test]
    fn test_prepared_statement_cache() {
        let cache = PreparedStatementCache::new();
        let stmt = PreparedStatement {
            query_text: "MATCH (n) RETURN n".into(),
            param_count: 0,
            created_at: std::time::Instant::now(),
            use_count: Arc::new(Mutex::new(0)),
        };
        cache.put("stmt1", stmt);
        assert_eq!(cache.len(), 1);
        let retrieved = cache.get("stmt1").unwrap();
        assert_eq!(retrieved.query_text, "MATCH (n) RETURN n");
        assert_eq!(*retrieved.use_count.lock().unwrap(), 1);
    }
}
