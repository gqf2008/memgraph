//! Server administration: connections, queries, metrics, health.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// Unique connection ID.
pub type ConnectionId = u64;

/// Active query tracking.
#[derive(Clone, Debug)]
pub struct ActiveQuery {
    pub query_id: u64,
    pub connection_id: ConnectionId,
    pub query_text: String,
    pub started_at: Instant,
}

/// Connection metadata.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    pub id: ConnectionId,
    pub peer_addr: String,
    pub connected_at: Instant,
    pub user: Option<String>,
    pub client_name: String,
    pub bolt_version: (u8, u8),
}

/// Server metrics (Prometheus-compatible).
#[derive(Debug, Default)]
pub struct ServerMetrics {
    pub total_queries: AtomicU64,
    pub total_transactions: AtomicU64,
    pub failed_queries: AtomicU64,
    pub active_connections: AtomicU64,
    pub bytes_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub query_duration_ms_total: AtomicU64,
}

impl Clone for ServerMetrics {
    fn clone(&self) -> Self {
        Self {
            total_queries: AtomicU64::new(self.total_queries.load(Ordering::Relaxed)),
            total_transactions: AtomicU64::new(self.total_transactions.load(Ordering::Relaxed)),
            failed_queries: AtomicU64::new(self.failed_queries.load(Ordering::Relaxed)),
            active_connections: AtomicU64::new(self.active_connections.load(Ordering::Relaxed)),
            bytes_received: AtomicU64::new(self.bytes_received.load(Ordering::Relaxed)),
            bytes_sent: AtomicU64::new(self.bytes_sent.load(Ordering::Relaxed)),
            query_duration_ms_total: AtomicU64::new(
                self.query_duration_ms_total.load(Ordering::Relaxed),
            ),
        }
    }
}

impl ServerMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_queries(&self) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_transactions(&self) {
        self.total_transactions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failed(&self) {
        self.failed_queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_bytes_received(&self, n: u64) {
        self.bytes_received.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_bytes_sent(&self, n: u64) {
        self.bytes_sent.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_query_duration(&self, ms: u64) {
        self.query_duration_ms_total
            .fetch_add(ms, Ordering::Relaxed);
    }

    /// Render as Prometheus exposition format.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();
        out.push_str("# TYPE memgraph_queries_total counter\n");
        out.push_str(&format!(
            "memgraph_queries_total {}\n",
            self.total_queries.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE memgraph_transactions_total counter\n");
        out.push_str(&format!(
            "memgraph_transactions_total {}\n",
            self.total_transactions.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE memgraph_failed_queries_total counter\n");
        out.push_str(&format!(
            "memgraph_failed_queries_total {}\n",
            self.failed_queries.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE memgraph_active_connections gauge\n");
        out.push_str(&format!(
            "memgraph_active_connections {}\n",
            self.active_connections.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE memgraph_bytes_received_total counter\n");
        out.push_str(&format!(
            "memgraph_bytes_received_total {}\n",
            self.bytes_received.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE memgraph_bytes_sent_total counter\n");
        out.push_str(&format!(
            "memgraph_bytes_sent_total {}\n",
            self.bytes_sent.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE memgraph_query_duration_ms_total counter\n");
        out.push_str(&format!(
            "memgraph_query_duration_ms_total {}\n",
            self.query_duration_ms_total.load(Ordering::Relaxed)
        ));
        out
    }
}

/// Admin state shared across the server.
pub struct AdminState {
    next_connection_id: AtomicU64,
    next_query_id: AtomicU64,
    connections: RwLock<HashMap<ConnectionId, ConnectionInfo>>,
    active_queries: RwLock<HashMap<u64, ActiveQuery>>,
    metrics: ServerMetrics,
    kill_flags: Mutex<HashMap<u64, AtomicBool>>,
    /// Query text fingerprint → aggregated stats.
    query_stats: RwLock<HashMap<String, QueryStats>>,
    /// Slow query log (circular buffer).
    slow_queries: Mutex<Vec<SlowQueryEntry>>,
    /// Runtime configuration.
    config: RwLock<ServerConfig>,
    /// Rate limiter for client IPs.
    rate_limiter: RateLimiter,
    /// Per-user connection quota.
    user_quota: UserQuota,
    /// Server start time for uptime tracking.
    start_time: Instant,
}

impl AdminState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::with_config(ServerConfig::default()))
    }

    pub fn with_config(config: ServerConfig) -> Self {
        let rate_limiter =
            RateLimiter::new(config.rate_limit_max_tokens, config.rate_limit_refill_secs);
        let user_quota = UserQuota::new(config.max_connections_per_user);
        Self {
            next_connection_id: AtomicU64::new(1),
            next_query_id: AtomicU64::new(1),
            connections: RwLock::new(HashMap::new()),
            active_queries: RwLock::new(HashMap::new()),
            metrics: ServerMetrics::new(),
            kill_flags: Mutex::new(HashMap::new()),
            query_stats: RwLock::new(HashMap::new()),
            slow_queries: Mutex::new(Vec::new()),
            config: RwLock::new(config),
            rate_limiter,
            user_quota,
            start_time: Instant::now(),
        }
    }

    pub fn metrics(&self) -> &ServerMetrics {
        &self.metrics
    }

    pub fn config(&self) -> ServerConfig {
        self.config.read().unwrap().clone()
    }

    pub fn update_config(&self, new_config: ServerConfig) {
        *self.config.write().unwrap() = new_config;
    }

    /// Register a new connection and return its ID.
    pub fn register_connection(
        &self,
        peer: String,
        user: Option<String>,
        client_name: String,
        bolt_version: (u8, u8),
    ) -> ConnectionId {
        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let info = ConnectionInfo {
            id,
            peer_addr: peer,
            connected_at: Instant::now(),
            user,
            client_name,
            bolt_version,
        };
        self.connections.write().unwrap().insert(id, info);
        self.metrics.inc_connections();
        id
    }

    pub fn unregister_connection(&self, id: ConnectionId) {
        self.connections.write().unwrap().remove(&id);
        self.metrics.dec_connections();
    }

    pub fn list_connections(&self) -> Vec<ConnectionInfo> {
        self.connections.read().unwrap().values().cloned().collect()
    }

    pub fn connection_count(&self) -> usize {
        self.connections.read().unwrap().len()
    }

    /// Start tracking a query. Returns the query ID.
    pub fn start_query(&self, conn_id: ConnectionId, query: String) -> u64 {
        let qid = self.next_query_id.fetch_add(1, Ordering::Relaxed);
        let aq = ActiveQuery {
            query_id: qid,
            connection_id: conn_id,
            query_text: query,
            started_at: Instant::now(),
        };
        self.active_queries.write().unwrap().insert(qid, aq);
        self.kill_flags
            .lock()
            .unwrap()
            .insert(qid, AtomicBool::new(false));
        self.metrics.inc_queries();
        qid
    }

    pub fn finish_query(&self, qid: u64) {
        if let Some(aq) = self.active_queries.write().unwrap().remove(&qid) {
            let elapsed = aq.started_at.elapsed().as_millis() as u64;
            self.metrics.record_query_duration(elapsed);
            // Record per-query-pattern stats
            let fingerprint = query_fingerprint(&aq.query_text);
            self.query_stats
                .write()
                .unwrap()
                .entry(fingerprint)
                .or_default()
                .record(elapsed, false);
            // Slow query log
            let cfg = self.config.read().unwrap();
            if cfg.slow_query_threshold_ms > 0 && elapsed >= cfg.slow_query_threshold_ms {
                let entry = SlowQueryEntry {
                    query_id: qid,
                    connection_id: aq.connection_id,
                    query_text: aq.query_text,
                    duration_ms: elapsed,
                    timestamp: std::time::SystemTime::now(),
                };
                let mut slow = self.slow_queries.lock().unwrap();
                slow.push(entry);
                if slow.len() > cfg.slow_query_max_entries {
                    slow.remove(0);
                }
            }
        }
        self.kill_flags.lock().unwrap().remove(&qid);
    }

    pub fn fail_query(&self, qid: u64) {
        if let Some(aq) = self.active_queries.write().unwrap().remove(&qid) {
            let elapsed = aq.started_at.elapsed().as_millis() as u64;
            self.metrics.record_query_duration(elapsed);
            let fingerprint = query_fingerprint(&aq.query_text);
            self.query_stats
                .write()
                .unwrap()
                .entry(fingerprint)
                .or_default()
                .record(elapsed, true);
        }
        self.kill_flags.lock().unwrap().remove(&qid);
        self.metrics.inc_failed();
    }

    pub fn list_active_queries(&self) -> Vec<ActiveQuery> {
        self.active_queries
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    pub fn kill_query(&self, qid: u64) -> bool {
        let flags = self.kill_flags.lock().unwrap();
        if let Some(flag) = flags.get(&qid) {
            flag.store(true, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn is_query_killed(&self, qid: u64) -> bool {
        let flags = self.kill_flags.lock().unwrap();
        flags
            .get(&qid)
            .map(|f: &AtomicBool| f.load(Ordering::Relaxed))
            .unwrap_or(false)
    }

    /// Return aggregated query statistics.
    pub fn query_stats(&self) -> HashMap<String, QueryStats> {
        self.query_stats.read().unwrap().clone()
    }

    /// Return slow query log entries (newest first).
    pub fn slow_queries(&self) -> Vec<SlowQueryEntry> {
        let slow = self.slow_queries.lock().unwrap();
        slow.iter().rev().cloned().collect()
    }

    /// Check if a client IP is rate-limited.
    pub fn check_rate_limit(&self, ip: &str) -> bool {
        self.rate_limiter.allow(ip)
    }

    /// Try to acquire a connection slot for a user.
    pub fn try_acquire_user_connection(&self, user: &str) -> bool {
        self.user_quota.try_acquire(user)
    }

    pub fn release_user_connection(&self, user: &str) {
        self.user_quota.release(user);
    }

    /// Health check: server is healthy if it can serve connections.
    pub fn health_check(&self) -> HealthStatus {
        HealthStatus {
            healthy: true,
            active_connections: self.connection_count(),
            active_queries: self.active_queries.read().unwrap().len(),
            uptime_secs: self.start_time.elapsed().as_secs(),
        }
    }
}

/// Create a simple fingerprint from a query text (lowercase, strip extra whitespace).
fn query_fingerprint(query: &str) -> String {
    let normalized: String = query
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    // Truncate very long queries
    if normalized.len() > 256 {
        normalized[..256].to_string()
    } else {
        normalized
    }
}

/// Query statistics for a single query pattern (text fingerprint).
#[derive(Clone, Debug, Default)]
pub struct QueryStats {
    pub count: u64,
    pub total_duration_ms: u64,
    pub max_duration_ms: u64,
    pub min_duration_ms: u64,
    pub failed_count: u64,
}

impl QueryStats {
    pub fn record(&mut self, duration_ms: u64, failed: bool) {
        self.count += 1;
        self.total_duration_ms += duration_ms;
        if duration_ms > self.max_duration_ms {
            self.max_duration_ms = duration_ms;
        }
        if self.min_duration_ms == 0 || duration_ms < self.min_duration_ms {
            self.min_duration_ms = duration_ms;
        }
        if failed {
            self.failed_count += 1;
        }
    }

    pub fn avg_duration_ms(&self) -> u64 {
        if self.count > 0 {
            self.total_duration_ms / self.count
        } else {
            0
        }
    }
}

/// Slow query log entry.
#[derive(Clone, Debug)]
pub struct SlowQueryEntry {
    pub query_id: u64,
    pub connection_id: ConnectionId,
    pub query_text: String,
    pub duration_ms: u64,
    pub timestamp: std::time::SystemTime,
}

/// Rate limiter for connections (token bucket per client IP).
pub struct RateLimiter {
    tokens: Mutex<HashMap<String, (u64, Instant)>>,
    max_tokens: u64,
    refill_interval: Duration,
}

impl RateLimiter {
    pub fn new(max_tokens: u64, refill_interval_secs: u64) -> Self {
        Self {
            tokens: Mutex::new(HashMap::new()),
            max_tokens,
            refill_interval: Duration::from_secs(refill_interval_secs),
        }
    }

    pub fn allow(&self, key: &str) -> bool {
        let mut tokens = self.tokens.lock().unwrap();
        let now = Instant::now();
        let entry = tokens
            .entry(key.to_string())
            .or_insert((self.max_tokens, now));
        let elapsed = now.duration_since(entry.1);
        let tokens_to_add = (elapsed.as_secs() / self.refill_interval.as_secs()) * self.max_tokens;
        entry.0 = (entry.0 + tokens_to_add).min(self.max_tokens);
        entry.1 = now;
        if entry.0 > 0 {
            entry.0 -= 1;
            true
        } else {
            false
        }
    }

    pub fn reset(&self, key: &str) {
        let mut tokens = self.tokens.lock().unwrap();
        tokens.remove(key);
    }
}

/// User connection quota tracker.
pub struct UserQuota {
    max_connections_per_user: usize,
    connections: Mutex<HashMap<String, usize>>,
}

impl UserQuota {
    pub fn new(max_connections_per_user: usize) -> Self {
        Self {
            max_connections_per_user,
            connections: Mutex::new(HashMap::new()),
        }
    }

    pub fn try_acquire(&self, user: &str) -> bool {
        let mut conns = self.connections.lock().unwrap();
        let count = conns.entry(user.to_string()).or_insert(0);
        if *count < self.max_connections_per_user {
            *count += 1;
            true
        } else {
            false
        }
    }

    pub fn release(&self, user: &str) {
        let mut conns = self.connections.lock().unwrap();
        if let Some(count) = conns.get_mut(user) {
            if *count > 0 {
                *count -= 1;
            }
            if *count == 0 {
                conns.remove(user);
            }
        }
    }

    pub fn current_for(&self, user: &str) -> usize {
        self.connections
            .lock()
            .unwrap()
            .get(user)
            .copied()
            .unwrap_or(0)
    }
}

/// Server configuration snapshot (for runtime reload support).
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub max_connections: usize,
    pub query_timeout_ms: u64,
    pub slow_query_threshold_ms: u64,
    pub slow_query_max_entries: usize,
    pub rate_limit_max_tokens: u64,
    pub rate_limit_refill_secs: u64,
    pub max_connections_per_user: usize,
    pub gc_interval_secs: u64,
    pub wal_sync_on_commit: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            query_timeout_ms: 0, // 0 = no timeout
            slow_query_threshold_ms: 1000,
            slow_query_max_entries: 100,
            rate_limit_max_tokens: 100,
            rate_limit_refill_secs: 1,
            max_connections_per_user: 100,
            gc_interval_secs: 30,
            wal_sync_on_commit: false,
        }
    }
}

/// Health check response.
#[derive(Clone, Debug)]
pub struct HealthStatus {
    pub healthy: bool,
    pub active_connections: usize,
    pub active_queries: usize,
    pub uptime_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_connection() {
        let admin = AdminState::new();
        let id = admin.register_connection(
            "127.0.0.1:7687".into(),
            Some("admin".into()),
            "neo4j-python/5.0".into(),
            (4, 4),
        );
        assert_eq!(admin.connection_count(), 1);
        let conns = admin.list_connections();
        assert_eq!(conns[0].id, id);
        assert_eq!(conns[0].user, Some("admin".into()));
    }

    #[test]
    fn test_unregister_connection() {
        let admin = AdminState::new();
        let id = admin.register_connection("127.0.0.1:7687".into(), None, "test".into(), (5, 2));
        admin.unregister_connection(id);
        assert_eq!(admin.connection_count(), 0);
    }

    #[test]
    fn test_query_lifecycle() {
        let admin = AdminState::new();
        let cid = admin.register_connection("127.0.0.1:7687".into(), None, "test".into(), (5, 2));
        let qid = admin.start_query(cid, "MATCH (n) RETURN n".into());
        assert_eq!(admin.list_active_queries().len(), 1);
        admin.finish_query(qid);
        assert_eq!(admin.list_active_queries().len(), 0);
    }

    #[test]
    fn test_kill_query() {
        let admin = AdminState::new();
        let cid = admin.register_connection("127.0.0.1:7687".into(), None, "test".into(), (5, 2));
        let qid = admin.start_query(cid, "MATCH (n) RETURN n".into());
        assert!(!admin.is_query_killed(qid));
        assert!(admin.kill_query(qid));
        assert!(admin.is_query_killed(qid));
    }

    #[test]
    fn test_metrics_counters() {
        let m = ServerMetrics::new();
        m.inc_queries();
        m.inc_queries();
        m.inc_failed();
        m.inc_connections();
        assert_eq!(m.total_queries.load(Ordering::Relaxed), 2);
        assert_eq!(m.failed_queries.load(Ordering::Relaxed), 1);
        assert_eq!(m.active_connections.load(Ordering::Relaxed), 1);
        let prom = m.to_prometheus();
        assert!(prom.contains("memgraph_queries_total 2"));
        assert!(prom.contains("memgraph_failed_queries_total 1"));
    }

    #[test]
    fn test_health_check() {
        let admin = AdminState::new();
        admin.register_connection("127.0.0.1:7687".into(), None, "test".into(), (5, 2));
        let health = admin.health_check();
        assert!(health.healthy);
        assert_eq!(health.active_connections, 1);
    }

    #[test]
    fn test_query_stats_tracking() {
        let admin = AdminState::new();
        let cid = admin.register_connection("127.0.0.1:7687".into(), None, "test".into(), (5, 2));
        let qid = admin.start_query(cid, "MATCH (n) RETURN n".into());
        std::thread::sleep(Duration::from_millis(10));
        admin.finish_query(qid);

        let stats = admin.query_stats();
        assert_eq!(stats.len(), 1);
        let qs = stats.values().next().unwrap();
        assert_eq!(qs.count, 1);
        assert!(qs.total_duration_ms >= 10);
    }

    #[test]
    fn test_slow_query_log() {
        let mut cfg = ServerConfig::default();
        cfg.slow_query_threshold_ms = 1;
        let admin = AdminState::with_config(cfg);
        let cid = admin.register_connection("127.0.0.1:7687".into(), None, "test".into(), (5, 2));
        let qid = admin.start_query(cid, "MATCH (n) RETURN n".into());
        std::thread::sleep(Duration::from_millis(10));
        admin.finish_query(qid);

        let slow = admin.slow_queries();
        assert_eq!(slow.len(), 1);
        assert_eq!(slow[0].query_text, "MATCH (n) RETURN n");
    }

    #[test]
    fn test_rate_limiter() {
        let rl = RateLimiter::new(2, 1);
        assert!(rl.allow("127.0.0.1"));
        assert!(rl.allow("127.0.0.1"));
        assert!(!rl.allow("127.0.0.1")); // exhausted
        assert!(rl.allow("192.168.1.1")); // different key
        rl.reset("127.0.0.1");
        assert!(rl.allow("127.0.0.1"));
    }

    #[test]
    fn test_user_quota() {
        let quota = UserQuota::new(2);
        assert!(quota.try_acquire("alice"));
        assert!(quota.try_acquire("alice"));
        assert!(!quota.try_acquire("alice")); // exhausted
        assert!(quota.try_acquire("bob")); // different user
        assert_eq!(quota.current_for("alice"), 2);
        quota.release("alice");
        assert_eq!(quota.current_for("alice"), 1);
        assert!(quota.try_acquire("alice")); // now available
    }

    #[test]
    fn test_query_fingerprint_normalization() {
        assert_eq!(
            query_fingerprint("MATCH (n) RETURN n"),
            "match (n) return n"
        );
        assert_eq!(
            query_fingerprint("  MATCH   (n)\nRETURN  n  "),
            "match (n) return n"
        );
    }

    #[test]
    fn test_config_reload() {
        let admin = AdminState::new();
        assert_eq!(admin.config().max_connections, 1000);
        let mut new_cfg = ServerConfig::default();
        new_cfg.max_connections = 500;
        admin.update_config(new_cfg);
        assert_eq!(admin.config().max_connections, 500);
    }
}
