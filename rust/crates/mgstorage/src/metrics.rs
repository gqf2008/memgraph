//! Storage-level metrics and operation counters.
//!
//! Tracks throughput, latency, and resource usage for observability.
//! Exposed via HTTP /metrics and internal diagnostics.

use std::sync::atomic::{AtomicU64, Ordering};

/// Counters for storage operations.
pub struct StorageMetrics {
    pub vertices_created: AtomicU64,
    pub vertices_deleted: AtomicU64,
    pub vertices_updated: AtomicU64,
    pub edges_created: AtomicU64,
    pub edges_deleted: AtomicU64,
    pub edges_updated: AtomicU64,
    pub properties_set: AtomicU64,
    pub properties_removed: AtomicU64,
    pub labels_added: AtomicU64,
    pub labels_removed: AtomicU64,
    pub transactions_committed: AtomicU64,
    pub transactions_aborted: AtomicU64,
    pub gc_deltas_collected: AtomicU64,
    pub ttl_vertices_expired: AtomicU64,
    pub index_queries: AtomicU64,
    pub full_scans: AtomicU64,
}

impl StorageMetrics {
    pub fn new() -> Self {
        Self {
            vertices_created: AtomicU64::new(0),
            vertices_deleted: AtomicU64::new(0),
            vertices_updated: AtomicU64::new(0),
            edges_created: AtomicU64::new(0),
            edges_deleted: AtomicU64::new(0),
            edges_updated: AtomicU64::new(0),
            properties_set: AtomicU64::new(0),
            properties_removed: AtomicU64::new(0),
            labels_added: AtomicU64::new(0),
            labels_removed: AtomicU64::new(0),
            transactions_committed: AtomicU64::new(0),
            transactions_aborted: AtomicU64::new(0),
            gc_deltas_collected: AtomicU64::new(0),
            ttl_vertices_expired: AtomicU64::new(0),
            index_queries: AtomicU64::new(0),
            full_scans: AtomicU64::new(0),
        }
    }

    pub fn inc_vertices_created(&self, n: u64) {
        self.vertices_created.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_vertices_deleted(&self, n: u64) {
        self.vertices_deleted.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_vertices_updated(&self, n: u64) {
        self.vertices_updated.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_edges_created(&self, n: u64) {
        self.edges_created.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_edges_deleted(&self, n: u64) {
        self.edges_deleted.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_edges_updated(&self, n: u64) {
        self.edges_updated.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_properties_set(&self, n: u64) {
        self.properties_set.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_properties_removed(&self, n: u64) {
        self.properties_removed.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_labels_added(&self, n: u64) {
        self.labels_added.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_labels_removed(&self, n: u64) {
        self.labels_removed.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_transactions_committed(&self, n: u64) {
        self.transactions_committed.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_transactions_aborted(&self, n: u64) {
        self.transactions_aborted.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_gc_deltas(&self, n: u64) {
        self.gc_deltas_collected.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_ttl_expired(&self, n: u64) {
        self.ttl_vertices_expired.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_index_queries(&self, n: u64) {
        self.index_queries.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_full_scans(&self, n: u64) {
        self.full_scans.fetch_add(n, Ordering::Relaxed);
    }

    /// Return a snapshot of all counters.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            vertices_created: self.vertices_created.load(Ordering::Relaxed),
            vertices_deleted: self.vertices_deleted.load(Ordering::Relaxed),
            vertices_updated: self.vertices_updated.load(Ordering::Relaxed),
            edges_created: self.edges_created.load(Ordering::Relaxed),
            edges_deleted: self.edges_deleted.load(Ordering::Relaxed),
            edges_updated: self.edges_updated.load(Ordering::Relaxed),
            properties_set: self.properties_set.load(Ordering::Relaxed),
            properties_removed: self.properties_removed.load(Ordering::Relaxed),
            labels_added: self.labels_added.load(Ordering::Relaxed),
            labels_removed: self.labels_removed.load(Ordering::Relaxed),
            transactions_committed: self.transactions_committed.load(Ordering::Relaxed),
            transactions_aborted: self.transactions_aborted.load(Ordering::Relaxed),
            gc_deltas_collected: self.gc_deltas_collected.load(Ordering::Relaxed),
            ttl_vertices_expired: self.ttl_vertices_expired.load(Ordering::Relaxed),
            index_queries: self.index_queries.load(Ordering::Relaxed),
            full_scans: self.full_scans.load(Ordering::Relaxed),
        }
    }
}

impl Default for StorageMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-only snapshot of metrics.
#[derive(Clone, Debug, Default)]
pub struct MetricsSnapshot {
    pub vertices_created: u64,
    pub vertices_deleted: u64,
    pub vertices_updated: u64,
    pub edges_created: u64,
    pub edges_deleted: u64,
    pub edges_updated: u64,
    pub properties_set: u64,
    pub properties_removed: u64,
    pub labels_added: u64,
    pub labels_removed: u64,
    pub transactions_committed: u64,
    pub transactions_aborted: u64,
    pub gc_deltas_collected: u64,
    pub ttl_vertices_expired: u64,
    pub index_queries: u64,
    pub full_scans: u64,
}

impl MetricsSnapshot {
    /// Export as Prometheus-style text.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();
        macro_rules! gauge {
            ($name:expr, $help:expr, $val:expr) => {
                out.push_str(&format!(
                    "# HELP memgraph_{} {}\n# TYPE memgraph_{} gauge\nmemgraph_{} {}\n",
                    $name, $help, $name, $name, $val
                ));
            };
        }
        gauge!(
            "vertices_created_total",
            "Total vertices created.",
            self.vertices_created
        );
        gauge!(
            "vertices_deleted_total",
            "Total vertices deleted.",
            self.vertices_deleted
        );
        gauge!(
            "edges_created_total",
            "Total edges created.",
            self.edges_created
        );
        gauge!(
            "edges_deleted_total",
            "Total edges deleted.",
            self.edges_deleted
        );
        gauge!(
            "transactions_committed_total",
            "Total committed transactions.",
            self.transactions_committed
        );
        gauge!(
            "transactions_aborted_total",
            "Total aborted transactions.",
            self.transactions_aborted
        );
        gauge!(
            "gc_deltas_collected_total",
            "Total GC deltas collected.",
            self.gc_deltas_collected
        );
        gauge!(
            "ttl_vertices_expired_total",
            "Total TTL-expired vertices.",
            self.ttl_vertices_expired
        );
        gauge!(
            "index_queries_total",
            "Total index queries.",
            self.index_queries
        );
        gauge!("full_scans_total", "Total full scans.", self.full_scans);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_counters() {
        let m = StorageMetrics::new();
        m.inc_vertices_created(5);
        m.inc_edges_created(3);
        m.inc_transactions_committed(1);

        let snap = m.snapshot();
        assert_eq!(snap.vertices_created, 5);
        assert_eq!(snap.edges_created, 3);
        assert_eq!(snap.transactions_committed, 1);
    }

    #[test]
    fn test_prometheus_output() {
        let m = StorageMetrics::new();
        m.inc_vertices_created(10);
        let snap = m.snapshot();
        let prom = snap.to_prometheus();
        assert!(prom.contains("memgraph_vertices_created_total"));
        assert!(prom.contains("10"));
    }
}
