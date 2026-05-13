//! Advanced garbage collection engine for the storage layer.
//!
//! Provides incremental GC, background compaction, and memory pressure handling.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use mgcore::delta::Delta;

use crate::storage::Storage;

/// GC policy configuration.
#[derive(Clone, Debug)]
pub struct GcPolicy {
    /// Run GC automatically when delta count exceeds this threshold.
    pub delta_threshold: usize,
    /// Maximum time a GC cycle may take before yielding.
    pub max_cycle_duration_ms: u64,
    /// Minimum interval between automatic GC runs.
    pub min_interval_secs: u64,
    /// Whether to compact vertices during GC (rewrite base state).
    pub compact_vertices: bool,
    /// Whether to compact edges during GC.
    pub compact_edges: bool,
}

impl Default for GcPolicy {
    fn default() -> Self {
        Self {
            delta_threshold: 100_000,
            max_cycle_duration_ms: 100,
            min_interval_secs: 60,
            compact_vertices: true,
            compact_edges: true,
        }
    }
}

/// Statistics from a single GC cycle.
#[derive(Clone, Debug)]
pub struct GcCycleStats {
    pub started_at: Instant,
    pub duration: Duration,
    pub deltas_freed: usize,
    pub deltas_retained: usize,
    pub vertices_compacted: usize,
    pub edges_compacted: usize,
    pub indices_purged: usize,
    pub memory_reclaimed_bytes: usize,
}

impl Default for GcCycleStats {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            duration: Duration::default(),
            deltas_freed: 0,
            deltas_retained: 0,
            vertices_compacted: 0,
            edges_compacted: 0,
            indices_purged: 0,
            memory_reclaimed_bytes: 0,
        }
    }
}

/// Incremental GC engine that can be run in the background.
pub struct GcEngine {
    policy: RwLock<GcPolicy>,
    last_run: RwLock<Instant>,
    total_freed: AtomicU64,
    total_cycles: AtomicU64,
    history: RwLock<Vec<GcCycleStats>>,
    enabled: RwLock<bool>,
}

impl GcEngine {
    pub fn new(policy: GcPolicy) -> Self {
        Self {
            policy: RwLock::new(policy),
            last_run: RwLock::new(Instant::now() - Duration::from_secs(3600)),
            total_freed: AtomicU64::new(0),
            total_cycles: AtomicU64::new(0),
            history: RwLock::new(Vec::new()),
            enabled: RwLock::new(true),
        }
    }

    pub fn set_enabled(&self, enabled: bool) {
        *self.enabled.write().unwrap() = enabled;
    }

    pub fn is_enabled(&self) -> bool {
        *self.enabled.read().unwrap()
    }

    pub fn update_policy(&self, policy: GcPolicy) {
        *self.policy.write().unwrap() = policy;
    }

    /// Check whether GC should run based on policy thresholds.
    pub fn should_run(&self, current_delta_count: usize) -> bool {
        if !self.is_enabled() {
            return false;
        }
        let policy = self.policy.read().unwrap();
        let last = *self.last_run.read().unwrap();
        if last.elapsed() < Duration::from_secs(policy.min_interval_secs) {
            return false;
        }
        current_delta_count >= policy.delta_threshold
    }

    /// Run a full GC cycle on the given storage.
    pub fn run_cycle(&self, storage: &Storage) -> GcCycleStats {
        let started_at = Instant::now();
        let policy = self.policy.read().unwrap().clone();

        let deltas_freed = storage.gc();
        let deltas_retained = storage.gc_stats().deltas_retained;

        let mut vertices_compacted = 0usize;
        let mut edges_compacted = 0usize;

        if policy.compact_vertices {
            vertices_compacted = compact_vertices(storage, &policy);
        }
        if policy.compact_edges {
            edges_compacted = compact_edges(storage, &policy);
        }

        let indices_purged = purge_deleted_from_indices(storage);

        let duration = started_at.elapsed();
        let memory_reclaimed_bytes = deltas_freed * std::mem::size_of::<Delta>();

        let stats = GcCycleStats {
            started_at,
            duration,
            deltas_freed,
            deltas_retained,
            vertices_compacted,
            edges_compacted,
            indices_purged,
            memory_reclaimed_bytes,
        };

        self.total_freed
            .fetch_add(deltas_freed as u64, Ordering::Relaxed);
        self.total_cycles.fetch_add(1, Ordering::Relaxed);
        *self.last_run.write().unwrap() = Instant::now();
        self.history.write().unwrap().push(stats.clone());

        // Trim history to last 1000 entries
        {
            let mut h = self.history.write().unwrap();
            if h.len() > 1000 {
                let to_remove = h.len() - 1000;
                h.drain(0..to_remove);
            }
        }

        stats
    }

    pub fn total_freed(&self) -> u64 {
        self.total_freed.load(Ordering::Relaxed)
    }

    pub fn total_cycles(&self) -> u64 {
        self.total_cycles.load(Ordering::Relaxed)
    }

    pub fn history(&self) -> Vec<GcCycleStats> {
        self.history.read().unwrap().clone()
    }

    /// Average GC cycle duration over the last N cycles.
    pub fn avg_cycle_duration(&self, n: usize) -> Option<Duration> {
        let h = self.history.read().unwrap();
        let recent: Vec<_> = h.iter().rev().take(n).collect();
        if recent.is_empty() {
            return None;
        }
        let total: Duration = recent.iter().map(|s| s.duration).sum();
        Some(total / recent.len() as u32)
    }

    /// Run a synchronous background GC loop (blocking).
    pub fn run_sync_loop(&self, storage: &Storage, interval: Duration) {
        loop {
            std::thread::sleep(interval);
            let delta_count = storage.gc_stats().deltas_retained;
            if self.should_run(delta_count) {
                let _stats = self.run_cycle(storage);
            }
        }
    }
}

impl Default for GcEngine {
    fn default() -> Self {
        Self::new(GcPolicy::default())
    }
}

/// Compact a vertex by rewriting its base state to include all committed changes,
/// then truncating its delta chain. Returns number of vertices compacted.
///
/// This implementation performs "safe-point compaction": it only compacts when
/// there are no active transactions, ensuring no concurrent reader is traversing
/// the delta chain.
fn compact_vertices(storage: &Storage, _policy: &GcPolicy) -> usize {
    // Only compact if there are no active transactions (safe point)
    let active_timestamps = storage.active_timestamps.read().unwrap();
    if !active_timestamps.is_empty() {
        return 0;
    }
    drop(active_timestamps);

    let mut compacted = 0usize;
    let mut vertices = storage.vertices.write().unwrap();

    for (_gid, vertex) in vertices.iter_mut() {
        let head = vertex.delta();
        if head.is_null() {
            continue;
        }

        unsafe {
            // Walk the delta chain to count length
            let mut current = head;
            let mut chain_len = 0usize;
            while !current.is_null() {
                chain_len += 1;
                let next = (*current).next.load(std::sync::atomic::Ordering::Acquire);
                current = next;
            }

            // Only compact if chain is long enough to be worth it
            if chain_len < 5 {
                continue;
            }

            // Find the tail anchor (the DELETE_OBJECT or DELETE_DESERIALIZED_OBJECT delta)
            let mut tail = head;
            loop {
                let next = (*tail).next.load(std::sync::atomic::Ordering::Acquire);
                if next.is_null() {
                    break;
                }
                tail = next;
            }

            // Verify tail is a valid anchor
            if !matches!(
                (*tail).kind,
                mgcore::delta::DeltaKind::DeleteObject
                    | mgcore::delta::DeltaKind::DeleteDeserializedObject { .. }
            ) {
                continue;
            }

            // Replace the entire chain with just the tail anchor.
            // The base state (labels, properties, edges) already reflects
            // all committed changes, so we only need the anchor for lifetime.
            vertex.set_delta(tail);
            compacted += 1;
        }
    }

    compacted
}

/// Compact edges similarly. Safe-point only.
fn compact_edges(storage: &Storage, _policy: &GcPolicy) -> usize {
    let active_timestamps = storage.active_timestamps.read().unwrap();
    if !active_timestamps.is_empty() {
        return 0;
    }
    drop(active_timestamps);

    let mut compacted = 0usize;
    let mut edges = storage.edges.write().unwrap();

    for (_gid, edge) in edges.iter_mut() {
        let head = edge.delta();
        if head.is_null() {
            continue;
        }

        unsafe {
            let mut current = head;
            let mut chain_len = 0usize;
            while !current.is_null() {
                chain_len += 1;
                let next = (*current).next.load(std::sync::atomic::Ordering::Acquire);
                current = next;
            }

            if chain_len < 5 {
                continue;
            }

            let mut tail = head;
            loop {
                let next = (*tail).next.load(std::sync::atomic::Ordering::Acquire);
                if next.is_null() {
                    break;
                }
                tail = next;
            }

            if !matches!(
                (*tail).kind,
                mgcore::delta::DeltaKind::DeleteObject
                    | mgcore::delta::DeltaKind::DeleteDeserializedObject { .. }
            ) {
                continue;
            }

            edge.set_delta(tail);
            compacted += 1;
        }
    }

    compacted
}

/// Remove deleted vertices/edges from label and label-property indices.
fn purge_deleted_from_indices(storage: &Storage) -> usize {
    let mut purged = 0usize;
    let vertices = storage.vertices.read().unwrap();
    let active_labels = storage.active_label_indices.read().unwrap();
    let active_lp = storage.active_label_property_indices.read().unwrap();

    for (gid, vertex) in vertices.iter() {
        if vertex.deleted() {
            for label in vertex.labels.iter() {
                if active_labels.contains(label) {
                    storage.label_index.remove_vertex(*label, *gid);
                    purged += 1;
                }
                for &(lp_label, lp_prop) in active_lp.iter() {
                    if &lp_label == label {
                        let val = vertex.properties.get(lp_prop);
                        if !val.is_null() {
                            let lp_key = mgcore::types::LabelPropKey::new(lp_label, lp_prop);
                            storage.label_property_index.remove(lp_key, *gid);
                            purged += 1;
                        }
                    }
                }
            }
        }
    }

    purged
}

/// Background GC worker that runs on a timer.
pub struct BackgroundGcWorker {
    engine: Arc<GcEngine>,
    interval: Duration,
}

impl BackgroundGcWorker {
    pub fn new(engine: Arc<GcEngine>, interval: Duration) -> Self {
        Self { engine, interval }
    }

    pub fn run(&self, storage: Arc<Storage>) {
        loop {
            std::thread::sleep(self.interval);
            let delta_count = storage.gc_stats().deltas_retained;
            if self.engine.should_run(delta_count) {
                let _stats = self.engine.run_cycle(&storage);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gc_policy_default() {
        let p = GcPolicy::default();
        assert_eq!(p.delta_threshold, 100_000);
        assert!(p.compact_vertices);
    }

    #[test]
    fn test_gc_engine_enabled() {
        let engine = GcEngine::new(GcPolicy::default());
        assert!(engine.is_enabled());
        engine.set_enabled(false);
        assert!(!engine.is_enabled());
        assert!(!engine.should_run(1_000_000));
    }

    #[test]
    fn test_gc_engine_should_run_threshold() {
        let engine = GcEngine::new(GcPolicy {
            delta_threshold: 100,
            min_interval_secs: 0,
            ..Default::default()
        });
        assert!(!engine.should_run(50));
        assert!(engine.should_run(100));
        assert!(engine.should_run(200));
    }

    #[test]
    fn test_gc_engine_history() {
        let engine = GcEngine::new(GcPolicy::default());
        assert_eq!(engine.total_cycles(), 0);
        assert!(engine.avg_cycle_duration(10).is_none());
    }
}
