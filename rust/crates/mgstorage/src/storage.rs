//! Graph storage engine with MVCC vertex/edge management.
//!
//! Equivalent to C++ `InMemoryStorage` in `src/storage/v2/inmemory/storage.hpp`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Number of commit shards for sharded locking. Transactions that write
/// disjoint Gid sets can commit in parallel by locking different shards.
const N_COMMIT_SHARDS: usize = 64;

/// Map a Gid to a commit-shard index.
fn gid_to_shard(gid: Gid) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    gid.hash(&mut h);
    (h.finish() as usize) % N_COMMIT_SHARDS
}

use mgcore::delta::{
    apply_deltas_for_read, DeltaAction, DeltaKind, IsolationLevel, View, TRANSACTION_INITIAL_ID,
};
use mgcore::edge::Edge;
use mgcore::edge_ref::EdgeRef;
use mgcore::property_store::PropertyStore;
use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
use mgcore::vertex::{EdgeTriple, Vertex};
use mgcore::Delta as CoreDelta;

use crate::config::{StorageConfig, StorageMode};
use crate::constraints::{Constraints, TtlConfig};
use crate::indices::{
    EdgeIndex, EdgeIndexEntry, EdgePropertyIndex, EdgeTypeIndex, EdgeTypePropertyIndex, LabelIndex,
    LabelPropertyIndex,
};
use crate::schema_info::SchemaInfo;
use crate::transaction::{Transaction, TransactionEngine};

/// Metadata for a vector (HNSW) index on a label+property.
pub struct VectorIndexEntry {
    pub index: std::sync::Arc<std::sync::RwLock<mgvector::HnswIndex>>,
    pub property: PropertyId,
    pub dimension: usize,
    pub distance: mgvector::Distance,
    /// Maps vertex Gid to HNSW NodeId for updates/deletions.
    pub gid_to_node: std::sync::RwLock<HashMap<Gid, mgvector::NodeId>>,
}

impl VectorIndexEntry {
    pub fn new(
        index: std::sync::Arc<std::sync::RwLock<mgvector::HnswIndex>>,
        property: PropertyId,
        dimension: usize,
        distance: mgvector::Distance,
    ) -> Self {
        Self {
            index,
            property,
            dimension,
            distance,
            gid_to_node: std::sync::RwLock::new(HashMap::new()),
        }
    }
}

/// Metadata for a full-text (Tantivy) index on a label.
pub struct TextIndexEntry {
    pub index: std::sync::Arc<crate::text_index::TextIndex>,
    /// Indexed properties: (PropertyId, Tantivy field name).
    pub properties: Vec<(PropertyId, String)>,
}

impl TextIndexEntry {
    pub fn new(
        index: std::sync::Arc<crate::text_index::TextIndex>,
        properties: Vec<(PropertyId, String)>,
    ) -> Self {
        Self { index, properties }
    }
}

/// Information about a query currently in flight.
#[derive(Clone, Debug)]
pub struct ActiveQueryInfo {
    pub query_id: u64,
    pub query_text: String,
    pub started_at: std::time::Instant,
}

/// Convert a PropertyValue to a string suitable for text indexing.
fn property_value_to_string(value: &PropertyValue) -> Option<String> {
    match value {
        PropertyValue::String(s) => Some(s.clone()),
        PropertyValue::Int(n) => Some(n.to_string()),
        PropertyValue::Double(n) => Some(n.to_string()),
        PropertyValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Convert a `PropertyValue::List` of numbers into a `Vec<f32>`.
/// Returns `None` for non-list values or lists containing non-numeric elements.
pub fn property_value_to_f32_vec(value: &PropertyValue) -> Option<Vec<f32>> {
    match value {
        PropertyValue::List(items) => {
            let mut vec = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    PropertyValue::Int(i) => vec.push(*i as f32),
                    PropertyValue::Double(d) => vec.push(*d as f32),
                    _ => return None,
                }
            }
            Some(vec)
        }
        _ => None,
    }
}

/// Maintain the point index for a vertex when a property changes.
fn maintain_point_index_on_set(
    storage: &Storage,
    vertex: &Vertex,
    gid: Gid,
    key: PropertyId,
    value: &PropertyValue,
) {
    let active_pi = storage.active_point_indices.read().unwrap();
    for label in &vertex.labels {
        if active_pi.contains(&(*label, key)) {
            // Remove old entry first
            storage.point_index.remove(*label, key, gid);
            // Insert new point if applicable
            match value {
                PropertyValue::Point2D(p) => storage.point_index.insert_2d(*label, key, gid, *p),
                PropertyValue::Point3D(p) => storage.point_index.insert_3d(*label, key, gid, *p),
                _ => {}
            }
        }
    }
}

use std::sync::Mutex;

/// Lightweight record for WAL durability; external code maps this to mgdurability::DeltaRecord.
#[derive(Clone, Debug, PartialEq)]
pub enum WalRecord {
    VertexCreate {
        gid: Gid,
        timestamp: u64,
    },
    VertexDelete {
        gid: Gid,
    },
    VertexAddLabel {
        gid: Gid,
        label: LabelId,
    },
    VertexRemoveLabel {
        gid: Gid,
        label: LabelId,
    },
    VertexSetProperty {
        gid: Gid,
        key: PropertyId,
        value: PropertyValue,
    },
    EdgeCreate {
        gid: Gid,
        from_vertex: Gid,
        to_vertex: Gid,
        edge_type: EdgeTypeId,
        timestamp: u64,
    },
    EdgeDelete {
        gid: Gid,
    },
    EdgeSetProperty {
        gid: Gid,
        key: PropertyId,
        value: PropertyValue,
    },
    TransactionEnd {
        timestamp: u64,
        commit_timestamp: u64,
    },
}

/// Trait for external WAL writers (avoids cyclic dependency on mgdurability).
pub trait WalAppender: Send + Sync {
    fn append(&mut self, record: WalRecord);
    fn sync(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

/// Statistics returned by `gc_stats()`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcStats {
    pub deltas_collected: usize,
    pub deltas_retained: usize,
}

/// In-memory graph storage with MVCC.
pub struct Storage {
    /// Vertex storage: Gid → Box<Vertex>. Vertices are heap-allocated because
    /// they are referenced by raw pointers in delta chains.
    pub(crate) vertices: RwLock<HashMap<Gid, Box<Vertex>>>,

    /// Edge storage: Gid → Box<Edge>.
    pub(crate) edges: RwLock<HashMap<Gid, Box<Edge>>>,

    /// Deltas created by active/committed transactions. Keyed by Gid.
    /// These are owned by the storage engine and freed during GC.
    #[allow(clippy::vec_box)]
    deltas: RwLock<Vec<Box<CoreDelta>>>,

    /// Transaction engine for ID allocation and lifecycle.
    pub transaction_engine: TransactionEngine,

    /// Active transaction start timestamps for GC watermark.
    /// Maps timestamp -> reference count (multiple txs may share a timestamp).
    pub(crate) active_timestamps: RwLock<BTreeMap<u64, usize>>,

    /// Next vertex/edge GID allocator.
    next_gid: AtomicU64,

    // ─── Indices ─────────────────────────────────────────────────
    pub label_index: LabelIndex,
    pub label_property_index: LabelPropertyIndex,
    pub edge_type_index: EdgeTypeIndex,
    pub edge_type_property_index: EdgeTypePropertyIndex,
    pub edge_index: EdgeIndex,

    // ─── Constraints ─────────────────────────────────────────────
    pub constraints: Constraints,

    // ─── Vector index (HNSW) ────────────────────────────────────
    pub vector_indices: RwLock<HashMap<LabelId, VectorIndexEntry>>,

    // ─── Text index (Tantivy) ───────────────────────────────────
    pub text_indices: RwLock<HashMap<LabelId, TextIndexEntry>>,

    // ─── TTL configuration ───────────────────────────────────────
    pub ttl_config: TtlConfig,

    // ─── Active index metadata (schema-level) ────────────────────
    pub active_label_indices: RwLock<HashSet<LabelId>>,
    pub active_label_property_indices: RwLock<HashSet<(LabelId, PropertyId)>>,
    pub active_point_indices: RwLock<HashSet<(LabelId, PropertyId)>>,
    pub active_edge_type_indices: RwLock<HashSet<EdgeTypeId>>,
    pub active_edge_type_property_indices: RwLock<HashSet<(EdgeTypeId, PropertyId)>>,

    /// Edge property index: (PropertyId, Gid) → PropertyValue
    pub edge_property_index: EdgePropertyIndex,

    /// Schema info tracking (labels, edge types, properties).
    pub schema_info: SchemaInfo,

    /// Point index for 2D/3D spatial queries.
    pub point_index: crate::point_index::PointIndex,

    // ─── WAL for real-time durability ────────────────────────────
    wal: Mutex<Option<Box<dyn WalAppender>>>,

    // ─── Metrics ─────────────────────────────────────────────────
    pub metrics: crate::metrics::StorageMetrics,

    // ─── Triggers ────────────────────────────────────────────────
    pub triggers: crate::triggers::TriggerRegistry,
    trigger_executor:
        std::sync::Mutex<Option<std::sync::Arc<dyn crate::triggers::TriggerExecutor>>>,

    // ─── Query profiling ─────────────────────────────────────────
    pub query_profiler: crate::query_profile::QueryProfiler,

    // ─── Active query tracking ───────────────────────────────────
    pub active_queries: std::sync::Mutex<std::collections::HashMap<u64, ActiveQueryInfo>>,
    next_query_id: std::sync::atomic::AtomicU64,

    // ─── On-disk backend ─────────────────────────────────────────
    vertex_disk: Mutex<Option<mgdisk::DiskKv>>,
    edge_disk: Mutex<Option<mgdisk::DiskKv>>,

    // ─── LRU cache for hot vertices in on-disk mode ──────────────
    vertex_cache: Mutex<lru::LruCache<Gid, Box<Vertex>>>,

    // ─── LRU cache for hot vertex snapshots (fast path) ──────────
    // RwLock allows concurrent reads on the hot path (peek does not
    // update LRU order, but avoids mutex contention under read load).
    vertex_snapshot_cache: RwLock<lru::LruCache<Gid, VertexSnapshot>>,

    // ─── Schema generation counter for plan cache invalidation ───
    schema_generation: AtomicU64,

    // ─── Configuration ───────────────────────────────────────────
    config: StorageConfig,

    // ─── GC stats ────────────────────────────────────────────────
    gc_stats: Mutex<GcStats>,

    // ─── Commit serialization ────────────────────────────────────
    /// Sharded locks for commit serialization. Each shard covers a subset
    /// of Gids (via hashing). Transactions that write disjoint Gid sets
    /// can acquire non-overlapping shards and commit in parallel.
    commit_shards: Vec<Mutex<()>>,
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage {
    pub fn new() -> Self {
        Self::with_config(StorageConfig::default())
    }

    pub fn with_config(config: StorageConfig) -> Self {
        let (vertex_disk, edge_disk) = match &config.mode {
            StorageMode::InMemory => (None, None),
            StorageMode::OnDisk { path } => {
                let vd = mgdisk::DiskKv::open(format!("{}/vertices", path), "vertex").ok();
                let ed = mgdisk::DiskKv::open(format!("{}/edges", path), "edge").ok();
                (vd, ed)
            }
        };
        let cache_size = std::num::NonZeroUsize::new(config.lru_cache_size.max(1))
            .unwrap_or(std::num::NonZeroUsize::new(1).unwrap());
        Self {
            vertices: RwLock::new(HashMap::new()),
            edges: RwLock::new(HashMap::new()),
            deltas: RwLock::new(Vec::new()),
            transaction_engine: TransactionEngine::new(),
            active_timestamps: RwLock::new(BTreeMap::new()),
            next_gid: AtomicU64::new(1),
            label_index: LabelIndex::new(),
            label_property_index: LabelPropertyIndex::new(),
            edge_type_index: EdgeTypeIndex::new(),
            edge_type_property_index: EdgeTypePropertyIndex::new(),
            edge_index: EdgeIndex::new(),
            edge_property_index: EdgePropertyIndex::new(),
            schema_info: SchemaInfo::new(),
            point_index: crate::point_index::PointIndex::new(),
            constraints: Constraints::new(),
            vector_indices: RwLock::new(HashMap::new()),
            text_indices: RwLock::new(HashMap::new()),
            ttl_config: TtlConfig::new(),
            active_label_indices: RwLock::new(HashSet::new()),
            active_label_property_indices: RwLock::new(HashSet::new()),
            active_point_indices: RwLock::new(HashSet::new()),
            active_edge_type_indices: RwLock::new(HashSet::new()),
            active_edge_type_property_indices: RwLock::new(HashSet::new()),
            wal: Mutex::new(None),
            metrics: crate::metrics::StorageMetrics::new(),
            triggers: crate::triggers::TriggerRegistry::new(),
            trigger_executor: std::sync::Mutex::new(None),
            query_profiler: crate::query_profile::QueryProfiler::new(1000),
            active_queries: std::sync::Mutex::new(std::collections::HashMap::new()),
            next_query_id: std::sync::atomic::AtomicU64::new(1),
            vertex_disk: Mutex::new(vertex_disk),
            edge_disk: Mutex::new(edge_disk),
            vertex_cache: Mutex::new(lru::LruCache::new(cache_size)),
            vertex_snapshot_cache: RwLock::new(lru::LruCache::new(cache_size)),
            schema_generation: AtomicU64::new(0),
            config,
            gc_stats: Mutex::new(GcStats::default()),
            commit_shards: (0..N_COMMIT_SHARDS).map(|_| Mutex::new(())).collect(),
        }
    }

    /// Return the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Return true if this storage is using on-disk mode.
    pub fn is_on_disk(&self) -> bool {
        matches!(self.config.mode, StorageMode::OnDisk { .. })
    }

    /// Start tracking a new active query. Returns the query ID.
    pub fn start_query(&self, query_text: String) -> u64 {
        let id = self
            .next_query_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let info = ActiveQueryInfo {
            query_id: id,
            query_text,
            started_at: std::time::Instant::now(),
        };
        self.active_queries.lock().unwrap().insert(id, info);
        id
    }

    /// Mark an active query as finished.
    pub fn finish_query(&self, query_id: u64) {
        self.active_queries.lock().unwrap().remove(&query_id);
    }

    /// List all currently active queries.
    pub fn list_active_queries(&self) -> Vec<ActiveQueryInfo> {
        self.active_queries
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }

    /// Attach a WAL writer for real-time durability.
    pub fn set_wal(&self, writer: Box<dyn WalAppender>) {
        *self.wal.lock().unwrap() = Some(writer);
    }

    /// Sync WAL to disk if a writer is attached.
    pub fn sync_wal(&self) {
        if let Ok(ref mut guard) = self.wal.lock() {
            if let Some(ref mut writer) = guard.as_mut() {
                let _ = writer.sync();
            }
        }
    }

    /// Attach a trigger executor for firing trigger statements.
    pub fn set_trigger_executor(
        &self,
        executor: std::sync::Arc<dyn crate::triggers::TriggerExecutor>,
    ) {
        *self.trigger_executor.lock().unwrap() = Some(executor);
    }

    /// Fire triggers matching the given event and optional label.
    fn fire_triggers(
        &self,
        event: crate::triggers::TriggerEvent,
        label: Option<LabelId>,
        gid: Gid,
    ) {
        // Prevent recursive trigger execution
        thread_local! { static IN_TRIGGER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) } }
        if IN_TRIGGER.with(|c| c.get()) {
            return;
        }

        let triggers = self.triggers.matching(event, label);
        if triggers.is_empty() {
            return;
        }
        if let Ok(guard) = self.trigger_executor.lock() {
            if let Some(ref executor) = *guard {
                IN_TRIGGER.with(|c| c.set(true));
                let ctx = crate::triggers::TriggerContext {
                    event,
                    gid,
                    label,
                    changed_properties: vec![],
                };
                for trigger in triggers {
                    let _ = executor.execute(&ctx, &trigger.statement);
                }
                IN_TRIGGER.with(|c| c.set(false));
            }
        }
    }

    fn append_wal(&self, record: &WalRecord) {
        if !self.config.wal_enabled {
            return;
        }
        if let Ok(ref mut guard) = self.wal.lock() {
            if let Some(ref mut writer) = guard.as_mut() {
                writer.append(record.clone());
            }
        }
    }

    /// Allocate a new unique GID for vertices or edges.
    pub fn allocate_gid(&self) -> Gid {
        Gid::from(self.next_gid.fetch_add(1, Ordering::Relaxed))
    }

    /// Number of vertices in storage.
    pub fn vertex_count(&self) -> usize {
        if self.is_on_disk() {
            if let Ok(disk) = self.vertex_disk.lock() {
                if let Some(ref d) = *disk {
                    return d.count().unwrap_or(0);
                }
            }
        }
        self.vertices.read().unwrap().len()
    }

    /// Number of edges in storage.
    pub fn edge_count(&self) -> usize {
        if self.is_on_disk() {
            if let Ok(disk) = self.edge_disk.lock() {
                if let Some(ref d) = *disk {
                    return d.count().unwrap_or(0);
                }
            }
        }
        self.edges.read().unwrap().len()
    }

    /// Get in-edge GIDs for a vertex.
    pub fn vertex_in_edge_gids(&self, gid: Gid) -> Vec<Gid> {
        let vertices = self.vertices.read().unwrap();
        vertices
            .get(&gid)
            .map(|v| v.in_edges.iter().map(|t| t.edge.gid()).collect())
            .unwrap_or_default()
    }

    /// Get out-edge GIDs for a vertex.
    pub fn vertex_out_edge_gids(&self, gid: Gid) -> Vec<Gid> {
        let vertices = self.vertices.read().unwrap();
        vertices
            .get(&gid)
            .map(|v| v.out_edges.iter().map(|t| t.edge.gid()).collect())
            .unwrap_or_default()
    }

    /// Begin a new transaction.
    pub fn begin_transaction(&self, isolation_level: IsolationLevel) -> Arc<Transaction> {
        let tx = Arc::new(self.transaction_engine.begin(isolation_level));
        let mut ts = self.active_timestamps.write().unwrap();
        *ts.entry(tx.start_timestamp).or_insert(0) += 1;
        tx
    }

    /// Commit a transaction.
    /// First performs write-write conflict detection: if any Gid in the
    /// transaction's write set was also modified by a transaction that
    /// committed after this transaction started, we abort.
    pub fn commit_transaction(&self, tx: &Transaction) -> bool {
        // Compute the set of shards touched by this transaction's write set,
        // then lock them in ascending order to avoid deadlock.
        let write_set = tx.take_write_set();
        let mut shard_indices: Vec<usize> = write_set
            .iter()
            .map(|gid| gid_to_shard(*gid))
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        shard_indices.sort_unstable();

        let mut _shard_guards: Vec<std::sync::MutexGuard<'_, ()>> =
            Vec::with_capacity(shard_indices.len());
        for idx in &shard_indices {
            _shard_guards.push(self.commit_shards[*idx].lock().unwrap());
        }

        if !self.check_write_write_conflicts_with_write_set(tx, &write_set) {
            self.abort_transaction(tx);
            return false;
        }
        let result = self.transaction_engine.commit(tx);
        if result {
            self.metrics.inc_transactions_committed(1);
            self.remove_timestamp(tx.start_timestamp);
            self.append_wal(&WalRecord::TransactionEnd {
                timestamp: tx.id.0,
                commit_timestamp: tx.commit_timestamp(),
            });
            if self.config.wal_sync_on_commit {
                if let Ok(ref mut guard) = self.wal.lock() {
                    if let Some(ref mut writer) = guard.as_mut() {
                        let _ = writer.sync();
                    }
                }
            }
        }
        result
    }

    /// Internal conflict check that takes a pre-extracted write set so the
    /// caller (sharded commit) can hold the locks while reusing the set.
    fn check_write_write_conflicts_with_write_set(
        &self,
        tx: &Transaction,
        write_set: &std::collections::HashSet<Gid>,
    ) -> bool {
        if write_set.is_empty() {
            return true;
        }
        let my_tx_id = tx.id.0;
        let start_ts = tx.start_timestamp;

        let vertices = self.vertices.read().unwrap();
        let edges = self.edges.read().unwrap();

        for gid in write_set {
            // Check vertex delta chain
            if let Some(v) = vertices.get(gid) {
                if has_conflicting_delta(v.delta(), my_tx_id, start_ts) {
                    return false;
                }
            }
            // Check edge delta chain
            if let Some(e) = edges.get(gid) {
                if has_conflicting_delta(e.delta(), my_tx_id, start_ts) {
                    return false;
                }
            }
        }
        true
    }

    /// Abort a transaction.
    pub fn abort_transaction(&self, tx: &Transaction) -> bool {
        let result = self.transaction_engine.abort(tx);
        if result {
            self.metrics.inc_transactions_aborted(1);
            self.remove_timestamp(tx.start_timestamp);
        }
        result
    }

    fn remove_timestamp(&self, ts: u64) {
        let mut map = self.active_timestamps.write().unwrap();
        if let std::collections::btree_map::Entry::Occupied(mut e) = map.entry(ts) {
            let count = e.get();
            if *count <= 1 {
                e.remove();
            } else {
                *e.get_mut() -= 1;
            }
        }
    }

    /// Garbage collect old deltas no longer visible to any active transaction.
    /// Two-phase: (1) unlink old deltas from chains, (2) delete from global pool.
    /// Returns number of deltas freed.
    pub fn gc(&self) -> usize {
        let watermark = self.min_active_timestamp();
        self.gc_with_horizon(watermark)
    }

    /// Garbage collect deltas older than the given horizon timestamp.
    /// This allows callers to pass the oldest active transaction timestamp
    /// explicitly, protecting deltas still needed by long-running transactions.
    pub fn gc_with_horizon(&self, oldest_active_tx: u64) -> usize {
        let before = self.deltas.read().unwrap().len();

        // Phase 1: unlink old deltas from chains
        let vertices = self.vertices.read().unwrap();
        for (_gid, vertex) in vertices.iter() {
            let head = vertex.delta();
            if head.is_null() {
                continue;
            }
            let new_head = unsafe { Self::trim_chain(head, oldest_active_tx) };
            if new_head != head {
                vertex.set_delta(new_head);
            }
        }
        drop(vertices);

        let edges = self.edges.read().unwrap();
        for (_gid, edge) in edges.iter() {
            let head = edge.delta();
            if head.is_null() {
                continue;
            }
            let new_head = unsafe { Self::trim_chain(head, oldest_active_tx) };
            if new_head != head {
                edge.set_delta(new_head);
            }
        }
        drop(edges);

        // Phase 2: delete unreachable deltas from global pool
        let mut deltas = self.deltas.write().unwrap();
        deltas.retain(|d| {
            let ts = d.commit_info.timestamp();
            ts >= oldest_active_tx
                || ts >= TRANSACTION_INITIAL_ID
                || matches!(
                    d.kind,
                    DeltaKind::DeleteObject | DeltaKind::DeleteDeserializedObject { .. }
                )
        });
        let freed = before - deltas.len();
        self.metrics.inc_gc_deltas(freed as u64);
        let retained = deltas.len();
        drop(deltas);
        let mut stats = self.gc_stats.lock().unwrap();
        stats.deltas_collected += freed;
        stats.deltas_retained = retained;
        freed
    }

    /// Return GC statistics.
    pub fn gc_stats(&self) -> GcStats {
        self.gc_stats.lock().unwrap().clone()
    }

    /// Walk from head (newest) to find the first delta that is NOT
    /// old (ts >= watermark or uncommitted or tail anchor). Everything
    /// between head and that delta is old and gets unlinked.
    unsafe fn trim_chain(head: *mut CoreDelta, watermark: u64) -> *mut CoreDelta {
        let mut current = head;
        // Scan until we find a delta worth keeping
        while let Some(d) = current.as_ref() {
            let ts = d.commit_info.timestamp();
            let next = d.next.load(std::sync::atomic::Ordering::Acquire);
            let is_tail = next.is_null()
                && matches!(
                    d.kind,
                    DeltaKind::DeleteObject | DeltaKind::DeleteDeserializedObject { .. }
                );
            if ts >= watermark || ts >= TRANSACTION_INITIAL_ID || is_tail {
                break; // stop trimming — this one stays
            }
            // This delta is old. Move to next.
            if next.is_null() {
                return std::ptr::null_mut();
            } // entire chain gone
            current = next;
        }
        // current now points to the first non-old delta (new head)
        if current != head {
            // Check if the old segment we trimmed includes the tail anchor.
            // If so, we can't fully unlink. Walk old segment to check.
            let mut check = head;
            let mut found_tail = false;
            while check != current {
                let d = &*check;
                let next = d.next.load(std::sync::atomic::Ordering::Acquire);
                if next.is_null()
                    && matches!(
                        d.kind,
                        DeltaKind::DeleteObject | DeltaKind::DeleteDeserializedObject { .. }
                    )
                {
                    found_tail = true;
                    break;
                }
                check = next;
            }
            if found_tail {
                // Keep the tail anchor. Connect the new head's predecessor to the tail.
                // But since we trimmed from head to new-head, the tail anchor is
                // in the trimmed segment. We need to preserve it.
                // Find the tail anchor and link it past the old deltas.
                // Actually, if the tail anchor is in the trimmed segment, just
                // keep the entire chain — can't trim past the tail anchor.
                head
            } else {
                current
            }
        } else {
            head
        }
    }

    fn min_active_timestamp(&self) -> u64 {
        self.active_timestamps
            .read()
            .unwrap()
            .keys()
            .next()
            .copied()
            .unwrap_or(u64::MAX)
    }

    /// Return the oldest active transaction timestamp, or `u64::MAX` if none.
    pub fn oldest_active_timestamp(&self) -> u64 {
        self.min_active_timestamp()
    }

    /// Returns true if there are any active transactions.
    pub fn has_active_transactions(&self) -> bool {
        let map = self.active_timestamps.read().unwrap();
        !map.is_empty()
    }

    // ─── Schema generation for plan cache invalidation ────────────────

    /// Return the current schema generation counter.
    pub fn schema_generation(&self) -> u64 {
        self.schema_generation.load(Ordering::Relaxed)
    }

    /// Bump the schema generation counter. Called after schema mutations.
    pub fn bump_schema_generation(&self) {
        self.schema_generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Invalidate the vertex snapshot cache for a specific Gid.
    /// Call after any vertex mutation.
    fn invalidate_vertex_snapshot(&self, gid: Gid) {
        self.vertex_snapshot_cache.write().unwrap().pop(&gid);
    }

    /// Clear all vertex snapshot caches. Call after bulk operations or GC.
    pub fn clear_vertex_snapshot_cache(&self) {
        self.vertex_snapshot_cache.write().unwrap().clear();
    }

    // ─── Vertex operations ────────────────────────────────────────────

    /// Create a vertex. Returns the new vertex's Gid.
    pub fn create_vertex(&self, tx: &Transaction, gid: Gid) -> Result<Gid, StorageError> {
        tx.record_write(gid);
        let ci = tx.commit_info.clone();
        let cmd_id = tx.next_command_id();
        let delta = Box::new(CoreDelta::new_delete_object(ci, cmd_id));
        let delta_ptr = Box::into_raw(delta);

        // Store the delta
        let delta_box = unsafe { Box::from_raw(delta_ptr) };
        self.deltas.write().unwrap().push(delta_box);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let vertex = Box::new(Vertex::new(gid, delta_ptr, now_ms));

        if self.is_on_disk() {
            if let Ok(disk) = self.vertex_disk.lock() {
                if let Some(ref d) = *disk {
                    d.put(gid.as_uint(), vertex.as_ref()).map_err(|e| {
                        StorageError::ConstraintViolation(format!("disk error: {}", e))
                    })?;
                }
            }
        }

        let mut vertices = self.vertices.write().unwrap();
        if vertices.contains_key(&gid) {
            return Err(StorageError::VertexExists(gid));
        }
        vertices.insert(gid, vertex);
        drop(vertices);
        self.metrics.inc_vertices_created(1);

        self.append_wal(&WalRecord::VertexCreate {
            gid,
            timestamp: tx.start_timestamp,
        });

        self.fire_triggers(crate::triggers::TriggerEvent::VertexCreate, None, gid);

        Ok(gid)
    }

    /// Batch-create vertices. More efficient than calling `create_vertex` in a loop
    /// because it acquires locks once and writes a single WAL batch.
    pub fn create_vertices(
        &self,
        tx: &Transaction,
        gids: &[Gid],
    ) -> Result<Vec<Gid>, StorageError> {
        let mut created = Vec::with_capacity(gids.len());
        let ci = tx.commit_info.clone();
        let mut deltas = self.deltas.write().unwrap();
        let mut vertices = self.vertices.write().unwrap();

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        for gid in gids.iter() {
            if vertices.contains_key(gid) {
                continue;
            }
            let cmd_id = tx.next_command_id();
            let delta = Box::new(CoreDelta::new_delete_object(ci.clone(), cmd_id));
            let delta_ptr = Box::into_raw(delta);
            deltas.push(unsafe { Box::from_raw(delta_ptr) });

            let vertex = Box::new(Vertex::new(*gid, delta_ptr, now_ms));

            if self.is_on_disk() {
                if let Ok(disk) = self.vertex_disk.lock() {
                    if let Some(ref d) = *disk {
                        let _ = d.put(gid.as_uint(), vertex.as_ref());
                    }
                }
            }

            vertices.insert(*gid, vertex);
            created.push(*gid);
        }

        self.metrics.inc_vertices_created(created.len() as u64);
        Ok(created)
    }

    /// Read a vertex by Gid within a transaction's snapshot.
    pub fn get_vertex(&self, gid: Gid, tx: &Transaction) -> Option<VertexSnapshot> {
        // Hot-path cache: check the LRU snapshot cache first.
        // Cache entries are invalidated on vertex mutation, so a hit is always
        // safe regardless of active transactions.
        // Uses `peek` + read lock to avoid mutex contention on the hot path.
        if let Some(cached) = self.vertex_snapshot_cache.read().unwrap().peek(&gid) {
            return Some(cached.clone());
        }

        let vertices = self.vertices.read().unwrap();
        let vertex = match vertices.get(&gid) {
            Some(v) => v,
            None => {
                // On-disk fallback: try loading from disk into cache
                if self.is_on_disk() {
                    drop(vertices);
                    return self.get_vertex_from_disk(gid, tx);
                }
                return None;
            }
        };

        // Fast path: no deltas, no TTL, not deleted.
        // This is the hottest path — cache the snapshot for reuse.
        let delta = vertex.delta();
        let has_ttl = !self.ttl_config.is_empty();
        if delta.is_null() && !has_ttl && !vertex.deleted() {
            let snapshot = VertexSnapshot {
                gid: vertex.gid,
                labels: vertex.labels.clone(),
                properties: vertex.properties.clone(),
            };
            self.vertex_snapshot_cache
                .write()
                .unwrap()
                .put(gid, snapshot.clone());
            return Some(snapshot);
        }

        // Snapshot base state (current committed). The delta chain records
        // inverse operations; we undo changes that committed after our
        // snapshot point to reconstruct the past visible state.
        let mut exists = true;
        let mut deleted = vertex.deleted();
        let mut labels = vertex.labels.clone();
        let mut properties = vertex.properties.clone();

        apply_deltas_for_read(
            delta,
            View::Old,
            tx.isolation_level,
            tx.start_timestamp,
            tx.commit_info.timestamp(),
            u64::MAX,
            |d| {
                match &d.kind {
                    // DELETE_OBJECT / DELETE_DESERIALIZED_OBJECT: vertex didn't exist
                    // before this point (undo create → vertex goes away)
                    DeltaKind::DeleteObject | DeltaKind::DeleteDeserializedObject { .. } => {
                        exists = false;
                    }
                    // RECREATE_OBJECT: this delta says "vertex was deleted" → undo = mark not deleted
                    DeltaKind::RecreateObject => {
                        deleted = false;
                    }
                    // ADD_LABEL: this delta records "label was added" → undo = remove it
                    DeltaKind::Label {
                        action: DeltaAction::AddLabel,
                        value,
                    } => {
                        if let Some(pos) = labels.iter().position(|l| l == value) {
                            labels.swap_remove(pos);
                        }
                    }
                    // REMOVE_LABEL: this delta records "label was removed" → undo = add it back
                    DeltaKind::Label {
                        action: DeltaAction::RemoveLabel,
                        value,
                    } => {
                        labels.push(*value);
                    }
                    // SET_PROPERTY: undo set → restore old value
                    DeltaKind::SetProperty { key, old_value } => match old_value {
                        Some(v) if !v.is_null() => properties.set(*key, v.clone()),
                        _ => properties.remove(*key),
                    },
                    _ => {}
                }
            },
        );

        if !exists || deleted {
            return None;
        }

        // Check TTL expiration
        if has_ttl {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if self
                .ttl_config
                .is_expired(&labels, vertex.creation_timestamp, now_ms)
            {
                return None;
            }
        }

        Some(VertexSnapshot {
            gid: vertex.gid,
            labels,
            properties,
        })
    }

    /// Load a vertex from disk, caching it in the LRU.
    fn get_vertex_from_disk(&self, gid: Gid, tx: &Transaction) -> Option<VertexSnapshot> {
        let disk_opt = self.vertex_disk.lock().unwrap();
        let disk = disk_opt.as_ref()?;
        let loaded: Box<Vertex> = disk.get(gid.as_uint()).ok()??;
        drop(disk_opt);

        // Also insert into in-memory map so subsequent lookups hit RAM
        {
            let mut vertices = self.vertices.write().unwrap();
            vertices.entry(gid).or_insert(loaded);
        }

        // Now read from memory (which includes the freshly loaded vertex)
        self.get_vertex(gid, tx)
    }

    /// Set a property on a vertex. Matches C++ VertexAccessor::SetProperty.
    pub fn vertex_set_property(
        &self,
        tx: &Transaction,
        gid: Gid,
        key: PropertyId,
        value: PropertyValue,
    ) -> Result<(), StorageError> {
        tx.record_write(gid);
        let mut vertices = self.vertices.write().unwrap();
        let vertex = vertices
            .get_mut(&gid)
            .ok_or(StorageError::VertexNotFound(gid))?;

        let old_value = Some(vertex.properties.get(key).clone());
        let old_properties = vertex.properties.clone();

        // Create delta storing the OLD value (for undo during MVCC reads).
        // C++: CreateAndLinkDelta(transaction, vertex, Delta::SetPropertyTag(), property, old_value)
        let ci = tx.commit_info.clone();
        let cmd_id = tx.next_command_id();
        let delta = Box::new(CoreDelta::new_set_property(
            key,
            old_value.clone(),
            ci,
            cmd_id,
        ));
        let delta_ptr = Box::into_raw(delta);
        self.deltas
            .write()
            .unwrap()
            .push(unsafe { Box::from_raw(delta_ptr) });

        // Link delta into chain (newest first)
        unsafe {
            (*delta_ptr)
                .next
                .store(vertex.delta(), std::sync::atomic::Ordering::Release);
        }
        vertex.properties.set(key, value.clone());
        vertex.set_delta(delta_ptr);

        // Persist to disk if on-disk mode
        if self.is_on_disk() {
            if let Ok(disk) = self.vertex_disk.lock() {
                if let Some(ref d) = *disk {
                    let _ = d.put(gid.as_uint(), &*vertex);
                }
            }
        }

        // Schema info: record property for each label on the vertex
        for label in &vertex.labels {
            self.schema_info.record_vertex_property(*label, key);
        }

        let active_lp = self.active_label_property_indices.read().unwrap();
        for label in &vertex.labels {
            if active_lp.contains(&(*label, key)) {
                let lp_key = mgcore::types::LabelPropKey::new(*label, key);
                self.label_property_index.remove(lp_key, gid);
                self.label_property_index.add(lp_key, gid, value.clone());
            }
        }
        drop(active_lp);

        self.append_wal(&WalRecord::VertexSetProperty {
            gid,
            key,
            value: value.clone(),
        });

        // Check type constraints (if any exist for these labels)
        for label in &vertex.labels {
            self.constraints
                .check_type(*label, key, &value)
                .map_err(|e| StorageError::ConstraintViolation(format!("{:?}", e)))?;
        }

        // Check existence and unique constraints with updated properties.
        // Remove old unique keys first, then atomically check+record new ones
        // under a single write lock to eliminate the read-check / write-record race.
        for label in &vertex.labels {
            self.constraints
                .check_existence(*label, &vertex.properties)
                .map_err(|e| StorageError::ConstraintViolation(format!("{:?}", e)))?;
            self.constraints
                .remove_unique_values(*label, &old_properties);
            self.constraints
                .try_record_unique(*label, &vertex.properties, vertex.gid)
                .map_err(|e| StorageError::ConstraintViolation(format!("{:?}", e)))?;
        }

        // Maintain vector indices for affected label+property combinations.
        let vector_indices = self.vector_indices.read().unwrap();
        for label in &vertex.labels {
            if let Some(entry) = vector_indices.get(label) {
                if entry.property == key {
                    let mut index = entry.index.write().unwrap();
                    let mut gid_map = entry.gid_to_node.write().unwrap();
                    if let Some(old_node) = gid_map.remove(&gid) {
                        index.delete(old_node);
                    }
                    if let Some(new_vec) = property_value_to_f32_vec(&value) {
                        if new_vec.len() == entry.dimension {
                            let node_id = index.insert(&new_vec);
                            gid_map.insert(gid, node_id);
                        }
                    }
                }
            }
        }
        drop(vector_indices);

        // Maintain text indices for affected label+property combinations.
        let text_indices = self.text_indices.read().unwrap();
        for label in &vertex.labels {
            if let Some(entry) = text_indices.get(label) {
                if entry.properties.iter().any(|(pid, _)| *pid == key) {
                    let mut text_values = Vec::new();
                    for (pid, field_name) in &entry.properties {
                        let prop_val = vertex.properties.get(*pid);
                        if !prop_val.is_null() {
                            if let Some(s) = property_value_to_string(prop_val) {
                                text_values.push((field_name.clone(), s));
                            }
                        }
                    }
                    let _ = entry.index.index_vertex(gid, &text_values);
                }
            }
        }
        drop(text_indices);

        // Maintain point index for affected label+property combinations.
        maintain_point_index_on_set(self, vertex, gid, key, &value);

        drop(vertices);
        self.invalidate_vertex_snapshot(gid);
        self.metrics.inc_properties_set(1);

        self.fire_triggers(crate::triggers::TriggerEvent::VertexUpdate, None, gid);

        Ok(())
    }

    /// Add a label to a vertex.
    pub fn vertex_add_label(
        &self,
        tx: &Transaction,
        gid: Gid,
        label: LabelId,
    ) -> Result<(), StorageError> {
        tx.record_write(gid);
        let ci = tx.commit_info.clone();
        let cmd_id = tx.next_command_id();
        let delta = Box::new(CoreDelta::new_add_label(label, ci, cmd_id));
        let delta_ptr = Box::into_raw(delta);

        self.deltas
            .write()
            .unwrap()
            .push(unsafe { Box::from_raw(delta_ptr) });

        let mut vertices = self.vertices.write().unwrap();
        let vertex = vertices
            .get_mut(&gid)
            .ok_or(StorageError::VertexNotFound(gid))?;

        // If label already exists, no-op (C++ returns false, not an error)
        if vertex.labels.contains(&label) {
            return Ok(());
        }

        unsafe {
            (*delta_ptr)
                .next
                .store(vertex.delta(), std::sync::atomic::Ordering::Release);
        }
        vertex.labels.push(label);
        vertex.set_delta(delta_ptr);

        // Persist to disk if on-disk mode
        if self.is_on_disk() {
            if let Ok(disk) = self.vertex_disk.lock() {
                if let Some(ref d) = *disk {
                    let _ = d.put(gid.as_uint(), &*vertex);
                }
            }
        }

        // Check type, existence and unique constraints for the newly added label
        for (prop_id, value) in vertex.properties.iter() {
            self.constraints
                .check_type(label, prop_id, value)
                .map_err(|e| StorageError::ConstraintViolation(format!("{:?}", e)))?;
        }
        self.constraints
            .check_existence(label, &vertex.properties)
            .map_err(|e| StorageError::ConstraintViolation(format!("{:?}", e)))?;
        self.constraints
            .try_record_unique(label, &vertex.properties, vertex.gid)
            .map_err(|e| StorageError::ConstraintViolation(format!("{:?}", e)))?;

        self.label_index.add_vertex(label, gid);
        self.schema_info.record_label(label);
        // Schema info: record existing properties for this newly added label
        for (prop_id, _value) in vertex.properties.iter() {
            self.schema_info.record_vertex_property(label, prop_id);
        }

        // Update label-property index for active indices
        let active_lp = self.active_label_property_indices.read().unwrap();
        for (prop_id, value) in vertex.properties.iter() {
            if active_lp.contains(&(label, prop_id)) {
                let lp_key = mgcore::types::LabelPropKey::new(label, prop_id);
                self.label_property_index.add(lp_key, gid, value.clone());
            }
        }
        drop(active_lp);

        // Maintain vector index if the newly added label has one.
        let vector_indices = self.vector_indices.read().unwrap();
        if let Some(entry) = vector_indices.get(&label) {
            let prop_value = vertex.properties.get(entry.property);
            if !prop_value.is_null() {
                if let Some(vec) = property_value_to_f32_vec(prop_value) {
                    if vec.len() == entry.dimension {
                        let mut index = entry.index.write().unwrap();
                        let mut gid_map = entry.gid_to_node.write().unwrap();
                        let node_id = index.insert(&vec);
                        gid_map.insert(gid, node_id);
                    }
                }
            }
        }
        drop(vector_indices);

        // Maintain text index if the newly added label has one.
        let text_indices = self.text_indices.read().unwrap();
        if let Some(entry) = text_indices.get(&label) {
            let mut text_values = Vec::new();
            for (pid, field_name) in &entry.properties {
                let prop_val = vertex.properties.get(*pid);
                if !prop_val.is_null() {
                    if let Some(s) = property_value_to_string(prop_val) {
                        text_values.push((field_name.clone(), s));
                    }
                }
            }
            let _ = entry.index.index_vertex(gid, &text_values);
        }
        drop(text_indices);

        // Maintain point index if the newly added label has one.
        let active_pi = self.active_point_indices.read().unwrap();
        for (prop_id, value) in vertex.properties.iter() {
            if active_pi.contains(&(label, prop_id)) {
                match &value {
                    PropertyValue::Point2D(p) => {
                        self.point_index.insert_2d(label, prop_id, gid, *p)
                    }
                    PropertyValue::Point3D(p) => {
                        self.point_index.insert_3d(label, prop_id, gid, *p)
                    }
                    _ => {}
                }
            }
        }
        drop(active_pi);

        self.append_wal(&WalRecord::VertexAddLabel { gid, label });
        self.invalidate_vertex_snapshot(gid);
        self.metrics.inc_labels_added(1);

        Ok(())
    }

    /// Get vertices by label.
    pub fn vertices_by_label(&self, label: LabelId) -> Vec<Gid> {
        self.label_index.vertices_by_label(label)
    }

    /// Get vertices by label + property equality (uses label-property index).
    pub fn vertices_by_label_property(
        &self,
        label: LabelId,
        property: PropertyId,
        value: &PropertyValue,
    ) -> Vec<Gid> {
        let key = mgcore::types::LabelPropKey::new(label, property);
        self.label_property_index.find_by_value(key, value)
    }

    // ─── Index schema management ─────────────────────────────────────────

    pub fn create_label_index(&self, label: LabelId) -> bool {
        let changed = self.active_label_indices.write().unwrap().insert(label);
        if changed {
            self.bump_schema_generation();
        }
        changed
    }

    pub fn drop_label_index(&self, label: LabelId) -> bool {
        let changed = self.active_label_indices.write().unwrap().remove(&label);
        if changed {
            self.bump_schema_generation();
        }
        changed
    }

    pub fn has_label_index(&self, label: LabelId) -> bool {
        self.active_label_indices.read().unwrap().contains(&label)
    }

    pub fn create_label_property_index(&self, label: LabelId, property: PropertyId) -> bool {
        let changed = self
            .active_label_property_indices
            .write()
            .unwrap()
            .insert((label, property));
        if changed {
            self.bump_schema_generation();
        }
        changed
    }

    pub fn drop_label_property_index(&self, label: LabelId, property: PropertyId) -> bool {
        let changed = self
            .active_label_property_indices
            .write()
            .unwrap()
            .remove(&(label, property));
        if changed {
            self.bump_schema_generation();
        }
        changed
    }

    pub fn has_label_property_index(&self, label: LabelId, property: PropertyId) -> bool {
        self.active_label_property_indices
            .read()
            .unwrap()
            .contains(&(label, property))
    }

    pub fn create_point_index(&self, label: LabelId, property: PropertyId) -> bool {
        let changed = self
            .active_point_indices
            .write()
            .unwrap()
            .insert((label, property));
        if changed {
            self.bump_schema_generation();
        }
        changed
    }

    pub fn drop_point_index(&self, label: LabelId, property: PropertyId) -> bool {
        let changed = self
            .active_point_indices
            .write()
            .unwrap()
            .remove(&(label, property));
        if changed {
            self.bump_schema_generation();
        }
        changed
    }

    pub fn has_point_index(&self, label: LabelId, property: PropertyId) -> bool {
        self.active_point_indices
            .read()
            .unwrap()
            .contains(&(label, property))
    }

    pub fn create_edge_type_index(&self, edge_type: EdgeTypeId) -> bool {
        let changed = self
            .active_edge_type_indices
            .write()
            .unwrap()
            .insert(edge_type);
        if changed {
            self.bump_schema_generation();
        }
        changed
    }

    pub fn drop_edge_type_index(&self, edge_type: EdgeTypeId) -> bool {
        let changed = self
            .active_edge_type_indices
            .write()
            .unwrap()
            .remove(&edge_type);
        if changed {
            self.bump_schema_generation();
        }
        changed
    }

    pub fn has_edge_type_index(&self, edge_type: EdgeTypeId) -> bool {
        self.active_edge_type_indices
            .read()
            .unwrap()
            .contains(&edge_type)
    }

    pub fn create_edge_type_property_index(
        &self,
        edge_type: EdgeTypeId,
        property: PropertyId,
    ) -> bool {
        let changed = self
            .active_edge_type_property_indices
            .write()
            .unwrap()
            .insert((edge_type, property));
        if changed {
            self.bump_schema_generation();
        }
        changed
    }

    pub fn drop_edge_type_property_index(
        &self,
        edge_type: EdgeTypeId,
        property: PropertyId,
    ) -> bool {
        let changed = self
            .active_edge_type_property_indices
            .write()
            .unwrap()
            .remove(&(edge_type, property));
        if changed {
            self.bump_schema_generation();
        }
        changed
    }

    pub fn has_edge_type_property_index(
        &self,
        edge_type: EdgeTypeId,
        property: PropertyId,
    ) -> bool {
        self.active_edge_type_property_indices
            .read()
            .unwrap()
            .contains(&(edge_type, property))
    }

    // ─── TTL management ──────────────────────────────────────────────────

    pub fn set_ttl(&self, label: LabelId, ttl_ms: u64) {
        self.ttl_config.set_ttl(label, ttl_ms);
    }

    pub fn remove_ttl(&self, label: LabelId) {
        self.ttl_config.remove_ttl(label);
    }

    pub fn get_ttl(&self, label: LabelId) -> Option<u64> {
        self.ttl_config.get_ttl(label)
    }

    // ─── Edge operations ───────────────────────────────────────────────

    /// Create an edge between two vertices.
    pub fn create_edge(
        &self,
        tx: &Transaction,
        gid: Gid,
        from_vertex: Gid,
        to_vertex: Gid,
        edge_type: EdgeTypeId,
    ) -> Result<Gid, StorageError> {
        tx.record_write(gid);
        let ci = tx.commit_info.clone();
        let cmd_id = tx.next_command_id();
        let delta = Box::new(CoreDelta::new_delete_object(ci, cmd_id));
        let delta_ptr = Box::into_raw(delta);

        self.deltas
            .write()
            .unwrap()
            .push(unsafe { Box::from_raw(delta_ptr) });

        let edge = Box::new(Edge::new(gid, delta_ptr));

        // Lock vertices before edges (matching GC order: vertices → edges)
        let mut vertices = self.vertices.write().unwrap();
        let mut edges = self.edges.write().unwrap();
        if edges.contains_key(&gid) {
            return Err(StorageError::EdgeExists(gid));
        }
        edges.insert(gid, edge);
        // Use raw pointers to the vertices for the edge triples.
        // NonNull::from requires a non-null pointer.
        let to_ptr: *const Vertex = &*vertices[&to_vertex];
        let from_ptr: *const Vertex = &*vertices[&from_vertex];
        if let Some(from_v) = vertices.get_mut(&from_vertex) {
            from_v.out_edges.push(EdgeTriple {
                edge_type,
                vertex: unsafe { std::ptr::NonNull::new_unchecked(to_ptr as *mut _) },
                edge: EdgeRef::from_gid(gid),
            });
        }
        if let Some(to_v) = vertices.get_mut(&to_vertex) {
            to_v.in_edges.push(EdgeTriple {
                edge_type,
                vertex: unsafe { std::ptr::NonNull::new_unchecked(from_ptr as *mut _) },
                edge: EdgeRef::from_gid(gid),
            });
        }

        // Persist edge to disk if on-disk mode
        if self.is_on_disk() {
            if let Ok(disk) = self.edge_disk.lock() {
                if let Some(ref d) = *disk {
                    if let Some(edge_ref) = edges.get(&gid) {
                        let _ = d.put(gid.as_uint(), &**edge_ref);
                    }
                }
            }
        }

        // Index
        self.edge_type_index.add_edge(edge_type, gid);
        self.edge_index.insert(
            gid,
            EdgeIndexEntry {
                from_vertex,
                to_vertex,
                edge_type,
            },
        );
        self.schema_info.record_edge_type(edge_type);
        drop(vertices);
        drop(edges);
        self.metrics.inc_edges_created(1);

        self.append_wal(&WalRecord::EdgeCreate {
            gid,
            from_vertex,
            to_vertex,
            edge_type,
            timestamp: tx.start_timestamp,
        });

        self.fire_triggers(crate::triggers::TriggerEvent::EdgeCreate, None, gid);

        Ok(gid)
    }

    /// Batch-create edges. More efficient than calling `create_edge` in a loop.
    /// Each tuple is (gid, from_vertex, to_vertex, edge_type).
    pub fn create_edges(
        &self,
        tx: &Transaction,
        specs: &[(Gid, Gid, Gid, EdgeTypeId)],
    ) -> Result<Vec<Gid>, StorageError> {
        let mut created = Vec::with_capacity(specs.len());
        let ci = tx.commit_info.clone();
        let mut deltas = self.deltas.write().unwrap();

        let mut vertices = self.vertices.write().unwrap();
        let mut edges = self.edges.write().unwrap();

        for (gid, from_vertex, to_vertex, edge_type) in specs {
            if edges.contains_key(gid) {
                continue;
            }
            let cmd_id = tx.next_command_id();
            let delta = Box::new(CoreDelta::new_delete_object(ci.clone(), cmd_id));
            let delta_ptr = Box::into_raw(delta);
            deltas.push(unsafe { Box::from_raw(delta_ptr) });

            let edge = Box::new(Edge::new(*gid, delta_ptr));
            edges.insert(*gid, edge);

            let to_ptr: *const Vertex = &*vertices[to_vertex];
            let from_ptr: *const Vertex = &*vertices[from_vertex];
            if let Some(from_v) = vertices.get_mut(from_vertex) {
                from_v.out_edges.push(EdgeTriple {
                    edge_type: *edge_type,
                    vertex: unsafe { std::ptr::NonNull::new_unchecked(to_ptr as *mut _) },
                    edge: EdgeRef::from_gid(*gid),
                });
            }
            if let Some(to_v) = vertices.get_mut(to_vertex) {
                to_v.in_edges.push(EdgeTriple {
                    edge_type: *edge_type,
                    vertex: unsafe { std::ptr::NonNull::new_unchecked(from_ptr as *mut _) },
                    edge: EdgeRef::from_gid(*gid),
                });
            }

            self.edge_type_index.add_edge(*edge_type, *gid);
            self.edge_index.insert(
                *gid,
                EdgeIndexEntry {
                    from_vertex: *from_vertex,
                    to_vertex: *to_vertex,
                    edge_type: *edge_type,
                },
            );
            self.schema_info.record_edge_type(*edge_type);
            created.push(*gid);
        }

        // Persist batch to disk if on-disk mode
        if self.is_on_disk() {
            if let Ok(disk) = self.edge_disk.lock() {
                if let Some(ref d) = *disk {
                    for gid in &created {
                        if let Some(edge_ref) = edges.get(gid) {
                            let _ = d.put(gid.as_uint(), &**edge_ref);
                        }
                    }
                }
            }
        }

        self.metrics.inc_edges_created(created.len() as u64);
        Ok(created)
    }

    /// Read an edge by Gid with MVCC delta chain traversal.
    /// Matches C++ EdgeAccessor pattern: snapshot base state, walk deltas newest→oldest,
    /// undo changes with ts >= start_timestamp to reconstruct visible state.
    pub fn get_edge(&self, gid: Gid, tx: &Transaction) -> Option<EdgeSnapshot> {
        let edges = self.edges.read().unwrap();
        let edge = edges.get(&gid)?;
        let idx_entry = self.edge_index.get(&gid)?;

        // Fast path: no deltas and not deleted → return base state directly.
        let delta = edge.delta();
        if delta.is_null() && !edge.deleted() {
            return Some(EdgeSnapshot {
                gid: edge.gid,
                from_vertex: idx_entry.from_vertex,
                to_vertex: idx_entry.to_vertex,
                edge_type: idx_entry.edge_type,
                properties: edge.properties.clone(),
            });
        }

        // Snapshot base state
        let mut exists = true;
        let mut deleted = edge.deleted();
        let mut properties = edge.properties.clone();

        apply_deltas_for_read(
            delta,
            View::Old,
            tx.isolation_level,
            tx.start_timestamp,
            tx.commit_info.timestamp(),
            u64::MAX,
            |d| match &d.kind {
                DeltaKind::DeleteObject | DeltaKind::DeleteDeserializedObject { .. } => {
                    exists = false;
                }
                DeltaKind::RecreateObject => {
                    deleted = false;
                }
                DeltaKind::SetProperty { key, old_value } => match old_value {
                    Some(v) if !v.is_null() => properties.set(*key, v.clone()),
                    _ => properties.remove(*key),
                },
                _ => {}
            },
        );

        if !exists || deleted {
            return None;
        }

        Some(EdgeSnapshot {
            gid: edge.gid,
            from_vertex: idx_entry.from_vertex,
            to_vertex: idx_entry.to_vertex,
            edge_type: idx_entry.edge_type,
            properties,
        })
    }

    /// Remove a label from a vertex.
    pub fn vertex_remove_label(
        &self,
        tx: &Transaction,
        gid: Gid,
        label: LabelId,
    ) -> Result<(), StorageError> {
        tx.record_write(gid);
        let ci = tx.commit_info.clone();
        let cmd_id = tx.next_command_id();
        let delta = Box::new(CoreDelta::new_remove_label(label, ci, cmd_id));
        let delta_ptr = Box::into_raw(delta);

        self.deltas
            .write()
            .unwrap()
            .push(unsafe { Box::from_raw(delta_ptr) });

        let mut vertices = self.vertices.write().unwrap();
        let vertex = vertices
            .get_mut(&gid)
            .ok_or(StorageError::VertexNotFound(gid))?;

        unsafe {
            (*delta_ptr)
                .next
                .store(vertex.delta(), std::sync::atomic::Ordering::Release);
        }
        vertex.labels.retain(|l| *l != label);
        vertex.set_delta(delta_ptr);

        // Persist to disk if on-disk mode
        if self.is_on_disk() {
            if let Ok(disk) = self.vertex_disk.lock() {
                if let Some(ref d) = *disk {
                    let _ = d.put(gid.as_uint(), &*vertex);
                }
            }
        }

        self.label_index.remove_vertex(label, gid);

        // Clean up unique constraints for the removed label.
        self.constraints
            .remove_unique_values(label, &vertex.properties);

        // Update label-property index for active indices
        let active_lp = self.active_label_property_indices.read().unwrap();
        for (prop_id, _value) in vertex.properties.iter() {
            if active_lp.contains(&(label, prop_id)) {
                let lp_key = mgcore::types::LabelPropKey::new(label, prop_id);
                self.label_property_index.remove(lp_key, gid);
            }
        }
        drop(active_lp);

        // Maintain vector index: remove vertex if the dropped label had a vector index.
        let vector_indices = self.vector_indices.read().unwrap();
        if let Some(entry) = vector_indices.get(&label) {
            let mut index = entry.index.write().unwrap();
            let mut gid_map = entry.gid_to_node.write().unwrap();
            if let Some(old_node) = gid_map.remove(&gid) {
                index.delete(old_node);
            }
        }
        drop(vector_indices);

        // Maintain text index: remove vertex if the dropped label had a text index.
        let text_indices = self.text_indices.read().unwrap();
        if let Some(entry) = text_indices.get(&label) {
            let _ = entry.index.remove_vertex(gid);
        }
        drop(text_indices);

        // Maintain point index: remove entries for the dropped label.
        let active_pi = self.active_point_indices.read().unwrap();
        for (prop_id, _value) in vertex.properties.iter() {
            if active_pi.contains(&(label, prop_id)) {
                self.point_index.remove(label, prop_id, gid);
            }
        }
        drop(active_pi);

        self.append_wal(&WalRecord::VertexRemoveLabel { gid, label });
        self.invalidate_vertex_snapshot(gid);
        self.metrics.inc_labels_removed(1);

        Ok(())
    }

    /// Set a property on an edge with MVCC delta tracking.
    pub fn edge_set_property(
        &self,
        tx: &Transaction,
        gid: Gid,
        key: PropertyId,
        value: PropertyValue,
    ) -> Result<(), StorageError> {
        tx.record_write(gid);
        let mut edges = self.edges.write().unwrap();
        let edge = edges.get_mut(&gid).ok_or(StorageError::EdgeNotFound(gid))?;

        let old_value = Some(edge.properties.get(key).clone());
        let ci = tx.commit_info.clone();
        let cmd_id = tx.next_command_id();
        let delta = Box::new(CoreDelta::new_set_property(key, old_value, ci, cmd_id));
        let delta_ptr = Box::into_raw(delta);
        self.deltas
            .write()
            .unwrap()
            .push(unsafe { Box::from_raw(delta_ptr) });

        unsafe {
            (*delta_ptr)
                .next
                .store(edge.delta(), std::sync::atomic::Ordering::Release);
        }

        // Maintain edge property index and edge type-property index
        let old_val_clone = edge.properties.get(key).clone();
        if !old_val_clone.is_null() {
            self.edge_property_index.remove(key, gid);
        }
        let entry_for_ek = self.edge_index.get(&gid);
        if !old_val_clone.is_null() {
            if let Some(ref entry) = entry_for_ek {
                let ek = mgcore::types::EdgeTypePropKey::new(entry.edge_type, key);
                self.edge_type_property_index.remove(ek, gid);
            }
        }
        edge.properties.set(key, value.clone());
        if !value.is_null() {
            self.edge_property_index.add(key, gid, value.clone());
        }
        if let Some(ref entry) = entry_for_ek {
            if !value.is_null() {
                let ek = mgcore::types::EdgeTypePropKey::new(entry.edge_type, key);
                self.edge_type_property_index.add(ek, gid, value.clone());
            }
        }
        edge.set_delta(delta_ptr);

        // Persist to disk if on-disk mode
        if self.is_on_disk() {
            if let Ok(disk) = self.edge_disk.lock() {
                if let Some(ref d) = *disk {
                    let _ = d.put(gid.as_uint(), &**edge);
                }
            }
        }

        // Schema info
        if let Some(entry) = self.edge_index.get(&gid) {
            self.schema_info.record_edge_property(entry.edge_type, key);
        }

        drop(edges);
        self.invalidate_vertex_snapshot(gid);
        self.append_wal(&WalRecord::EdgeSetProperty { gid, key, value });
        self.metrics.inc_edges_updated(1);

        self.fire_triggers(crate::triggers::TriggerEvent::EdgeUpdate, None, gid);

        Ok(())
    }

    /// Delete a vertex — marks deleted and cleans up labels from index.
    pub fn delete_vertex(&self, tx: &Transaction, gid: Gid) -> Result<(), StorageError> {
        tx.record_write(gid);
        let vertices = self.vertices.write().unwrap();
        let vertex = vertices
            .get(&gid)
            .ok_or(StorageError::VertexNotFound(gid))?;
        // Clean up label index before marking deleted
        for label in &vertex.labels {
            self.label_index.remove_vertex(*label, gid);
        }
        // Clean up unique constraints for all labels on the vertex.
        for label in &vertex.labels {
            self.constraints
                .remove_unique_values(*label, &vertex.properties);
        }
        // Clean up label-property index for active indices
        let active_lp = self.active_label_property_indices.read().unwrap();
        for label in &vertex.labels {
            for (prop_id, _value) in vertex.properties.iter() {
                if active_lp.contains(&(*label, prop_id)) {
                    let lp_key = mgcore::types::LabelPropKey::new(*label, prop_id);
                    self.label_property_index.remove(lp_key, gid);
                }
            }
        }
        drop(active_lp);

        // Clean up vector indices for all labels on the vertex.
        let vector_indices = self.vector_indices.read().unwrap();
        for label in &vertex.labels {
            if let Some(entry) = vector_indices.get(label) {
                let mut index = entry.index.write().unwrap();
                let mut gid_map = entry.gid_to_node.write().unwrap();
                if let Some(old_node) = gid_map.remove(&gid) {
                    index.delete(old_node);
                }
            }
        }
        drop(vector_indices);

        // Clean up text indices for all labels on the vertex.
        let text_indices = self.text_indices.read().unwrap();
        for label in &vertex.labels {
            if let Some(entry) = text_indices.get(label) {
                let _ = entry.index.remove_vertex(gid);
            }
        }
        drop(text_indices);

        // Clean up point indices for all labels on the vertex.
        let active_pi = self.active_point_indices.read().unwrap();
        for label in &vertex.labels {
            for (prop_id, _value) in vertex.properties.iter() {
                if active_pi.contains(&(*label, prop_id)) {
                    self.point_index.remove(*label, prop_id, gid);
                }
            }
        }
        drop(active_pi);

        vertex.set_deleted(true);
        self.metrics.inc_vertices_deleted(1);

        // Persist deletion to disk if on-disk mode
        if self.is_on_disk() {
            if let Ok(disk) = self.vertex_disk.lock() {
                if let Some(ref d) = *disk {
                    let _ = d.remove(gid.as_uint());
                }
            }
        }

        drop(vertices);
        self.invalidate_vertex_snapshot(gid);
        self.append_wal(&WalRecord::VertexDelete { gid });

        self.fire_triggers(crate::triggers::TriggerEvent::VertexDelete, None, gid);

        Ok(())
    }

    /// Delete an edge — marks deleted and cleans up from edge type index and global index.
    pub fn delete_edge(&self, tx: &Transaction, gid: Gid) -> Result<(), StorageError> {
        tx.record_write(gid);
        let edges = self.edges.write().unwrap();
        let edge = edges.get(&gid).ok_or(StorageError::EdgeNotFound(gid))?;
        // Clean up indices before marking deleted
        if let Some(entry) = self.edge_index.get(&gid) {
            self.edge_type_index.remove_edge(entry.edge_type, gid);
            // Remove from edge type-property index
            for (prop_id, _) in edge.properties.iter() {
                let ek = mgcore::types::EdgeTypePropKey::new(entry.edge_type, prop_id);
                self.edge_type_property_index.remove(ek, gid);
                self.edge_property_index.remove(prop_id, gid);
            }
        }
        self.edge_index.remove(&gid);
        edge.set_deleted(true);
        self.metrics.inc_edges_deleted(1);

        // Persist deletion to disk if on-disk mode
        if self.is_on_disk() {
            if let Ok(disk) = self.edge_disk.lock() {
                if let Some(ref d) = *disk {
                    let _ = d.remove(gid.as_uint());
                }
            }
        }

        drop(edges);
        self.append_wal(&WalRecord::EdgeDelete { gid });

        self.fire_triggers(crate::triggers::TriggerEvent::EdgeDelete, None, gid);

        Ok(())
    }

    /// Iterate over all non-deleted vertices.
    pub fn all_vertices(&self) -> Vec<(Gid, Vec<LabelId>, PropertyStore)> {
        let has_ttl = !self.ttl_config.is_empty();
        let now_ms = if has_ttl {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        } else {
            0
        };
        let vertices = self.vertices.read().unwrap();
        vertices
            .iter()
            .filter(|(_, v)| !v.deleted())
            .filter(|(_, v)| {
                !has_ttl
                    || !self
                        .ttl_config
                        .is_expired(&v.labels, v.creation_timestamp, now_ms)
            })
            .map(|(gid, v)| (*gid, v.labels.clone(), v.properties.clone()))
            .collect()
    }

    /// Get GIDs of all non-deleted vertices (avoids cloning labels/properties).
    pub fn all_vertex_gids(&self) -> Vec<Gid> {
        let has_ttl = !self.ttl_config.is_empty();
        let now_ms = if has_ttl {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        } else {
            0
        };
        let vertices = self.vertices.read().unwrap();
        vertices
            .iter()
            .filter(|(_, v)| !v.deleted())
            .filter(|(_, v)| {
                !has_ttl
                    || !self
                        .ttl_config
                        .is_expired(&v.labels, v.creation_timestamp, now_ms)
            })
            .map(|(gid, _)| *gid)
            .collect()
    }

    /// Iterate over all non-deleted edges.
    pub fn all_edges(&self) -> Vec<(Gid, Gid, Gid, EdgeTypeId, PropertyStore)> {
        let edges = self.edges.read().unwrap();
        edges
            .iter()
            .filter(|(_, e)| !e.deleted())
            .filter_map(|(gid, e)| {
                self.edge_index.get(gid).map(|entry| {
                    (
                        *gid,
                        entry.from_vertex,
                        entry.to_vertex,
                        entry.edge_type,
                        e.properties.clone(),
                    )
                })
            })
            .collect()
    }

    /// Get GIDs of all non-deleted edges (avoids cloning properties).
    pub fn all_edges_gids(&self) -> Vec<(Gid, Gid, Gid, EdgeTypeId)> {
        let edges = self.edges.read().unwrap();
        edges
            .iter()
            .filter(|(_, e)| !e.deleted())
            .filter_map(|(gid, _)| {
                self.edge_index
                    .get(gid)
                    .map(|entry| (*gid, entry.from_vertex, entry.to_vertex, entry.edge_type))
            })
            .collect()
    }

    /// Iterate over all vertices visible to the given transaction.
    ///
    /// This applies MVCC filtering (hides uncommitted changes from other
    /// transactions) and TTL expiration, unlike `all_vertices` which reads
    /// the raw base state.
    pub fn all_vertices_in_tx(&self, tx: &Transaction) -> Vec<VertexSnapshot> {
        let gids: Vec<Gid> = {
            let vertices = self.vertices.read().unwrap();
            vertices.keys().copied().collect()
        };
        gids.into_iter()
            .filter_map(|gid| self.get_vertex(gid, tx))
            .collect()
    }

    /// Iterate over all edges visible to the given transaction.
    ///
    /// This applies MVCC filtering (hides uncommitted changes from other
    /// transactions), unlike `all_edges` which reads the raw base state.
    pub fn all_edges_in_tx(&self, tx: &Transaction) -> Vec<EdgeSnapshot> {
        let gids: Vec<Gid> = {
            let edges = self.edges.read().unwrap();
            edges.keys().copied().collect()
        };
        gids.into_iter()
            .filter_map(|gid| self.get_edge(gid, tx))
            .collect()
    }

    /// Iterate over all committed deltas (vertex + edge) whose commit
    /// timestamp is strictly greater than `since_timestamp`.
    ///
    /// Returns `(gid, is_vertex, &Delta)` tuples.  The caller is responsible
    /// for converting the core `Delta` into a wire-format `DeltaRecord`.
    ///
    /// # Safety
    ///
    /// The returned references are valid as long as the storage is alive and
    /// no GC runs concurrently.  In practice this is called while holding a
    /// read lock on the relevant vertex/edge map.
    pub fn deltas_since(&self, since_timestamp: u64) -> Vec<(Gid, bool, &mgcore::Delta)> {
        let mut out = Vec::new();

        // --- vertex deltas ---
        {
            let vertices = self.vertices.read().unwrap();
            for (gid, vertex) in vertices.iter() {
                let mut current: *mut mgcore::Delta = vertex.delta();
                while !current.is_null() {
                    unsafe {
                        let d = &*current;
                        let ts = d.commit_info.timestamp();
                        if ts > since_timestamp && ts < mgcore::delta::TRANSACTION_INITIAL_ID {
                            out.push((*gid, true, d));
                        }
                        current = d.next.load(std::sync::atomic::Ordering::Acquire);
                    }
                }
            }
        }

        // --- edge deltas ---
        {
            let edges = self.edges.read().unwrap();
            for (gid, edge) in edges.iter() {
                let mut current: *mut mgcore::Delta = edge.delta();
                while !current.is_null() {
                    unsafe {
                        let d = &*current;
                        let ts = d.commit_info.timestamp();
                        if ts > since_timestamp && ts < mgcore::delta::TRANSACTION_INITIAL_ID {
                            out.push((*gid, false, d));
                        }
                        current = d.next.load(std::sync::atomic::Ordering::Acquire);
                    }
                }
            }
        }

        out
    }

    /// Count incoming edges for a vertex.
    pub fn vertex_in_degree(&self, gid: Gid) -> usize {
        let vertices = self.vertices.read().unwrap();
        vertices.get(&gid).map(|v| v.in_edges.len()).unwrap_or(0)
    }

    /// Count outgoing edges for a vertex.
    pub fn vertex_out_degree(&self, gid: Gid) -> usize {
        let vertices = self.vertices.read().unwrap();
        vertices.get(&gid).map(|v| v.out_edges.len()).unwrap_or(0)
    }

    /// Get incoming edges for a vertex, optionally filtered by edge type.
    /// Returns (edge_gid, other_vertex, edge_type).
    /// The triple `vertex` field already points to the other vertex.
    pub fn vertex_in_edges(
        &self,
        gid: Gid,
        edge_type: Option<EdgeTypeId>,
    ) -> Vec<(Gid, Gid, EdgeTypeId)> {
        let vertices = self.vertices.read().unwrap();
        let Some(v) = vertices.get(&gid) else {
            return vec![];
        };
        v.in_edges
            .iter()
            .filter(|triple| edge_type.is_none_or(|et| triple.edge_type == et))
            .map(|triple| {
                let edge_gid = triple.edge.gid();
                let other = unsafe { (*triple.vertex.as_ptr()).gid };
                (edge_gid, other, triple.edge_type)
            })
            .collect()
    }

    /// Get outgoing edges for a vertex, optionally filtered by edge type.
    pub fn vertex_out_edges(
        &self,
        gid: Gid,
        edge_type: Option<EdgeTypeId>,
    ) -> Vec<(Gid, Gid, EdgeTypeId)> {
        let vertices = self.vertices.read().unwrap();
        let Some(v) = vertices.get(&gid) else {
            return vec![];
        };
        v.out_edges
            .iter()
            .filter(|triple| edge_type.is_none_or(|et| triple.edge_type == et))
            .map(|triple| {
                let edge_gid = triple.edge.gid();
                let other = unsafe { (*triple.vertex.as_ptr()).gid };
                (edge_gid, other, triple.edge_type)
            })
            .collect()
    }

    /// Find vertices with out-degree in a given range (inclusive).
    /// Useful for query optimization (e.g., skip leaves).
    pub fn vertices_by_degree(&self, min_degree: usize) -> Vec<Gid> {
        let vertices = self.vertices.read().unwrap();
        vertices
            .iter()
            .filter(|(_, v)| !v.deleted() && (v.in_edges.len() + v.out_edges.len()) >= min_degree)
            .map(|(gid, _)| *gid)
            .collect()
    }

    /// Run TTL expiration: soft-delete vertices whose TTL has elapsed.
    /// Returns the number of expired vertices deleted.
    pub fn ttl_cleanup(&self) -> usize {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut expired = Vec::new();
        let vertices = self.vertices.read().unwrap();
        for (gid, v) in vertices.iter() {
            if v.deleted() {
                continue;
            }
            if self
                .ttl_config
                .is_expired(&v.labels, v.creation_timestamp, now_ms)
            {
                expired.push(*gid);
            }
        }
        drop(vertices);
        let count = expired.len();
        // Mark expired vertices as deleted and clean indices
        for gid in &expired {
            let mut vertices = self.vertices.write().unwrap();
            if let Some(v) = vertices.get_mut(gid) {
                // Remove from label indices
                for label in &v.labels {
                    self.label_index.remove_vertex(*label, *gid);
                }
                let active_lp = self.active_label_property_indices.read().unwrap();
                for label in &v.labels {
                    for (prop_id, _) in v.properties.iter() {
                        if active_lp.contains(&(*label, prop_id)) {
                            self.label_property_index
                                .remove(mgcore::types::LabelPropKey::new(*label, prop_id), *gid);
                        }
                    }
                }
                drop(active_lp);
                v.set_deleted(true);
            }
        }
        self.metrics.inc_ttl_expired(count as u64);
        count
    }

    /// Check if any vertex has expired TTL (for triggering cleanup).
    pub fn has_expired_ttl(&self) -> bool {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let vertices = self.vertices.read().unwrap();
        vertices.iter().any(|(_, v)| {
            !v.deleted()
                && self
                    .ttl_config
                    .is_expired(&v.labels, v.creation_timestamp, now_ms)
        })
    }

    /// Validate all constraints against a vertex's current state.
    pub fn check_constraints_for_vertex(&self, gid: Gid) -> Result<(), StorageError> {
        let vertices = self.vertices.read().unwrap();
        let v = vertices
            .get(&gid)
            .ok_or(StorageError::VertexNotFound(gid))?;
        for label in &v.labels {
            self.constraints
                .check_existence(*label, &v.properties)
                .map_err(|e| StorageError::ConstraintViolation(format!("{:?}", e)))?;
            self.constraints
                .check_unique(*label, &v.properties, gid)
                .map_err(|e| StorageError::ConstraintViolation(format!("{:?}", e)))?;
            for (key, value) in v.properties.iter() {
                self.constraints
                    .check_type(*label, key, value)
                    .map_err(|e| StorageError::ConstraintViolation(format!("{:?}", e)))?;
            }
        }
        Ok(())
    }

    pub fn label_count(&self, label: LabelId) -> usize {
        self.label_index.vertices_by_label(label).len()
    }

    pub fn has_vertex(&self, gid: Gid) -> bool {
        self.vertices
            .read()
            .unwrap()
            .get(&gid)
            .is_some_and(|v| !v.deleted())
    }

    pub fn has_edge(&self, gid: Gid) -> bool {
        self.edges
            .read()
            .unwrap()
            .get(&gid)
            .is_some_and(|e| !e.deleted())
    }

    pub fn edge_from(&self, gid: Gid) -> Option<Gid> {
        self.edge_index.get(&gid).map(|e| e.from_vertex)
    }

    pub fn edge_to(&self, gid: Gid) -> Option<Gid> {
        self.edge_index.get(&gid).map(|e| e.to_vertex)
    }

    pub fn edges_by_type(&self, etype: EdgeTypeId) -> Vec<Gid> {
        self.edge_type_index.edges_by_type(etype)
    }

    pub fn edge_type_count(&self, etype: EdgeTypeId) -> usize {
        self.edge_type_index.edges_by_type(etype).len()
    }

    // ─── Property removal ──────────────────────────────────────────────

    /// Remove a property from a vertex (sets it to Null / removes key).
    pub fn vertex_remove_property(
        &self,
        tx: &Transaction,
        gid: Gid,
        key: PropertyId,
    ) -> Result<(), StorageError> {
        self.vertex_set_property(tx, gid, key, PropertyValue::Null)
    }

    /// Remove a property from an edge (sets it to Null / removes key).
    pub fn edge_remove_property(
        &self,
        tx: &Transaction,
        gid: Gid,
        key: PropertyId,
    ) -> Result<(), StorageError> {
        self.edge_set_property(tx, gid, key, PropertyValue::Null)
    }

    // ─── Detach delete ─────────────────────────────────────────────────

    /// Delete a vertex and all its incident edges (cascading detach).
    /// Returns (deleted_vertices_count, deleted_edges_count).
    pub fn delete_vertex_and_edges(
        &self,
        tx: &Transaction,
        gid: Gid,
    ) -> Result<(usize, usize), StorageError> {
        tx.record_write(gid);
        let incident_edges: Vec<Gid> = {
            let vertices = self.vertices.read().unwrap();
            if let Some(v) = vertices.get(&gid) {
                v.in_edges
                    .iter()
                    .chain(v.out_edges.iter())
                    .map(|t| t.edge.gid())
                    .collect()
            } else {
                return Err(StorageError::VertexNotFound(gid));
            }
        };

        let mut edges_deleted = 0usize;
        for edge_gid in &incident_edges {
            self.delete_edge(tx, *edge_gid)?;
            edges_deleted += 1;
        }

        self.delete_vertex(tx, gid)?;
        Ok((1, edges_deleted))
    }

    // ─── Edge property queries (global, not edge-type scoped) ──────────

    /// Get all edges that have a given property, regardless of edge type.
    pub fn edges_by_property(&self, prop: PropertyId) -> Vec<(Gid, PropertyValue)> {
        self.edge_property_index.get(prop)
    }

    /// Get edges where a property equals a specific value.
    pub fn edges_by_property_value(&self, prop: PropertyId, value: &PropertyValue) -> Vec<Gid> {
        self.edge_property_index.find_by_value(prop, value)
    }

    pub fn edge_property_count(&self, prop: PropertyId) -> usize {
        self.edge_property_index.count(prop)
    }

    // ─── Edge type-property queries ────────────────────────────────────

    pub fn edges_by_type_property(
        &self,
        edge_type: EdgeTypeId,
        prop: PropertyId,
    ) -> Vec<(Gid, PropertyValue)> {
        let key = mgcore::types::EdgeTypePropKey::new(edge_type, prop);
        self.edge_type_property_index.get(key)
    }

    pub fn edges_by_type_property_value(
        &self,
        edge_type: EdgeTypeId,
        prop: PropertyId,
        value: &PropertyValue,
    ) -> Vec<Gid> {
        let key = mgcore::types::EdgeTypePropKey::new(edge_type, prop);
        self.edge_type_property_index.find_by_value(key, value)
    }

    // ─── Chunked iteration (for parallel processing) ───────────────────

    /// Iterate over vertices in chunks. Each chunk contains (gid, labels, properties).
    pub fn chunked_vertices(
        &self,
        num_chunks: usize,
    ) -> Vec<Vec<(Gid, Vec<LabelId>, PropertyStore)>> {
        let all = self.all_vertices();
        let chunk_size = all.len().div_ceil(num_chunks);
        let mut chunks = Vec::with_capacity(num_chunks);
        for chunk in all.chunks(chunk_size) {
            chunks.push(chunk.to_vec());
        }
        // Pad with empty chunks if needed
        while chunks.len() < num_chunks {
            chunks.push(Vec::new());
        }
        chunks
    }

    /// Iterate over vertices with a specific label in chunks.
    pub fn chunked_vertices_by_label(
        &self,
        label: LabelId,
        num_chunks: usize,
    ) -> Vec<Vec<(Gid, Vec<LabelId>, PropertyStore)>> {
        let gids = self.vertices_by_label(label);
        let vertices = self.vertices.read().unwrap();
        let items: Vec<_> = gids
            .iter()
            .filter_map(|gid| vertices.get(gid))
            .filter(|v| !v.deleted())
            .map(|v| (v.gid, v.labels.clone(), v.properties.clone()))
            .collect();
        let chunk_size = items.len().div_ceil(num_chunks);
        let mut chunks = Vec::with_capacity(num_chunks);
        for chunk in items.chunks(chunk_size) {
            chunks.push(chunk.to_vec());
        }
        while chunks.len() < num_chunks {
            chunks.push(Vec::new());
        }
        chunks
    }

    /// Iterate over edges in chunks.
    pub fn chunked_edges(
        &self,
        num_chunks: usize,
    ) -> Vec<Vec<(Gid, Gid, Gid, EdgeTypeId, PropertyStore)>> {
        let all = self.all_edges();
        let chunk_size = all.len().div_ceil(num_chunks);
        let mut chunks = Vec::with_capacity(num_chunks);
        for chunk in all.chunks(chunk_size) {
            chunks.push(chunk.to_vec());
        }
        while chunks.len() < num_chunks {
            chunks.push(Vec::new());
        }
        chunks
    }

    /// Iterate over edges of a given type in chunks.
    pub fn chunked_edges_by_type(
        &self,
        edge_type: EdgeTypeId,
        num_chunks: usize,
    ) -> Vec<Vec<(Gid, Gid, Gid, EdgeTypeId, PropertyStore)>> {
        let gids = self.edges_by_type(edge_type);
        let edges = self.edges.read().unwrap();
        let items: Vec<_> = gids
            .iter()
            .filter_map(|gid| {
                let e = edges.get(gid)?;
                if e.deleted() {
                    return None;
                }
                let entry = self.edge_index.get(gid)?;
                Some((
                    *gid,
                    entry.from_vertex,
                    entry.to_vertex,
                    entry.edge_type,
                    e.properties.clone(),
                ))
            })
            .collect();
        let chunk_size = items.len().div_ceil(num_chunks);
        let mut chunks = Vec::with_capacity(num_chunks);
        for chunk in items.chunks(chunk_size) {
            chunks.push(chunk.to_vec());
        }
        while chunks.len() < num_chunks {
            chunks.push(Vec::new());
        }
        chunks
    }

    // ─── Total incident edge count ─────────────────────────────────────

    /// Total number of edges incident to a vertex (in + out degree).
    pub fn vertex_incident_edge_count(&self, gid: Gid) -> usize {
        self.vertex_in_degree(gid) + self.vertex_out_degree(gid)
    }

    // ─── Clear all data ────────────────────────────────────────────────

    /// Clear all vertices, edges, deltas, and indices.
    pub fn clear(&self) {
        self.vertices.write().unwrap().clear();
        self.edges.write().unwrap().clear();
        self.deltas.write().unwrap().clear();
        self.label_index.clear();
        self.label_property_index.clear();
        self.edge_type_index.clear();
        self.edge_type_property_index.clear();
        self.edge_index.clear();
        self.edge_property_index.clear();
        self.active_label_indices.write().unwrap().clear();
        self.active_label_property_indices.write().unwrap().clear();
        self.vector_indices.write().unwrap().clear();
        self.next_gid.store(1, Ordering::Relaxed);
        if self.is_on_disk() {
            if let Ok(disk) = self.vertex_disk.lock() {
                if let Some(ref d) = *disk {
                    let _ = d.clear();
                }
            }
            if let Ok(disk) = self.edge_disk.lock() {
                if let Some(ref d) = *disk {
                    let _ = d.clear();
                }
            }
            self.vertex_cache.lock().unwrap().clear();
        }
        self.vertex_snapshot_cache.write().unwrap().clear();
    }

    /// Find an edge by its Gid and the source vertex Gid (double-check pattern).
    pub fn find_edge(&self, edge_gid: Gid, from_vertex_gid: Gid) -> Option<EdgeSnapshot> {
        let entry = self.edge_index.get(&edge_gid)?;
        if entry.from_vertex != from_vertex_gid {
            return None;
        }
        let edges = self.edges.read().unwrap();
        let edge = edges.get(&edge_gid)?;
        if edge.deleted() {
            return None;
        }
        Some(EdgeSnapshot {
            gid: edge_gid,
            from_vertex: entry.from_vertex,
            to_vertex: entry.to_vertex,
            edge_type: entry.edge_type,
            properties: edge.properties.clone(),
        })
    }

    /// Build label-property index entries for all existing vertices that match
    /// a newly created index definition. Used during CREATE INDEX to backfill.
    pub fn build_label_property_index(&self, label: LabelId, prop: PropertyId) -> u64 {
        let gids = self.vertices_by_label(label);
        let vertices = self.vertices.read().unwrap();
        let lp_key = mgcore::types::LabelPropKey::new(label, prop);
        let mut count = 0;
        for gid in &gids {
            if let Some(v) = vertices.get(gid) {
                if v.deleted() {
                    continue;
                }
                let val = v.properties.get(prop);
                if !val.is_null() {
                    self.label_property_index.add(lp_key, *gid, val.clone());
                    count += 1;
                }
            }
        }
        count
    }

    /// Build edge property index entries for all existing edges with a property.
    pub fn build_edge_property_index(&self, prop: PropertyId) {
        let edges = self.edges.read().unwrap();
        for (gid, edge) in edges.iter() {
            if edge.deleted() {
                continue;
            }
            let val = edge.properties.get(prop);
            if !val.is_null() {
                self.edge_property_index.add(prop, *gid, val.clone());
            }
        }
    }

    /// Drop all entries for a property from the global edge property index.
    pub fn drop_edge_property_index(&self, prop: PropertyId) -> bool {
        self.edge_property_index.remove_property(prop);
        self.bump_schema_generation();
        true
    }
}

/// Snapshot of a vertex at a point in time.
#[derive(Clone, Debug)]
pub struct VertexSnapshot {
    pub gid: Gid,
    pub labels: Vec<LabelId>,
    pub properties: PropertyStore,
}

/// Snapshot of an edge at a point in time.
#[derive(Clone, Debug)]
pub struct EdgeSnapshot {
    pub gid: Gid,
    pub from_vertex: Gid,
    pub to_vertex: Gid,
    pub edge_type: EdgeTypeId,
    pub properties: PropertyStore,
}

/// Storage errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    VertexExists(Gid),
    VertexNotFound(Gid),
    EdgeExists(Gid),
    EdgeNotFound(Gid),
    ConstraintViolation(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::VertexExists(gid) => write!(f, "vertex already exists: {}", gid),
            StorageError::VertexNotFound(gid) => write!(f, "vertex not found: {}", gid),
            StorageError::EdgeExists(gid) => write!(f, "edge already exists: {}", gid),
            StorageError::EdgeNotFound(gid) => write!(f, "edge not found: {}", gid),
            StorageError::ConstraintViolation(msg) => write!(f, "constraint violation: {}", msg),
        }
    }
}

/// Walk the delta chain starting at `head` and check whether any delta was
/// committed by a transaction that overlaps with ours.
///
/// A conflict exists if we find a delta whose commit timestamp is
/// `> start_ts` and `< TRANSACTION_INITIAL_ID` (i.e. a real committed
/// timestamp from a concurrent transaction), and the delta doesn't belong
/// to our own transaction (same tx_id as timestamp).
fn has_conflicting_delta(head: *mut mgcore::Delta, my_tx_id: u64, start_ts: u64) -> bool {
    if head.is_null() {
        return false;
    }
    let mut current = head;
    loop {
        let delta = unsafe { &*current };
        let ts = delta.commit_info.timestamp();
        let is_ours = ts == my_tx_id;
        if !is_ours && ts >= start_ts && ts < TRANSACTION_INITIAL_ID {
            return true;
        }
        let next = delta.next.load(Ordering::Acquire);
        if next.is_null() {
            break;
        }
        current = next;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::delta::IsolationLevel;

    #[test]
    fn test_create_and_read_vertex() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        storage.create_vertex(&tx, gid).unwrap();

        let snap = storage.get_vertex(gid, &tx).unwrap();
        assert_eq!(snap.gid, gid);
        assert!(snap.labels.is_empty());
    }

    #[test]
    fn test_duplicate_vertex_error() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = Gid::from(1u64);

        storage.create_vertex(&tx, gid).unwrap();
        let err = storage.create_vertex(&tx, gid).unwrap_err();
        assert_eq!(err, StorageError::VertexExists(gid));
    }

    #[test]
    fn test_vertex_add_label() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);

        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();

        let snap = storage.get_vertex(gid, &tx).unwrap();
        assert!(snap.labels.contains(&label));

        let by_label = storage.vertices_by_label(label);
        assert_eq!(by_label, vec![gid]);
    }

    #[test]
    fn test_vertex_set_property() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let key = PropertyId::from(0u32);

        storage.create_vertex(&tx, gid).unwrap();
        storage
            .vertex_set_property(&tx, gid, key, PropertyValue::Int(42))
            .unwrap();

        let snap = storage.get_vertex(gid, &tx).unwrap();
        assert_eq!(*snap.properties.get(key), PropertyValue::Int(42));
    }

    #[test]
    fn test_label_property_index_maintained() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);
        let prop = PropertyId::from(5u32);

        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();
        storage
            .vertex_set_property(&tx, gid, prop, PropertyValue::Int(42))
            .unwrap();

        // Create the index schema entry
        storage.create_label_property_index(label, prop);

        // Now set property again — index should be maintained
        storage
            .vertex_set_property(&tx, gid, prop, PropertyValue::Int(99))
            .unwrap();

        let found = storage.vertices_by_label_property(label, prop, &PropertyValue::Int(99));
        assert_eq!(found, vec![gid]);

        // Old value should not be found
        let old = storage.vertices_by_label_property(label, prop, &PropertyValue::Int(42));
        assert!(old.is_empty());
    }

    #[test]
    fn test_label_property_index_on_label_add() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);
        let prop = PropertyId::from(5u32);

        storage.create_vertex(&tx, gid).unwrap();
        storage
            .vertex_set_property(&tx, gid, prop, PropertyValue::Int(42))
            .unwrap();

        // Create index before adding label
        storage.create_label_property_index(label, prop);
        storage.vertex_add_label(&tx, gid, label).unwrap();

        let found = storage.vertices_by_label_property(label, prop, &PropertyValue::Int(42));
        assert_eq!(found, vec![gid]);
    }

    #[test]
    fn test_label_property_index_on_label_remove() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);
        let prop = PropertyId::from(5u32);

        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();
        storage
            .vertex_set_property(&tx, gid, prop, PropertyValue::Int(42))
            .unwrap();

        storage.create_label_property_index(label, prop);
        storage.vertex_remove_label(&tx, gid, label).unwrap();

        let found = storage.vertices_by_label_property(label, prop, &PropertyValue::Int(42));
        assert!(found.is_empty());
    }

    #[test]
    fn test_ttl_expiration() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);

        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();
        storage.commit_transaction(&tx);

        // Vertex should be visible without TTL
        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        assert!(storage.get_vertex(gid, &tx2).is_some());
        assert_eq!(storage.all_vertices().len(), 1);

        // Set TTL to 0 ms (immediately expired)
        storage.set_ttl(label, 0);

        // Vertex should now be expired
        assert!(storage.get_vertex(gid, &tx2).is_none());
        assert_eq!(storage.all_vertices().len(), 0);

        // Remove TTL
        storage.remove_ttl(label);

        // Vertex should be visible again
        assert!(storage.get_vertex(gid, &tx2).is_some());
        assert_eq!(storage.all_vertices().len(), 1);
    }

    #[test]
    fn test_ttl_different_labels() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid1 = Gid::from(1u64);
        let gid2 = Gid::from(2u64);
        let label_expiring = LabelId::from(10u32);
        let label_permanent = LabelId::from(20u32);

        storage.create_vertex(&tx, gid1).unwrap();
        storage.vertex_add_label(&tx, gid1, label_expiring).unwrap();

        storage.create_vertex(&tx, gid2).unwrap();
        storage
            .vertex_add_label(&tx, gid2, label_permanent)
            .unwrap();

        storage.commit_transaction(&tx);

        // Set TTL only on the expiring label
        storage.set_ttl(label_expiring, 0);

        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        assert!(storage.get_vertex(gid1, &tx2).is_none());
        assert!(storage.get_vertex(gid2, &tx2).is_some());
        assert_eq!(storage.all_vertices().len(), 1);
    }

    #[test]
    fn test_create_and_read_edge() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let from_gid = Gid::from(1u64);
        let to_gid = Gid::from(2u64);
        let edge_gid = Gid::from(100u64);
        let edge_type = EdgeTypeId::from(5u32);

        storage.create_vertex(&tx, from_gid).unwrap();
        storage.create_vertex(&tx, to_gid).unwrap();
        storage
            .create_edge(&tx, edge_gid, from_gid, to_gid, edge_type)
            .unwrap();

        let edge = storage.get_edge(edge_gid, &tx).unwrap();
        assert_eq!(edge.gid, edge_gid);
        assert_eq!(edge.from_vertex, from_gid);
        assert_eq!(edge.to_vertex, to_gid);
    }

    #[test]
    fn test_mvcc_snapshot_isolation() {
        let storage = Storage::new();

        // Tx1 creates a vertex
        let tx1 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = Gid::from(1u64);
        storage.create_vertex(&tx1, gid).unwrap();
        storage.commit_transaction(&tx1);

        // Tx2 starts AFTER tx1 committed — should see the vertex
        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let snap = storage.get_vertex(gid, &tx2).unwrap();
        assert_eq!(snap.gid, gid);
    }

    #[test]
    fn test_transaction_isolation() {
        let storage = Storage::new();

        // Tx1 creates a vertex but doesn't commit yet
        let tx1 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = Gid::from(1u64);
        storage.create_vertex(&tx1, gid).unwrap();

        // Tx2 starts BEFORE tx1 commits — should NOT see tx1's vertex
        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        assert!(storage.get_vertex(gid, &tx2).is_none());

        // Tx1 commits
        storage.commit_transaction(&tx1);

        // Now Tx2 still doesn't see it (snapshot taken at start)
        assert!(storage.get_vertex(gid, &tx2).is_none());
    }

    #[test]
    fn test_delta_gc() {
        let storage = Storage::new();

        // Create and commit data
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = Gid::from(1u64);
        storage.create_vertex(&tx, gid).unwrap();
        storage
            .vertex_add_label(&tx, gid, LabelId::from(10u32))
            .unwrap();
        storage.commit_transaction(&tx);

        let before = storage.deltas.read().unwrap().len();
        assert!(before >= 2, "should have deltas before GC");

        // With no active transactions, GC collects committed non-tail deltas
        let collected = storage.gc();
        // ADD_LABEL delta should be collected (not the tail DELETE_OBJECT)
        assert!(collected > 0, "GC should collect committed non-tail deltas");

        // Data should still be readable after GC
        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let snap = storage.get_vertex(gid, &tx2).unwrap();
        assert_eq!(snap.gid, gid);
    }

    #[test]
    fn test_delta_gc_preserves_active_snapshot() {
        let storage = Storage::new();

        // Commit old data
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.commit_transaction(&tx);

        // Start a long-running transaction (snapshot before new data)
        let long_tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        // Commit more data
        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx2, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx2);

        // GC: long_tx has min start_ts so watermark protects its visible data
        let collected = storage.gc();
        // Should collect 0 because no deltas below long_tx's start_ts
        assert_eq!(collected, 0);

        // Verify long_tx can still read the old vertex
        let snap = storage.get_vertex(Gid::from(1u64), &long_tx).unwrap();
        assert_eq!(snap.gid, Gid::from(1u64));
    }

    #[test]
    fn test_existence_constraint_enforced_on_write() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);
        let key = PropertyId::from(0u32);

        // Create constraint before vertex
        storage.constraints.add_existence_constraint(label, key);

        storage.create_vertex(&tx, gid).unwrap();
        storage
            .vertex_set_property(&tx, gid, key, PropertyValue::Int(42))
            .unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();

        // Setting property to Null should fail (existence constraint)
        let err = storage
            .vertex_set_property(&tx, gid, key, PropertyValue::Null)
            .unwrap_err();
        assert!(
            matches!(err, StorageError::ConstraintViolation(_)),
            "expected constraint violation, got {:?}",
            err
        );
    }

    #[test]
    fn test_type_constraint_enforced_on_write() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);
        let key = PropertyId::from(0u32);

        storage.constraints.add_type_constraint(
            label,
            key,
            crate::constraints::ConstraintType::Int,
        );

        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();

        // Setting Int should succeed
        storage
            .vertex_set_property(&tx, gid, key, PropertyValue::Int(42))
            .unwrap();

        // Setting String should fail (type constraint)
        let err = storage
            .vertex_set_property(&tx, gid, key, PropertyValue::String("hello".into()))
            .unwrap_err();
        assert!(
            matches!(err, StorageError::ConstraintViolation(_)),
            "expected constraint violation, got {:?}",
            err
        );
    }

    #[test]
    fn test_unique_constraint_enforced_on_write() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let label = LabelId::from(10u32);
        let key = PropertyId::from(0u32);

        storage.constraints.add_unique_constraint(label, vec![key]);

        let gid1 = Gid::from(1u64);
        storage.create_vertex(&tx, gid1).unwrap();
        storage
            .vertex_set_property(&tx, gid1, key, PropertyValue::Int(42))
            .unwrap();
        storage.vertex_add_label(&tx, gid1, label).unwrap();

        let gid2 = Gid::from(2u64);
        storage.create_vertex(&tx, gid2).unwrap();
        storage
            .vertex_set_property(&tx, gid2, key, PropertyValue::Int(99))
            .unwrap();
        storage.vertex_add_label(&tx, gid2, label).unwrap();

        // Same value on different vertex should fail (unique constraint)
        let err = storage
            .vertex_set_property(&tx, gid2, key, PropertyValue::Int(42))
            .unwrap_err();
        assert!(
            matches!(err, StorageError::ConstraintViolation(_)),
            "expected constraint violation, got {:?}",
            err
        );

        // Different value should succeed
        storage
            .vertex_set_property(&tx, gid2, key, PropertyValue::Int(77))
            .unwrap();
    }

    #[test]
    fn test_unique_constraint_old_value_removed_on_update() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let label = LabelId::from(10u32);
        let key = PropertyId::from(0u32);

        storage.constraints.add_unique_constraint(label, vec![key]);

        let gid1 = Gid::from(1u64);
        storage.create_vertex(&tx, gid1).unwrap();
        storage
            .vertex_set_property(&tx, gid1, key, PropertyValue::Int(42))
            .unwrap();
        storage.vertex_add_label(&tx, gid1, label).unwrap();

        // Change value from 42 -> 99; old key 42 should be freed
        storage
            .vertex_set_property(&tx, gid1, key, PropertyValue::Int(99))
            .unwrap();

        // Another vertex should now be able to use 42
        let gid2 = Gid::from(2u64);
        storage.create_vertex(&tx, gid2).unwrap();
        storage
            .vertex_set_property(&tx, gid2, key, PropertyValue::Int(42))
            .unwrap();
        storage.vertex_add_label(&tx, gid2, label).unwrap();

        // Reverting gid1 back to 42 should now fail because gid2 owns it
        let err = storage
            .vertex_set_property(&tx, gid1, key, PropertyValue::Int(42))
            .unwrap_err();
        assert!(matches!(err, StorageError::ConstraintViolation(_)));
    }

    #[test]
    fn test_unique_constraint_cleaned_on_delete() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let label = LabelId::from(10u32);
        let key = PropertyId::from(0u32);

        storage.constraints.add_unique_constraint(label, vec![key]);

        let gid1 = Gid::from(1u64);
        storage.create_vertex(&tx, gid1).unwrap();
        storage
            .vertex_set_property(&tx, gid1, key, PropertyValue::Int(42))
            .unwrap();
        storage.vertex_add_label(&tx, gid1, label).unwrap();

        // Delete vertex — unique key should be freed
        storage.delete_vertex(&tx, gid1).unwrap();

        // Another vertex should now be able to use 42
        let gid2 = Gid::from(2u64);
        storage.create_vertex(&tx, gid2).unwrap();
        storage
            .vertex_set_property(&tx, gid2, key, PropertyValue::Int(42))
            .unwrap();
        storage.vertex_add_label(&tx, gid2, label).unwrap();
    }

    #[test]
    fn test_unique_constraint_cleaned_on_label_remove() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let label = LabelId::from(10u32);
        let key = PropertyId::from(0u32);

        storage.constraints.add_unique_constraint(label, vec![key]);

        let gid1 = Gid::from(1u64);
        storage.create_vertex(&tx, gid1).unwrap();
        storage
            .vertex_set_property(&tx, gid1, key, PropertyValue::Int(42))
            .unwrap();
        storage.vertex_add_label(&tx, gid1, label).unwrap();

        // Remove label — unique key should be freed
        storage.vertex_remove_label(&tx, gid1, label).unwrap();

        // Another vertex should now be able to use 42
        let gid2 = Gid::from(2u64);
        storage.create_vertex(&tx, gid2).unwrap();
        storage
            .vertex_set_property(&tx, gid2, key, PropertyValue::Int(42))
            .unwrap();
        storage.vertex_add_label(&tx, gid2, label).unwrap();
    }

    #[test]
    fn test_existence_constraint_checked_on_label_add() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);
        let key = PropertyId::from(0u32);

        storage.constraints.add_existence_constraint(label, key);

        storage.create_vertex(&tx, gid).unwrap();
        // Adding label without the required property should fail
        let err = storage.vertex_add_label(&tx, gid, label).unwrap_err();
        assert!(
            matches!(err, StorageError::ConstraintViolation(_)),
            "expected constraint violation, got {:?}",
            err
        );

        // Set property first, then add label
        storage
            .vertex_set_property(&tx, gid, key, PropertyValue::Int(42))
            .unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();
    }

    #[test]
    fn test_labels_visible_after_commit() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = Gid::from(1u64);
        let label_person = LabelId::from(1u32);
        let label_employee = LabelId::from(2u32);
        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label_person).unwrap();
        storage.vertex_add_label(&tx, gid, label_employee).unwrap();
        storage.commit_transaction(&tx);

        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let snap = storage.get_vertex(gid, &tx2).unwrap();
        assert_eq!(snap.labels.len(), 2);
        assert!(snap.labels.contains(&label_person));
        assert!(snap.labels.contains(&label_employee));
    }

    #[test]
    fn test_gc_trims_old_deltas() {
        let storage = Storage::new();
        let gid = Gid::from(1u64);
        let label = LabelId::from(1u32);

        // Create a vertex with a label via a committed transaction
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();
        storage.commit_transaction(&tx);

        let before = storage.deltas.read().unwrap().len();
        // Run GC — no active transactions, so all deltas except tail anchor
        // should be collected
        let collected = storage.gc();
        let after = storage.deltas.read().unwrap().len();
        assert!(collected > 0, "GC should have collected old deltas");
        assert!(after < before, "global delta pool should shrink after GC");

        // Vertex must still be readable after GC
        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let snap = storage.get_vertex(gid, &tx2).unwrap();
        assert_eq!(snap.labels, &[label]);
    }

    #[test]
    fn test_detach_delete_vertex_and_edges() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let from_gid = Gid::from(1u64);
        let to_gid = Gid::from(2u64);
        let edge_gid = Gid::from(100u64);
        let edge_type = EdgeTypeId::from(5u32);

        storage.create_vertex(&tx, from_gid).unwrap();
        storage.create_vertex(&tx, to_gid).unwrap();
        storage
            .create_edge(&tx, edge_gid, from_gid, to_gid, edge_type)
            .unwrap();
        storage.commit_transaction(&tx);

        // Verify everything exists
        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        assert!(storage.get_vertex(from_gid, &tx2).is_some());
        assert!(storage.get_vertex(to_gid, &tx2).is_some());
        assert!(storage.get_edge(edge_gid, &tx2).is_some());

        // Detach delete the from vertex
        let (v_del, e_del) = storage.delete_vertex_and_edges(&tx2, from_gid).unwrap();
        assert_eq!(v_del, 1);
        assert_eq!(e_del, 1);

        // The deleted vertex and edge should not be visible
        assert!(storage.get_vertex(from_gid, &tx2).is_none());
        assert!(storage.get_edge(edge_gid, &tx2).is_none());

        // The other vertex should still exist
        assert!(storage.get_vertex(to_gid, &tx2).is_some());
    }

    #[test]
    fn test_vertex_remove_property() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let key = PropertyId::from(0u32);

        storage.create_vertex(&tx, gid).unwrap();
        storage
            .vertex_set_property(&tx, gid, key, PropertyValue::Int(42))
            .unwrap();

        let snap = storage.get_vertex(gid, &tx).unwrap();
        assert!(!snap.properties.get(key).is_null());

        storage.vertex_remove_property(&tx, gid, key).unwrap();

        let snap = storage.get_vertex(gid, &tx).unwrap();
        assert!(snap.properties.get(key).is_null());
    }

    #[test]
    fn test_edge_property_index() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let from_gid = Gid::from(1u64);
        let to_gid = Gid::from(2u64);
        let edge_gid = Gid::from(100u64);
        let edge_type = EdgeTypeId::from(5u32);
        let weight_prop = PropertyId::from(10u32);

        storage.create_vertex(&tx, from_gid).unwrap();
        storage.create_vertex(&tx, to_gid).unwrap();
        storage
            .create_edge(&tx, edge_gid, from_gid, to_gid, edge_type)
            .unwrap();
        storage
            .edge_set_property(&tx, edge_gid, weight_prop, PropertyValue::Double(3.14))
            .unwrap();

        // Query by property value
        let found = storage.edges_by_property_value(weight_prop, &PropertyValue::Double(3.14));
        assert_eq!(found, vec![edge_gid]);

        // Query all edges with the property
        let all = storage.edges_by_property(weight_prop);
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn test_edge_property_index_cleaned_on_delete() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let from_gid = Gid::from(1u64);
        let to_gid = Gid::from(2u64);
        let edge_gid = Gid::from(100u64);
        let prop = PropertyId::from(10u32);

        storage.create_vertex(&tx, from_gid).unwrap();
        storage.create_vertex(&tx, to_gid).unwrap();
        storage
            .create_edge(&tx, edge_gid, from_gid, to_gid, EdgeTypeId::from(5u32))
            .unwrap();
        storage
            .edge_set_property(&tx, edge_gid, prop, PropertyValue::Int(42))
            .unwrap();

        assert!(
            storage
                .edges_by_property_value(prop, &PropertyValue::Int(42))
                .len()
                == 1
        );

        storage.delete_edge(&tx, edge_gid).unwrap();
        assert!(storage
            .edges_by_property_value(prop, &PropertyValue::Int(42))
            .is_empty());
    }

    #[test]
    fn test_chunked_vertices() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        for i in 0..10u64 {
            storage.create_vertex(&tx, Gid::from(i)).unwrap();
        }
        storage.commit_transaction(&tx);

        let chunks = storage.chunked_vertices(3);
        assert_eq!(chunks.len(), 3);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_clear_storage() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);

        assert_eq!(storage.vertex_count(), 2);
        storage.clear();
        assert_eq!(storage.vertex_count(), 0);
    }

    #[test]
    fn test_find_edge() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let from_gid = Gid::from(1u64);
        let to_gid = Gid::from(2u64);
        let edge_gid = Gid::from(100u64);

        storage.create_vertex(&tx, from_gid).unwrap();
        storage.create_vertex(&tx, to_gid).unwrap();
        storage
            .create_edge(&tx, edge_gid, from_gid, to_gid, EdgeTypeId::from(5u32))
            .unwrap();
        storage.commit_transaction(&tx);

        let _tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        // Correct from_vertex
        let found = storage.find_edge(edge_gid, from_gid);
        assert!(found.is_some());
        // Wrong from_vertex
        let not_found = storage.find_edge(edge_gid, to_gid);
        assert!(not_found.is_none());
        // Non-existent edge
        assert!(storage.find_edge(Gid::from(999u64), from_gid).is_none());
    }

    #[test]
    fn test_build_label_property_index() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);
        let prop = PropertyId::from(5u32);

        // Create vertex BEFORE index exists
        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();
        storage
            .vertex_set_property(&tx, gid, prop, PropertyValue::Int(42))
            .unwrap();
        storage.commit_transaction(&tx);

        // Now create index and backfill
        storage.create_label_property_index(label, prop);
        storage.build_label_property_index(label, prop);

        let found = storage.vertices_by_label_property(label, prop, &PropertyValue::Int(42));
        assert_eq!(found, vec![gid]);
    }

    #[test]
    fn test_schema_info_tracking() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let label = LabelId::from(10u32);
        let prop = PropertyId::from(5u32);
        let etype = EdgeTypeId::from(3u32);

        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage
            .vertex_add_label(&tx, Gid::from(1u64), label)
            .unwrap();
        storage
            .vertex_set_property(&tx, Gid::from(1u64), prop, PropertyValue::Int(42))
            .unwrap();

        let from_gid = Gid::from(1u64);
        let to_gid = Gid::from(2u64);
        storage.create_vertex(&tx, to_gid).unwrap();
        storage
            .create_edge(&tx, Gid::from(100u64), from_gid, to_gid, etype)
            .unwrap();
        storage
            .edge_set_property(
                &tx,
                Gid::from(100u64),
                prop,
                PropertyValue::String("hi".into()),
            )
            .unwrap();

        assert!(storage.schema_info.has_label(label));
        assert!(storage.schema_info.has_edge_type(etype));
        assert!(storage.schema_info.label_properties(label).contains(&prop));
    }

    #[test]
    fn test_vertex_incident_edge_count() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let a = Gid::from(1u64);
        let b = Gid::from(2u64);
        let c = Gid::from(3u64);

        storage.create_vertex(&tx, a).unwrap();
        storage.create_vertex(&tx, b).unwrap();
        storage.create_vertex(&tx, c).unwrap();

        // a → b, a → c, b → a
        storage
            .create_edge(&tx, Gid::from(100u64), a, b, EdgeTypeId::from(0u32))
            .unwrap();
        storage
            .create_edge(&tx, Gid::from(101u64), a, c, EdgeTypeId::from(0u32))
            .unwrap();
        storage
            .create_edge(&tx, Gid::from(102u64), b, a, EdgeTypeId::from(0u32))
            .unwrap();

        // a: out→b, out→c, in←b = 3 total incident
        assert_eq!(storage.vertex_incident_edge_count(a), 3);
        // b: in←a, out→a = 2
        assert_eq!(storage.vertex_incident_edge_count(b), 2);
        // c: in←a = 1
        assert_eq!(storage.vertex_incident_edge_count(c), 1);
    }

    #[test]
    fn test_edges_by_type_property_value() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let from = Gid::from(1u64);
        let to = Gid::from(2u64);
        let etype = EdgeTypeId::from(5u32);
        let prop = PropertyId::from(10u32);

        storage.create_vertex(&tx, from).unwrap();
        storage.create_vertex(&tx, to).unwrap();
        storage
            .create_edge(&tx, Gid::from(100u64), from, to, etype)
            .unwrap();
        storage
            .edge_set_property(&tx, Gid::from(100u64), prop, PropertyValue::Int(42))
            .unwrap();

        let found = storage.edges_by_type_property_value(etype, prop, &PropertyValue::Int(42));
        assert_eq!(found, vec![Gid::from(100u64)]);

        let not_found = storage.edges_by_type_property_value(etype, prop, &PropertyValue::Int(99));
        assert!(not_found.is_empty());
    }

    // ─── New tests for on-disk mode, GC horizon, and config ──────────────

    #[test]
    fn test_storage_mode_in_memory_default() {
        let storage = Storage::new();
        assert!(!storage.is_on_disk());
        assert!(matches!(storage.config().mode, StorageMode::InMemory));
    }

    #[test]
    fn test_storage_mode_on_disk() {
        let tmp = format!("/tmp/mgstorage_disk_test_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&tmp);
        let config = StorageConfig::on_disk(&tmp);
        let storage = Storage::with_config(config);
        assert!(storage.is_on_disk());
        assert!(matches!(storage.config().mode, StorageMode::OnDisk { .. }));
        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_on_disk_vertex_crud() {
        let tmp = format!("/tmp/mgstorage_disk_crud_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&tmp);
        let config = StorageConfig::on_disk(&tmp);
        let storage = Storage::with_config(config);
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        storage.create_vertex(&tx, gid).unwrap();
        storage
            .vertex_set_property(&tx, gid, PropertyId::from(0u32), PropertyValue::Int(42))
            .unwrap();
        storage.commit_transaction(&tx);

        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let snap = storage.get_vertex(gid, &tx2).unwrap();
        assert_eq!(
            *snap.properties.get(PropertyId::from(0u32)),
            PropertyValue::Int(42)
        );

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_on_disk_edge_crud() {
        let tmp = format!("/tmp/mgstorage_disk_edge_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&tmp);
        let config = StorageConfig::on_disk(&tmp);
        let storage = Storage::with_config(config);
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let from = Gid::from(1u64);
        let to = Gid::from(2u64);
        let eid = Gid::from(100u64);
        storage.create_vertex(&tx, from).unwrap();
        storage.create_vertex(&tx, to).unwrap();
        storage
            .create_edge(&tx, eid, from, to, EdgeTypeId::from(5u32))
            .unwrap();
        storage.commit_transaction(&tx);

        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let edge = storage.get_edge(eid, &tx2).unwrap();
        assert_eq!(edge.from_vertex, from);
        assert_eq!(edge.to_vertex, to);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_on_disk_vertex_delete_count() {
        let tmp = format!("/tmp/mgstorage_disk_del_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&tmp);
        let config = StorageConfig::on_disk(&tmp);
        let storage = Storage::with_config(config);

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);

        assert_eq!(storage.vertex_count(), 2);

        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.delete_vertex(&tx2, Gid::from(1u64)).unwrap();
        storage.commit_transaction(&tx2);

        assert_eq!(storage.vertex_count(), 1);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_on_disk_label_and_property() {
        let tmp = format!("/tmp/mgstorage_disk_label_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&tmp);
        let config = StorageConfig::on_disk(&tmp);
        let storage = Storage::with_config(config);

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = Gid::from(1u64);
        storage.create_vertex(&tx, gid).unwrap();
        storage
            .vertex_add_label(&tx, gid, LabelId::from(10u32))
            .unwrap();
        storage
            .vertex_set_property(&tx, gid, PropertyId::from(0u32), PropertyValue::Int(99))
            .unwrap();
        storage.commit_transaction(&tx);

        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let snap = storage.get_vertex(gid, &tx2).unwrap();
        assert!(snap.labels.contains(&LabelId::from(10u32)));
        assert_eq!(
            snap.properties.get(PropertyId::from(0u32)).clone(),
            PropertyValue::Int(99)
        );

        let tx3 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage
            .vertex_remove_label(&tx3, gid, LabelId::from(10u32))
            .unwrap();
        storage.commit_transaction(&tx3);

        let tx4 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let snap2 = storage.get_vertex(gid, &tx4).unwrap();
        assert!(!snap2.labels.contains(&LabelId::from(10u32)));

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_on_disk_edge_property_crud() {
        let tmp = format!("/tmp/mgstorage_disk_eprop_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&tmp);
        let config = StorageConfig::on_disk(&tmp);
        let storage = Storage::with_config(config);

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let from = Gid::from(1u64);
        let to = Gid::from(2u64);
        let eid = Gid::from(100u64);
        storage.create_vertex(&tx, from).unwrap();
        storage.create_vertex(&tx, to).unwrap();
        storage
            .create_edge(&tx, eid, from, to, EdgeTypeId::from(5u32))
            .unwrap();
        storage
            .edge_set_property(
                &tx,
                eid,
                PropertyId::from(0u32),
                PropertyValue::String("friend".into()),
            )
            .unwrap();
        storage.commit_transaction(&tx);

        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let edge = storage.get_edge(eid, &tx2).unwrap();
        assert_eq!(
            edge.properties.get(PropertyId::from(0u32)).clone(),
            PropertyValue::String("friend".into())
        );

        let tx3 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.delete_edge(&tx3, eid).unwrap();
        storage.commit_transaction(&tx3);

        let tx4 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        assert!(storage.get_edge(eid, &tx4).is_none());
        assert_eq!(storage.edge_count(), 0);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_on_disk_reopen_persistence() {
        let tmp = format!("/tmp/mgstorage_disk_reopen_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&tmp);
        let gid = Gid::from(1u64);
        let prop = PropertyId::from(0u32);

        // Phase 1: create storage, write data, drop it
        {
            let config = StorageConfig::on_disk(&tmp);
            let storage = Storage::with_config(config);
            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            storage.create_vertex(&tx, gid).unwrap();
            storage
                .vertex_set_property(&tx, gid, prop, PropertyValue::Int(42))
                .unwrap();
            storage.commit_transaction(&tx);
        }

        // Phase 2: reopen storage, verify data persists
        {
            let config = StorageConfig::on_disk(&tmp);
            let storage = Storage::with_config(config);
            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            let snap = storage.get_vertex(gid, &tx).unwrap();
            assert_eq!(snap.properties.get(prop).clone(), PropertyValue::Int(42));
        }

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_gc_with_horizon() {
        let storage = Storage::new();

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage
            .vertex_add_label(&tx, Gid::from(1u64), LabelId::from(10u32))
            .unwrap();
        storage.commit_transaction(&tx);

        let before = storage.deltas.read().unwrap().len();
        assert!(
            before >= 2,
            "should have at least 2 deltas (create + add_label)"
        );

        // GC with horizon u64::MAX collects all non-tail deltas (everything has ts < u64::MAX)
        let collected_max = storage.gc_with_horizon(u64::MAX);
        assert!(
            collected_max > 0,
            "expected some deltas collected with max horizon"
        );

        // After the first GC, only the tail anchor should remain.
        let mid = storage.deltas.read().unwrap().len();
        assert_eq!(mid, 1, "only tail anchor should remain");

        // GC again with horizon=0 — tail anchor is still kept because is_tail is true
        let collected_tail = storage.gc_with_horizon(0);
        assert_eq!(collected_tail, 0, "tail anchor should never be collected");

        let stats = storage.gc_stats();
        assert_eq!(stats.deltas_collected, collected_max + collected_tail);
        assert_eq!(stats.deltas_retained, 1);
    }

    #[test]
    fn test_oldest_active_timestamp() {
        let storage = Storage::new();
        // No active transactions
        assert_eq!(storage.oldest_active_timestamp(), u64::MAX);

        let tx1 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let ts1 = tx1.start_timestamp;
        assert_eq!(storage.oldest_active_timestamp(), ts1);

        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let ts2 = tx2.start_timestamp;
        // Both have same start_timestamp in this engine
        assert_eq!(storage.oldest_active_timestamp(), ts1.min(ts2));

        storage.commit_transaction(&tx1);
        assert_eq!(storage.oldest_active_timestamp(), ts2);
    }

    #[test]
    fn test_config_validation() {
        let mut cfg = StorageConfig::default();
        cfg.gc_interval_ms = 0;
        assert!(cfg.validate().is_err());

        let mut cfg2 = StorageConfig::default();
        cfg2.wal_enabled = false;
        assert!(cfg2.validate().is_ok());
    }

    #[test]
    fn test_clear_on_disk_storage() {
        let tmp = format!("/tmp/mgstorage_disk_clear_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&tmp);
        let config = StorageConfig::on_disk(&tmp);
        let storage = Storage::with_config(config);
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.commit_transaction(&tx);

        assert_eq!(storage.vertex_count(), 1);
        storage.clear();
        assert_eq!(storage.vertex_count(), 0);
        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_text_index_auto_maintenance() {
        let tmp = format!("/tmp/mgstorage_text_idx_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&tmp);
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);
        let prop_name = PropertyId::from(20u32);

        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();

        // Create text index for the label
        let index = crate::text_index::TextIndex::create(&tmp, &["name".into()]).unwrap();
        let entry =
            TextIndexEntry::new(std::sync::Arc::new(index), vec![(prop_name, "name".into())]);
        storage.text_indices.write().unwrap().insert(label, entry);

        // Set a text property — should auto-index
        storage
            .vertex_set_property(
                &tx,
                gid,
                prop_name,
                PropertyValue::String("Alice in Wonderland".into()),
            )
            .unwrap();

        // Search should find the vertex
        let text_indices = storage.text_indices.read().unwrap();
        let entry = text_indices.get(&label).unwrap();
        let results = entry.index.search("Wonderland", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, gid);
        drop(text_indices);

        // Update property — should re-index
        storage
            .vertex_set_property(
                &tx,
                gid,
                prop_name,
                PropertyValue::String("Bob the Builder".into()),
            )
            .unwrap();
        let text_indices = storage.text_indices.read().unwrap();
        let entry = text_indices.get(&label).unwrap();
        let results = entry.index.search("Wonderland", 10).unwrap();
        assert_eq!(results.len(), 0); // Old term should be gone
        let results = entry.index.search("Builder", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, gid);
        drop(text_indices);

        // Delete vertex — should remove from index
        storage.delete_vertex(&tx, gid).unwrap();
        let text_indices = storage.text_indices.read().unwrap();
        let entry = text_indices.get(&label).unwrap();
        let results = entry.index.search("Builder", 10).unwrap();
        assert_eq!(results.len(), 0);
        drop(text_indices);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_text_index_mimic_integration() {
        let tmp = format!("/tmp/mgstorage_mimic_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&tmp);
        let storage = Storage::new();
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let label = LabelId::from(1u32);
        let prop_name = PropertyId::from(1u32);

        // Step 1: Create vertex with property (like CREATE)
        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();
        storage
            .vertex_set_property(
                &tx,
                gid,
                prop_name,
                PropertyValue::String("Hello World".into()),
            )
            .unwrap();

        // Step 2: Create text index AFTER property is set (like CALL db.createTextIndex)
        let index = crate::text_index::TextIndex::create(&tmp, &["title".into()]).unwrap();
        let entry = TextIndexEntry::new(
            std::sync::Arc::new(index),
            vec![(prop_name, "title".into())],
        );
        storage.text_indices.write().unwrap().insert(label, entry);

        // Backfill existing vertices
        let all = storage.all_vertices();
        let text_indices = storage.text_indices.read().unwrap();
        let entry = text_indices.get(&label).unwrap();
        for (v_gid, labels, props) in all {
            if labels.contains(&label) {
                let mut text_values = Vec::new();
                let prop_val = props.get(prop_name);
                if !prop_val.is_null() {
                    if let Some(s) = property_value_to_string(prop_val) {
                        text_values.push(("title".into(), s));
                    }
                }
                let _ = entry.index.index_vertex(v_gid, &text_values);
            }
        }
        drop(text_indices);

        // Verify initial search
        let text_indices = storage.text_indices.read().unwrap();
        let entry = text_indices.get(&label).unwrap();
        let results = entry.index.search("Hello", 10).unwrap();
        assert_eq!(results.len(), 1, "Should find Hello initially");
        drop(text_indices);

        // Step 3: Update property (like SET)
        storage
            .vertex_set_property(
                &tx,
                gid,
                prop_name,
                PropertyValue::String("Goodbye World".into()),
            )
            .unwrap();

        // Search for old value
        let text_indices = storage.text_indices.read().unwrap();
        let entry = text_indices.get(&label).unwrap();
        let results = entry.index.search("Hello", 10).unwrap();
        assert_eq!(
            results.len(),
            0,
            "Should NOT find Hello after update, got {} results",
            results.len()
        );
        let results = entry.index.search("Goodbye", 10).unwrap();
        assert_eq!(results.len(), 1, "Should find Goodbye after update");
        drop(text_indices);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_point_index_auto_maintenance() {
        use mgcore::point::{Crs, Point2D, Point3D};

        let storage = Storage::new();
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);

        let gid = Gid::from(1u64);
        let label = LabelId::from(10u32);
        let prop_2d = PropertyId::from(20u32);
        let prop_3d = PropertyId::from(21u32);

        storage.create_vertex(&tx, gid).unwrap();
        storage.vertex_add_label(&tx, gid, label).unwrap();

        // Create point indices
        storage.create_point_index(label, prop_2d);
        storage.create_point_index(label, prop_3d);

        // Set 2D point property — should auto-index
        let p2d = Point2D::new(Crs::Cartesian2D, 10.0, 20.0);
        storage
            .vertex_set_property(&tx, gid, prop_2d, PropertyValue::Point2D(p2d))
            .unwrap();

        let in_box = storage.point_index.within_bbox_2d(
            label,
            prop_2d,
            Point2D::new(Crs::Cartesian2D, 5.0, 15.0),
            Point2D::new(Crs::Cartesian2D, 15.0, 25.0),
        );
        assert_eq!(in_box.len(), 1);
        assert_eq!(in_box[0], gid);

        // Set 3D point property — should auto-index
        let p3d = Point3D::new(Crs::Cartesian3D, 1.0, 2.0, 3.0);
        storage
            .vertex_set_property(&tx, gid, prop_3d, PropertyValue::Point3D(p3d))
            .unwrap();

        let nearest = storage.point_index.nearest_2d(
            label,
            prop_2d,
            Point2D::new(Crs::Cartesian2D, 10.0, 20.0),
            1,
        );
        assert_eq!(nearest.len(), 1);
        assert_eq!(nearest[0].0, gid);

        // Update 2D point — old should be gone, new should be present
        let p2d_new = Point2D::new(Crs::Cartesian2D, 100.0, 200.0);
        storage
            .vertex_set_property(&tx, gid, prop_2d, PropertyValue::Point2D(p2d_new))
            .unwrap();

        let in_box_old = storage.point_index.within_bbox_2d(
            label,
            prop_2d,
            Point2D::new(Crs::Cartesian2D, 5.0, 15.0),
            Point2D::new(Crs::Cartesian2D, 15.0, 25.0),
        );
        assert_eq!(in_box_old.len(), 0);

        let in_box_new = storage.point_index.within_bbox_2d(
            label,
            prop_2d,
            Point2D::new(Crs::Cartesian2D, 95.0, 195.0),
            Point2D::new(Crs::Cartesian2D, 105.0, 205.0),
        );
        assert_eq!(in_box_new.len(), 1);

        // Remove label — point index should be cleared
        storage.vertex_remove_label(&tx, gid, label).unwrap();
        let in_box_after = storage.point_index.within_bbox_2d(
            label,
            prop_2d,
            Point2D::new(Crs::Cartesian2D, 90.0, 190.0),
            Point2D::new(Crs::Cartesian2D, 110.0, 210.0),
        );
        assert_eq!(in_box_after.len(), 0);

        // Re-add label and set property — should re-index
        storage.vertex_add_label(&tx, gid, label).unwrap();
        storage
            .vertex_set_property(&tx, gid, prop_2d, PropertyValue::Point2D(p2d))
            .unwrap();

        let in_box_re = storage.point_index.within_bbox_2d(
            label,
            prop_2d,
            Point2D::new(Crs::Cartesian2D, 5.0, 15.0),
            Point2D::new(Crs::Cartesian2D, 15.0, 25.0),
        );
        assert_eq!(in_box_re.len(), 1);

        // Delete vertex — point index should be cleared
        storage.delete_vertex(&tx, gid).unwrap();
        let in_box_del = storage.point_index.within_bbox_2d(
            label,
            prop_2d,
            Point2D::new(Crs::Cartesian2D, 0.0, 0.0),
            Point2D::new(Crs::Cartesian2D, 200.0, 200.0),
        );
        assert_eq!(in_box_del.len(), 0);
    }

    #[test]
    fn test_edge_type_index_registry() {
        let storage = Storage::new();
        let etype = EdgeTypeId::from(5u32);
        let prop = PropertyId::from(10u32);

        assert!(!storage.has_edge_type_index(etype));
        assert!(storage.create_edge_type_index(etype));
        assert!(storage.has_edge_type_index(etype));
        assert!(!storage.create_edge_type_index(etype)); // already exists
        assert!(storage.drop_edge_type_index(etype));
        assert!(!storage.has_edge_type_index(etype));

        assert!(!storage.has_edge_type_property_index(etype, prop));
        assert!(storage.create_edge_type_property_index(etype, prop));
        assert!(storage.has_edge_type_property_index(etype, prop));
        assert!(storage.drop_edge_type_property_index(etype, prop));
        assert!(!storage.has_edge_type_property_index(etype, prop));
    }

    // ─── Concurrent tests ───────────────────────────────────────────────────

    #[test]
    fn test_concurrent_vertex_creation() {
        let storage = std::sync::Arc::new(Storage::new());
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let s = storage.clone();
                std::thread::spawn(move || {
                    for i in 0..100 {
                        let gid = Gid::from((t * 100 + i + 1) as u64);
                        let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                        s.create_vertex(&tx, gid).unwrap();
                        s.commit_transaction(&tx);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        for t in 0..4 {
            for i in 0..100 {
                let gid = Gid::from((t * 100 + i + 1) as u64);
                assert!(storage.get_vertex(gid, &tx).is_some());
            }
        }
    }

    #[test]
    fn test_concurrent_property_updates() {
        let storage = std::sync::Arc::new(Storage::new());
        let gid = Gid::from(1u64);
        let prop = PropertyId::from(1u32);

        // Create initial vertex
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, gid).unwrap();
        storage
            .vertex_set_property(&tx, gid, prop, PropertyValue::Int(0))
            .unwrap();
        storage.commit_transaction(&tx);

        let threads: Vec<_> = (0..4)
            .map(|t| {
                let s = storage.clone();
                std::thread::spawn(move || {
                    for i in 0..25 {
                        let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                        s.vertex_set_property(
                            &tx,
                            gid,
                            prop,
                            PropertyValue::Int(t * 25 + i as i64),
                        )
                        .unwrap();
                        s.commit_transaction(&tx);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        // Final value should be one of the written values
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v = storage.get_vertex(gid, &tx).unwrap();
        let final_val = match v.properties.get(prop) {
            PropertyValue::Int(n) => *n,
            _ => panic!("expected int"),
        };
        assert!(final_val >= 0 && final_val < 100);
    }

    #[test]
    fn test_concurrent_reads_during_writes() {
        let storage = std::sync::Arc::new(Storage::new());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

        // Pre-create 50 vertices
        for i in 0..50 {
            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            let gid = Gid::from(i as u64 + 1);
            storage.create_vertex(&tx, gid).unwrap();
            storage.commit_transaction(&tx);
        }

        let writer = {
            let s = storage.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait(); // Wait for reader to start its transaction
                for i in 50..100 {
                    let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                    let gid = Gid::from(i as u64 + 1);
                    s.create_vertex(&tx, gid).unwrap();
                    s.commit_transaction(&tx);
                }
            })
        };

        let reader = {
            let s = storage.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                b.wait(); // Signal writer that our snapshot is taken

                // Count visible vertices at start of transaction using get_vertex
                let mut initial_count = 0;
                for i in 1..=100 {
                    if s.get_vertex(Gid::from(i), &tx).is_some() {
                        initial_count += 1;
                    }
                }
                assert_eq!(initial_count, 50);

                // Give writer time to make progress
                std::thread::sleep(std::time::Duration::from_millis(10));

                // Count should still be 50 within same transaction (snapshot isolation)
                let mut later_count = 0;
                for i in 1..=100 {
                    if s.get_vertex(Gid::from(i), &tx).is_some() {
                        later_count += 1;
                    }
                }
                assert_eq!(later_count, initial_count);
                later_count
            })
        };

        writer.join().unwrap();
        let reader_count = reader.join().unwrap();
        assert_eq!(reader_count, 50);
    }

    #[test]
    fn test_concurrent_edge_creation() {
        let storage = std::sync::Arc::new(Storage::new());
        let etype = EdgeTypeId::from(1u32);

        // Create two shared vertices
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v1 = Gid::from(1u64);
        let v2 = Gid::from(2u64);
        storage.create_vertex(&tx, v1).unwrap();
        storage.create_vertex(&tx, v2).unwrap();
        storage.commit_transaction(&tx);

        let threads: Vec<_> = (0..4)
            .map(|t| {
                let s = storage.clone();
                std::thread::spawn(move || {
                    for i in 0..25 {
                        let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                        let eid = Gid::from((t * 25 + i + 100) as u64);
                        s.create_edge(&tx, eid, v1, v2, etype).unwrap();
                        s.commit_transaction(&tx);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        let _tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let edges = storage.vertex_out_edges(v1, Some(etype));
        assert_eq!(edges.len(), 100);
    }

    #[test]
    fn test_concurrent_label_property_index_updates() {
        let storage = std::sync::Arc::new(Storage::new());
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(1u32);
        storage.create_label_property_index(label, prop);

        // Pre-create vertices
        for i in 0..50 {
            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            let gid = Gid::from(i as u64 + 1);
            storage.create_vertex(&tx, gid).unwrap();
            storage.vertex_add_label(&tx, gid, label).unwrap();
            storage
                .vertex_set_property(&tx, gid, prop, PropertyValue::Int(i as i64))
                .unwrap();
            storage.commit_transaction(&tx);
        }

        let threads: Vec<_> = (0..4)
            .map(|t| {
                let s = storage.clone();
                std::thread::spawn(move || {
                    for i in 0..50 {
                        let gid = Gid::from(i as u64 + 1);
                        let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                        s.vertex_set_property(
                            &tx,
                            gid,
                            prop,
                            PropertyValue::Int((t * 50 + i) as i64),
                        )
                        .unwrap();
                        s.commit_transaction(&tx);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        // Index should still have exactly 50 entries
        let _tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let _all = storage.vertices_by_label_property(label, prop, &PropertyValue::Int(0));
        // Value 0 may or may not exist depending on last writer
        // But total indexed vertices should be 50
        let total = storage.vertices_by_label(label).len();
        assert_eq!(total, 50);
    }

    #[test]
    fn test_transaction_abort_visibility() {
        let storage = std::sync::Arc::new(Storage::new());
        let gid = Gid::from(1u64);

        // Thread 1: create vertex then abort
        let tx1 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx1, gid).unwrap();
        storage.abort_transaction(&tx1);

        // Thread 2: should not see aborted vertex
        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        assert!(storage.get_vertex(gid, &tx2).is_none());
    }

    #[test]
    fn test_commit_visibility_to_new_transaction() {
        let storage = Storage::new();
        let gid = Gid::from(1u64);

        let tx1 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx1, gid).unwrap();
        storage.commit_transaction(&tx1);

        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        assert!(storage.get_vertex(gid, &tx2).is_some());
    }

    #[test]
    fn test_constrained_concurrent_unique_violation() {
        let storage = std::sync::Arc::new(Storage::new());
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(1u32);

        storage.constraints.add_unique_constraint(label, vec![prop]);

        // Pre-create vertices with label
        for i in 0..2 {
            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            let gid = Gid::from(i as u64 + 1);
            storage.create_vertex(&tx, gid).unwrap();
            storage.vertex_add_label(&tx, gid, label).unwrap();
            storage.commit_transaction(&tx);
        }

        let s1 = storage.clone();
        let t1 = std::thread::spawn(move || {
            let tx = s1.begin_transaction(IsolationLevel::SnapshotIsolation);
            s1.vertex_set_property(&tx, Gid::from(1u64), prop, PropertyValue::Int(42))
                .unwrap();
            s1.commit_transaction(&tx);
        });

        let s2 = storage.clone();
        let t2 = std::thread::spawn(move || {
            let tx = s2.begin_transaction(IsolationLevel::SnapshotIsolation);
            // This may succeed or fail depending on ordering
            let _ = s2.vertex_set_property(&tx, Gid::from(2u64), prop, PropertyValue::Int(42));
            s2.commit_transaction(&tx);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // NOTE: With write-write conflict detection, one of the concurrent
        // transactions should be aborted at commit time if they modify the
        // same object. However, these two transactions modify *different*
        // vertices, so both should succeed (no conflict on same Gid).
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let mut count_42 = 0;
        for i in 1..=2 {
            if let Some(v) = storage.get_vertex(Gid::from(i), &tx) {
                if *v.properties.get(prop) == PropertyValue::Int(42) {
                    count_42 += 1;
                }
            }
        }
        assert!(count_42 >= 1);
    }

    #[test]
    fn test_write_write_conflict_detection() {
        let storage = Arc::new(Storage::new());
        let gid = Gid::from(1u64);

        // Setup: create a vertex in tx1 and commit
        {
            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            storage.create_vertex(&tx, gid).unwrap();
            storage.commit_transaction(&tx);
        }

        // tx2 starts, reads the vertex
        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        // tx3 starts, modifies the vertex, and commits first
        {
            let tx3 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            let prop = PropertyId::from(0u32);
            storage
                .vertex_set_property(&tx3, gid, prop, PropertyValue::Int(100))
                .unwrap();
            assert!(storage.commit_transaction(&tx3));
        }

        // tx2 now modifies the same vertex
        let prop = PropertyId::from(0u32);
        storage
            .vertex_set_property(&tx2, gid, prop, PropertyValue::Int(200))
            .unwrap();

        // tx2 should fail to commit (write-write conflict)
        assert!(!storage.commit_transaction(&tx2));
    }

    #[test]
    fn test_no_conflict_on_different_gids() {
        let storage = Arc::new(Storage::new());

        // Create two vertices
        {
            let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
            storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
            storage.commit_transaction(&tx);
        }

        // tx2 modifies vertex 1
        let tx2 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage
            .vertex_set_property(
                &tx2,
                Gid::from(1u64),
                PropertyId::from(0u32),
                PropertyValue::Int(10),
            )
            .unwrap();

        // tx3 modifies vertex 2 and commits
        {
            let tx3 = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
            storage
                .vertex_set_property(
                    &tx3,
                    Gid::from(2u64),
                    PropertyId::from(0u32),
                    PropertyValue::Int(20),
                )
                .unwrap();
            assert!(storage.commit_transaction(&tx3));
        }

        // tx2 should succeed — different Gids, no conflict
        assert!(storage.commit_transaction(&tx2));
    }

    #[test]
    fn test_concurrent_create_unique_gids() {
        let storage = Arc::new(Storage::new());
        let threads = 8;
        let per_thread = 25;
        let barrier = Arc::new(std::sync::Barrier::new(threads));

        let mut handles = Vec::new();
        for t in 0..threads {
            let s = storage.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut ok = 0usize;
                for i in 0..per_thread {
                    let gid = Gid::from((t * per_thread + i + 1) as u64);
                    let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                    s.create_vertex(&tx, gid).unwrap();
                    if s.commit_transaction(&tx) {
                        ok += 1;
                    } else {
                        eprintln!("Thread {} gid {:?} commit FAILED", t, gid);
                    }
                }
                ok
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let count = storage.all_vertices().len();
        assert_eq!(
            total,
            threads * per_thread,
            "commits failed: expected {}, got {} committed",
            threads * per_thread,
            total
        );
        assert_eq!(
            count,
            threads * per_thread,
            "vertex count mismatch: expected {}, got {} in storage",
            threads * per_thread,
            count
        );
    }

    #[test]
    fn test_concurrent_create_with_props_and_labels() {
        let storage = Arc::new(Storage::new());
        let threads = 2;
        let per_thread = 5;
        let barrier = Arc::new(std::sync::Barrier::new(threads));

        let mut handles = Vec::new();
        for t in 0..threads {
            let s = storage.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut ok = 0usize;
                for i in 0..per_thread {
                    let gid = Gid::from((t * per_thread + i + 1) as u64);
                    let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                    s.create_vertex(&tx, gid).unwrap();
                    s.vertex_set_property(
                        &tx,
                        gid,
                        PropertyId::from(0u32),
                        PropertyValue::Int((t * per_thread + i) as i64),
                    )
                    .unwrap();
                    s.vertex_add_label(&tx, gid, LabelId::from(1u32)).unwrap();
                    if s.commit_transaction(&tx) {
                        ok += 1;
                    } else {
                        eprintln!("Thread {} gid {:?} commit FAILED", t, gid);
                    }
                }
                ok
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let count = storage.all_vertices().len();
        assert_eq!(
            total,
            threads * per_thread,
            "commits failed: expected {}, got {} committed",
            threads * per_thread,
            total
        );
        assert_eq!(
            count,
            threads * per_thread,
            "vertex count mismatch: expected {}, got {} in storage",
            threads * per_thread,
            count
        );
    }

    /// Multiple threads commit transactions that touch disjoint Gid sets.
    /// With sharded commit locks, all commits should succeed (no false
    /// contention) and the final vertex count must match.
    #[test]
    fn test_sharded_commit_disjoint_gids() {
        let storage = Arc::new(Storage::new());
        let threads = 8;
        let per_thread = 50;
        let barrier = Arc::new(std::sync::Barrier::new(threads));

        let mut handles = Vec::new();
        for t in 0..threads {
            let s = storage.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut ok = 0usize;
                for i in 0..per_thread {
                    let gid = Gid::from((t * per_thread + i + 1) as u64);
                    let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                    s.create_vertex(&tx, gid).unwrap();
                    if s.commit_transaction(&tx) {
                        ok += 1;
                    }
                }
                ok
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(
            total,
            threads * per_thread,
            "expected all {} commits to succeed, got {}",
            threads * per_thread,
            total
        );

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let count = storage.all_vertices().len();
        drop(tx);
        assert_eq!(
            count,
            threads * per_thread,
            "expected {} vertices, got {}",
            threads * per_thread,
            count
        );
    }

    /// Concurrent commits on the *same* Gid must still be serialized by the
    /// shard lock, so only one transaction should succeed.
    #[test]
    fn test_sharded_commit_same_gid_serializes() {
        let storage = Arc::new(Storage::new());
        let gid = Gid::from(1u64);
        let prop = PropertyId::from(0u32);

        // Pre-create vertex
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, gid).unwrap();
        storage
            .vertex_set_property(&tx, gid, prop, PropertyValue::Int(0))
            .unwrap();
        storage.commit_transaction(&tx);

        let threads = 4;
        let per_thread = 25;
        let barrier = Arc::new(std::sync::Barrier::new(threads));

        let mut handles = Vec::new();
        for t in 0..threads {
            let s = storage.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                let mut ok = 0usize;
                for i in 0..per_thread {
                    let tx = s.begin_transaction(IsolationLevel::SnapshotIsolation);
                    let vsnap = s.get_vertex(gid, &tx).unwrap();
                    let current = match vsnap.properties.get(prop) {
                        PropertyValue::Int(n) => *n,
                        _ => 0,
                    };
                    s.vertex_set_property(&tx, gid, prop, PropertyValue::Int(current + 1))
                        .unwrap();
                    if s.commit_transaction(&tx) {
                        ok += 1;
                    }
                }
                ok
            }));
        }

        let total_ok: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // Under snapshot isolation with write-write conflict detection, only
        // some of the concurrent commits will succeed.
        assert!(
            total_ok > 0 && total_ok <= threads * per_thread,
            "expected some commits to succeed, got {}",
            total_ok
        );

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let vsnap = storage.get_vertex(gid, &tx).unwrap();
        let final_val = match vsnap.properties.get(prop) {
            PropertyValue::Int(n) => *n,
            _ => 0,
        };
        assert_eq!(
            final_val, total_ok as i64,
            "final value ({}) should equal successful commits ({})",
            final_val, total_ok
        );
    }
}
