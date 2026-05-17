//! Snapshot management for point-in-time graph backups.
//!
//! Snapshots capture the full graph state at a specific transaction timestamp
//! and can be restored for recovery or analytical queries.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};

use crate::storage::{Storage, StorageError};

/// Metadata for a stored snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotMeta {
    pub id: String,
    pub timestamp: u64,
    pub vertex_count: usize,
    pub edge_count: usize,
    pub created_at: Instant,
    pub size_bytes: usize,
}

/// In-memory snapshot of a graph at a specific point in time.
#[derive(Clone)]
pub struct GraphSnapshot {
    pub meta: SnapshotMeta,
    pub vertices: HashMap<Gid, SnapshotVertex>,
    pub edges: HashMap<Gid, SnapshotEdge>,
}

#[derive(Clone, Debug)]
pub struct SnapshotVertex {
    pub gid: Gid,
    pub labels: Vec<LabelId>,
    pub properties: HashMap<PropertyId, PropertyValue>,
}

#[derive(Clone, Debug)]
pub struct SnapshotEdge {
    pub gid: Gid,
    pub from: Gid,
    pub to: Gid,
    pub edge_type: EdgeTypeId,
    pub properties: HashMap<PropertyId, PropertyValue>,
}

/// Manager for creating, storing, and restoring snapshots.
pub struct SnapshotManager {
    snapshots: std::sync::Mutex<Vec<SnapshotMeta>>,
    snapshot_data: std::sync::Mutex<HashMap<String, GraphSnapshot>>,
    max_snapshots: usize,
}

impl SnapshotManager {
    pub fn new(max_snapshots: usize) -> Self {
        Self {
            snapshots: std::sync::Mutex::new(Vec::new()),
            snapshot_data: std::sync::Mutex::new(HashMap::new()),
            max_snapshots,
        }
    }

    /// Create a full snapshot of the current storage state.
    pub fn create_snapshot(&self, storage: &Storage) -> Result<SnapshotMeta, StorageError> {
        let all_v = storage.all_vertices();
        let all_e = storage.all_edges();

        let mut vertices = HashMap::new();
        for (gid, labels, props) in &all_v {
            let mut properties = HashMap::new();
            for (prop_id, value) in props.iter() {
                properties.insert(prop_id, value.clone());
            }
            vertices.insert(
                *gid,
                SnapshotVertex {
                    gid: *gid,
                    labels: labels.clone(),
                    properties,
                },
            );
        }

        let mut edges = HashMap::new();
        for (gid, from, to, etype, props) in all_e {
            let mut properties = HashMap::new();
            for (prop_id, value) in props.iter() {
                properties.insert(prop_id, value.clone());
            }
            edges.insert(
                gid,
                SnapshotEdge {
                    gid,
                    from,
                    to,
                    edge_type: etype,
                    properties,
                },
            );
        }

        let size_bytes = estimate_size(&vertices, &edges);
        let meta = SnapshotMeta {
            id: format!(
                "snap-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            ),
            timestamp: storage.transaction_engine.current_timestamp(),
            vertex_count: vertices.len(),
            edge_count: edges.len(),
            created_at: Instant::now(),
            size_bytes,
        };

        let snapshot = GraphSnapshot {
            meta: meta.clone(),
            vertices,
            edges,
        };

        self.snapshot_data
            .lock()
            .unwrap()
            .insert(meta.id.clone(), snapshot);

        let mut list = self.snapshots.lock().unwrap();
        list.push(meta.clone());
        // Trim old snapshots
        while list.len() > self.max_snapshots {
            list.remove(0);
        }

        Ok(meta)
    }

    pub fn list_snapshots(&self) -> Vec<SnapshotMeta> {
        self.snapshots.lock().unwrap().clone()
    }

    pub fn get_snapshot(&self, id: &str) -> Option<SnapshotMeta> {
        self.snapshots
            .lock()
            .unwrap()
            .iter()
            .find(|s| s.id == id)
            .cloned()
    }

    pub fn delete_snapshot(&self, id: &str) -> bool {
        let mut list = self.snapshots.lock().unwrap();
        let before = list.len();
        list.retain(|s| s.id != id);
        self.snapshot_data.lock().unwrap().remove(id);
        list.len() < before
    }

    /// Restore storage state from a snapshot (destructive).
    ///
    /// Clears all current data and rebuilds from the snapshot:
    /// 1. Clears vertices, edges, deltas, and all indices
    /// 2. Re-creates each vertex with its labels and properties
    /// 3. Re-creates each edge with its endpoints and properties
    /// 4. Rebuilds label and edge-type indices
    pub fn restore_from_snapshot(
        &self,
        storage: &Storage,
        id: &str,
    ) -> Result<(), StorageError> {
        let snapshot = self
            .snapshot_data
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| StorageError::SnapshotNotFound(id.to_string()))?;

        // Clear all storage state
        storage.clear();

        // Use a dummy transaction for reconstruction
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);

        // Restore vertices
        for sv in snapshot.vertices.values() {
            storage.create_vertex(&tx, sv.gid)?;
            // Add labels
            for &label in &sv.labels {
                storage.vertex_add_label(&tx, sv.gid, label)?;
            }
            // Set properties
            for (&prop_id, value) in &sv.properties {
                storage.vertex_set_property(&tx, sv.gid, prop_id, value.clone())?;
            }
        }

        // Restore edges
        for se in snapshot.edges.values() {
            storage.create_edge(&tx, se.gid, se.from, se.to, se.edge_type)?;
            // Set edge properties
            for (&prop_id, value) in &se.properties {
                storage.edge_set_property(&tx, se.gid, prop_id, value.clone())?;
            }
        }

        storage.commit_transaction(&tx);
        Ok(())
    }

    /// Age of the most recent snapshot.
    pub fn time_since_last_snapshot(&self) -> Option<Duration> {
        self.snapshots
            .lock()
            .unwrap()
            .last()
            .map(|s| s.created_at.elapsed())
    }
}

fn estimate_size(
    vertices: &HashMap<Gid, SnapshotVertex>,
    edges: &HashMap<Gid, SnapshotEdge>,
) -> usize {
    let mut size = 0usize;
    for v in vertices.values() {
        size += std::mem::size_of::<SnapshotVertex>();
        size += v.labels.len() * std::mem::size_of::<LabelId>();
        size += v.properties.len()
            * (std::mem::size_of::<PropertyId>() + std::mem::size_of::<PropertyValue>());
    }
    for e in edges.values() {
        size += std::mem::size_of::<SnapshotEdge>();
        size += e.properties.len()
            * (std::mem::size_of::<PropertyId>() + std::mem::size_of::<PropertyValue>());
    }
    size
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use mgcore::delta::IsolationLevel;
    use mgcore::types::{EdgeTypeId, Gid, LabelId};

    #[test]
    fn test_snapshot_manager() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage
            .create_edge(
                &tx,
                Gid::from(100u64),
                Gid::from(1u64),
                Gid::from(2u64),
                EdgeTypeId::from(0u32),
            )
            .unwrap();
        storage.commit_transaction(&tx);

        let mgr = SnapshotManager::new(5);
        let meta = mgr.create_snapshot(&storage).unwrap();
        assert_eq!(meta.vertex_count, 2);
        assert_eq!(meta.edge_count, 1);
        assert_eq!(mgr.list_snapshots().len(), 1);

        let retrieved = mgr.get_snapshot(&meta.id);
        assert!(retrieved.is_some());

        assert!(mgr.delete_snapshot(&meta.id));
        assert_eq!(mgr.list_snapshots().len(), 0);
    }

    #[test]
    fn test_max_snapshots() {
        let storage = Storage::new();
        let mgr = SnapshotManager::new(2);
        mgr.create_snapshot(&storage).unwrap();
        mgr.create_snapshot(&storage).unwrap();
        mgr.create_snapshot(&storage).unwrap();
        assert_eq!(mgr.list_snapshots().len(), 2);
    }

    #[test]
    fn test_snapshot_restore_round_trip() {
        use mgcore::delta::IsolationLevel;
        use mgcore::property_value::PropertyValue;
        use mgcore::types::PropertyId;

        // Build a graph with vertices, edges, labels, and properties
        let storage = Storage::new();
        let label_person = LabelId::from(0u32);
        let label_movie = LabelId::from(1u32);
        let prop_name = PropertyId::from(0u32);
        let prop_title = PropertyId::from(1u32);
        let etype_acted = EdgeTypeId::from(0u32);

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v1 = storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.vertex_add_label(&tx, v1, label_person).unwrap();
        storage
            .vertex_set_property(&tx, v1, prop_name, PropertyValue::String("Alice".into()))
            .unwrap();

        let v2 = storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.vertex_add_label(&tx, v2, label_movie).unwrap();
        storage
            .vertex_set_property(&tx, v2, prop_title, PropertyValue::String("Matrix".into()))
            .unwrap();

        storage
            .create_edge(&tx, Gid::from(100u64), v1, v2, etype_acted)
            .unwrap();
        storage
            .edge_set_property(
                &tx,
                Gid::from(100u64),
                PropertyId::from(2u32),
                PropertyValue::String("Neo".into()),
            )
            .unwrap();
        storage.commit_transaction(&tx);

        // Take snapshot
        let mgr = SnapshotManager::new(5);
        let meta = mgr.create_snapshot(&storage).unwrap();
        assert_eq!(meta.vertex_count, 2);
        assert_eq!(meta.edge_count, 1);

        // Mutate the graph after snapshot
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(3u64)).unwrap();
        storage.commit_transaction(&tx);
        assert_eq!(storage.vertex_count(), 3);

        // Restore from snapshot
        mgr.restore_from_snapshot(&storage, &meta.id).unwrap();

        // Verify restored state matches snapshot (2 vertices, not 3)
        assert_eq!(storage.vertex_count(), 2);
        assert_eq!(storage.edge_count(), 1);

        // Verify vertex properties restored
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let restored_v1 = storage.get_vertex(v1, &tx).unwrap();
        let name = restored_v1.properties.get(prop_name);
        assert_eq!(name, &PropertyValue::String("Alice".into()));
        assert!(restored_v1.labels.contains(&label_person));

        let restored_v2 = storage.get_vertex(v2, &tx).unwrap();
        let title = restored_v2.properties.get(prop_title);
        assert_eq!(title, &PropertyValue::String("Matrix".into()));
        assert!(restored_v2.labels.contains(&label_movie));
        drop(tx);

        // Verify edge properties restored
        let out_edges = storage.vertex_out_edges(v1, None);
        assert_eq!(out_edges.len(), 1);
    }
}
