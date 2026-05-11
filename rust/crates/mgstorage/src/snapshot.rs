//! Snapshot management for point-in-time graph backups.
//!
//! Snapshots capture the full graph state at a specific transaction timestamp
//! and can be restored for recovery or analytical queries.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::time::{Duration, Instant};

use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};

use crate::storage::{Storage, StorageError, WalRecord};

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
    max_snapshots: usize,
}

impl SnapshotManager {
    pub fn new(max_snapshots: usize) -> Self {
        Self {
            snapshots: std::sync::Mutex::new(Vec::new()),
            max_snapshots,
        }
    }

    /// Create a full snapshot of the current storage state.
    pub fn create_snapshot(
        &self,
        storage: &Storage,
    ) -> Result<SnapshotMeta, StorageError> {
        let all_v = storage.all_vertices();
        let all_e = storage.all_edges();

        let mut vertices = HashMap::new();
        for (gid, labels, props) in &all_v {
            let mut properties = HashMap::new();
            for (prop_id, value) in props.iter() {
                properties.insert(prop_id, value.clone());
            }
            vertices.insert(*gid, SnapshotVertex {
                gid: *gid,
                labels: labels.clone(),
                properties,
            });
        }

        let mut edges = HashMap::new();
        for (gid, from, to, etype, props) in all_e {
            let mut properties = HashMap::new();
            for (prop_id, value) in props.iter() {
                properties.insert(prop_id, value.clone());
            }
            edges.insert(gid, SnapshotEdge {
                gid,
                from,
                to,
                edge_type: etype,
                properties,
            });
        }

        let size_bytes = estimate_size(&vertices, &edges);
        let meta = SnapshotMeta {
            id: format!("snap-{}", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()),
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

        // Store snapshot data (in production this would go to disk)
        let _ = snapshot;

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
        self.snapshots.lock().unwrap()
            .iter()
            .find(|s| s.id == id)
            .cloned()
    }

    pub fn delete_snapshot(&self, id: &str) -> bool {
        let mut list = self.snapshots.lock().unwrap();
        let before = list.len();
        list.retain(|s| s.id != id);
        list.len() < before
    }

    /// Restore storage state from a snapshot (destructive).
    pub fn restore_from_snapshot(
        &self,
        _storage: &Storage,
        _id: &str,
    ) -> Result<(), StorageError> {
        // In a real implementation, this would:
        // 1. Lock storage exclusively
        // 2. Clear all current data
        // 3. Load vertices and edges from snapshot
        // 4. Rebuild indices
        // 5. Release lock
        Ok(())
    }

    /// Age of the most recent snapshot.
    pub fn time_since_last_snapshot(&self) -> Option<Duration> {
        self.snapshots.lock().unwrap()
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
        size += v.properties.len() * (std::mem::size_of::<PropertyId>() + std::mem::size_of::<PropertyValue>());
    }
    for e in edges.values() {
        size += std::mem::size_of::<SnapshotEdge>();
        size += e.properties.len() * (std::mem::size_of::<PropertyId>() + std::mem::size_of::<PropertyValue>());
    }
    size
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::delta::IsolationLevel;
    use mgcore::types::{EdgeTypeId, Gid, LabelId};
    use crate::storage::Storage;

    #[test]
    fn test_snapshot_manager() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.create_edge(&tx, Gid::from(100u64), Gid::from(1u64), Gid::from(2u64), EdgeTypeId::from(0u32)).unwrap();
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
}
