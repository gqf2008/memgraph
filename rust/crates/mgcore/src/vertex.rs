use crate::edge_ref::EdgeRef;
use crate::pointer_pack::PointerPack;
use crate::property_store::PropertyStore;
use crate::spin_lock::RwSpinLock;
use crate::types::{EdgeTypeId, Gid, LabelId};

use std::ptr::NonNull;

// ─── EdgeTriple ────────────────────────────────────────────────────────────

/// (edge_type, vertex, edge_ref) — stored in Vertex's in/out edge lists.
/// Equivalent to C++ `EdgeTriple`.
#[derive(Clone, Debug)]
pub struct EdgeTriple {
    pub edge_type: EdgeTypeId,
    pub vertex: NonNull<crate::vertex::Vertex>,
    pub edge: EdgeRef,
}

// ─── Vertex ────────────────────────────────────────────────────────────────

/// A graph vertex (node).
///
/// Layout:
///   - Gid:             8 bytes
///   - labels:         24 bytes (Vec)
///   - in_edges:       24 bytes (Vec)
///   - out_edges:      24 bytes (Vec)
///   - properties:     ≤8 bytes (pointer to PropertyStore storage)
///   - delta_:          8 bytes (PointerPack<2> = AtomicU64)
///   - lock:            4 bytes (AtomicU32)
///     Total:            ~100 bytes
pub struct Vertex {
    /// Globally unique vertex ID.
    pub gid: Gid,

    /// Labels attached to this vertex.
    pub labels: Vec<LabelId>,

    /// Incoming edges (edge_type, other_vertex, edge_ref).
    pub in_edges: Vec<EdgeTriple>,

    /// Outgoing edges (edge_type, other_vertex, edge_ref).
    pub out_edges: Vec<EdgeTriple>,

    /// Property values.
    pub properties: PropertyStore,

    /// Packed pointer to head of delta chain + 2 flag bits
    ///   bit 0: deleted
    ///   bit 1: has_uncommitted_non_sequential_deltas
    delta_: PointerPack<2>,

    /// Reader-writer lock for concurrent access.
    pub lock: RwSpinLock,

    /// Creation timestamp in milliseconds since epoch (used for TTL).
    pub creation_timestamp: u64,
}

impl Vertex {
    const DELETED_BIT: u8 = 0;
    const NON_SEQ_DELTAS_BIT: u8 = 1;

    /// Create a new Vertex with an initial DELETE_OBJECT delta.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn new(gid: Gid, delta: *mut crate::delta::Delta, creation_timestamp: u64) -> Self {
        debug_assert!(
            delta.is_null()
                || unsafe { (*delta).action() } == crate::delta::DeltaAction::DeleteObject
                || unsafe { (*delta).action() }
                    == crate::delta::DeltaAction::DeleteDeserializedObject,
            "Vertex must be created with an initial DELETE_OBJECT delta"
        );
        Self {
            gid,
            labels: Vec::new(),
            in_edges: Vec::new(),
            out_edges: Vec::new(),
            properties: PropertyStore::new(),
            delta_: PointerPack::new_with(delta, 0),
            lock: RwSpinLock::new(),
            creation_timestamp,
        }
    }

    pub fn delta(&self) -> *mut crate::delta::Delta {
        self.delta_.get_ptr()
    }

    pub fn set_delta(&self, d: *mut crate::delta::Delta) {
        self.delta_.set_ptr(d);
    }

    pub fn deleted(&self) -> bool {
        self.delta_.get::<{ Self::DELETED_BIT }, 1>() != 0
    }

    pub fn set_deleted(&self, b: bool) {
        self.delta_
            .set::<{ Self::DELETED_BIT }, 1>(if b { 1 } else { 0 });
    }

    pub fn has_uncommitted_non_sequential_deltas(&self) -> bool {
        self.delta_.get::<{ Self::NON_SEQ_DELTAS_BIT }, 1>() != 0
    }

    pub fn set_has_uncommitted_non_sequential_deltas(&self, b: bool) {
        self.delta_
            .set::<{ Self::NON_SEQ_DELTAS_BIT }, 1>(if b { 1 } else { 0 });
    }
}

impl PartialEq for Vertex {
    fn eq(&self, other: &Self) -> bool {
        self.gid == other.gid
    }
}

impl Eq for Vertex {}

impl PartialOrd for Vertex {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Vertex {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.gid.cmp(&other.gid)
    }
}

impl std::fmt::Debug for Vertex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Vertex")
            .field("gid", &self.gid)
            .field("labels", &self.labels)
            .field("in_edges", &self.in_edges.len())
            .field("out_edges", &self.out_edges.len())
            .field("deleted", &self.deleted())
            .finish()
    }
}

// Safety: Vertex is only accessed under its lock or the storage-level GC lock.
unsafe impl Send for Vertex {}
unsafe impl Sync for Vertex {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::{CommitInfo, Delta};
    use std::sync::Arc;

    #[test]
    fn test_vertex_create() {
        let ci = Arc::new(CommitInfo::new(100));
        let d = Box::into_raw(Box::new(Delta::new_delete_object(ci, 0)));
        let v = Vertex::new(Gid::from(42u64), d, 0);
        assert_eq!(v.gid, Gid::from(42u64));
        assert!(!v.deleted());
        assert_eq!(v.delta(), d);

        unsafe {
            let _ = Box::from_raw(d);
        }
    }

    #[test]
    fn test_vertex_deleted_flag() {
        let v = Vertex::new(Gid::from(1u64), std::ptr::null_mut(), 0);
        assert!(!v.deleted());
        v.set_deleted(true);
        assert!(v.deleted());
        v.set_deleted(false);
        assert!(!v.deleted());
    }

    #[test]
    fn test_vertex_non_seq_flag() {
        let v = Vertex::new(Gid::from(2u64), std::ptr::null_mut(), 0);
        assert!(!v.has_uncommitted_non_sequential_deltas());
        v.set_has_uncommitted_non_sequential_deltas(true);
        assert!(v.has_uncommitted_non_sequential_deltas());
    }
}
