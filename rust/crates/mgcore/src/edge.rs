use crate::pointer_pack::PointerPack;
use crate::property_store::PropertyStore;
use crate::spin_lock::RwSpinLock;
use crate::types::Gid;

// ─── Edge ──────────────────────────────────────────────────────────────────

/// A graph edge (relationship).
///
/// Equivalent to C++ `Edge`.
pub struct Edge {
    /// Globally unique edge ID.
    pub gid: Gid,

    /// Property values.
    pub properties: PropertyStore,

    /// Reader-writer lock for concurrent access.
    pub lock: RwSpinLock,

    /// Packed pointer to head of delta chain + 1 flag bit
    ///   bit 0: deleted
    delta_: PointerPack<1>,
}

impl Edge {
    const DELETED_BIT: u8 = 0;

    /// Create a new Edge with an initial DELETE_OBJECT delta.
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn new(gid: Gid, delta: *mut crate::delta::Delta) -> Self {
        debug_assert!(
            delta.is_null()
                || unsafe { (*delta).action() } == crate::delta::DeltaAction::DeleteObject
                || unsafe { (*delta).action() }
                    == crate::delta::DeltaAction::DeleteDeserializedObject,
            "Edge must be created with an initial DELETE_OBJECT delta"
        );
        Self {
            gid,
            properties: PropertyStore::new(),
            lock: RwSpinLock::new(),
            delta_: PointerPack::new_with(delta, 0),
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
}

impl PartialEq for Edge {
    fn eq(&self, other: &Self) -> bool {
        self.gid == other.gid
    }
}

impl Eq for Edge {}

impl PartialOrd for Edge {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.gid.cmp(&other.gid))
    }
}

impl Ord for Edge {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.gid.cmp(&other.gid)
    }
}

impl std::fmt::Debug for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Edge")
            .field("gid", &self.gid)
            .field("deleted", &self.deleted())
            .finish()
    }
}

// Safety: Edge is only accessed under its lock or the storage-level GC lock.
unsafe impl Send for Edge {}
unsafe impl Sync for Edge {}

// ─── EdgeMetadata ──────────────────────────────────────────────────────────

/// Edge index entry: maps gid → from_vertex.
/// Used by the global edge index for quick edge lookups.
///
/// Equivalent to C++ `EdgeMetadata`.
#[derive(Debug)]
pub struct EdgeMetadata {
    pub gid: Gid,
    pub from_vertex: *mut crate::vertex::Vertex,
}

impl PartialEq for EdgeMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.gid == other.gid
    }
}

impl Eq for EdgeMetadata {}

impl PartialOrd for EdgeMetadata {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.gid.cmp(&other.gid))
    }
}

impl Ord for EdgeMetadata {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.gid.cmp(&other.gid)
    }
}

// Safety: EdgeMetadata pointer is only dereferenced under storage locks.
unsafe impl Send for EdgeMetadata {}
unsafe impl Sync for EdgeMetadata {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::{CommitInfo, Delta};
    use std::sync::Arc;

    #[test]
    fn test_edge_create() {
        let ci = Arc::new(CommitInfo::new(200));
        let d = Box::into_raw(Box::new(Delta::new_delete_object(ci, 0)));
        let e = Edge::new(Gid::from(99u64), d);
        assert_eq!(e.gid, Gid::from(99u64));
        assert!(!e.deleted());
        assert_eq!(e.delta(), d);

        unsafe {
            let _ = Box::from_raw(d);
        }
    }

    #[test]
    fn test_edge_deleted_flag() {
        let ci = Arc::new(CommitInfo::new(300));
        let d = Box::into_raw(Box::new(Delta::new_delete_object(ci, 0)));
        let e = Edge::new(Gid::from(1u64), d);
        assert!(!e.deleted());
        e.set_deleted(true);
        assert!(e.deleted());
        e.set_deleted(false);
        assert!(!e.deleted());

        unsafe {
            let _ = Box::from_raw(d);
        }
    }
}
