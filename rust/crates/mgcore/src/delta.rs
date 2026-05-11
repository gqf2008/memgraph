use crate::edge_ref::EdgeRef;
use crate::property_value::PropertyValue;
use crate::types::{EdgeTypeId, LabelId, PropertyId};

use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;

// ─── DeltaAction ───────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum DeltaAction {
    DeleteDeserializedObject = 0,
    DeleteObject = 1,
    RecreateObject = 2,
    SetProperty = 3,
    AddLabel = 4,
    RemoveLabel = 5,
    AddInEdge = 6,
    AddOutEdge = 7,
    RemoveInEdge = 8,
    RemoveOutEdge = 9,
}

impl DeltaAction {
    pub fn can_be_non_sequential(self) -> bool {
        matches!(self, Self::RemoveInEdge | Self::RemoveOutEdge)
    }
}

// ─── DeltaChainState ───────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum DeltaChainState {
    /// Normal MVCC delta: traversal stops at transaction boundaries.
    Sequential = 0,
    /// Can traverse past other transactions' uncommitted edge deltas.
    NonSequential = 1,
    /// Has blocking operations upstream, preventing non-sequential writes.
    ForcedSequential = 2,
}

// ─── CommitInfo ────────────────────────────────────────────────────────────

/// Shared commit-timestamp for a transaction's deltas.
/// Equivalent to C++ `CommitInfo` — ref-counted via Arc.
pub struct CommitInfo {
    timestamp: AtomicU64,
}

impl CommitInfo {
    pub fn new(ts: u64) -> Self {
        Self {
            timestamp: AtomicU64::new(ts),
        }
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp.load(Ordering::Acquire)
    }

    pub fn set_timestamp(&self, ts: u64) {
        self.timestamp.store(ts, Ordering::Release);
    }
}

// ─── TaggedPtr ─────────────────────────────────────────────────────────────

/// Tagged pointer encoding (Delta* | Vertex* | Edge* | null) in the low 2 bits
/// of a u64. All three types are ≥8-byte aligned, so low 3 bits are zero.
///
/// Replaces C++ `PreviousPtr`.
pub struct TaggedPtr {
    storage: AtomicU64,
}

const TAG_DELTA: u64 = 0b01;
const TAG_VERTEX: u64 = 0b10;
const TAG_EDGE: u64 = 0b11;
const TAG_MASK: u64 = 0b11;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TaggedPtrValue {
    Null,
    DeltaPtr(NonNull<crate::Delta>),
    VertexPtr(NonNull<crate::vertex::Vertex>),
    EdgePtr(NonNull<crate::edge::Edge>),
}

impl TaggedPtr {
    pub const fn new() -> Self {
        Self {
            storage: AtomicU64::new(0),
        }
    }

    fn encode<T>(ptr: *const T, tag: u64) -> u64 {
        let addr = ptr as u64;
        debug_assert!(addr & TAG_MASK == 0, "pointer must be ≥8-byte aligned");
        addr | tag
    }

    pub fn load(&self, order: Ordering) -> TaggedPtrValue {
        let raw = self.storage.load(order);
        Self::decode(raw)
    }

    fn decode(raw: u64) -> TaggedPtrValue {
        let tag = raw & TAG_MASK;
        let addr = raw & !TAG_MASK;
        match tag {
            TAG_DELTA => TaggedPtrValue::DeltaPtr(unsafe {
                NonNull::new_unchecked(addr as *mut crate::Delta)
            }),
            TAG_VERTEX => TaggedPtrValue::VertexPtr(unsafe {
                NonNull::new_unchecked(addr as *mut crate::vertex::Vertex)
            }),
            TAG_EDGE => TaggedPtrValue::EdgePtr(unsafe {
                NonNull::new_unchecked(addr as *mut crate::edge::Edge)
            }),
            _ => TaggedPtrValue::Null,
        }
    }

    pub fn store(&self, val: TaggedPtrValue, order: Ordering) {
        let raw = match val {
            TaggedPtrValue::Null => 0,
            TaggedPtrValue::DeltaPtr(p) => Self::encode(p.as_ptr(), TAG_DELTA),
            TaggedPtrValue::VertexPtr(p) => Self::encode(p.as_ptr(), TAG_VERTEX),
            TaggedPtrValue::EdgePtr(p) => Self::encode(p.as_ptr(), TAG_EDGE),
        };
        self.storage.store(raw, order);
    }
}

impl fmt::Debug for TaggedPtr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TaggedPtr({:?})", self.load(Ordering::Acquire))
    }
}

impl Default for TaggedPtr {
    fn default() -> Self {
        Self::new()
    }
}

// ─── TaggedVertexPtr ──────────────────────────────────────────────────────

/// Vertex pointer with 2 flag bits for DeltaChainState.
/// Replaces C++ `TaggedVertexPtr`.
pub struct TaggedVertexPtr {
    storage: AtomicU64,
}

const VERTEX_STATE_MASK: u64 = 0x03;

impl TaggedVertexPtr {
    pub fn new(vertex: *mut crate::vertex::Vertex, state: DeltaChainState) -> Self {
        let addr = vertex as u64;
        debug_assert!(
            addr & VERTEX_STATE_MASK == 0,
            "vertex must be 4-byte aligned"
        );
        Self {
            storage: AtomicU64::new(addr | (state as u64)),
        }
    }

    pub fn get(&self) -> *mut crate::vertex::Vertex {
        let raw = self.storage.load(Ordering::Acquire);
        (raw & !VERTEX_STATE_MASK) as *mut crate::vertex::Vertex
    }

    pub fn state(&self) -> DeltaChainState {
        let raw = self.storage.load(Ordering::Acquire);
        match raw & VERTEX_STATE_MASK {
            0 => DeltaChainState::Sequential,
            1 => DeltaChainState::NonSequential,
            _ => DeltaChainState::ForcedSequential,
        }
    }

    pub fn store(&self, vertex: *mut crate::vertex::Vertex, state: DeltaChainState) {
        let addr = vertex as u64;
        debug_assert!(addr & VERTEX_STATE_MASK == 0);
        self.storage.store(addr | (state as u64), Ordering::Release);
    }
}

impl fmt::Debug for TaggedVertexPtr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TaggedVertexPtr({:p}, {:?})", self.get(), self.state())
    }
}

// Safety: TaggedVertexPtr is essentially an AtomicU64.
unsafe impl Send for TaggedVertexPtr {}
unsafe impl Sync for TaggedVertexPtr {}

// ─── Delta ─────────────────────────────────────────────────────────────────

/// The core MVCC unit — a node in an undo-log chain.
///
/// Each Delta records one atomic change (label add, property set, edge
/// creation, …). Deltas are linked via `next` and `prev` to form per-object
/// version chains. The commit timestamp in `commit_info` determines visibility.
///
/// Memory model (same as C++):
/// - Deltas are allocated from a page-slab arena and never individually freed.
/// - `commit_info` is shared across all deltas of a transaction.
/// - `next` / `prev` use atomic access for lock-free traversal.
pub struct Delta {
    /// Shared commit info (ref-counted, owned by the creating transaction).
    pub commit_info: Arc<CommitInfo>,

    /// Ordering within a transaction (increasing).
    pub command_id: u64,

    /// Previous delta / vertex / edge in the chain (tagged pointer).
    pub prev: TaggedPtr,

    /// Next delta in the chain (atomic, lock-free traversal).
    pub next: AtomicPtr<Delta>,

    /// What this delta does, plus variant-specific payload.
    pub kind: DeltaKind,
}

/// Variant-specific data for a Delta.
///
/// Layout matches the C++ union inside Delta (≤56 bytes total for the Delta).
#[derive(Debug)]
pub enum DeltaKind {
    /// DELETE_DESERIALIZED_OBJECT — tombstone for disk-loaded objects.
    DeleteDeserializedObject {
        /// Optional old disk key (RocksDB key for compaction).
        old_disk_key: Option<String>,
        /// Timestamp from the deserialized key.
        ts: u64,
    },

    /// DELETE_OBJECT — logical deletion.
    DeleteObject,

    /// RECREATE_OBJECT — undo a deletion (used in GC/recovery).
    RecreateObject,

    /// SET_PROPERTY — old property value (None = property didn't exist).
    SetProperty {
        key: PropertyId,
        /// Old value that was replaced (for undo during MVCC reads).
        old_value: Option<PropertyValue>,
    },

    /// ADD_LABEL / REMOVE_LABEL — vertex label mutation.
    Label {
        action: DeltaAction, // AddLabel or RemoveLabel
        value: LabelId,
    },

    /// ADD_IN_EDGE / ADD_OUT_EDGE / REMOVE_IN_EDGE / REMOVE_OUT_EDGE
    VertexEdge {
        action: DeltaAction,
        edge_type: EdgeTypeId,
        vertex: TaggedVertexPtr,
        edge: EdgeRef,
    },
}

impl Delta {
    /// Create a DELETE_OBJECT delta (the most common initial delta).
    pub fn new_delete_object(commit_info: Arc<CommitInfo>, command_id: u64) -> Self {
        Self {
            commit_info,
            command_id,
            prev: TaggedPtr::new(),
            next: AtomicPtr::new(std::ptr::null_mut()),
            kind: DeltaKind::DeleteObject,
        }
    }

    pub fn new_add_label(label: LabelId, commit_info: Arc<CommitInfo>, command_id: u64) -> Self {
        Self {
            commit_info,
            command_id,
            prev: TaggedPtr::new(),
            next: AtomicPtr::new(std::ptr::null_mut()),
            kind: DeltaKind::Label {
                action: DeltaAction::AddLabel,
                value: label,
            },
        }
    }

    pub fn new_remove_label(label: LabelId, commit_info: Arc<CommitInfo>, command_id: u64) -> Self {
        Self {
            commit_info,
            command_id,
            prev: TaggedPtr::new(),
            next: AtomicPtr::new(std::ptr::null_mut()),
            kind: DeltaKind::Label {
                action: DeltaAction::RemoveLabel,
                value: label,
            },
        }
    }

    pub fn new_set_property(
        key: PropertyId,
        old_value: Option<PropertyValue>,
        commit_info: Arc<CommitInfo>,
        command_id: u64,
    ) -> Self {
        Self {
            commit_info,
            command_id,
            prev: TaggedPtr::new(),
            next: AtomicPtr::new(std::ptr::null_mut()),
            kind: DeltaKind::SetProperty { key, old_value },
        }
    }

    pub fn action(&self) -> DeltaAction {
        match &self.kind {
            DeltaKind::DeleteDeserializedObject { .. } => DeltaAction::DeleteDeserializedObject,
            DeltaKind::DeleteObject => DeltaAction::DeleteObject,
            DeltaKind::RecreateObject => DeltaAction::RecreateObject,
            DeltaKind::SetProperty { .. } => DeltaAction::SetProperty,
            DeltaKind::Label { action, .. } => *action,
            DeltaKind::VertexEdge { action, .. } => *action,
        }
    }

    /// Whether this delta is non-sequential (relaxes MVCC ordering for edge ops).
    pub fn is_non_sequential(&self) -> bool {
        if !self.action().can_be_non_sequential() {
            return false;
        }
        if let DeltaKind::VertexEdge { vertex, .. } = &self.kind {
            vertex.state() == DeltaChainState::NonSequential
        } else {
            false
        }
    }
}

impl fmt::Debug for Delta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delta")
            .field("action", &self.action())
            .field("command_id", &self.command_id)
            .field("commit_ts", &self.commit_info.timestamp())
            .field("is_non_seq", &self.is_non_sequential())
            .field("kind", &self.kind)
            .finish()
    }
}

// Safety: Delta is only accessed under proper synchronization (vertex/edge
// locks, GC epoch, or atomic next/prev). Raw pointers inside DeltaKind never
// outlive the owning page-slab arena.
unsafe impl Send for Delta {}
unsafe impl Sync for Delta {}

// ─── MVCC traversal ────────────────────────────────────────────────────────

/// Isolation levels for transaction visibility.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IsolationLevel {
    SnapshotIsolation,
    ReadCommitted,
    ReadUncommitted,
}

/// View mode: NEW (don't see own latest changes) or OLD (see all).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum View {
    New,
    Old,
}

/// kTransactionInitialId from C++ — any timestamp below this is a real commit.
pub const TRANSACTION_INITIAL_ID: u64 = 1u64 << 62;

/// Walk the delta chain and call `cb` for each visible delta.
///
/// Equivalent to C++ `ApplyDeltasForRead`.
/// Returns the number of deltas processed.
pub fn apply_deltas_for_read<F>(
    delta: *const Delta,
    view: View,
    isolation_level: IsolationLevel,
    start_timestamp: u64,
    commit_timestamp: u64,
    command_id: u64,
    mut cb: F,
) -> usize
where
    F: FnMut(&Delta),
{
    if delta.is_null() || isolation_level == IsolationLevel::ReadUncommitted {
        return 0;
    }

    let mut n_processed = 0;
    let mut current = delta;

    // Safety: the delta chain is kept alive by GC (committed transactions
    // only become unlinkable once no active transaction can reference them).
    unsafe {
        while let Some(d) = current.as_ref() {
            let ts = d.commit_info.timestamp();

            // C++: deltas with ts < start_timestamp are already in base state → skip
            let should_skip = match isolation_level {
                IsolationLevel::SnapshotIsolation => ts < start_timestamp,
                IsolationLevel::ReadCommitted => ts < TRANSACTION_INITIAL_ID,
                IsolationLevel::ReadUncommitted => false,
            };

            if should_skip {
                if d.is_non_sequential() {
                    current = d.next.load(Ordering::Acquire);
                    continue;
                } else {
                    break;
                }
            }

            // Skip own changes: View::OLD skips all own deltas, but must continue
            // processing older deltas from other transactions that may have committed
            // after our snapshot point.
            // C++: if (view == View::OLD && ts == commit_timestamp &&
            //      (cid < transaction->command_id || ...))
            if view == View::Old && ts == commit_timestamp && d.command_id < command_id {
                current = d.next.load(Ordering::Acquire);
                continue;
            }

            // Skip own NEW changes: newer own deltas are not yet visible, but must
            // continue processing older deltas from other transactions.
            if view == View::New && ts == commit_timestamp && d.command_id <= command_id {
                current = d.next.load(Ordering::Acquire);
                continue;
            }

            cb(d);
            n_processed += 1;

            current = d.next.load(Ordering::Acquire);
        }
    }

    n_processed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tagged_ptr_roundtrip() {
        let d = Box::new(Delta::new_delete_object(Arc::new(CommitInfo::new(42)), 0));
        let ptr = TaggedPtr::new();
        ptr.store(
            TaggedPtrValue::DeltaPtr(NonNull::from(Box::leak(d))),
            Ordering::Release,
        );
        let loaded = ptr.load(Ordering::Acquire);
        match loaded {
            TaggedPtrValue::DeltaPtr(p) => unsafe {
                assert_eq!((*p.as_ptr()).commit_info.timestamp(), 42);
                let _ = Box::from_raw(p.as_ptr());
            },
            _ => panic!("expected DeltaPtr"),
        }
    }

    #[test]
    fn test_apply_deltas_snapshot_isolation() {
        // Deltas with ts >= start_timestamp are processed (undo).
        // Deltas with ts < start_timestamp are already in base state → skipped.
        // Use ts = 300 > start_timestamp(200) so deltas are processed.
        let ci = Arc::new(CommitInfo::new(300));

        // d2 (newer, command_id=1) → d1 (older, command_id=0)
        let d2 = Box::new(Delta::new_add_label(LabelId::from(2u32), ci.clone(), 1));
        let d1 = Box::new(Delta::new_add_label(LabelId::from(1u32), ci.clone(), 0));

        d2.next
            .store(Box::into_raw(d1) as *mut Delta, Ordering::Release);

        let mut labels = Vec::new();
        apply_deltas_for_read(
            &*d2,
            View::Old,
            IsolationLevel::SnapshotIsolation,
            200, // start_timestamp < commit_ts(300) → process these deltas
            200,
            0,
            |delta| {
                if let DeltaKind::Label { value, .. } = &delta.kind {
                    labels.push(*value);
                }
            },
        );

        // Traversal is newest→oldest: d2 (label=2) first, then d1 (label=1)
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0], LabelId::from(2u32));
        assert_eq!(labels[1], LabelId::from(1u32));

        unsafe {
            let _ = Box::from_raw(d2.next.load(Ordering::Acquire));
        }
    }
}
