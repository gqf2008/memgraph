//! Replication hooks for integrating storage operations with the replication stream.
//!
//! When a write operation occurs on the main instance, the storage engine
//! can emit replication events that are consumed by `mgrepl` and sent to replicas.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};

use crate::storage::WalRecord;

/// An event that should be replicated to follower instances.
#[derive(Clone, Debug, PartialEq)]
pub enum ReplicationEvent {
    VertexCreate { gid: Gid },
    VertexDelete { gid: Gid },
    VertexAddLabel { gid: Gid, label: LabelId },
    VertexRemoveLabel { gid: Gid, label: LabelId },
    VertexSetProperty { gid: Gid, key: PropertyId, value: PropertyValue },
    EdgeCreate { gid: Gid, from: Gid, to: Gid, edge_type: EdgeTypeId },
    EdgeDelete { gid: Gid },
    EdgeSetProperty { gid: Gid, key: PropertyId, value: PropertyValue },
    TransactionCommit { timestamp: u64 },
    IndexCreate { label: LabelId, property: Option<PropertyId> },
    IndexDrop { label: LabelId, property: Option<PropertyId> },
    ConstraintCreate { definition: String },
    ConstraintDrop { definition: String },
}

/// A hook that intercepts storage mutations and produces replication events.
pub trait ReplicationHook: Send + Sync {
    fn on_event(&self, event: ReplicationEvent);
    fn flush(&self) {}
}

/// In-memory buffer of replication events for testing and async handoff.
pub struct BufferedReplicationHook {
    buffer: Mutex<VecDeque<ReplicationEvent>>,
    max_size: usize,
    total_emitted: Mutex<u64>,
}

impl BufferedReplicationHook {
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::with_capacity(max_size)),
            max_size,
            total_emitted: Mutex::new(0),
        }
    }

    pub fn drain(&self, count: usize) -> Vec<ReplicationEvent> {
        let mut buf = self.buffer.lock().unwrap();
        let mut result = Vec::with_capacity(count.min(buf.len()));
        for _ in 0..count {
            if let Some(ev) = buf.pop_front() {
                result.push(ev);
            }
        }
        result
    }

    pub fn len(&self) -> usize {
        self.buffer.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.lock().unwrap().is_empty()
    }

    pub fn clear(&self) {
        self.buffer.lock().unwrap().clear();
    }

    pub fn total_emitted(&self) -> u64 {
        *self.total_emitted.lock().unwrap()
    }

    /// Convert a WalRecord into zero or more ReplicationEvents.
    pub fn wal_to_events(record: &WalRecord) -> Vec<ReplicationEvent> {
        match record {
            WalRecord::VertexCreate { gid, .. } => vec![ReplicationEvent::VertexCreate { gid: *gid }],
            WalRecord::VertexDelete { gid } => vec![ReplicationEvent::VertexDelete { gid: *gid }],
            WalRecord::VertexAddLabel { gid, label } => vec![ReplicationEvent::VertexAddLabel { gid: *gid, label: *label }],
            WalRecord::VertexRemoveLabel { gid, label } => vec![ReplicationEvent::VertexRemoveLabel { gid: *gid, label: *label }],
            WalRecord::VertexSetProperty { gid, key, value } => vec![ReplicationEvent::VertexSetProperty { gid: *gid, key: *key, value: value.clone() }],
            WalRecord::EdgeCreate { gid, from_vertex, to_vertex, edge_type, .. } => vec![ReplicationEvent::EdgeCreate { gid: *gid, from: *from_vertex, to: *to_vertex, edge_type: *edge_type }],
            WalRecord::EdgeDelete { gid } => vec![ReplicationEvent::EdgeDelete { gid: *gid }],
            WalRecord::EdgeSetProperty { gid, key, value } => vec![ReplicationEvent::EdgeSetProperty { gid: *gid, key: *key, value: value.clone() }],
            WalRecord::TransactionEnd { timestamp, .. } => vec![ReplicationEvent::TransactionCommit { timestamp: *timestamp }],
        }
    }
}

impl ReplicationHook for BufferedReplicationHook {
    fn on_event(&self, event: ReplicationEvent) {
        let mut buf = self.buffer.lock().unwrap();
        if buf.len() >= self.max_size {
            buf.pop_front();
        }
        buf.push_back(event);
        *self.total_emitted.lock().unwrap() += 1;
    }
}

/// Multi-casts replication events to multiple downstream hooks.
pub struct MulticastReplicationHook {
    hooks: Mutex<Vec<Arc<dyn ReplicationHook>>>,
}

impl MulticastReplicationHook {
    pub fn new() -> Self {
        Self { hooks: Mutex::new(Vec::new()) }
    }

    pub fn add_hook(&self, hook: Arc<dyn ReplicationHook>) {
        self.hooks.lock().unwrap().push(hook);
    }

    pub fn remove_hook(&self, hook: &Arc<dyn ReplicationHook>) {
        let ptr = Arc::as_ptr(hook);
        self.hooks.lock().unwrap().retain(|h| Arc::as_ptr(h) != ptr);
    }
}

impl ReplicationHook for MulticastReplicationHook {
    fn on_event(&self, event: ReplicationEvent) {
        for hook in self.hooks.lock().unwrap().iter() {
            hook.on_event(event.clone());
        }
    }

    fn flush(&self) {
        for hook in self.hooks.lock().unwrap().iter() {
            hook.flush();
        }
    }
}

impl Default for MulticastReplicationHook {
    fn default() -> Self { Self::new() }
}

/// Filter hook that only forwards events matching a predicate.
pub struct FilteredReplicationHook {
    predicate: Box<dyn Fn(&ReplicationEvent) -> bool + Send + Sync>,
    inner: Arc<dyn ReplicationHook>,
}

impl FilteredReplicationHook {
    pub fn new<P>(predicate: P, inner: Arc<dyn ReplicationHook>) -> Self
    where
        P: Fn(&ReplicationEvent) -> bool + Send + Sync + 'static,
    {
        Self {
            predicate: Box::new(predicate),
            inner,
        }
    }
}

impl ReplicationHook for FilteredReplicationHook {
    fn on_event(&self, event: ReplicationEvent) {
        if (self.predicate)(&event) {
            self.inner.on_event(event);
        }
    }

    fn flush(&self) {
        self.inner.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffered_hook() {
        let hook = BufferedReplicationHook::new(10);
        hook.on_event(ReplicationEvent::VertexCreate { gid: Gid::from(1u64) });
        hook.on_event(ReplicationEvent::VertexCreate { gid: Gid::from(2u64) });
        assert_eq!(hook.len(), 2);
        let drained = hook.drain(1);
        assert_eq!(drained.len(), 1);
        assert_eq!(hook.len(), 1);
        assert_eq!(hook.total_emitted(), 2);
    }

    #[test]
    fn test_multicast_hook() {
        let a = Arc::new(BufferedReplicationHook::new(10));
        let b = Arc::new(BufferedReplicationHook::new(10));
        let multi = MulticastReplicationHook::new();
        multi.add_hook(a.clone());
        multi.add_hook(b.clone());
        multi.on_event(ReplicationEvent::VertexCreate { gid: Gid::from(1u64) });
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn test_filtered_hook() {
        let inner = Arc::new(BufferedReplicationHook::new(10));
        let filtered = FilteredReplicationHook::new(
            |ev| matches!(ev, ReplicationEvent::VertexCreate { .. }),
            inner.clone(),
        );
        filtered.on_event(ReplicationEvent::VertexCreate { gid: Gid::from(1u64) });
        filtered.on_event(ReplicationEvent::EdgeDelete { gid: Gid::from(100u64) });
        assert_eq!(inner.len(), 1);
    }

    #[test]
    fn test_wal_to_events() {
        let wal = WalRecord::VertexCreate { gid: Gid::from(1u64), timestamp: 1 };
        let events = BufferedReplicationHook::wal_to_events(&wal);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ReplicationEvent::VertexCreate { gid } if gid == Gid::from(1u64)));
    }
}
