//! Lock-free database indices for efficient vertex/edge lookup.
//!
//! Equivalent to C++ `inmemory/label_index.hpp`, `inmemory/label_property_index.hpp`, etc.
//! Uses `crossbeam_skiplist::SkipMap` for lock-free concurrent access.

use crossbeam_skiplist::SkipMap;
use std::sync::atomic::AtomicUsize;

use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, EdgeTypePropKey, Gid, LabelId, LabelPropKey, PropertyId};

/// Label index: maps (LabelId, Gid) → () for all vertices with a given label.
///
/// Entries are ordered by LabelId then Gid, allowing efficient range scans.
pub struct LabelIndex {
    index: SkipMap<(LabelId, Gid), ()>,
}

impl Default for LabelIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl LabelIndex {
    pub fn new() -> Self {
        Self {
            index: SkipMap::new(),
        }
    }

    /// Add a vertex to the label index.
    pub fn add_vertex(&self, label: LabelId, gid: Gid) {
        self.index.insert((label, gid), ());
    }

    /// Remove a vertex from the label index.
    pub fn remove_vertex(&self, label: LabelId, gid: Gid) {
        self.index.remove(&(label, gid));
    }

    /// Get all vertices with a given label, sorted by Gid.
    pub fn vertices_by_label(&self, label: LabelId) -> Vec<Gid> {
        let start = (label, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == label)
            .map(|entry| entry.key().1)
            .collect()
    }

    /// Check if any vertex has the given label.
    pub fn has_label(&self, label: LabelId) -> bool {
        let start = (label, Gid::from_uint(0));
        self.index
            .range(start..)
            .next()
            .is_some_and(|entry| entry.key().0 == label)
    }

    /// Count vertices with a given label.
    pub fn vertex_count_by_label(&self, label: LabelId) -> u64 {
        let start = (label, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == label)
            .count() as u64
    }

    /// Total number of indexed (label, vertex) entries.
    pub fn total_entries(&self) -> u64 {
        self.index.len() as u64
    }

    /// Count of entries (approximate; O(n) because SkipMap has no `len`).
    pub fn entry_count(&self) -> usize {
        self.index.iter().count()
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.index.clear();
    }
}

/// Label-property index: maps (LabelPropKey, Gid) → PropertyValue.
///
/// Used for filtering by property value within a label. Ordered by key then Gid.
pub struct LabelPropertyIndex {
    index: SkipMap<(LabelPropKey, Gid), PropertyValue>,
}

impl Default for LabelPropertyIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl LabelPropertyIndex {
    pub fn new() -> Self {
        Self {
            index: SkipMap::new(),
        }
    }

    /// Index a vertex's property.
    pub fn add(&self, key: LabelPropKey, gid: Gid, value: PropertyValue) {
        self.index.insert((key, gid), value);
    }

    /// Remove a vertex's property from the index.
    pub fn remove(&self, key: LabelPropKey, gid: Gid) {
        self.index.remove(&(key, gid));
    }

    /// Get all (Gid, PropertyValue) pairs for a given key.
    pub fn get(&self, key: LabelPropKey) -> Vec<(Gid, PropertyValue)> {
        let start = (key, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == key)
            .map(|entry| (entry.key().1, entry.value().clone()))
            .collect()
    }

    /// Find vertices where the indexed property equals a specific value.
    pub fn find_by_value(&self, key: LabelPropKey, value: &PropertyValue) -> Vec<Gid> {
        let start = (key, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == key)
            .filter(|entry| entry.value() == value)
            .map(|entry| entry.key().1)
            .collect()
    }

    pub fn clear(&self) {
        self.index.clear();
    }

    /// Count vertices indexed for a given label-property key.
    pub fn vertex_count_by_label_property(&self, label: LabelId, prop: PropertyId) -> u64 {
        let key = LabelPropKey::new(label, prop);
        let start = (key, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == key)
            .count() as u64
    }
}

/// Edge type index: maps (EdgeTypeId, Gid) → () for all edges with a given type.
pub struct EdgeTypeIndex {
    index: SkipMap<(EdgeTypeId, Gid), ()>,
}

impl Default for EdgeTypeIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl EdgeTypeIndex {
    pub fn new() -> Self {
        Self {
            index: SkipMap::new(),
        }
    }

    pub fn add_edge(&self, edge_type: EdgeTypeId, gid: Gid) {
        self.index.insert((edge_type, gid), ());
    }

    pub fn remove_edge(&self, edge_type: EdgeTypeId, gid: Gid) {
        self.index.remove(&(edge_type, gid));
    }

    pub fn edges_by_type(&self, edge_type: EdgeTypeId) -> Vec<Gid> {
        let start = (edge_type, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == edge_type)
            .map(|entry| entry.key().1)
            .collect()
    }

    pub fn clear(&self) {
        self.index.clear();
    }
}

/// Edge type-property index: maps (EdgeTypePropKey, Gid) → PropertyValue.
pub struct EdgeTypePropertyIndex {
    index: SkipMap<(EdgeTypePropKey, Gid), PropertyValue>,
}

impl Default for EdgeTypePropertyIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl EdgeTypePropertyIndex {
    pub fn new() -> Self {
        Self {
            index: SkipMap::new(),
        }
    }

    pub fn add(&self, key: EdgeTypePropKey, gid: Gid, value: PropertyValue) {
        self.index.insert((key, gid), value);
    }

    pub fn remove(&self, key: EdgeTypePropKey, gid: Gid) {
        self.index.remove(&(key, gid));
    }

    pub fn get(&self, key: EdgeTypePropKey) -> Vec<(Gid, PropertyValue)> {
        let start = (key, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == key)
            .map(|entry| (entry.key().1, entry.value().clone()))
            .collect()
    }

    pub fn find_by_value(&self, key: EdgeTypePropKey, value: &PropertyValue) -> Vec<Gid> {
        let start = (key, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == key)
            .filter(|entry| entry.value() == value)
            .map(|entry| entry.key().1)
            .collect()
    }

    pub fn clear(&self) {
        self.index.clear();
    }
}

/// Global edge index: maps Gid → (from_vertex Gid, to_vertex Gid, edge_type).
/// Used for direct edge lookup by Gid.
pub struct EdgeIndex {
    index: SkipMap<Gid, EdgeIndexEntry>,
}

#[derive(Clone, Debug)]
pub struct EdgeIndexEntry {
    pub from_vertex: Gid,
    pub to_vertex: Gid,
    pub edge_type: EdgeTypeId,
}

impl Default for EdgeIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl EdgeIndex {
    pub fn new() -> Self {
        Self {
            index: SkipMap::new(),
        }
    }

    pub fn insert(&self, gid: Gid, entry: EdgeIndexEntry) {
        self.index.insert(gid, entry);
    }

    pub fn remove(&self, gid: &Gid) {
        self.index.remove(gid);
    }

    pub fn get(&self, gid: &Gid) -> Option<EdgeIndexEntry> {
        self.index.get(gid).map(|e| e.value().clone())
    }

    pub fn clear(&self) {
        self.index.clear();
    }
}

/// Edge property index: maps (PropertyId, Gid) → PropertyValue.
/// No edge_type prefix — used for global property-based edge queries.
pub struct EdgePropertyIndex {
    index: SkipMap<(PropertyId, Gid), PropertyValue>,
}

impl Default for EdgePropertyIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl EdgePropertyIndex {
    pub fn new() -> Self {
        Self {
            index: SkipMap::new(),
        }
    }

    pub fn add(&self, prop: PropertyId, gid: Gid, value: PropertyValue) {
        self.index.insert((prop, gid), value);
    }

    pub fn remove(&self, prop: PropertyId, gid: Gid) {
        self.index.remove(&(prop, gid));
    }

    pub fn get(&self, prop: PropertyId) -> Vec<(Gid, PropertyValue)> {
        let start = (prop, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == prop)
            .map(|entry| (entry.key().1, entry.value().clone()))
            .collect()
    }

    pub fn find_by_value(&self, prop: PropertyId, value: &PropertyValue) -> Vec<Gid> {
        let start = (prop, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == prop)
            .filter(|entry| entry.value() == value)
            .map(|entry| entry.key().1)
            .collect()
    }

    pub fn count(&self, prop: PropertyId) -> usize {
        let start = (prop, Gid::from_uint(0));
        self.index
            .range(start..)
            .take_while(|entry| entry.key().0 == prop)
            .count()
    }

    pub fn clear(&self) {
        self.index.clear();
    }
}

/// Thread-safe reference-counted index statistics.
pub struct IndexStats {
    pub lookups: AtomicUsize,
    pub hits: AtomicUsize,
}

impl Default for IndexStats {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexStats {
    pub fn new() -> Self {
        Self {
            lookups: AtomicUsize::new(0),
            hits: AtomicUsize::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_label_index_add_and_query() {
        let idx = LabelIndex::new();
        let l = LabelId::from(1u32);
        let g1 = Gid::from(10u64);
        let g2 = Gid::from(20u64);

        idx.add_vertex(l, g1);
        idx.add_vertex(l, g2);
        let result = idx.vertices_by_label(l);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&g1));
        assert!(result.contains(&g2));

        idx.remove_vertex(l, g1);
        let result = idx.vertices_by_label(l);
        assert_eq!(result, vec![g2]);

        idx.remove_vertex(l, g2);
        assert!(!idx.has_label(l));
    }

    #[test]
    fn test_label_property_index() {
        let idx = LabelPropertyIndex::new();
        let key = LabelPropKey::new(LabelId::from(1u32), PropertyId::from(2u32));
        let gid = Gid::from(42u64);

        idx.add(key, gid, PropertyValue::Int(100));
        idx.add(key, Gid::from(43u64), PropertyValue::Int(200));

        let results = idx.find_by_value(key, &PropertyValue::Int(100));
        assert_eq!(results, vec![gid]);
    }

    #[test]
    fn test_edge_type_index() {
        let idx = EdgeTypeIndex::new();
        let et = EdgeTypeId::from(5u32);
        let g1 = Gid::from(1u64);

        idx.add_edge(et, g1);
        assert_eq!(idx.edges_by_type(et), vec![g1]);

        idx.remove_edge(et, g1);
        assert!(idx.edges_by_type(et).is_empty());
    }

    #[test]
    fn test_edge_index() {
        let idx = EdgeIndex::new();
        let gid = Gid::from(99u64);
        let entry = EdgeIndexEntry {
            from_vertex: Gid::from(1u64),
            to_vertex: Gid::from(2u64),
            edge_type: EdgeTypeId::from(3u32),
        };

        idx.insert(gid, entry.clone());
        let found = idx.get(&gid).unwrap();
        assert_eq!(found.from_vertex, Gid::from(1u64));
        assert_eq!(found.to_vertex, Gid::from(2u64));

        idx.remove(&gid);
        assert!(idx.get(&gid).is_none());
    }

    #[test]
    fn test_concurrent_label_index() {
        let idx = std::sync::Arc::new(LabelIndex::new());
        let label = LabelId::from(1u32);
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let idx = idx.clone();
                thread::spawn(move || {
                    for i in 0..100 {
                        let gid = Gid::from((t * 100 + i) as u64);
                        idx.add_vertex(label, gid);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        let result = idx.vertices_by_label(label);
        assert_eq!(result.len(), 400);
    }

    #[test]
    fn test_concurrent_mixed_operations() {
        let idx = std::sync::Arc::new(LabelPropertyIndex::new());
        let key = LabelPropKey::new(LabelId::from(1u32), PropertyId::from(2u32));

        let threads: Vec<_> = (0..4)
            .map(|t| {
                let idx = idx.clone();
                thread::spawn(move || {
                    for i in 0..50 {
                        let gid = Gid::from((t * 50 + i) as u64);
                        idx.add(key, gid, PropertyValue::Int(i as i64));
                        if i % 2 == 0 {
                            idx.remove(key, gid);
                        }
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        // Only odd i values (per thread) should remain: 25 per thread = 100 total
        let all = idx.get(key);
        assert_eq!(all.len(), 100);
    }
}
