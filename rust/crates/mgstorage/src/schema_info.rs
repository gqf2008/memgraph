//! Schema info tracking for vertex/edge property types.
//! Matches C++ `storage/v2/schema_info.cpp`.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use mgcore::types::{EdgeTypeId, LabelId, PropertyId};

/// Tracks which property types exist per label or edge type.
#[derive(Debug)]
pub struct SchemaInfo {
    /// Per-label property type sets.
    vertex_props: RwLock<HashMap<LabelId, HashSet<PropertyId>>>,
    /// Per-edge-type property type sets.
    edge_props: RwLock<HashMap<EdgeTypeId, HashSet<PropertyId>>>,
    /// Label names present in the database.
    stored_labels: RwLock<HashSet<LabelId>>,
    /// Edge type names present.
    stored_edge_types: RwLock<HashSet<EdgeTypeId>>,
}

impl SchemaInfo {
    pub fn new() -> Self {
        Self {
            vertex_props: RwLock::new(HashMap::new()),
            edge_props: RwLock::new(HashMap::new()),
            stored_labels: RwLock::new(HashSet::new()),
            stored_edge_types: RwLock::new(HashSet::new()),
        }
    }

    pub fn record_label(&self, label: LabelId) {
        self.stored_labels.write().unwrap().insert(label);
    }

    pub fn record_edge_type(&self, etype: EdgeTypeId) {
        self.stored_edge_types.write().unwrap().insert(etype);
    }

    pub fn record_vertex_property(&self, label: LabelId, prop: PropertyId) {
        self.vertex_props
            .write()
            .unwrap()
            .entry(label)
            .or_default()
            .insert(prop);
    }

    pub fn record_edge_property(&self, etype: EdgeTypeId, prop: PropertyId) {
        self.edge_props
            .write()
            .unwrap()
            .entry(etype)
            .or_default()
            .insert(prop);
    }

    pub fn remove_vertex_property(&self, label: LabelId, prop: PropertyId) {
        if let Ok(mut map) = self.vertex_props.write() {
            if let Some(props) = map.get_mut(&label) {
                props.remove(&prop);
            }
        }
    }

    pub fn has_label(&self, label: LabelId) -> bool {
        self.stored_labels.read().unwrap().contains(&label)
    }

    pub fn has_edge_type(&self, etype: EdgeTypeId) -> bool {
        self.stored_edge_types.read().unwrap().contains(&etype)
    }

    pub fn label_properties(&self, label: LabelId) -> HashSet<PropertyId> {
        self.vertex_props
            .read()
            .unwrap()
            .get(&label)
            .cloned()
            .unwrap_or_default()
    }

    pub fn edge_type_properties(&self, etype: EdgeTypeId) -> HashSet<PropertyId> {
        self.edge_props
            .read()
            .unwrap()
            .get(&etype)
            .cloned()
            .unwrap_or_default()
    }

    pub fn all_labels(&self) -> HashSet<LabelId> {
        self.stored_labels.read().unwrap().clone()
    }

    pub fn all_edge_types(&self) -> HashSet<EdgeTypeId> {
        self.stored_edge_types.read().unwrap().clone()
    }

    pub fn all_property_keys(&self) -> HashSet<PropertyId> {
        let mut keys = HashSet::new();
        if let Ok(vprops) = self.vertex_props.read() {
            for props in vprops.values() {
                keys.extend(props.iter().copied());
            }
        }
        if let Ok(eprops) = self.edge_props.read() {
            for props in eprops.values() {
                keys.extend(props.iter().copied());
            }
        }
        keys
    }
}

impl Default for SchemaInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_label_and_property() {
        let si = SchemaInfo::new();
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(0u32);
        assert!(!si.has_label(label));
        si.record_label(label);
        assert!(si.has_label(label));
        si.record_vertex_property(label, prop);
        assert!(si.label_properties(label).contains(&prop));
    }

    #[test]
    fn test_edge_type_tracking() {
        let si = SchemaInfo::new();
        let etype = EdgeTypeId::from(1u32);
        assert!(!si.has_edge_type(etype));
        si.record_edge_type(etype);
        assert!(si.has_edge_type(etype));
    }
}
