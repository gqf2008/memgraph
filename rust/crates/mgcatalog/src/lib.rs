#![allow(unused_mut)]
//! # mgcatalog — Schema catalog for name→ID resolution
//!
//! Maps label/property/edge type names to their integer IDs.
//! Thread-safe, used by parser and interpreter.
//!
//! Extended with schema versioning, constraint/index metadata,
//! trigger registry, and statistics tracking.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use mgcore::name_id_mapper::NameIdMapper;
use mgcore::types::{EdgeTypeId, LabelId, PropertyId};

static CATALOG_INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Schema catalog for resolving string names to internal IDs.
pub struct Catalog {
    labels: RwLock<NameIdMapper<LabelId>>,
    properties: RwLock<NameIdMapper<PropertyId>>,
    edge_types: RwLock<NameIdMapper<EdgeTypeId>>,
    next_label_id: RwLock<u32>,
    next_property_id: RwLock<u32>,
    next_edge_type_id: RwLock<u32>,

    // ─── Extended schema metadata ────────────────────────────────────────
    /// Unique instance ID for cache keying (never reused).
    instance_id: u64,
    /// Schema version for migration tracking.
    schema_version: RwLock<u64>,
    /// Constraint definitions.
    constraints: RwLock<Vec<ConstraintDef>>,
    /// Index definitions.
    indices: RwLock<Vec<IndexDef>>,
    /// Trigger definitions.
    triggers: RwLock<Vec<TriggerDef>>,
    /// Registered procedure signatures.
    procedures: RwLock<HashMap<String, ProcedureSignature>>,
    /// Label → set of property IDs commonly used (schema inference).
    label_properties: RwLock<HashMap<LabelId, HashSet<PropertyId>>>,
    /// Label → approximate entity count (for query planning).
    label_stats: RwLock<HashMap<LabelId, u64>>,
    /// Edge type → approximate edge count.
    edge_type_stats: RwLock<HashMap<EdgeTypeId, u64>>,
}

/// Constraint definition.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConstraintDef {
    pub name: String,
    pub kind: ConstraintKind,
    pub label: LabelId,
    pub property: Option<PropertyId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ConstraintKind {
    Unique,
    Existence,
    Type,
}

/// Index definition.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub kind: IndexKind,
    pub label: LabelId,
    pub property: Option<PropertyId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexKind {
    Label,
    LabelProperty,
    Text,
    Vector,
}

/// Trigger definition.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TriggerDef {
    pub name: String,
    pub event: TriggerEvent,
    pub query: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerEvent {
    Create,
    Delete,
    Update,
}

/// Procedure signature for introspection.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProcedureSignature {
    pub name: String,
    pub args: Vec<(String, String)>,
    pub return_fields: Vec<(String, String)>,
    pub is_write_procedure: bool,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            labels: RwLock::new(NameIdMapper::new()),
            properties: RwLock::new(NameIdMapper::new()),
            edge_types: RwLock::new(NameIdMapper::new()),
            next_label_id: RwLock::new(1),
            next_property_id: RwLock::new(1),
            next_edge_type_id: RwLock::new(1),
            instance_id: CATALOG_INSTANCE_COUNTER.fetch_add(1, Ordering::SeqCst),
            schema_version: RwLock::new(1),
            constraints: RwLock::new(Vec::new()),
            indices: RwLock::new(Vec::new()),
            triggers: RwLock::new(Vec::new()),
            procedures: RwLock::new(HashMap::new()),
            label_properties: RwLock::new(HashMap::new()),
            label_stats: RwLock::new(HashMap::new()),
            edge_type_stats: RwLock::new(HashMap::new()),
        }
    }

    /// Return the unique instance ID for this catalog.
    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    // ─── Basic ID resolution ─────────────────────────────────────────────

    /// Resolve or create a label ID from a name.
    pub fn label(&self, name: &str) -> LabelId {
        let mapper = self.labels.read().unwrap();
        if let Some(id) = mapper.id_by_name(name) {
            return id;
        }
        drop(mapper);
        let mapper = self.labels.write().unwrap();
        let id = self.next_label();
        mapper.insert(id, name)
    }

    /// Resolve or create a property ID from a name.
    pub fn property(&self, name: &str) -> PropertyId {
        let mapper = self.properties.read().unwrap();
        if let Some(id) = mapper.id_by_name(name) {
            return id;
        }
        drop(mapper);
        let mapper = self.properties.write().unwrap();
        let id = self.next_property();
        mapper.insert(id, name)
    }

    /// Resolve or create an edge type ID from a name.
    pub fn edge_type(&self, name: &str) -> EdgeTypeId {
        let mapper = self.edge_types.read().unwrap();
        if let Some(id) = mapper.id_by_name(name) {
            return id;
        }
        drop(mapper);
        let mapper = self.edge_types.write().unwrap();
        let id = self.next_edge_type();
        mapper.insert(id, name)
    }

    /// Look up a label name by ID.
    pub fn label_name(&self, id: LabelId) -> String {
        self.labels.read().unwrap().name_by_id(id)
    }

    /// Look up a property name by ID.
    pub fn property_name(&self, id: PropertyId) -> String {
        self.properties.read().unwrap().name_by_id(id)
    }

    /// Look up an edge type name by ID.
    pub fn edge_type_name(&self, id: EdgeTypeId) -> String {
        self.edge_types.read().unwrap().name_by_id(id)
    }

    /// Dump all name→ID mappings for persistence.
    pub fn dump_mappings(&self) -> (Vec<(String, LabelId)>, Vec<(String, PropertyId)>, Vec<(String, EdgeTypeId)>) {
        (
            self.labels.read().unwrap().dump_pairs(),
            self.properties.read().unwrap().dump_pairs(),
            self.edge_types.read().unwrap().dump_pairs(),
        )
    }

    /// Restore name→ID mappings from persisted data.
    pub fn load_mappings(&self, labels: &[(String, LabelId)], properties: &[(String, PropertyId)], edge_types: &[(String, EdgeTypeId)]) {
        self.labels.write().unwrap().load_pairs(labels);
        self.properties.write().unwrap().load_pairs(properties);
        self.edge_types.write().unwrap().load_pairs(edge_types);
    }

    /// Check if a label name exists.
    pub fn has_label(&self, name: &str) -> bool {
        self.labels.read().unwrap().id_by_name(name).is_some()
    }

    /// Check if a property name exists.
    pub fn has_property(&self, name: &str) -> bool {
        self.properties.read().unwrap().id_by_name(name).is_some()
    }

    /// Check if an edge type name exists.
    pub fn has_edge_type(&self, name: &str) -> bool {
        self.edge_types.read().unwrap().id_by_name(name).is_some()
    }

    /// Get the number of registered labels.
    pub fn label_count(&self) -> usize {
        self.labels.read().unwrap().len()
    }

    /// Get the number of registered properties.
    pub fn property_count(&self) -> usize {
        self.properties.read().unwrap().len()
    }

    /// Get the number of registered edge types.
    pub fn edge_type_count(&self) -> usize {
        self.edge_types.read().unwrap().len()
    }

    /// Get all label names.
    pub fn label_names(&self) -> Vec<String> {
        self.labels.read().unwrap().all_names()
    }

    /// Get all property names.
    pub fn property_names(&self) -> Vec<String> {
        self.properties.read().unwrap().all_names()
    }

    /// Get all edge type names.
    pub fn edge_type_names(&self) -> Vec<String> {
        self.edge_types.read().unwrap().all_names()
    }

    // ─── Schema versioning ───────────────────────────────────────────────

    pub fn schema_version(&self) -> u64 {
        *self.schema_version.read().unwrap()
    }

    pub fn bump_schema_version(&self) {
        let mut v = self.schema_version.write().unwrap();
        *v += 1;
    }

    // ─── Constraints ─────────────────────────────────────────────────────

    pub fn add_constraint(&self, def: ConstraintDef) {
        self.constraints.write().unwrap().push(def);
        self.bump_schema_version();
    }

    pub fn drop_constraint(&self, name: &str) {
        let mut c = self.constraints.write().unwrap();
        c.retain(|d| d.name != name);
        self.bump_schema_version();
    }

    pub fn list_constraints(&self) -> Vec<ConstraintDef> {
        self.constraints.read().unwrap().clone()
    }

    pub fn constraints_for_label(&self, label: LabelId) -> Vec<ConstraintDef> {
        self.constraints.read().unwrap()
            .iter()
            .filter(|c| c.label == label)
            .cloned()
            .collect()
    }

    // ─── Indices ─────────────────────────────────────────────────────────

    pub fn add_index(&self, def: IndexDef) {
        self.indices.write().unwrap().push(def);
        self.bump_schema_version();
    }

    pub fn drop_index(&self, name: &str) {
        let mut i = self.indices.write().unwrap();
        i.retain(|d| d.name != name);
        self.bump_schema_version();
    }

    pub fn list_indices(&self) -> Vec<IndexDef> {
        self.indices.read().unwrap().clone()
    }

    pub fn indices_for_label(&self, label: LabelId) -> Vec<IndexDef> {
        self.indices.read().unwrap()
            .iter()
            .filter(|i| i.label == label)
            .cloned()
            .collect()
    }

    // ─── Triggers ────────────────────────────────────────────────────────

    pub fn add_trigger(&self, def: TriggerDef) {
        self.triggers.write().unwrap().push(def);
        self.bump_schema_version();
    }

    pub fn drop_trigger(&self, name: &str) {
        let mut t = self.triggers.write().unwrap();
        t.retain(|d| d.name != name);
        self.bump_schema_version();
    }

    pub fn list_triggers(&self) -> Vec<TriggerDef> {
        self.triggers.read().unwrap().clone()
    }

    pub fn triggers_for_event(&self, event: TriggerEvent) -> Vec<TriggerDef> {
        self.triggers.read().unwrap()
            .iter()
            .filter(|t| t.event == event)
            .cloned()
            .collect()
    }

    // ─── Procedures ──────────────────────────────────────────────────────

    pub fn register_procedure(&self, sig: ProcedureSignature) {
        self.procedures.write().unwrap().insert(sig.name.clone(), sig);
    }

    pub fn get_procedure(&self, name: &str) -> Option<ProcedureSignature> {
        self.procedures.read().unwrap().get(name).cloned()
    }

    pub fn list_procedures(&self) -> Vec<ProcedureSignature> {
        self.procedures.read().unwrap().values().cloned().collect()
    }

    pub fn drop_procedure(&self, name: &str) {
        self.procedures.write().unwrap().remove(name);
    }

    // ─── Schema inference ────────────────────────────────────────────────

    /// Record that a property is used on entities with a given label.
    pub fn add_label_property(&self, label: LabelId, property: PropertyId) {
        let mut map = self.label_properties.write().unwrap();
        map.entry(label).or_default().insert(property);
    }

    /// Get all known properties for a label.
    pub fn properties_for_label(&self, label: LabelId) -> Vec<PropertyId> {
        let map = self.label_properties.read().unwrap();
        map.get(&label).map(|s| s.iter().copied().collect()).unwrap_or_default()
    }

    // ─── Statistics ──────────────────────────────────────────────────────

    pub fn set_label_stat(&self, label: LabelId, count: u64) {
        self.label_stats.write().unwrap().insert(label, count);
    }

    pub fn label_stat(&self, label: LabelId) -> u64 {
        self.label_stats.read().unwrap().get(&label).copied().unwrap_or(0)
    }

    pub fn set_edge_type_stat(&self, etype: EdgeTypeId, count: u64) {
        self.edge_type_stats.write().unwrap().insert(etype, count);
    }

    pub fn edge_type_stat(&self, etype: EdgeTypeId) -> u64 {
        self.edge_type_stats.read().unwrap().get(&etype).copied().unwrap_or(0)
    }

    pub fn all_label_stats(&self) -> HashMap<LabelId, u64> {
        self.label_stats.read().unwrap().clone()
    }

    /// Dump full schema state for persistence.
    pub fn dump_schema_state(&self) -> SchemaState {
        let (labels, properties, edge_types) = self.dump_mappings();
        SchemaState {
            version: self.schema_version(),
            labels,
            properties,
            edge_types,
            constraints: self.list_constraints(),
            indices: self.list_indices(),
            triggers: self.list_triggers(),
            procedures: self.list_procedures(),
            label_stats: self.all_label_stats(),
        }
    }

    /// Load full schema state from persisted data.
    pub fn load_schema_state(&self, state: &SchemaState) {
        self.load_mappings(&state.labels, &state.properties, &state.edge_types);
        *self.schema_version.write().unwrap() = state.version;
        *self.constraints.write().unwrap() = state.constraints.clone();
        *self.indices.write().unwrap() = state.indices.clone();
        *self.triggers.write().unwrap() = state.triggers.clone();
        *self.procedures.write().unwrap() = state.procedures.iter()
            .map(|p| (p.name.clone(), p.clone()))
            .collect();
        *self.label_stats.write().unwrap() = state.label_stats.clone();
    }

    fn next_label(&self) -> LabelId {
        let mut n = self.next_label_id.write().unwrap();
        let id = LabelId::from_uint(*n);
        *n += 1;
        id
    }

    fn next_property(&self) -> PropertyId {
        let mut n = self.next_property_id.write().unwrap();
        let id = PropertyId::from_uint(*n);
        *n += 1;
        id
    }

    fn next_edge_type(&self) -> EdgeTypeId {
        let mut n = self.next_edge_type_id.write().unwrap();
        let id = EdgeTypeId::from_uint(*n);
        *n += 1;
        id
    }
}

/// Full schema state for persistence.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SchemaState {
    pub version: u64,
    pub labels: Vec<(String, LabelId)>,
    pub properties: Vec<(String, PropertyId)>,
    pub edge_types: Vec<(String, EdgeTypeId)>,
    pub constraints: Vec<ConstraintDef>,
    pub indices: Vec<IndexDef>,
    pub triggers: Vec<TriggerDef>,
    pub procedures: Vec<ProcedureSignature>,
    pub label_stats: HashMap<LabelId, u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_label() {
        let cat = Catalog::new();
        let id1 = cat.label("Person");
        let id2 = cat.label("Person");
        assert_eq!(id1, id2);
        let id3 = cat.label("Company");
        assert_ne!(id1, id3);
        assert_eq!(cat.label_name(id1), "Person");
    }

    #[test]
    fn test_resolve_property() {
        let cat = Catalog::new();
        let id = cat.property("name");
        assert_eq!(cat.property_name(id), "name");
    }

    #[test]
    fn test_resolve_edge_type() {
        let cat = Catalog::new();
        let id = cat.edge_type("KNOWS");
        assert_eq!(cat.edge_type_name(id), "KNOWS");
    }

    #[test]
    fn test_has_methods() {
        let cat = Catalog::new();
        assert!(!cat.has_label("Person"));
        cat.label("Person");
        assert!(cat.has_label("Person"));
        assert!(!cat.has_label("Company"));
    }

    #[test]
    fn test_counts() {
        let cat = Catalog::new();
        assert_eq!(cat.label_count(), 0);
        assert_eq!(cat.property_count(), 0);
        assert_eq!(cat.edge_type_count(), 0);

        cat.label("A");
        cat.label("B");
        cat.property("x");
        cat.edge_type("R");

        assert_eq!(cat.label_count(), 2);
        assert_eq!(cat.property_count(), 1);
        assert_eq!(cat.edge_type_count(), 1);
    }

    #[test]
    fn test_all_names() {
        let cat = Catalog::new();
        cat.label("Person");
        cat.label("Company");
        let names = cat.label_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"Person".to_string()));
        assert!(names.contains(&"Company".to_string()));
    }

    #[test]
    fn test_dump_and_load_mappings() {
        let cat1 = Catalog::new();
        let lid = cat1.label("Person");
        let pid = cat1.property("name");
        let eid = cat1.edge_type("KNOWS");

        let (labels, props, edges) = cat1.dump_mappings();

        let cat2 = Catalog::new();
        cat2.load_mappings(&labels, &props, &edges);

        assert!(cat2.has_label("Person"));
        assert!(cat2.has_property("name"));
        assert!(cat2.has_edge_type("KNOWS"));
        assert_eq!(cat2.label_name(lid), "Person");
        assert_eq!(cat2.property_name(pid), "name");
        assert_eq!(cat2.edge_type_name(eid), "KNOWS");
    }

    #[test]
    fn test_constraints() {
        let cat = Catalog::new();
        let label = cat.label("Person");
        let prop = cat.property("email");

        cat.add_constraint(ConstraintDef {
            name: "unique_email".into(),
            kind: ConstraintKind::Unique,
            label,
            property: Some(prop),
        });

        assert_eq!(cat.list_constraints().len(), 1);
        assert_eq!(cat.constraints_for_label(label).len(), 1);

        cat.drop_constraint("unique_email");
        assert!(cat.list_constraints().is_empty());
    }

    #[test]
    fn test_indices() {
        let cat = Catalog::new();
        let label = cat.label("Person");
        let prop = cat.property("name");

        cat.add_index(IndexDef {
            name: "idx_person_name".into(),
            kind: IndexKind::LabelProperty,
            label,
            property: Some(prop),
        });

        assert_eq!(cat.list_indices().len(), 1);
        assert_eq!(cat.indices_for_label(label).len(), 1);

        cat.drop_index("idx_person_name");
        assert!(cat.list_indices().is_empty());
    }

    #[test]
    fn test_triggers() {
        let cat = Catalog::new();
        cat.add_trigger(TriggerDef {
            name: "audit_create".into(),
            event: TriggerEvent::Create,
            query: "CREATE (a:Audit {time: timestamp()})".into(),
        });

        assert_eq!(cat.list_triggers().len(), 1);
        assert_eq!(cat.triggers_for_event(TriggerEvent::Create).len(), 1);
        assert_eq!(cat.triggers_for_event(TriggerEvent::Delete).len(), 0);

        cat.drop_trigger("audit_create");
        assert!(cat.list_triggers().is_empty());
    }

    #[test]
    fn test_procedures() {
        let cat = Catalog::new();
        let sig = ProcedureSignature {
            name: "example.proc".into(),
            args: vec![("input".into(), "string".into())],
            return_fields: vec![("output".into(), "int".into())],
            is_write_procedure: false,
        };
        cat.register_procedure(sig.clone());

        assert_eq!(cat.list_procedures().len(), 1);
        assert!(cat.get_procedure("example.proc").is_some());
        assert!(cat.get_procedure("missing").is_none());

        cat.drop_procedure("example.proc");
        assert!(cat.list_procedures().is_empty());
    }

    #[test]
    fn test_schema_inference() {
        let cat = Catalog::new();
        let label = cat.label("Person");
        let name_prop = cat.property("name");
        let age_prop = cat.property("age");

        cat.add_label_property(label, name_prop);
        cat.add_label_property(label, age_prop);

        let props = cat.properties_for_label(label);
        assert_eq!(props.len(), 2);
        assert!(props.contains(&name_prop));
        assert!(props.contains(&age_prop));
    }

    #[test]
    fn test_statistics() {
        let cat = Catalog::new();
        let label = cat.label("Person");
        cat.set_label_stat(label, 1000);
        assert_eq!(cat.label_stat(label), 1000);
        assert_eq!(cat.label_stat(cat.label("Missing")), 0);
    }

    #[test]
    fn test_schema_version_bump() {
        let cat = Catalog::new();
        assert_eq!(cat.schema_version(), 1);
        cat.bump_schema_version();
        assert_eq!(cat.schema_version(), 2);
    }

    #[test]
    fn test_schema_state_roundtrip() {
        let cat = Catalog::new();
        let lid = cat.label("Person");
        let pid = cat.property("name");
        cat.set_label_stat(lid, 42);
        cat.add_constraint(ConstraintDef {
            name: "c1".into(),
            kind: ConstraintKind::Unique,
            label: lid,
            property: Some(pid),
        });

        let state = cat.dump_schema_state();
        let cat2 = Catalog::new();
        cat2.load_schema_state(&state);

        assert_eq!(cat2.schema_version(), cat.schema_version());
        assert_eq!(cat2.label_stat(lid), 42);
        assert_eq!(cat2.list_constraints().len(), 1);
        assert_eq!(cat2.label_name(lid), "Person");
    }
}
