//! Constraints: unique, existence, and type constraints for graph data.

use std::collections::HashMap;
use std::sync::RwLock;

use mgcore::property_store::PropertyStore;
use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};

/// Error from constraint violation.
#[derive(Clone, Debug, PartialEq)]
pub enum ConstraintError {
    UniqueViolation {
        label: LabelId,
        properties: Vec<PropertyId>,
        existing_gid: Gid,
    },
    ExistenceViolation {
        label: LabelId,
        property: PropertyId,
    },
    TypeViolation {
        label: LabelId,
        property: PropertyId,
        expected: ConstraintType,
        got: String,
    },
    EdgeTypeViolation {
        edge_type: EdgeTypeId,
        property: PropertyId,
        expected: ConstraintType,
        got: String,
    },
}

/// Expected property type for type constraints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConstraintType {
    Bool,
    Int,
    Double,
    String,
    List,
    Map,
    Date,
    LocalTime,
    LocalDateTime,
    Duration,
    Point2D,
    Point3D,
    Enum,
}

impl ConstraintType {
    pub fn from_value(val: &PropertyValue) -> Self {
        match val {
            PropertyValue::Bool(_) => ConstraintType::Bool,
            PropertyValue::Int(_) => ConstraintType::Int,
            PropertyValue::Double(_) => ConstraintType::Double,
            PropertyValue::String(_) => ConstraintType::String,
            PropertyValue::List(_) => ConstraintType::List,
            PropertyValue::Map(_) => ConstraintType::Map,
            PropertyValue::Date(_) => ConstraintType::Date,
            PropertyValue::LocalTime(_) => ConstraintType::LocalTime,
            PropertyValue::LocalDateTime(_) => ConstraintType::LocalDateTime,
            PropertyValue::Duration(_) => ConstraintType::Duration,
            PropertyValue::Point2D(_) => ConstraintType::Point2D,
            PropertyValue::Point3D(_) => ConstraintType::Point3D,
            _ => ConstraintType::Enum,
        }
    }

    pub fn matches_value(&self, val: &PropertyValue) -> bool {
        if matches!(val, PropertyValue::Null) {
            return true;
        }
        *self == Self::from_value(val)
    }
}

struct UniqueConstraintEntry {
    label: LabelId,
    properties: Vec<PropertyId>,
    /// String-formatted property values → Gid for uniqueness checking.
    values: RwLock<HashMap<String, Gid>>,
}

struct ExistenceConstraintEntry {
    label: LabelId,
    property: PropertyId,
}

struct TypeConstraintEntry {
    label: LabelId,
    property: PropertyId,
    expected: ConstraintType,
}

struct EdgeTypeConstraintEntry {
    edge_type: EdgeTypeId,
    property: PropertyId,
    expected: ConstraintType,
}

/// All constraints enforced on the graph.
/// Serializable description of a constraint.
#[derive(Clone, Debug, PartialEq)]
pub struct ConstraintInfo {
    pub kind: ConstraintKind,
    pub label: LabelId,
    pub property: PropertyId,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ConstraintKind {
    Unique,
    Existence,
    Type,
}

pub struct Constraints {
    unique: RwLock<Vec<UniqueConstraintEntry>>,
    existence: RwLock<Vec<ExistenceConstraintEntry>>,
    type_constraints: RwLock<Vec<TypeConstraintEntry>>,
    edge_type_constraints: RwLock<Vec<EdgeTypeConstraintEntry>>,
}

fn format_unique_key(
    constraint: &UniqueConstraintEntry,
    property_values: &PropertyStore,
) -> Option<String> {
    let parts: Vec<String> = constraint
        .properties
        .iter()
        .map(|p| {
            let v = property_values.get(*p);
            if v.is_null() {
                None
            } else {
                Some(format!("{}", v))
            }
        })
        .collect::<Option<Vec<_>>>()?;
    Some(parts.join("|"))
}

impl Default for Constraints {
    fn default() -> Self {
        Self::new()
    }
}

impl Constraints {
    pub fn new() -> Self {
        Self {
            unique: RwLock::new(Vec::new()),
            existence: RwLock::new(Vec::new()),
            type_constraints: RwLock::new(Vec::new()),
            edge_type_constraints: RwLock::new(Vec::new()),
        }
    }

    // ─── Unique constraints ──────────────────────────────────────────────

    pub fn add_unique_constraint(&self, label: LabelId, properties: Vec<PropertyId>) {
        let mut guard = self.unique.write().unwrap();
        if !guard
            .iter()
            .any(|c| c.label == label && c.properties == properties)
        {
            guard.push(UniqueConstraintEntry {
                label,
                properties,
                values: RwLock::new(HashMap::new()),
            });
        }
    }

    pub fn remove_unique_constraint(&self, label: LabelId, properties: &[PropertyId]) {
        let mut guard = self.unique.write().unwrap();
        guard.retain(|c| !(c.label == label && c.properties == properties));
    }

    pub fn check_unique(
        &self,
        label: LabelId,
        property_values: &PropertyStore,
        gid: Gid,
    ) -> Result<(), ConstraintError> {
        let guard = self.unique.read().unwrap();
        for constraint in guard.iter() {
            if constraint.label != label {
                continue;
            }
            let key = match format_unique_key(constraint, property_values) {
                Some(k) => k,
                None => continue, // Skip if not all required properties are present
            };
            let values = constraint.values.read().unwrap();
            if let Some(&existing_gid) = values.get(&key) {
                if existing_gid != gid {
                    return Err(ConstraintError::UniqueViolation {
                        label: constraint.label,
                        properties: constraint.properties.clone(),
                        existing_gid,
                    });
                }
            }
        }
        Ok(())
    }

    /// Atomically check and record unique values under a single write lock.
    /// Returns `Ok(())` if the value is newly inserted or already owned by `gid`.
    /// Returns `Err` if another gid already owns the key.
    pub fn try_record_unique(
        &self,
        label: LabelId,
        property_values: &PropertyStore,
        gid: Gid,
    ) -> Result<(), ConstraintError> {
        let guard = self.unique.read().unwrap();
        for constraint in guard.iter() {
            if constraint.label != label {
                continue;
            }
            let key = match format_unique_key(constraint, property_values) {
                Some(k) => k,
                None => continue,
            };
            let mut values = constraint.values.write().unwrap();
            if let Some(&existing_gid) = values.get(&key) {
                if existing_gid != gid {
                    return Err(ConstraintError::UniqueViolation {
                        label: constraint.label,
                        properties: constraint.properties.clone(),
                        existing_gid,
                    });
                }
                // Same gid — already recorded, nothing to do.
            } else {
                values.insert(key, gid);
            }
        }
        Ok(())
    }

    pub fn record_unique_values(&self, label: LabelId, property_values: &PropertyStore, gid: Gid) {
        let guard = self.unique.read().unwrap();
        for constraint in guard.iter() {
            if constraint.label != label {
                continue;
            }
            let key = match format_unique_key(constraint, property_values) {
                Some(k) => k,
                None => continue, // Skip if not all required properties are present
            };
            constraint.values.write().unwrap().insert(key, gid);
        }
    }

    pub fn remove_unique_values(&self, label: LabelId, property_values: &PropertyStore) {
        let guard = self.unique.read().unwrap();
        for constraint in guard.iter() {
            if constraint.label != label {
                continue;
            }
            let key = match format_unique_key(constraint, property_values) {
                Some(k) => k,
                None => continue,
            };
            constraint.values.write().unwrap().remove(&key);
        }
    }

    // ─── Existence constraints ───────────────────────────────────────────

    pub fn add_existence_constraint(&self, label: LabelId, property: PropertyId) {
        let mut guard = self.existence.write().unwrap();
        if !guard
            .iter()
            .any(|c| c.label == label && c.property == property)
        {
            guard.push(ExistenceConstraintEntry { label, property });
        }
    }

    pub fn remove_existence_constraint(&self, label: LabelId, property: PropertyId) {
        let mut guard = self.existence.write().unwrap();
        guard.retain(|c| !(c.label == label && c.property == property));
    }

    pub fn check_existence(
        &self,
        label: LabelId,
        property_values: &PropertyStore,
    ) -> Result<(), ConstraintError> {
        let guard = self.existence.read().unwrap();
        for constraint in guard.iter() {
            if constraint.label != label {
                continue;
            }
            let val = property_values.get(constraint.property);
            if val.is_null() {
                return Err(ConstraintError::ExistenceViolation {
                    label: constraint.label,
                    property: constraint.property,
                });
            }
        }
        Ok(())
    }

    // ─── Type constraints ────────────────────────────────────────────────

    pub fn add_type_constraint(
        &self,
        label: LabelId,
        property: PropertyId,
        expected: ConstraintType,
    ) {
        let mut guard = self.type_constraints.write().unwrap();
        if !guard
            .iter()
            .any(|c| c.label == label && c.property == property)
        {
            guard.push(TypeConstraintEntry {
                label,
                property,
                expected,
            });
        }
    }

    pub fn remove_type_constraint(&self, label: LabelId, property: PropertyId) {
        let mut guard = self.type_constraints.write().unwrap();
        guard.retain(|c| !(c.label == label && c.property == property));
    }

    pub fn check_type(
        &self,
        label: LabelId,
        property: PropertyId,
        value: &PropertyValue,
    ) -> Result<(), ConstraintError> {
        let guard = self.type_constraints.read().unwrap();
        for constraint in guard.iter() {
            if constraint.label != label || constraint.property != property {
                continue;
            }
            if !constraint.expected.matches_value(value) {
                return Err(ConstraintError::TypeViolation {
                    label: constraint.label,
                    property: constraint.property,
                    expected: constraint.expected,
                    got: format!("{:?}", ConstraintType::from_value(value)),
                });
            }
        }
        Ok(())
    }

    // ─── Edge type constraints ──────────────────────────────────────────

    pub fn add_edge_type_constraint(
        &self,
        edge_type: EdgeTypeId,
        property: PropertyId,
        expected: ConstraintType,
    ) {
        let mut guard = self.edge_type_constraints.write().unwrap();
        if !guard
            .iter()
            .any(|c| c.edge_type == edge_type && c.property == property)
        {
            guard.push(EdgeTypeConstraintEntry {
                edge_type,
                property,
                expected,
            });
        }
    }

    pub fn remove_edge_type_constraint(&self, edge_type: EdgeTypeId, property: PropertyId) {
        let mut guard = self.edge_type_constraints.write().unwrap();
        guard.retain(|c| !(c.edge_type == edge_type && c.property == property));
    }

    pub fn check_edge_type(
        &self,
        edge_type: EdgeTypeId,
        property: PropertyId,
        value: &PropertyValue,
    ) -> Result<(), ConstraintError> {
        let guard = self.edge_type_constraints.read().unwrap();
        for constraint in guard.iter() {
            if constraint.edge_type != edge_type || constraint.property != property {
                continue;
            }
            if !constraint.expected.matches_value(value) {
                return Err(ConstraintError::EdgeTypeViolation {
                    edge_type: constraint.edge_type,
                    property: constraint.property,
                    expected: constraint.expected,
                    got: format!("{:?}", ConstraintType::from_value(value)),
                });
            }
        }
        Ok(())
    }

    pub fn has_edge_type_constraint(&self, edge_type: EdgeTypeId, property: PropertyId) -> bool {
        self.edge_type_constraints
            .read()
            .unwrap()
            .iter()
            .any(|c| c.edge_type == edge_type && c.property == property)
    }

    pub fn drop_edge_type_constraint(&self, edge_type: EdgeTypeId, property: PropertyId) {
        self.remove_edge_type_constraint(edge_type, property);
    }

    // ─── Query ───────────────────────────────────────────────────────────

    pub fn has_unique_constraint(&self, label: LabelId, properties: &[PropertyId]) -> bool {
        self.unique
            .read()
            .unwrap()
            .iter()
            .any(|c| c.label == label && c.properties == properties)
    }

    pub fn has_existence_constraint(&self, label: LabelId, property: PropertyId) -> bool {
        self.existence
            .read()
            .unwrap()
            .iter()
            .any(|c| c.label == label && c.property == property)
    }

    pub fn has_type_constraint(&self, label: LabelId, property: PropertyId) -> bool {
        self.type_constraints
            .read()
            .unwrap()
            .iter()
            .any(|c| c.label == label && c.property == property)
    }

    pub fn drop_unique_constraint(&self, label: LabelId, properties: &[PropertyId]) {
        self.unique
            .write()
            .unwrap()
            .retain(|c| !(c.label == label && c.properties == properties));
    }

    pub fn drop_existence_constraint(&self, label: LabelId, property: PropertyId) {
        self.existence
            .write()
            .unwrap()
            .retain(|c| !(c.label == label && c.property == property));
    }

    pub fn drop_type_constraint(&self, label: LabelId, property: PropertyId) {
        self.type_constraints
            .write()
            .unwrap()
            .retain(|c| !(c.label == label && c.property == property));
    }

    pub fn list(&self) -> Vec<ConstraintInfo> {
        let mut out = Vec::new();
        for c in self.unique.read().unwrap().iter() {
            out.push(ConstraintInfo {
                kind: ConstraintKind::Unique,
                label: c.label,
                property: c
                    .properties
                    .first()
                    .copied()
                    .unwrap_or(PropertyId::from(0u32)),
            });
        }
        for c in self.existence.read().unwrap().iter() {
            out.push(ConstraintInfo {
                kind: ConstraintKind::Existence,
                label: c.label,
                property: c.property,
            });
        }
        for c in self.type_constraints.read().unwrap().iter() {
            out.push(ConstraintInfo {
                kind: ConstraintKind::Type,
                label: c.label,
                property: c.property,
            });
        }
        out
    }
}

// ─── TTL ──────────────────────────────────────────────────────────────────

/// Time-to-live configuration per label.
pub struct TtlConfig {
    /// Map from label to TTL duration in milliseconds.
    ttls: RwLock<HashMap<LabelId, u64>>,
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl TtlConfig {
    pub fn new() -> Self {
        Self {
            ttls: RwLock::new(HashMap::new()),
        }
    }

    pub fn set_ttl(&self, label: LabelId, ttl_ms: u64) {
        self.ttls.write().unwrap().insert(label, ttl_ms);
    }

    pub fn remove_ttl(&self, label: LabelId) {
        self.ttls.write().unwrap().remove(&label);
    }

    pub fn get_ttl(&self, label: LabelId) -> Option<u64> {
        self.ttls.read().unwrap().get(&label).copied()
    }

    /// Check if a vertex with the given labels has expired based on TTL.
    pub fn is_expired(&self, labels: &[LabelId], creation_timestamp: u64, now_ms: u64) -> bool {
        let guard = self.ttls.read().unwrap();
        for label in labels {
            if let Some(ttl_ms) = guard.get(label) {
                if creation_timestamp + *ttl_ms <= now_ms {
                    return true;
                }
            }
        }
        false
    }

    /// True when no TTL rules have been configured.
    pub fn is_empty(&self) -> bool {
        self.ttls.read().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unique_constraint() {
        let constraints = Constraints::new();
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(0u32);

        constraints.add_unique_constraint(label, vec![prop]);

        let mut vals = PropertyStore::new();
        vals.set(prop, PropertyValue::Int(42));
        assert!(constraints
            .check_unique(label, &vals, Gid::from(1u64))
            .is_ok());
        constraints.record_unique_values(label, &vals, Gid::from(1u64));

        // Same value, different gid → violation
        assert!(constraints
            .check_unique(label, &vals, Gid::from(2u64))
            .is_err());

        // Different value → ok
        vals.set(prop, PropertyValue::Int(99));
        assert!(constraints
            .check_unique(label, &vals, Gid::from(2u64))
            .is_ok());
    }

    #[test]
    fn test_existence_constraint() {
        let constraints = Constraints::new();
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(0u32);

        constraints.add_existence_constraint(label, prop);

        let mut vals = PropertyStore::new();
        vals.set(prop, PropertyValue::Int(42));
        assert!(constraints.check_existence(label, &vals).is_ok());

        vals.set(prop, PropertyValue::Null);
        assert!(constraints.check_existence(label, &vals).is_err());

        let empty = PropertyStore::new();
        assert!(constraints.check_existence(label, &empty).is_err());
    }

    #[test]
    fn test_type_constraint() {
        let constraints = Constraints::new();
        let label = LabelId::from(1u32);
        let prop = PropertyId::from(0u32);

        constraints.add_type_constraint(label, prop, ConstraintType::Int);
        assert!(constraints
            .check_type(label, prop, &PropertyValue::Int(42))
            .is_ok());
        assert!(constraints
            .check_type(label, prop, &PropertyValue::String("hi".into()))
            .is_err());
        assert!(constraints
            .check_type(label, prop, &PropertyValue::Null)
            .is_ok());
    }

    #[test]
    fn test_ttl_config() {
        let ttl = TtlConfig::new();
        let label = LabelId::from(1u32);
        ttl.set_ttl(label, 1000);
        assert!(!ttl.is_expired(&[label], 100, 500));
        assert!(ttl.is_expired(&[label], 100, 1200));
    }

    #[test]
    fn test_edge_type_constraint() {
        let constraints = Constraints::new();
        let edge_type = EdgeTypeId::from(5u32);
        let prop = PropertyId::from(0u32);

        constraints.add_edge_type_constraint(edge_type, prop, ConstraintType::String);
        assert!(constraints
            .check_edge_type(edge_type, prop, &PropertyValue::String("hello".into()))
            .is_ok());
        assert!(constraints
            .check_edge_type(edge_type, prop, &PropertyValue::Int(42))
            .is_err());
        assert!(constraints
            .check_edge_type(edge_type, prop, &PropertyValue::Null)
            .is_ok());

        // Different edge type — not constrained
        let other_type = EdgeTypeId::from(6u32);
        assert!(constraints
            .check_edge_type(other_type, prop, &PropertyValue::Int(42))
            .is_ok());

        // Remove constraint
        constraints.remove_edge_type_constraint(edge_type, prop);
        assert!(constraints
            .check_edge_type(edge_type, prop, &PropertyValue::Int(42))
            .is_ok());
    }
}
