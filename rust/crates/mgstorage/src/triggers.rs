//! Trigger system for executing callbacks on graph mutations.
//!
//! Equivalent to C++ trigger support in `src/query/trigger_context.hpp`.
//! Triggers fire BEFORE or AFTER vertex/edge creation, update, or deletion.

use std::collections::HashMap;
use std::sync::RwLock;

use mgcore::property_value::PropertyValue;
use mgcore::types::{Gid, LabelId, PropertyId};

/// When a trigger fires relative to the mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerTiming {
    Before,
    After,
}

/// What event causes the trigger to fire.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TriggerEvent {
    VertexCreate,
    VertexDelete,
    VertexUpdate,
    EdgeCreate,
    EdgeDelete,
    EdgeUpdate,
}

/// A trigger definition.
#[derive(Clone, Debug)]
pub struct Trigger {
    pub name: String,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    /// If Some, only fire for this label (vertex label or edge type).
    pub label_filter: Option<LabelId>,
    pub statement: String,
}

/// Trigger execution context passed to the trigger body.
#[derive(Clone, Debug)]
pub struct TriggerContext {
    pub event: TriggerEvent,
    pub gid: Gid,
    pub label: Option<LabelId>,
    pub changed_properties: Vec<(PropertyId, Option<PropertyValue>, Option<PropertyValue>)>,
}

/// Registry of active triggers.
pub struct TriggerRegistry {
    triggers: RwLock<HashMap<String, Trigger>>,
}

impl TriggerRegistry {
    pub fn new() -> Self {
        Self {
            triggers: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new trigger.
    pub fn create(&self, trigger: Trigger) -> Result<(), String> {
        let mut guard = self.triggers.write().unwrap();
        if guard.contains_key(&trigger.name) {
            return Err(format!("trigger '{}' already exists", trigger.name));
        }
        guard.insert(trigger.name.clone(), trigger);
        Ok(())
    }

    /// Remove a trigger by name.
    pub fn drop(&self, name: &str) -> bool {
        self.triggers.write().unwrap().remove(name).is_some()
    }

    /// List all registered triggers.
    pub fn list(&self) -> Vec<Trigger> {
        self.triggers.read().unwrap().values().cloned().collect()
    }

    /// Find triggers matching a specific event and optional label.
    pub fn matching(&self, event: TriggerEvent, label: Option<LabelId>) -> Vec<Trigger> {
        self.triggers
            .read()
            .unwrap()
            .values()
            .filter(|t| t.event == event)
            .filter(|t| match (t.label_filter, label) {
                (Some(f), Some(l)) => f == l,
                (Some(_), None) => false,
                _ => true,
            })
            .cloned()
            .collect()
    }

    /// Check if a trigger with the given name exists.
    pub fn has(&self, name: &str) -> bool {
        self.triggers.read().unwrap().contains_key(name)
    }
}

/// Trait for executing trigger statements.
pub trait TriggerExecutor: Send + Sync {
    fn execute(&self, ctx: &TriggerContext, statement: &str) -> Result<(), String>;
}

/// A no-op executor for testing.
pub struct NoopTriggerExecutor;

impl TriggerExecutor for NoopTriggerExecutor {
    fn execute(&self, _ctx: &TriggerContext, _statement: &str) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_list() {
        let reg = TriggerRegistry::new();
        let t = Trigger {
            name: "on_create".into(),
            timing: TriggerTiming::After,
            event: TriggerEvent::VertexCreate,
            label_filter: None,
            statement: "RETURN 1".into(),
        };
        reg.create(t.clone()).unwrap();
        assert_eq!(reg.list().len(), 1);
        assert!(reg.has("on_create"));
    }

    #[test]
    fn test_duplicate_name_fails() {
        let reg = TriggerRegistry::new();
        let t = Trigger {
            name: "dup".into(),
            timing: TriggerTiming::Before,
            event: TriggerEvent::EdgeCreate,
            label_filter: None,
            statement: "".into(),
        };
        reg.create(t.clone()).unwrap();
        assert!(reg.create(t).is_err());
    }

    #[test]
    fn test_drop() {
        let reg = TriggerRegistry::new();
        let t = Trigger {
            name: "tmp".into(),
            timing: TriggerTiming::After,
            event: TriggerEvent::VertexDelete,
            label_filter: None,
            statement: "".into(),
        };
        reg.create(t).unwrap();
        assert!(reg.drop("tmp"));
        assert!(!reg.has("tmp"));
    }

    #[test]
    fn test_matching_by_event() {
        let reg = TriggerRegistry::new();
        reg.create(Trigger {
            name: "v_create".into(),
            timing: TriggerTiming::After,
            event: TriggerEvent::VertexCreate,
            label_filter: None,
            statement: "".into(),
        }).unwrap();
        reg.create(Trigger {
            name: "v_delete".into(),
            timing: TriggerTiming::After,
            event: TriggerEvent::VertexDelete,
            label_filter: None,
            statement: "".into(),
        }).unwrap();

        let matches = reg.matching(TriggerEvent::VertexCreate, None);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].name, "v_create");
    }

    #[test]
    fn test_matching_by_label() {
        let label = LabelId::from(5u32);
        let reg = TriggerRegistry::new();
        reg.create(Trigger {
            name: "person_create".into(),
            timing: TriggerTiming::After,
            event: TriggerEvent::VertexCreate,
            label_filter: Some(label),
            statement: "".into(),
        }).unwrap();
        reg.create(Trigger {
            name: "any_create".into(),
            timing: TriggerTiming::After,
            event: TriggerEvent::VertexCreate,
            label_filter: None,
            statement: "".into(),
        }).unwrap();

        let matches = reg.matching(TriggerEvent::VertexCreate, Some(label));
        assert_eq!(matches.len(), 2); // both match

        let other_label = LabelId::from(99u32);
        let matches_other = reg.matching(TriggerEvent::VertexCreate, Some(other_label));
        assert_eq!(matches_other.len(), 1); // only the unfiltered one
    }

    #[test]
    fn test_noop_executor() {
        let ctx = TriggerContext {
            event: TriggerEvent::VertexCreate,
            gid: Gid::from(1u64),
            label: None,
            changed_properties: vec![],
        };
        let exec = NoopTriggerExecutor;
        assert!(exec.execute(&ctx, "RETURN 1").is_ok());
    }
}
