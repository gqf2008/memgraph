use std::collections::HashMap;
use std::sync::RwLock;

/// Bidirectional string ↔ integer interning for label and property names.
///
/// Equivalent to C++ `NameIdMapper`.
/// Thread-safe: all read operations acquire a shared lock; writes acquire exclusive.
#[derive(Debug)]
pub struct NameIdMapper<T> {
    id_to_name: RwLock<HashMap<T, String>>,
    name_to_id: RwLock<HashMap<String, T>>,
}

impl<T> NameIdMapper<T>
where
    T: Copy + Eq + std::hash::Hash + Default,
{
    pub fn new() -> Self {
        Self {
            id_to_name: RwLock::new(HashMap::new()),
            name_to_id: RwLock::new(HashMap::new()),
        }
    }

    /// Look up a name by id. Returns empty string if not found.
    pub fn name_by_id(&self, id: T) -> String {
        let map = self.id_to_name.read().unwrap();
        map.get(&id).cloned().unwrap_or_default()
    }

    /// Look up an id by name. Returns None if not found.
    pub fn id_by_name(&self, name: &str) -> Option<T> {
        let map = self.name_to_id.read().unwrap();
        map.get(name).copied()
    }

    /// Insert a mapping, returning the id. If the name already exists, returns existing id.
    /// If the id already exists for the name, returns the existing id.
    pub fn insert(&self, id: T, name: &str) -> T {
        // Fast path: check if name already mapped
        {
            let map = self.name_to_id.read().unwrap();
            if let Some(&existing) = map.get(name) {
                return existing;
            }
        }
        // Slow path: insert
        {
            let mut n2i = self.name_to_id.write().unwrap();
            let mut i2n = self.id_to_name.write().unwrap();
            n2i.insert(name.to_string(), id);
            i2n.insert(id, name.to_string());
        }
        id
    }

    /// Clear all mappings.
    pub fn clear(&self) {
        self.name_to_id.write().unwrap().clear();
        self.id_to_name.write().unwrap().clear();
    }

    pub fn len(&self) -> usize {
        self.name_to_id.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.name_to_id.read().unwrap().is_empty()
    }

    /// Return all registered names.
    pub fn all_names(&self) -> Vec<String> {
        self.name_to_id.read().unwrap().keys().cloned().collect()
    }

    /// Export all name→ID pairs for persistence.
    pub fn dump_pairs(&self) -> Vec<(String, T)> {
        let map = self.name_to_id.read().unwrap();
        map.iter().map(|(name, &id)| (name.clone(), id)).collect()
    }

    /// Import name→ID pairs from persisted state.
    pub fn load_pairs(&self, pairs: &[(String, T)]) {
        let mut n2i = self.name_to_id.write().unwrap();
        let mut i2n = self.id_to_name.write().unwrap();
        for (name, id) in pairs {
            n2i.insert(name.clone(), *id);
            i2n.insert(*id, name.clone());
        }
    }
}

impl<T> Default for NameIdMapper<T>
where
    T: Copy + Eq + std::hash::Hash + Default,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Specialized type aliases matching C++ usage.
pub type LabelMapper = NameIdMapper<crate::types::LabelId>;
pub type PropertyMapper = NameIdMapper<crate::types::PropertyId>;
pub type EdgeTypeMapper = NameIdMapper<crate::types::EdgeTypeId>;
