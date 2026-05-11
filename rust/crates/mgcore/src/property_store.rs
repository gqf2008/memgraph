use std::collections::HashMap;

use crate::property_value::PropertyValue;
use crate::types::PropertyId;

/// Arena-backed property storage for vertices and edges.
///
/// Properties are keyed by PropertyId and stored as PropertyValue.
/// For high performance, the backing store will eventually be a type-specialized
/// arena (matching C++ PropertyStore). Currently uses a Vec as the backing store;
/// this should be replaced with a slab-allocated store in Phase 1.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PropertyStore {
    /// Properties indexed by PropertyId. The vector grows to accommodate the
    /// maximum PropertyId inserted. Unset properties are PropertyValue::Null.
    storage: Vec<PropertyValue>,
}

impl PropertyStore {
    pub const fn new() -> Self {
        Self {
            storage: Vec::new(),
        }
    }

    /// Get property value. Returns Null if not set.
    pub fn get(&self, key: PropertyId) -> &PropertyValue {
        let idx = key.as_uint() as usize;
        if idx < self.storage.len() {
            &self.storage[idx]
        } else {
            // Return a static Null reference. Safe because PropertyValue::Null is Copy-like.
            &PROPERTY_NULL
        }
    }

    /// Set a property value. Grows the storage vector if needed.
    pub fn set(&mut self, key: PropertyId, value: PropertyValue) {
        let idx = key.as_uint() as usize;
        if idx >= self.storage.len() {
            self.storage.resize(idx + 1, PropertyValue::Null);
        }
        self.storage[idx] = value;
    }

    /// Remove a property (sets it to Null).
    pub fn remove(&mut self, key: PropertyId) {
        let idx = key.as_uint() as usize;
        if idx < self.storage.len() {
            self.storage[idx] = PropertyValue::Null;
        }
    }

    /// Check if a property is set (not Null).
    pub fn is_set(&self, key: PropertyId) -> bool {
        !self.get(key).is_null()
    }

    /// Iterate over all non-null properties.
    pub fn iter(&self) -> impl Iterator<Item = (PropertyId, &PropertyValue)> {
        self.storage
            .iter()
            .enumerate()
            .filter(|(_, v)| !v.is_null())
            .map(|(i, v)| (PropertyId::from_uint(i as u32), v))
    }

    /// Number of set properties.
    pub fn len(&self) -> usize {
        self.storage.iter().filter(|v| !v.is_null()).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a PropertyStore from a HashMap (for testing).
    pub fn from_map(map: HashMap<PropertyId, PropertyValue>) -> Self {
        let max_id = map.keys().map(|k| k.as_uint()).max().unwrap_or(0);
        let mut storage = vec![PropertyValue::Null; max_id as usize + 1];
        for (id, val) in map {
            storage[id.as_uint() as usize] = val;
        }
        Self { storage }
    }

    /// Remove trailing Null entries to shrink the backing storage.
    pub fn compact(&mut self) {
        let last_set = self
            .storage
            .iter()
            .rposition(|v| !v.is_null());
        match last_set {
            Some(idx) => self.storage.truncate(idx + 1),
            None => self.storage.clear(),
        }
        self.storage.shrink_to_fit();
    }

    /// Estimate the in-memory size of this property store in bytes.
    pub fn estimate_size(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let vec_cap = self.storage.capacity() * std::mem::size_of::<PropertyValue>();
        // Rough estimate of heap data inside PropertyValue variants.
        let heap_estimate: usize = self
            .storage
            .iter()
            .map(|v| match v {
                PropertyValue::String(s) => s.len(),
                PropertyValue::List(l) => l.len() * std::mem::size_of::<PropertyValue>(),
                PropertyValue::Map(m) => {
                    m.iter()
                        .map(|(k, v)| k.len() + std::mem::size_of::<PropertyValue>() + std::mem::size_of::<String>())
                        .sum()
                }
                _ => 0,
            })
            .sum();
        base + vec_cap + heap_estimate
    }
}

/// Static null value for out-of-bounds gets.
static PROPERTY_NULL: PropertyValue = PropertyValue::Null;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_store_basic() {
        let mut store = PropertyStore::new();
        let p0 = PropertyId::from_uint(0);
        let p1 = PropertyId::from_uint(1);

        assert!(store.get(p0).is_null());

        store.set(p0, PropertyValue::Int(42));
        assert!(!store.get(p0).is_null());
        assert_eq!(*store.get(p0), PropertyValue::Int(42));

        store.set(p1, PropertyValue::String("hello".into()));
        assert_eq!(store.len(), 2);

        store.remove(p0);
        assert!(store.get(p0).is_null());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_property_store_iter() {
        let mut store = PropertyStore::new();
        store.set(PropertyId::from_uint(0), PropertyValue::Bool(true));
        store.set(PropertyId::from_uint(2), PropertyValue::Double(3.14));

        let items: Vec<_> = store.iter().collect();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_property_store_growth() {
        let mut store = PropertyStore::new();
        // Set a property at a high index — should resize
        store.set(PropertyId::from_uint(100), PropertyValue::Int(-1));
        assert_eq!(store.get(PropertyId::from_uint(100)), &PropertyValue::Int(-1));
        assert!(store.get(PropertyId::from_uint(0)).is_null());
    }

    #[test]
    fn test_property_store_compact() {
        let mut store = PropertyStore::new();
        store.set(PropertyId::from_uint(0), PropertyValue::Int(1));
        store.set(PropertyId::from_uint(5), PropertyValue::Int(2));
        store.remove(PropertyId::from_uint(5));
        store.compact();
        assert_eq!(store.storage.len(), 1);
        assert_eq!(store.get(PropertyId::from_uint(0)), &PropertyValue::Int(1));
        assert!(store.get(PropertyId::from_uint(5)).is_null());
    }

    #[test]
    fn test_property_store_compact_all_null() {
        let mut store = PropertyStore::new();
        store.set(PropertyId::from_uint(2), PropertyValue::Null);
        store.compact();
        assert!(store.storage.is_empty());
    }

    #[test]
    fn test_property_store_estimate_size() {
        let mut store = PropertyStore::new();
        store.set(PropertyId::from_uint(0), PropertyValue::Int(42));
        store.set(PropertyId::from_uint(1), PropertyValue::String("hello".into()));
        let size = store.estimate_size();
        assert!(size > 0);
        // Should account for the string heap data
        assert!(size >= std::mem::size_of::<PropertyStore>() + store.storage.capacity() * std::mem::size_of::<PropertyValue>());
    }
}
