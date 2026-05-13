//! Database accessor — the query engine's read/write interface to storage.
//!
//! Equivalent to C++ `DbAccessor` in `src/query/db_accessor.hpp`.

use std::sync::Arc;

use mgcore::delta::IsolationLevel;
use mgcore::property_store::PropertyStore;
use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};

use crate::storage::{EdgeSnapshot, Storage, StorageError, VertexSnapshot};
use crate::transaction::Transaction;

/// High-level database accessor. Wraps a storage reference and transaction,
/// providing the API the query engine calls for CRUD operations.
pub struct DbAccessor<'a> {
    storage: &'a Storage,
    tx: Arc<Transaction>,
}

impl<'a> DbAccessor<'a> {
    pub fn new(storage: &'a Storage, tx: Arc<Transaction>) -> Self {
        Self { storage, tx }
    }

    /// Begin a new transaction at the snapshot isolation level.
    pub fn begin(storage: &'a Storage) -> Self {
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        Self::new(storage, tx)
    }

    // ─── Vertex CRUD ──────────────────────────────────────────────────

    pub fn create_vertex(&self, gid: Gid) -> Result<Gid, StorageError> {
        self.storage.create_vertex(&self.tx, gid)
    }

    pub fn get_vertex(&self, gid: Gid) -> Option<VertexSnapshot> {
        self.storage.get_vertex(gid, &self.tx)
    }

    pub fn vertex_add_label(&self, gid: Gid, label: LabelId) -> Result<(), StorageError> {
        self.storage.vertex_add_label(&self.tx, gid, label)
    }

    pub fn vertex_set_property(
        &self,
        gid: Gid,
        key: PropertyId,
        value: PropertyValue,
    ) -> Result<(), StorageError> {
        self.storage.vertex_set_property(&self.tx, gid, key, value)
    }

    pub fn vertices_by_label(&self, label: LabelId) -> Vec<Gid> {
        self.storage.vertices_by_label(label)
    }

    // ─── Edge CRUD ────────────────────────────────────────────────────

    pub fn create_edge(
        &self,
        gid: Gid,
        from_vertex: Gid,
        to_vertex: Gid,
        edge_type: EdgeTypeId,
    ) -> Result<Gid, StorageError> {
        self.storage
            .create_edge(&self.tx, gid, from_vertex, to_vertex, edge_type)
    }

    pub fn get_edge(&self, gid: Gid) -> Option<EdgeSnapshot> {
        self.storage.get_edge(gid, &self.tx)
    }

    // ─── Edge iteration ────────────────────────────────────────────────

    pub fn vertex_in_degree(&self, gid: Gid) -> usize {
        self.storage.vertex_in_degree(gid)
    }

    pub fn vertex_out_degree(&self, gid: Gid) -> usize {
        self.storage.vertex_out_degree(gid)
    }

    pub fn vertex_in_edges(
        &self,
        gid: Gid,
        edge_type: Option<EdgeTypeId>,
    ) -> Vec<(Gid, Gid, EdgeTypeId)> {
        self.storage.vertex_in_edges(gid, edge_type)
    }

    pub fn vertex_out_edges(
        &self,
        gid: Gid,
        edge_type: Option<EdgeTypeId>,
    ) -> Vec<(Gid, Gid, EdgeTypeId)> {
        self.storage.vertex_out_edges(gid, edge_type)
    }

    pub fn vertices_by_degree(&self, min_degree: usize) -> Vec<Gid> {
        self.storage.vertices_by_degree(min_degree)
    }

    pub fn all_vertices(&self) -> Vec<(Gid, Vec<LabelId>, PropertyStore)> {
        self.storage.all_vertices()
    }

    pub fn all_edges(&self) -> Vec<(Gid, Gid, Gid, EdgeTypeId, PropertyStore)> {
        self.storage.all_edges()
    }

    pub fn has_vertex(&self, gid: Gid) -> bool {
        self.storage.has_vertex(gid)
    }
    pub fn has_edge(&self, gid: Gid) -> bool {
        self.storage.has_edge(gid)
    }
    pub fn edge_from(&self, gid: Gid) -> Option<Gid> {
        self.storage.edge_from(gid)
    }
    pub fn edge_to(&self, gid: Gid) -> Option<Gid> {
        self.storage.edge_to(gid)
    }
    pub fn vertex_count(&self) -> usize {
        self.storage.vertex_count()
    }
    pub fn edge_count(&self) -> usize {
        self.storage.edge_count()
    }
    pub fn label_count(&self, label: LabelId) -> usize {
        self.storage.label_count(label)
    }

    pub fn vertex_remove_label(&self, gid: Gid, label: LabelId) -> Result<(), StorageError> {
        self.storage.vertex_remove_label(&self.tx, gid, label)
    }

    pub fn vertex_remove_property(&self, gid: Gid, key: PropertyId) -> Result<(), StorageError> {
        self.storage.vertex_remove_property(&self.tx, gid, key)
    }

    pub fn delete_vertex(&self, gid: Gid) -> Result<(), StorageError> {
        self.storage.delete_vertex(&self.tx, gid)
    }

    pub fn delete_vertex_and_edges(&self, gid: Gid) -> Result<(usize, usize), StorageError> {
        self.storage.delete_vertex_and_edges(&self.tx, gid)
    }

    pub fn edge_set_property(
        &self,
        gid: Gid,
        key: PropertyId,
        value: PropertyValue,
    ) -> Result<(), StorageError> {
        self.storage.edge_set_property(&self.tx, gid, key, value)
    }

    pub fn edge_remove_property(&self, gid: Gid, key: PropertyId) -> Result<(), StorageError> {
        self.storage.edge_remove_property(&self.tx, gid, key)
    }

    pub fn delete_edge(&self, gid: Gid) -> Result<(), StorageError> {
        self.storage.delete_edge(&self.tx, gid)
    }

    pub fn edges_by_property(&self, prop: PropertyId) -> Vec<(Gid, PropertyValue)> {
        self.storage.edges_by_property(prop)
    }

    pub fn edges_by_property_value(&self, prop: PropertyId, value: &PropertyValue) -> Vec<Gid> {
        self.storage.edges_by_property_value(prop, value)
    }

    pub fn edges_by_type(&self, etype: EdgeTypeId) -> Vec<Gid> {
        self.storage.edges_by_type(etype)
    }

    pub fn edge_type_count(&self, etype: EdgeTypeId) -> usize {
        self.storage.edge_type_count(etype)
    }

    pub fn edges_by_type_property(
        &self,
        edge_type: EdgeTypeId,
        prop: PropertyId,
    ) -> Vec<(Gid, PropertyValue)> {
        self.storage.edges_by_type_property(edge_type, prop)
    }

    pub fn edges_by_type_property_value(
        &self,
        edge_type: EdgeTypeId,
        prop: PropertyId,
        value: &PropertyValue,
    ) -> Vec<Gid> {
        self.storage
            .edges_by_type_property_value(edge_type, prop, value)
    }

    pub fn find_edge(&self, edge_gid: Gid, from_vertex_gid: Gid) -> Option<EdgeSnapshot> {
        self.storage.find_edge(edge_gid, from_vertex_gid)
    }

    pub fn chunked_vertices(
        &self,
        num_chunks: usize,
    ) -> Vec<Vec<(Gid, Vec<LabelId>, PropertyStore)>> {
        self.storage.chunked_vertices(num_chunks)
    }

    pub fn chunked_vertices_by_label(
        &self,
        label: LabelId,
        num_chunks: usize,
    ) -> Vec<Vec<(Gid, Vec<LabelId>, PropertyStore)>> {
        self.storage.chunked_vertices_by_label(label, num_chunks)
    }

    pub fn chunked_edges(
        &self,
        num_chunks: usize,
    ) -> Vec<Vec<(Gid, Gid, Gid, EdgeTypeId, PropertyStore)>> {
        self.storage.chunked_edges(num_chunks)
    }

    pub fn chunked_edges_by_type(
        &self,
        edge_type: EdgeTypeId,
        num_chunks: usize,
    ) -> Vec<Vec<(Gid, Gid, Gid, EdgeTypeId, PropertyStore)>> {
        self.storage.chunked_edges_by_type(edge_type, num_chunks)
    }

    pub fn vertex_incident_edge_count(&self, gid: Gid) -> usize {
        self.storage.vertex_incident_edge_count(gid)
    }

    pub fn vertices_by_label_property(
        &self,
        label: LabelId,
        property: PropertyId,
        value: &PropertyValue,
    ) -> Vec<Gid> {
        self.storage
            .vertices_by_label_property(label, property, value)
    }

    // ─── Point index access ───────────────────────────────────────────

    pub fn point_within_bbox_2d(
        &self,
        label: LabelId,
        prop: PropertyId,
        lower_left: mgcore::point::Point2D,
        upper_right: mgcore::point::Point2D,
    ) -> Vec<Gid> {
        self.storage
            .point_index
            .within_bbox_2d(label, prop, lower_left, upper_right)
    }

    pub fn point_nearest_2d(
        &self,
        label: LabelId,
        prop: PropertyId,
        query: mgcore::point::Point2D,
        k: usize,
    ) -> Vec<(Gid, f64)> {
        self.storage.point_index.nearest_2d(label, prop, query, k)
    }

    // ─── Index metadata ───────────────────────────────────────────────

    pub fn has_label_index(&self, label: LabelId) -> bool {
        self.storage.has_label_index(label)
    }

    pub fn has_label_property_index(&self, label: LabelId, property: PropertyId) -> bool {
        self.storage.has_label_property_index(label, property)
    }

    pub fn create_label_index(&self, label: LabelId) -> bool {
        self.storage.create_label_index(label)
    }

    pub fn create_label_property_index(&self, label: LabelId, property: PropertyId) -> (bool, u64) {
        let created = self.storage.create_label_property_index(label, property);
        if created {
            let count = self.storage.build_label_property_index(label, property);
            (true, count)
        } else {
            (false, 0)
        }
    }

    pub fn create_edge_property_index(&self, property: PropertyId) -> bool {
        // Global edge property index: always active after first use
        self.storage.build_edge_property_index(property);
        true
    }

    // ─── TTL management ───────────────────────────────────────────────

    pub fn set_ttl(&self, label: LabelId, ttl_ms: u64) {
        self.storage.set_ttl(label, ttl_ms);
    }

    pub fn remove_ttl(&self, label: LabelId) {
        self.storage.remove_ttl(label);
    }

    pub fn get_ttl(&self, label: LabelId) -> Option<u64> {
        self.storage.get_ttl(label)
    }

    pub fn ttl_cleanup(&self) -> usize {
        self.storage.ttl_cleanup()
    }

    // ─── GC ───────────────────────────────────────────────────────────

    pub fn gc(&self) -> usize {
        self.storage.gc()
    }

    // ─── Schema info ──────────────────────────────────────────────────

    pub fn schema_has_label(&self, label: LabelId) -> bool {
        self.storage.schema_info.has_label(label)
    }

    pub fn schema_has_edge_type(&self, etype: EdgeTypeId) -> bool {
        self.storage.schema_info.has_edge_type(etype)
    }

    pub fn schema_label_properties(&self, label: LabelId) -> std::collections::HashSet<PropertyId> {
        self.storage.schema_info.label_properties(label)
    }

    pub fn schema_all_labels(&self) -> std::collections::HashSet<LabelId> {
        self.storage.schema_info.all_labels()
    }

    pub fn schema_all_edge_types(&self) -> std::collections::HashSet<EdgeTypeId> {
        self.storage.schema_info.all_edge_types()
    }

    // ─── Clear ────────────────────────────────────────────────────────

    pub fn clear(&self) {
        self.storage.clear();
    }

    // ─── Transaction control ──────────────────────────────────────────

    pub fn commit(self) -> bool {
        self.storage.commit_transaction(&self.tx)
    }

    pub fn abort(self) -> bool {
        self.storage.abort_transaction(&self.tx)
    }

    pub fn transaction(&self) -> &Transaction {
        &self.tx
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::types::LabelId;

    #[test]
    fn test_accessor_create_vertex() {
        let storage = Storage::new();
        let accessor = DbAccessor::begin(&storage);

        let gid = Gid::from(1u64);
        accessor.create_vertex(gid).unwrap();

        let snap = accessor.get_vertex(gid).unwrap();
        assert_eq!(snap.gid, gid);
    }

    #[test]
    fn test_accessor_add_label_and_query() {
        let storage = Storage::new();
        let accessor = DbAccessor::begin(&storage);

        let gid = Gid::from(1u64);
        let label = LabelId::from(5u32);

        accessor.create_vertex(gid).unwrap();
        accessor.vertex_add_label(gid, label).unwrap();

        let by_label = accessor.vertices_by_label(label);
        assert_eq!(by_label, vec![gid]);
    }

    #[test]
    fn test_accessor_commit_and_read() {
        let storage = Storage::new();

        // Create and commit
        let accessor1 = DbAccessor::begin(&storage);
        let gid = Gid::from(1u64);
        accessor1.create_vertex(gid).unwrap();
        assert!(accessor1.commit());

        // New transaction should see committed data
        let accessor2 = DbAccessor::begin(&storage);
        let snap = accessor2.get_vertex(gid).unwrap();
        assert_eq!(snap.gid, gid);
    }
}
