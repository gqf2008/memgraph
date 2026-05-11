//! Stream, transformation, trigger, text index, and vector index C API.
//!
//! These types extend `mg_procedure.h` with batch-read stream support,
//! Kafka/Pulsar transformation context, trigger introspection, and
//! text/vector search execution.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

use crate::*;

// ─── Stream types (batch read procedures) ─────────────────────────────────

/// Opaque stream handle for batch read procedures.
#[repr(C)]
pub struct mgp_stream {
    batches: Vec<*mut mgp_stream_batch>,
    pos: usize,
}

/// A batch of records pulled from a stream.
#[repr(C)]
pub struct mgp_stream_batch {
    records: Vec<*mut mgp_result_record>,
}

#[no_mangle]
pub unsafe extern "C" fn mgp_stream_make_empty(result: *mut *mut mgp_stream) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_stream { batches: Vec::new(), pos: 0 }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_stream_destroy(stream: *mut mgp_stream) {
    if stream.is_null() { return; }
    let s = Box::from_raw(stream);
    for batch in s.batches {
        if !batch.is_null() { mgp_stream_batch_destroy(batch); }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_stream_batch_make_empty(result: *mut *mut mgp_stream_batch) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_stream_batch { records: Vec::new() }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_stream_batch_size(batch: *const mgp_stream_batch, result: *mut usize) -> MgpError {
    if batch.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*batch).records.len();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_stream_batch_at(
    batch: *const mgp_stream_batch,
    index: usize,
    result: *mut *mut mgp_result_record,
) -> MgpError {
    if batch.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let records = &(*batch).records;
    if index >= records.len() { return MgpError::OutOfRange; }
    *result = records[index];
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_stream_batch_destroy(batch: *mut mgp_stream_batch) {
    if batch.is_null() { return; }
    let b = Box::from_raw(batch);
    for record in b.records {
        if !record.is_null() {
            // Records share the same layout as mgp_result_record.
            drop(Box::from_raw(record));
        }
    }
}

// ─── Transformation context (Kafka/Pulsar transformations) ────────────────

/// Context passed to transformation procedures.
#[repr(C)]
pub struct mgp_transformation_ctx {
    pub graph: *mut mgp_graph,
    pub records: Vec<*mut mgp_result_record>,
}

#[no_mangle]
pub unsafe extern "C" fn mgp_transformation_ctx_graph(
    ctx: *mut mgp_transformation_ctx,
    result: *mut *mut mgp_graph,
) -> MgpError {
    if ctx.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*ctx).graph;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_transformation_ctx_add_record(
    ctx: *mut mgp_transformation_ctx,
    record: *mut mgp_result_record,
) -> MgpError {
    if ctx.is_null() || record.is_null() { return MgpError::InvalidArgument; }
    (*ctx).records.push(record);
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_transformation_ctx_destroy(ctx: *mut mgp_transformation_ctx) {
    if ctx.is_null() { return; }
    let _ = Box::from_raw(ctx);
}

// ─── Trigger C API ────────────────────────────────────────────────────────

/// Event type that caused a trigger to fire.
#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MgpTriggerEventType {
    CreateVertex = 0,
    CreateEdge = 1,
    DeleteVertex = 2,
    DeleteEdge = 3,
    UpdateVertex = 4,
    UpdateEdge = 5,
    SetPropertyVertex = 6,
    SetPropertyEdge = 7,
    RemovePropertyVertex = 8,
    RemovePropertyEdge = 9,
}

/// Opaque trigger context.
#[repr(C)]
pub struct mgp_trigger_context {
    pub event: MgpTriggerEventType,
    /// Created vertices (gids) during this trigger event.
    pub created_vertices: Vec<u64>,
    /// Created edges (gids) during this trigger event.
    pub created_edges: Vec<u64>,
    /// Deleted vertices (gids) during this trigger event.
    pub deleted_vertices: Vec<u64>,
    /// Deleted edges (gids) during this trigger event.
    pub deleted_edges: Vec<u64>,
    /// Vertices with property set (gid, property_name, old_value, new_value).
    pub set_property_vertices: Vec<(u64, CString, *mut mgp_value, *mut mgp_value)>,
    /// Vertices with property removed (gid, property_name, old_value).
    pub removed_property_vertices: Vec<(u64, CString, *mut mgp_value)>,
    /// Reference graph for resolving gids into vertex/edge objects.
    pub graph: *const mgp_graph,
    /// Vertex state before mutation (for update triggers).
    pub vertex_before: Option<*mut mgp_vertex>,
    /// Vertex state after mutation (for update triggers).
    pub vertex_after: Option<*mut mgp_vertex>,
    /// Edge state before mutation.
    pub edge_before: Option<*mut mgp_edge>,
    /// Edge state after mutation.
    pub edge_after: Option<*mut mgp_edge>,
}

#[no_mangle]
pub unsafe extern "C" fn mgp_trigger_context_event_type(
    ctx: *const mgp_trigger_context,
    result: *mut MgpTriggerEventType,
) -> MgpError {
    if ctx.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*ctx).event;
    MgpError::NoError
}

macro_rules! trigger_iter_fn {
    ($fn_name:ident, $field:ident, $iter_ty:ident) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fn_name(
            ctx: *const mgp_trigger_context,
            result: *mut *mut $iter_ty,
        ) -> MgpError {
            if ctx.is_null() || result.is_null() { return MgpError::InvalidArgument; }
            let keys = (*ctx).$field.clone();
            let graph = (*ctx).graph;
            *result = Box::into_raw(Box::new($iter_ty { graph, keys, pos: 0 }));
            MgpError::NoError
        }
    };
}

trigger_iter_fn!(mgp_trigger_context_created_vertices, created_vertices, mgp_vertices_iterator);
trigger_iter_fn!(mgp_trigger_context_created_edges, created_edges, mgp_edges_iterator);
trigger_iter_fn!(mgp_trigger_context_deleted_vertices, deleted_vertices, mgp_vertices_iterator);
trigger_iter_fn!(mgp_trigger_context_deleted_edges, deleted_edges, mgp_edges_iterator);

/// Iterator for vertices with property set.
#[repr(C)]
pub struct mgp_set_property_vertices_iterator {
    items: Vec<(u64, CString, *mut mgp_value, *mut mgp_value)>,
    pos: usize,
    graph: *const mgp_graph,
}

#[no_mangle]
pub unsafe extern "C" fn mgp_trigger_context_set_property_vertices(
    ctx: *const mgp_trigger_context,
    result: *mut *mut mgp_set_property_vertices_iterator,
) -> MgpError {
    if ctx.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let items = (*ctx).set_property_vertices.clone();
    let graph = (*ctx).graph;
    *result = Box::into_raw(Box::new(mgp_set_property_vertices_iterator { items, pos: 0, graph }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_set_property_vertices_iterator_get(
    it: *mut mgp_set_property_vertices_iterator,
    result: *mut *mut mgp_vertex,
    property_name: *mut *const c_char,
    old_value: *mut *mut mgp_value,
    new_value: *mut *mut mgp_value,
) -> MgpError {
    if it.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let it = &mut *it;
    if it.pos >= it.items.len() {
        *result = ptr::null_mut();
        return MgpError::NoError;
    }
    let (gid, ref name, old, new) = it.items[it.pos];
    it.pos += 1;
    *result = Box::into_raw(Box::new(mgp_vertex { gid, graph: it.graph }));
    if !property_name.is_null() {
        *property_name = name.as_ptr();
    }
    if !old_value.is_null() {
        *old_value = old;
    }
    if !new_value.is_null() {
        *new_value = new;
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_set_property_vertices_iterator_destroy(it: *mut mgp_set_property_vertices_iterator) {
    if it.is_null() { return; }
    let _ = Box::from_raw(it);
}

/// Iterator for vertices with property removed.
#[repr(C)]
pub struct mgp_removed_property_vertices_iterator {
    items: Vec<(u64, CString, *mut mgp_value)>,
    pos: usize,
    graph: *const mgp_graph,
}

#[no_mangle]
pub unsafe extern "C" fn mgp_trigger_context_removed_property_vertices(
    ctx: *const mgp_trigger_context,
    result: *mut *mut mgp_removed_property_vertices_iterator,
) -> MgpError {
    if ctx.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let items = (*ctx).removed_property_vertices.clone();
    let graph = (*ctx).graph;
    *result = Box::into_raw(Box::new(mgp_removed_property_vertices_iterator { items, pos: 0, graph }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_removed_property_vertices_iterator_get(
    it: *mut mgp_removed_property_vertices_iterator,
    result: *mut *mut mgp_vertex,
    property_name: *mut *const c_char,
    old_value: *mut *mut mgp_value,
) -> MgpError {
    if it.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let it = &mut *it;
    if it.pos >= it.items.len() {
        *result = ptr::null_mut();
        return MgpError::NoError;
    }
    let (gid, ref name, old) = it.items[it.pos];
    it.pos += 1;
    *result = Box::into_raw(Box::new(mgp_vertex { gid, graph: it.graph }));
    if !property_name.is_null() {
        *property_name = name.as_ptr();
    }
    if !old_value.is_null() {
        *old_value = old;
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_removed_property_vertices_iterator_destroy(it: *mut mgp_removed_property_vertices_iterator) {
    if it.is_null() { return; }
    let _ = Box::from_raw(it);
}

#[no_mangle]
pub unsafe extern "C" fn mgp_trigger_context_vertex_before(
    ctx: *const mgp_trigger_context,
    result: *mut *mut mgp_vertex,
) -> MgpError {
    if ctx.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*ctx).vertex_before {
        Some(v) => *result = v,
        None => *result = ptr::null_mut(),
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_trigger_context_vertex_after(
    ctx: *const mgp_trigger_context,
    result: *mut *mut mgp_vertex,
) -> MgpError {
    if ctx.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*ctx).vertex_after {
        Some(v) => *result = v,
        None => *result = ptr::null_mut(),
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_trigger_context_edge_before(
    ctx: *const mgp_trigger_context,
    result: *mut *mut mgp_edge,
) -> MgpError {
    if ctx.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*ctx).edge_before {
        Some(e) => *result = e,
        None => *result = ptr::null_mut(),
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_trigger_context_edge_after(
    ctx: *const mgp_trigger_context,
    result: *mut *mut mgp_edge,
) -> MgpError {
    if ctx.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*ctx).edge_after {
        Some(e) => *result = e,
        None => *result = ptr::null_mut(),
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_trigger_context_destroy(ctx: *mut mgp_trigger_context) {
    if ctx.is_null() { return; }
    let c = Box::from_raw(ctx);
    for (_, _, old, new) in &c.set_property_vertices {
        if !old.is_null() { mgp_value_destroy(*old); }
        if !new.is_null() { mgp_value_destroy(*new); }
    }
    for (_, _, old) in &c.removed_property_vertices {
        if !old.is_null() { mgp_value_destroy(*old); }
    }
    if let Some(v) = c.vertex_before { mgp_vertex_destroy(v); }
    if let Some(v) = c.vertex_after { mgp_vertex_destroy(v); }
    if let Some(e) = c.edge_before { mgp_edge_destroy(e); }
    if let Some(e) = c.edge_after { mgp_edge_destroy(e); }
}

// ─── Text index C API execution ───────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_search_text_index(
    g: *mut mgp_graph,
    index_name: *const c_char,
    query: *const c_char,
    limit: usize,
    result: *mut *mut mgp_list,
) -> MgpError {
    if g.is_null() || index_name.is_null() || query.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(index_name).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let query_str = CStr::from_ptr(query).to_string_lossy();

    let text_indices = storage.text_indices.read().unwrap();
    let entry = match text_indices.get(&label_id) {
        Some(e) => e,
        None => {
            *result = Box::into_raw(Box::new(mgp_list { items: Vec::new() }));
            return MgpError::NoError;
        }
    };

    let search_results = match entry.index.search(&query_str, limit) {
        Ok(r) => r,
        Err(_) => {
            *result = Box::into_raw(Box::new(mgp_list { items: Vec::new() }));
            return MgpError::NoError;
        }
    };
    drop(text_indices);

    let mut items = Vec::new();
    for (gid, score) in search_results {
        let vertex = Box::into_raw(Box::new(mgp_vertex { gid: gid.as_uint(), graph: g }));
        let vertex_val = mgp_value::boxed(ValueInner::Vertex(vertex));
        let score_val = mgp_value::boxed(ValueInner::Double(score as f64));
        let pair = Box::into_raw(Box::new(mgp_list { items: vec![vertex_val, score_val] }));
        items.push(mgp_value::boxed(ValueInner::List(pair)));
    }

    *result = Box::into_raw(Box::new(mgp_list { items }));
    MgpError::NoError
}

// ─── Vector index C API execution ─────────────────────────────────────────

/// Search an HNSW vector index and return results as a list of
/// `[vertex, distance, similarity]` triples.
#[no_mangle]
pub unsafe extern "C" fn mgp_search_vector_index(
    g: *mut mgp_graph,
    index_name: *const c_char,
    query: *mut mgp_list,
    result_size: c_int,
    result: *mut *mut mgp_list,
) -> MgpError {
    if g.is_null() || index_name.is_null() || query.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(index_name).to_string_lossy();
    let label_id = catalog.label(&label_name);

    let mut query_vec = Vec::new();
    for item in &(*query).items {
        match &(**item).inner {
            ValueInner::Double(d) => query_vec.push(*d as f32),
            ValueInner::Int(i) => query_vec.push(*i as f32),
            _ => return MgpError::ValueConversion,
        }
    }

    let indices = storage.vector_indices.read().unwrap();
    let entry = match indices.get(&label_id) {
        Some(e) => e,
        None => {
            drop(indices);
            *result = Box::into_raw(Box::new(mgp_list { items: Vec::new() }));
            return MgpError::NoError;
        }
    };
    let index = entry.index.read().unwrap();

    if query_vec.len() != entry.dimension {
        drop(index);
        drop(indices);
        return MgpError::InvalidArgument;
    }

    let k = if result_size > 0 { result_size as usize } else { 10 };
    let search_results = index.search(&query_vec, k);
    drop(index);
    drop(indices);

    let mut items = Vec::new();
    for (node_id, distance) in search_results {
        let vertex = Box::into_raw(Box::new(mgp_vertex { gid: node_id as u64, graph: g }));
        let vertex_val = mgp_value::boxed(ValueInner::Vertex(vertex));
        let dist_val = mgp_value::boxed(ValueInner::Double(distance as f64));
        let sim_val = mgp_value::boxed(ValueInner::Double(1.0 / (1.0 + distance as f64)));

        let triple = Box::into_raw(Box::new(mgp_list { items: vec![vertex_val, dist_val, sim_val] }));
        items.push(mgp_value::boxed(ValueInner::List(triple)));
    }

    *result = Box::into_raw(Box::new(mgp_list { items }));
    MgpError::NoError
}
