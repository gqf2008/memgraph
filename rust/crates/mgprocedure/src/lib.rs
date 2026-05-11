#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]
//! # mgprocedure — C API for query module compatibility (`mg_procedure.h`).
//!
//! All functions return `MgpError` (matching `enum mgp_error` in C) and
//! write outputs through `**result` pointers. Backing storage is real Rust
//! structs allocated via the layout-tracking allocator in `MgpMemory`.
//!
//! Coverage: error enum, allocator, value tagged union, list, map, and
//! core value/list/map operations are implemented. Graph/vertex/edge,
//! procedure registration, streams, temporal arithmetic, and indexes are
//! signature-correct stubs returning `NotYetImplemented`.

use std::alloc::{self, Layout};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::{LazyLock, Mutex};
use chrono::{Datelike, Timelike};

// ─── Error codes (must match `enum mgp_error` in mg_procedure.h) ──────────

#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MgpError {
    NoError = 0,
    UnknownError = 1,
    UnableToAllocate = 2,
    InsufficientBuffer = 3,
    OutOfRange = 4,
    LogicError = 5,
    DeletedObject = 6,
    InvalidArgument = 7,
    KeyAlreadyExists = 8,
    ImmutableObject = 9,
    ValueConversion = 10,
    SerializationError = 11,
    AuthorizationError = 12,
    NotYetImplemented = 13,
}

#[no_mangle]
pub extern "C" fn mgp_is_enterprise_valid() -> c_int {
    0
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MgpValueType {
    Null = 0,
    Bool = 1,
    Int = 2,
    Double = 3,
    String = 4,
    List = 5,
    Map = 6,
    Vertex = 7,
    Edge = 8,
    Path = 9,
    Date = 10,
    LocalTime = 11,
    LocalDateTime = 12,
    Duration = 13,
    ZonedDateTime = 14,
    Point2D = 15,
    Point3D = 16,
    Enum = 17,
}

// ─── Allocator (tracks Layout per pointer for safe free) ──────────────────

struct AllocTracker {
    layouts: Mutex<HashMap<usize, Layout>>,
    bytes_used: Mutex<usize>,
}

impl AllocTracker {
    fn new() -> Self {
        Self { layouts: Mutex::new(HashMap::new()), bytes_used: Mutex::new(0) }
    }

    unsafe fn alloc(&self, size: usize, align: usize) -> *mut u8 {
        if size == 0 { return ptr::null_mut(); }
        let Ok(layout) = Layout::from_size_align(size, align) else { return ptr::null_mut(); };
        let p = alloc::alloc(layout);
        if !p.is_null() {
            self.layouts.lock().unwrap().insert(p as usize, layout);
            *self.bytes_used.lock().unwrap() += size;
        }
        p
    }

    unsafe fn free(&self, p: *mut u8) {
        if p.is_null() { return; }
        if let Some(layout) = self.layouts.lock().unwrap().remove(&(p as usize)) {
            let mut used = self.bytes_used.lock().unwrap();
            *used = used.saturating_sub(layout.size());
            alloc::dealloc(p, layout);
        }
    }

    fn bytes(&self) -> usize { *self.bytes_used.lock().unwrap() }
}

static GLOBAL_ALLOCATOR: LazyLock<AllocTracker> = LazyLock::new(AllocTracker::new);

#[repr(C)]
pub struct mgp_memory {
    tracker: *const AllocTracker,
}

impl mgp_memory {
    fn tracker(&self) -> &AllocTracker {
        if self.tracker.is_null() {
            &GLOBAL_ALLOCATOR
        } else {
            unsafe { &*self.tracker }
        }
    }
}

// ─── Value backing (tagged union — opaque to C) ───────────────────────────

#[allow(dead_code)]
#[derive(Clone)]
enum ValueInner {
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    String(CString),
    List(*mut mgp_list),
    Map(*mut mgp_map),
    Vertex(*mut mgp_vertex),
    Edge(*mut mgp_edge),
    Path(*mut mgp_path),
    Date(i64),
    LocalTime(i64),
    LocalDateTime(i64),
    Duration { months: i64, days: i64, micros: i64 },
    ZonedDateTime { micros: i64, offset: i16, tz: CString },
    Point2D { crs: u16, x: f64, y: f64 },
    Point3D { crs: u16, x: f64, y: f64, z: f64 },
    Enum { ty: CString, val: CString },
}

#[repr(C)]
pub struct mgp_value {
    tag: MgpValueType,
    inner: ValueInner,
}

impl mgp_value {
    fn boxed(inner: ValueInner) -> *mut mgp_value {
        let tag = match &inner {
            ValueInner::Null => MgpValueType::Null,
            ValueInner::Bool(_) => MgpValueType::Bool,
            ValueInner::Int(_) => MgpValueType::Int,
            ValueInner::Double(_) => MgpValueType::Double,
            ValueInner::String(_) => MgpValueType::String,
            ValueInner::List(_) => MgpValueType::List,
            ValueInner::Map(_) => MgpValueType::Map,
            ValueInner::Vertex(_) => MgpValueType::Vertex,
            ValueInner::Edge(_) => MgpValueType::Edge,
            ValueInner::Path(_) => MgpValueType::Path,
            ValueInner::Date(_) => MgpValueType::Date,
            ValueInner::LocalTime(_) => MgpValueType::LocalTime,
            ValueInner::LocalDateTime(_) => MgpValueType::LocalDateTime,
            ValueInner::Duration { .. } => MgpValueType::Duration,
            ValueInner::ZonedDateTime { .. } => MgpValueType::ZonedDateTime,
            ValueInner::Point2D { .. } => MgpValueType::Point2D,
            ValueInner::Point3D { .. } => MgpValueType::Point3D,
            ValueInner::Enum { .. } => MgpValueType::Enum,
        };
        Box::into_raw(Box::new(mgp_value { tag, inner }))
    }
}

#[repr(C)]
pub struct mgp_list {
    items: Vec<*mut mgp_value>,
}

#[repr(C)]
pub struct mgp_map {
    items: HashMap<CString, *mut mgp_value>,
}

// ─── Graph types backed by mgstorage ───────────────────────────────────────

#[repr(C)]
pub struct mgp_graph {
    storage: *const mgstorage::storage::Storage,
    tx: *const mgstorage::transaction::Transaction,
    catalog: *const mgcatalog::Catalog,
}

#[repr(C)]
pub struct mgp_vertex {
    gid: u64,
    graph: *const mgp_graph,
}

#[repr(C)]
pub struct mgp_edge {
    gid: u64,
    graph: *const mgp_graph,
}

#[repr(C)]
pub struct mgp_vertices_iterator {
    graph: *const mgp_graph,
    keys: Vec<u64>,
    pos: usize,
}

#[repr(C)]
pub struct mgp_edges_iterator {
    graph: *const mgp_graph,
    keys: Vec<u64>,
    pos: usize,
}

#[repr(C)]
pub struct mgp_properties_iterator {
    items: Vec<(*const c_char, *mut mgp_value)>,
    pos: usize,
}

// ─── Path ──────────────────────────────────────────────────────────────────

#[repr(C)]
pub struct mgp_path {
    vertices: Vec<*mut mgp_vertex>,
    edges: Vec<*mut mgp_edge>,
}

// ─── Type descriptors ──────────────────────────────────────────────────────

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MgpTypeTag {
    Any = 0,
    Bool = 1,
    Int = 2,
    Float = 3,
    Number = 4,
    String = 5,
    Map = 6,
    Node = 7,
    Relationship = 8,
    Path = 9,
    Date = 10,
    LocalTime = 11,
    LocalDateTime = 12,
    Duration = 13,
    ZonedDateTime = 14,
    Enum = 15,
    List = 16,
    Nullable = 17,
    Point2D = 18,
    Point3D = 19,
}

#[repr(C)]
pub struct mgp_type {
    tag: MgpTypeTag,
    elem: *mut mgp_type,
}

// Safe: mgp_type is immutable after creation and only accessed through C pointers.
unsafe impl Sync for mgp_type {}

macro_rules! type_singleton {
    ($name:ident, $tag:expr) => {
        static $name: mgp_type = mgp_type {
            tag: $tag,
            elem: std::ptr::null_mut(),
        };
    };
}

type_singleton!(TYPE_ANY, MgpTypeTag::Any);
type_singleton!(TYPE_BOOL, MgpTypeTag::Bool);
type_singleton!(TYPE_INT, MgpTypeTag::Int);
type_singleton!(TYPE_FLOAT, MgpTypeTag::Float);
type_singleton!(TYPE_NUMBER, MgpTypeTag::Number);
type_singleton!(TYPE_STRING, MgpTypeTag::String);
type_singleton!(TYPE_MAP, MgpTypeTag::Map);
type_singleton!(TYPE_NODE, MgpTypeTag::Node);
type_singleton!(TYPE_RELATIONSHIP, MgpTypeTag::Relationship);
type_singleton!(TYPE_PATH, MgpTypeTag::Path);
type_singleton!(TYPE_DATE, MgpTypeTag::Date);
type_singleton!(TYPE_LOCAL_TIME, MgpTypeTag::LocalTime);
type_singleton!(TYPE_LOCAL_DATE_TIME, MgpTypeTag::LocalDateTime);
type_singleton!(TYPE_DURATION, MgpTypeTag::Duration);
type_singleton!(TYPE_ZONED_DATE_TIME, MgpTypeTag::ZonedDateTime);
type_singleton!(TYPE_ENUM, MgpTypeTag::Enum);
type_singleton!(TYPE_POINT_2D, MgpTypeTag::Point2D);
type_singleton!(TYPE_POINT_3D, MgpTypeTag::Point3D);

pub mod stream;

// ─── Still-opaque types (not yet backed) ───────────────────────────────────

macro_rules! opaque {
    ($($name:ident),* $(,)?) => {
        $(
            #[repr(C)]
            pub struct $name { _private: [u8; 0] }
        )*
    };
}

/// Procedure callback signature.
pub type MgpProcCb = unsafe extern "C" fn(*mut mgp_list, *mut mgp_graph, *mut mgp_result, *mut mgp_memory);
/// Function callback signature.
pub type MgpFuncCb = unsafe extern "C" fn(*mut mgp_list, *mut mgp_graph, *mut mgp_func_result, *mut mgp_memory);

#[repr(C)]
pub struct mgp_result {
    error: Option<CString>,
    records: Vec<*mut mgp_result_record>,
    reserved: usize,
}

#[repr(C)]
pub struct mgp_result_record {
    fields: HashMap<CString, *mut mgp_value>,
}

#[repr(C)]
pub struct mgp_func_result {
    error: Option<CString>,
    value: Option<*mut mgp_value>,
}

/// Argument descriptor for procedures/functions.
struct ArgDesc {
    name: CString,
    ty: *mut mgp_type,
    default_value: Option<*mut mgp_value>,
}

/// Result field descriptor for procedures.
struct ResultField {
    name: CString,
    ty: *mut mgp_type,
    deprecated: bool,
}

#[repr(C)]
pub struct mgp_proc {
    name: CString,
    callback: MgpProcCb,
    args: Vec<ArgDesc>,
    opt_args: Vec<ArgDesc>,
    results: Vec<ResultField>,
    is_write: bool,
}

#[repr(C)]
pub struct mgp_func {
    name: CString,
    callback: MgpFuncCb,
    args: Vec<ArgDesc>,
    opt_args: Vec<ArgDesc>,
}

#[repr(C)]
pub struct mgp_module {
    procs: Vec<*mut mgp_proc>,
    funcs: Vec<*mut mgp_func>,
}

#[repr(C)]
pub struct mgp_map_item {
    key: *const c_char,
    value: *mut mgp_value,
}

#[repr(C)]
pub struct mgp_map_items_iterator {
    items: Vec<(CString, *mut mgp_value)>,
    pos: usize,
}

#[repr(C)]
pub struct mgp_duration {
    months: i64,
    days: i64,
    micros: i64,
}

#[repr(C)]
pub struct mgp_zoned_date_time {
    micros: i64,
    offset: i16,
    tz: CString,
}

#[repr(C)]
pub struct mgp_point_2d {
    crs: u16,
    x: f64,
    y: f64,
}

#[repr(C)]
pub struct mgp_point_3d {
    crs: u16,
    x: f64,
    y: f64,
    z: f64,
}

#[repr(C)]
pub struct mgp_enum {
    ty: CString,
    val: CString,
}

#[repr(C)]
pub struct mgp_date {
    pub days: i64,
}

#[repr(C)]
pub struct mgp_local_time {
    pub micros: i64,
}

#[repr(C)]
pub struct mgp_local_date_time {
    pub micros: i64,
}

opaque! {
    mgp_list_iterator,
    mgp_map_iterator,
    mgp_func_context,
    mgp_message,
    mgp_messages,
    mgp_vector_search_result,
}

#[repr(C)]
pub struct mgp_execution_result {
    columns: Vec<CString>,
    rows: Vec<*mut mgp_map>,
    current: usize,
}

#[repr(C)]
pub struct mgp_execution_headers {
    columns: Vec<CString>,
}

#[repr(C)]
pub struct mgp_label { pub name: *const c_char }

#[repr(C)]
pub struct mgp_edge_type { pub name: *const c_char }

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct mgp_vertex_id { pub as_int: i64 }

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct mgp_edge_id { pub as_int: i64 }

#[repr(C)]
pub struct mgp_property {
    pub name: *const c_char,
    pub value: *mut mgp_value,
}

#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MgpLogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    Critical = 5,
}

#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MgpSourceType { Kafka = 0, Pulsar = 1 }

#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TextSearchMode { SpecifiedProperties = 0, Regex = 1, AllProperties = 2 }

// ─── Memory API ───────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_alloc(memory: *mut mgp_memory, size: usize, result: *mut *mut c_void) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let tracker = if memory.is_null() { &GLOBAL_ALLOCATOR } else { (*memory).tracker() };
    let p = tracker.alloc(size, 8);
    if p.is_null() && size != 0 { return MgpError::UnableToAllocate; }
    *result = p as *mut c_void;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_aligned_alloc(memory: *mut mgp_memory, size: usize, alignment: usize, result: *mut *mut c_void) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let tracker = if memory.is_null() { &GLOBAL_ALLOCATOR } else { (*memory).tracker() };
    let p = tracker.alloc(size, alignment);
    if p.is_null() && size != 0 { return MgpError::UnableToAllocate; }
    *result = p as *mut c_void;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_free(memory: *mut mgp_memory, ptr: *mut c_void) {
    let tracker = if memory.is_null() { &GLOBAL_ALLOCATOR } else { (*memory).tracker() };
    tracker.free(ptr as *mut u8);
}

#[no_mangle]
pub unsafe extern "C" fn mgp_global_alloc(size: usize, result: *mut *mut c_void) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let p = GLOBAL_ALLOCATOR.alloc(size, 8);
    if p.is_null() && size != 0 { return MgpError::UnableToAllocate; }
    *result = p as *mut c_void;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_global_aligned_alloc(size: usize, alignment: usize, result: *mut *mut c_void) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let p = GLOBAL_ALLOCATOR.alloc(size, alignment);
    if p.is_null() && size != 0 { return MgpError::UnableToAllocate; }
    *result = p as *mut c_void;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_global_free(ptr: *mut c_void) {
    GLOBAL_ALLOCATOR.free(ptr as *mut u8);
}

// ─── Value: make ──────────────────────────────────────────────────────────

unsafe fn out_value(result: *mut *mut mgp_value, inner: ValueInner) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(inner);
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_null(_memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Null)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_bool(val: i32, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Bool(val != 0))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_int(val: i64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Int(val))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_double(val: f64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Double(val))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_string(val: *const c_char, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let cstr = CStr::from_ptr(val);
    out_value(result, ValueInner::String(cstr.to_owned()))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_list(list: *mut mgp_list, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::List(list))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_map(map: *mut mgp_map, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Map(map))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_vertex(v: *mut mgp_vertex, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Vertex(v))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_edge(e: *mut mgp_edge, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Edge(e))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_path(p: *mut mgp_path, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Path(p))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_date(days: i64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Date(days))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_local_time(us: i64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::LocalTime(us))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_local_date_time(us: i64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::LocalDateTime(us))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_duration(months: i64, days: i64, micros: i64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Duration { months, days, micros })
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_point_2d(crs: u16, x: f64, y: f64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Point2D { crs, x, y })
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_point_3d(crs: u16, x: f64, y: f64, z: f64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    out_value(result, ValueInner::Point3D { crs, x, y, z })
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_copy(val: *const mgp_value, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    out_value(result, (*val).inner.clone())
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_destroy(val: *mut mgp_value) {
    if val.is_null() { return; }
    drop(Box::from_raw(val));
}

// ─── Value: type query ────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_type(val: *const mgp_value, result: *mut MgpValueType) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*val).tag;
    MgpError::NoError
}

macro_rules! is_variant {
    ($fn_name:ident, $variant:pat) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fn_name(val: *const mgp_value, result: *mut i32) -> MgpError {
            if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
            *result = matches!((*val).inner, $variant) as i32;
            MgpError::NoError
        }
    };
}

is_variant!(mgp_value_is_null, ValueInner::Null);
is_variant!(mgp_value_is_bool, ValueInner::Bool(_));
is_variant!(mgp_value_is_int, ValueInner::Int(_));
is_variant!(mgp_value_is_double, ValueInner::Double(_));
is_variant!(mgp_value_is_string, ValueInner::String(_));
is_variant!(mgp_value_is_list, ValueInner::List(_));
is_variant!(mgp_value_is_map, ValueInner::Map(_));
is_variant!(mgp_value_is_vertex, ValueInner::Vertex(_));
is_variant!(mgp_value_is_edge, ValueInner::Edge(_));
is_variant!(mgp_value_is_path, ValueInner::Path(_));
is_variant!(mgp_value_is_date, ValueInner::Date(_));
is_variant!(mgp_value_is_local_time, ValueInner::LocalTime(_));
is_variant!(mgp_value_is_local_date_time, ValueInner::LocalDateTime(_));
is_variant!(mgp_value_is_duration, ValueInner::Duration { .. });
is_variant!(mgp_value_is_zoned_date_time, ValueInner::ZonedDateTime { .. });
is_variant!(mgp_value_is_point_2d, ValueInner::Point2D { .. });
is_variant!(mgp_value_is_point_3d, ValueInner::Point3D { .. });
is_variant!(mgp_value_is_enum, ValueInner::Enum { .. });

// ─── Value: getters ───────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_bool(val: *const mgp_value, result: *mut i32) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match &(*val).inner {
        ValueInner::Bool(b) => { *result = *b as i32; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_int(val: *const mgp_value, result: *mut i64) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match &(*val).inner {
        ValueInner::Int(i) => { *result = *i; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_double(val: *const mgp_value, result: *mut f64) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match &(*val).inner {
        ValueInner::Double(d) => { *result = *d; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_string(val: *const mgp_value, result: *mut *const c_char) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match &(*val).inner {
        ValueInner::String(s) => { *result = s.as_ptr(); MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_list(val: *const mgp_value, result: *mut *const mgp_list) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match &(*val).inner {
        ValueInner::List(p) => { *result = *p as *const mgp_list; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_map(val: *const mgp_value, result: *mut *const mgp_map) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match &(*val).inner {
        ValueInner::Map(p) => { *result = *p as *const mgp_map; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_vertex(val: *const mgp_value, result: *mut *const mgp_vertex) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match &(*val).inner {
        ValueInner::Vertex(p) => { *result = *p as *const mgp_vertex; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_edge(val: *const mgp_value, result: *mut *const mgp_edge) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match &(*val).inner {
        ValueInner::Edge(p) => { *result = *p as *const mgp_edge; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_path(val: *const mgp_value, result: *mut *const mgp_path) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match &(*val).inner {
        ValueInner::Path(p) => { *result = *p as *const mgp_path; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

// ─── Value type predicates ────────────────────────────────────────────────

macro_rules! value_is_fn {
    ($name:ident, $variant:pat) => {
        #[no_mangle]
        pub unsafe extern "C" fn $name(val: *const mgp_value, result: *mut i32) -> MgpError {
            if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
            *result = matches!((*val).inner, $variant) as i32;
            MgpError::NoError
        }
    };
}

// ─── Value getters for temporal/spatial/enum types ──────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_date(val: *const mgp_value, result: *mut i64) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*val).inner {
        ValueInner::Date(d) => { *result = d; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_local_time(val: *const mgp_value, result: *mut i64) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*val).inner {
        ValueInner::LocalTime(t) => { *result = t; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_local_date_time(val: *const mgp_value, result: *mut i64) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*val).inner {
        ValueInner::LocalDateTime(dt) => { *result = dt; MgpError::NoError }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_duration(val: *const mgp_value, result: *mut *mut mgp_duration) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*val).inner {
        ValueInner::Duration { months, days, micros } => {
            let d = Box::into_raw(Box::new(mgp_duration { months, days, micros }));
            *result = d;
            MgpError::NoError
        }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_zoned_date_time(val: *const mgp_value, result: *mut *mut mgp_zoned_date_time) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*val).inner.clone() {
        ValueInner::ZonedDateTime { micros, offset, tz } => {
            let z = Box::into_raw(Box::new(mgp_zoned_date_time {
                micros,
                offset,
                tz: CString::new(tz.to_bytes()).unwrap_or_default(),
            }));
            *result = z;
            MgpError::NoError
        }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_point_2d(val: *const mgp_value, result: *mut *mut mgp_point_2d) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*val).inner {
        ValueInner::Point2D { crs, x, y } => {
            let p = Box::into_raw(Box::new(mgp_point_2d { crs, x, y }));
            *result = p;
            MgpError::NoError
        }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_point_3d(val: *const mgp_value, result: *mut *mut mgp_point_3d) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*val).inner {
        ValueInner::Point3D { crs, x, y, z } => {
            let p = Box::into_raw(Box::new(mgp_point_3d { crs, x, y, z }));
            *result = p;
            MgpError::NoError
        }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_get_enum(val: *const mgp_value, result: *mut *mut mgp_enum) -> MgpError {
    if val.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    match (*val).inner.clone() {
        ValueInner::Enum { ty, val: v } => {
            let e = Box::into_raw(Box::new(mgp_enum {
                ty: CString::new(ty.to_bytes()).unwrap_or_default(),
                val: CString::new(v.to_bytes()).unwrap_or_default(),
            }));
            *result = e;
            MgpError::NoError
        }
        _ => MgpError::ValueConversion,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_zoned_date_time(micros: i64, offset: i16, tz: *const c_char, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if tz.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let tz_str = CStr::from_ptr(tz).to_owned();
    *result = mgp_value::boxed(ValueInner::ZonedDateTime { micros, offset, tz: tz_str });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_value_make_enum(enum_type: *const c_char, value: *const c_char, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if enum_type.is_null() || value.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let ty = CStr::from_ptr(enum_type).to_owned();
    let val = CStr::from_ptr(value).to_owned();
    *result = mgp_value::boxed(ValueInner::Enum { ty, val });
    MgpError::NoError
}

// ─── List ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_list_make_empty(capacity: usize, _memory: *mut mgp_memory, result: *mut *mut mgp_list) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_list { items: Vec::with_capacity(capacity) }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_destroy(list: *mut mgp_list) {
    if list.is_null() { return; }
    let l = Box::from_raw(list);
    for v in l.items {
        if !v.is_null() { drop(Box::from_raw(v)); }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_append(list: *mut mgp_list, val: *mut mgp_value) -> MgpError {
    if list.is_null() || val.is_null() { return MgpError::InvalidArgument; }
    let cloned = mgp_value::boxed((*val).inner.clone());
    (*list).items.push(cloned);
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_append_extend(list: *mut mgp_list, val: *mut mgp_value) -> MgpError {
    mgp_list_append(list, val)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_size(list: *const mgp_list, result: *mut usize) -> MgpError {
    if list.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*list).items.len();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_capacity(list: *const mgp_list, result: *mut usize) -> MgpError {
    if list.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*list).items.capacity();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_at(list: *const mgp_list, index: usize, result: *mut *mut mgp_value) -> MgpError {
    if list.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let items = &(*list).items;
    if index >= items.len() { return MgpError::OutOfRange; }
    *result = items[index];
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_reserve(list: *mut mgp_list, n: usize) -> MgpError {
    if list.is_null() { return MgpError::InvalidArgument; }
    (*list).items.reserve(n);
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_append_move(list: *mut mgp_list, val: *mut mgp_value) -> MgpError {
    mgp_list_append(list, val)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_copy(list: *mut mgp_list, _memory: *mut mgp_memory, result: *mut *mut mgp_list) -> MgpError {
    if list.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let mut new_list = mgp_list { items: Vec::new() };
    for item in &(*list).items {
        new_list.items.push(mgp_value::boxed((**item).inner.clone()));
    }
    *result = Box::into_raw(Box::new(new_list));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_contains_deleted(_list: *const mgp_list, result: *mut i32) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 0;
    MgpError::NoError
}

// ─── Map ──────────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_map_make_empty(_memory: *mut mgp_memory, result: *mut *mut mgp_map) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_map { items: HashMap::new() }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_destroy(map: *mut mgp_map) {
    if map.is_null() { return; }
    let m = Box::from_raw(map);
    for (_, v) in m.items {
        if !v.is_null() { drop(Box::from_raw(v)); }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_insert(map: *mut mgp_map, key: *const c_char, val: *mut mgp_value) -> MgpError {
    if map.is_null() || key.is_null() || val.is_null() { return MgpError::InvalidArgument; }
    let k = CStr::from_ptr(key).to_owned();
    if (*map).items.contains_key(&k) { return MgpError::KeyAlreadyExists; }
    let cloned = mgp_value::boxed((*val).inner.clone());
    (*map).items.insert(k, cloned);
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_update(map: *mut mgp_map, key: *const c_char, val: *mut mgp_value) -> MgpError {
    if map.is_null() || key.is_null() || val.is_null() { return MgpError::InvalidArgument; }
    let k = CStr::from_ptr(key).to_owned();
    let cloned = mgp_value::boxed((*val).inner.clone());
    if let Some(old) = (*map).items.insert(k, cloned) {
        if !old.is_null() { drop(Box::from_raw(old)); }
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_erase(map: *mut mgp_map, key: *const c_char) -> MgpError {
    if map.is_null() || key.is_null() { return MgpError::InvalidArgument; }
    let k = CStr::from_ptr(key);
    if let Some(v) = (*map).items.remove(k) {
        if !v.is_null() { drop(Box::from_raw(v)); }
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_size(map: *const mgp_map, result: *mut usize) -> MgpError {
    if map.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*map).items.len();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_at(map: *const mgp_map, key: *const c_char, result: *mut *mut mgp_value) -> MgpError {
    if map.is_null() || key.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let k = CStr::from_ptr(key);
    match (*map).items.get(k) {
        Some(v) => { *result = *v; MgpError::NoError }
        None => MgpError::OutOfRange,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_key_exists(map: *const mgp_map, key: *const c_char, result: *mut i32) -> MgpError {
    if map.is_null() || key.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let k = CStr::from_ptr(key);
    *result = (*map).items.contains_key(k) as i32;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_copy(map: *mut mgp_map, _memory: *mut mgp_memory, result: *mut *mut mgp_map) -> MgpError {
    if map.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let mut new_map = mgp_map { items: HashMap::new() };
    for (k, v) in &(*map).items {
        let cloned = mgp_value::boxed((**v).inner.clone());
        new_map.items.insert(CString::new(k.to_bytes()).unwrap_or_default(), cloned);
    }
    *result = Box::into_raw(Box::new(new_map));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_contains_deleted(_map: *const mgp_map, result: *mut i32) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_insert_move(map: *mut mgp_map, key: *const c_char, val: *mut mgp_value) -> MgpError {
    mgp_map_insert(map, key, val)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_update_move(map: *mut mgp_map, key: *const c_char, val: *mut mgp_value) -> MgpError {
    mgp_map_update(map, key, val)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_iter_items(map: *mut mgp_map, result: *mut *mut mgp_map_items_iterator) -> MgpError {
    if map.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let mut items = Vec::new();
    for (k, v) in &(*map).items {
        items.push((CString::new(k.to_bytes()).unwrap_or_default(), *v));
    }
    *result = Box::into_raw(Box::new(mgp_map_items_iterator { items, pos: 0 }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_items_iterator_get(it: *mut mgp_map_items_iterator, result: *mut *mut mgp_map_item) -> MgpError {
    if it.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let it = &mut *it;
    if it.pos >= it.items.len() {
        *result = ptr::null_mut();
        return MgpError::NoError;
    }
    let (ref key, value) = it.items[it.pos];
    it.pos += 1;
    let item = Box::into_raw(Box::new(mgp_map_item { key: key.as_ptr(), value }));
    *result = item;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_items_iterator_next(it: *mut mgp_map_items_iterator, result: *mut *mut mgp_map_item) -> MgpError {
    mgp_map_items_iterator_get(it, result)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_items_iterator_destroy(it: *mut mgp_map_items_iterator) {
    if it.is_null() { return; }
    drop(Box::from_raw(it));
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_item_key(item: *mut mgp_map_item, result: *mut *const c_char) -> MgpError {
    if item.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*item).key;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_map_item_value(item: *mut mgp_map_item, result: *mut *mut mgp_value) -> MgpError {
    if item.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*item).value;
    MgpError::NoError
}

// ─── Memory tracking ──────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_memory_tracked_bytes(memory: *const mgp_memory, result: *mut usize) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let tracker = if memory.is_null() { &GLOBAL_ALLOCATOR } else { (*memory).tracker() };
    *result = tracker.bytes();
    MgpError::NoError
}

// ─── Logging ──────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_log(level: i32, msg: *const c_char) -> MgpError {
    if msg.is_null() { return MgpError::InvalidArgument; }
    let s = CStr::from_ptr(msg).to_string_lossy();
    let prefix = match level {
        0 => "TRACE", 1 => "DEBUG", 2 => "INFO", 3 => "WARN", 4 => "ERROR", 5 => "CRITICAL",
        _ => "LOG",
    };
    eprintln!("[{}] {}", prefix, s);
    MgpError::NoError
}

// ─── Module entry point type ──────────────────────────────────────────────

pub type MgpMainFn = unsafe extern "C" fn(
    args: *mut mgp_list,
    graph: *mut mgp_graph,
    memory: *mut mgp_memory,
    result: *mut mgp_result,
) -> MgpError;

// ─── Stubs returning NotYetImplemented (correct ABI shape) ────────────────
//
// These exist so MAGE modules can link without missing symbols. Implementing
// them requires wiring through to mgstorage and mginterp, which lands in a
// later phase. Each writes a sentinel value to its output pointer if any.

macro_rules! todo_out_ptr {
    ($($name:ident($($arg:ident: $ty:ty),*) -> *mut $out:ty;)*) => {
        $(
            #[no_mangle]
            pub unsafe extern "C" fn $name($(_: $ty,)* result: *mut *mut $out) -> MgpError {
                if !result.is_null() { *result = ptr::null_mut(); }
                MgpError::NotYetImplemented
            }
        )*
    };
}


macro_rules! todo_void {
    ($($name:ident($($arg:ident: $ty:ty),*);)*) => {
        $(
            #[no_mangle]
            pub unsafe extern "C" fn $name($(_: $ty),*) -> MgpError { MgpError::NotYetImplemented }
        )*
    };
}

// ─── Graph / Vertex / Edge implementations ─────────────────────────────────

use mgcore::point::{Crs, Point2D, Point3D};
use mgcore::property_value::PropertyValue;
use mgcore::temporal::{Date, Duration, LocalDateTime, LocalTime, ZonedDateTime};
use mgcore::types::Gid;
use mgstorage::storage::Storage;

// ─── Graph queries ──────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_is_mutable(_g: *const mgp_graph, result: *mut i32) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 1;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_is_transactional(_g: *const mgp_graph, result: *mut i32) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 1;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_approximate_vertex_count(g: *const mgp_graph, result: *mut u64) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    if g.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    *result = storage.vertex_count() as u64;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_approximate_edge_count(g: *const mgp_graph, result: *mut u64) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    if g.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    *result = storage.edge_count() as u64;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_has_text_index(_g: *const mgp_graph, _name: *const c_char, result: *mut i32) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_must_abort(_g: *const mgp_graph, result: *mut i32) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 0;
    MgpError::NoError
}

// ─── Graph iteration ────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_iter_vertices(g: *mut mgp_graph, _memory: *mut mgp_memory, result: *mut *mut mgp_vertices_iterator) -> MgpError {
    if g.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let keys: Vec<u64> = storage.all_vertices().iter().map(|(gid, _, _)| gid.as_uint()).collect();
    *result = Box::into_raw(Box::new(mgp_vertices_iterator { graph: g, keys, pos: 0 }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_get_vertex_by_id(g: *mut mgp_graph, id: mgp_vertex_id, _memory: *mut mgp_memory, result: *mut *mut mgp_vertex) -> MgpError {
    if g.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let tx = &*(*g).tx;
    let gid = Gid::from(id.as_int as u64);
    if storage.get_vertex(gid, tx).is_some() {
        *result = Box::into_raw(Box::new(mgp_vertex { gid: id.as_int as u64, graph: g }));
        MgpError::NoError
    } else {
        *result = ptr::null_mut();
        MgpError::NoError
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_create_vertex(g: *mut mgp_graph, _memory: *mut mgp_memory, result: *mut *mut mgp_vertex) -> MgpError {
    if g.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*g).storage as *mut Storage);
    let tx = &*(*g).tx;
    let gid = storage.allocate_gid();
    match storage.create_vertex(tx, gid) {
        Ok(_) => {
            *result = Box::into_raw(Box::new(mgp_vertex { gid: gid.as_uint(), graph: g }));
            MgpError::NoError
        }
        Err(_) => {
            *result = ptr::null_mut();
            MgpError::LogicError
        }
    }
}

// ─── Vertex queries ─────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_get_id(v: *const mgp_vertex, result: *mut mgp_vertex_id) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_vertex_id { as_int: (*v).gid as i64 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_copy(v: *mut mgp_vertex, _memory: *mut mgp_memory, result: *mut *mut mgp_vertex) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_vertex { gid: (*v).gid, graph: (*v).graph }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_destroy(v: *mut mgp_vertex) {
    if v.is_null() { return; }
    drop(Box::from_raw(v));
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_labels_count(v: *const mgp_vertex, result: *mut usize) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let gid = (*v).gid;
    let gid = Gid::from(gid);
    match storage.get_vertex(gid, tx) {
        Some(snap) => { *result = snap.labels.len(); MgpError::NoError }
        None => { *result = 0; MgpError::DeletedObject }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_has_label(v: *const mgp_vertex, label: mgp_label, result: *mut i32) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    if label.name.is_null() { *result = 0; return MgpError::NoError; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let label_name = CStr::from_ptr(label.name).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let gid = Gid::from((*v).gid);
    match storage.get_vertex(gid, tx) {
        Some(snap) => {
            *result = if snap.labels.contains(&label_id) { 1 } else { 0 };
            MgpError::NoError
        }
        None => {
            *result = 0;
            MgpError::DeletedObject
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_has_label_named(v: *const mgp_vertex, label_name: *const c_char, result: *mut i32) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let name = CStr::from_ptr(label_name).to_string_lossy();
    let label_id = catalog.label(&name);
    let gid = Gid::from((*v).gid);
    match storage.get_vertex(gid, tx) {
        Some(snap) => {
            *result = if snap.labels.contains(&label_id) { 1 } else { 0 };
            MgpError::NoError
        }
        None => {
            *result = 0;
            MgpError::DeletedObject
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_label_at(v: *const mgp_vertex, idx: usize, result: *mut mgp_label) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let gid = Gid::from((*v).gid);
    match storage.get_vertex(gid, tx) {
        Some(snap) => {
            if idx >= snap.labels.len() {
                (*result).name = ptr::null();
                return MgpError::OutOfRange;
            }
            let label_id = snap.labels[idx];
            let name = catalog.label_name(label_id);
            let cname = CString::new(name).unwrap_or_default();
            let ptr = cname.into_raw();
            (*result).name = ptr;
            MgpError::NoError
        }
        None => {
            (*result).name = ptr::null();
            MgpError::DeletedObject
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_is_deleted(v: *const mgp_vertex, result: *mut i32) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let gid = (*v).gid;
    let gid = Gid::from(gid);
    *result = if storage.get_vertex(gid, tx).is_some() { 0 } else { 1 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_equal(v1: *const mgp_vertex, v2: *const mgp_vertex, result: *mut i32) -> MgpError {
    if v1.is_null() || v2.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = if (*v1).gid == (*v2).gid { 1 } else { 0 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_underlying_graph_is_mutable(_v: *const mgp_vertex, result: *mut i32) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 1;
    MgpError::NoError
}

// ─── Vertex edge iteration ──────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_get_in_degree(v: *const mgp_vertex, result: *mut usize) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let gid = Gid::from((*v).gid);
    *result = storage.vertex_in_edge_gids(gid).len();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_get_out_degree(v: *const mgp_vertex, result: *mut usize) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let gid = Gid::from((*v).gid);
    *result = storage.vertex_out_edge_gids(gid).len();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_iter_in_edges(v: *const mgp_vertex, _memory: *mut mgp_memory, result: *mut *mut mgp_edges_iterator) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let gid = Gid::from((*v).gid);
    let keys: Vec<u64> = storage.vertex_in_edge_gids(gid).iter().map(|g| g.as_uint()).collect();
    *result = Box::into_raw(Box::new(mgp_edges_iterator { graph, keys, pos: 0 }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_iter_out_edges(v: *const mgp_vertex, _memory: *mut mgp_memory, result: *mut *mut mgp_edges_iterator) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let gid = Gid::from((*v).gid);
    let keys: Vec<u64> = storage.vertex_out_edge_gids(gid).iter().map(|g| g.as_uint()).collect();
    *result = Box::into_raw(Box::new(mgp_edges_iterator { graph, keys, pos: 0 }));
    MgpError::NoError
}

/// Convert a PropertyValue to a heap-allocated mgp_value.
fn property_value_to_mgp_value(pv: &PropertyValue) -> *mut mgp_value {
    let inner = match pv {
        PropertyValue::Null => ValueInner::Null,
        PropertyValue::Bool(b) => ValueInner::Bool(*b),
        PropertyValue::Int(i) => ValueInner::Int(*i),
        PropertyValue::Double(d) => ValueInner::Double(*d),
        PropertyValue::String(s) => ValueInner::String(CString::new(s.as_str()).unwrap_or_default()),
        PropertyValue::List(items) => {
            let list = Box::into_raw(Box::new(mgp_list { items: Vec::new() }));
            for item in items {
                let v = property_value_to_mgp_value(item);
                unsafe { (*list).items.push(v); }
            }
            ValueInner::List(list)
        }
        PropertyValue::Map(entries) => {
            let map = Box::into_raw(Box::new(mgp_map { items: HashMap::new() }));
            for (k, v) in entries {
                let key = CString::new(k.as_str()).unwrap_or_default();
                let val = property_value_to_mgp_value(v);
                unsafe { (*map).items.insert(key, val); }
            }
            ValueInner::Map(map)
        }
        PropertyValue::Vertex(_) | PropertyValue::Edge(_) | PropertyValue::Path(_) => ValueInner::Null,
        PropertyValue::Date(d) => ValueInner::Date(d.days_since_epoch),
        PropertyValue::LocalTime(t) => ValueInner::LocalTime(t.microseconds),
        PropertyValue::LocalDateTime(dt) => ValueInner::LocalDateTime(dt.microseconds),
        PropertyValue::ZonedDateTime(zdt) => ValueInner::ZonedDateTime {
            micros: zdt.utc_microseconds,
            offset: zdt.offset_minutes,
            tz: CString::new(zdt.timezone.as_str()).unwrap_or_default(),
        },
        PropertyValue::Duration(d) => ValueInner::Duration {
            months: d.months,
            days: d.days,
            micros: d.microseconds,
        },
        PropertyValue::Point2D(p) => ValueInner::Point2D { crs: p.crs as u16, x: p.x, y: p.y },
        PropertyValue::Point3D(p) => ValueInner::Point3D { crs: p.crs as u16, x: p.x, y: p.y, z: p.z },
        PropertyValue::Enum { enum_type, value } => ValueInner::Enum {
            ty: CString::new(enum_type.as_str()).unwrap_or_default(),
            val: CString::new(value.as_str()).unwrap_or_default(),
        },
    };
    mgp_value::boxed(inner)
}

/// Convert an mgp_value to a PropertyValue for storage.
unsafe fn mgp_value_to_property_value(val: *const mgp_value) -> Option<PropertyValue> {
    if val.is_null() { return Some(PropertyValue::Null); }
    Some(match &(*val).inner {
        ValueInner::Null => PropertyValue::Null,
        ValueInner::Bool(b) => PropertyValue::Bool(*b),
        ValueInner::Int(i) => PropertyValue::Int(*i),
        ValueInner::Double(d) => PropertyValue::Double(*d),
        ValueInner::String(s) => PropertyValue::String(s.to_string_lossy().into_owned()),
        ValueInner::List(list) => {
            let items: Vec<PropertyValue> = (**list).items.iter()
                .filter_map(|item| mgp_value_to_property_value(*item))
                .collect();
            PropertyValue::List(items)
        }
        ValueInner::Map(map) => {
            let mut entries = Vec::new();
            for (k, v) in &(**map).items {
                if let Some(pv) = mgp_value_to_property_value(*v) {
                    entries.push((k.to_string_lossy().into_owned(), pv));
                }
            }
            PropertyValue::Map(entries)
        }
        ValueInner::Date(days) => PropertyValue::Date(Date::from_days(*days)),
        ValueInner::LocalTime(us) => PropertyValue::LocalTime(LocalTime::from_microseconds(*us)),
        ValueInner::LocalDateTime(us) => PropertyValue::LocalDateTime(LocalDateTime::from_microseconds(*us)),
        ValueInner::Duration { months, days, micros } => PropertyValue::Duration(Duration::new(*months, *days, *micros)),
        ValueInner::ZonedDateTime { micros, offset, tz } => PropertyValue::ZonedDateTime(ZonedDateTime::new(*micros, *offset, tz.to_string_lossy().into_owned())),
        ValueInner::Point2D { crs, x, y } => {
            let crs = match *crs {
                4326 => Crs::WGS84,
                7203 => Crs::Cartesian2D,
                9157 => Crs::Cartesian3D,
                4979 => Crs::WGS843D,
                _ => Crs::Cartesian2D,
            };
            PropertyValue::Point2D(Point2D::new(crs, *x, *y))
        }
        ValueInner::Point3D { crs, x, y, z } => {
            let crs = match *crs {
                4326 => Crs::WGS84,
                7203 => Crs::Cartesian2D,
                9157 => Crs::Cartesian3D,
                4979 => Crs::WGS843D,
                _ => Crs::Cartesian3D,
            };
            PropertyValue::Point3D(Point3D::new(crs, *x, *y, *z))
        }
        ValueInner::Enum { ty, val } => PropertyValue::Enum {
            enum_type: ty.to_string_lossy().into_owned(),
            value: val.to_string_lossy().into_owned(),
        },
        ValueInner::Vertex(_) | ValueInner::Edge(_) | ValueInner::Path(_) => return None,
    })
}

/// Get the catalog for name resolution from a graph pointer.
unsafe fn graph_catalog(graph: *const mgp_graph) -> &'static mgcatalog::Catalog {
    if !graph.is_null() && !(*graph).catalog.is_null() {
        &*(*graph).catalog
    } else {
        static GLOBAL_CATALOG: LazyLock<mgcatalog::Catalog> = LazyLock::new(mgcatalog::Catalog::new);
        &GLOBAL_CATALOG
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_iter_properties(v: *const mgp_vertex, _memory: *mut mgp_memory, result: *mut *mut mgp_properties_iterator) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let gid = Gid::from((*v).gid);
    let snap = match storage.get_vertex(gid, tx) {
        Some(s) => s,
        None => { *result = ptr::null_mut(); return MgpError::NoError; }
    };
    let catalog = graph_catalog(graph);
    let mut items = Vec::new();
    for (prop_id, prop_val) in snap.properties.iter() {
        let name = CString::new(catalog.property_name(prop_id)).unwrap_or_default();
        let value = property_value_to_mgp_value(prop_val);
        items.push((name.as_ptr() as *const c_char, value));
        // Leak the CString so the pointer remains valid for the iterator lifetime.
        let _ = name.into_raw();
    }
    *result = Box::into_raw(Box::new(mgp_properties_iterator { items, pos: 0 }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_get_property(
    v: *const mgp_vertex,
    name: *const c_char,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_value,
) -> MgpError {
    if v.is_null() || name.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let prop_name = CStr::from_ptr(name).to_string_lossy();
    let prop_id = catalog.property(&prop_name);
    let gid = Gid::from((*v).gid);
    match storage.get_vertex(gid, tx) {
        Some(snap) => {
            let val = snap.properties.get(prop_id);
            *result = property_value_to_mgp_value(val);
            MgpError::NoError
        }
        None => {
            *result = ptr::null_mut();
            MgpError::DeletedObject
        }
    }
}

// ─── Edge queries ───────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_get_id(e: *const mgp_edge, result: *mut mgp_edge_id) -> MgpError {
    if e.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_edge_id { as_int: (*e).gid as i64 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_copy(e: *mut mgp_edge, _memory: *mut mgp_memory, result: *mut *mut mgp_edge) -> MgpError {
    if e.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_edge { gid: (*e).gid, graph: (*e).graph }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_destroy(e: *mut mgp_edge) {
    if e.is_null() { return; }
    drop(Box::from_raw(e));
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_get_type(e: *const mgp_edge, result: *mut mgp_edge_type) -> MgpError {
    if e.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*e).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let gid = Gid::from((*e).gid);
    match storage.get_edge(gid, tx) {
        Some(snap) => {
            let name = catalog.edge_type_name(snap.edge_type);
            let cname = CString::new(name).unwrap_or_default();
            let ptr = cname.into_raw();
            (*result).name = ptr;
            MgpError::NoError
        }
        None => {
            (*result).name = ptr::null();
            MgpError::DeletedObject
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_get_from(e: *const mgp_edge, result: *mut *mut mgp_vertex) -> MgpError {
    if e.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*e).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let gid = Gid::from((*e).gid);
    match storage.get_edge(gid, tx) {
        Some(snap) => {
            let v = Box::into_raw(Box::new(mgp_vertex { gid: snap.from_vertex.as_uint(), graph }));
            *result = v;
            MgpError::NoError
        }
        None => {
            *result = ptr::null_mut();
            MgpError::DeletedObject
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_get_to(e: *const mgp_edge, result: *mut *mut mgp_vertex) -> MgpError {
    if e.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*e).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let gid = Gid::from((*e).gid);
    match storage.get_edge(gid, tx) {
        Some(snap) => {
            let v = Box::into_raw(Box::new(mgp_vertex { gid: snap.to_vertex.as_uint(), graph }));
            *result = v;
            MgpError::NoError
        }
        None => {
            *result = ptr::null_mut();
            MgpError::DeletedObject
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_is_deleted(e: *const mgp_edge, result: *mut i32) -> MgpError {
    if e.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*e).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let gid = (*e).gid;
    let gid = Gid::from(gid);
    *result = if storage.get_edge(gid, tx).is_some() { 0 } else { 1 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_equal(e1: *const mgp_edge, e2: *const mgp_edge, result: *mut i32) -> MgpError {
    if e1.is_null() || e2.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = if (*e1).gid == (*e2).gid { 1 } else { 0 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_underlying_graph_is_mutable(_e: *const mgp_edge, result: *mut i32) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 1;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_iter_properties(e: *mut mgp_edge, _memory: *mut mgp_memory, result: *mut *mut mgp_properties_iterator) -> MgpError {
    if e.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*e).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let gid = (*e).gid;
    let gid = Gid::from(gid);
    let snap = match storage.get_edge(gid, tx) {
        Some(s) => s,
        None => { *result = ptr::null_mut(); return MgpError::NoError; }
    };
    let catalog = graph_catalog(graph);
    let mut items = Vec::new();
    for (prop_id, prop_val) in snap.properties.iter() {
        let name = CString::new(catalog.property_name(prop_id)).unwrap_or_default();
        let value = property_value_to_mgp_value(prop_val);
        items.push((name.as_ptr() as *const c_char, value));
        let _ = name.into_raw();
    }
    *result = Box::into_raw(Box::new(mgp_properties_iterator { items, pos: 0 }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_get_property(
    e: *const mgp_edge,
    name: *const c_char,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_value,
) -> MgpError {
    if e.is_null() || name.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let graph = (*e).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*graph).storage;
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let prop_name = CStr::from_ptr(name).to_string_lossy();
    let prop_id = catalog.property(&prop_name);
    let gid = Gid::from((*e).gid);
    match storage.get_edge(gid, tx) {
        Some(snap) => {
            let val = snap.properties.get(prop_id);
            *result = property_value_to_mgp_value(val);
            MgpError::NoError
        }
        None => {
            *result = ptr::null_mut();
            MgpError::DeletedObject
        }
    }
}

// ─── Iterator implementations ───────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_vertices_iterator_get(it: *mut mgp_vertices_iterator, result: *mut *const mgp_vertex) -> MgpError {
    if it.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let it = &mut *it;
    if it.pos >= it.keys.len() {
        *result = ptr::null();
        return MgpError::NoError;
    }
    let gid = it.keys[it.pos];
    it.pos += 1;
    let v = Box::into_raw(Box::new(mgp_vertex { gid, graph: it.graph }));
    *result = v;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertices_iterator_destroy(it: *mut mgp_vertices_iterator) {
    if it.is_null() { return; }
    drop(Box::from_raw(it));
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertices_iterator_next(it: *mut mgp_vertices_iterator, result: *mut *const mgp_vertex) -> MgpError {
    mgp_vertices_iterator_get(it, result)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertices_iterator_underlying_graph_is_mutable(_it: *const mgp_vertices_iterator, result: *mut i32) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 1;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edges_iterator_get(it: *mut mgp_edges_iterator, result: *mut *const mgp_edge) -> MgpError {
    if it.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let it = &mut *it;
    if it.pos >= it.keys.len() {
        *result = ptr::null();
        return MgpError::NoError;
    }
    let gid = it.keys[it.pos];
    it.pos += 1;
    let e = Box::into_raw(Box::new(mgp_edge { gid, graph: it.graph }));
    *result = e;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edges_iterator_destroy(it: *mut mgp_edges_iterator) {
    if it.is_null() { return; }
    drop(Box::from_raw(it));
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edges_iterator_next(it: *mut mgp_edges_iterator, result: *mut *const mgp_edge) -> MgpError {
    mgp_edges_iterator_get(it, result)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edges_iterator_underlying_graph_is_mutable(_it: *const mgp_edges_iterator, result: *mut i32) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 1;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_properties_iterator_get(it: *mut mgp_properties_iterator, result: *mut *mut mgp_property) -> MgpError {
    if it.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let it = &mut *it;
    if it.pos >= it.items.len() {
        *result = ptr::null_mut();
        return MgpError::NoError;
    }
    let (name, value) = it.items[it.pos];
    it.pos += 1;
    let prop = Box::into_raw(Box::new(mgp_property { name, value }));
    *result = prop;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_properties_iterator_destroy(it: *mut mgp_properties_iterator) {
    if it.is_null() { return; }
    drop(Box::from_raw(it));
}

#[no_mangle]
pub unsafe extern "C" fn mgp_properties_iterator_next(it: *mut mgp_properties_iterator, result: *mut *mut mgp_property) -> MgpError {
    mgp_properties_iterator_get(it, result)
}

// ─── Path ───────────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_path_size(p: *const mgp_path, result: *mut usize) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*p).edges.len();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_path_vertex_at(p: *const mgp_path, index: usize, result: *mut *mut mgp_vertex) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let path = &*p;
    if index >= path.vertices.len() {
        *result = ptr::null_mut();
        return MgpError::OutOfRange;
    }
    let v = path.vertices[index];
    *result = Box::into_raw(Box::new(mgp_vertex { gid: (*v).gid, graph: (*v).graph }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_path_edge_at(p: *const mgp_path, index: usize, result: *mut *mut mgp_edge) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let path = &*p;
    if index >= path.edges.len() {
        *result = ptr::null_mut();
        return MgpError::OutOfRange;
    }
    let e = path.edges[index];
    *result = Box::into_raw(Box::new(mgp_edge { gid: (*e).gid, graph: (*e).graph }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_path_equal(p1: *const mgp_path, p2: *const mgp_path, result: *mut i32) -> MgpError {
    if p1.is_null() || p2.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let a = &*p1;
    let b = &*p2;
    if a.vertices.len() != b.vertices.len() || a.edges.len() != b.edges.len() {
        *result = 0;
        return MgpError::NoError;
    }
    for i in 0..a.vertices.len() {
        if (*a.vertices[i]).gid != (*b.vertices[i]).gid {
            *result = 0;
            return MgpError::NoError;
        }
    }
    for i in 0..a.edges.len() {
        if (*a.edges[i]).gid != (*b.edges[i]).gid {
            *result = 0;
            return MgpError::NoError;
        }
    }
    *result = 1;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_path_contains_deleted(p: *const mgp_path, result: *mut i32) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let path = &*p;
    for v in &path.vertices {
        let mut deleted = 0i32;
        if mgp_vertex_is_deleted(*v, &mut deleted) != MgpError::NoError || deleted != 0 {
            *result = 1;
            return MgpError::NoError;
        }
    }
    for e in &path.edges {
        let mut deleted = 0i32;
        if mgp_edge_is_deleted(*e, &mut deleted) != MgpError::NoError || deleted != 0 {
            *result = 1;
            return MgpError::NoError;
        }
    }
    *result = 0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_path_copy(p: *mut mgp_path, _memory: *mut mgp_memory, result: *mut *mut mgp_path) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let path = &*p;
    let mut vertices = Vec::with_capacity(path.vertices.len());
    for v in &path.vertices {
        let mut copy: *mut mgp_vertex = ptr::null_mut();
        mgp_vertex_copy(*v, _memory, &mut copy);
        vertices.push(copy);
    }
    let mut edges = Vec::with_capacity(path.edges.len());
    for e in &path.edges {
        let mut copy: *mut mgp_edge = ptr::null_mut();
        mgp_edge_copy(*e, _memory, &mut copy);
        edges.push(copy);
    }
    *result = Box::into_raw(Box::new(mgp_path { vertices, edges }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_path_make_with_start(v: *mut mgp_vertex, _memory: *mut mgp_memory, result: *mut *mut mgp_path) -> MgpError {
    if v.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let mut copy: *mut mgp_vertex = ptr::null_mut();
    mgp_vertex_copy(v, _memory, &mut copy);
    *result = Box::into_raw(Box::new(mgp_path {
        vertices: vec![copy],
        edges: Vec::new(),
    }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_path_expand(p: *mut mgp_path, edge: *mut mgp_edge) -> MgpError {
    if p.is_null() || edge.is_null() { return MgpError::InvalidArgument; }
    let path = &mut *p;
    if path.vertices.is_empty() {
        return MgpError::LogicError;
    }
    let mut copy: *mut mgp_edge = ptr::null_mut();
    mgp_edge_copy(edge, ptr::null_mut(), &mut copy);
    path.edges.push(copy);
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_path_pop(p: *mut mgp_path) -> MgpError {
    if p.is_null() { return MgpError::InvalidArgument; }
    let path = &mut *p;
    if path.edges.is_empty() {
        return MgpError::OutOfRange;
    }
    if let Some(e) = path.edges.pop() {
        mgp_edge_destroy(e);
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_path_destroy(p: *mut mgp_path) {
    if p.is_null() { return; }
    let path = Box::from_raw(p);
    for v in path.vertices {
        mgp_vertex_destroy(v);
    }
    for e in path.edges {
        mgp_edge_destroy(e);
    }
}

// ─── Mutation ───────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_set_property(v: *mut mgp_vertex, name: *const c_char, val: *mut mgp_value) -> MgpError {
    if v.is_null() || name.is_null() || val.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*graph).storage as *mut Storage);
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let property_name = CStr::from_ptr(name).to_string_lossy();
    let property_id = catalog.property(&property_name);
    let pv = match mgp_value_to_property_value(val) {
        Some(pv) => pv,
        None => return MgpError::ValueConversion,
    };
    let gid = Gid::from((*v).gid);
    match storage.vertex_set_property(tx, gid, property_id, pv) {
        Ok(()) => MgpError::NoError,
        Err(_) => MgpError::LogicError,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_set_properties(v: *mut mgp_vertex, properties: *mut mgp_map) -> MgpError {
    if v.is_null() || properties.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*graph).storage as *mut Storage);
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let gid = Gid::from((*v).gid);
    for (key_cstr, val_ptr) in &(*properties).items {
        let property_name = key_cstr.to_string_lossy();
        let property_id = catalog.property(&property_name);
        let pv = match mgp_value_to_property_value(*val_ptr) {
            Some(pv) => pv,
            None => continue,
        };
        if storage.vertex_set_property(tx, gid, property_id, pv).is_err() {
            return MgpError::LogicError;
        }
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_add_label(v: *mut mgp_vertex, label: mgp_label) -> MgpError {
    if v.is_null() || label.name.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*graph).storage as *mut Storage);
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let label_name = CStr::from_ptr(label.name).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let gid = Gid::from((*v).gid);
    match storage.vertex_add_label(tx, gid, label_id) {
        Ok(()) => MgpError::NoError,
        Err(_) => MgpError::LogicError,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_vertex_remove_label(v: *mut mgp_vertex, label: mgp_label) -> MgpError {
    if v.is_null() || label.name.is_null() { return MgpError::InvalidArgument; }
    let graph = (*v).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*graph).storage as *mut Storage);
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let label_name = CStr::from_ptr(label.name).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let gid = Gid::from((*v).gid);
    match storage.vertex_remove_label(tx, gid, label_id) {
        Ok(()) => MgpError::NoError,
        Err(_) => MgpError::LogicError,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_set_property(e: *mut mgp_edge, name: *const c_char, val: *mut mgp_value) -> MgpError {
    if e.is_null() || name.is_null() || val.is_null() { return MgpError::InvalidArgument; }
    let graph = (*e).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*graph).storage as *mut Storage);
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let property_name = CStr::from_ptr(name).to_string_lossy();
    let property_id = catalog.property(&property_name);
    let pv = match mgp_value_to_property_value(val) {
        Some(pv) => pv,
        None => return MgpError::ValueConversion,
    };
    let gid = Gid::from((*e).gid);
    match storage.edge_set_property(tx, gid, property_id, pv) {
        Ok(()) => MgpError::NoError,
        Err(_) => MgpError::LogicError,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_edge_set_properties(e: *mut mgp_edge, properties: *mut mgp_map) -> MgpError {
    if e.is_null() || properties.is_null() { return MgpError::InvalidArgument; }
    let graph = (*e).graph;
    if graph.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*graph).storage as *mut Storage);
    let tx = &*(*graph).tx;
    let catalog = graph_catalog(graph);
    let gid = Gid::from((*e).gid);
    for (key_cstr, val_ptr) in &(*properties).items {
        let property_name = key_cstr.to_string_lossy();
        let property_id = catalog.property(&property_name);
        let pv = match mgp_value_to_property_value(*val_ptr) {
            Some(pv) => pv,
            None => continue,
        };
        if storage.edge_set_property(tx, gid, property_id, pv).is_err() {
            return MgpError::LogicError;
        }
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_create_edge(g: *mut mgp_graph, from: *mut mgp_vertex, to: *mut mgp_vertex, ty: mgp_edge_type, _memory: *mut mgp_memory, result: *mut *mut mgp_edge) -> MgpError {
    if g.is_null() || from.is_null() || to.is_null() || ty.name.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*g).storage as *mut Storage);
    let tx = &*(*g).tx;
    let catalog = graph_catalog(g);
    let type_name = CStr::from_ptr(ty.name).to_string_lossy();
    let edge_type = catalog.edge_type(&type_name);
    let from_gid = Gid::from((*from).gid);
    let to_gid = Gid::from((*to).gid);
    let gid = storage.allocate_gid();
    match storage.create_edge(tx, gid, from_gid, to_gid, edge_type) {
        Ok(_) => {
            *result = Box::into_raw(Box::new(mgp_edge { gid: gid.as_uint(), graph: g }));
            MgpError::NoError
        }
        Err(_) => {
            *result = ptr::null_mut();
            MgpError::LogicError
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_delete_vertex(g: *mut mgp_graph, v: *mut mgp_vertex) -> MgpError {
    if g.is_null() || v.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*g).storage as *mut Storage);
    let tx = &*(*g).tx;
    let gid = Gid::from((*v).gid);
    match storage.delete_vertex(tx, gid) {
        Ok(()) => MgpError::NoError,
        Err(_) => MgpError::LogicError,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_detach_delete_vertex(g: *mut mgp_graph, v: *mut mgp_vertex) -> MgpError {
    if g.is_null() || v.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*g).storage as *mut Storage);
    let tx = &*(*g).tx;
    let gid = Gid::from((*v).gid);
    let in_edges = storage.vertex_in_edge_gids(gid);
    let out_edges = storage.vertex_out_edge_gids(gid);
    for edge_gid in in_edges.iter().chain(out_edges.iter()) {
        let _ = storage.delete_edge(tx, *edge_gid);
    }
    match storage.delete_vertex(tx, gid) {
        Ok(()) => MgpError::NoError,
        Err(_) => MgpError::LogicError,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_delete_edge(g: *mut mgp_graph, e: *mut mgp_edge) -> MgpError {
    if g.is_null() || e.is_null() { return MgpError::InvalidArgument; }
    let storage = &mut *((*g).storage as *mut Storage);
    let tx = &*(*g).tx;
    let gid = Gid::from((*e).gid);
    match storage.delete_edge(tx, gid) {
        Ok(()) => MgpError::NoError,
        Err(_) => MgpError::LogicError,
    }
}

// ─── Opaque type destructors (remaining) ────────────────────────────────────

macro_rules! noop_destroy {
    ($($name:ident($ty:ty);)*) => {
        $(
            #[no_mangle]
            pub unsafe extern "C" fn $name(_p: *mut $ty) {}
        )*
    };
}

noop_destroy! {
    mgp_list_iterator_destroy(mgp_list_iterator);
    mgp_map_iterator_destroy(mgp_map_iterator);
    mgp_date_destroy(mgp_date);
    mgp_local_time_destroy(mgp_local_time);
    mgp_local_date_time_destroy(mgp_local_date_time);
    mgp_duration_destroy(mgp_duration);
    mgp_zoned_date_time_destroy(mgp_zoned_date_time);
    mgp_point_2d_destroy(mgp_point_2d);
    mgp_point_3d_destroy(mgp_point_3d);
    mgp_enum_destroy(mgp_enum);
    mgp_execution_result_destroy(mgp_execution_result);
    mgp_vector_search_result_destroy(mgp_vector_search_result);
}

// ─── Temporal helpers ───────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_date_now(_memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let days = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() / 86400;
    *result = mgp_value::boxed(ValueInner::Date(days as i64));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_date_get_year(date: *mut mgp_date, result: *mut i64) -> MgpError {
    if date.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let days = (*date).days;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let d = epoch + chrono::Duration::days(days);
    *result = d.year() as i64;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_date_get_month(date: *mut mgp_date, result: *mut i64) -> MgpError {
    if date.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let days = (*date).days;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let d = epoch + chrono::Duration::days(days);
    *result = d.month() as i64;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_date_get_day(date: *mut mgp_date, result: *mut i64) -> MgpError {
    if date.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let days = (*date).days;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let d = epoch + chrono::Duration::days(days);
    *result = d.day() as i64;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_date_timestamp(date: *mut mgp_date, result: *mut i64) -> MgpError {
    if date.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*date).days * 86400;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_now(_memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let micros = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros() as i64;
    *result = mgp_value::boxed(ValueInner::LocalTime(micros % 86400000000));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_get_hour(time: *mut mgp_local_time, result: *mut i64) -> MgpError {
    if time.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*time).micros / 3_600_000_000;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_get_minute(time: *mut mgp_local_time, result: *mut i64) -> MgpError {
    if time.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = ((*time).micros % 3_600_000_000) / 60_000_000;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_get_second(time: *mut mgp_local_time, result: *mut i64) -> MgpError {
    if time.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = ((*time).micros % 60_000_000) / 1_000_000;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_get_microsecond(time: *mut mgp_local_time, result: *mut i64) -> MgpError {
    if time.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*time).micros % 1_000;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_get_millisecond(time: *mut mgp_local_time, result: *mut i64) -> MgpError {
    if time.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = ((*time).micros % 1_000_000) / 1_000;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_timestamp(time: *mut mgp_local_time, result: *mut i64) -> MgpError {
    if time.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*time).micros;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_now(_memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let micros = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros() as i64;
    *result = mgp_value::boxed(ValueInner::LocalDateTime(micros));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_timestamp(dt: *mut mgp_local_date_time, result: *mut i64) -> MgpError {
    if dt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*dt).micros;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_now(_memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let micros = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros() as i64;
    *result = mgp_value::boxed(ValueInner::ZonedDateTime { micros, offset: 0, tz: CString::new("UTC").unwrap() });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_timestamp(zdt: *mut mgp_zoned_date_time, result: *mut i64) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*zdt).micros;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_duration_from_microseconds(micros: i64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(ValueInner::Duration { months: 0, days: 0, micros });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_duration_get_microseconds(dur: *mut mgp_duration, result: *mut i64) -> MgpError {
    if result.is_null() || dur.is_null() { return MgpError::InvalidArgument; }
    *result = (*dur).micros;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_unordered_map_make_empty(memory: *mut mgp_memory, result: *mut *mut mgp_map) -> MgpError {
    mgp_map_make_empty(memory, result)
}

// ─── Temporal arithmetic ────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_date_add_duration(date: *mut mgp_date, dur: *mut mgp_duration, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || date.is_null() || dur.is_null() { return MgpError::InvalidArgument; }
    let d = mgcore::temporal::Date::from_days((*date).days);
    let duration = mgcore::temporal::Duration::new((*dur).months, (*dur).days, (*dur).micros);
    let new_days = d.days_since_epoch() + duration.days + duration.months * 30;
    let new_date = mgp_date { days: new_days };
    *result = mgp_value::boxed(ValueInner::Date(new_date.days));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_date_sub_duration(date: *mut mgp_date, dur: *mut mgp_duration, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || date.is_null() || dur.is_null() { return MgpError::InvalidArgument; }
    let d = mgcore::temporal::Date::from_days((*date).days);
    let duration = mgcore::temporal::Duration::new((*dur).months, (*dur).days, (*dur).micros);
    let new_days = d.days_since_epoch() - duration.days - duration.months * 30;
    *result = mgp_value::boxed(ValueInner::Date(new_days));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_add_duration(lt: *mut mgp_local_time, dur: *mut mgp_duration, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || lt.is_null() || dur.is_null() { return MgpError::InvalidArgument; }
    let new_micros = (*lt).micros + (*dur).micros;
    *result = mgp_value::boxed(ValueInner::LocalTime(new_micros));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_sub_duration(lt: *mut mgp_local_time, dur: *mut mgp_duration, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || lt.is_null() || dur.is_null() { return MgpError::InvalidArgument; }
    let new_micros = (*lt).micros - (*dur).micros;
    *result = mgp_value::boxed(ValueInner::LocalTime(new_micros));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_add_duration(dt: *mut mgp_local_date_time, dur: *mut mgp_duration, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || dt.is_null() || dur.is_null() { return MgpError::InvalidArgument; }
    let new_micros = (*dt).micros + (*dur).micros + (*dur).days * 86_400_000_000i64 + (*dur).months * 30 * 86_400_000_000i64;
    *result = mgp_value::boxed(ValueInner::LocalDateTime(new_micros));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_sub_duration(dt: *mut mgp_local_date_time, dur: *mut mgp_duration, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || dt.is_null() || dur.is_null() { return MgpError::InvalidArgument; }
    let new_micros = (*dt).micros - (*dur).micros - (*dur).days * 86_400_000_000i64 - (*dur).months * 30 * 86_400_000_000i64;
    *result = mgp_value::boxed(ValueInner::LocalDateTime(new_micros));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_add_duration(zdt: *mut mgp_zoned_date_time, dur: *mut mgp_duration, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || zdt.is_null() || dur.is_null() { return MgpError::InvalidArgument; }
    let new_micros = (*zdt).micros + (*dur).micros + (*dur).days * 86_400_000_000i64 + (*dur).months * 30 * 86_400_000_000i64;
    let cloned_tz = (*zdt).tz.clone();
    *result = mgp_value::boxed(ValueInner::ZonedDateTime {
        micros: new_micros,
        offset: (*zdt).offset,
        tz: cloned_tz,
    });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_sub_duration(zdt: *mut mgp_zoned_date_time, dur: *mut mgp_duration, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || zdt.is_null() || dur.is_null() { return MgpError::InvalidArgument; }
    let new_micros = (*zdt).micros - (*dur).micros - (*dur).days * 86_400_000_000i64 - (*dur).months * 30 * 86_400_000_000i64;
    let cloned_tz = (*zdt).tz.clone();
    *result = mgp_value::boxed(ValueInner::ZonedDateTime {
        micros: new_micros,
        offset: (*zdt).offset,
        tz: cloned_tz,
    });
    MgpError::NoError
}

// ─── Temporal copy/equal/diff ───────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_date_copy(src: *mut mgp_date, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if src.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(ValueInner::Date((*src).days));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_copy(src: *mut mgp_local_time, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if src.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(ValueInner::LocalTime((*src).micros));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_copy(src: *mut mgp_local_date_time, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if src.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(ValueInner::LocalDateTime((*src).micros));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_duration_copy(src: *mut mgp_duration, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if src.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(ValueInner::Duration {
        months: (*src).months,
        days: (*src).days,
        micros: (*src).micros,
    });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_copy(src: *mut mgp_zoned_date_time, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if src.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let cloned_tz = (*src).tz.clone();
    *result = mgp_value::boxed(ValueInner::ZonedDateTime {
        micros: (*src).micros,
        offset: (*src).offset,
        tz: cloned_tz,
    });
    MgpError::NoError
}

macro_rules! temporal_equal {
    ($fn_name:ident, $ty:ty, $field:ident) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fn_name(a: *mut $ty, b: *mut $ty, result: *mut i32) -> MgpError {
            if result.is_null() || a.is_null() || b.is_null() { return MgpError::InvalidArgument; }
            *result = if (*a).$field == (*b).$field { 1 } else { 0 };
            MgpError::NoError
        }
    };
}

temporal_equal!(mgp_date_equal, mgp_date, days);
temporal_equal!(mgp_local_time_equal, mgp_local_time, micros);
temporal_equal!(mgp_local_date_time_equal, mgp_local_date_time, micros);
temporal_equal!(mgp_duration_equal, mgp_duration, micros);

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_equal(a: *mut mgp_zoned_date_time, b: *mut mgp_zoned_date_time, result: *mut i32) -> MgpError {
    if result.is_null() || a.is_null() || b.is_null() { return MgpError::InvalidArgument; }
    *result = if (*a).micros == (*b).micros && (*a).offset == (*b).offset { 1 } else { 0 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_duration_add(a: *mut mgp_duration, b: *mut mgp_duration, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || a.is_null() || b.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(ValueInner::Duration {
        months: (*a).months + (*b).months,
        days: (*a).days + (*b).days,
        micros: (*a).micros + (*b).micros,
    });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_duration_sub(a: *mut mgp_duration, b: *mut mgp_duration, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || a.is_null() || b.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(ValueInner::Duration {
        months: (*a).months - (*b).months,
        days: (*a).days - (*b).days,
        micros: (*a).micros - (*b).micros,
    });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_duration_neg(dur: *mut mgp_duration, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() || dur.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(ValueInner::Duration {
        months: -(*dur).months,
        days: -(*dur).days,
        micros: -(*dur).micros,
    });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_date_diff(a: *mut mgp_date, b: *mut mgp_date, result: *mut *mut mgp_duration) -> MgpError {
    if result.is_null() || a.is_null() || b.is_null() { return MgpError::InvalidArgument; }
    let days = (*a).days - (*b).days;
    *result = Box::into_raw(Box::new(mgp_duration { months: 0, days, micros: 0 }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_diff(a: *mut mgp_local_time, b: *mut mgp_local_time, result: *mut *mut mgp_duration) -> MgpError {
    if result.is_null() || a.is_null() || b.is_null() { return MgpError::InvalidArgument; }
    let micros = (*a).micros - (*b).micros;
    *result = Box::into_raw(Box::new(mgp_duration { months: 0, days: 0, micros }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_diff(a: *mut mgp_local_date_time, b: *mut mgp_local_date_time, result: *mut *mut mgp_duration) -> MgpError {
    if result.is_null() || a.is_null() || b.is_null() { return MgpError::InvalidArgument; }
    let micros = (*a).micros - (*b).micros;
    *result = Box::into_raw(Box::new(mgp_duration { months: 0, days: 0, micros }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_diff(a: *mut mgp_zoned_date_time, b: *mut mgp_zoned_date_time, result: *mut *mut mgp_duration) -> MgpError {
    if result.is_null() || a.is_null() || b.is_null() { return MgpError::InvalidArgument; }
    let micros = (*a).micros - (*b).micros;
    *result = Box::into_raw(Box::new(mgp_duration { months: 0, days: 0, micros }));
    MgpError::NoError
}

// ─── Temporal from_parameters / from_string ─────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_date_from_parameters(year: i64, month: i64, day: i64, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    if let Some(d) = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days = d.signed_duration_since(epoch).num_days();
        mgp_value_make_date(days, memory, result)
    } else {
        *result = ptr::null_mut();
        MgpError::InvalidArgument
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_date_from_string(s: *const c_char, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let s = unsafe { CStr::from_ptr(s) }.to_str().unwrap_or("");
    if let Ok(d) = s.parse::<chrono::NaiveDate>() {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days = d.signed_duration_since(epoch).num_days();
        mgp_value_make_date(days, memory, result)
    } else {
        *result = ptr::null_mut();
        MgpError::InvalidArgument
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_from_parameters(hour: i64, minute: i64, second: i64, microsecond: i64, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    if let Some(t) = chrono::NaiveTime::from_hms_micro_opt(hour as u32, minute as u32, second as u32, microsecond as u32) {
        let us = t.hour() as i64 * 3600_000_000 + t.minute() as i64 * 60_000_000 + t.second() as i64 * 1_000_000 + t.nanosecond() as i64 / 1000;
        mgp_value_make_local_time(us, memory, result)
    } else {
        *result = ptr::null_mut();
        MgpError::InvalidArgument
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_time_from_string(s: *const c_char, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let s = unsafe { CStr::from_ptr(s) }.to_str().unwrap_or("");
    if let Ok(t) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
        let us = t.hour() as i64 * 3600_000_000 + t.minute() as i64 * 60_000_000 + t.second() as i64 * 1_000_000 + t.nanosecond() as i64 / 1000;
        mgp_value_make_local_time(us, memory, result)
    } else {
        *result = ptr::null_mut();
        MgpError::InvalidArgument
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_from_parameters(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, microsecond: i64, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    if let Some(d) = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
        if let Some(t) = d.and_hms_micro_opt(hour as u32, minute as u32, second as u32, microsecond as u32) {
            let us = t.and_utc().timestamp_micros();
            mgp_value_make_local_date_time(us, memory, result)
        } else {
            *result = ptr::null_mut();
            MgpError::InvalidArgument
        }
    } else {
        *result = ptr::null_mut();
        MgpError::InvalidArgument
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_from_string(s: *const c_char, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let s = unsafe { CStr::from_ptr(s) }.to_str().unwrap_or("");
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        let us = dt.and_utc().timestamp_micros();
        mgp_value_make_local_date_time(us, memory, result)
    } else {
        *result = ptr::null_mut();
        MgpError::InvalidArgument
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_from_parameters(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, microsecond: i64, offset: i64, tz: *const c_char, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    if let Some(d) = chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
        if let Some(t) = d.and_hms_micro_opt(hour as u32, minute as u32, second as u32, microsecond as u32) {
            let us = t.and_utc().timestamp_micros();
            let tz_name = if tz.is_null() { "" } else { unsafe { CStr::from_ptr(tz) }.to_str().unwrap_or("") };
            mgp_value_make_zoned_date_time(us, offset as i16, tz_name.as_ptr() as *const c_char, memory, result)
        } else {
            *result = ptr::null_mut();
            MgpError::InvalidArgument
        }
    } else {
        *result = ptr::null_mut();
        MgpError::InvalidArgument
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_from_string(s: *const c_char, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let s = unsafe { CStr::from_ptr(s) }.to_str().unwrap_or("");
    // Try RFC3339 / ISO8601 format
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        let us = dt.timestamp_micros();
        let offset = dt.offset().local_minus_utc() as i16;
        let tz = "";
        mgp_value_make_zoned_date_time(us, offset, tz.as_ptr() as *const c_char, memory, result)
    } else {
        *result = ptr::null_mut();
        MgpError::InvalidArgument
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_duration_from_parameters(months: i64, days: i64, microseconds: i64, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    mgp_value_make_duration(months, days, microseconds, memory, result)
}

fn parse_duration_str(s: &str) -> Option<(i64, i64, i64)> {
    // ISO 8601 duration: P[n]Y[n]M[n]DT[n]H[n]M[n]S
    let s = s.trim();
    if !s.starts_with('P') { return None; }
    let s = &s[1..];
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut microseconds: i64 = 0;
    let mut num_str = String::new();
    let mut in_time = false;
    for ch in s.chars() {
        match ch {
            'T' => { in_time = true; }
            '0'..='9' | '-' | '+' | '.' => { num_str.push(ch); }
            'Y' => { months += num_str.parse::<i64>().ok()? * 12; num_str.clear(); }
            'M' if in_time => {
                microseconds += num_str.parse::<i64>().ok()? * 60_000_000;
                num_str.clear();
            }
            'M' => { months += num_str.parse::<i64>().ok()?; num_str.clear(); }
            'D' => { days += num_str.parse::<i64>().ok()?; num_str.clear(); }
            'H' => { microseconds += num_str.parse::<i64>().ok()? * 3600_000_000; num_str.clear(); }
            'S' => {
                if num_str.contains('.') {
                    let f: f64 = num_str.parse().ok()?;
                    microseconds += (f * 1_000_000.0) as i64;
                } else {
                    microseconds += num_str.parse::<i64>().ok()? * 1_000_000;
                }
                num_str.clear();
            }
            _ => return None,
        }
    }
    if !num_str.is_empty() { return None; }
    Some((months, days, microseconds))
}

#[no_mangle]
pub unsafe extern "C" fn mgp_duration_from_string(s: *const c_char, memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let s = unsafe { CStr::from_ptr(s) }.to_str().unwrap_or("");
    if let Some((months, days, micros)) = parse_duration_str(s) {
        mgp_value_make_duration(months, days, micros, memory, result)
    } else {
        *result = ptr::null_mut();
        MgpError::InvalidArgument
    }
}

// ─── Temporal getters ───────────────────────────────────────────────────────

/// Extract date component from local_date_time micros (epoch-based).
unsafe fn ldt_date_parts(dt: *mut mgp_local_date_time) -> (i64, i64, i64) {
    let days = (*dt).micros / 86_400_000_000;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let d = epoch + chrono::Duration::days(days);
    (d.year() as i64, d.month() as i64, d.day() as i64)
}

/// Extract time-of-day component from micros since midnight.
fn time_parts_from_micros(micros: i64) -> (i64, i64, i64, i64, i64) {
    let hour = micros / 3_600_000_000;
    let minute = (micros % 3_600_000_000) / 60_000_000;
    let second = (micros % 60_000_000) / 1_000_000;
    let millisecond = (micros % 1_000_000) / 1_000;
    let microsecond = micros % 1_000;
    (hour, minute, second, millisecond, microsecond)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_get_year(dt: *mut mgp_local_date_time, result: *mut i64) -> MgpError {
    if dt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = ldt_date_parts(dt).0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_get_month(dt: *mut mgp_local_date_time, result: *mut i64) -> MgpError {
    if dt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = ldt_date_parts(dt).1;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_get_day(dt: *mut mgp_local_date_time, result: *mut i64) -> MgpError {
    if dt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = ldt_date_parts(dt).2;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_get_hour(dt: *mut mgp_local_date_time, result: *mut i64) -> MgpError {
    if dt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let time_micros = (*dt).micros % 86_400_000_000;
    *result = time_parts_from_micros(time_micros).0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_get_minute(dt: *mut mgp_local_date_time, result: *mut i64) -> MgpError {
    if dt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let time_micros = (*dt).micros % 86_400_000_000;
    *result = time_parts_from_micros(time_micros).1;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_get_second(dt: *mut mgp_local_date_time, result: *mut i64) -> MgpError {
    if dt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let time_micros = (*dt).micros % 86_400_000_000;
    *result = time_parts_from_micros(time_micros).2;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_get_millisecond(dt: *mut mgp_local_date_time, result: *mut i64) -> MgpError {
    if dt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let time_micros = (*dt).micros % 86_400_000_000;
    *result = time_parts_from_micros(time_micros).3;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_local_date_time_get_microsecond(dt: *mut mgp_local_date_time, result: *mut i64) -> MgpError {
    if dt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let time_micros = (*dt).micros % 86_400_000_000;
    *result = time_parts_from_micros(time_micros).4;
    MgpError::NoError
}

// ─── ZonedDateTime getters ──────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_get_year(zdt: *mut mgp_zoned_date_time, result: *mut i64) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let days = (*zdt).micros / 86_400_000_000;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let d = epoch + chrono::Duration::days(days);
    *result = d.year() as i64;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_get_month(zdt: *mut mgp_zoned_date_time, result: *mut i64) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let days = (*zdt).micros / 86_400_000_000;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let d = epoch + chrono::Duration::days(days);
    *result = d.month() as i64;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_get_day(zdt: *mut mgp_zoned_date_time, result: *mut i64) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let days = (*zdt).micros / 86_400_000_000;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let d = epoch + chrono::Duration::days(days);
    *result = d.day() as i64;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_get_hour(zdt: *mut mgp_zoned_date_time, result: *mut i64) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let time_micros = (*zdt).micros % 86_400_000_000;
    *result = time_parts_from_micros(time_micros).0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_get_minute(zdt: *mut mgp_zoned_date_time, result: *mut i64) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let time_micros = (*zdt).micros % 86_400_000_000;
    *result = time_parts_from_micros(time_micros).1;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_get_second(zdt: *mut mgp_zoned_date_time, result: *mut i64) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let time_micros = (*zdt).micros % 86_400_000_000;
    *result = time_parts_from_micros(time_micros).2;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_get_millisecond(zdt: *mut mgp_zoned_date_time, result: *mut i64) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let time_micros = (*zdt).micros % 86_400_000_000;
    *result = time_parts_from_micros(time_micros).3;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_get_microsecond(zdt: *mut mgp_zoned_date_time, result: *mut i64) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let time_micros = (*zdt).micros % 86_400_000_000;
    *result = time_parts_from_micros(time_micros).4;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_get_offset(zdt: *mut mgp_zoned_date_time, result: *mut i64) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*zdt).offset as i64;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_zoned_date_time_get_timezone(zdt: *mut mgp_zoned_date_time, result: *mut *const c_char) -> MgpError {
    if zdt.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*zdt).tz.as_ptr();
    MgpError::NoError
}

// ─── Point operations ───────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_point_2d_make(crs: u16, x: f64, y: f64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(ValueInner::Point2D { crs, x, y });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_2d_get_x(p: *mut mgp_point_2d, result: *mut f64) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*p).x;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_2d_get_y(p: *mut mgp_point_2d, result: *mut f64) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*p).y;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_2d_get_srid(p: *mut mgp_point_2d, result: *mut u16) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*p).crs;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_2d_copy(p: *mut mgp_point_2d, _memory: *mut mgp_memory, result: *mut *mut mgp_point_2d) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_point_2d { crs: (*p).crs, x: (*p).x, y: (*p).y }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_2d_equal(p1: *mut mgp_point_2d, p2: *mut mgp_point_2d, result: *mut i32) -> MgpError {
    if p1.is_null() || p2.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = if (*p1).crs == (*p2).crs && (*p1).x == (*p2).x && (*p1).y == (*p2).y { 1 } else { 0 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_3d_make(crs: u16, x: f64, y: f64, z: f64, _memory: *mut mgp_memory, result: *mut *mut mgp_value) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_value::boxed(ValueInner::Point3D { crs, x, y, z });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_3d_get_x(p: *mut mgp_point_3d, result: *mut f64) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*p).x;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_3d_get_y(p: *mut mgp_point_3d, result: *mut f64) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*p).y;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_3d_get_z(p: *mut mgp_point_3d, result: *mut f64) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*p).z;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_3d_get_srid(p: *mut mgp_point_3d, result: *mut u16) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*p).crs;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_3d_copy(p: *mut mgp_point_3d, _memory: *mut mgp_memory, result: *mut *mut mgp_point_3d) -> MgpError {
    if p.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_point_3d { crs: (*p).crs, x: (*p).x, y: (*p).y, z: (*p).z }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_point_3d_equal(p1: *mut mgp_point_3d, p2: *mut mgp_point_3d, result: *mut i32) -> MgpError {
    if p1.is_null() || p2.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = if (*p1).crs == (*p2).crs && (*p1).x == (*p2).x && (*p1).y == (*p2).y && (*p1).z == (*p2).z { 1 } else { 0 };
    MgpError::NoError
}

// ─── Enum operations ────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_enum_make(enum_type: *const c_char, value: *const c_char, _memory: *mut mgp_memory, result: *mut *mut mgp_enum) -> MgpError {
    if enum_type.is_null() || value.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let ty = CString::new(CStr::from_ptr(enum_type).to_bytes()).unwrap_or_default();
    let val = CString::new(CStr::from_ptr(value).to_bytes()).unwrap_or_default();
    *result = Box::into_raw(Box::new(mgp_enum { ty, val }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_enum_get_type_name(e: *mut mgp_enum, result: *mut *const c_char) -> MgpError {
    if e.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*e).ty.as_ptr();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_enum_get_value_name(e: *mut mgp_enum, result: *mut *const c_char) -> MgpError {
    if e.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = (*e).val.as_ptr();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_enum_copy(e: *mut mgp_enum, _memory: *mut mgp_memory, result: *mut *mut mgp_enum) -> MgpError {
    if e.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_enum {
        ty: CString::new((*e).ty.to_bytes()).unwrap_or_default(),
        val: CString::new((*e).val.to_bytes()).unwrap_or_default(),
    }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_enum_equal(e1: *mut mgp_enum, e2: *mut mgp_enum, result: *mut i32) -> MgpError {
    if e1.is_null() || e2.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = if (*e1).ty == (*e2).ty && (*e1).val == (*e2).val { 1 } else { 0 };
    MgpError::NoError
}

// ─── Result / Record API ────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_result_set_error_msg(result: *mut mgp_result, msg: *const c_char) -> MgpError {
    if result.is_null() || msg.is_null() { return MgpError::InvalidArgument; }
    let res = &mut *result;
    let cmsg = CString::new(CStr::from_ptr(msg).to_bytes()).unwrap_or_default();
    res.error = Some(cmsg);
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_result_reserve(result: *mut mgp_result, n: usize) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    (*result).reserved = n;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_result_new_record(result: *mut mgp_result, out: *mut *mut mgp_result_record) -> MgpError {
    if result.is_null() || out.is_null() { return MgpError::InvalidArgument; }
    let record = Box::into_raw(Box::new(mgp_result_record { fields: HashMap::new() }));
    (*result).records.push(record);
    *out = record;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_result_record_insert(record: *mut mgp_result_record, name: *const c_char, val: *mut mgp_value) -> MgpError {
    if record.is_null() || name.is_null() || val.is_null() { return MgpError::InvalidArgument; }
    let key = CString::new(CStr::from_ptr(name).to_bytes()).unwrap_or_default();
    (*record).fields.insert(key, val);
    MgpError::NoError
}

// ─── Func result API ────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_func_result_set_error_msg(result: *mut mgp_func_result, msg: *const c_char, _memory: *mut mgp_memory) -> MgpError {
    if result.is_null() || msg.is_null() { return MgpError::InvalidArgument; }
    let cmsg = CString::new(CStr::from_ptr(msg).to_bytes()).unwrap_or_default();
    (*result).error = Some(cmsg);
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_func_result_set_value(result: *mut mgp_func_result, val: *mut mgp_value, _memory: *mut mgp_memory) -> MgpError {
    if result.is_null() || val.is_null() { return MgpError::InvalidArgument; }
    (*result).value = Some(val);
    MgpError::NoError
}

// ─── Module / Procedure / Function registration ─────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_module_add_read_procedure(module: *mut mgp_module, name: *const c_char, cb: MgpProcCb, result: *mut *mut mgp_proc) -> MgpError {
    if module.is_null() || name.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let cname = CString::new(CStr::from_ptr(name).to_bytes()).unwrap_or_default();
    let proc = Box::into_raw(Box::new(mgp_proc {
        name: cname,
        callback: cb,
        args: Vec::new(),
        opt_args: Vec::new(),
        results: Vec::new(),
        is_write: false,
    }));
    (*module).procs.push(proc);
    *result = proc;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_module_add_write_procedure(module: *mut mgp_module, name: *const c_char, cb: MgpProcCb, result: *mut *mut mgp_proc) -> MgpError {
    if module.is_null() || name.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let cname = CString::new(CStr::from_ptr(name).to_bytes()).unwrap_or_default();
    let proc = Box::into_raw(Box::new(mgp_proc {
        name: cname,
        callback: cb,
        args: Vec::new(),
        opt_args: Vec::new(),
        results: Vec::new(),
        is_write: true,
    }));
    (*module).procs.push(proc);
    *result = proc;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_module_add_transformation(module: *mut mgp_module, name: *const c_char, cb: MgpProcCb, result: *mut *mut mgp_proc) -> MgpError {
    // Transformations are treated as read procedures in this implementation
    mgp_module_add_read_procedure(module, name, cb, result)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_module_add_batch_read_procedure(module: *mut mgp_module, name: *const c_char, cb: MgpProcCb, _initializer: MgpProcCb, _cleanup: MgpProcCb, result: *mut *mut mgp_proc) -> MgpError {
    // Batch read procedures map to regular read procedures
    mgp_module_add_read_procedure(module, name, cb, result)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_module_add_batch_write_procedure(module: *mut mgp_module, name: *const c_char, cb: MgpProcCb, _initializer: MgpProcCb, _cleanup: MgpProcCb, result: *mut *mut mgp_proc) -> MgpError {
    // Batch write procedures map to regular write procedures
    mgp_module_add_write_procedure(module, name, cb, result)
}

#[no_mangle]
pub unsafe extern "C" fn mgp_module_add_function(module: *mut mgp_module, name: *const c_char, cb: MgpFuncCb, result: *mut *mut mgp_func) -> MgpError {
    if module.is_null() || name.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let cname = CString::new(CStr::from_ptr(name).to_bytes()).unwrap_or_default();
    let func = Box::into_raw(Box::new(mgp_func {
        name: cname,
        callback: cb,
        args: Vec::new(),
        opt_args: Vec::new(),
    }));
    (*module).funcs.push(func);
    *result = func;
    MgpError::NoError
}

// ─── Procedure arg/result API ───────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_proc_add_arg(proc_: *mut mgp_proc, name: *const c_char, ty: *mut mgp_type) -> MgpError {
    if proc_.is_null() || name.is_null() || ty.is_null() { return MgpError::InvalidArgument; }
    let cname = CString::new(CStr::from_ptr(name).to_bytes()).unwrap_or_default();
    (*proc_).args.push(ArgDesc { name: cname, ty, default_value: None });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_proc_add_opt_arg(proc_: *mut mgp_proc, name: *const c_char, ty: *mut mgp_type, def: *mut mgp_value) -> MgpError {
    if proc_.is_null() || name.is_null() || ty.is_null() { return MgpError::InvalidArgument; }
    let cname = CString::new(CStr::from_ptr(name).to_bytes()).unwrap_or_default();
    (*proc_).opt_args.push(ArgDesc { name: cname, ty, default_value: Some(def) });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_proc_add_result(proc_: *mut mgp_proc, name: *const c_char, ty: *mut mgp_type) -> MgpError {
    if proc_.is_null() || name.is_null() || ty.is_null() { return MgpError::InvalidArgument; }
    let cname = CString::new(CStr::from_ptr(name).to_bytes()).unwrap_or_default();
    (*proc_).results.push(ResultField { name: cname, ty, deprecated: false });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_proc_add_deprecated_result(proc_: *mut mgp_proc, name: *const c_char, ty: *mut mgp_type) -> MgpError {
    if proc_.is_null() || name.is_null() || ty.is_null() { return MgpError::InvalidArgument; }
    let cname = CString::new(CStr::from_ptr(name).to_bytes()).unwrap_or_default();
    (*proc_).results.push(ResultField { name: cname, ty, deprecated: true });
    MgpError::NoError
}

// ─── Function arg API ───────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_func_add_arg(func: *mut mgp_func, name: *const c_char, ty: *mut mgp_type) -> MgpError {
    if func.is_null() || name.is_null() || ty.is_null() { return MgpError::InvalidArgument; }
    let cname = CString::new(CStr::from_ptr(name).to_bytes()).unwrap_or_default();
    (*func).args.push(ArgDesc { name: cname, ty, default_value: None });
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_func_add_opt_arg(func: *mut mgp_func, name: *const c_char, ty: *mut mgp_type, def: *mut mgp_value) -> MgpError {
    if func.is_null() || name.is_null() || ty.is_null() { return MgpError::InvalidArgument; }
    let cname = CString::new(CStr::from_ptr(name).to_bytes()).unwrap_or_default();
    (*func).opt_args.push(ArgDesc { name: cname, ty, default_value: Some(def) });
    MgpError::NoError
}

// ─── Type descriptors ───────────────────────────────────────────────────────

macro_rules! type_fn {
    ($fn_name:ident, $static:ident) => {
        #[no_mangle]
        pub unsafe extern "C" fn $fn_name(result: *mut *mut mgp_type) -> MgpError {
            if result.is_null() { return MgpError::InvalidArgument; }
            *result = &$static as *const mgp_type as *mut mgp_type;
            MgpError::NoError
        }
    };
}

type_fn!(mgp_type_any, TYPE_ANY);
type_fn!(mgp_type_bool, TYPE_BOOL);
type_fn!(mgp_type_int, TYPE_INT);
type_fn!(mgp_type_float, TYPE_FLOAT);
type_fn!(mgp_type_number, TYPE_NUMBER);
type_fn!(mgp_type_string, TYPE_STRING);
type_fn!(mgp_type_map, TYPE_MAP);
type_fn!(mgp_type_node, TYPE_NODE);
type_fn!(mgp_type_relationship, TYPE_RELATIONSHIP);
type_fn!(mgp_type_path, TYPE_PATH);
type_fn!(mgp_type_date, TYPE_DATE);
type_fn!(mgp_type_local_time, TYPE_LOCAL_TIME);
type_fn!(mgp_type_local_date_time, TYPE_LOCAL_DATE_TIME);
type_fn!(mgp_type_duration, TYPE_DURATION);
type_fn!(mgp_type_zoned_date_time, TYPE_ZONED_DATE_TIME);
type_fn!(mgp_type_enum, TYPE_ENUM);
type_fn!(mgp_type_point_2d, TYPE_POINT_2D);
type_fn!(mgp_type_point_3d, TYPE_POINT_3D);

#[no_mangle]
pub unsafe extern "C" fn mgp_type_list(elem: *mut mgp_type, result: *mut *mut mgp_type) -> MgpError {
    if elem.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_type { tag: MgpTypeTag::List, elem }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_type_nullable(ty: *mut mgp_type, result: *mut *mut mgp_type) -> MgpError {
    if ty.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    *result = Box::into_raw(Box::new(mgp_type { tag: MgpTypeTag::Nullable, elem: ty }));
    MgpError::NoError
}

// ─── Index management ──────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_create_label_index(
    g: *mut mgp_graph,
    label: *const c_char,
    result: *mut c_int,
) -> MgpError {
    if g.is_null() || label.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &mut *((*g).storage as *mut Storage);
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let created = storage.create_label_index(label_id);
    *result = if created { 1 } else { 0 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_drop_label_index(
    g: *mut mgp_graph,
    label: *const c_char,
    result: *mut c_int,
) -> MgpError {
    if g.is_null() || label.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &mut *((*g).storage as *mut Storage);
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let dropped = storage.drop_label_index(label_id);
    *result = if dropped { 1 } else { 0 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_create_label_property_index(
    g: *mut mgp_graph,
    label: *const c_char,
    property: *const c_char,
    result: *mut c_int,
) -> MgpError {
    if g.is_null() || label.is_null() || property.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &mut *((*g).storage as *mut Storage);
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let prop_name = CStr::from_ptr(property).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let prop_id = catalog.property(&prop_name);
    let created = storage.create_label_property_index(label_id, prop_id);
    *result = if created { 1 } else { 0 };
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_drop_label_property_index(
    g: *mut mgp_graph,
    label: *const c_char,
    property: *const c_char,
    result: *mut c_int,
) -> MgpError {
    if g.is_null() || label.is_null() || property.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &mut *((*g).storage as *mut Storage);
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let prop_name = CStr::from_ptr(property).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let prop_id = catalog.property(&prop_name);
    let dropped = storage.drop_label_property_index(label_id, prop_id);
    *result = if dropped { 1 } else { 0 };
    MgpError::NoError
}

// ─── Constraint management ─────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_create_existence_constraint(
    g: *mut mgp_graph,
    label: *const c_char,
    property: *const c_char,
    result: *mut c_int,
) -> MgpError {
    if g.is_null() || label.is_null() || property.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &mut *((*g).storage as *mut Storage);
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let prop_name = CStr::from_ptr(property).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let prop_id = catalog.property(&prop_name);
    if storage.constraints.has_existence_constraint(label_id, prop_id) {
        *result = 0;
    } else {
        storage.constraints.add_existence_constraint(label_id, prop_id);
        *result = 1;
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_drop_existence_constraint(
    g: *mut mgp_graph,
    label: *const c_char,
    property: *const c_char,
    result: *mut c_int,
) -> MgpError {
    if g.is_null() || label.is_null() || property.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &mut *((*g).storage as *mut Storage);
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let prop_name = CStr::from_ptr(property).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let prop_id = catalog.property(&prop_name);
    if storage.constraints.has_existence_constraint(label_id, prop_id) {
        storage.constraints.remove_existence_constraint(label_id, prop_id);
        *result = 1;
    } else {
        *result = 0;
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_create_unique_constraint(
    g: *mut mgp_graph,
    label: *const c_char,
    properties: *const mgp_list,
    result: *mut c_int,
) -> MgpError {
    if g.is_null() || label.is_null() || properties.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &mut *((*g).storage as *mut Storage);
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let label_id = catalog.label(&label_name);

    // Extract property names from list
    let mut prop_ids = Vec::new();
    let list = &*properties;
    for i in 0..list.items.len() {
        let item = list.items[i];
        if item.is_null() {
            return MgpError::InvalidArgument;
        }
        let val = &*item;
        match val.inner {
            ValueInner::String(ref s) => {
                prop_ids.push(catalog.property(s.to_str().unwrap_or("")));
            }
            _ => return MgpError::InvalidArgument,
        }
    }

    if storage.constraints.has_unique_constraint(label_id, &prop_ids) {
        *result = 0;
    } else {
        storage.constraints.add_unique_constraint(label_id, prop_ids);
        *result = 1;
    }
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_drop_unique_constraint(
    g: *mut mgp_graph,
    label: *const c_char,
    properties: *const mgp_list,
    result: *mut c_int,
) -> MgpError {
    if g.is_null() || label.is_null() || properties.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &mut *((*g).storage as *mut Storage);
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let label_id = catalog.label(&label_name);

    let mut prop_ids = Vec::new();
    let list = &*properties;
    for i in 0..list.items.len() {
        let item = list.items[i];
        if item.is_null() {
            return MgpError::InvalidArgument;
        }
        let val = &*item;
        match val.inner {
            ValueInner::String(ref s) => {
                prop_ids.push(catalog.property(s.to_str().unwrap_or("")));
            }
            _ => return MgpError::InvalidArgument,
        }
    }

    if storage.constraints.has_unique_constraint(label_id, &prop_ids) {
        storage.constraints.remove_unique_constraint(label_id, &prop_ids);
        *result = 1;
    } else {
        *result = 0;
    }
    MgpError::NoError
}

// ─── Text / vector index ──────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_create_text_index(
    g: *mut mgp_graph,
    label: *const c_char,
    props: *const mgp_list,
) -> MgpError {
    if g.is_null() || label.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let label_id = catalog.label(&label_name);

    // Parse property names from props list
    let prop_names: Vec<String> = if props.is_null() {
        vec!["value".into()]
    } else {
        let items = &(*props).items;
        if items.is_empty() {
            vec!["value".into()]
        } else {
            items.iter().filter_map(|item| {
                match &(**item).inner {
                    ValueInner::String(s) => Some(s.to_string_lossy().into_owned()),
                    _ => None,
                }
            }).collect()
        }
    };

    // Create a Tantivy text index in a temp directory. In production, this
    // should use a configurable data path.
    let path = format!("/tmp/mg_text_idx_{}", label_name);
    let _ = std::fs::remove_dir_all(&path);
    match mgstorage::text_index::TextIndex::create(&path, &prop_names) {
        Ok(index) => {
            let mut indexed_props = Vec::new();
            for name in &prop_names {
                let pid = catalog.property(name);
                indexed_props.push((pid, name.clone()));
            }
            let entry = mgstorage::storage::TextIndexEntry::new(std::sync::Arc::new(index), indexed_props.clone());
            storage.text_indices.write().unwrap().insert(label_id, entry);

            // Index existing vertices with this label
            let all = storage.all_vertices();
            let text_indices = storage.text_indices.read().unwrap();
            let entry = text_indices.get(&label_id).unwrap();
            for (gid, labels, props) in all {
                if labels.contains(&label_id) {
                    let mut text_values = Vec::new();
                    for (pid, field_name) in &indexed_props {
                        let prop_val = props.get(*pid);
                        if !prop_val.is_null() {
                            let s = match prop_val {
                                mgcore::property_value::PropertyValue::String(s) => s.clone(),
                                mgcore::property_value::PropertyValue::Int(n) => n.to_string(),
                                mgcore::property_value::PropertyValue::Double(n) => n.to_string(),
                                mgcore::property_value::PropertyValue::Bool(b) => b.to_string(),
                                _ => continue,
                            };
                            text_values.push((field_name.clone(), s));
                        }
                    }
                    let _ = entry.index.index_vertex(gid, &text_values);
                }
            }
            drop(text_indices);

            MgpError::NoError
        }
        Err(_) => MgpError::SerializationError,
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_drop_text_index(
    g: *mut mgp_graph,
    label: *const c_char,
) -> MgpError {
    if g.is_null() || label.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let path = format!("/tmp/mg_text_idx_{}", label_name);
    let _ = std::fs::remove_dir_all(&path);
    storage.text_indices.write().unwrap().remove(&label_id);
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_create_vector_index(
    g: *mut mgp_graph,
    label: *const c_char,
    props: *const mgp_list,
    dim: usize,
    metric: *const c_char,
) -> MgpError {
    if g.is_null() || label.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let dimension = if dim > 0 { dim } else { 128 };
    let distance = if metric.is_null() {
        mgvector::Distance::Cosine
    } else {
        match CStr::from_ptr(metric).to_string_lossy().as_ref() {
            "euclidean" | "l2" => mgvector::Distance::Euclidean,
            "dot" | "inner_product" => mgvector::Distance::Dot,
            _ => mgvector::Distance::Cosine,
        }
    };

    // Parse property name from props list (first element must be a string)
    let prop_name = if props.is_null() {
        return MgpError::InvalidArgument;
    } else {
        let items = &(*props).items;
        if items.is_empty() {
            return MgpError::InvalidArgument;
        }
        match &(*items[0]).inner {
            ValueInner::String(s) => s.to_string_lossy().into_owned(),
            _ => return MgpError::InvalidArgument,
        }
    };
    let property_id = catalog.property(&prop_name);

    let index = std::sync::Arc::new(std::sync::RwLock::new(
        mgvector::HnswIndex::new(dimension, distance, mgvector::HnswConfig::default())
    ));
    let entry = mgstorage::storage::VectorIndexEntry::new(
        index.clone(),
        property_id,
        dimension,
        distance,
    );

    // Backfill: scan existing vertices with this label and batch-insert their vectors.
    let all_vertices = storage.all_vertices();
    let mut vectors = Vec::with_capacity(all_vertices.len());
    for (gid, labels, properties) in all_vertices {
        if !labels.contains(&label_id) {
            continue;
        }
        let prop = properties.get(property_id);
        if let Some(vec) = mgstorage::storage::property_value_to_f32_vec(prop) {
            if vec.len() == dimension {
                vectors.push((vec, gid));
            }
        }
    }

    if !vectors.is_empty() {
        let mut idx = index.write().unwrap();
        let id_map = idx.insert_batch_with_ids(&vectors);
        drop(idx);

        let mut gid_map = entry.gid_to_node.write().unwrap();
        for (_, gid) in &vectors {
            if let Some(node_id) = id_map.get(gid) {
                gid_map.insert(*gid, *node_id);
            }
        }
    }

    storage.vector_indices.write().unwrap().insert(label_id, entry);
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_drop_vector_index(
    g: *mut mgp_graph,
    label: *const c_char,
) -> MgpError {
    if g.is_null() || label.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(label).to_string_lossy();
    let label_id = catalog.label(&label_name);
    storage.vector_indices.write().unwrap().remove(&label_id);
    MgpError::NoError
}

// ─── Vector / text search execution ────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_search_vector_index(
    g: *mut mgp_graph,
    index_name: *const c_char,
    query: *mut mgp_list,
    result_size: c_int,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_map,
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
            _ => {
                return build_search_error_result(result, "Vector search query must contain only numeric values");
            }
        }
    }

    let indices = storage.vector_indices.read().unwrap();
    let entry = match indices.get(&label_id) {
        Some(e) => e,
        None => return build_search_error_result(result, "Vector index not found"),
    };
    let index = entry.index.read().unwrap();

    if query_vec.len() != entry.dimension {
        drop(index);
        drop(indices);
        return build_search_error_result(result, "Query vector dimension mismatch");
    }

    let k = if result_size > 0 { result_size as usize } else { 10 };
    let search_results = index.search(&query_vec, k);
    drop(index);
    drop(indices);

    let mut result_map = mgp_map { items: HashMap::new() };
    let results_list = Box::into_raw(Box::new(mgp_list { items: Vec::new() }));

    for (node_id, distance) in search_results {
        let gid = Gid::from(node_id as u64);
        let vertex = Box::into_raw(Box::new(mgp_vertex { gid: gid.as_uint(), graph: g }));
        let vertex_val = mgp_value::boxed(ValueInner::Vertex(vertex));
        let dist_val = mgp_value::boxed(ValueInner::Double(distance as f64));
        let sim_val = mgp_value::boxed(ValueInner::Double(1.0 / (1.0 + distance as f64)));

        let triple = Box::into_raw(Box::new(mgp_list { items: vec![vertex_val, dist_val, sim_val] }));
        let triple_val = mgp_value::boxed(ValueInner::List(triple));
        (*results_list).items.push(triple_val);
    }

    let results_val = mgp_value::boxed(ValueInner::List(results_list));
    let key = CString::new("search_results").unwrap();
    result_map.items.insert(key, results_val);

    *result = Box::into_raw(Box::new(result_map));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_search_vector_index_on_edges(
    g: *mut mgp_graph,
    index_name: *const c_char,
    query: *mut mgp_list,
    result_size: c_int,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_map,
) -> MgpError {
    // Edge vector indices are not yet supported; return empty results.
    if g.is_null() || index_name.is_null() || query.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    build_search_error_result(result, "Edge vector search not yet implemented")
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_search_text_index(
    g: *mut mgp_graph,
    index_name: *const c_char,
    search_query: *const c_char,
    _search_mode: c_int,
    limit: usize,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_map,
) -> MgpError {
    if g.is_null() || index_name.is_null() || search_query.is_null() || result.is_null() {
        return MgpError::InvalidArgument;
    }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let label_name = CStr::from_ptr(index_name).to_string_lossy();
    let label_id = catalog.label(&label_name);
    let query_str = CStr::from_ptr(search_query).to_string_lossy();

    let text_indices = storage.text_indices.read().unwrap();
    let entry = match text_indices.get(&label_id) {
        Some(e) => e,
        None => return build_search_error_result(result, "Text index not found"),
    };

    let search_results = match entry.index.search(&query_str, limit) {
        Ok(r) => r,
        Err(_) => return build_search_error_result(result, "Text search failed"),
    };
    drop(text_indices);

    let mut result_map = mgp_map { items: std::collections::HashMap::new() };
    let results_list = Box::into_raw(Box::new(mgp_list { items: Vec::new() }));

    for (gid, score) in search_results {
        let vertex = Box::into_raw(Box::new(mgp_vertex { gid: gid.as_uint(), graph: g }));
        let vertex_val = mgp_value::boxed(ValueInner::Vertex(vertex));
        let score_val = mgp_value::boxed(ValueInner::Double(score as f64));

        let pair = Box::into_raw(Box::new(mgp_list { items: vec![vertex_val, score_val] }));
        let pair_val = mgp_value::boxed(ValueInner::List(pair));
        (*results_list).items.push(pair_val);
    }

    let results_val = mgp_value::boxed(ValueInner::List(results_list));
    let key = CString::new("search_results").unwrap();
    result_map.items.insert(key, results_val);

    *result = Box::into_raw(Box::new(result_map));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_search_text_edge_index(
    _g: *mut mgp_graph,
    _index_name: *const c_char,
    _search_query: *const c_char,
    _search_mode: c_int,
    _limit: usize,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_map,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    build_search_error_result(result, "Edge text search not yet implemented")
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_aggregate_over_text_index(
    _g: *mut mgp_graph,
    _index_name: *const c_char,
    _search_query: *const c_char,
    _aggregation_query: *const c_char,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_map,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    build_search_error_result(result, "Text index aggregation not yet implemented")
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_aggregate_over_text_edge_index(
    _g: *mut mgp_graph,
    _index_name: *const c_char,
    _search_query: *const c_char,
    _aggregation_query: *const c_char,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_map,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    build_search_error_result(result, "Edge text index aggregation not yet implemented")
}

unsafe fn build_search_error_result(result: *mut *mut mgp_map, msg: &str) -> MgpError {
    let mut result_map = mgp_map { items: HashMap::new() };
    let err_val = mgp_value::boxed(ValueInner::String(CString::new(msg).unwrap_or_default()));
    let key = CString::new("error_msg").unwrap();
    result_map.items.insert(key, err_val);
    *result = Box::into_raw(Box::new(result_map));
    MgpError::NoError
}

// ─── Index / constraint listing ────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_list_all_label_indices(
    g: *mut mgp_graph,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_list,
) -> MgpError {
    if g.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let mut list = mgp_list { items: Vec::new() };
    let active = storage.active_label_indices.read().unwrap();
    for &label_id in active.iter() {
        let name = catalog.label_name(label_id);
        let val = mgp_value::boxed(ValueInner::String(CString::new(name).unwrap_or_default()));
        list.items.push(val);
    }
    drop(active);
    *result = Box::into_raw(Box::new(list));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_all_label_property_indices(
    g: *mut mgp_graph,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_list,
) -> MgpError {
    if g.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let mut list = mgp_list { items: Vec::new() };
    let active = storage.active_label_property_indices.read().unwrap();
    for &(label_id, prop_id) in active.iter() {
        let label_name = catalog.label_name(label_id);
        let prop_name = catalog.property_name(prop_id);
        let mut map = mgp_map { items: HashMap::new() };
        let label_val = mgp_value::boxed(ValueInner::String(CString::new(label_name).unwrap_or_default()));
        let prop_val = mgp_value::boxed(ValueInner::String(CString::new(prop_name).unwrap_or_default()));
        map.items.insert(CString::new("label").unwrap(), label_val);
        map.items.insert(CString::new("property").unwrap(), prop_val);
        let map_ptr = Box::into_raw(Box::new(map));
        let map_val = mgp_value::boxed(ValueInner::Map(map_ptr));
        list.items.push(map_val);
    }
    *result = Box::into_raw(Box::new(list));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_all_unique_constraints(
    g: *mut mgp_graph,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_list,
) -> MgpError {
    if g.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let mut list = mgp_list { items: Vec::new() };
    for c in storage.constraints.list() {
        if !matches!(c.kind, mgstorage::constraints::ConstraintKind::Unique) { continue; }
        let mut map = mgp_map { items: HashMap::new() };
        let label_name = catalog.label_name(c.label);
        let prop_name = catalog.property_name(c.property);
        map.items.insert(CString::new("label").unwrap(), mgp_value::boxed(ValueInner::String(CString::new(label_name).unwrap_or_default())));
        map.items.insert(CString::new("property").unwrap(), mgp_value::boxed(ValueInner::String(CString::new(prop_name).unwrap_or_default())));
        let map_ptr = Box::into_raw(Box::new(map));
        list.items.push(mgp_value::boxed(ValueInner::Map(map_ptr)));
    }
    *result = Box::into_raw(Box::new(list));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_list_all_existence_constraints(
    g: *mut mgp_graph,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_list,
) -> MgpError {
    if g.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let mut list = mgp_list { items: Vec::new() };
    for c in storage.constraints.list() {
        if !matches!(c.kind, mgstorage::constraints::ConstraintKind::Existence) { continue; }
        let mut map = mgp_map { items: HashMap::new() };
        let label_name = catalog.label_name(c.label);
        let prop_name = catalog.property_name(c.property);
        map.items.insert(CString::new("label").unwrap(), mgp_value::boxed(ValueInner::String(CString::new(label_name).unwrap_or_default())));
        map.items.insert(CString::new("property").unwrap(), mgp_value::boxed(ValueInner::String(CString::new(prop_name).unwrap_or_default())));
        let map_ptr = Box::into_raw(Box::new(map));
        list.items.push(mgp_value::boxed(ValueInner::Map(map_ptr)));
    }
    *result = Box::into_raw(Box::new(list));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_graph_show_index_info(
    g: *mut mgp_graph,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_map,
) -> MgpError {
    if g.is_null() || result.is_null() { return MgpError::InvalidArgument; }
    let storage = &*(*g).storage;
    let catalog = graph_catalog(g);
    let mut result_map = mgp_map { items: HashMap::new() };

    // Label indices
    let label_idx_list = Box::into_raw(Box::new(mgp_list { items: Vec::new() }));
    let active_label = storage.active_label_indices.read().unwrap();
    for &label_id in active_label.iter() {
        let name = catalog.label_name(label_id);
        let val = mgp_value::boxed(ValueInner::String(CString::new(name).unwrap_or_default()));
        (*label_idx_list).items.push(val);
    }
    drop(active_label);
    result_map.items.insert(CString::new("label_indices").unwrap(), mgp_value::boxed(ValueInner::List(label_idx_list)));

    // Label-property indices
    let lp_idx_list = Box::into_raw(Box::new(mgp_list { items: Vec::new() }));
    let active_lp = storage.active_label_property_indices.read().unwrap();
    for &(label_id, prop_id) in active_lp.iter() {
        let mut map = mgp_map { items: HashMap::new() };
        map.items.insert(CString::new("label").unwrap(), mgp_value::boxed(ValueInner::String(CString::new(catalog.label_name(label_id)).unwrap_or_default())));
        map.items.insert(CString::new("property").unwrap(), mgp_value::boxed(ValueInner::String(CString::new(catalog.property_name(prop_id)).unwrap_or_default())));
        let map_ptr = Box::into_raw(Box::new(map));
        (*lp_idx_list).items.push(mgp_value::boxed(ValueInner::Map(map_ptr)));
    }
    drop(active_lp);
    result_map.items.insert(CString::new("label_property_indices").unwrap(), mgp_value::boxed(ValueInner::List(lp_idx_list)));

    *result = Box::into_raw(Box::new(result_map));
    MgpError::NoError
}

// ─── Subquery execution ────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_execute_query(
    graph: *mut mgp_graph,
    query: *const c_char,
    _params: *mut mgp_map,
    _memory: *mut mgp_memory,
    result: *mut *mut mgp_execution_result,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let query_str = unsafe { CStr::from_ptr(query) }.to_str().unwrap_or("");
    let storage = unsafe { &*(*graph).storage };
    let catalog = graph_catalog(graph);
    match mginterp::execute_with_catalog(storage, query_str, Some(catalog)) {
        Ok(query_result) => {
            let columns: Vec<CString> = query_result.columns.iter()
                .map(|c| CString::new(c.as_str()).unwrap_or_default())
                .collect();
            let mut rows = Vec::new();
            for row in query_result.rows {
                let mut map = mgp_map { items: HashMap::new() };
                for (key, val) in row {
                    let ckey = CString::new(key).unwrap_or_default();
                    map.items.insert(ckey, property_value_to_mgp_value(&val));
                }
                rows.push(Box::into_raw(Box::new(map)));
            }
            *result = Box::into_raw(Box::new(mgp_execution_result {
                columns,
                rows,
                current: 0,
            }));
            MgpError::NoError
        }
        Err(_) => {
            *result = ptr::null_mut();
            MgpError::InvalidArgument
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_pull_one(
    result: *mut mgp_execution_result,
    _memory: *mut mgp_memory,
    out_map: *mut *mut mgp_map,
) -> MgpError {
    if out_map.is_null() { return MgpError::InvalidArgument; }
    let res = unsafe { &mut *result };
    if res.current < res.rows.len() {
        *out_map = res.rows[res.current];
        res.current += 1;
        MgpError::NoError
    } else {
        *out_map = ptr::null_mut();
        MgpError::NoError
    }
}

#[no_mangle]
pub unsafe extern "C" fn mgp_fetch_execution_headers(
    result: *mut mgp_execution_result,
    out_headers: *mut *mut mgp_execution_headers,
) -> MgpError {
    if out_headers.is_null() { return MgpError::InvalidArgument; }
    let res = unsafe { &*result };
    *out_headers = Box::into_raw(Box::new(mgp_execution_headers {
        columns: res.columns.clone(),
    }));
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_execution_headers_size(
    headers: *mut mgp_execution_headers,
    result: *mut usize,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let h = unsafe { &*headers };
    *result = h.columns.len();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_execution_headers_at(
    headers: *mut mgp_execution_headers,
    index: usize,
    result: *mut *const c_char,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    let h = unsafe { &*headers };
    if let Some(c) = h.columns.get(index) {
        *result = c.as_ptr();
        MgpError::NoError
    } else {
        *result = ptr::null();
        MgpError::OutOfRange
    }
}

// ─── Stream message API stubs ──────────────────────────────────────────────

#[repr(C)]
#[derive(Clone, Copy)]
pub enum mgp_source_type {
    Kafka = 0,
    Pulsar = 1,
}

#[no_mangle]
pub unsafe extern "C" fn mgp_message_source_type(
    _message: *mut mgp_message,
    result: *mut mgp_source_type,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = mgp_source_type::Kafka;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_message_payload(
    _message: *mut mgp_message,
    result: *mut *const c_char,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = ptr::null();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_message_payload_size(
    _message: *mut mgp_message,
    result: *mut usize,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_message_topic_name(
    _message: *mut mgp_message,
    result: *mut *const c_char,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = ptr::null();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_message_key(
    _message: *mut mgp_message,
    result: *mut *const c_char,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = ptr::null();
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_message_key_size(
    _message: *mut mgp_message,
    result: *mut usize,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_message_timestamp(
    _message: *mut mgp_message,
    result: *mut i64,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_message_offset(
    _message: *mut mgp_message,
    result: *mut i64,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_messages_size(
    _messages: *mut mgp_messages,
    result: *mut usize,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = 0;
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_messages_at(
    _messages: *mut mgp_messages,
    _index: usize,
    result: *mut *mut mgp_message,
) -> MgpError {
    if result.is_null() { return MgpError::InvalidArgument; }
    *result = ptr::null_mut();
    MgpError::OutOfRange
}

// ─── Misc stubs ────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn mgp_track_current_thread_allocations(_memory: *mut mgp_memory) -> MgpError {
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_untrack_current_thread_allocations(_memory: *mut mgp_memory) -> MgpError {
    MgpError::NoError
}

#[no_mangle]
pub unsafe extern "C" fn mgp_init_module(_module: *mut mgp_module, _memory: *mut mgp_memory) -> MgpError {
    MgpError::NoError
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_match_c_header() {
        assert_eq!(MgpError::NoError as i32, 0);
        assert_eq!(MgpError::UnableToAllocate as i32, 2);
        assert_eq!(MgpError::DeletedObject as i32, 6);
        assert_eq!(MgpError::KeyAlreadyExists as i32, 8);
        assert_eq!(MgpError::ImmutableObject as i32, 9);
        assert_eq!(MgpError::SerializationError as i32, 11);
        assert_eq!(MgpError::AuthorizationError as i32, 12);
        assert_eq!(MgpError::NotYetImplemented as i32, 13);
    }

    #[test]
    fn value_make_and_get_int() {
        unsafe {
            let mut v: *mut mgp_value = ptr::null_mut();
            assert_eq!(mgp_value_make_int(42, ptr::null_mut(), &mut v), MgpError::NoError);
            assert!(!v.is_null());
            let mut got = 0i64;
            assert_eq!(mgp_value_get_int(v, &mut got), MgpError::NoError);
            assert_eq!(got, 42);
            let mut is_int = 0i32;
            assert_eq!(mgp_value_is_int(v, &mut is_int), MgpError::NoError);
            assert_eq!(is_int, 1);
            mgp_value_destroy(v);
        }
    }

    #[test]
    fn value_type_dispatch() {
        unsafe {
            let mut v: *mut mgp_value = ptr::null_mut();
            mgp_value_make_bool(1, ptr::null_mut(), &mut v);
            let mut ty = MgpValueType::Null;
            assert_eq!(mgp_value_get_type(v, &mut ty), MgpError::NoError);
            assert_eq!(ty, MgpValueType::Bool);
            let mut got = 0i32;
            mgp_value_get_bool(v, &mut got);
            assert_eq!(got, 1);
            mgp_value_destroy(v);
        }
    }

    #[test]
    fn value_string_roundtrip() {
        unsafe {
            let s = CString::new("hello").unwrap();
            let mut v: *mut mgp_value = ptr::null_mut();
            assert_eq!(mgp_value_make_string(s.as_ptr(), ptr::null_mut(), &mut v), MgpError::NoError);
            let mut p: *const c_char = ptr::null();
            assert_eq!(mgp_value_get_string(v, &mut p), MgpError::NoError);
            assert_eq!(CStr::from_ptr(p).to_str().unwrap(), "hello");
            mgp_value_destroy(v);
        }
    }

    #[test]
    fn value_double_conversion_error() {
        unsafe {
            let mut v: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(7, ptr::null_mut(), &mut v);
            let mut d = 0.0;
            assert_eq!(mgp_value_get_double(v, &mut d), MgpError::ValueConversion);
            mgp_value_destroy(v);
        }
    }

    #[test]
    fn list_append_and_iterate() {
        unsafe {
            let mut list: *mut mgp_list = ptr::null_mut();
            assert_eq!(mgp_list_make_empty(4, ptr::null_mut(), &mut list), MgpError::NoError);
            for i in 0..3 {
                let mut v: *mut mgp_value = ptr::null_mut();
                mgp_value_make_int(i, ptr::null_mut(), &mut v);
                assert_eq!(mgp_list_append(list, v), MgpError::NoError);
                mgp_value_destroy(v);
            }
            let mut size = 0;
            mgp_list_size(list, &mut size);
            assert_eq!(size, 3);

            let mut item: *mut mgp_value = ptr::null_mut();
            mgp_list_at(list, 1, &mut item);
            let mut got = 0i64;
            mgp_value_get_int(item, &mut got);
            assert_eq!(got, 1);
            mgp_list_destroy(list);
        }
    }

    #[test]
    fn list_at_out_of_range() {
        unsafe {
            let mut list: *mut mgp_list = ptr::null_mut();
            mgp_list_make_empty(0, ptr::null_mut(), &mut list);
            let mut item: *mut mgp_value = ptr::null_mut();
            assert_eq!(mgp_list_at(list, 0, &mut item), MgpError::OutOfRange);
            mgp_list_destroy(list);
        }
    }

    #[test]
    fn map_insert_and_lookup() {
        unsafe {
            let mut map: *mut mgp_map = ptr::null_mut();
            mgp_map_make_empty(ptr::null_mut(), &mut map);
            let key = CString::new("k").unwrap();
            let mut v: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(99, ptr::null_mut(), &mut v);
            assert_eq!(mgp_map_insert(map, key.as_ptr(), v), MgpError::NoError);
            assert_eq!(mgp_map_insert(map, key.as_ptr(), v), MgpError::KeyAlreadyExists);

            let mut got: *mut mgp_value = ptr::null_mut();
            mgp_map_at(map, key.as_ptr(), &mut got);
            let mut n = 0i64;
            mgp_value_get_int(got, &mut n);
            assert_eq!(n, 99);

            let mut size = 0;
            mgp_map_size(map, &mut size);
            assert_eq!(size, 1);

            let mut exists = 0i32;
            mgp_key_exists(map, key.as_ptr(), &mut exists);
            assert_eq!(exists, 1);

            mgp_value_destroy(v);
            mgp_map_destroy(map);
        }
    }

    #[test]
    fn allocator_tracks_layout() {
        unsafe {
            let mut p: *mut c_void = ptr::null_mut();
            assert_eq!(mgp_global_alloc(64, &mut p), MgpError::NoError);
            assert!(!p.is_null());
            let mut bytes = 0;
            mgp_memory_tracked_bytes(ptr::null(), &mut bytes);
            assert!(bytes >= 64);
            mgp_global_free(p);
            mgp_memory_tracked_bytes(ptr::null(), &mut bytes);
        }
    }

    #[test]
    fn null_arg_returns_invalid_argument() {
        unsafe {
            assert_eq!(mgp_value_get_int(ptr::null(), ptr::null_mut()), MgpError::InvalidArgument);
            let mut v: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(1, ptr::null_mut(), &mut v);
            assert_eq!(mgp_value_get_int(v, ptr::null_mut()), MgpError::InvalidArgument);
            mgp_value_destroy(v);
        }
    }

    #[test]
    fn graph_null_returns_invalid_argument() {
        unsafe {
            let mut g_count: u64 = 99;
            assert_eq!(
                mgp_graph_approximate_vertex_count(ptr::null(), &mut g_count),
                MgpError::InvalidArgument
            );
        }
    }

    #[test]
    fn type_descriptor_returns_singleton() {
        unsafe {
            let mut t: *mut mgp_type = ptr::null_mut();
            assert_eq!(mgp_type_int(&mut t), MgpError::NoError);
            assert!(!t.is_null());
        }
    }

    #[test]
    fn path_make_expand_pop() {
        unsafe {
            let v: *mut mgp_vertex = Box::into_raw(Box::new(mgp_vertex { gid: 1, graph: ptr::null() }));
            let mut path: *mut mgp_path = ptr::null_mut();
            assert_eq!(mgp_path_make_with_start(v, ptr::null_mut(), &mut path), MgpError::NoError);
            assert!(!path.is_null());

            let mut size = 0usize;
            assert_eq!(mgp_path_size(path, &mut size), MgpError::NoError);
            assert_eq!(size, 0);

            let e: *mut mgp_edge = Box::into_raw(Box::new(mgp_edge { gid: 10, graph: ptr::null() }));
            assert_eq!(mgp_path_expand(path, e), MgpError::NoError);

            assert_eq!(mgp_path_size(path, &mut size), MgpError::NoError);
            assert_eq!(size, 1);

            assert_eq!(mgp_path_pop(path), MgpError::NoError);
            assert_eq!(mgp_path_size(path, &mut size), MgpError::NoError);
            assert_eq!(size, 0);

            assert_eq!(mgp_path_pop(path), MgpError::OutOfRange);

            mgp_path_destroy(path);
            mgp_vertex_destroy(v);
            mgp_edge_destroy(e);
        }
    }

    #[test]
    fn path_copy_and_equal() {
        unsafe {
            let v: *mut mgp_vertex = Box::into_raw(Box::new(mgp_vertex { gid: 1, graph: ptr::null() }));
            let mut path1: *mut mgp_path = ptr::null_mut();
            mgp_path_make_with_start(v, ptr::null_mut(), &mut path1);

            let mut path2: *mut mgp_path = ptr::null_mut();
            assert_eq!(mgp_path_copy(path1, ptr::null_mut(), &mut path2), MgpError::NoError);

            let mut eq = 0i32;
            assert_eq!(mgp_path_equal(path1, path2, &mut eq), MgpError::NoError);
            assert_eq!(eq, 1);

            mgp_path_destroy(path1);
            mgp_path_destroy(path2);
            mgp_vertex_destroy(v);
        }
    }

    #[test]
    fn type_list_and_nullable() {
        unsafe {
            let mut base: *mut mgp_type = ptr::null_mut();
            assert_eq!(mgp_type_int(&mut base), MgpError::NoError);

            let mut list_ty: *mut mgp_type = ptr::null_mut();
            assert_eq!(mgp_type_list(base, &mut list_ty), MgpError::NoError);
            assert!(!list_ty.is_null());

            let mut nullable_ty: *mut mgp_type = ptr::null_mut();
            assert_eq!(mgp_type_nullable(base, &mut nullable_ty), MgpError::NoError);
            assert!(!nullable_ty.is_null());
        }
    }

    #[test]
    fn mutation_roundtrip() {
        use mgcore::delta::IsolationLevel;
        use mgstorage::storage::Storage;
        use mgcatalog::Catalog;

        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let catalog = Catalog::new();

        let graph = mgp_graph {
            storage: &storage as *const Storage,
            tx: &*tx as *const mgstorage::transaction::Transaction,
            catalog: &catalog as *const Catalog,
        };

        unsafe {
            // Create a vertex
            let mut v: *mut mgp_vertex = ptr::null_mut();
            assert_eq!(mgp_graph_create_vertex(&graph as *const _ as *mut _, ptr::null_mut(), &mut v), MgpError::NoError);
            assert!(!v.is_null());

            // Set a property
            let mut val: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(42, ptr::null_mut(), &mut val);
            let key = CString::new("age").unwrap();
            assert_eq!(mgp_vertex_set_property(v, key.as_ptr(), val), MgpError::NoError);
            mgp_value_destroy(val);

            // Add a label
            let label = CString::new("Person").unwrap();
            assert_eq!(mgp_vertex_add_label(v, mgp_label { name: label.as_ptr() }), MgpError::NoError);

            // Create a second vertex for edge creation
            let mut v2: *mut mgp_vertex = ptr::null_mut();
            assert_eq!(mgp_graph_create_vertex(&graph as *const _ as *mut _, ptr::null_mut(), &mut v2), MgpError::NoError);

            // Create an edge
            let mut e: *mut mgp_edge = ptr::null_mut();
            let edge_type = CString::new("KNOWS").unwrap();
            assert_eq!(mgp_graph_create_edge(&graph as *const _ as *mut _, v, v2, mgp_edge_type { name: edge_type.as_ptr() }, ptr::null_mut(), &mut e), MgpError::NoError);
            assert!(!e.is_null());

            // Delete the edge
            assert_eq!(mgp_graph_delete_edge(&graph as *const _ as *mut _, e), MgpError::NoError);
            mgp_edge_destroy(e);

            // Delete vertices
            assert_eq!(mgp_graph_delete_vertex(&graph as *const _ as *mut _, v2), MgpError::NoError);
            assert_eq!(mgp_graph_delete_vertex(&graph as *const _ as *mut _, v), MgpError::NoError);
            mgp_vertex_destroy(v2);
            mgp_vertex_destroy(v);
        }
    }

    #[test]
    fn vertex_label_and_edge_type_roundtrip() {
        use mgcore::delta::IsolationLevel;
        use mgstorage::storage::Storage;
        use mgcatalog::Catalog;

        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let catalog = Catalog::new();

        let graph = mgp_graph {
            storage: &storage as *const Storage,
            tx: &*tx as *const mgstorage::transaction::Transaction,
            catalog: &catalog as *const Catalog,
        };

        unsafe {
            let mut v: *mut mgp_vertex = ptr::null_mut();
            assert_eq!(mgp_graph_create_vertex(&graph as *const _ as *mut _, ptr::null_mut(), &mut v), MgpError::NoError);

            // Add label via C API
            let label = CString::new("Person").unwrap();
            assert_eq!(mgp_vertex_add_label(v, mgp_label { name: label.as_ptr() }), MgpError::NoError);

            // Check label count
            let mut count: usize = 0;
            assert_eq!(mgp_vertex_labels_count(v, &mut count), MgpError::NoError);
            assert_eq!(count, 1);

            // Check has_label
            let mut has: i32 = 0;
            assert_eq!(mgp_vertex_has_label(v, mgp_label { name: label.as_ptr() }, &mut has), MgpError::NoError);
            assert_eq!(has, 1);

            // Check has_label_named
            let mut has2: i32 = 0;
            assert_eq!(mgp_vertex_has_label_named(v, label.as_ptr(), &mut has2), MgpError::NoError);
            assert_eq!(has2, 1);

            // Check label_at
            let mut got_label = mgp_label { name: ptr::null() };
            assert_eq!(mgp_vertex_label_at(v, 0, &mut got_label), MgpError::NoError);
            assert!(!got_label.name.is_null());
            assert_eq!(CStr::from_ptr(got_label.name).to_string_lossy(), "Person");

            // Create edge with type
            let mut v2: *mut mgp_vertex = ptr::null_mut();
            assert_eq!(mgp_graph_create_vertex(&graph as *const _ as *mut _, ptr::null_mut(), &mut v2), MgpError::NoError);
            let edge_type = CString::new("KNOWS").unwrap();
            let mut e: *mut mgp_edge = ptr::null_mut();
            assert_eq!(mgp_graph_create_edge(&graph as *const _ as *mut _, v, v2, mgp_edge_type { name: edge_type.as_ptr() }, ptr::null_mut(), &mut e), MgpError::NoError);

            // Check edge type
            let mut got_type = mgp_edge_type { name: ptr::null() };
            assert_eq!(mgp_edge_get_type(e, &mut got_type), MgpError::NoError);
            assert!(!got_type.name.is_null());
            assert_eq!(CStr::from_ptr(got_type.name).to_string_lossy(), "KNOWS");

            // Property iteration uses real names
            let mut val: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(99, ptr::null_mut(), &mut val);
            let key = CString::new("score").unwrap();
            assert_eq!(mgp_vertex_set_property(v, key.as_ptr(), val), MgpError::NoError);
            mgp_value_destroy(val);

            let mut pit: *mut mgp_properties_iterator = ptr::null_mut();
            assert_eq!(mgp_vertex_iter_properties(v, ptr::null_mut(), &mut pit), MgpError::NoError);
            assert!(!pit.is_null());

            let mut prop: *mut mgp_property = ptr::null_mut();
            assert_eq!(mgp_properties_iterator_get(pit, &mut prop), MgpError::NoError);
            assert!(!prop.is_null());
            assert_eq!(CStr::from_ptr((*prop).name).to_string_lossy(), "score");
            mgp_properties_iterator_destroy(pit);

            mgp_edge_destroy(e);
            mgp_vertex_destroy(v2);
            mgp_vertex_destroy(v);
        }
    }

    #[test]
    fn module_procedure_registration_roundtrip() {
        unsafe extern "C" fn dummy_proc(_args: *mut mgp_list, _graph: *mut mgp_graph, _result: *mut mgp_result, _memory: *mut mgp_memory) {}
        unsafe extern "C" fn dummy_func(_args: *mut mgp_list, _graph: *mut mgp_graph, _result: *mut mgp_func_result, _memory: *mut mgp_memory) {}

        unsafe {
            let mut module = mgp_module { procs: Vec::new(), funcs: Vec::new() };

            // Register a read procedure
            let mut proc: *mut mgp_proc = ptr::null_mut();
            let name = CString::new("my_proc").unwrap();
            assert_eq!(mgp_module_add_read_procedure(&mut module, name.as_ptr(), dummy_proc, &mut proc), MgpError::NoError);
            assert!(!proc.is_null());
            assert_eq!((*proc).is_write, false);

            // Add arg and result
            let mut ty: *mut mgp_type = ptr::null_mut();
            assert_eq!(mgp_type_int(&mut ty), MgpError::NoError);
            let arg_name = CString::new("input").unwrap();
            assert_eq!(mgp_proc_add_arg(proc, arg_name.as_ptr(), ty), MgpError::NoError);
            let res_name = CString::new("output").unwrap();
            assert_eq!(mgp_proc_add_result(proc, res_name.as_ptr(), ty), MgpError::NoError);

            // Register a write procedure
            let mut wproc: *mut mgp_proc = ptr::null_mut();
            let wname = CString::new("my_write_proc").unwrap();
            assert_eq!(mgp_module_add_write_procedure(&mut module, wname.as_ptr(), dummy_proc, &mut wproc), MgpError::NoError);
            assert_eq!((*wproc).is_write, true);

            // Register a function
            let mut func: *mut mgp_func = ptr::null_mut();
            let fname = CString::new("my_func").unwrap();
            assert_eq!(mgp_module_add_function(&mut module, fname.as_ptr(), dummy_func, &mut func), MgpError::NoError);
            assert!(!func.is_null());

            // Result API
            let mut result = mgp_result { error: None, records: Vec::new(), reserved: 0 };
            assert_eq!(mgp_result_reserve(&mut result, 10), MgpError::NoError);
            assert_eq!(result.reserved, 10);

            let mut record: *mut mgp_result_record = ptr::null_mut();
            assert_eq!(mgp_result_new_record(&mut result, &mut record), MgpError::NoError);
            assert!(!record.is_null());

            let mut val: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(42, ptr::null_mut(), &mut val);
            let field = CString::new("answer").unwrap();
            assert_eq!(mgp_result_record_insert(record, field.as_ptr(), val), MgpError::NoError);

            // Func result API
            let mut fresult = mgp_func_result { error: None, value: None };
            let mut fval: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(99, ptr::null_mut(), &mut fval);
            assert_eq!(mgp_func_result_set_value(&mut fresult, fval, ptr::null_mut()), MgpError::NoError);
            assert!(fresult.value.is_some());

            // Error on result
            let mut eresult = mgp_result { error: None, records: Vec::new(), reserved: 0 };
            let errmsg = CString::new("something went wrong").unwrap();
            assert_eq!(mgp_result_set_error_msg(&mut eresult, errmsg.as_ptr()), MgpError::NoError);
            assert!(eresult.error.is_some());
        }
    }

    #[test]
    fn index_and_constraint_apis() {
        use mgstorage::storage::Storage;
        unsafe {
            let storage = Box::into_raw(Box::new(Storage::new()));
            let catalog = Box::into_raw(Box::new(mgcatalog::Catalog::new()));
            let tx = Box::into_raw(Box::new((*storage).transaction_engine.begin(mgcore::delta::IsolationLevel::SnapshotIsolation)));
            let mut graph = mgp_graph { storage, tx, catalog };

            // Label index
            let label = CString::new("Person").unwrap();
            let mut result: c_int = 0;
            assert_eq!(mgp_create_label_index(&mut graph, label.as_ptr(), &mut result), MgpError::NoError);
            assert_eq!(result, 1); // created
            assert_eq!(mgp_create_label_index(&mut graph, label.as_ptr(), &mut result), MgpError::NoError);
            assert_eq!(result, 0); // already exists
            assert_eq!(mgp_drop_label_index(&mut graph, label.as_ptr(), &mut result), MgpError::NoError);
            assert_eq!(result, 1); // dropped
            assert_eq!(mgp_drop_label_index(&mut graph, label.as_ptr(), &mut result), MgpError::NoError);
            assert_eq!(result, 0); // not found

            // Label-property index
            let prop = CString::new("name").unwrap();
            assert_eq!(mgp_create_label_property_index(&mut graph, label.as_ptr(), prop.as_ptr(), &mut result), MgpError::NoError);
            assert_eq!(result, 1);
            assert_eq!(mgp_drop_label_property_index(&mut graph, label.as_ptr(), prop.as_ptr(), &mut result), MgpError::NoError);
            assert_eq!(result, 1);

            // Existence constraint
            assert_eq!(mgp_create_existence_constraint(&mut graph, label.as_ptr(), prop.as_ptr(), &mut result), MgpError::NoError);
            assert_eq!(result, 1);
            assert_eq!(mgp_create_existence_constraint(&mut graph, label.as_ptr(), prop.as_ptr(), &mut result), MgpError::NoError);
            assert_eq!(result, 0); // already exists
            assert_eq!(mgp_drop_existence_constraint(&mut graph, label.as_ptr(), prop.as_ptr(), &mut result), MgpError::NoError);
            assert_eq!(result, 1);
            assert_eq!(mgp_drop_existence_constraint(&mut graph, label.as_ptr(), prop.as_ptr(), &mut result), MgpError::NoError);
            assert_eq!(result, 0); // not found

            // Unique constraint via property list
            let mut list = mgp_list { items: Vec::new() };
            let prop_val = CString::new("name").unwrap();
            let mut v: *mut mgp_value = ptr::null_mut();
            mgp_value_make_string(prop_val.as_ptr(), ptr::null_mut(), &mut v);
            list.items.push(v);
            assert_eq!(mgp_create_unique_constraint(&mut graph, label.as_ptr(), &list, &mut result), MgpError::NoError);
            assert_eq!(result, 1);
            assert_eq!(mgp_create_unique_constraint(&mut graph, label.as_ptr(), &list, &mut result), MgpError::NoError);
            assert_eq!(result, 0); // already exists
            assert_eq!(mgp_drop_unique_constraint(&mut graph, label.as_ptr(), &list, &mut result), MgpError::NoError);
            assert_eq!(result, 1);
            assert_eq!(mgp_drop_unique_constraint(&mut graph, label.as_ptr(), &list, &mut result), MgpError::NoError);
            assert_eq!(result, 0); // not found

            mgp_value_destroy(v);
            drop(Box::from_raw(tx));
            drop(Box::from_raw(catalog));
            drop(Box::from_raw(storage));
        }
    }

    #[test]
    fn vertex_get_property_roundtrip() {
        use mgstorage::storage::Storage;
        unsafe {
            let storage = Box::into_raw(Box::new(Storage::new()));
            let catalog = Box::into_raw(Box::new(mgcatalog::Catalog::new()));
            let tx = Box::into_raw(Box::new((*storage).transaction_engine.begin(mgcore::delta::IsolationLevel::SnapshotIsolation)));
            let mut graph = mgp_graph { storage, tx, catalog };

            let mut v: *mut mgp_vertex = ptr::null_mut();
            assert_eq!(mgp_graph_create_vertex(&mut graph, ptr::null_mut(), &mut v), MgpError::NoError);

            let key = CString::new("score").unwrap();
            let mut val: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(42, ptr::null_mut(), &mut val);
            assert_eq!(mgp_vertex_set_property(v, key.as_ptr(), val), MgpError::NoError);

            let mut got: *mut mgp_value = ptr::null_mut();
            assert_eq!(mgp_vertex_get_property(v, key.as_ptr(), ptr::null_mut(), &mut got), MgpError::NoError);
            assert!(!got.is_null());
            let mut n = 0i64;
            mgp_value_get_int(got, &mut n);
            assert_eq!(n, 42);

            mgp_value_destroy(val);
            mgp_vertex_destroy(v);
            drop(Box::from_raw(tx));
            drop(Box::from_raw(catalog));
            drop(Box::from_raw(storage));
        }
    }

    #[test]
    fn edge_get_property_and_endpoints() {
        use mgstorage::storage::Storage;
        unsafe {
            let storage = Box::into_raw(Box::new(Storage::new()));
            let catalog = Box::into_raw(Box::new(mgcatalog::Catalog::new()));
            let tx = Box::into_raw(Box::new((*storage).transaction_engine.begin(mgcore::delta::IsolationLevel::SnapshotIsolation)));
            let mut graph = mgp_graph { storage, tx, catalog };

            let mut v1: *mut mgp_vertex = ptr::null_mut();
            let mut v2: *mut mgp_vertex = ptr::null_mut();
            mgp_graph_create_vertex(&mut graph, ptr::null_mut(), &mut v1);
            mgp_graph_create_vertex(&mut graph, ptr::null_mut(), &mut v2);

            let etype_name = CString::new("KNOWS").unwrap();
            let etype = mgp_edge_type { name: etype_name.as_ptr() };
            let mut e: *mut mgp_edge = ptr::null_mut();
            mgp_graph_create_edge(&mut graph, v1, v2, etype, ptr::null_mut(), &mut e);

            let key = CString::new("since").unwrap();
            let mut val: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(2020, ptr::null_mut(), &mut val);
            mgp_edge_set_property(e, key.as_ptr(), val);

            let mut got: *mut mgp_value = ptr::null_mut();
            assert_eq!(mgp_edge_get_property(e, key.as_ptr(), ptr::null_mut(), &mut got), MgpError::NoError);
            let mut n = 0i64;
            mgp_value_get_int(got, &mut n);
            assert_eq!(n, 2020);

            let mut from_v: *mut mgp_vertex = ptr::null_mut();
            let mut to_v: *mut mgp_vertex = ptr::null_mut();
            assert_eq!(mgp_edge_get_from(e, &mut from_v), MgpError::NoError);
            assert_eq!(mgp_edge_get_to(e, &mut to_v), MgpError::NoError);
            assert!(!from_v.is_null());
            assert!(!to_v.is_null());

            mgp_value_destroy(val);
            mgp_edge_destroy(e);
            mgp_vertex_destroy(v1);
            mgp_vertex_destroy(v2);
            drop(Box::from_raw(tx));
            drop(Box::from_raw(catalog));
            drop(Box::from_raw(storage));
        }
    }

    #[test]
    fn path_vertex_and_edge_at() {
        unsafe {
            let v1 = Box::into_raw(Box::new(mgp_vertex { gid: 1, graph: ptr::null() }));
            let e = Box::into_raw(Box::new(mgp_edge { gid: 10, graph: ptr::null() }));
            let mut path: *mut mgp_path = ptr::null_mut();
            mgp_path_make_with_start(v1, ptr::null_mut(), &mut path);
            mgp_path_expand(path, e);

            let mut got_v: *mut mgp_vertex = ptr::null_mut();
            assert_eq!(mgp_path_vertex_at(path, 0, &mut got_v), MgpError::NoError);
            assert_eq!((*got_v).gid, 1);

            let mut got_e: *mut mgp_edge = ptr::null_mut();
            assert_eq!(mgp_path_edge_at(path, 0, &mut got_e), MgpError::NoError);
            assert_eq!((*got_e).gid, 10);

            assert_eq!(mgp_path_vertex_at(path, 99, &mut got_v), MgpError::OutOfRange);
            assert_eq!(mgp_path_edge_at(path, 99, &mut got_e), MgpError::OutOfRange);

            mgp_path_destroy(path);
        }
    }

    #[test]
    fn list_copy_preserves_items() {
        unsafe {
            let mut list: *mut mgp_list = ptr::null_mut();
            mgp_list_make_empty(2, ptr::null_mut(), &mut list);
            let mut v: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(7, ptr::null_mut(), &mut v);
            mgp_list_append(list, v);
            mgp_value_destroy(v);

            let mut copy: *mut mgp_list = ptr::null_mut();
            assert_eq!(mgp_list_copy(list, ptr::null_mut(), &mut copy), MgpError::NoError);

            let mut size = 0;
            mgp_list_size(copy, &mut size);
            assert_eq!(size, 1);
            let mut item: *mut mgp_value = ptr::null_mut();
            mgp_list_at(copy, 0, &mut item);
            let mut n = 0i64;
            mgp_value_get_int(item, &mut n);
            assert_eq!(n, 7);

            mgp_list_destroy(list);
            mgp_list_destroy(copy);
        }
    }

    #[test]
    fn map_copy_preserves_entries() {
        unsafe {
            let mut map: *mut mgp_map = ptr::null_mut();
            mgp_map_make_empty(ptr::null_mut(), &mut map);
            let key = CString::new("x").unwrap();
            let mut v: *mut mgp_value = ptr::null_mut();
            mgp_value_make_int(5, ptr::null_mut(), &mut v);
            mgp_map_insert(map, key.as_ptr(), v);
            mgp_value_destroy(v);

            let mut copy: *mut mgp_map = ptr::null_mut();
            assert_eq!(mgp_map_copy(map, ptr::null_mut(), &mut copy), MgpError::NoError);

            let mut got: *mut mgp_value = ptr::null_mut();
            mgp_map_at(copy, key.as_ptr(), &mut got);
            let mut n = 0i64;
            mgp_value_get_int(got, &mut n);
            assert_eq!(n, 5);

            mgp_map_destroy(map);
            mgp_map_destroy(copy);
        }
    }

    #[test]
    fn vertices_iterator_next_and_mutability() {
        use mgstorage::storage::Storage;
        unsafe {
            let storage = Box::into_raw(Box::new(Storage::new()));
            let catalog = Box::into_raw(Box::new(mgcatalog::Catalog::new()));
            let tx = Box::into_raw(Box::new((*storage).transaction_engine.begin(mgcore::delta::IsolationLevel::SnapshotIsolation)));
            let mut graph = mgp_graph { storage, tx, catalog };

            let mut v: *mut mgp_vertex = ptr::null_mut();
            mgp_graph_create_vertex(&mut graph, ptr::null_mut(), &mut v);
            mgp_vertex_destroy(v);

            let mut it: *mut mgp_vertices_iterator = ptr::null_mut();
            mgp_graph_iter_vertices(&mut graph, ptr::null_mut(), &mut it);

            let mut got: *const mgp_vertex = ptr::null();
            assert_eq!(mgp_vertices_iterator_next(it, &mut got), MgpError::NoError);
            assert!(!got.is_null());

            let mut mutable = 0i32;
            assert_eq!(mgp_vertices_iterator_underlying_graph_is_mutable(it, &mut mutable), MgpError::NoError);
            assert_eq!(mutable, 1);

            mgp_vertices_iterator_destroy(it);
            drop(Box::from_raw(tx));
            drop(Box::from_raw(catalog));
            drop(Box::from_raw(storage));
        }
    }

    #[test]
    fn value_type_predicates_for_all_types() {
        unsafe {
            let mut v: *mut mgp_value = ptr::null_mut();

            mgp_value_make_null(ptr::null_mut(), &mut v);
            let mut flag = 0i32;
            mgp_value_is_null(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_is_int(v, &mut flag); assert_eq!(flag, 0);
            mgp_value_destroy(v);

            mgp_value_make_int(1, ptr::null_mut(), &mut v);
            mgp_value_is_int(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_is_bool(v, &mut flag); assert_eq!(flag, 0);
            mgp_value_destroy(v);

            mgp_value_make_double(1.5, ptr::null_mut(), &mut v);
            mgp_value_is_double(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_destroy(v);

            mgp_value_make_string(CString::new("s").unwrap().as_ptr(), ptr::null_mut(), &mut v);
            mgp_value_is_string(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_destroy(v);

            mgp_value_make_date(100, ptr::null_mut(), &mut v);
            mgp_value_is_date(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_destroy(v);

            mgp_value_make_local_time(50, ptr::null_mut(), &mut v);
            mgp_value_is_local_time(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_destroy(v);

            mgp_value_make_local_date_time(200, ptr::null_mut(), &mut v);
            mgp_value_is_local_date_time(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_destroy(v);

            mgp_value_make_duration(1, 2, 3, ptr::null_mut(), &mut v);
            mgp_value_is_duration(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_destroy(v);

            mgp_value_make_zoned_date_time(10, 0, CString::new("UTC").unwrap().as_ptr(), ptr::null_mut(), &mut v);
            mgp_value_is_zoned_date_time(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_destroy(v);

            mgp_value_make_point_2d(7203, 1.0, 2.0, ptr::null_mut(), &mut v);
            mgp_value_is_point_2d(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_destroy(v);

            mgp_value_make_point_3d(9157, 1.0, 2.0, 3.0, ptr::null_mut(), &mut v);
            mgp_value_is_point_3d(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_destroy(v);

            mgp_value_make_enum(CString::new("Color").unwrap().as_ptr(), CString::new("Red").unwrap().as_ptr(), ptr::null_mut(), &mut v);
            mgp_value_is_enum(v, &mut flag); assert_eq!(flag, 1);
            mgp_value_destroy(v);
        }
    }

    #[test]
    fn stream_batch_make_size_at() {
        use crate::stream::*;
        unsafe {
            let mut batch: *mut mgp_stream_batch = ptr::null_mut();
            assert_eq!(mgp_stream_batch_make_empty(&mut batch), MgpError::NoError);
            assert!(!batch.is_null());

            let mut size = 99usize;
            assert_eq!(mgp_stream_batch_size(batch, &mut size), MgpError::NoError);
            assert_eq!(size, 0);

            let mut at: *mut mgp_result_record = ptr::null_mut();
            assert_eq!(mgp_stream_batch_at(batch, 0, &mut at), MgpError::OutOfRange);

            mgp_stream_batch_destroy(batch);
        }
    }

    #[test]
    fn stream_make_and_destroy() {
        use crate::stream::*;
        unsafe {
            let mut stream: *mut mgp_stream = ptr::null_mut();
            assert_eq!(mgp_stream_make_empty(&mut stream), MgpError::NoError);
            assert!(!stream.is_null());
            mgp_stream_destroy(stream);
        }
    }

    #[test]
    fn transformation_ctx_graph_and_add_record() {
        use crate::stream::*;
        unsafe {
            let mut ctx: *mut mgp_transformation_ctx = Box::into_raw(Box::new(mgp_transformation_ctx {
                graph: ptr::null_mut(),
                records: Vec::new(),
            }));

            let mut g: *mut mgp_graph = ptr::null_mut();
            assert_eq!(mgp_transformation_ctx_graph(ctx, &mut g), MgpError::NoError);
            assert!(g.is_null());

            let record = Box::into_raw(Box::new(mgp_result_record { fields: HashMap::new() }));
            assert_eq!(mgp_transformation_ctx_add_record(ctx, record), MgpError::NoError);
            assert_eq!((*ctx).records.len(), 1);

            mgp_transformation_ctx_destroy(ctx);
        }
    }

    #[test]
    fn trigger_context_event_type_and_iterators() {
        use crate::stream::*;
        unsafe {
            let ctx = Box::into_raw(Box::new(mgp_trigger_context {
                event: MgpTriggerEventType::CreateVertex,
                created_vertices: vec![1, 2],
                created_edges: vec![10],
                deleted_vertices: vec![],
                deleted_edges: vec![],
                set_property_vertices: vec![],
                removed_property_vertices: vec![],
                graph: ptr::null(),
                vertex_before: None,
                vertex_after: None,
                edge_before: None,
                edge_after: None,
            }));

            let mut event = MgpTriggerEventType::DeleteEdge;
            assert_eq!(mgp_trigger_context_event_type(ctx, &mut event), MgpError::NoError);
            assert_eq!(event, MgpTriggerEventType::CreateVertex);

            let mut it: *mut mgp_vertices_iterator = ptr::null_mut();
            assert_eq!(mgp_trigger_context_created_vertices(ctx, &mut it), MgpError::NoError);
            assert!(!it.is_null());

            let mut v: *const mgp_vertex = ptr::null();
            assert_eq!(mgp_vertices_iterator_get(it, &mut v), MgpError::NoError);
            assert!(!v.is_null());
            assert_eq!((*v).gid, 1);
            mgp_vertices_iterator_destroy(it);

            let mut eit: *mut mgp_edges_iterator = ptr::null_mut();
            assert_eq!(mgp_trigger_context_created_edges(ctx, &mut eit), MgpError::NoError);
            let mut e: *const mgp_edge = ptr::null();
            assert_eq!(mgp_edges_iterator_get(eit, &mut e), MgpError::NoError);
            assert!(!e.is_null());
            assert_eq!((*e).gid, 10);
            mgp_edges_iterator_destroy(eit);

            mgp_trigger_context_destroy(ctx);
        }
    }

    #[test]
    fn trigger_context_set_and_removed_property_vertices() {
        use crate::stream::*;
        unsafe {
            let old_val_set = mgp_value::boxed(ValueInner::Int(10));
            let new_val = mgp_value::boxed(ValueInner::Int(20));
            let old_val_removed = mgp_value::boxed(ValueInner::Int(30));
            let ctx = Box::into_raw(Box::new(mgp_trigger_context {
                event: MgpTriggerEventType::SetPropertyVertex,
                created_vertices: vec![],
                created_edges: vec![],
                deleted_vertices: vec![],
                deleted_edges: vec![],
                set_property_vertices: vec![(5, CString::new("age").unwrap(), old_val_set, new_val)],
                removed_property_vertices: vec![(6, CString::new("name").unwrap(), old_val_removed)],
                graph: ptr::null(),
                vertex_before: None,
                vertex_after: None,
                edge_before: None,
                edge_after: None,
            }));

            let mut it: *mut mgp_set_property_vertices_iterator = ptr::null_mut();
            assert_eq!(mgp_trigger_context_set_property_vertices(ctx, &mut it), MgpError::NoError);
            let mut v: *mut mgp_vertex = ptr::null_mut();
            let mut pname: *const c_char = ptr::null();
            let mut ov: *mut mgp_value = ptr::null_mut();
            let mut nv: *mut mgp_value = ptr::null_mut();
            assert_eq!(mgp_set_property_vertices_iterator_get(it, &mut v, &mut pname, &mut ov, &mut nv), MgpError::NoError);
            assert!(!v.is_null());
            assert_eq!((*v).gid, 5);
            assert_eq!(CStr::from_ptr(pname).to_string_lossy(), "age");
            let mut n = 0i64;
            mgp_value_get_int(ov, &mut n);
            assert_eq!(n, 10);
            mgp_value_get_int(nv, &mut n);
            assert_eq!(n, 20);
            mgp_set_property_vertices_iterator_destroy(it);

            let mut rit: *mut mgp_removed_property_vertices_iterator = ptr::null_mut();
            assert_eq!(mgp_trigger_context_removed_property_vertices(ctx, &mut rit), MgpError::NoError);
            let mut rv: *mut mgp_vertex = ptr::null_mut();
            let mut rpname: *const c_char = ptr::null();
            let mut rov: *mut mgp_value = ptr::null_mut();
            assert_eq!(mgp_removed_property_vertices_iterator_get(rit, &mut rv, &mut rpname, &mut rov), MgpError::NoError);
            assert!(!rv.is_null());
            assert_eq!((*rv).gid, 6);
            mgp_removed_property_vertices_iterator_destroy(rit);

            mgp_trigger_context_destroy(ctx);
        }
    }

    #[test]
    fn trigger_context_vertex_before_after() {
        use crate::stream::*;
        unsafe {
            let vb = Box::into_raw(Box::new(mgp_vertex { gid: 7, graph: ptr::null() }));
            let va = Box::into_raw(Box::new(mgp_vertex { gid: 8, graph: ptr::null() }));
            let eb = Box::into_raw(Box::new(mgp_edge { gid: 70, graph: ptr::null() }));
            let ea = Box::into_raw(Box::new(mgp_edge { gid: 80, graph: ptr::null() }));
            let ctx = Box::into_raw(Box::new(mgp_trigger_context {
                event: MgpTriggerEventType::UpdateVertex,
                created_vertices: vec![],
                created_edges: vec![],
                deleted_vertices: vec![],
                deleted_edges: vec![],
                set_property_vertices: vec![],
                removed_property_vertices: vec![],
                graph: ptr::null(),
                vertex_before: Some(vb),
                vertex_after: Some(va),
                edge_before: Some(eb),
                edge_after: Some(ea),
            }));

            let mut got_vb: *mut mgp_vertex = ptr::null_mut();
            let mut got_va: *mut mgp_vertex = ptr::null_mut();
            let mut got_eb: *mut mgp_edge = ptr::null_mut();
            let mut got_ea: *mut mgp_edge = ptr::null_mut();
            assert_eq!(mgp_trigger_context_vertex_before(ctx, &mut got_vb), MgpError::NoError);
            assert_eq!(mgp_trigger_context_vertex_after(ctx, &mut got_va), MgpError::NoError);
            assert_eq!(mgp_trigger_context_edge_before(ctx, &mut got_eb), MgpError::NoError);
            assert_eq!(mgp_trigger_context_edge_after(ctx, &mut got_ea), MgpError::NoError);
            assert_eq!((*got_vb).gid, 7);
            assert_eq!((*got_va).gid, 8);
            assert_eq!((*got_eb).gid, 70);
            assert_eq!((*got_ea).gid, 80);

            mgp_trigger_context_destroy(ctx);
        }
    }

    #[test]
    fn search_text_index_finds_indexed_vertex() {
        use mgstorage::storage::Storage;
        use crate::stream::*;
        unsafe {
            let storage = Box::into_raw(Box::new(Storage::new()));
            let catalog = Box::into_raw(Box::new(mgcatalog::Catalog::new()));
            let tx = Box::into_raw(Box::new((*storage).transaction_engine.begin(mgcore::delta::IsolationLevel::SnapshotIsolation)));
            let graph = mgp_graph { storage, tx, catalog };

            // Create a vertex with a label and text property
            let label_id = (*catalog).label("Post");
            let prop_id = (*catalog).property("title");
            (*storage).create_vertex(&*tx, mgcore::types::Gid::from(1u64)).unwrap();
            (*storage).vertex_add_label(&*tx, mgcore::types::Gid::from(1u64), label_id).unwrap();
            (*storage).vertex_set_property(&*tx, mgcore::types::Gid::from(1u64), prop_id, mgcore::property_value::PropertyValue::String("Hello World".into())).unwrap();

            // Create text index via C API
            let mut prop_list: *mut mgp_list = ptr::null_mut();
            mgp_list_make_empty(2, ptr::null_mut(), &mut prop_list);
            let mut v: *mut mgp_value = ptr::null_mut();
            mgp_value_make_string(CString::new("title").unwrap().as_ptr(), ptr::null_mut(), &mut v);
            mgp_list_append(prop_list, v);
            mgp_value_destroy(v);
            assert_eq!(mgp_create_text_index(&graph as *const _ as *mut _, CString::new("Post").unwrap().as_ptr(), prop_list), MgpError::NoError);

            // Search for "Hello"
            let mut result: *mut mgp_list = ptr::null_mut();
            assert_eq!(mgp_search_text_index(&graph as *const _ as *mut _, CString::new("Post").unwrap().as_ptr(), CString::new("Hello").unwrap().as_ptr(), 10, &mut result), MgpError::NoError);
            assert!(!result.is_null());
            let mut size = 99usize;
            mgp_list_size(result, &mut size);
            assert_eq!(size, 1); // Found 1 result

            mgp_list_destroy(prop_list);
            mgp_list_destroy(result);

            // Cleanup
            let _ = Box::from_raw(tx);
            let _ = Box::from_raw(catalog);
            let _ = Box::from_raw(storage);
        }
    }

    #[test]
    fn search_vector_index_empty_when_no_index() {
        use mgstorage::storage::Storage;
        use crate::stream::*;
        unsafe {
            let storage = Box::into_raw(Box::new(Storage::new()));
            let catalog = Box::into_raw(Box::new(mgcatalog::Catalog::new()));
            let tx = Box::into_raw(Box::new((*storage).transaction_engine.begin(mgcore::delta::IsolationLevel::SnapshotIsolation)));
            let graph = mgp_graph { storage, tx, catalog };

            let mut query: *mut mgp_list = ptr::null_mut();
            mgp_list_make_empty(2, ptr::null_mut(), &mut query);
            let mut v: *mut mgp_value = ptr::null_mut();
            mgp_value_make_double(1.0, ptr::null_mut(), &mut v);
            mgp_list_append(query, v);
            mgp_value_destroy(v);
            mgp_value_make_double(0.0, ptr::null_mut(), &mut v);
            mgp_list_append(query, v);
            mgp_value_destroy(v);

            let mut result: *mut mgp_list = ptr::null_mut();
            assert_eq!(mgp_search_vector_index(&graph as *const _ as *mut _, CString::new("Person").unwrap().as_ptr(), query, 5, &mut result), MgpError::NoError);
            assert!(!result.is_null());
            let mut size = 99usize;
            mgp_list_size(result, &mut size);
            assert_eq!(size, 0);

            mgp_list_destroy(query);
            mgp_list_destroy(result);
            drop(Box::from_raw(tx));
            drop(Box::from_raw(catalog));
            drop(Box::from_raw(storage));
        }
    }

    #[test]
    fn date_get_fields_correct() {
        unsafe {
            // 2000-01-02 = epoch + 10957 days (2000 is a leap year)
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let target = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
            let days = (target - epoch).num_days();
            let date = mgp_date { days };

            let mut year: i64 = 0;
            let mut month: i64 = 0;
            let mut day: i64 = 0;
            let mut ts: i64 = 0;

            assert_eq!(mgp_date_get_year(&date as *const _ as *mut _, &mut year), MgpError::NoError);
            assert_eq!(mgp_date_get_month(&date as *const _ as *mut _, &mut month), MgpError::NoError);
            assert_eq!(mgp_date_get_day(&date as *const _ as *mut _, &mut day), MgpError::NoError);
            assert_eq!(mgp_date_timestamp(&date as *const _ as *mut _, &mut ts), MgpError::NoError);

            assert_eq!(year, 2024);
            assert_eq!(month, 6);
            assert_eq!(day, 15);
            assert_eq!(ts, days * 86400);
        }
    }

    #[test]
    fn local_time_get_fields_correct() {
        unsafe {
            // 14:30:45.123456 = 14*3600000000 + 30*60000000 + 45*1000000 + 123456
            let micros: i64 = 14 * 3_600_000_000 + 30 * 60_000_000 + 45 * 1_000_000 + 123_456;
            let time = mgp_local_time { micros };

            let mut hour: i64 = 0;
            let mut minute: i64 = 0;
            let mut second: i64 = 0;
            let mut millisecond: i64 = 0;
            let mut microsecond: i64 = 0;
            let mut ts: i64 = 0;

            assert_eq!(mgp_local_time_get_hour(&time as *const _ as *mut _, &mut hour), MgpError::NoError);
            assert_eq!(mgp_local_time_get_minute(&time as *const _ as *mut _, &mut minute), MgpError::NoError);
            assert_eq!(mgp_local_time_get_second(&time as *const _ as *mut _, &mut second), MgpError::NoError);
            assert_eq!(mgp_local_time_get_millisecond(&time as *const _ as *mut _, &mut millisecond), MgpError::NoError);
            assert_eq!(mgp_local_time_get_microsecond(&time as *const _ as *mut _, &mut microsecond), MgpError::NoError);
            assert_eq!(mgp_local_time_timestamp(&time as *const _ as *mut _, &mut ts), MgpError::NoError);

            assert_eq!(hour, 14);
            assert_eq!(minute, 30);
            assert_eq!(second, 45);
            assert_eq!(millisecond, 123);
            assert_eq!(microsecond, 456);
            assert_eq!(ts, micros);
        }
    }

    #[test]
    fn local_date_time_get_fields_correct() {
        unsafe {
            // 2024-06-15 14:30:45.123456
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let target = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
            let days = (target - epoch).num_days();
            let time_micros: i64 = 14 * 3_600_000_000 + 30 * 60_000_000 + 45 * 1_000_000 + 123_456;
            let micros = days * 86_400_000_000 + time_micros;
            let dt = mgp_local_date_time { micros };

            let mut year: i64 = 0;
            let mut month: i64 = 0;
            let mut day: i64 = 0;
            let mut hour: i64 = 0;
            let mut minute: i64 = 0;
            let mut second: i64 = 0;
            let mut millisecond: i64 = 0;
            let mut microsecond: i64 = 0;
            let mut ts: i64 = 0;

            assert_eq!(mgp_local_date_time_get_year(&dt as *const _ as *mut _, &mut year), MgpError::NoError);
            assert_eq!(mgp_local_date_time_get_month(&dt as *const _ as *mut _, &mut month), MgpError::NoError);
            assert_eq!(mgp_local_date_time_get_day(&dt as *const _ as *mut _, &mut day), MgpError::NoError);
            assert_eq!(mgp_local_date_time_get_hour(&dt as *const _ as *mut _, &mut hour), MgpError::NoError);
            assert_eq!(mgp_local_date_time_get_minute(&dt as *const _ as *mut _, &mut minute), MgpError::NoError);
            assert_eq!(mgp_local_date_time_get_second(&dt as *const _ as *mut _, &mut second), MgpError::NoError);
            assert_eq!(mgp_local_date_time_get_millisecond(&dt as *const _ as *mut _, &mut millisecond), MgpError::NoError);
            assert_eq!(mgp_local_date_time_get_microsecond(&dt as *const _ as *mut _, &mut microsecond), MgpError::NoError);
            assert_eq!(mgp_local_date_time_timestamp(&dt as *const _ as *mut _, &mut ts), MgpError::NoError);

            assert_eq!(year, 2024);
            assert_eq!(month, 6);
            assert_eq!(day, 15);
            assert_eq!(hour, 14);
            assert_eq!(minute, 30);
            assert_eq!(second, 45);
            assert_eq!(millisecond, 123);
            assert_eq!(microsecond, 456);
            assert_eq!(ts, micros);
        }
    }

    #[test]
    fn zoned_date_time_get_fields_correct() {
        unsafe {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let target = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
            let days = (target - epoch).num_days();
            let time_micros: i64 = 14 * 3_600_000_000 + 30 * 60_000_000 + 45 * 1_000_000;
            let micros = days * 86_400_000_000 + time_micros;
            let zdt = mgp_zoned_date_time {
                micros,
                offset: 3600,
                tz: CString::new("Europe/Berlin").unwrap(),
            };

            let mut year: i64 = 0;
            let mut month: i64 = 0;
            let mut day: i64 = 0;
            let mut hour: i64 = 0;
            let mut offset: i64 = 0;
            let mut tz_ptr: *const c_char = ptr::null();

            assert_eq!(mgp_zoned_date_time_get_year(&zdt as *const _ as *mut _, &mut year), MgpError::NoError);
            assert_eq!(mgp_zoned_date_time_get_month(&zdt as *const _ as *mut _, &mut month), MgpError::NoError);
            assert_eq!(mgp_zoned_date_time_get_day(&zdt as *const _ as *mut _, &mut day), MgpError::NoError);
            assert_eq!(mgp_zoned_date_time_get_hour(&zdt as *const _ as *mut _, &mut hour), MgpError::NoError);
            assert_eq!(mgp_zoned_date_time_get_offset(&zdt as *const _ as *mut _, &mut offset), MgpError::NoError);
            assert_eq!(mgp_zoned_date_time_get_timezone(&zdt as *const _ as *mut _, &mut tz_ptr), MgpError::NoError);

            assert_eq!(year, 2024);
            assert_eq!(month, 6);
            assert_eq!(day, 15);
            assert_eq!(hour, 14);
            assert_eq!(offset, 3600);
            assert_eq!(CStr::from_ptr(tz_ptr).to_str().unwrap(), "Europe/Berlin");
        }
    }

    #[test]
    fn date_epoch_zero_fields() {
        unsafe {
            let date = mgp_date { days: 0 };
            let mut year: i64 = 0;
            let mut month: i64 = 0;
            let mut day: i64 = 0;
            assert_eq!(mgp_date_get_year(&date as *const _ as *mut _, &mut year), MgpError::NoError);
            assert_eq!(mgp_date_get_month(&date as *const _ as *mut _, &mut month), MgpError::NoError);
            assert_eq!(mgp_date_get_day(&date as *const _ as *mut _, &mut day), MgpError::NoError);
            assert_eq!(year, 1970);
            assert_eq!(month, 1);
            assert_eq!(day, 1);
        }
    }

    #[test]
    fn local_time_midnight_fields() {
        unsafe {
            let time = mgp_local_time { micros: 0 };
            let mut hour: i64 = 99;
            let mut minute: i64 = 99;
            let mut second: i64 = 99;
            assert_eq!(mgp_local_time_get_hour(&time as *const _ as *mut _, &mut hour), MgpError::NoError);
            assert_eq!(mgp_local_time_get_minute(&time as *const _ as *mut _, &mut minute), MgpError::NoError);
            assert_eq!(mgp_local_time_get_second(&time as *const _ as *mut _, &mut second), MgpError::NoError);
            assert_eq!(hour, 0);
            assert_eq!(minute, 0);
            assert_eq!(second, 0);
        }
    }

    #[test]
    fn search_vector_index_hits_when_index_exists() {
        use mgstorage::storage::Storage;
        use crate::stream::*;
        unsafe {
            let storage = Box::into_raw(Box::new(Storage::new()));
            let catalog = Box::into_raw(Box::new(mgcatalog::Catalog::new()));
            let tx = Box::into_raw(Box::new((*storage).transaction_engine.begin(mgcore::delta::IsolationLevel::SnapshotIsolation)));
            let mut graph = mgp_graph { storage, tx, catalog };

            // Create the vector index
            let label = CString::new("Item").unwrap();
            let mut prop_list: *mut mgp_list = ptr::null_mut();
            mgp_list_make_empty(1, ptr::null_mut(), &mut prop_list);
            let mut prop_val: *mut mgp_value = ptr::null_mut();
            mgp_value_make_string(CString::new("embedding").unwrap().as_ptr(), ptr::null_mut(), &mut prop_val);
            mgp_list_append(prop_list, prop_val);
            mgp_value_destroy(prop_val);
            assert_eq!(mgp_create_vector_index(&mut graph, label.as_ptr(), prop_list, 2, CString::new("cosine").unwrap().as_ptr()), MgpError::NoError);
            mgp_list_destroy(prop_list);

            // Insert vectors into the HNSW index manually via VectorIndexEntry
            {
                let indices = (*storage).vector_indices.read().unwrap();
                let entry = indices.get(&mgcatalog::Catalog::new().label("Item")).unwrap();
                let mut index = entry.index.write().unwrap();
                let mut gid_map = entry.gid_to_node.write().unwrap();
                let n1 = index.insert(&[1.0f32, 0.0f32]);
                let n2 = index.insert(&[0.0f32, 1.0f32]);
                gid_map.insert(Gid::from(1u64), n1);
                gid_map.insert(Gid::from(2u64), n2);
            }

            // Search
            let mut query: *mut mgp_list = ptr::null_mut();
            mgp_list_make_empty(2, ptr::null_mut(), &mut query);
            let mut v: *mut mgp_value = ptr::null_mut();
            mgp_value_make_double(1.0, ptr::null_mut(), &mut v);
            mgp_list_append(query, v);
            mgp_value_destroy(v);
            mgp_value_make_double(0.0, ptr::null_mut(), &mut v);
            mgp_list_append(query, v);
            mgp_value_destroy(v);

            let mut result: *mut mgp_list = ptr::null_mut();
            assert_eq!(mgp_search_vector_index(&mut graph, label.as_ptr(), query, 2, &mut result), MgpError::NoError);
            assert!(!result.is_null());
            let mut size = 0usize;
            mgp_list_size(result, &mut size);
            assert_eq!(size, 2);

            // Inspect first result triple
            let mut first: *mut mgp_value = ptr::null_mut();
            mgp_list_at(result, 0, &mut first);
            let mut triple: *const mgp_list = ptr::null();
            mgp_value_get_list(first, &mut triple);
            let mut triple_len = 0usize;
            mgp_list_size(triple, &mut triple_len);
            assert_eq!(triple_len, 3);

            mgp_list_destroy(query);
            mgp_list_destroy(result);
            drop(Box::from_raw(tx));
            drop(Box::from_raw(catalog));
            drop(Box::from_raw(storage));
        }
    }
}
