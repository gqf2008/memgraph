// ─── PropertyValue — 17-type tagged union ──────────────────────────────────

use std::fmt;

use crate::point::{Point2D, Point3D};
use crate::temporal::{
    Date, Duration, LocalDateTime, LocalTime, ZonedDateTime,
};
use crate::types::{EdgeTypeId, Gid, LabelId};

/// Full Cypher-compatible property value type.
///
/// Matches the C++ PropertyValue tagged union with 17 variants.
/// Boxed variants for large/uncommon types to keep stack size reasonable.
#[derive(Clone, PartialEq, Debug)]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    String(String),
    List(Vec<PropertyValue>),
    Map(Vec<(String, PropertyValue)>),
    Vertex(VertexRef),
    Edge(EdgeRefValue),
    Path(PathValue),
    Date(Date),
    LocalTime(LocalTime),
    LocalDateTime(LocalDateTime),
    ZonedDateTime(ZonedDateTime),
    Duration(Duration),
    Point2D(Point2D),
    Point3D(Point3D),
    Enum {
        enum_type: String,
        value: String,
    },
}

impl PropertyValue {
    /// Returns the type tag matching `mgp_value_type` enum.
    pub fn type_tag(&self) -> u8 {
        match self {
            PropertyValue::Null => 0,
            PropertyValue::Bool(_) => 1,
            PropertyValue::Int(_) => 2,
            PropertyValue::Double(_) => 3,
            PropertyValue::String(_) => 4,
            PropertyValue::List(_) => 5,
            PropertyValue::Map(_) => 6,
            PropertyValue::Vertex(_) => 7,
            PropertyValue::Edge(_) => 8,
            PropertyValue::Path(_) => 9,
            PropertyValue::Date(_) => 10,
            PropertyValue::LocalTime(_) => 11,
            PropertyValue::LocalDateTime(_) => 12,
            PropertyValue::Duration(_) => 13,
            PropertyValue::ZonedDateTime(_) => 14,
            PropertyValue::Point2D(_) => 15,
            PropertyValue::Point3D(_) => 16,
            PropertyValue::Enum { .. } => 17,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, PropertyValue::Null)
    }

    /// Cypher truthiness: Null, false, 0, empty string, empty list are falsy.
    pub fn is_truthy(&self) -> bool {
        match self {
            PropertyValue::Null => false,
            PropertyValue::Bool(b) => *b,
            PropertyValue::Int(n) => *n != 0,
            PropertyValue::String(s) => !s.is_empty(),
            PropertyValue::List(l) => !l.is_empty(),
            _ => true,
        }
    }
}

impl fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyValue::Null => write!(f, "null"),
            PropertyValue::Bool(v) => write!(f, "{}", v),
            PropertyValue::Int(v) => write!(f, "{}", v),
            PropertyValue::Double(v) => write!(f, "{}", v),
            PropertyValue::String(v) => write!(f, "\"{}\"", v),
            PropertyValue::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            PropertyValue::Map(entries) => {
                write!(f, "{{")?;
                for (i, (k, v)) in entries.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", k, v)?;
                }
                write!(f, "}}")
            }
            PropertyValue::Vertex(v) => write!(f, "(:{})", v.gid),
            PropertyValue::Edge(e) => write!(f, "[:{}]", e.gid),
            PropertyValue::Path(_) => write!(f, "<path>"),
            PropertyValue::Date(d) => write!(f, "{}", d),
            PropertyValue::LocalTime(t) => write!(f, "{}", t),
            PropertyValue::LocalDateTime(dt) => write!(f, "{}", dt),
            PropertyValue::ZonedDateTime(dt) => write!(f, "{}", dt),
            PropertyValue::Duration(d) => write!(f, "{}", d),
            PropertyValue::Point2D(p) => write!(f, "{}", p),
            PropertyValue::Point3D(p) => write!(f, "{}", p),
            PropertyValue::Enum { enum_type, value } => {
                write!(f, "{}.{}", enum_type, value)
            }
        }
    }
}

// ─── Lightweight references (for query results, not storage) ─────────────────

#[derive(Clone, PartialEq, Debug)]
pub struct VertexRef {
    pub gid: Gid,
    pub labels: Vec<LabelId>,
    pub properties: crate::property_store::PropertyStore,
}

impl VertexRef {
    pub fn new(gid: Gid, labels: Vec<LabelId>, properties: crate::property_store::PropertyStore) -> Self {
        Self { gid, labels, properties }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct EdgeRefValue {
    pub gid: Gid,
    pub edge_type: EdgeTypeId,
    pub from_vertex: Gid,
    pub to_vertex: Gid,
    pub properties: crate::property_store::PropertyStore,
}

impl EdgeRefValue {
    pub fn new(gid: Gid, edge_type: EdgeTypeId, from_vertex: Gid, to_vertex: Gid, properties: crate::property_store::PropertyStore) -> Self {
        Self { gid, edge_type, from_vertex, to_vertex, properties }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct PathValue {
    pub vertices: Vec<VertexRef>,
    pub edges: Vec<EdgeRefValue>,
}

impl PathValue {
    pub fn new(start: VertexRef) -> Self {
        Self {
            vertices: vec![start],
            edges: Vec::new(),
        }
    }

    pub fn add_edge(&mut self, edge: EdgeRefValue) {
        self.edges.push(edge);
    }

    pub fn add_vertex(&mut self, vertex: VertexRef) {
        self.vertices.push(vertex);
    }

    pub fn end(&self) -> &VertexRef {
        self.vertices.last().unwrap_or(&self.vertices[0])
    }
}
