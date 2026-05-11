//! Reader for C++ Memgraph snapshot/WAL format (versions 14-34).
//!
//! C++ format uses marker-based encoding where each value is prefixed
//! by a single-byte `Marker` identifying the type, followed by the data.
//! This module maps C++ markers to Rust SLK-equivalent types.

use mgcore::point::Crs;
use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
use crate::snapshot::{NameMapperSnapshot, SnapshotData, VertexSnapshotEntry, EdgeSnapshotEntry};

/// C++ wire format markers (memgraph::wire_format::Marker).
#[allow(unused)]
mod marker {
    pub const TYPE_NULL: u8 = 0x10;
    pub const TYPE_BOOL: u8 = 0x11;
    pub const TYPE_INT: u8 = 0x12;
    pub const TYPE_DOUBLE: u8 = 0x13;
    pub const TYPE_STRING: u8 = 0x14;
    pub const TYPE_LIST: u8 = 0x15;
    pub const TYPE_MAP: u8 = 0x16;
    pub const TYPE_PROPERTY_VALUE: u8 = 0x17;
    pub const TYPE_TEMPORAL_DATA: u8 = 0x18;
    pub const TYPE_ZONED_TEMPORAL_DATA: u8 = 0x19;
    pub const TYPE_ENUM: u8 = 0x1a;
    pub const TYPE_POINT_2D: u8 = 0x1b;
    pub const TYPE_POINT_3D: u8 = 0x1c;

    pub const SECTION_VERTEX: u8 = 0x20;
    pub const SECTION_EDGE: u8 = 0x21;
    pub const SECTION_MAPPER: u8 = 0x22;
    pub const SECTION_METADATA: u8 = 0x23;
    pub const SECTION_INDICES: u8 = 0x24;
    pub const SECTION_CONSTRAINTS: u8 = 0x25;
    pub const SECTION_DELTA: u8 = 0x26;
    pub const SECTION_EPOCH_HISTORY: u8 = 0x27;
    pub const SECTION_EDGE_INDICES: u8 = 0x28;
    pub const SECTION_ENUMS: u8 = 0x29;
    pub const SECTION_TTL: u8 = 0x2a;
    pub const SECTION_DESCRIPTIONS: u8 = 0x2b;
    pub const SECTION_OFFSETS: u8 = 0x42;

    pub const VALUE_FALSE: u8 = 0xf0;
    pub const VALUE_TRUE: u8 = 0xf1;
}

/// Error returned by C++ format parsing.
#[derive(Debug)]
pub enum CppFormatError {
    Io(std::io::Error),
    Corrupt(String),
    UnsupportedVersion(u64),
}

impl std::fmt::Display for CppFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CppFormatError::Io(e) => write!(f, "I/O error: {}", e),
            CppFormatError::Corrupt(s) => write!(f, "corrupt C++ data: {}", s),
            CppFormatError::UnsupportedVersion(v) => write!(f, "unsupported C++ version: {}", v),
        }
    }
}

impl std::error::Error for CppFormatError {}

impl From<std::io::Error> for CppFormatError {
    fn from(e: std::io::Error) -> Self {
        CppFormatError::Io(e)
    }
}

/// Minimal reader for C++ marker-based encoding.
struct CppReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> CppReader<'a> {
    fn new(data: &'a [u8]) -> Self { Self { data, pos: 0 } }

    fn remaining(&self) -> usize { self.data.len() - self.pos }

    fn read_byte(&mut self) -> Result<u8, CppFormatError> {
        if self.pos >= self.data.len() {
            return Err(CppFormatError::Corrupt("unexpected end of data".into()));
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn read_u64_le(&mut self) -> Result<u64, CppFormatError> {
        if self.pos + 8 > self.data.len() {
            return Err(CppFormatError::Corrupt("unexpected end reading u64".into()));
        }
        let bytes: [u8; 8] = self.data[self.pos..self.pos + 8].try_into().unwrap();
        self.pos += 8;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_i64_le(&mut self) -> Result<i64, CppFormatError> {
        self.read_u64_le().map(|v| v as i64)
    }

    fn read_f64_le(&mut self) -> Result<f64, CppFormatError> {
        if self.pos + 8 > self.data.len() {
            return Err(CppFormatError::Corrupt("unexpected end reading f64".into()));
        }
        let bytes: [u8; 8] = self.data[self.pos..self.pos + 8].try_into().unwrap();
        self.pos += 8;
        Ok(f64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> Result<String, CppFormatError> {
        let len = self.read_u64_le()? as usize;
        if self.pos + len > self.data.len() {
            return Err(CppFormatError::Corrupt("string length exceeds data".into()));
        }
        let s = String::from_utf8_lossy(&self.data[self.pos..self.pos + len]).into_owned();
        self.pos += len;
        Ok(s)
    }

    fn read_property_value(&mut self) -> Result<PropertyValue, CppFormatError> {
        let marker = self.read_byte()?;
        match marker {
            marker::TYPE_NULL => Ok(PropertyValue::Null),
            marker::TYPE_BOOL => Ok(PropertyValue::Bool(self.read_byte()? != 0)),
            marker::TYPE_INT => Ok(PropertyValue::Int(self.read_i64_le()?)),
            marker::TYPE_DOUBLE => Ok(PropertyValue::Double(self.read_f64_le()?)),
            marker::TYPE_STRING => Ok(PropertyValue::String(self.read_string()?)),
            marker::TYPE_LIST => {
                let len = self.read_u64_le()? as usize;
                let mut items = Vec::with_capacity(len.min(10000));
                for _ in 0..len {
                    items.push(self.read_property_value()?);
                }
                Ok(PropertyValue::List(items))
            }
            marker::TYPE_MAP => {
                let len = self.read_u64_le()? as usize;
                let mut entries = Vec::with_capacity(len.min(10000));
                for _ in 0..len {
                    let key = self.read_string()?;
                    let val = self.read_property_value()?;
                    entries.push((key, val));
                }
                Ok(PropertyValue::Map(entries))
            }
            marker::VALUE_FALSE => Ok(PropertyValue::Bool(false)),
            marker::VALUE_TRUE => Ok(PropertyValue::Bool(true)),
            other => Err(CppFormatError::Corrupt(format!(
                "unrecognized C++ property value marker: 0x{:02x} at offset {}",
                other, self.pos - 1
            )))
        }
    }

    /// Read a (string, u64) pair used in mapper sections.
    fn read_name_mapping(&mut self) -> Result<(String, u64), CppFormatError> {
        let name = self.read_string()?;
        let id = self.read_u64_le()?;
        Ok((name, id))
    }
}

/// Read a C++-format snapshot (versions 14-34) and convert to Rust SnapshotData.
pub fn read_cpp_snapshot(data: &[u8], version: u64) -> Result<SnapshotData, CppFormatError> {
    if version < 14 || version > 34 {
        return Err(CppFormatError::UnsupportedVersion(version));
    }

    // C++ snapshot structure: [magic 4B][version u64 LE][section VERTEX][section EDGE]...
    if data.len() < 12 {
        return Err(CppFormatError::Corrupt("too short".into()));
    }

    let mut reader = CppReader::new(&data[12..]);
    let mut vertices = Vec::new();
    let mut edges = Vec::new();
    let mut name_mapper = NameMapperSnapshot {
        labels: vec![],
        properties: vec![],
        edge_types: vec![],
    };

    while reader.remaining() > 0 {
        let section_marker = match reader.read_byte() {
            Ok(m) => m,
            Err(_) => break,
        };

        match section_marker {
            marker::SECTION_VERTEX => {
                let count = reader.read_u64_le()? as usize;
                for _ in 0..count {
                    let gid = Gid::from(reader.read_u64_le()?);
                    let label_count = reader.read_u64_le()? as usize;
                    let mut labels = Vec::with_capacity(label_count);
                    for _ in 0..label_count {
                        labels.push(LabelId::from(reader.read_u64_le()? as u32));
                    }
                    let prop_count = reader.read_u64_le()? as usize;
                    let mut properties = Vec::with_capacity(prop_count);
                    for _ in 0..prop_count {
                        let key = PropertyId::from(reader.read_u64_le()? as u32);
                        let value = reader.read_property_value()?;
                        properties.push((key, value));
                    }
                    vertices.push(VertexSnapshotEntry { gid, labels, properties });
                }
            }
            marker::SECTION_EDGE => {
                let count = reader.read_u64_le()? as usize;
                for _ in 0..count {
                    let gid = Gid::from(reader.read_u64_le()?);
                    let from_vertex = Gid::from(reader.read_u64_le()?);
                    let to_vertex = Gid::from(reader.read_u64_le()?);
                    let edge_type = EdgeTypeId::from(reader.read_u64_le()? as u32);
                    let prop_count = reader.read_u64_le()? as usize;
                    let mut properties = Vec::with_capacity(prop_count);
                    for _ in 0..prop_count {
                        let key = PropertyId::from(reader.read_u64_le()? as u32);
                        let value = reader.read_property_value()?;
                        properties.push((key, value));
                    }
                    edges.push(EdgeSnapshotEntry { gid, from_vertex, to_vertex, edge_type, properties });
                }
            }
            marker::SECTION_MAPPER => {
                // Format: [label_count: u64][(name, id)*N][prop_count: u64][(name, id)*N][edge_type_count: u64][(name, id)*N]
                let label_count = reader.read_u64_le()? as usize;
                for _ in 0..label_count {
                    let (name, id) = reader.read_name_mapping()?;
                    name_mapper.labels.push((name, LabelId::from(id as u32)));
                }
                let prop_count = reader.read_u64_le()? as usize;
                for _ in 0..prop_count {
                    let (name, id) = reader.read_name_mapping()?;
                    name_mapper.properties.push((name, PropertyId::from(id as u32)));
                }
                let edge_type_count = reader.read_u64_le()? as usize;
                for _ in 0..edge_type_count {
                    let (name, id) = reader.read_name_mapping()?;
                    name_mapper.edge_types.push((name, EdgeTypeId::from(id as u32)));
                }
            }
            marker::SECTION_METADATA => {
                // Format: [epoch_id: u64][last_commit_timestamp: u64]
                // Skip for now — not needed for basic recovery
                let _epoch_id = reader.read_u64_le()?;
                let _last_commit_timestamp = reader.read_u64_le()?;
            }
            marker::SECTION_INDICES => {
                // Format: [label_index_count: u64][label_id*N][label_prop_index_count: u64][(label_id, prop_id)*N]
                let label_index_count = reader.read_u64_le()? as usize;
                for _ in 0..label_index_count {
                    let _label_id = reader.read_u64_le()?;
                }
                let label_prop_index_count = reader.read_u64_le()? as usize;
                for _ in 0..label_prop_index_count {
                    let _label_id = reader.read_u64_le()?;
                    let _prop_id = reader.read_u64_le()?;
                }
            }
            marker::SECTION_CONSTRAINTS => {
                // Format: [existence_count: u64][(label_id, prop_id)*N][unique_count: u64][...][type_count: u64][...]
                let existence_count = reader.read_u64_le()? as usize;
                for _ in 0..existence_count {
                    let _label_id = reader.read_u64_le()?;
                    let _prop_id = reader.read_u64_le()?;
                }
                let unique_count = reader.read_u64_le()? as usize;
                for _ in 0..unique_count {
                    let _label_id = reader.read_u64_le()?;
                    let prop_count = reader.read_u64_le()? as usize;
                    for _ in 0..prop_count {
                        let _prop_id = reader.read_u64_le()?;
                    }
                }
                let type_count = reader.read_u64_le()? as usize;
                for _ in 0..type_count {
                    let _label_id = reader.read_u64_le()?;
                    let _prop_id = reader.read_u64_le()?;
                    let _type_tag = reader.read_u64_le()?;
                }
            }
            marker::SECTION_EPOCH_HISTORY => {
                // Format: [count: u64][(epoch_id, timestamp)*N]
                let count = reader.read_u64_le()? as usize;
                for _ in 0..count {
                    let _epoch_id = reader.read_u64_le()?;
                    let _timestamp = reader.read_u64_le()?;
                }
            }
            marker::SECTION_EDGE_INDICES => {
                // Format: [edge_type_index_count: u64][edge_type_id*N][edge_prop_index_count: u64][...]
                let edge_type_index_count = reader.read_u64_le()? as usize;
                for _ in 0..edge_type_index_count {
                    let _edge_type_id = reader.read_u64_le()?;
                }
                let edge_prop_index_count = reader.read_u64_le()? as usize;
                for _ in 0..edge_prop_index_count {
                    let _edge_type_id = reader.read_u64_le()?;
                    let _prop_id = reader.read_u64_le()?;
                }
            }
            marker::SECTION_ENUMS => {
                // Format: [count: u64][(name, value_count, values)*N]
                let count = reader.read_u64_le()? as usize;
                for _ in 0..count {
                    let _name = reader.read_string()?;
                    let value_count = reader.read_u64_le()? as usize;
                    for _ in 0..value_count {
                        let _value = reader.read_string()?;
                    }
                }
            }
            marker::SECTION_TTL => {
                // Format: [count: u64][(label_id, ttl_ms)*N]
                let count = reader.read_u64_le()? as usize;
                for _ in 0..count {
                    let _label_id = reader.read_u64_le()?;
                    let _ttl_ms = reader.read_u64_le()?;
                }
            }
            marker::SECTION_DESCRIPTIONS => {
                // Format: [count: u64][(key, value)*N]
                let count = reader.read_u64_le()? as usize;
                for _ in 0..count {
                    let _key = reader.read_string()?;
                    let _value = reader.read_string()?;
                }
            }
            marker::SECTION_OFFSETS => {
                // Format: [count: u64][(section_marker, offset)*N]
                let count = reader.read_u64_le()? as usize;
                for _ in 0..count {
                    let _section_marker = reader.read_byte()?;
                    let _offset = reader.read_u64_le()?;
                }
            }
            marker::SECTION_DELTA => {
                // Delta section in snapshots — skip for now
                let count = reader.read_u64_le()? as usize;
                for _ in 0..count {
                    // Delta record parsing would go here
                    let _timestamp = reader.read_u64_le()?;
                    let _gid = reader.read_u64_le()?;
                }
            }
            other => {
                // Unknown section marker. We can't safely skip it without
                // knowing its length, so we stop parsing. VERTEX, EDGE,
                // and MAPPER sections have already been handled.
                return Err(CppFormatError::Corrupt(format!(
                    "unknown section marker: 0x{:02x} at offset {}",
                    other, reader.pos - 1
                )));
            }
        }
    }

    Ok(SnapshotData {
        name_mapper,
        vertices,
        edges,
    })
}

// ─── C++ format writers (Rust → C++) ───────────────────────────────────────

/// Helper to write a PropertyValue in C++ marker-based wire format.
pub fn write_cpp_property_value(buf: &mut Vec<u8>, pv: &PropertyValue) {
    match pv {
        PropertyValue::Null => buf.push(marker::TYPE_NULL),
        PropertyValue::Bool(true) => buf.push(marker::VALUE_TRUE),
        PropertyValue::Bool(false) => buf.push(marker::VALUE_FALSE),
        PropertyValue::Int(n) => {
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        PropertyValue::Double(f) => {
            buf.push(marker::TYPE_DOUBLE);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        PropertyValue::String(s) => {
            buf.push(marker::TYPE_STRING);
            buf.extend_from_slice(&(s.len() as u64).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        PropertyValue::List(items) => {
            buf.push(marker::TYPE_LIST);
            buf.extend_from_slice(&(items.len() as u64).to_le_bytes());
            for item in items {
                write_cpp_property_value(buf, item);
            }
        }
        PropertyValue::Map(entries) => {
            buf.push(marker::TYPE_MAP);
            buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
            for (k, v) in entries {
                buf.extend_from_slice(&(k.len() as u64).to_le_bytes());
                buf.extend_from_slice(k.as_bytes());
                write_cpp_property_value(buf, v);
            }
        }
        PropertyValue::Date(d) => {
            buf.push(marker::TYPE_TEMPORAL_DATA);
            buf.push(marker::TYPE_TEMPORAL_DATA);
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&0u64.to_le_bytes()); // type_tag = 0 (Date)
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&(d.days_since_epoch as i64).to_le_bytes());
        }
        PropertyValue::LocalTime(t) => {
            buf.push(marker::TYPE_TEMPORAL_DATA);
            buf.push(marker::TYPE_TEMPORAL_DATA);
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&1u64.to_le_bytes()); // type_tag = 1 (LocalTime)
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&(t.microseconds as i64).to_le_bytes());
        }
        PropertyValue::LocalDateTime(dt) => {
            buf.push(marker::TYPE_TEMPORAL_DATA);
            buf.push(marker::TYPE_TEMPORAL_DATA);
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&2u64.to_le_bytes()); // type_tag = 2 (LocalDateTime)
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&(dt.microseconds as i64).to_le_bytes());
        }
        PropertyValue::Duration(d) => {
            buf.push(marker::TYPE_TEMPORAL_DATA);
            buf.push(marker::TYPE_TEMPORAL_DATA);
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&3u64.to_le_bytes()); // type_tag = 3 (Duration)
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&(d.microseconds as i64).to_le_bytes());
        }
        PropertyValue::ZonedDateTime(zdt) => {
            buf.push(marker::TYPE_ZONED_TEMPORAL_DATA);
            buf.push(marker::TYPE_ZONED_TEMPORAL_DATA);
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&0u64.to_le_bytes()); // type_tag
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&(zdt.utc_microseconds as i64).to_le_bytes());
            if zdt.timezone.is_empty() {
                buf.push(marker::TYPE_INT);
                buf.extend_from_slice(&(zdt.offset_minutes as u64).to_le_bytes());
            } else {
                buf.push(marker::TYPE_STRING);
                buf.extend_from_slice(&(zdt.timezone.len() as u64).to_le_bytes());
                buf.extend_from_slice(zdt.timezone.as_bytes());
            }
        }
        PropertyValue::Enum { enum_type, value } => {
            buf.push(marker::TYPE_ENUM);
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&(enum_type.len() as u64).to_le_bytes());
            buf.extend_from_slice(enum_type.as_bytes());
            buf.push(marker::TYPE_INT);
            buf.extend_from_slice(&(value.len() as u64).to_le_bytes());
            buf.extend_from_slice(value.as_bytes());
        }
        PropertyValue::Point2D(p) => {
            buf.push(marker::TYPE_POINT_2D);
            buf.push(marker::TYPE_INT);
            let srid = match p.crs {
                Crs::WGS84 => 4326u64,
                Crs::Cartesian2D => 7203u64,
                Crs::Cartesian3D => 9157u64,
                Crs::WGS843D => 4979u64,
            };
            buf.extend_from_slice(&srid.to_le_bytes());
            buf.push(marker::TYPE_DOUBLE);
            buf.extend_from_slice(&p.x.to_le_bytes());
            buf.push(marker::TYPE_DOUBLE);
            buf.extend_from_slice(&p.y.to_le_bytes());
        }
        PropertyValue::Point3D(p) => {
            buf.push(marker::TYPE_POINT_3D);
            buf.push(marker::TYPE_INT);
            let srid = match p.crs {
                Crs::WGS84 => 4326u64,
                Crs::Cartesian2D => 7203u64,
                Crs::Cartesian3D => 9157u64,
                Crs::WGS843D => 4979u64,
            };
            buf.extend_from_slice(&srid.to_le_bytes());
            buf.push(marker::TYPE_DOUBLE);
            buf.extend_from_slice(&p.x.to_le_bytes());
            buf.push(marker::TYPE_DOUBLE);
            buf.extend_from_slice(&p.y.to_le_bytes());
            buf.push(marker::TYPE_DOUBLE);
            buf.extend_from_slice(&p.z.to_le_bytes());
        }
        PropertyValue::Vertex(_) | PropertyValue::Edge(_) | PropertyValue::Path(_) => {
            // Serialize as map with metadata for compatibility
            buf.push(marker::TYPE_MAP);
            buf.extend_from_slice(&1u64.to_le_bytes());
            buf.extend_from_slice(&3u64.to_le_bytes());
            buf.extend_from_slice(b"id");
            match pv {
                PropertyValue::Vertex(vr) => {
                    buf.push(marker::TYPE_INT);
                    buf.extend_from_slice(&(vr.gid.as_int() as i64).to_le_bytes());
                }
                PropertyValue::Edge(er) => {
                    buf.push(marker::TYPE_INT);
                    buf.extend_from_slice(&(er.gid.as_int() as i64).to_le_bytes());
                }
                PropertyValue::Path(_) => {
                    buf.push(marker::TYPE_LIST);
                    buf.extend_from_slice(&0u64.to_le_bytes());
                }
                _ => unreachable!(),
            }
        }
    }
}

/// Write a SnapshotData in C++ format (version 20).
///
/// Output structure:
/// `[MGsn magic: 4B][version: u64 LE][SECTION_MAPPER][SECTION_VERTEX][SECTION_EDGE]`
pub fn write_cpp_snapshot(data: &SnapshotData) -> Vec<u8> {
    let mut buf = vec![b'M', b'G', b's', b'n'];
    buf.extend_from_slice(&20u64.to_le_bytes());

    // SECTION_MAPPER
    buf.push(marker::SECTION_MAPPER);
    buf.extend_from_slice(&(data.name_mapper.labels.len() as u64).to_le_bytes());
    for (name, id) in &data.name_mapper.labels {
        buf.extend_from_slice(&(name.len() as u64).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&(id.as_uint() as u64).to_le_bytes());
    }
    buf.extend_from_slice(&(data.name_mapper.properties.len() as u64).to_le_bytes());
    for (name, id) in &data.name_mapper.properties {
        buf.extend_from_slice(&(name.len() as u64).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&(id.as_uint() as u64).to_le_bytes());
    }
    buf.extend_from_slice(&(data.name_mapper.edge_types.len() as u64).to_le_bytes());
    for (name, id) in &data.name_mapper.edge_types {
        buf.extend_from_slice(&(name.len() as u64).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&(id.as_uint() as u64).to_le_bytes());
    }

    // SECTION_VERTEX
    buf.push(marker::SECTION_VERTEX);
    buf.extend_from_slice(&(data.vertices.len() as u64).to_le_bytes());
    for v in &data.vertices {
        buf.extend_from_slice(&(v.gid.as_int() as u64).to_le_bytes());
        buf.extend_from_slice(&(v.labels.len() as u64).to_le_bytes());
        for label in &v.labels {
            buf.extend_from_slice(&(label.as_uint() as u64).to_le_bytes());
        }
        buf.extend_from_slice(&(v.properties.len() as u64).to_le_bytes());
        for (key, value) in &v.properties {
            buf.extend_from_slice(&(key.as_uint() as u64).to_le_bytes());
            write_cpp_property_value(&mut buf, value);
        }
    }

    // SECTION_EDGE
    buf.push(marker::SECTION_EDGE);
    buf.extend_from_slice(&(data.edges.len() as u64).to_le_bytes());
    for e in &data.edges {
        buf.extend_from_slice(&(e.gid.as_int() as u64).to_le_bytes());
        buf.extend_from_slice(&(e.from_vertex.as_int() as u64).to_le_bytes());
        buf.extend_from_slice(&(e.to_vertex.as_int() as u64).to_le_bytes());
        buf.extend_from_slice(&(e.edge_type.as_uint() as u64).to_le_bytes());
        buf.extend_from_slice(&(e.properties.len() as u64).to_le_bytes());
        for (key, value) in &e.properties {
            buf.extend_from_slice(&(key.as_uint() as u64).to_le_bytes());
            write_cpp_property_value(&mut buf, value);
        }
    }

    buf
}

/// Write a WAL record in C++ format (version 20).
///
/// Each record: `[timestamp: u64 LE][delta_type: u8][payload_len: u64 LE][payload]`
pub fn write_cpp_wal_record(buf: &mut Vec<u8>, timestamp: u64, delta_type: u8, payload: &[u8]) {
    buf.extend_from_slice(&timestamp.to_le_bytes());
    buf.push(delta_type);
    buf.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    buf.extend_from_slice(payload);
}

/// Build a complete C++ WAL file from records.
///
/// Output: `[MGwl magic: 4B][version: u64 LE][records...]`
pub fn write_cpp_wal(records: &[(u64, u8, Vec<u8>)]) -> Vec<u8> {
    let mut buf = vec![b'M', b'G', b'w', b'l'];
    buf.extend_from_slice(&20u64.to_le_bytes());
    for (timestamp, delta_type, payload) in records {
        write_cpp_wal_record(&mut buf, *timestamp, *delta_type, payload);
    }
    buf
}

/// Read a C++-format WAL file (versions 14-34).
/// Returns a vector of (timestamp, delta_type, payload) tuples as a skeleton.
pub fn read_cpp_wal(data: &[u8], version: u64) -> Result<Vec<(u64, u8, Vec<u8>)>, CppFormatError> {
    if version < 14 || version > 34 {
        return Err(CppFormatError::UnsupportedVersion(version));
    }
    if data.len() < 12 {
        return Err(CppFormatError::Corrupt("WAL too short".into()));
    }

    let mut reader = CppReader::new(&data[12..]);
    let mut records = Vec::new();

    while reader.remaining() > 0 {
        // WAL record: [timestamp: u64][delta_type: u8][payload_len: u64][payload: bytes]
        let timestamp = match reader.read_u64_le() {
            Ok(ts) => ts,
            Err(_) => break,
        };
        let delta_type = reader.read_byte()?;
        let payload_len = reader.read_u64_le()? as usize;
        if reader.remaining() < payload_len {
            return Err(CppFormatError::Corrupt("WAL payload truncated".into()));
        }
        let payload = reader.data[reader.pos..reader.pos + payload_len].to_vec();
        reader.pos += payload_len;
        records.push((timestamp, delta_type, payload));
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_cpp_snapshot_empty() {
        // Magic (4 bytes) + version (8 bytes) = 12 bytes minimum
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());
        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert!(result.vertices.is_empty());
        assert!(result.edges.is_empty());
        assert!(result.name_mapper.labels.is_empty());
    }

    #[test]
    fn test_read_cpp_snapshot_unsupported_version() {
        let data = vec![0u8; 12];
        assert!(
            matches!(read_cpp_snapshot(&data, 5), Err(CppFormatError::UnsupportedVersion(5)))
        );
        assert!(
            matches!(read_cpp_snapshot(&data, 99), Err(CppFormatError::UnsupportedVersion(99)))
        );
    }

    #[test]
    fn test_read_cpp_snapshot_vertex_section() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        // SECTION_VERTEX
        data.push(marker::SECTION_VERTEX);
        data.extend_from_slice(&1u64.to_le_bytes()); // count = 1
        data.extend_from_slice(&42u64.to_le_bytes()); // gid
        data.extend_from_slice(&1u64.to_le_bytes()); // label_count
        data.extend_from_slice(&7u64.to_le_bytes()); // label id
        data.extend_from_slice(&1u64.to_le_bytes()); // prop_count
        data.extend_from_slice(&3u64.to_le_bytes()); // prop_id
        data.push(marker::TYPE_INT);
        data.extend_from_slice(&99i64.to_le_bytes());

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert_eq!(result.vertices.len(), 1);
        assert_eq!(result.vertices[0].gid, Gid::from(42u64));
        assert_eq!(result.vertices[0].labels, vec![LabelId::from(7u32)]);
        assert_eq!(result.vertices[0].properties.len(), 1);
        assert_eq!(result.vertices[0].properties[0].0, PropertyId::from(3u32));
        assert!(matches!(result.vertices[0].properties[0].1, PropertyValue::Int(99)));
    }

    #[test]
    fn test_read_cpp_snapshot_edge_section() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        // SECTION_EDGE
        data.push(marker::SECTION_EDGE);
        data.extend_from_slice(&1u64.to_le_bytes()); // count
        data.extend_from_slice(&10u64.to_le_bytes()); // gid
        data.extend_from_slice(&1u64.to_le_bytes()); // from
        data.extend_from_slice(&2u64.to_le_bytes()); // to
        data.extend_from_slice(&5u64.to_le_bytes()); // edge_type
        data.extend_from_slice(&0u64.to_le_bytes()); // prop_count

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].gid, Gid::from(10u64));
        assert_eq!(result.edges[0].from_vertex, Gid::from(1u64));
        assert_eq!(result.edges[0].to_vertex, Gid::from(2u64));
        assert_eq!(result.edges[0].edge_type, EdgeTypeId::from(5u32));
    }

    #[test]
    fn test_read_cpp_snapshot_mapper_section() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        // SECTION_MAPPER
        data.push(marker::SECTION_MAPPER);
        // Labels
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&5u64.to_le_bytes()); // "hello" len
        data.extend_from_slice(b"hello");
        data.extend_from_slice(&1u64.to_le_bytes()); // label id
        // Properties
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&4u64.to_le_bytes()); // "name" len
        data.extend_from_slice(b"name");
        data.extend_from_slice(&2u64.to_le_bytes()); // prop id
        // Edge types
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&5u64.to_le_bytes()); // "KNOWS" len
        data.extend_from_slice(b"KNOWS");
        data.extend_from_slice(&3u64.to_le_bytes()); // edge type id

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert_eq!(result.name_mapper.labels.len(), 1);
        assert_eq!(result.name_mapper.labels[0].0, "hello");
        assert_eq!(result.name_mapper.labels[0].1, LabelId::from(1u32));
        assert_eq!(result.name_mapper.properties.len(), 1);
        assert_eq!(result.name_mapper.properties[0].0, "name");
        assert_eq!(result.name_mapper.properties[0].1, PropertyId::from(2u32));
        assert_eq!(result.name_mapper.edge_types.len(), 1);
        assert_eq!(result.name_mapper.edge_types[0].0, "KNOWS");
        assert_eq!(result.name_mapper.edge_types[0].1, EdgeTypeId::from(3u32));
    }

    #[test]
    fn test_read_cpp_snapshot_metadata_section() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_METADATA);
        data.extend_from_slice(&7u64.to_le_bytes()); // epoch_id
        data.extend_from_slice(&12345u64.to_le_bytes()); // last_commit_timestamp

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert!(result.vertices.is_empty());
    }

    #[test]
    fn test_read_cpp_snapshot_indices_section() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_INDICES);
        data.extend_from_slice(&1u64.to_le_bytes()); // label_index_count
        data.extend_from_slice(&5u64.to_le_bytes()); // label_id
        data.extend_from_slice(&1u64.to_le_bytes()); // label_prop_index_count
        data.extend_from_slice(&5u64.to_le_bytes()); // label_id
        data.extend_from_slice(&2u64.to_le_bytes()); // prop_id

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert!(result.vertices.is_empty());
    }

    #[test]
    fn test_read_cpp_snapshot_constraints_section() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_CONSTRAINTS);
        data.extend_from_slice(&1u64.to_le_bytes()); // existence_count
        data.extend_from_slice(&1u64.to_le_bytes()); // label_id
        data.extend_from_slice(&2u64.to_le_bytes()); // prop_id
        data.extend_from_slice(&0u64.to_le_bytes()); // unique_count
        data.extend_from_slice(&0u64.to_le_bytes()); // type_count

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert!(result.vertices.is_empty());
    }

    #[test]
    fn test_read_cpp_snapshot_enums_section() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_ENUMS);
        data.extend_from_slice(&1u64.to_le_bytes()); // count
        data.extend_from_slice(&6u64.to_le_bytes()); // "Status" len
        data.extend_from_slice(b"Status");
        data.extend_from_slice(&2u64.to_le_bytes()); // value_count
        data.extend_from_slice(&4u64.to_le_bytes()); // "Open" len
        data.extend_from_slice(b"Open");
        data.extend_from_slice(&6u64.to_le_bytes()); // "Closed" len
        data.extend_from_slice(b"Closed");

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert!(result.vertices.is_empty());
    }

    #[test]
    fn test_read_cpp_snapshot_ttl_section() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_TTL);
        data.extend_from_slice(&1u64.to_le_bytes()); // count
        data.extend_from_slice(&1u64.to_le_bytes()); // label_id
        data.extend_from_slice(&3600000u64.to_le_bytes()); // ttl_ms

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert!(result.vertices.is_empty());
    }

    #[test]
    fn test_read_cpp_snapshot_descriptions_section() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_DESCRIPTIONS);
        data.extend_from_slice(&1u64.to_le_bytes()); // count
        data.extend_from_slice(&4u64.to_le_bytes()); // "key1" len
        data.extend_from_slice(b"key1");
        data.extend_from_slice(&6u64.to_le_bytes()); // "value1" len
        data.extend_from_slice(b"value1");

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert!(result.vertices.is_empty());
    }

    #[test]
    fn test_read_cpp_snapshot_offsets_section() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_OFFSETS);
        data.extend_from_slice(&1u64.to_le_bytes()); // count
        data.push(marker::SECTION_VERTEX);
        data.extend_from_slice(&100u64.to_le_bytes()); // offset

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert!(result.vertices.is_empty());
    }

    #[test]
    fn test_read_cpp_wal_basic() {
        let mut data = vec![b'M', b'G', b'w', b'l'];
        data.extend_from_slice(&20u64.to_le_bytes());

        // Record 1
        data.extend_from_slice(&100u64.to_le_bytes()); // timestamp
        data.push(0x01); // delta_type
        data.extend_from_slice(&4u64.to_le_bytes()); // payload_len
        data.extend_from_slice(b"test");

        // Record 2
        data.extend_from_slice(&200u64.to_le_bytes());
        data.push(0x02);
        data.extend_from_slice(&2u64.to_le_bytes());
        data.extend_from_slice(b"ab");

        let records = read_cpp_wal(&data, 20).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, 100);
        assert_eq!(records[0].1, 0x01);
        assert_eq!(records[0].2, b"test");
        assert_eq!(records[1].0, 200);
        assert_eq!(records[1].1, 0x02);
        assert_eq!(records[1].2, b"ab");
    }

    #[test]
    fn test_read_cpp_wal_truncated() {
        let mut data = vec![b'M', b'G', b'w', b'l'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.extend_from_slice(&100u64.to_le_bytes());
        data.push(0x01);
        data.extend_from_slice(&100u64.to_le_bytes()); // payload_len larger than remaining data

        assert!(matches!(read_cpp_wal(&data, 20), Err(CppFormatError::Corrupt(_))));
    }

    #[test]
    fn test_read_cpp_snapshot_corrupt_too_short() {
        let data = vec![0u8; 5];
        assert!(matches!(read_cpp_snapshot(&data, 20), Err(CppFormatError::Corrupt(_))));
    }

    #[test]
    fn test_read_cpp_snapshot_property_value_bool_true() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_VERTEX);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes()); // gid
        data.extend_from_slice(&0u64.to_le_bytes()); // no labels
        data.extend_from_slice(&1u64.to_le_bytes()); // 1 property
        data.extend_from_slice(&0u64.to_le_bytes()); // prop_id
        data.push(marker::VALUE_TRUE);

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert_eq!(result.vertices[0].properties[0].1, PropertyValue::Bool(true));
    }

    #[test]
    fn test_read_cpp_snapshot_property_value_bool_false() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_VERTEX);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes()); // gid
        data.extend_from_slice(&0u64.to_le_bytes()); // no labels
        data.extend_from_slice(&1u64.to_le_bytes()); // 1 property
        data.extend_from_slice(&0u64.to_le_bytes()); // prop_id
        data.push(marker::VALUE_FALSE);

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert_eq!(result.vertices[0].properties[0].1, PropertyValue::Bool(false));
    }

    #[test]
    fn test_read_cpp_snapshot_property_value_list() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_VERTEX);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes()); // gid
        data.extend_from_slice(&0u64.to_le_bytes()); // no labels
        data.extend_from_slice(&1u64.to_le_bytes()); // 1 property
        data.extend_from_slice(&0u64.to_le_bytes()); // prop_id
        data.push(marker::TYPE_LIST);
        data.extend_from_slice(&2u64.to_le_bytes()); // len
        data.push(marker::TYPE_INT);
        data.extend_from_slice(&1i64.to_le_bytes());
        data.push(marker::TYPE_INT);
        data.extend_from_slice(&2i64.to_le_bytes());

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert_eq!(result.vertices[0].properties[0].1, PropertyValue::List(vec![
            PropertyValue::Int(1),
            PropertyValue::Int(2),
        ]));
    }

    #[test]
    fn test_read_cpp_snapshot_property_value_map() {
        let mut data = vec![b'M', b'G', b's', b'n'];
        data.extend_from_slice(&20u64.to_le_bytes());

        data.push(marker::SECTION_VERTEX);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes()); // gid
        data.extend_from_slice(&0u64.to_le_bytes()); // no labels
        data.extend_from_slice(&1u64.to_le_bytes()); // 1 property
        data.extend_from_slice(&0u64.to_le_bytes()); // prop_id
        data.push(marker::TYPE_MAP);
        data.extend_from_slice(&1u64.to_le_bytes()); // len
        data.extend_from_slice(&3u64.to_le_bytes()); // "key" len
        data.extend_from_slice(b"key");
        data.push(marker::TYPE_STRING);
        data.extend_from_slice(&5u64.to_le_bytes()); // "value" len
        data.extend_from_slice(b"value");

        let result = read_cpp_snapshot(&data, 20).unwrap();
        assert_eq!(result.vertices[0].properties[0].1, PropertyValue::Map(vec![
            ("key".into(), PropertyValue::String("value".into())),
        ]));
    }
}
