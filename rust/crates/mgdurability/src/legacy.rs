//! Legacy readers for C++ Memgraph snapshot/WAL format (versions 14-34).
//!
//! C++ format uses marker-based encoding where each value is prefixed
//! by a single-byte `Marker` identifying the type, followed by the data.
//! This module maps C++ markers to Rust SLK-equivalent types.

use std::path::Path;

use std::collections::HashMap;

use crate::delta_record::DeltaRecord;
use crate::snapshot::{EdgeSnapshotEntry, NameMapperSnapshot, SnapshotData, VertexSnapshotEntry};
use crate::wal::WalError;
use mgcore::point::{Crs, Point2D, Point3D};
use mgcore::property_value::PropertyValue;
use mgcore::temporal::{Date, Duration, LocalDateTime, LocalTime, ZonedDateTime};
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};

// ─── Public convenience API ────────────────────────────────────────────────

/// Detect the C++ format version from a file path.
///
/// Returns `Some(version)` for legacy C++ files (versions 14-34),
/// or `None` if the file is not a recognized legacy format
/// (including current Rust format, unknown versions, or I/O errors).
pub fn detect_format_version(path: impl AsRef<Path>) -> Option<u16> {
    let data = std::fs::read(path.as_ref()).ok()?;
    let format = crate::version::detect_format(&data).ok()?;
    match format {
        crate::version::FormatKind::LegacyCpp(v) if (14..=34).contains(&v) => Some(v as u16),
        _ => None,
    }
}

/// Classification of legacy delta types found in C++ WAL files.
///
/// Maps the raw marker byte to a typed enum for easier inspection
/// before converting to [`DeltaRecord`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyDeltaType {
    VertexCreate,
    VertexDelete,
    VertexAddLabel,
    VertexRemoveLabel,
    VertexSetProperty,
    EdgeCreate,
    EdgeDelete,
    EdgeSetProperty,
    TransactionEnd,
    LabelIndexCreate,
    LabelIndexDrop,
    LabelPropertyIndexCreate,
    LabelPropertyIndexDrop,
    ExistenceConstraintCreate,
    ExistenceConstraintDrop,
    UniqueConstraintCreate,
    UniqueConstraintDrop,
    Unknown(u8),
}

impl LegacyDeltaType {
    /// Parse a legacy delta tag into its typed classification.
    pub fn from_tag(tag: u8, version: u64) -> Self {
        let mapped = if version >= 20 {
            tag
        } else {
            match tag {
                legacy_delta::V14_VERTEX_CREATE => legacy_delta::V20_VERTEX_CREATE,
                legacy_delta::V14_VERTEX_DELETE => legacy_delta::V20_VERTEX_DELETE,
                legacy_delta::V14_VERTEX_ADD_LABEL => legacy_delta::V20_VERTEX_ADD_LABEL,
                legacy_delta::V14_VERTEX_REMOVE_LABEL => legacy_delta::V20_VERTEX_REMOVE_LABEL,
                legacy_delta::V14_VERTEX_SET_PROPERTY => legacy_delta::V20_VERTEX_SET_PROPERTY,
                legacy_delta::V14_EDGE_CREATE => legacy_delta::V20_EDGE_CREATE,
                legacy_delta::V14_EDGE_DELETE => legacy_delta::V20_EDGE_DELETE,
                legacy_delta::V14_EDGE_SET_PROPERTY => legacy_delta::V20_EDGE_SET_PROPERTY,
                legacy_delta::V14_TRANSACTION_END => legacy_delta::V20_TRANSACTION_END,
                other => other,
            }
        };
        match mapped {
            legacy_delta::V20_VERTEX_CREATE => LegacyDeltaType::VertexCreate,
            legacy_delta::V20_VERTEX_DELETE => LegacyDeltaType::VertexDelete,
            legacy_delta::V20_VERTEX_ADD_LABEL => LegacyDeltaType::VertexAddLabel,
            legacy_delta::V20_VERTEX_REMOVE_LABEL => LegacyDeltaType::VertexRemoveLabel,
            legacy_delta::V20_VERTEX_SET_PROPERTY => LegacyDeltaType::VertexSetProperty,
            legacy_delta::V20_EDGE_CREATE => LegacyDeltaType::EdgeCreate,
            legacy_delta::V20_EDGE_DELETE => LegacyDeltaType::EdgeDelete,
            legacy_delta::V20_EDGE_SET_PROPERTY => LegacyDeltaType::EdgeSetProperty,
            legacy_delta::V20_TRANSACTION_END => LegacyDeltaType::TransactionEnd,
            legacy_delta::V20_LABEL_INDEX_CREATE => LegacyDeltaType::LabelIndexCreate,
            legacy_delta::V20_LABEL_INDEX_DROP => LegacyDeltaType::LabelIndexDrop,
            legacy_delta::V20_LABEL_PROPERTIES_INDEX_CREATE => {
                LegacyDeltaType::LabelPropertyIndexCreate
            }
            legacy_delta::V20_LABEL_PROPERTIES_INDEX_DROP => {
                LegacyDeltaType::LabelPropertyIndexDrop
            }
            legacy_delta::V20_EXISTENCE_CONSTRAINT_CREATE => {
                LegacyDeltaType::ExistenceConstraintCreate
            }
            legacy_delta::V20_EXISTENCE_CONSTRAINT_DROP => LegacyDeltaType::ExistenceConstraintDrop,
            legacy_delta::V20_UNIQUE_CONSTRAINT_CREATE => LegacyDeltaType::UniqueConstraintCreate,
            legacy_delta::V20_UNIQUE_CONSTRAINT_DROP => LegacyDeltaType::UniqueConstraintDrop,
            other => LegacyDeltaType::Unknown(other),
        }
    }
}

/// Read a single legacy delta record from a C++ WAL stream.
///
/// The `reader` must be positioned at a delta tag byte. The tag is read,
/// then the payload fields are decoded according to the version.
///
/// # Errors
/// Returns `CppFormatError::Corrupt` if the stream ends prematurely or
/// contains an unrecognized tag.
pub fn read_legacy_delta_record(
    reader: &mut CppReader<'_>,
    version: u64,
) -> Result<DeltaRecord, CppFormatError> {
    let tag = reader.read_byte()?;
    LegacyWalReader::decode_record(reader, tag, version)
}

/// Read a legacy snapshot file and return its [`SnapshotData`].
///
/// This is a convenience wrapper around [`LegacySnapshotReader::read`]
/// that accepts a file path instead of raw bytes.
///
/// # Errors
/// Returns `CppFormatError` on I/O failure, corrupt data, or unsupported version.
pub fn read_legacy_snapshot(
    path: impl AsRef<Path>,
    version: u64,
) -> Result<SnapshotData, CppFormatError> {
    let data = std::fs::read(path.as_ref())?;
    LegacySnapshotReader::read(&data, version)
}

/// Read a legacy WAL file and return all [`DeltaRecord`]s.
///
/// This is a convenience wrapper around [`LegacyWalReader::read`]
/// that accepts a file path instead of raw bytes.
///
/// # Errors
/// Returns `CppFormatError` on I/O failure, corrupt data, or unsupported version.
pub fn read_legacy_wal(
    path: impl AsRef<Path>,
    version: u64,
) -> Result<Vec<DeltaRecord>, CppFormatError> {
    let data = std::fs::read(path.as_ref())?;
    LegacyWalReader::read(&data, version)
}

// ─── C++ wire format markers (memgraph::wire_format::Marker) ───────────────

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

    pub const VALUE_FALSE: u8 = 0x00;
    pub const VALUE_TRUE: u8 = 0xff;
}

// ─── Older delta marker constants (v14-v20 differ from current) ────────────

mod legacy_delta {
    pub const V14_VERTEX_CREATE: u8 = 0x40;
    pub const V14_VERTEX_DELETE: u8 = 0x41;
    pub const V14_VERTEX_ADD_LABEL: u8 = 0x42;
    pub const V14_VERTEX_REMOVE_LABEL: u8 = 0x43;
    pub const V14_VERTEX_SET_PROPERTY: u8 = 0x44;
    pub const V14_EDGE_CREATE: u8 = 0x45;
    pub const V14_EDGE_DELETE: u8 = 0x46;
    pub const V14_EDGE_SET_PROPERTY: u8 = 0x47;
    pub const V14_TRANSACTION_END: u8 = 0x48;

    pub const V20_VERTEX_CREATE: u8 = 0x50;
    pub const V20_VERTEX_DELETE: u8 = 0x51;
    pub const V20_VERTEX_ADD_LABEL: u8 = 0x52;
    pub const V20_VERTEX_REMOVE_LABEL: u8 = 0x53;
    pub const V20_VERTEX_SET_PROPERTY: u8 = 0x54;
    pub const V20_EDGE_CREATE: u8 = 0x55;
    pub const V20_EDGE_DELETE: u8 = 0x56;
    pub const V20_EDGE_SET_PROPERTY: u8 = 0x57;
    pub const V20_TRANSACTION_END: u8 = 0x58;
    pub const V20_LABEL_INDEX_CREATE: u8 = 0x59;
    pub const V20_LABEL_INDEX_DROP: u8 = 0x5a;
    pub const V20_LABEL_PROPERTIES_INDEX_CREATE: u8 = 0x5b;
    pub const V20_LABEL_PROPERTIES_INDEX_DROP: u8 = 0x5c;
    pub const V20_EXISTENCE_CONSTRAINT_CREATE: u8 = 0x5d;
    pub const V20_EXISTENCE_CONSTRAINT_DROP: u8 = 0x5e;
    pub const V20_UNIQUE_CONSTRAINT_CREATE: u8 = 0x5f;
    pub const V20_UNIQUE_CONSTRAINT_DROP: u8 = 0x60;
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

impl From<std::io::Error> for CppFormatError {
    fn from(e: std::io::Error) -> Self {
        CppFormatError::Io(e)
    }
}

impl From<CppFormatError> for WalError {
    fn from(e: CppFormatError) -> Self {
        match e {
            CppFormatError::Io(io) => WalError::Io(io),
            CppFormatError::Corrupt(s) => WalError::Corrupt(s),
            CppFormatError::UnsupportedVersion(v) => WalError::UnsupportedVersion(v),
        }
    }
}

// ─── Low-level C++ marker reader ───────────────────────────────────────────

/// Low-level reader for C++ marker-based wire format.
pub struct CppReader<'a> {
    data: &'a [u8],
    pos: usize,
    /// Enum type_id → (type_name, value_id → value_name) mappings for decoding
    /// TYPE_ENUM property values.
    pub enum_mappings: HashMap<u64, (String, HashMap<u64, String>)>,
}

impl<'a> CppReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            enum_mappings: HashMap::new(),
        }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn peek_byte(&self) -> Result<u8, CppFormatError> {
        if self.pos >= self.data.len() {
            return Err(CppFormatError::Corrupt("unexpected end of data".into()));
        }
        Ok(self.data[self.pos])
    }

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
            marker::TYPE_BOOL => {
                let val = self.read_byte()?;
                Ok(PropertyValue::Bool(val == marker::VALUE_TRUE))
            }
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
            marker::TYPE_TEMPORAL_DATA => {
                let inner = self.read_byte()?;
                if inner != marker::TYPE_TEMPORAL_DATA {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_TEMPORAL_DATA inner marker, got 0x{:02x}",
                        inner
                    )));
                }
                let type_tag = self.read_tagged_u64()?;
                let microseconds = self.read_tagged_u64()? as i64;
                match type_tag {
                    0 => Ok(PropertyValue::Date(Date::from_days(microseconds))),
                    1 => Ok(PropertyValue::LocalTime(LocalTime::from_microseconds(
                        microseconds,
                    ))),
                    2 => Ok(PropertyValue::LocalDateTime(
                        LocalDateTime::from_microseconds(microseconds),
                    )),
                    3 => Ok(PropertyValue::Duration(Duration::new(0, 0, microseconds))),
                    other => Err(CppFormatError::Corrupt(format!(
                        "unknown temporal type: {}",
                        other
                    ))),
                }
            }
            marker::TYPE_ZONED_TEMPORAL_DATA => {
                let inner = self.read_byte()?;
                if inner != marker::TYPE_ZONED_TEMPORAL_DATA {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_ZONED_TEMPORAL_DATA inner marker, got 0x{:02x}",
                        inner
                    )));
                }
                let _type_tag = self.read_tagged_u64()?;
                let microseconds = self.read_tagged_u64()? as i64;
                let tz_marker = self.read_byte()?;
                match tz_marker {
                    marker::TYPE_STRING => {
                        let timezone = self.read_string()?;
                        Ok(PropertyValue::ZonedDateTime(ZonedDateTime::new(
                            microseconds,
                            0,
                            timezone,
                        )))
                    }
                    marker::TYPE_INT => {
                        let offset_minutes = self.read_u64_le()? as i16;
                        Ok(PropertyValue::ZonedDateTime(ZonedDateTime::new(
                            microseconds,
                            offset_minutes,
                            String::new(),
                        )))
                    }
                    other => Err(CppFormatError::Corrupt(format!(
                        "unknown zoned temporal timezone marker: 0x{:02x}",
                        other
                    ))),
                }
            }
            marker::TYPE_ENUM => {
                let type_tag = self.read_byte()?;
                if type_tag != marker::TYPE_INT {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_INT for enum type_id, got 0x{:02x}",
                        type_tag
                    )));
                }
                let type_id = self.read_u64_le()?;
                let value_tag = self.read_byte()?;
                if value_tag != marker::TYPE_INT {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_INT for enum value_id, got 0x{:02x}",
                        value_tag
                    )));
                }
                let value_id = self.read_u64_le()?;
                if let Some((type_name, value_map)) = self.enum_mappings.get(&type_id) {
                    if let Some(value_name) = value_map.get(&value_id) {
                        return Ok(PropertyValue::Enum {
                            enum_type: type_name.clone(),
                            value: value_name.clone(),
                        });
                    }
                }
                Err(CppFormatError::Corrupt(format!(
                    "cannot resolve enum type_id={} value_id={} (no catalog mapping)",
                    type_id, value_id
                )))
            }
            marker::TYPE_POINT_2D => {
                let srid_tag = self.read_byte()?;
                if srid_tag != marker::TYPE_INT {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_INT for point srid, got 0x{:02x}",
                        srid_tag
                    )));
                }
                let srid = self.read_u64_le()?;
                let x_tag = self.read_byte()?;
                if x_tag != marker::TYPE_DOUBLE {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_DOUBLE for point x, got 0x{:02x}",
                        x_tag
                    )));
                }
                let x = self.read_f64_le()?;
                let y_tag = self.read_byte()?;
                if y_tag != marker::TYPE_DOUBLE {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_DOUBLE for point y, got 0x{:02x}",
                        y_tag
                    )));
                }
                let y = self.read_f64_le()?;
                let crs = match srid {
                    4326 => Crs::WGS84,
                    7203 => Crs::Cartesian2D,
                    9157 => Crs::Cartesian3D,
                    4979 => Crs::WGS843D,
                    other => {
                        return Err(CppFormatError::Corrupt(format!(
                            "unknown CRS srid: {}",
                            other
                        )))
                    }
                };
                Ok(PropertyValue::Point2D(Point2D::new(crs, x, y)))
            }
            marker::TYPE_POINT_3D => {
                let srid_tag = self.read_byte()?;
                if srid_tag != marker::TYPE_INT {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_INT for point srid, got 0x{:02x}",
                        srid_tag
                    )));
                }
                let srid = self.read_u64_le()?;
                let x_tag = self.read_byte()?;
                if x_tag != marker::TYPE_DOUBLE {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_DOUBLE for point x, got 0x{:02x}",
                        x_tag
                    )));
                }
                let x = self.read_f64_le()?;
                let y_tag = self.read_byte()?;
                if y_tag != marker::TYPE_DOUBLE {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_DOUBLE for point y, got 0x{:02x}",
                        y_tag
                    )));
                }
                let y = self.read_f64_le()?;
                let z_tag = self.read_byte()?;
                if z_tag != marker::TYPE_DOUBLE {
                    return Err(CppFormatError::Corrupt(format!(
                        "expected TYPE_DOUBLE for point z, got 0x{:02x}",
                        z_tag
                    )));
                }
                let z = self.read_f64_le()?;
                let crs = match srid {
                    4326 => Crs::WGS84,
                    7203 => Crs::Cartesian2D,
                    9157 => Crs::Cartesian3D,
                    4979 => Crs::WGS843D,
                    other => {
                        return Err(CppFormatError::Corrupt(format!(
                            "unknown CRS srid: {}",
                            other
                        )))
                    }
                };
                Ok(PropertyValue::Point3D(Point3D::new(crs, x, y, z)))
            }
            other => Err(CppFormatError::Corrupt(format!(
                "unrecognized C++ property value marker: 0x{:02x} at offset {}",
                other,
                self.pos - 1
            ))),
        }
    }

    /// Read a u64 that is prefixed by a TYPE_INT marker (C++ WriteUint format).
    fn read_tagged_u64(&mut self) -> Result<u64, CppFormatError> {
        let tag = self.read_byte()?;
        if tag != marker::TYPE_INT {
            return Err(CppFormatError::Corrupt(format!(
                "expected TYPE_INT marker, got 0x{:02x}",
                tag
            )));
        }
        self.read_u64_le()
    }
}

// ─── Section offsets struct ────────────────────────────────────────────────

/// Offsets to each section in a C++ snapshot with SECTION_OFFSETS.
#[derive(Default, Clone, Copy)]
struct SectionOffsets {
    edges: u64,
    vertices: u64,
    #[allow(dead_code)]
    indices: u64,
    edge_indices: u64,
    constraints: u64,
    mapper: u64,
    enums: u64,
    epoch_history: u64,
    metadata: u64,
    edge_batches: u64,
    vertex_batches: u64,
    ttl: u64,
    descriptions: u64,
}

impl SectionOffsets {
    /// Read offsets from a C++ snapshot SECTION_OFFSETS.
    /// The `reader` must be positioned right after the SECTION_OFFSETS marker.
    fn read(reader: &mut CppReader<'_>, version: u64) -> Result<Self, CppFormatError> {
        let mut offsets = SectionOffsets {
            edges: reader.read_tagged_u64()?,
            vertices: reader.read_tagged_u64()?,
            indices: reader.read_tagged_u64()?,
            ..SectionOffsets::default()
        };
        if version >= 17 {
            offsets.edge_indices = reader.read_tagged_u64()?;
        }
        offsets.constraints = reader.read_tagged_u64()?;
        offsets.mapper = reader.read_tagged_u64()?;
        if version >= 18 {
            offsets.enums = reader.read_tagged_u64()?;
        }
        offsets.epoch_history = reader.read_tagged_u64()?;
        offsets.metadata = reader.read_tagged_u64()?;
        if version >= 15 {
            offsets.edge_batches = reader.read_tagged_u64()?;
            offsets.vertex_batches = reader.read_tagged_u64()?;
        }
        if version >= 30 {
            offsets.ttl = reader.read_tagged_u64()?;
        }
        if version >= 34 {
            offsets.descriptions = reader.read_tagged_u64()?;
        }
        Ok(offsets)
    }
}

fn read_enum_mappings(
    reader: &mut CppReader<'_>,
) -> Result<HashMap<u64, (String, HashMap<u64, String>)>, CppFormatError> {
    let count = reader.read_u64_le()? as usize;
    let mut mappings = HashMap::with_capacity(count);
    for type_id in 0..count as u64 {
        let type_name = reader.read_string()?;
        let value_count = reader.read_u64_le()? as usize;
        let mut value_map = HashMap::with_capacity(value_count);
        for value_id in 0..value_count as u64 {
            let value_name = reader.read_string()?;
            value_map.insert(value_id, value_name);
        }
        mappings.insert(type_id, (type_name, value_map));
    }
    Ok(mappings)
}

// ─── LegacySnapshotReader ──────────────────────────────────────────────────

/// Reads C++ v14-v34 snapshot files and converts them to [`SnapshotData`].
pub struct LegacySnapshotReader;

impl LegacySnapshotReader {
    /// Read a C++-format snapshot from raw bytes.
    pub fn read(data: &[u8], version: u64) -> Result<SnapshotData, CppFormatError> {
        if data.len() < 12 {
            return Err(CppFormatError::Corrupt("too short".into()));
        }
        if !(14..=34).contains(&version) {
            return Err(CppFormatError::UnsupportedVersion(version));
        }

        // C++ snapshots always start with SECTION_OFFSETS (v14+).
        // Read offsets so we can jump to sections in any order.
        let mut initial = CppReader::new(&data[12..]);
        let offsets = if initial.remaining() > 0 && initial.peek_byte()? == marker::SECTION_OFFSETS
        {
            let _ = initial.read_byte()?; // consume SECTION_OFFSETS marker
            Some(SectionOffsets::read(&mut initial, version)?)
        } else {
            None
        };

        // Try to read enum mappings first (needed for TYPE_ENUM property values).
        let mut enum_mappings: HashMap<u64, (String, HashMap<u64, String>)> = HashMap::new();
        if let Some(ref off) = offsets {
            if off.enums > 0 && (12 + off.enums as usize) < data.len() {
                let mut er = CppReader::new(&data[12 + off.enums as usize..]);
                let _ = er.read_byte()?; // SECTION_ENUMS marker
                enum_mappings = read_enum_mappings(&mut er)?;
            }
        }

        // Helper to read vertices at a given offset.
        let read_vertices = |offset: u64| -> Result<Vec<VertexSnapshotEntry>, CppFormatError> {
            if offset == 0 || (12 + offset as usize) >= data.len() {
                return Ok(Vec::new());
            }
            let mut r = CppReader::new(&data[12 + offset as usize..]);
            r.enum_mappings = enum_mappings.clone();
            let _ = r.read_byte()?; // SECTION_VERTEX marker
            let count = r.read_u64_le()? as usize;
            let mut verts = Vec::with_capacity(count);
            for _ in 0..count {
                let gid = Gid::from(r.read_u64_le()?);
                let label_count = r.read_u64_le()? as usize;
                let mut labels = Vec::with_capacity(label_count);
                for _ in 0..label_count {
                    labels.push(LabelId::from(r.read_u64_le()? as u32));
                }
                let prop_count = r.read_u64_le()? as usize;
                let mut properties = Vec::with_capacity(prop_count);
                for _ in 0..prop_count {
                    let key = PropertyId::from(r.read_u64_le()? as u32);
                    let value = r.read_property_value()?;
                    properties.push((key, value));
                }
                verts.push(VertexSnapshotEntry {
                    gid,
                    labels,
                    properties,
                });
            }
            Ok(verts)
        };

        // Helper to read edges at a given offset.
        let read_edges = |offset: u64| -> Result<Vec<EdgeSnapshotEntry>, CppFormatError> {
            if offset == 0 || (12 + offset as usize) >= data.len() {
                return Ok(Vec::new());
            }
            let mut r = CppReader::new(&data[12 + offset as usize..]);
            r.enum_mappings = enum_mappings.clone();
            let _ = r.read_byte()?; // SECTION_EDGE marker
            let count = r.read_u64_le()? as usize;
            let mut edgs = Vec::with_capacity(count);
            for _ in 0..count {
                let gid = Gid::from(r.read_u64_le()?);
                let from_vertex = Gid::from(r.read_u64_le()?);
                let to_vertex = Gid::from(r.read_u64_le()?);
                let edge_type = EdgeTypeId::from(r.read_u64_le()? as u32);
                let prop_count = r.read_u64_le()? as usize;
                let mut properties = Vec::with_capacity(prop_count);
                for _ in 0..prop_count {
                    let key = PropertyId::from(r.read_u64_le()? as u32);
                    let value = r.read_property_value()?;
                    properties.push((key, value));
                }
                edgs.push(EdgeSnapshotEntry {
                    gid,
                    from_vertex,
                    to_vertex,
                    edge_type,
                    properties,
                });
            }
            Ok(edgs)
        };

        // Helper to read mapper at a given offset.
        let read_mapper = |offset: u64| -> Result<NameMapperSnapshot, CppFormatError> {
            if offset == 0 || (12 + offset as usize) >= data.len() {
                return Ok(NameMapperSnapshot {
                    labels: vec![],
                    properties: vec![],
                    edge_types: vec![],
                });
            }
            let mut r = CppReader::new(&data[12 + offset as usize..]);
            let _ = r.read_byte()?; // SECTION_MAPPER marker
            let label_count = r.read_u64_le()? as usize;
            let mut labels = Vec::with_capacity(label_count);
            for _ in 0..label_count {
                let name = r.read_string()?;
                let id = LabelId::from(r.read_u64_le()? as u32);
                labels.push((name, id));
            }
            let prop_count = r.read_u64_le()? as usize;
            let mut properties = Vec::with_capacity(prop_count);
            for _ in 0..prop_count {
                let name = r.read_string()?;
                let id = PropertyId::from(r.read_u64_le()? as u32);
                properties.push((name, id));
            }
            let edge_type_count = r.read_u64_le()? as usize;
            let mut edge_types = Vec::with_capacity(edge_type_count);
            for _ in 0..edge_type_count {
                let name = r.read_string()?;
                let id = EdgeTypeId::from(r.read_u64_le()? as u32);
                edge_types.push((name, id));
            }
            Ok(NameMapperSnapshot {
                labels,
                properties,
                edge_types,
            })
        };

        let (vertices, edges, name_mapper) = if let Some(ref off) = offsets {
            (
                read_vertices(off.vertices)?,
                read_edges(off.edges)?,
                read_mapper(off.mapper)?,
            )
        } else {
            // Fallback sequential scan for malformed snapshots without offsets.
            let mut reader = CppReader::new(&data[12..]);
            reader.enum_mappings = enum_mappings.clone();
            let mut verts = Vec::new();
            let mut edgs = Vec::new();
            let mut mapper = NameMapperSnapshot {
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
                            verts.push(VertexSnapshotEntry {
                                gid,
                                labels,
                                properties,
                            });
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
                            edgs.push(EdgeSnapshotEntry {
                                gid,
                                from_vertex,
                                to_vertex,
                                edge_type,
                                properties,
                            });
                        }
                    }
                    marker::SECTION_MAPPER => {
                        let label_count = reader.read_u64_le()? as usize;
                        let mut labels = Vec::with_capacity(label_count);
                        for _ in 0..label_count {
                            let name = reader.read_string()?;
                            let id = LabelId::from(reader.read_u64_le()? as u32);
                            labels.push((name, id));
                        }
                        let prop_count = reader.read_u64_le()? as usize;
                        let mut properties = Vec::with_capacity(prop_count);
                        for _ in 0..prop_count {
                            let name = reader.read_string()?;
                            let id = PropertyId::from(reader.read_u64_le()? as u32);
                            properties.push((name, id));
                        }
                        let edge_type_count = reader.read_u64_le()? as usize;
                        let mut edge_types = Vec::with_capacity(edge_type_count);
                        for _ in 0..edge_type_count {
                            let name = reader.read_string()?;
                            let id = EdgeTypeId::from(reader.read_u64_le()? as u32);
                            edge_types.push((name, id));
                        }
                        mapper = NameMapperSnapshot {
                            labels,
                            properties,
                            edge_types,
                        };
                    }
                    marker::SECTION_ENUMS => {
                        // Read enum mappings if encountered before VERTEX/EDGE
                        enum_mappings = read_enum_mappings(&mut reader)?;
                        reader.enum_mappings = enum_mappings.clone();
                    }
                    _ => {
                        // Unknown section — can't safely skip without knowing length,
                        // so stop parsing. VERTEX/EDGE/MAPPER should already be read.
                        break;
                    }
                }
            }
            (verts, edgs, mapper)
        };

        Ok(SnapshotData {
            name_mapper,
            vertices,
            edges,
        })
    }
}

// ─── LegacyWalReader ───────────────────────────────────────────────────────

/// Reads C++ v14-v34 WAL files and converts records to [`DeltaRecord`].
pub struct LegacyWalReader;

impl LegacyWalReader {
    /// Read a C++-format WAL from raw bytes.
    pub fn read(data: &[u8], version: u64) -> Result<Vec<DeltaRecord>, CppFormatError> {
        if data.len() < 12 {
            return Err(CppFormatError::Corrupt("too short".into()));
        }
        if !(14..=34).contains(&version) {
            return Err(CppFormatError::UnsupportedVersion(version));
        }

        let mut reader = CppReader::new(&data[12..]);
        let mut records = Vec::new();

        while reader.remaining() > 0 {
            // C++ WAL format: each record starts with a delta marker byte
            let tag = match reader.read_byte() {
                Ok(t) => t,
                Err(_) => break,
            };
            let record = Self::decode_record(&mut reader, tag, version)?;
            records.push(record);
        }

        Ok(records)
    }

    fn decode_record(
        reader: &mut CppReader<'_>,
        tag: u8,
        version: u64,
    ) -> Result<DeltaRecord, CppFormatError> {
        // Map legacy tags to current DeltaRecord variants
        let mapped_tag = Self::map_tag(tag, version);
        match mapped_tag {
            legacy_delta::V20_VERTEX_CREATE | legacy_delta::V14_VERTEX_CREATE => {
                let gid = Gid::from(reader.read_u64_le()?);
                let timestamp = reader.read_u64_le()?;
                Ok(DeltaRecord::VertexCreate { gid, timestamp })
            }
            legacy_delta::V20_VERTEX_DELETE | legacy_delta::V14_VERTEX_DELETE => {
                let gid = Gid::from(reader.read_u64_le()?);
                Ok(DeltaRecord::VertexDelete { gid })
            }
            legacy_delta::V20_VERTEX_ADD_LABEL | legacy_delta::V14_VERTEX_ADD_LABEL => {
                let gid = Gid::from(reader.read_u64_le()?);
                let label = LabelId::from(reader.read_u64_le()? as u32);
                Ok(DeltaRecord::VertexAddLabel { gid, label })
            }
            legacy_delta::V20_VERTEX_REMOVE_LABEL | legacy_delta::V14_VERTEX_REMOVE_LABEL => {
                let gid = Gid::from(reader.read_u64_le()?);
                let label = LabelId::from(reader.read_u64_le()? as u32);
                Ok(DeltaRecord::VertexRemoveLabel { gid, label })
            }
            legacy_delta::V20_VERTEX_SET_PROPERTY | legacy_delta::V14_VERTEX_SET_PROPERTY => {
                let gid = Gid::from(reader.read_u64_le()?);
                let key = PropertyId::from(reader.read_u64_le()? as u32);
                let value = reader.read_property_value()?;
                Ok(DeltaRecord::VertexSetProperty { gid, key, value })
            }
            legacy_delta::V20_EDGE_CREATE | legacy_delta::V14_EDGE_CREATE => {
                let gid = Gid::from(reader.read_u64_le()?);
                let from_vertex = Gid::from(reader.read_u64_le()?);
                let to_vertex = Gid::from(reader.read_u64_le()?);
                let edge_type = EdgeTypeId::from(reader.read_u64_le()? as u32);
                let timestamp = reader.read_u64_le()?;
                Ok(DeltaRecord::EdgeCreate {
                    gid,
                    from_vertex,
                    to_vertex,
                    edge_type,
                    timestamp,
                })
            }
            legacy_delta::V20_EDGE_DELETE | legacy_delta::V14_EDGE_DELETE => {
                let gid = Gid::from(reader.read_u64_le()?);
                Ok(DeltaRecord::EdgeDelete { gid })
            }
            legacy_delta::V20_EDGE_SET_PROPERTY | legacy_delta::V14_EDGE_SET_PROPERTY => {
                let gid = Gid::from(reader.read_u64_le()?);
                let key = PropertyId::from(reader.read_u64_le()? as u32);
                let value = reader.read_property_value()?;
                Ok(DeltaRecord::EdgeSetProperty { gid, key, value })
            }
            legacy_delta::V20_TRANSACTION_END | legacy_delta::V14_TRANSACTION_END => {
                let timestamp = reader.read_u64_le()?;
                let commit_timestamp = reader.read_u64_le()?;
                Ok(DeltaRecord::TransactionEnd {
                    timestamp,
                    commit_timestamp,
                })
            }
            legacy_delta::V20_LABEL_INDEX_CREATE => {
                let label = LabelId::from(reader.read_u64_le()? as u32);
                Ok(DeltaRecord::LabelIndexCreate { label })
            }
            legacy_delta::V20_LABEL_INDEX_DROP => {
                let label = LabelId::from(reader.read_u64_le()? as u32);
                Ok(DeltaRecord::LabelIndexDrop { label })
            }
            legacy_delta::V20_LABEL_PROPERTIES_INDEX_CREATE => {
                let label = LabelId::from(reader.read_u64_le()? as u32);
                let property = PropertyId::from(reader.read_u64_le()? as u32);
                Ok(DeltaRecord::LabelPropertyIndexCreate { label, property })
            }
            legacy_delta::V20_LABEL_PROPERTIES_INDEX_DROP => {
                let label = LabelId::from(reader.read_u64_le()? as u32);
                let property = PropertyId::from(reader.read_u64_le()? as u32);
                Ok(DeltaRecord::LabelPropertyIndexDrop { label, property })
            }
            legacy_delta::V20_EXISTENCE_CONSTRAINT_CREATE => {
                let label = LabelId::from(reader.read_u64_le()? as u32);
                let property = PropertyId::from(reader.read_u64_le()? as u32);
                Ok(DeltaRecord::ExistenceConstraintCreate { label, property })
            }
            legacy_delta::V20_EXISTENCE_CONSTRAINT_DROP => {
                let label = LabelId::from(reader.read_u64_le()? as u32);
                let property = PropertyId::from(reader.read_u64_le()? as u32);
                Ok(DeltaRecord::ExistenceConstraintDrop { label, property })
            }
            legacy_delta::V20_UNIQUE_CONSTRAINT_CREATE => {
                let label = LabelId::from(reader.read_u64_le()? as u32);
                let prop_count = reader.read_u64_le()? as usize;
                let mut properties = Vec::with_capacity(prop_count);
                for _ in 0..prop_count {
                    properties.push(PropertyId::from(reader.read_u64_le()? as u32));
                }
                Ok(DeltaRecord::UniqueConstraintCreate { label, properties })
            }
            legacy_delta::V20_UNIQUE_CONSTRAINT_DROP => {
                let label = LabelId::from(reader.read_u64_le()? as u32);
                let prop_count = reader.read_u64_le()? as usize;
                let mut properties = Vec::with_capacity(prop_count);
                for _ in 0..prop_count {
                    properties.push(PropertyId::from(reader.read_u64_le()? as u32));
                }
                Ok(DeltaRecord::UniqueConstraintDrop { label, properties })
            }
            other => Err(CppFormatError::Corrupt(format!(
                "unknown legacy WAL delta tag: 0x{:02x} (version {})",
                other, version
            ))),
        }
    }

    fn map_tag(tag: u8, version: u64) -> u8 {
        if version >= 20 {
            // v20+ tags already match the V20 constants (same as current)
            tag
        } else {
            // v14 tags: map to v20 equivalents for unified decoding
            match tag {
                legacy_delta::V14_VERTEX_CREATE => legacy_delta::V20_VERTEX_CREATE,
                legacy_delta::V14_VERTEX_DELETE => legacy_delta::V20_VERTEX_DELETE,
                legacy_delta::V14_VERTEX_ADD_LABEL => legacy_delta::V20_VERTEX_ADD_LABEL,
                legacy_delta::V14_VERTEX_REMOVE_LABEL => legacy_delta::V20_VERTEX_REMOVE_LABEL,
                legacy_delta::V14_VERTEX_SET_PROPERTY => legacy_delta::V20_VERTEX_SET_PROPERTY,
                legacy_delta::V14_EDGE_CREATE => legacy_delta::V20_EDGE_CREATE,
                legacy_delta::V14_EDGE_DELETE => legacy_delta::V20_EDGE_DELETE,
                legacy_delta::V14_EDGE_SET_PROPERTY => legacy_delta::V20_EDGE_SET_PROPERTY,
                legacy_delta::V14_TRANSACTION_END => legacy_delta::V20_TRANSACTION_END,
                other => other,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::property_value::PropertyValue;
    use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};

    fn build_cpp_snapshot_v20() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&20u64.to_le_bytes());

        // SECTION_VERTEX
        buf.push(marker::SECTION_VERTEX);
        buf.extend_from_slice(&2u64.to_le_bytes()); // count
                                                    // vertex 1
        buf.extend_from_slice(&1u64.to_le_bytes()); // gid
        buf.extend_from_slice(&1u64.to_le_bytes()); // label_count
        buf.extend_from_slice(&10u64.to_le_bytes()); // label id
        buf.extend_from_slice(&1u64.to_le_bytes()); // prop_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // property id
        buf.push(marker::TYPE_INT);
        buf.extend_from_slice(&42i64.to_le_bytes());
        // vertex 2
        buf.extend_from_slice(&2u64.to_le_bytes()); // gid
        buf.extend_from_slice(&0u64.to_le_bytes()); // label_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // prop_count

        // SECTION_EDGE
        buf.push(marker::SECTION_EDGE);
        buf.extend_from_slice(&1u64.to_le_bytes()); // count
        buf.extend_from_slice(&100u64.to_le_bytes()); // gid
        buf.extend_from_slice(&1u64.to_le_bytes()); // from
        buf.extend_from_slice(&2u64.to_le_bytes()); // to
        buf.extend_from_slice(&5u64.to_le_bytes()); // edge_type
        buf.extend_from_slice(&1u64.to_le_bytes()); // prop_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // property id
        buf.push(marker::TYPE_STRING);
        buf.extend_from_slice(&1u64.to_le_bytes()); // string len
        buf.push(b'e');

        buf
    }

    #[test]
    fn test_legacy_snapshot_reader_v20() {
        let data = build_cpp_snapshot_v20();
        let snap = LegacySnapshotReader::read(&data, 20).unwrap();
        assert_eq!(snap.vertices.len(), 2);
        assert_eq!(snap.vertices[0].gid, Gid::from(1u64));
        assert_eq!(snap.vertices[0].labels, vec![LabelId::from(10u32)]);
        assert_eq!(
            snap.vertices[0].properties,
            vec![(PropertyId::from(0u32), PropertyValue::Int(42))]
        );
        assert_eq!(snap.edges.len(), 1);
        assert_eq!(snap.edges[0].gid, Gid::from(100u64));
        assert_eq!(snap.edges[0].from_vertex, Gid::from(1u64));
        assert_eq!(snap.edges[0].to_vertex, Gid::from(2u64));
    }

    #[test]
    fn test_legacy_snapshot_reader_v14() {
        let mut data = build_cpp_snapshot_v20();
        // Patch version to 14
        data[4..12].copy_from_slice(&14u64.to_le_bytes());
        let snap = LegacySnapshotReader::read(&data, 14).unwrap();
        assert_eq!(snap.vertices.len(), 2);
        assert_eq!(snap.edges.len(), 1);
    }

    #[test]
    fn test_legacy_snapshot_with_mapper() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&20u64.to_le_bytes());

        // SECTION_MAPPER
        buf.push(marker::SECTION_MAPPER);
        buf.extend_from_slice(&1u64.to_le_bytes()); // label count
        buf.extend_from_slice(&3u64.to_le_bytes()); // "foo" len
        buf.extend_from_slice(b"foo");
        buf.extend_from_slice(&7u32.to_le_bytes()); // label id (as u64 in C++ format, but we write u64)
                                                    // Actually C++ writes u64 for IDs in mapper section
                                                    // Fix: write u64
                                                    // Let me rebuild carefully
        let mut buf2 = Vec::new();
        buf2.extend_from_slice(b"MGsn");
        buf2.extend_from_slice(&20u64.to_le_bytes());
        buf2.push(marker::SECTION_MAPPER);
        buf2.extend_from_slice(&1u64.to_le_bytes()); // label count
        buf2.extend_from_slice(&3u64.to_le_bytes()); // name len
        buf2.extend_from_slice(b"foo");
        buf2.extend_from_slice(&7u64.to_le_bytes()); // label id
        buf2.extend_from_slice(&0u64.to_le_bytes()); // prop count
        buf2.extend_from_slice(&0u64.to_le_bytes()); // edge type count

        // SECTION_VERTEX
        buf2.push(marker::SECTION_VERTEX);
        buf2.extend_from_slice(&0u64.to_le_bytes());

        // SECTION_EDGE
        buf2.push(marker::SECTION_EDGE);
        buf2.extend_from_slice(&0u64.to_le_bytes());

        let snap = LegacySnapshotReader::read(&buf2, 20).unwrap();
        assert_eq!(snap.name_mapper.labels.len(), 1);
        assert_eq!(snap.name_mapper.labels[0].0, "foo");
        assert_eq!(snap.name_mapper.labels[0].1, LabelId::from(7u32));
    }

    #[test]
    fn test_legacy_snapshot_unsupported_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&99u64.to_le_bytes());
        let err = LegacySnapshotReader::read(&buf, 99).unwrap_err();
        assert!(matches!(err, CppFormatError::UnsupportedVersion(99)));
    }

    fn build_cpp_wal_v20() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGwl");
        buf.extend_from_slice(&20u64.to_le_bytes());

        // VertexCreate
        buf.push(legacy_delta::V20_VERTEX_CREATE);
        buf.extend_from_slice(&10u64.to_le_bytes()); // gid
        buf.extend_from_slice(&100u64.to_le_bytes()); // timestamp

        // VertexSetProperty
        buf.push(legacy_delta::V20_VERTEX_SET_PROPERTY);
        buf.extend_from_slice(&10u64.to_le_bytes()); // gid
        buf.extend_from_slice(&0u64.to_le_bytes()); // key
        buf.push(marker::TYPE_STRING);
        buf.extend_from_slice(&5u64.to_le_bytes()); // len
        buf.extend_from_slice(b"hello");

        // TransactionEnd
        buf.push(legacy_delta::V20_TRANSACTION_END);
        buf.extend_from_slice(&1u64.to_le_bytes()); // timestamp
        buf.extend_from_slice(&101u64.to_le_bytes()); // commit_timestamp

        buf
    }

    #[test]
    fn test_legacy_wal_reader_v20() {
        let data = build_cpp_wal_v20();
        let records = LegacyWalReader::read(&data, 20).unwrap();
        assert_eq!(records.len(), 3);
        assert!(
            matches!(records[0], DeltaRecord::VertexCreate { gid, .. } if gid == Gid::from(10u64))
        );
        assert!(matches!(records[1], DeltaRecord::VertexSetProperty { .. }));
        assert!(matches!(
            records[2],
            DeltaRecord::TransactionEnd {
                timestamp: 1,
                commit_timestamp: 101
            }
        ));
    }

    #[test]
    fn test_legacy_wal_reader_v14_tag_mapping() {
        let mut data = build_cpp_wal_v20();
        data[4..12].copy_from_slice(&14u64.to_le_bytes());
        // Remap tags from V20 to V14
        // V14_VERTEX_CREATE = 0x40, V20 = 0x50
        // We need to patch the tag bytes in the payload
        let payload_start = 12;
        data[payload_start] = legacy_delta::V14_VERTEX_CREATE;
        data[payload_start + 1 + 8 + 8] = legacy_delta::V14_VERTEX_SET_PROPERTY;
        data[payload_start + 1 + 8 + 8 + 1 + 8 + 8 + 1 + 1 + 8 + 5] =
            legacy_delta::V14_TRANSACTION_END;

        let records = LegacyWalReader::read(&data, 14).unwrap();
        assert_eq!(records.len(), 3);
        assert!(matches!(records[0], DeltaRecord::VertexCreate { .. }));
        assert!(matches!(records[2], DeltaRecord::TransactionEnd { .. }));
    }

    #[test]
    fn test_legacy_wal_unsupported_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGwl");
        buf.extend_from_slice(&99u64.to_le_bytes());
        let err = LegacyWalReader::read(&buf, 99).unwrap_err();
        assert!(matches!(err, CppFormatError::UnsupportedVersion(99)));
    }

    #[test]
    fn test_legacy_wal_edge_create() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGwl");
        buf.extend_from_slice(&20u64.to_le_bytes());
        buf.push(legacy_delta::V20_EDGE_CREATE);
        buf.extend_from_slice(&100u64.to_le_bytes()); // gid
        buf.extend_from_slice(&1u64.to_le_bytes()); // from
        buf.extend_from_slice(&2u64.to_le_bytes()); // to
        buf.extend_from_slice(&5u64.to_le_bytes()); // edge_type
        buf.extend_from_slice(&200u64.to_le_bytes()); // timestamp

        let records = LegacyWalReader::read(&buf, 20).unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(
            records[0],
            DeltaRecord::EdgeCreate { gid, from_vertex, to_vertex, edge_type, timestamp }
            if gid == Gid::from(100u64)
                && from_vertex == Gid::from(1u64)
                && to_vertex == Gid::from(2u64)
                && edge_type == EdgeTypeId::from(5u32)
                && timestamp == 200
        ));
    }

    #[test]
    fn test_legacy_wal_constraint_deltas() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGwl");
        buf.extend_from_slice(&20u64.to_le_bytes());

        buf.push(legacy_delta::V20_EXISTENCE_CONSTRAINT_CREATE);
        buf.extend_from_slice(&1u64.to_le_bytes()); // label
        buf.extend_from_slice(&2u64.to_le_bytes()); // property

        buf.push(legacy_delta::V20_UNIQUE_CONSTRAINT_CREATE);
        buf.extend_from_slice(&3u64.to_le_bytes()); // label
        buf.extend_from_slice(&2u64.to_le_bytes()); // prop_count
        buf.extend_from_slice(&4u64.to_le_bytes()); // prop0
        buf.extend_from_slice(&5u64.to_le_bytes()); // prop1

        let records = LegacyWalReader::read(&buf, 20).unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(
            records[0],
            DeltaRecord::ExistenceConstraintCreate { label, property }
            if label == LabelId::from(1u32) && property == PropertyId::from(2u32)
        ));
        assert!(matches!(
            records[1],
            DeltaRecord::UniqueConstraintCreate { label, ref properties }
            if label == LabelId::from(3u32) && properties == &[PropertyId::from(4u32), PropertyId::from(5u32)]
        ));
    }

    // ─── New tests for public convenience API ──────────────────────────────

    #[test]
    fn test_detect_format_version_legacy() {
        let path = "/tmp/mg_detect_legacy_v20.snap";
        let _ = std::fs::remove_file(path);
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&20u64.to_le_bytes());
        std::fs::write(path, &buf).unwrap();

        assert_eq!(detect_format_version(path), Some(20));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_detect_format_version_current_is_none() {
        let path = "/tmp/mg_detect_current.snap";
        let _ = std::fs::remove_file(path);
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&mgslk::DURABILITY_VERSION.to_le_bytes());
        std::fs::write(path, &buf).unwrap();

        assert_eq!(detect_format_version(path), None);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_legacy_delta_type_enum() {
        assert_eq!(
            LegacyDeltaType::from_tag(legacy_delta::V20_VERTEX_CREATE, 20),
            LegacyDeltaType::VertexCreate
        );
        assert_eq!(
            LegacyDeltaType::from_tag(legacy_delta::V20_EDGE_DELETE, 20),
            LegacyDeltaType::EdgeDelete
        );
        assert_eq!(
            LegacyDeltaType::from_tag(legacy_delta::V20_TRANSACTION_END, 20),
            LegacyDeltaType::TransactionEnd
        );
        assert_eq!(
            LegacyDeltaType::from_tag(legacy_delta::V20_UNIQUE_CONSTRAINT_CREATE, 20),
            LegacyDeltaType::UniqueConstraintCreate
        );
        assert_eq!(
            LegacyDeltaType::from_tag(0xff, 20),
            LegacyDeltaType::Unknown(0xff)
        );

        // v14 tag mapping
        assert_eq!(
            LegacyDeltaType::from_tag(legacy_delta::V14_VERTEX_CREATE, 14),
            LegacyDeltaType::VertexCreate
        );
        assert_eq!(
            LegacyDeltaType::from_tag(legacy_delta::V14_EDGE_SET_PROPERTY, 14),
            LegacyDeltaType::EdgeSetProperty
        );
    }

    #[test]
    fn test_read_legacy_delta_record_roundtrip() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGwl");
        buf.extend_from_slice(&20u64.to_le_bytes());
        buf.push(legacy_delta::V20_VERTEX_CREATE);
        buf.extend_from_slice(&42u64.to_le_bytes()); // gid
        buf.extend_from_slice(&100u64.to_le_bytes()); // timestamp

        let mut reader = CppReader::new(&buf[12..]);
        let record = read_legacy_delta_record(&mut reader, 20).unwrap();
        assert!(
            matches!(record, DeltaRecord::VertexCreate { gid, timestamp } if gid == Gid::from(42u64) && timestamp == 100)
        );
    }

    #[test]
    fn test_read_legacy_snapshot_path_api() {
        let path = "/tmp/mg_legacy_snap_path_api.snap";
        let _ = std::fs::remove_file(path);
        let data = build_cpp_snapshot_v20();
        std::fs::write(path, &data).unwrap();

        let snap = read_legacy_snapshot(path, 20).unwrap();
        assert_eq!(snap.vertices.len(), 2);
        assert_eq!(snap.edges.len(), 1);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_read_legacy_wal_path_api() {
        let path = "/tmp/mg_legacy_wal_path_api.wal";
        let _ = std::fs::remove_file(path);
        let data = build_cpp_wal_v20();
        std::fs::write(path, &data).unwrap();

        let records = read_legacy_wal(path, 20).unwrap();
        assert_eq!(records.len(), 3);
        assert!(matches!(records[0], DeltaRecord::VertexCreate { .. }));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_read_legacy_snapshot_invalid_version() {
        let path = "/tmp/mg_legacy_snap_bad_ver.snap";
        let _ = std::fs::remove_file(path);
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&999u64.to_le_bytes());
        std::fs::write(path, &buf).unwrap();

        let err = read_legacy_snapshot(path, 999).unwrap_err();
        assert!(matches!(err, CppFormatError::UnsupportedVersion(999)));
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn test_read_legacy_wal_invalid_version() {
        let path = "/tmp/mg_legacy_wal_bad_ver.wal";
        let _ = std::fs::remove_file(path);
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGwl");
        buf.extend_from_slice(&13u64.to_le_bytes());
        std::fs::write(path, &buf).unwrap();

        let err = read_legacy_wal(path, 13).unwrap_err();
        assert!(matches!(err, CppFormatError::UnsupportedVersion(13)));
        std::fs::remove_file(path).ok();
    }

    // ─── Tests for SECTION_OFFSETS and advanced types ────────────────────────

    fn write_tagged_u64(buf: &mut Vec<u8>, v: u64) {
        buf.push(marker::TYPE_INT);
        buf.extend_from_slice(&v.to_le_bytes());
    }

    #[test]
    fn test_legacy_snapshot_temporal_property_values() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&20u64.to_le_bytes());

        // SECTION_VERTEX
        buf.push(marker::SECTION_VERTEX);
        buf.extend_from_slice(&1u64.to_le_bytes()); // count
        buf.extend_from_slice(&1u64.to_le_bytes()); // gid
        buf.extend_from_slice(&0u64.to_le_bytes()); // label_count
        buf.extend_from_slice(&4u64.to_le_bytes()); // prop_count

        // Property 1: Date (days since epoch = 1970-01-01 = 0 days)
        buf.extend_from_slice(&0u64.to_le_bytes()); // prop_id
        buf.push(marker::TYPE_TEMPORAL_DATA);
        buf.push(marker::TYPE_TEMPORAL_DATA); // inner marker
        write_tagged_u64(&mut buf, 0); // type = Date
        write_tagged_u64(&mut buf, 0); // microseconds = days = 0

        // Property 2: LocalTime (1 microsecond past midnight)
        buf.extend_from_slice(&1u64.to_le_bytes()); // prop_id
        buf.push(marker::TYPE_TEMPORAL_DATA);
        buf.push(marker::TYPE_TEMPORAL_DATA);
        write_tagged_u64(&mut buf, 1); // type = LocalTime
        write_tagged_u64(&mut buf, 1); // microseconds = 1

        // Property 3: LocalDateTime
        buf.extend_from_slice(&2u64.to_le_bytes()); // prop_id
        buf.push(marker::TYPE_TEMPORAL_DATA);
        buf.push(marker::TYPE_TEMPORAL_DATA);
        write_tagged_u64(&mut buf, 2); // type = LocalDateTime
        write_tagged_u64(&mut buf, 1_000_000); // microseconds

        // Property 4: Duration
        buf.extend_from_slice(&3u64.to_le_bytes()); // prop_id
        buf.push(marker::TYPE_TEMPORAL_DATA);
        buf.push(marker::TYPE_TEMPORAL_DATA);
        write_tagged_u64(&mut buf, 3); // type = Duration
        write_tagged_u64(&mut buf, 3_600_000_000); // microseconds = 1 hour

        let snap = LegacySnapshotReader::read(&buf, 20).unwrap();
        assert_eq!(snap.vertices.len(), 1);
        let props = &snap.vertices[0].properties;
        assert_eq!(props[0].1, PropertyValue::Date(Date::from_days(0)));
        assert_eq!(
            props[1].1,
            PropertyValue::LocalTime(LocalTime::from_microseconds(1))
        );
        assert_eq!(
            props[2].1,
            PropertyValue::LocalDateTime(LocalDateTime::from_microseconds(1_000_000))
        );
        assert_eq!(
            props[3].1,
            PropertyValue::Duration(Duration::new(0, 0, 3_600_000_000))
        );
    }

    #[test]
    fn test_legacy_snapshot_zoned_temporal_property_value() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&20u64.to_le_bytes());

        buf.push(marker::SECTION_VERTEX);
        buf.extend_from_slice(&1u64.to_le_bytes()); // count
        buf.extend_from_slice(&1u64.to_le_bytes()); // gid
        buf.extend_from_slice(&0u64.to_le_bytes()); // label_count
        buf.extend_from_slice(&1u64.to_le_bytes()); // prop_count

        buf.extend_from_slice(&0u64.to_le_bytes()); // prop_id
        buf.push(marker::TYPE_ZONED_TEMPORAL_DATA);
        buf.push(marker::TYPE_ZONED_TEMPORAL_DATA); // inner marker
        write_tagged_u64(&mut buf, 0); // type = ZonedDateTime
        write_tagged_u64(&mut buf, 1_000_000); // microseconds
        buf.push(marker::TYPE_STRING);
        buf.extend_from_slice(&12u64.to_le_bytes()); // len
        buf.extend_from_slice(b"Europe/Paris");

        let snap = LegacySnapshotReader::read(&buf, 20).unwrap();
        assert_eq!(snap.vertices.len(), 1);
        assert!(
            matches!(&snap.vertices[0].properties[0].1, PropertyValue::ZonedDateTime(zdt) if zdt.utc_microseconds == 1_000_000 && zdt.timezone == "Europe/Paris")
        );
    }

    #[test]
    fn test_legacy_snapshot_point2d_property_value() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&20u64.to_le_bytes());

        buf.push(marker::SECTION_VERTEX);
        buf.extend_from_slice(&1u64.to_le_bytes()); // count
        buf.extend_from_slice(&1u64.to_le_bytes()); // gid
        buf.extend_from_slice(&0u64.to_le_bytes()); // label_count
        buf.extend_from_slice(&1u64.to_le_bytes()); // prop_count

        buf.extend_from_slice(&0u64.to_le_bytes()); // prop_id
        buf.push(marker::TYPE_POINT_2D);
        write_tagged_u64(&mut buf, 4326); // srid = WGS84
        buf.push(marker::TYPE_DOUBLE);
        buf.extend_from_slice(&45.8150f64.to_le_bytes()); // x
        buf.push(marker::TYPE_DOUBLE);
        buf.extend_from_slice(&15.9819f64.to_le_bytes()); // y

        let snap = LegacySnapshotReader::read(&buf, 20).unwrap();
        assert_eq!(snap.vertices.len(), 1);
        assert!(
            matches!(&snap.vertices[0].properties[0].1, PropertyValue::Point2D(p) if p.crs == Crs::WGS84 && (p.x - 45.8150).abs() < 0.0001)
        );
    }

    #[test]
    fn test_legacy_snapshot_point3d_property_value() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&20u64.to_le_bytes());

        buf.push(marker::SECTION_VERTEX);
        buf.extend_from_slice(&1u64.to_le_bytes()); // count
        buf.extend_from_slice(&1u64.to_le_bytes()); // gid
        buf.extend_from_slice(&0u64.to_le_bytes()); // label_count
        buf.extend_from_slice(&1u64.to_le_bytes()); // prop_count

        buf.extend_from_slice(&0u64.to_le_bytes()); // prop_id
        buf.push(marker::TYPE_POINT_3D);
        write_tagged_u64(&mut buf, 9157); // srid = Cartesian3D
        buf.push(marker::TYPE_DOUBLE);
        buf.extend_from_slice(&1.0f64.to_le_bytes());
        buf.push(marker::TYPE_DOUBLE);
        buf.extend_from_slice(&2.0f64.to_le_bytes());
        buf.push(marker::TYPE_DOUBLE);
        buf.extend_from_slice(&3.0f64.to_le_bytes());

        let snap = LegacySnapshotReader::read(&buf, 20).unwrap();
        assert_eq!(snap.vertices.len(), 1);
        assert!(
            matches!(&snap.vertices[0].properties[0].1, PropertyValue::Point3D(p) if p.crs == Crs::Cartesian3D && p.z == 3.0)
        );
    }

    #[test]
    fn test_legacy_snapshot_enum_property_value() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&20u64.to_le_bytes());

        // SECTION_OFFSETS
        buf.push(marker::SECTION_OFFSETS);
        let offset_vertices = 200u64;
        let offset_enums = 100u64;
        write_tagged_u64(&mut buf, 0u64); // edges
        write_tagged_u64(&mut buf, offset_vertices); // vertices
        write_tagged_u64(&mut buf, 0u64); // indices
        write_tagged_u64(&mut buf, 0u64); // edge_indices
        write_tagged_u64(&mut buf, 0u64); // constraints
        write_tagged_u64(&mut buf, 0u64); // mapper
        write_tagged_u64(&mut buf, offset_enums); // enums
        write_tagged_u64(&mut buf, 0u64); // epoch_history
        write_tagged_u64(&mut buf, 0u64); // metadata
        write_tagged_u64(&mut buf, 0u64); // edge_batches
        write_tagged_u64(&mut buf, 0u64); // vertex_batches

        // Pad to offset_enums
        while buf.len() < 12 + offset_enums as usize {
            buf.push(0);
        }
        // SECTION_ENUMS
        buf.push(marker::SECTION_ENUMS);
        buf.extend_from_slice(&1u64.to_le_bytes()); // 1 enum type
        buf.extend_from_slice(&6u64.to_le_bytes()); // "Status" len
        buf.extend_from_slice(b"Status");
        buf.extend_from_slice(&2u64.to_le_bytes()); // 2 values
        buf.extend_from_slice(&4u64.to_le_bytes()); // "Open" len
        buf.extend_from_slice(b"Open");
        buf.extend_from_slice(&6u64.to_le_bytes()); // "Closed" len
        buf.extend_from_slice(b"Closed");

        // Pad to offset_vertices
        while buf.len() < 12 + offset_vertices as usize {
            buf.push(0);
        }
        // SECTION_VERTEX
        buf.push(marker::SECTION_VERTEX);
        buf.extend_from_slice(&1u64.to_le_bytes()); // count
        buf.extend_from_slice(&1u64.to_le_bytes()); // gid
        buf.extend_from_slice(&0u64.to_le_bytes()); // label_count
        buf.extend_from_slice(&1u64.to_le_bytes()); // prop_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // prop_id
        buf.push(marker::TYPE_ENUM);
        buf.push(marker::TYPE_INT);
        buf.extend_from_slice(&0u64.to_le_bytes()); // type_id = 0
        buf.push(marker::TYPE_INT);
        buf.extend_from_slice(&1u64.to_le_bytes()); // value_id = 1

        let snap = LegacySnapshotReader::read(&buf, 20).unwrap();
        assert_eq!(snap.vertices.len(), 1);
        assert_eq!(
            snap.vertices[0].properties[0].1,
            PropertyValue::Enum {
                enum_type: "Status".into(),
                value: "Closed".into(),
            }
        );
    }
}
