//! Bolt PackStream value types and encoding.
//!
//! Matches the Bolt protocol specification for value serialization.
//! Uses marker-byte-prefixed encoding.

use std::collections::HashMap;
use std::fmt;
use std::io::Write;

#[allow(unused_imports)]
use mgcore::point::{Crs, Point2D, Point3D};
use mgcore::property_value::PropertyValue;
use mgcore::temporal::{Date, Duration, LocalDateTime, LocalTime, ZonedDateTime};

// ─── Marker bytes ───────────────────────────────────────────────────────

pub const MARKER_NULL: u8 = 0xC0;
pub const MARKER_FALSE: u8 = 0xC2;
pub const MARKER_TRUE: u8 = 0xC3;
pub const MARKER_INT8: u8 = 0xC8;
pub const MARKER_INT16: u8 = 0xC9;
pub const MARKER_INT32: u8 = 0xCA;
pub const MARKER_INT64: u8 = 0xCB;
pub const MARKER_FLOAT64: u8 = 0xC1;
pub const MARKER_TINY_STRING: u8 = 0x80;
pub const MARKER_STRING8: u8 = 0xD0;
pub const MARKER_STRING16: u8 = 0xD1;
pub const MARKER_STRING32: u8 = 0xD2;
pub const MARKER_BYTES8: u8 = 0xCC;
pub const MARKER_BYTES16: u8 = 0xCD;
pub const MARKER_BYTES32: u8 = 0xCE;
pub const MARKER_TINY_LIST: u8 = 0x90;
pub const MARKER_LIST8: u8 = 0xD4;
pub const MARKER_LIST16: u8 = 0xD5;
pub const MARKER_LIST32: u8 = 0xD6;
pub const MARKER_TINY_MAP: u8 = 0xA0;
pub const MARKER_MAP8: u8 = 0xD8;
pub const MARKER_MAP16: u8 = 0xD9;
pub const MARKER_MAP32: u8 = 0xDA;
pub const MARKER_TINY_STRUCT: u8 = 0xB0;
pub const MARKER_STRUCT8: u8 = 0xDC;
pub const MARKER_STRUCT16: u8 = 0xDD;

// ─── Struct signatures ──────────────────────────────────────────────────
// Match `src/communication/bolt/v1/codes.hpp` exactly.

pub const SIG_NODE: u8 = 0x4E;
pub const SIG_RELATIONSHIP: u8 = 0x52;
pub const SIG_UNBOUND_RELATIONSHIP: u8 = 0x72;
pub const SIG_PATH: u8 = 0x50;

pub const SIG_DATE: u8 = 0x44;
pub const SIG_DURATION: u8 = 0x45;
pub const SIG_DATETIME_LEGACY: u8 = 0x46;
pub const SIG_DATETIME_ZONE_ID_LEGACY: u8 = 0x66;
pub const SIG_DATETIME: u8 = 0x49;
pub const SIG_DATETIME_ZONE_ID: u8 = 0x69;
pub const SIG_LOCAL_DATETIME: u8 = 0x64;
pub const SIG_LOCAL_TIME: u8 = 0x74;
pub const SIG_POINT_2D: u8 = 0x58;
pub const SIG_POINT_3D: u8 = 0x59;

pub const SIG_SUCCESS: u8 = 0x70;
pub const SIG_RECORD: u8 = 0x71;
pub const SIG_IGNORED: u8 = 0x7E;
pub const SIG_FAILURE: u8 = 0x7F;

// ─── Bolt Value ──────────────────────────────────────────────────────────

/// Bolt protocol value (PackStream).
#[derive(Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Map(HashMap<String, Value>),
    Struct(u8, Vec<Value>),
}

impl Value {
    /// Encode this value to bytes (PackStream format, no chunking).
    pub fn encode(&self, w: &mut impl Write) -> std::io::Result<()> {
        match self {
            Value::Null => w.write_all(&[MARKER_NULL]),
            Value::Bool(true) => w.write_all(&[MARKER_TRUE]),
            Value::Bool(false) => w.write_all(&[MARKER_FALSE]),
            Value::Int(n) => {
                if (-0x10..=0x7F).contains(n) {
                    w.write_all(&[*n as u8])
                } else if (-0x80..=0x7F).contains(n) {
                    w.write_all(&[MARKER_INT8, *n as u8])
                } else if (-0x8000..=0x7FFF).contains(n) {
                    w.write_all(&[MARKER_INT16])?;
                    w.write_all(&(*n as i16).to_be_bytes())
                } else if (-0x8000_0000..=0x7FFF_FFFF).contains(n) {
                    w.write_all(&[MARKER_INT32])?;
                    w.write_all(&(*n as i32).to_be_bytes())
                } else {
                    w.write_all(&[MARKER_INT64])?;
                    w.write_all(&n.to_be_bytes())
                }
            }
            Value::Float(f) => {
                w.write_all(&[MARKER_FLOAT64])?;
                w.write_all(&f.to_be_bytes())
            }
            Value::String(s) => encode_string(w, s),
            Value::Bytes(b) => encode_bytes(w, b),
            Value::List(items) => encode_list(w, items),
            Value::Map(entries) => encode_map(w, entries),
            Value::Struct(tag, fields) => encode_struct(w, *tag, fields),
        }
    }

    // ─── Constructors for common message structures ─────────────────

    /// Node struct: signature 0x4E
    pub fn node(id: i64, labels: Vec<String>, properties: HashMap<String, Value>) -> Value {
        Value::Struct(
            SIG_NODE,
            vec![
                Value::Int(id),
                Value::List(labels.into_iter().map(Value::String).collect()),
                Value::Map(properties),
            ],
        )
    }

    /// Relationship struct: signature 0x52
    pub fn relationship(
        id: i64,
        start: i64,
        end: i64,
        rel_type: &str,
        properties: HashMap<String, Value>,
    ) -> Value {
        Value::Struct(
            SIG_RELATIONSHIP,
            vec![
                Value::Int(id),
                Value::Int(start),
                Value::Int(end),
                Value::String(rel_type.to_string()),
                Value::Map(properties),
            ],
        )
    }

    /// Unbound relationship struct: signature 0x72
    pub fn unbound_relationship(
        id: i64,
        rel_type: &str,
        properties: HashMap<String, Value>,
    ) -> Value {
        Value::Struct(
            SIG_UNBOUND_RELATIONSHIP,
            vec![
                Value::Int(id),
                Value::String(rel_type.to_string()),
                Value::Map(properties),
            ],
        )
    }

    /// Path struct: signature 0x50
    /// Encodes a path as [nodes, unbound_rels, sequence] where sequence is
    /// a list of indices: even positions are node offsets, odd positions are
    /// relationship offsets (positive = forward, negative = backward).
    pub fn path(nodes: Vec<Value>, rels: Vec<Value>) -> Value {
        // Build sequence: start at node 0, then alternate rel/node indices.
        // For simplicity we assume nodes and rels are in path order.
        let mut sequence = Vec::new();
        sequence.push(0i64); // first node
        for i in 0..rels.len() {
            sequence.push(i as i64 + 1); // rel index (1-based in unbound_rels list)
            sequence.push(i as i64 + 1); // node index
        }
        Value::Struct(
            SIG_PATH,
            vec![
                Value::List(nodes),
                Value::List(rels),
                Value::List(sequence.into_iter().map(Value::Int).collect()),
            ],
        )
    }

    /// Success message struct: signature 0x70
    pub fn success(metadata: HashMap<String, Value>) -> Value {
        Value::Struct(SIG_SUCCESS, vec![Value::Map(metadata)])
    }

    /// Failure message struct: signature 0x7F
    pub fn failure(code: &str, message: &str) -> Value {
        let mut metadata = HashMap::new();
        metadata.insert("code".into(), Value::String(code.into()));
        metadata.insert("message".into(), Value::String(message.into()));
        Value::Struct(SIG_FAILURE, vec![Value::Map(metadata)])
    }

    /// Record message struct: signature 0x71
    pub fn record(fields: Vec<Value>) -> Value {
        Value::Struct(SIG_RECORD, vec![Value::List(fields)])
    }

    /// Ignored message struct: signature 0x7E
    pub fn ignored() -> Value {
        Value::Struct(SIG_IGNORED, vec![])
    }

    // ─── Type accessors ─────────────────────────────────────────────

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<Value>> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    // ─── Temporal constructors ──────────────────────────────────────

    /// Date: TinyStruct1 (sig 0x44) with `days_since_epoch`.
    pub fn date(d: Date) -> Value {
        Value::Struct(SIG_DATE, vec![Value::Int(d.days_since_epoch)])
    }

    /// LocalTime: TinyStruct1 (sig 0x74) with `nanoseconds_since_midnight`.
    /// mgcore stores microseconds; Bolt expects nanoseconds.
    pub fn local_time(t: LocalTime) -> Value {
        Value::Struct(SIG_LOCAL_TIME, vec![Value::Int(t.microseconds * 1_000)])
    }

    /// LocalDateTime: TinyStruct2 (sig 0x64) with `seconds`, `sub_second_nanos`.
    /// mgcore stores microseconds since epoch; split into seconds + remainder
    /// using Euclidean division so the sub-second component is always
    /// non-negative — matches the C++ encoder behavior for negative timestamps.
    pub fn local_date_time(dt: LocalDateTime) -> Value {
        let (secs, sub_ns) = split_micros_to_secs_nanos(dt.microseconds);
        Value::Struct(
            SIG_LOCAL_DATETIME,
            vec![Value::Int(secs), Value::Int(sub_ns)],
        )
    }

    /// Duration: TinyStruct4 (sig 0x45) with `months`, `days`, `seconds`,
    /// `sub_second_nanos`. Bolt always sends the four fields explicitly; mgcore
    /// stores months/days separately and the remainder as microseconds.
    pub fn duration(d: Duration) -> Value {
        let (secs, sub_ns) = split_micros_to_secs_nanos(d.microseconds);
        Value::Struct(
            SIG_DURATION,
            vec![
                Value::Int(d.months),
                Value::Int(d.days),
                Value::Int(secs),
                Value::Int(sub_ns),
            ],
        )
    }

    /// V5+ ZonedDateTime: TinyStruct3 (sig 0x49 / 0x69 with tz name) with
    /// `utc_seconds`, `sub_second_nanos`, and either an offset (seconds) or a
    /// timezone identifier.
    pub fn zoned_date_time_v5(dt: &ZonedDateTime) -> Value {
        let (secs, sub_ns) = split_micros_to_secs_nanos(dt.utc_microseconds);
        if dt.timezone.is_empty() {
            Value::Struct(
                SIG_DATETIME,
                vec![
                    Value::Int(secs),
                    Value::Int(sub_ns),
                    Value::Int(dt.offset_minutes as i64 * 60),
                ],
            )
        } else {
            Value::Struct(
                SIG_DATETIME_ZONE_ID,
                vec![
                    Value::Int(secs),
                    Value::Int(sub_ns),
                    Value::String(dt.timezone.clone()),
                ],
            )
        }
    }

    /// Legacy (Bolt v4) ZonedDateTime: TinyStruct3 (sig 0x46 / 0x66 with tz
    /// name) with `local_seconds`, `sub_second_nanos`, and either offset or
    /// tz id. The "local seconds" representation is `utc_seconds + offset`.
    pub fn zoned_date_time_legacy(dt: &ZonedDateTime) -> Value {
        let local_us = dt
            .utc_microseconds
            .saturating_add(dt.offset_minutes as i64 * 60 * 1_000_000);
        let (secs, sub_ns) = split_micros_to_secs_nanos(local_us);
        if dt.timezone.is_empty() {
            Value::Struct(
                SIG_DATETIME_LEGACY,
                vec![
                    Value::Int(secs),
                    Value::Int(sub_ns),
                    Value::Int(dt.offset_minutes as i64 * 60),
                ],
            )
        } else {
            Value::Struct(
                SIG_DATETIME_ZONE_ID_LEGACY,
                vec![
                    Value::Int(secs),
                    Value::Int(sub_ns),
                    Value::String(dt.timezone.clone()),
                ],
            )
        }
    }

    /// Pick the V5 or legacy ZonedDateTime form based on the negotiated Bolt
    /// major version. v4 → legacy, v5+ → V5.
    pub fn zoned_date_time(dt: &ZonedDateTime, bolt_major: u8) -> Value {
        if bolt_major >= 5 {
            Self::zoned_date_time_v5(dt)
        } else {
            Self::zoned_date_time_legacy(dt)
        }
    }

    /// Point2D: TinyStruct3 (sig 0x58) with `srid`, `x`, `y`.
    pub fn point_2d(p: Point2D) -> Value {
        Value::Struct(
            SIG_POINT_2D,
            vec![
                Value::Int(p.crs as u16 as i64),
                Value::Float(p.x),
                Value::Float(p.y),
            ],
        )
    }

    /// Point3D: TinyStruct4 (sig 0x59) with `srid`, `x`, `y`, `z`.
    pub fn point_3d(p: Point3D) -> Value {
        Value::Struct(
            SIG_POINT_3D,
            vec![
                Value::Int(p.crs as u16 as i64),
                Value::Float(p.x),
                Value::Float(p.y),
                Value::Float(p.z),
            ],
        )
    }

    /// Convert from mgcore PropertyValue to Bolt Value.
    ///
    /// `bolt_major` is the negotiated Bolt major version (used to select
    /// V5 vs legacy ZonedDateTime encodings). Defaults to 5 in
    /// [`Value::from_property_value`].
    pub fn from_property_value_v(pv: &PropertyValue, bolt_major: u8) -> Value {
        match pv {
            PropertyValue::Null => Value::Null,
            PropertyValue::Bool(b) => Value::Bool(*b),
            PropertyValue::Int(n) => Value::Int(*n),
            PropertyValue::Double(f) => Value::Float(*f),
            PropertyValue::String(s) => Value::String(s.clone()),
            PropertyValue::List(items) => Value::List(
                items
                    .iter()
                    .map(|v| Value::from_property_value_v(v, bolt_major))
                    .collect(),
            ),
            PropertyValue::Map(entries) => {
                let mut map = HashMap::new();
                for (k, v) in entries {
                    map.insert(k.clone(), Value::from_property_value_v(v, bolt_major));
                }
                Value::Map(map)
            }
            PropertyValue::Vertex(v) => Value::node(v.gid.as_int(), vec![], HashMap::new()),
            PropertyValue::Edge(e) => Value::relationship(
                e.gid.as_int(),
                e.from_vertex.as_int(),
                e.to_vertex.as_int(),
                &format!("{}", e.edge_type.as_uint()),
                HashMap::new(),
            ),
            PropertyValue::Date(d) => Value::date(*d),
            PropertyValue::LocalTime(t) => Value::local_time(*t),
            PropertyValue::LocalDateTime(dt) => Value::local_date_time(*dt),
            PropertyValue::ZonedDateTime(dt) => Value::zoned_date_time(dt, bolt_major),
            PropertyValue::Duration(d) => Value::duration(*d),
            PropertyValue::Point2D(p) => Value::point_2d(*p),
            PropertyValue::Point3D(p) => Value::point_3d(*p),
            _ => Value::Null,
        }
    }

    /// Convert from mgcore PropertyValue to Bolt Value, defaulting to V5
    /// ZonedDateTime semantics. Use [`Value::from_property_value_v`] to pick
    /// the legacy form for Bolt v4 clients.
    pub fn from_property_value(pv: &PropertyValue) -> Value {
        Value::from_property_value_v(pv, 5)
    }
}

/// Split a microsecond count into (seconds, sub_second_nanos), where the
/// sub-second component is always in `[0, 1_000_000_000)`. Uses Euclidean
/// division so negative timestamps still produce a non-negative remainder —
/// the same convention the C++ encoder relies on.
fn split_micros_to_secs_nanos(micros: i64) -> (i64, i64) {
    let secs = micros.div_euclid(1_000_000);
    let sub_us = micros.rem_euclid(1_000_000);
    (secs, sub_us * 1_000)
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "Null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(n) => write!(f, "{}", n),
            Value::Float(x) => write!(f, "{}", x),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Bytes(b) => write!(f, "Bytes({} bytes)", b.len()),
            Value::List(items) => write!(f, "List({:?})", items),
            Value::Map(m) => write!(f, "Map({:?})", m),
            Value::Struct(tag, fields) => write!(f, "Struct(0x{:02X}, {:?})", tag, fields),
        }
    }
}

// ─── Encoding helpers ────────────────────────────────────────────────────

/// Encode a size-prefixed collection: tiny (4-bit), uint8, uint16, or uint32 length.
/// `marker_tiny` is OR'd with the 4-bit size; `marker_8/16/32` are the fixed markers.
fn encode_size_prefix(
    w: &mut impl Write,
    len: usize,
    marker_tiny: u8,
    marker_8: u8,
    marker_16: u8,
    marker_32: u8,
) -> std::io::Result<()> {
    if len <= 0x0F {
        w.write_all(&[marker_tiny | len as u8])
    } else if len <= 0xFF {
        w.write_all(&[marker_8, len as u8])
    } else if len <= 0xFFFF {
        w.write_all(&[marker_16])?;
        w.write_all(&(len as u16).to_be_bytes())
    } else {
        w.write_all(&[marker_32])?;
        w.write_all(&(len as u32).to_be_bytes())
    }
}

fn encode_string(w: &mut impl Write, s: &str) -> std::io::Result<()> {
    encode_size_prefix(
        w,
        s.len(),
        MARKER_TINY_STRING,
        MARKER_STRING8,
        MARKER_STRING16,
        MARKER_STRING32,
    )?;
    w.write_all(s.as_bytes())
}

fn encode_bytes(w: &mut impl Write, b: &[u8]) -> std::io::Result<()> {
    encode_size_prefix(
        w,
        b.len(),
        0x00,
        MARKER_BYTES8,
        MARKER_BYTES16,
        MARKER_BYTES32,
    )?;
    w.write_all(b)
}

fn encode_list(w: &mut impl Write, items: &[Value]) -> std::io::Result<()> {
    encode_size_prefix(
        w,
        items.len(),
        MARKER_TINY_LIST,
        MARKER_LIST8,
        MARKER_LIST16,
        MARKER_LIST32,
    )?;
    for item in items {
        item.encode(w)?;
    }
    Ok(())
}

fn encode_map(w: &mut impl Write, entries: &HashMap<String, Value>) -> std::io::Result<()> {
    encode_size_prefix(
        w,
        entries.len(),
        MARKER_TINY_MAP,
        MARKER_MAP8,
        MARKER_MAP16,
        MARKER_MAP32,
    )?;
    for (key, value) in entries {
        encode_string(w, key)?;
        value.encode(w)?;
    }
    Ok(())
}

fn encode_struct(w: &mut impl Write, tag: u8, fields: &[Value]) -> std::io::Result<()> {
    let len = fields.len();
    if len <= 0x0F {
        w.write_all(&[MARKER_TINY_STRUCT | len as u8, tag])?;
    } else if len <= 0xFF {
        w.write_all(&[MARKER_STRUCT8, len as u8, tag])?;
    } else {
        w.write_all(&[MARKER_STRUCT16])?;
        w.write_all(&(len as u16).to_be_bytes())?;
        w.write_all(&[tag])?;
    }
    for field in fields {
        field.encode(w)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_null() {
        let mut buf = Vec::new();
        Value::Null.encode(&mut buf).unwrap();
        assert_eq!(buf, &[0xC0]);
    }

    #[test]
    fn test_encode_bool() {
        let mut buf = Vec::new();
        Value::Bool(true).encode(&mut buf).unwrap();
        assert_eq!(buf, &[0xC3]);
    }

    #[test]
    fn test_encode_small_int() {
        let mut buf = Vec::new();
        Value::Int(42).encode(&mut buf).unwrap();
        assert_eq!(buf, &[0x2A]); // tiny int
    }

    #[test]
    fn test_encode_big_int() {
        let mut buf = Vec::new();
        Value::Int(1000).encode(&mut buf).unwrap();
        assert_eq!(&buf[0..3], &[MARKER_INT16, 0x03, 0xE8]);
    }

    #[test]
    fn test_encode_string() {
        let mut buf = Vec::new();
        Value::String("hi".into()).encode(&mut buf).unwrap();
        assert_eq!(&buf[0..3], &[0x82, b'h', b'i']);
    }

    #[test]
    fn test_encode_success() {
        let mut meta = HashMap::new();
        meta.insert("server".into(), Value::String("memgraph-rs/0.1".into()));
        let mut buf = Vec::new();
        Value::success(meta).encode(&mut buf).unwrap();
        // B0 70 ... (tiny struct, tag 0x70, 1 field)
        assert_eq!(buf[0], 0xB1);
        assert_eq!(buf[1], 0x70);
    }

    // ─── Temporal/spatial encoding ──────────────────────────────────

    #[test]
    fn test_encode_date() {
        let mut buf = Vec::new();
        Value::date(Date::from_days(19000))
            .encode(&mut buf)
            .unwrap();
        // TinyStruct1 (0xB1) + sig 0x44 + Int16(19000) = 0xC9 0x4A 0x38
        assert_eq!(buf, &[0xB1, 0x44, MARKER_INT16, 0x4A, 0x38]);
    }

    #[test]
    fn test_encode_local_time_unit_conversion() {
        // 1 microsecond → 1000 nanoseconds on the wire.
        let mut buf = Vec::new();
        Value::local_time(LocalTime::from_microseconds(1))
            .encode(&mut buf)
            .unwrap();
        // Int16(1000): 0xC9 0x03 0xE8
        assert_eq!(buf, &[0xB1, 0x74, MARKER_INT16, 0x03, 0xE8]);
    }

    #[test]
    fn test_encode_local_date_time_split() {
        // 1.5s past epoch = 1_500_000us → seconds=1, sub_ns=500_000_000.
        let mut buf = Vec::new();
        Value::local_date_time(LocalDateTime::from_microseconds(1_500_000))
            .encode(&mut buf)
            .unwrap();
        // B2 64 01 CA 1D CD 65 00
        assert_eq!(buf[0], 0xB2);
        assert_eq!(buf[1], SIG_LOCAL_DATETIME);
        assert_eq!(buf[2], 0x01); // seconds = 1 (tiny int)
        assert_eq!(buf[3], MARKER_INT32); // 500_000_000 fits in i32
        assert_eq!(&buf[4..8], &500_000_000i32.to_be_bytes());
    }

    #[test]
    fn test_encode_local_date_time_negative_uses_euclidean() {
        // -500_000us = 0.5s before epoch → seconds=-1, sub_ns=500_000_000.
        let v = Value::local_date_time(LocalDateTime::from_microseconds(-500_000));
        if let Value::Struct(sig, fields) = v {
            assert_eq!(sig, SIG_LOCAL_DATETIME);
            assert_eq!(fields, vec![Value::Int(-1), Value::Int(500_000_000)]);
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_encode_duration_layout() {
        // 1 month, 5 days, 30.5s = 30_500_000us → seconds=30, sub_ns=500_000_000.
        let v = Value::duration(Duration::new(1, 5, 30_500_000));
        if let Value::Struct(sig, fields) = v {
            assert_eq!(sig, SIG_DURATION);
            assert_eq!(
                fields,
                vec![
                    Value::Int(1),
                    Value::Int(5),
                    Value::Int(30),
                    Value::Int(500_000_000),
                ]
            );
        } else {
            panic!("expected struct");
        }
        // Wire form starts with TinyStruct4.
        let mut buf = Vec::new();
        Value::duration(Duration::new(0, 0, 0))
            .encode(&mut buf)
            .unwrap();
        assert_eq!(buf, &[0xB4, SIG_DURATION, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_encode_zoned_date_time_v5_offset() {
        // utc=2024-05-28T12:00:00Z = 1716897600s, offset +60min, no tz name.
        let dt = ZonedDateTime::new(1_716_897_600_000_000, 60, String::new());
        let v = Value::zoned_date_time_v5(&dt);
        if let Value::Struct(sig, fields) = v {
            assert_eq!(sig, SIG_DATETIME); // 0x49 — offset variant
            assert_eq!(fields.len(), 3);
            assert_eq!(fields[0], Value::Int(1_716_897_600));
            assert_eq!(fields[1], Value::Int(0));
            assert_eq!(fields[2], Value::Int(3600)); // offset in seconds
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_encode_zoned_date_time_v5_tz_id() {
        let dt = ZonedDateTime::new(1_716_897_600_000_000, 60, "Europe/Paris".into());
        let v = Value::zoned_date_time_v5(&dt);
        if let Value::Struct(sig, fields) = v {
            assert_eq!(sig, SIG_DATETIME_ZONE_ID); // 0x69 — tz id variant
            assert_eq!(fields[2], Value::String("Europe/Paris".into()));
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_encode_zoned_date_time_legacy_uses_local_seconds() {
        // utc=1_716_897_600s, +60min offset → local=1_716_901_200s.
        let dt = ZonedDateTime::new(1_716_897_600_000_000, 60, String::new());
        let v = Value::zoned_date_time_legacy(&dt);
        if let Value::Struct(sig, fields) = v {
            assert_eq!(sig, SIG_DATETIME_LEGACY); // 0x46
            assert_eq!(fields[0], Value::Int(1_716_901_200));
            assert_eq!(fields[2], Value::Int(3600));
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_encode_zoned_date_time_picks_form_by_version() {
        let dt = ZonedDateTime::new(0, 0, String::new());
        match Value::zoned_date_time(&dt, 4) {
            Value::Struct(sig, _) => assert_eq!(sig, SIG_DATETIME_LEGACY),
            _ => panic!("expected struct"),
        }
        match Value::zoned_date_time(&dt, 5) {
            Value::Struct(sig, _) => assert_eq!(sig, SIG_DATETIME),
            _ => panic!("expected struct"),
        }
    }

    #[test]
    fn test_encode_point_2d_srid() {
        let p = Point2D::new(Crs::WGS84, 15.9819, 45.8150);
        let v = Value::point_2d(p);
        if let Value::Struct(sig, fields) = v {
            assert_eq!(sig, SIG_POINT_2D);
            assert_eq!(fields[0], Value::Int(4326));
            assert_eq!(fields[1], Value::Float(15.9819));
            assert_eq!(fields[2], Value::Float(45.8150));
        } else {
            panic!("expected struct");
        }
        // TinyStruct3 prefix + sig.
        let mut buf = Vec::new();
        Value::point_2d(p).encode(&mut buf).unwrap();
        assert_eq!(buf[0], 0xB3);
        assert_eq!(buf[1], SIG_POINT_2D);
    }

    #[test]
    fn test_encode_point_3d_srid() {
        let p = Point3D::new(Crs::Cartesian3D, 1.0, 2.0, 3.0);
        let v = Value::point_3d(p);
        if let Value::Struct(sig, fields) = v {
            assert_eq!(sig, SIG_POINT_3D);
            assert_eq!(fields[0], Value::Int(9157));
            assert_eq!(fields.len(), 4);
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_from_property_value_temporal() {
        let pv = PropertyValue::Date(Date::from_days(19000));
        match Value::from_property_value(&pv) {
            Value::Struct(sig, _) => assert_eq!(sig, SIG_DATE),
            _ => panic!("expected struct"),
        }
        let pv = PropertyValue::Point2D(Point2D::new(Crs::WGS84, 0.0, 0.0));
        match Value::from_property_value(&pv) {
            Value::Struct(sig, _) => assert_eq!(sig, SIG_POINT_2D),
            _ => panic!("expected struct"),
        }
    }

    #[test]
    fn test_from_property_value_v_picks_zdt_form() {
        let pv = PropertyValue::ZonedDateTime(ZonedDateTime::new(0, 0, String::new()));
        match Value::from_property_value_v(&pv, 4) {
            Value::Struct(sig, _) => assert_eq!(sig, SIG_DATETIME_LEGACY),
            _ => panic!("expected struct"),
        }
        match Value::from_property_value_v(&pv, 5) {
            Value::Struct(sig, _) => assert_eq!(sig, SIG_DATETIME),
            _ => panic!("expected struct"),
        }
    }
}
