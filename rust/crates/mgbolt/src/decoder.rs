//! PackStream decoder — deserializes Bolt values from raw bytes.

use std::collections::HashMap;

use crate::value::{Value, MARKER_FALSE, MARKER_FLOAT64, MARKER_INT16, MARKER_INT32, MARKER_INT64, MARKER_INT8, MARKER_LIST16, MARKER_LIST32, MARKER_LIST8, MARKER_MAP16, MARKER_MAP32, MARKER_MAP8, MARKER_NULL, MARKER_STRING16, MARKER_STRING32, MARKER_STRING8, MARKER_STRUCT16, MARKER_STRUCT8, MARKER_TRUE};

/// PackStream decoding error.
#[derive(Debug)]
pub enum DecodeError {
    UnexpectedEof,
    InvalidMarker(u8),
    InvalidUtf8,
}

/// Decode a complete PackStream value from a byte slice.
pub fn decode_value(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.is_empty() {
        return Err(DecodeError::UnexpectedEof);
    }
    let marker = data[0];
    match marker {
        MARKER_NULL => Ok((Value::Null, 1)),
        MARKER_TRUE => Ok((Value::Bool(true), 1)),
        MARKER_FALSE => Ok((Value::Bool(false), 1)),
        MARKER_FLOAT64 => {
            if data.len() < 9 { return Err(DecodeError::UnexpectedEof); }
            let f = f64::from_be_bytes([data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]]);
            Ok((Value::Float(f), 9))
        }
        MARKER_INT8 => {
            if data.len() < 2 { return Err(DecodeError::UnexpectedEof); }
            Ok((Value::Int(data[1] as i8 as i64), 2))
        }
        MARKER_INT16 => {
            if data.len() < 3 { return Err(DecodeError::UnexpectedEof); }
            let n = i16::from_be_bytes([data[1], data[2]]) as i64;
            Ok((Value::Int(n), 3))
        }
        MARKER_INT32 => {
            if data.len() < 5 { return Err(DecodeError::UnexpectedEof); }
            let n = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as i64;
            Ok((Value::Int(n), 5))
        }
        MARKER_INT64 => {
            if data.len() < 9 { return Err(DecodeError::UnexpectedEof); }
            let n = i64::from_be_bytes([data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]]);
            Ok((Value::Int(n), 9))
        }
        // Tiny int: marker encodes value -16..127
        // 0x00-0x7F → 0..127, 0xF0-0xFF → -16..-1
        // Exclude ranges claimed by tiny string/list/map/struct (0x80-0xBF)
        _ if marker <= 0x7F || marker >= 0xF0 => {
            Ok((Value::Int(marker as i8 as i64), 1))
        }
        // String
        MARKER_STRING8 => decode_string8(data),
        MARKER_STRING16 => decode_string16(data),
        MARKER_STRING32 => decode_string32(data),
        _ if (0x80..=0x8F).contains(&marker) => {
            let len = (marker & 0x0F) as usize;
            decode_str(data, 1, len)
        }
        // List
        MARKER_LIST8 => decode_list8(data),
        MARKER_LIST16 => decode_list16(data),
        MARKER_LIST32 => decode_list32(data),
        _ if (0x90..=0x9F).contains(&marker) => {
            let count = (marker & 0x0F) as usize;
            decode_list(data, 1, count)
        }
        // Map
        MARKER_MAP8 => decode_map8(data),
        MARKER_MAP16 => decode_map16(data),
        MARKER_MAP32 => decode_map32(data),
        _ if (0xA0..=0xAF).contains(&marker) => {
            let count = (marker & 0x0F) as usize;
            decode_map(data, 1, count)
        }
        // Struct
        MARKER_STRUCT8 => decode_struct8(data),
        MARKER_STRUCT16 => decode_struct16(data),
        _ if (0xB0..=0xBF).contains(&marker) => {
            let count = (marker & 0x0F) as usize;
            if data.len() < 2 { return Err(DecodeError::UnexpectedEof); }
            let tag = data[1];
            decode_struct(data, 2, count, tag)
        }
        _ => Err(DecodeError::InvalidMarker(marker)),
    }
}

// ─── String helpers ──────────────────────────────────────────────────────

fn decode_str(data: &[u8], start: usize, len: usize) -> Result<(Value, usize), DecodeError> {
    let end = start + len;
    if data.len() < end { return Err(DecodeError::UnexpectedEof); }
    let s = std::str::from_utf8(&data[start..end]).map_err(|e| {
        DecodeError::InvalidUtf8
    })?;
    Ok((Value::String(s.to_string()), end))
}

fn decode_string8(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 2 { return Err(DecodeError::UnexpectedEof); }
    let len = data[1] as usize;
    decode_str(data, 2, len)
}

fn decode_string16(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 3 { return Err(DecodeError::UnexpectedEof); }
    let len = u16::from_be_bytes([data[1], data[2]]) as usize;
    decode_str(data, 3, len)
}

fn decode_string32(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 5 { return Err(DecodeError::UnexpectedEof); }
    let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
    decode_str(data, 5, len)
}

// ─── List helpers ─────────────────────────────────────────────────────────

fn decode_list(data: &[u8], mut pos: usize, count: usize) -> Result<(Value, usize), DecodeError> {
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let (val, used) = decode_value(&data[pos..])?;
        items.push(val);
        pos += used;
    }
    Ok((Value::List(items), pos))
}

fn decode_list8(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 2 { return Err(DecodeError::UnexpectedEof); }
    decode_list(data, 2, data[1] as usize)
}

fn decode_list16(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 3 { return Err(DecodeError::UnexpectedEof); }
    decode_list(data, 3, u16::from_be_bytes([data[1], data[2]]) as usize)
}

fn decode_list32(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 5 { return Err(DecodeError::UnexpectedEof); }
    decode_list(data, 5, u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize)
}

// ─── Map helpers ──────────────────────────────────────────────────────────

fn decode_map(data: &[u8], mut pos: usize, count: usize) -> Result<(Value, usize), DecodeError> {
    let mut map = HashMap::with_capacity(count);
    for _ in 0..count {
        let (key, used) = decode_value(&data[pos..])?;
        pos += used;
        let (val, used) = decode_value(&data[pos..])?;
        pos += used;
        if let Value::String(k) = key {
            map.insert(k, val);
        }
    }
    Ok((Value::Map(map), pos))
}

fn decode_map8(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 2 { return Err(DecodeError::UnexpectedEof); }
    decode_map(data, 2, data[1] as usize)
}

fn decode_map16(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 3 { return Err(DecodeError::UnexpectedEof); }
    decode_map(data, 3, u16::from_be_bytes([data[1], data[2]]) as usize)
}

fn decode_map32(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 5 { return Err(DecodeError::UnexpectedEof); }
    decode_map(data, 5, u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize)
}

// ─── Struct helpers ───────────────────────────────────────────────────────

fn decode_struct(data: &[u8], mut pos: usize, count: usize, tag: u8) -> Result<(Value, usize), DecodeError> {
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let (val, used) = decode_value(&data[pos..])?;
        fields.push(val);
        pos += used;
    }
    Ok((Value::Struct(tag, fields), pos))
}

fn decode_struct8(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 3 { return Err(DecodeError::UnexpectedEof); }
    let count = data[1] as usize;
    let tag = data[2];
    decode_struct(data, 3, count, tag)
}

fn decode_struct16(data: &[u8]) -> Result<(Value, usize), DecodeError> {
    if data.len() < 4 { return Err(DecodeError::UnexpectedEof); }
    let count = u16::from_be_bytes([data[1], data[2]]) as usize;
    let tag = data[3];
    decode_struct(data, 4, count, tag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_null() {
        assert_eq!(decode_value(&[0xC0]).unwrap().0, Value::Null);
    }

    #[test]
    fn test_decode_bool() {
        assert_eq!(decode_value(&[0xC3]).unwrap().0, Value::Bool(true));
        assert_eq!(decode_value(&[0xC2]).unwrap().0, Value::Bool(false));
    }

    #[test]
    fn test_decode_tiny_int() {
        assert_eq!(decode_value(&[0x2A]).unwrap().0, Value::Int(42));
    }

    #[test]
    fn test_decode_int16() {
        let mut buf = Vec::new();
        Value::Int(1000).encode(&mut buf).unwrap();
        assert_eq!(decode_value(&buf).unwrap().0, Value::Int(1000));
    }

    #[test]
    fn test_decode_string() {
        let mut buf = Vec::new();
        Value::String("hello".into()).encode(&mut buf).unwrap();
        assert_eq!(decode_value(&buf).unwrap().0, Value::String("hello".into()));
    }

    #[test]
    fn test_decode_struct() {
        let mut buf = Vec::new();
        Value::success(HashMap::new()).encode(&mut buf).unwrap();
        let (val, _) = decode_value(&buf).unwrap();
        if let Value::Struct(tag, _) = val {
            assert_eq!(tag, 0x70);
        } else {
            panic!("expected struct");
        }
    }

    #[test]
    fn test_roundtrip_all() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(42),
            Value::Int(-1),
            Value::Int(1000),
            Value::Int(100000),
            Value::Float(3.14),
            Value::String("hello".into()),
            Value::String("".into()),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
            Value::Struct(0x70, vec![Value::Map(HashMap::new())]),
        ];
        for v in values {
            let mut buf = Vec::new();
            v.encode(&mut buf).unwrap();
            let (decoded, used) = decode_value(&buf).unwrap();
            assert_eq!(used, buf.len(), "used bytes mismatch for {:?}", v);
            assert_eq!(decoded, v, "roundtrip failed for {:?}", v);
        }
    }
}
