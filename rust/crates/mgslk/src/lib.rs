//! # mgslk — SaveLoadKit serialization framework
//!
//! Bit-for-bit compatible with C++ `memgraph::slk`. Used by WAL, snapshots, RPC,
//! and replication.
//!
//! ## Wire format (same as C++)
//!
//! - **Primitives**: little-endian, fixed-size
//! - **String/Vec/Map**: u64 LE size prefix + data
//! - **Optional**: bool exists + value
//! - **Enum**: cast to underlying integer type
//! - **Segment framing**: `[u32 LE segment_size][data...]...[0x00000000 footer]`
//! - Max segment payload: 256 KiB

// ─── Constants ──────────────────────────────────────────────────────────────

pub const K_SEGMENT_MAX_DATA_SIZE: usize = 262_144;
pub const K_FOOTER: u32 = 0;
pub const K_FILE_DATA_MASK: u32 = 0xFFFF_FFFF;

pub const SNAPSHOT_MAGIC: &[u8; 4] = b"MGsn";
pub const WAL_MAGIC: &[u8; 4] = b"MGwl";
pub const DURABILITY_VERSION: u64 = 100;
pub const CPP_OLDEST_VERSION: u64 = 14;
pub const CPP_NEWEST_VERSION: u64 = 35;

pub fn is_cpp_version(v: u64) -> bool {
    (CPP_OLDEST_VERSION..=CPP_NEWEST_VERSION).contains(&v)
}

// ─── Error type ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlkDecodeError(String);

impl std::fmt::Display for SlkDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlkDecodeError: {}", self.0)
    }
}

impl std::error::Error for SlkDecodeError {}

impl From<String> for SlkDecodeError {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SlkDecodeError {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

// ─── Traits ─────────────────────────────────────────────────────────────────

/// Types that can be serialized to SLK.
pub trait SlkSave {
    fn slk_save(&self, builder: &mut Builder);
}

/// Types that can be deserialized from SLK.
pub trait SlkLoad: Sized {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError>;
}

// ─── Builder ────────────────────────────────────────────────────────────────

type WriteFn = Box<dyn FnMut(&[u8], bool)>;

/// SLK output stream. Segments data on the fly and writes via callback.
/// Matches C++ `slk::Builder`.
pub struct Builder {
    write: WriteFn,
    buffer: Vec<u8>,
}

impl Builder {
    pub fn new(write: impl FnMut(&[u8], bool) + 'static) -> Self {
        Self {
            write: Box::new(write),
            buffer: Vec::with_capacity(K_SEGMENT_MAX_DATA_SIZE + 8),
        }
    }

    /// Create a Builder that collects output into a Vec<u8>.
    /// Useful for encoding small messages without managing a closure.
    pub fn new_collecting() -> (Self, BuilderCollector) {
        let data = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let data_clone = data.clone();
        let builder = Self::new(move |bytes: &[u8], _final: bool| {
            data_clone.borrow_mut().extend_from_slice(bytes);
        });
        (builder, BuilderCollector { data })
    }

    /// Internal: save raw bytes into the current segment. Flushes if full.
    pub fn save_raw(&mut self, data: &[u8]) {
        if self.buffer.len() + data.len() > K_SEGMENT_MAX_DATA_SIZE {
            self.flush_segment(false);
        }
        self.buffer.extend_from_slice(data);
    }

    fn flush_segment(&mut self, final_segment: bool) {
        if self.buffer.is_empty() && !final_segment {
            return;
        }
        let seg_size = self.buffer.len() as u32;
        (self.write)(&seg_size.to_le_bytes(), false);
        if !self.buffer.is_empty() {
            (self.write)(&self.buffer, false);
            self.buffer.clear();
        }
        // Finalization writes the 0-footer
        if final_segment {
            (self.write)(&K_FOOTER.to_le_bytes(), true);
        }
    }

    /// Finalize the stream (flush last segment + footer). Call exactly once.
    pub fn finalize(&mut self) {
        self.flush_segment(true);
    }

    pub fn pos(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Save a u64 size prefix followed by a series of items using a custom save fn.
    pub fn save_collection<T>(
        &mut self,
        items: impl Iterator<Item = T>,
        save_item: impl Fn(&T, &mut Builder),
    ) {
        let items: Vec<T> = items.collect();
        (items.len() as u64).slk_save(self);
        for item in &items {
            save_item(item, self);
        }
    }
}

/// Encode a value to SLK-framed bytes.
pub fn slk_encode<T: SlkSave>(value: &T) -> Vec<u8> {
    let (mut builder, collector) = Builder::new_collecting();
    value.slk_save(&mut builder);
    builder.finalize();
    collector.into_vec()
}

/// Collects the output of a Builder created by `Builder::new_collecting()`.
pub struct BuilderCollector {
    data: std::rc::Rc<std::cell::RefCell<Vec<u8>>>,
}

impl BuilderCollector {
    /// Get the collected bytes.
    pub fn into_vec(self) -> Vec<u8> {
        match std::rc::Rc::try_unwrap(self.data) {
            Ok(ref_cell) => ref_cell.into_inner(),
            Err(rc) => rc.borrow().clone(),
        }
    }
}

// ─── Reader ─────────────────────────────────────────────────────────────────

/// SLK input stream. Reads from a contiguous buffer containing SLK-framed
/// segments. Handles segment framing internally.
///
/// SLK items never cross segment boundaries, so `load_raw` returns slices
/// borrowing from the original data.
pub struct Reader<'a> {
    /// The full stream data (all segments + footer), owned externally.
    full_data: &'a [u8],
    /// Position in `full_data` past the end of the current segment payload.
    /// The next segment header starts here.
    next_segment_start: usize,
    /// Current segment payload slice.
    segment: &'a [u8],
    /// Read position within `segment`.
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Create a Reader from SLK-framed bytes. Reads the first segment header.
    pub fn new(data: &'a [u8]) -> Self {
        let mut r = Self {
            full_data: data,
            next_segment_start: 0,
            segment: &[],
            pos: 0,
        };
        r.read_segment();
        r
    }

    /// Read the next segment header and set `segment`/`pos`.
    fn read_segment(&mut self) {
        let start = self.next_segment_start;
        if start + 4 > self.full_data.len() {
            self.segment = &[];
            self.pos = 0;
            return;
        }
        let seg_size = u32::from_le_bytes([
            self.full_data[start],
            self.full_data[start + 1],
            self.full_data[start + 2],
            self.full_data[start + 3],
        ]) as usize;

        if seg_size == 0 {
            // Footer
            self.next_segment_start = start + 4;
            self.segment = &[];
            self.pos = 0;
            return;
        }

        let payload_start = start + 4;
        let payload_end = payload_start + seg_size;
        if payload_end > self.full_data.len() {
            self.segment = &[];
            self.pos = 0;
            return;
        }
        self.segment = &self.full_data[payload_start..payload_end];
        self.pos = 0;
        self.next_segment_start = payload_end;
    }

    /// Read `len` bytes from the current segment. SLK items never cross segment
    /// boundaries, so this either succeeds in the current segment or fails.
    pub fn load_raw(&mut self, len: usize) -> Result<&'a [u8], SlkDecodeError> {
        if self.pos + len > self.segment.len() {
            return Err(SlkDecodeError(format!(
                "not enough data: need {} bytes, {} remaining in segment",
                len,
                self.segment.len().saturating_sub(self.pos)
            )));
        }
        let slice = &self.segment[self.pos..self.pos + len];
        self.pos += len;
        // Auto-advance to next segment if this one is fully consumed
        if self.pos >= self.segment.len() {
            self.read_segment();
        }
        Ok(slice)
    }

    pub fn pos(&self) -> usize {
        self.pos
    }
}

// ─── Primitive impls ────────────────────────────────────────────────────────

// All integer/float primitives: little-endian, matching C++ HostToLittleEndian.

macro_rules! impl_slk_primitive {
    ($t:ty) => {
        impl SlkSave for $t {
            fn slk_save(&self, builder: &mut Builder) {
                builder.save_raw(&self.to_le_bytes());
            }
        }
        impl SlkLoad for $t {
            fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
                let bytes = reader.load_raw(std::mem::size_of::<$t>())?;
                Ok(<$t>::from_le_bytes(bytes.try_into().unwrap()))
            }
        }
    };
}

// bool is handled separately (no to_le_bytes in std)
impl SlkSave for bool {
    fn slk_save(&self, builder: &mut Builder) {
        builder.save_raw(&[*self as u8]);
    }
}
impl SlkLoad for bool {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let b = reader.load_raw(1)?;
        Ok(b[0] != 0)
    }
}

impl_slk_primitive!(i8);
impl_slk_primitive!(u8);
impl_slk_primitive!(i16);
impl_slk_primitive!(u16);
impl_slk_primitive!(i32);
impl_slk_primitive!(u32);
impl_slk_primitive!(i64);
impl_slk_primitive!(u64);

impl SlkSave for f32 {
    fn slk_save(&self, builder: &mut Builder) {
        self.to_bits().slk_save(builder);
    }
}

impl SlkLoad for f32 {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let bits = u32::slk_load(reader)?;
        Ok(f32::from_bits(bits))
    }
}

impl SlkSave for f64 {
    fn slk_save(&self, builder: &mut Builder) {
        self.to_bits().slk_save(builder);
    }
}

impl SlkLoad for f64 {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let bits = u64::slk_load(reader)?;
        Ok(f64::from_bits(bits))
    }
}

// ─── String ─────────────────────────────────────────────────────────────────

impl SlkSave for String {
    fn slk_save(&self, builder: &mut Builder) {
        (self.len() as u64).slk_save(builder);
        builder.save_raw(self.as_bytes());
    }
}

impl SlkLoad for String {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let size = u64::slk_load(reader)? as usize;
        let bytes = reader.load_raw(size)?;
        Ok(String::from_utf8_lossy(bytes).into_owned())
    }
}

impl SlkSave for &str {
    fn slk_save(&self, builder: &mut Builder) {
        (self.len() as u64).slk_save(builder);
        builder.save_raw(self.as_bytes());
    }
}

// ─── Box<T> ─────────────────────────────────────────────────────────────────

impl<T: SlkSave> SlkSave for Box<T> {
    fn slk_save(&self, builder: &mut Builder) {
        self.as_ref().slk_save(builder);
    }
}

impl<T: SlkLoad> SlkLoad for Box<T> {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        T::slk_load(reader).map(Box::new)
    }
}

// ─── Vec<T> ─────────────────────────────────────────────────────────────────

impl<T: SlkSave> SlkSave for Vec<T> {
    fn slk_save(&self, builder: &mut Builder) {
        (self.len() as u64).slk_save(builder);
        for item in self {
            item.slk_save(builder);
        }
    }
}

impl<T: SlkLoad> SlkLoad for Vec<T> {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let size = u64::slk_load(reader)? as usize;
        let mut items = Vec::with_capacity(size);
        for _ in 0..size {
            items.push(T::slk_load(reader)?);
        }
        Ok(items)
    }
}

// ─── Option<T> ──────────────────────────────────────────────────────────────

impl<T: SlkSave> SlkSave for Option<T> {
    fn slk_save(&self, builder: &mut Builder) {
        match self {
            None => false.slk_save(builder),
            Some(val) => {
                true.slk_save(builder);
                val.slk_save(builder);
            }
        }
    }
}

impl<T: SlkLoad> SlkLoad for Option<T> {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let exists = bool::slk_load(reader)?;
        if exists {
            Ok(Some(T::slk_load(reader)?))
        } else {
            Ok(None)
        }
    }
}

// ─── (A, B) ─────────────────────────────────────────────────────────────────

impl<A: SlkSave, B: SlkSave> SlkSave for (A, B) {
    fn slk_save(&self, builder: &mut Builder) {
        self.0.slk_save(builder);
        self.1.slk_save(builder);
    }
}

impl<A: SlkLoad, B: SlkLoad> SlkLoad for (A, B) {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok((A::slk_load(reader)?, B::slk_load(reader)?))
    }
}

// ─── std::collections impls ─────────────────────────────────────────────────

use std::collections::{HashMap, HashSet};

impl<K: SlkSave + Eq + std::hash::Hash, V: SlkSave> SlkSave for HashMap<K, V> {
    fn slk_save(&self, builder: &mut Builder) {
        (self.len() as u64).slk_save(builder);
        for (k, v) in self {
            k.slk_save(builder);
            v.slk_save(builder);
        }
    }
}

impl<K: SlkLoad + Eq + std::hash::Hash, V: SlkLoad> SlkLoad for HashMap<K, V> {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let size = u64::slk_load(reader)? as usize;
        let mut map = HashMap::with_capacity(size);
        for _ in 0..size {
            let k = K::slk_load(reader)?;
            let v = V::slk_load(reader)?;
            map.insert(k, v);
        }
        Ok(map)
    }
}

impl<T: SlkSave + Eq + std::hash::Hash> SlkSave for HashSet<T> {
    fn slk_save(&self, builder: &mut Builder) {
        (self.len() as u64).slk_save(builder);
        for item in self {
            item.slk_save(builder);
        }
    }
}

impl<T: SlkLoad + Eq + std::hash::Hash> SlkLoad for HashSet<T> {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let size = u64::slk_load(reader)? as usize;
        let mut set = HashSet::with_capacity(size);
        for _ in 0..size {
            set.insert(T::slk_load(reader)?);
        }
        Ok(set)
    }
}

// ─── Unit type (for empty structs) ──────────────────────────────────────────

impl SlkSave for () {
    fn slk_save(&self, _builder: &mut Builder) {}
}

impl SlkLoad for () {
    fn slk_load(_reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(())
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: serialize a single value and return the raw bytes (without SLK framing).
    fn slk_to_bytes(v: &impl SlkSave) -> Vec<u8> {
        let output = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let out_clone = output.clone();
        {
            let mut builder = Builder::new(move |data: &[u8], _final: bool| {
                out_clone.borrow_mut().extend_from_slice(data);
            });
            v.slk_save(&mut builder);
            builder.finalize();
        }
        let output = output.borrow();
        // Strip the segment framing to get just the payload
        // Format: [u32 LE size][payload][u32 LE footer=0]
        if output.len() >= 8 {
            let seg_size =
                u32::from_le_bytes([output[0], output[1], output[2], output[3]]) as usize;
            if seg_size + 4 + 4 == output.len() {
                return output[4..4 + seg_size].to_vec();
            }
        }
        output.clone()
    }

    /// Helper: deserialize a value from raw SLK bytes.
    fn slk_from_bytes<T: SlkLoad>(data: &[u8]) -> Result<T, SlkDecodeError> {
        // Wrap in a single segment
        let mut framed = Vec::new();
        framed.extend_from_slice(&(data.len() as u32).to_le_bytes());
        framed.extend_from_slice(data);
        let mut reader = Reader::new(&framed);
        T::slk_load(&mut reader)
    }

    fn roundtrip<T: SlkSave + SlkLoad + PartialEq + std::fmt::Debug>(val: T) {
        let bytes = slk_to_bytes(&val);
        let back: T = slk_from_bytes(&bytes).expect("deserialize failed");
        assert_eq!(val, back, "roundtrip failed for {:?}", val);
    }

    #[test]
    fn test_primitives_roundtrip() {
        roundtrip(true);
        roundtrip(false);
        roundtrip(0u8);
        roundtrip(255u8);
        roundtrip(-1i32);
        roundtrip(42i64);
        roundtrip(u64::MAX);
        roundtrip(3.14f64);
        roundtrip(-0.0f64);
        roundtrip(std::f64::consts::PI);
    }

    #[test]
    fn test_u32_le_encoding() {
        // Verify exact little-endian encoding (must match C++)
        let bytes = slk_to_bytes(&0x12345678u32);
        assert_eq!(bytes, &[0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn test_u64_le_encoding() {
        let bytes = slk_to_bytes(&0x0102030405060708u64);
        assert_eq!(bytes, &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
    }

    #[test]
    fn test_string_roundtrip() {
        roundtrip(String::new());
        roundtrip("hello world".to_string());
        roundtrip("🦀🚀".to_string()); // unicode
        roundtrip("a".repeat(10000)); // long string
    }

    #[test]
    fn test_string_format_matches_cpp() {
        // String = [size: u64 LE][utf8 bytes...]
        let bytes = slk_to_bytes(&"hi".to_string());
        assert_eq!(&bytes[0..8], &[2, 0, 0, 0, 0, 0, 0, 0]); // size=2
        assert_eq!(&bytes[8..10], b"hi");
    }

    #[test]
    fn test_vec_roundtrip() {
        roundtrip(vec![1i32, 2, 3]);
        roundtrip(Vec::<String>::new());
        roundtrip(vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_option_roundtrip() {
        roundtrip(None::<i32>);
        roundtrip(Some(42i64));
        roundtrip(Some("hello".to_string()));
    }

    #[test]
    fn test_option_format() {
        // None = false (1 byte)
        let bytes = slk_to_bytes(&None::<i32>);
        assert_eq!(bytes, &[0]); // false

        // Some(42i64) = true + 42i64 LE
        let bytes = slk_to_bytes(&Some(42i64));
        assert_eq!(bytes[0], 1); // true
        assert_eq!(&bytes[1..9], &42i64.to_le_bytes());
    }

    #[test]
    fn test_hashmap_roundtrip() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), 100i64);
        map.insert("key2".to_string(), 200i64);
        // HashMap iteration order is non-deterministic, so compare element-wise
        let bytes = slk_to_bytes(&map);
        let back: HashMap<String, i64> = slk_from_bytes(&bytes).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back.get("key1"), Some(&100));
        assert_eq!(back.get("key2"), Some(&200));
    }

    #[test]
    fn test_tuple_roundtrip() {
        roundtrip((42i32, "hello".to_string()));
        roundtrip((true, (3.14f64, "nested".to_string())));
    }

    #[test]
    fn test_builder_segment_flush() {
        let output = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let out_clone = output.clone();
        {
            let mut builder = Builder::new(move |data: &[u8], _final: bool| {
                out_clone.borrow_mut().extend_from_slice(data);
            });
            let big = vec![0u8; K_SEGMENT_MAX_DATA_SIZE + 100];
            builder.save_raw(&big);
            builder.finalize();
        }
        let output = output.borrow();
        // Should have 2 segment headers + data + footer
        assert!(output.len() > K_SEGMENT_MAX_DATA_SIZE);
        // Last 4 bytes are footer
        assert_eq!(&output[output.len() - 4..], &[0, 0, 0, 0]);
    }

    #[test]
    fn test_reader_insufficient_data() {
        let mut reader = Reader::new(&[1, 2, 3]);
        let err = reader.load_raw(10).unwrap_err();
        assert!(err.0.contains("not enough data"));
    }

    // ─── Extended roundtrip tests ──────────────────────────────────────

    #[test]
    fn test_nested_structures() {
        // Vec<Option<i64>>
        roundtrip(vec![Some(1i64), None, Some(3), None, Some(5)]);
        // Vec<Vec<i32>>
        roundtrip(vec![vec![1i32, 2], vec![], vec![3, 4, 5]]);
        // Option<Vec<String>>
        roundtrip(Some(vec!["a".to_string(), "b".to_string()]));
        roundtrip(None::<Vec<String>>);
        // HashMap<String, Vec<i64>>
        let mut map = HashMap::new();
        map.insert("odds".to_string(), vec![1i64, 3, 5]);
        map.insert("evens".to_string(), vec![2i64, 4, 6]);
        roundtrip(map);
        // Nested tuple
        roundtrip(((Some(42i64), "nested".to_string()), true));
    }

    #[test]
    fn test_edge_case_values() {
        roundtrip(vec![0u8; 0]); // empty vec
        roundtrip(String::from("\0null\0byte")); // embedded nulls
        roundtrip(vec![i64::MIN, -1, 0, 1, i64::MAX]);
        roundtrip(vec![f64::INFINITY, f64::NEG_INFINITY, 0.0f64, -0.0f64]);
        // NaN needs special handling (NaN != NaN)
        let nan_bytes = slk_to_bytes(&vec![f64::NAN]);
        let back: Vec<f64> = slk_from_bytes(&nan_bytes).unwrap();
        assert_eq!(back.len(), 1);
        assert!(back[0].is_nan());
        // Nested options
        roundtrip(Some(Some(Some(42i64))));
        roundtrip(None::<Option<Option<i64>>>);
        roundtrip(vec![None::<i64>, Some(1), None, Some(2)]);
    }

    #[test]
    fn test_large_collections() {
        // 10K element Vec
        let big: Vec<i64> = (0..10_000).map(|i| i * 7919 % 100003).collect();
        roundtrip(big);

        // Large string (100K chars)
        let big_str = "abcdefghij".repeat(10_000);
        roundtrip(big_str);
    }

    #[test]
    fn test_hashset_roundtrip() {
        let mut set = HashSet::new();
        set.insert(1u64);
        set.insert(2);
        set.insert(3);
        let bytes = slk_to_bytes(&set);
        let back: HashSet<u64> = slk_from_bytes(&bytes).unwrap();
        assert_eq!(back, set);

        // Empty set
        roundtrip(HashSet::<i32>::new());
    }

    #[test]
    fn test_reader_multi_segment() {
        // Build data that spans exactly two segments
        let segment1_data = vec![0xAAu8; K_SEGMENT_MAX_DATA_SIZE];
        let segment2_data = vec![0xBBu8; 128];

        let mut framed = Vec::new();
        // Segment 1
        framed.extend_from_slice(&(segment1_data.len() as u32).to_le_bytes());
        framed.extend_from_slice(&segment1_data);
        // Segment 2
        framed.extend_from_slice(&(segment2_data.len() as u32).to_le_bytes());
        framed.extend_from_slice(&segment2_data);
        // Footer
        framed.extend_from_slice(&0u32.to_le_bytes());

        let mut reader = Reader::new(&framed);

        // Read segment 1 completely
        let chunk1 = reader.load_raw(K_SEGMENT_MAX_DATA_SIZE).unwrap();
        assert_eq!(chunk1.len(), K_SEGMENT_MAX_DATA_SIZE);
        assert_eq!(chunk1[0], 0xAA);

        // Next read should come from segment 2 (auto-advanced)
        let chunk2 = reader.load_raw(128).unwrap();
        assert_eq!(chunk2.len(), 128);
        assert_eq!(chunk2[0], 0xBB);

        // Verify footer reached (empty segment)
        let chunk3 = reader.load_raw(1);
        assert!(chunk3.is_err());
    }

    #[test]
    fn test_varint_roundtrip() {
        let values = vec![
            0u64,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            2097151,
            2097152,
            268435455,
            268435456,
            u32::MAX as u64,
            u64::MAX,
        ];
        for v in values {
            let mut buf = Vec::new();
            encode_varint(&mut buf, v);
            let (decoded, len) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, v, "varint roundtrip failed for {}", v);
            assert_eq!(len, buf.len());
        }
    }

    #[test]
    fn test_zigzag_varint_roundtrip() {
        let values = vec![
            0i64,
            1,
            -1,
            2,
            -2,
            127,
            -127,
            128,
            -128,
            i32::MAX as i64,
            i32::MIN as i64,
            i64::MAX,
            i64::MIN,
        ];
        for v in values {
            let mut buf = Vec::new();
            encode_zigzag_varint(&mut buf, v);
            let (decoded, len) = decode_zigzag_varint(&buf).unwrap();
            assert_eq!(decoded, v, "zigzag varint roundtrip failed for {}", v);
            assert_eq!(len, buf.len());
        }
    }

    #[test]
    fn test_slk_encode_decode_u64() {
        // Test the public slk_encode function
        let encoded = slk_encode(&42u64);
        let mut reader = Reader::new(&encoded);
        let decoded = u64::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, 42);
    }

    #[test]
    fn test_slk_encode_decode_string() {
        let encoded = slk_encode(&"hello world".to_string());
        let mut reader = Reader::new(&encoded);
        let decoded = String::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, "hello world");
    }

    #[test]
    fn test_slk_encode_decode_nested() {
        let val: Vec<Option<(i32, String)>> =
            vec![Some((1, "one".into())), None, Some((3, "three".into()))];
        let encoded = slk_encode(&val);
        let mut reader = Reader::new(&encoded);
        let decoded = Vec::<Option<(i32, String)>>::slk_load(&mut reader).unwrap();
        for (i, item) in val.iter().enumerate() {
            match item {
                None => assert!(decoded[i].is_none()),
                Some((a, b)) => {
                    let (da, db) = decoded[i].as_ref().unwrap();
                    assert_eq!(a, da);
                    assert_eq!(b, db);
                }
            }
        }
    }

    // ─── Random fuzz-style roundtrip ──────────────────────────────────

    fn fuzz_lcg(seed: &mut u64) -> u64 {
        const A: u64 = 6364136223846793005;
        const C: u64 = 1442695040888963407;
        *seed = seed.wrapping_mul(A).wrapping_add(C);
        *seed
    }

    #[test]
    fn test_random_roundtrip_10k() {
        let mut seed = 0xDEADBEEFu64;
        for n in 0..10_000u64 {
            let val: Vec<Option<(i64, String)>> = (0..(fuzz_lcg(&mut seed) % 20) as usize)
                .map(|_| {
                    if fuzz_lcg(&mut seed) % 3 == 0 {
                        None
                    } else {
                        Some((
                            fuzz_lcg(&mut seed) as i64,
                            format!("v{}", fuzz_lcg(&mut seed)),
                        ))
                    }
                })
                .collect();
            let bytes = slk_to_bytes(&val);
            let back: Vec<Option<(i64, String)>> = slk_from_bytes(&bytes)
                .unwrap_or_else(|e| panic!("deserialize failed at iteration {}: {}", n, e));
            assert_eq!(val, back, "roundtrip failed at iteration {}", n);
        }
    }
}

// ─── Varint encoding (matches C++ SLK) ────────────────────────────────────

/// Encode a u64 as a varint (1-10 bytes, MSB continuation bit).
pub fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a varint from a byte slice. Returns (value, bytes_consumed).
pub fn decode_varint(data: &[u8]) -> Result<(u64, usize), SlkDecodeError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in data.iter().enumerate() {
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(SlkDecodeError("varint overflow".into()));
        }
    }
    Err(SlkDecodeError("truncated varint".into()))
}

/// Encode an i64 as a zigzag-varint (matches protobuf encoding).
pub fn encode_zigzag_varint(buf: &mut Vec<u8>, value: i64) {
    encode_varint(buf, ((value << 1) ^ (value >> 63)) as u64);
}

/// Decode a zigzag-varint to i64.
pub fn decode_zigzag_varint(data: &[u8]) -> Result<(i64, usize), SlkDecodeError> {
    let (raw, len) = decode_varint(data)?;
    let value = ((raw >> 1) as i64) ^ -((raw & 1) as i64);
    Ok((value, len))
}
