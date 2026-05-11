//! Kafka binary wire protocol (v3-v4 compatible).
//! Pure Rust implementation — no external dependencies.

use std::io::Read;

use crate::{KafkaError, KafkaMessage};

// ─── Primitive encoders ────────────────────────────────────────────────────

pub(crate) fn write_i16(buf: &mut Vec<u8>, v: i16) {
    buf.extend_from_slice(&v.to_be_bytes());
}

pub(crate) fn write_i32(buf: &mut Vec<u8>, v: i32) {
    buf.extend_from_slice(&v.to_be_bytes());
}

pub(crate) fn write_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

pub(crate) fn write_i8(buf: &mut Vec<u8>, v: i8) {
    buf.push(v as u8);
}

pub(crate) fn write_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

pub(crate) fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    write_i16(buf, bytes.len() as i16);
    buf.extend_from_slice(bytes);
}

pub(crate) fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    write_i32(buf, data.len() as i32);
    buf.extend_from_slice(data);
}

pub(crate) fn write_nullable_string(buf: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(v) => write_string(buf, v),
        None => write_i16(buf, -1),
    }
}

pub(crate) fn write_nullable_bytes(buf: &mut Vec<u8>, data: Option<&[u8]>) {
    match data {
        Some(d) => write_bytes(buf, d),
        None => write_i32(buf, -1),
    }
}

pub(crate) fn write_uuid(buf: &mut Vec<u8>, uuid: [u8; 16]) {
    buf.extend_from_slice(&uuid);
}

/// Unsigned varint encoder (Kafka v2+).
pub(crate) fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut b = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
        }
        buf.push(b);
        if v == 0 {
            break;
        }
    }
}

/// Signed varint encoder (zigzag: (n << 1) ^ (n >> 63)).
pub(crate) fn write_signed_varint(buf: &mut Vec<u8>, v: i64) {
    let encoded = ((v << 1) ^ (v >> 63)) as u64;
    write_varint(buf, encoded);
}

// ─── Primitive decoders ────────────────────────────────────────────────────

pub(crate) struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn read_i16(&mut self) -> Result<i16, KafkaError> {
        if self.pos + 2 > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let v = i16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn read_i32(&mut self) -> Result<i32, KafkaError> {
        if self.pos + 4 > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let v = i32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    pub fn read_i64(&mut self) -> Result<i64, KafkaError> {
        if self.pos + 8 > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let v = i64::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(v)
    }

    pub fn read_i8(&mut self) -> Result<i8, KafkaError> {
        if self.pos >= self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let v = self.data[self.pos] as i8;
        self.pos += 1;
        Ok(v)
    }

    pub fn read_u8(&mut self) -> Result<u8, KafkaError> {
        if self.pos >= self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    pub fn read_string(&mut self) -> Result<String, KafkaError> {
        let len = self.read_i16()?;
        if len == -1 {
            return Ok(String::new());
        } // null string
        let len = len as usize;
        if self.pos + len > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let s = String::from_utf8_lossy(&self.data[self.pos..self.pos + len]).into_owned();
        self.pos += len;
        Ok(s)
    }

    pub fn read_nullable_string(&mut self) -> Result<Option<String>, KafkaError> {
        let len = self.read_i16()?;
        if len == -1 {
            return Ok(None);
        }
        let len = len as usize;
        if self.pos + len > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let s = String::from_utf8_lossy(&self.data[self.pos..self.pos + len]).into_owned();
        self.pos += len;
        Ok(Some(s))
    }

    pub fn read_bytes(&mut self) -> Result<Vec<u8>, KafkaError> {
        let len = self.read_i32()?;
        if len == -1 {
            return Ok(Vec::new());
        } // null bytes
        let len = len as usize;
        if self.pos + len > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let v = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(v)
    }

    pub fn read_nullable_bytes(&mut self) -> Result<Option<Vec<u8>>, KafkaError> {
        let len = self.read_i32()?;
        if len == -1 {
            return Ok(None);
        }
        let len = len as usize;
        if self.pos + len > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let v = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(Some(v))
    }

    /// Read a UUID (16 bytes).
    pub fn read_uuid(&mut self) -> Result<[u8; 16], KafkaError> {
        if self.pos + 16 > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&self.data[self.pos..self.pos + 16]);
        self.pos += 16;
        Ok(uuid)
    }

    /// Read an unsigned varint.
    pub fn read_varint(&mut self) -> Result<u64, KafkaError> {
        let mut value: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            if self.pos >= self.data.len() {
                return Err(KafkaError::Protocol("eof".into()));
            }
            let b = self.data[self.pos];
            self.pos += 1;
            value |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 63 {
                return Err(KafkaError::Protocol("varint overflow".into()));
            }
        }
        Ok(value)
    }

    /// Read a signed varint (zigzag decoded).
    pub fn read_signed_varint(&mut self) -> Result<i64, KafkaError> {
        let encoded = self.read_varint()?;
        let v = (encoded >> 1) as i64;
        let sign = (encoded & 1) as i64;
        Ok(v ^ -sign)
    }

    pub fn read_varint_bytes(&mut self) -> Result<Vec<u8>, KafkaError> {
        let len = self.read_varint()? as usize;
        if len == 0 {
            return Ok(Vec::new());
        }
        if self.pos + len > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let v = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(v)
    }

    pub fn read_varint_string(&mut self) -> Result<String, KafkaError> {
        let bytes = self.read_varint_bytes()?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    /// Read a compact string (length as varint, len-1 encoded).
    pub fn read_compact_string(&mut self) -> Result<String, KafkaError> {
        let len = self.read_varint()?;
        if len == 0 {
            return Err(KafkaError::Protocol("null compact string".into()));
        }
        let len = (len - 1) as usize;
        if self.pos + len > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let s = String::from_utf8_lossy(&self.data[self.pos..self.pos + len]).into_owned();
        self.pos += len;
        Ok(s)
    }

    /// Read a nullable compact string.
    pub fn read_nullable_compact_string(&mut self) -> Result<Option<String>, KafkaError> {
        let len = self.read_varint()?;
        if len == 0 {
            return Ok(None);
        }
        let len = (len - 1) as usize;
        if self.pos + len > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let s = String::from_utf8_lossy(&self.data[self.pos..self.pos + len]).into_owned();
        self.pos += len;
        Ok(Some(s))
    }

    /// Read compact bytes.
    pub fn read_compact_bytes(&mut self) -> Result<Vec<u8>, KafkaError> {
        let len = self.read_varint()?;
        if len == 0 {
            return Err(KafkaError::Protocol("null compact bytes".into()));
        }
        let len = (len - 1) as usize;
        if self.pos + len > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let v = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(v)
    }

    /// Read nullable compact bytes.
    pub fn read_nullable_compact_bytes(&mut self) -> Result<Option<Vec<u8>>, KafkaError> {
        let len = self.read_varint()?;
        if len == 0 {
            return Ok(None);
        }
        let len = (len - 1) as usize;
        if self.pos + len > self.data.len() {
            return Err(KafkaError::Protocol("eof".into()));
        }
        let v = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(Some(v))
    }

    /// Read tagged fields (Kafka v2+ flexible version).
    pub fn read_tagged_fields(&mut self) -> Result<Vec<TaggedField>, KafkaError> {
        let count = self.read_varint()?;
        let mut fields = Vec::new();
        for _ in 0..count {
            let tag = self.read_varint()? as u32;
            let len = self.read_varint()? as usize;
            if self.pos + len > self.data.len() {
                return Err(KafkaError::Protocol("eof in tagged field".into()));
            }
            let data = self.data[self.pos..self.pos + len].to_vec();
            self.pos += len;
            fields.push(TaggedField { tag, data });
        }
        Ok(fields)
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn pos(&self) -> usize {
        self.pos
    }
}

/// A tagged field from Kafka's flexible request/response format.
#[derive(Clone, Debug, PartialEq)]
pub struct TaggedField {
    pub tag: u32,
    pub data: Vec<u8>,
}

// ─── Request/Response header helpers ───────────────────────────────────────

pub fn build_request_header(
    buf: &mut Vec<u8>,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &str,
) {
    write_i16(buf, api_key);
    write_i16(buf, api_version);
    write_i32(buf, correlation_id);
    write_string(buf, client_id);
}

/// Prepend the 4-byte message size to a request buffer.
pub fn frame_request(buf: Vec<u8>) -> Vec<u8> {
    let mut framed = Vec::with_capacity(buf.len() + 4);
    write_i32(&mut framed, buf.len() as i32);
    framed.extend_from_slice(&buf);
    framed
}

// ─── Metadata request/response ─────────────────────────────────────────────

/// Build a Metadata request (api_key=3, api_version=4).
pub fn build_metadata_request(correlation_id: i32, client_id: &str, topics: &[&str]) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 3, 4, correlation_id, client_id);
    // Topics array
    write_i32(&mut buf, topics.len() as i32);
    for t in topics {
        write_string(&mut buf, t);
    }
    write_i8(&mut buf, 0); // allow_auto_topic_creation = false
    frame_request(buf)
}

/// Parsed metadata from a Metadata response.
#[derive(Clone, Debug, PartialEq)]
pub struct TopicMetadata {
    pub name: String,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartitionMetadata {
    pub id: i32,
    pub leader: i32,
    pub leader_epoch: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopicMetadataList {
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
}

/// Parse a Metadata response.
pub fn parse_metadata_response(data: &[u8]) -> Result<TopicMetadataList, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let _throttle = r.read_i32()?;
    // Brokers array
    let broker_count = r.read_i32()?;
    let mut brokers = Vec::with_capacity(broker_count as usize);
    for _ in 0..broker_count {
        let node_id = r.read_i32()?;
        let host = r.read_string()?;
        let port = r.read_i32()?;
        let rack = r.read_nullable_string()?;
        brokers.push(BrokerMetadata {
            node_id,
            host,
            port,
            rack,
        });
    }
    // Cluster ID
    let cluster_id = r.read_nullable_string()?;
    // Controller ID
    let controller_id = r.read_i32()?;
    // Topics array
    let topic_count = r.read_i32()?;
    let mut topics = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let _error_code = r.read_i16()?;
        let name = r.read_string()?;
        let topic_id = r.read_uuid()?;
        let is_internal = r.read_i8()? != 0;
        let part_count = r.read_i32()?;
        let mut partitions = Vec::with_capacity(part_count as usize);
        for _ in 0..part_count {
            let _err = r.read_i16()?;
            let id = r.read_i32()?;
            let leader = r.read_i32()?;
            let leader_epoch = r.read_i32()?;
            let replica_count = r.read_i32()?;
            let mut replicas = Vec::with_capacity(replica_count as usize);
            for _ in 0..replica_count {
                replicas.push(r.read_i32()?);
            }
            let isr_count = r.read_i32()?;
            let mut isr = Vec::with_capacity(isr_count as usize);
            for _ in 0..isr_count {
                isr.push(r.read_i32()?);
            }
            let offline_count = r.read_i32()?;
            let mut offline_replicas = Vec::with_capacity(offline_count as usize);
            for _ in 0..offline_count {
                offline_replicas.push(r.read_i32()?);
            }
            partitions.push(PartitionMetadata {
                id,
                leader,
                leader_epoch,
                replicas,
                isr,
                offline_replicas,
            });
        }
        topics.push(TopicMetadata {
            name,
            topic_id,
            is_internal,
            partitions,
        });
    }
    Ok(TopicMetadataList {
        brokers,
        topics,
        cluster_id,
        controller_id,
    })
}

// ─── Fetch request/response ────────────────────────────────────────────────

/// Build a Fetch request (api_key=1, api_version=4).
pub fn build_fetch_request(
    correlation_id: i32,
    client_id: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    max_bytes: i32,
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 1, 4, correlation_id, client_id);
    write_i32(&mut buf, -1); // replica_id = -1 (consumer)
    write_i32(&mut buf, -1); // max_wait_ms
    write_i32(&mut buf, 0); // min_bytes
    write_i32(&mut buf, 1); // max_bytes (unused in v4, use per-partition)
    write_i8(&mut buf, 1); // isolation_level: read_committed
    // Session ID / Epoch
    write_i32(&mut buf, 0); // session_id
    write_i32(&mut buf, 0); // session_epoch
    // Topics array
    write_i32(&mut buf, 1);
    write_string(&mut buf, topic);
    write_i32(&mut buf, 1); // partitions
    write_i32(&mut buf, partition);
    write_i64(&mut buf, offset);
    write_i64(&mut buf, 0); // log_start_offset
    write_i32(&mut buf, max_bytes);
    // Forgotten topics (empty)
    write_i32(&mut buf, 0);
    frame_request(buf)
}

/// Parse a Fetch response. Returns (messages, new_high_watermark_offset).
pub fn parse_fetch_response(data: &[u8]) -> Result<(Vec<KafkaMessage>, i64), KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let _throttle = r.read_i32()?;
    let _error_code = r.read_i16()?;
    let _session_id = r.read_i32()?;
    let topic_count = r.read_i32()?;
    let mut messages = Vec::new();
    let mut max_offset: i64 = 0;

    for _ in 0..topic_count {
        let _topic = r.read_string()?;
        let part_count = r.read_i32()?;
        for _ in 0..part_count {
            let _partition = r.read_i32()?;
            let _error_code = r.read_i16()?;
            let high_watermark = r.read_i64()?;
            let _last_stable = r.read_i64()?;
            let _log_start = r.read_i64()?;
            let _aborted_count = r.read_i32()?;
            if _aborted_count > 0 {
                for _ in 0.._aborted_count {
                    let _ = r.read_i64()?;
                    let _ = r.read_i64()?;
                }
            }
            let _preferred = r.read_i32()?;
            let record_bytes = r.read_bytes()?;
            if !record_bytes.is_empty() {
                let batch_msgs = parse_record_batch(&record_bytes)?;
                for msg in batch_msgs {
                    if msg.offset > max_offset {
                        max_offset = msg.offset;
                    }
                    messages.push(msg);
                }
            }
            if high_watermark > max_offset {
                max_offset = high_watermark;
            }
        }
    }
    Ok((messages, max_offset + 1))
}

/// Parse a record batch (v2 magic).
fn parse_record_batch(data: &[u8]) -> Result<Vec<KafkaMessage>, KafkaError> {
    if data.len() < 61 {
        return Ok(Vec::new());
    }
    let mut r = Reader::new(data);
    let base_offset = r.read_i64()?;
    let _batch_len = r.read_i32()?;
    let _leader_epoch = r.read_i32()?;
    let _magic = r.read_i8()?;
    let _crc = r.read_i32()?;
    let _attrs = r.read_i16()?;
    let _last_offset_delta = r.read_i32()?;
    let _first_ts = r.read_i64()?;
    let _max_ts = r.read_i64()?;
    let _producer_id = r.read_i64()?;
    let _producer_epoch = r.read_i16()?;
    let _base_sequence = r.read_i32()?;
    let record_count = r.read_i32()? as usize;

    let mut messages = Vec::new();
    for _ in 0..record_count {
        let _len = r.read_varint()?;
        let _attrs = r.read_i8()?;
        let _timestamp_delta = r.read_varint()?;
        let offset_delta = r.read_varint()?;
        let key_bytes = r.read_varint_bytes()?;
        let value_bytes = r.read_varint_bytes()?;
        let headers_count = r.read_varint()?;
        if headers_count > 0 {
            for _ in 0..headers_count {
                let _ = r.read_varint_string()?;
                let _ = r.read_varint_bytes()?;
            }
        }
        messages.push(KafkaMessage {
            offset: base_offset + offset_delta as i64,
            key: if key_bytes.is_empty() {
                None
            } else {
                Some(key_bytes)
            },
            value: value_bytes,
        });
    }
    Ok(messages)
}

// ─── Produce request/response ──────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceRequestTopic {
    pub name: String,
    pub partitions: Vec<ProduceRequestPartition>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceRequestPartition {
    pub partition: i32,
    pub record_set: Vec<u8>,
}

pub fn build_produce_request(
    correlation_id: i32,
    client_id: &str,
    acks: i16,
    timeout_ms: i32,
    topics: &[ProduceRequestTopic],
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 0, 3, correlation_id, client_id);
    write_i16(&mut buf, acks);
    write_i32(&mut buf, timeout_ms);
    write_i32(&mut buf, topics.len() as i32);
    for topic in topics {
        write_string(&mut buf, &topic.name);
        write_i32(&mut buf, topic.partitions.len() as i32);
        for part in &topic.partitions {
            write_i32(&mut buf, part.partition);
            write_bytes(&mut buf, &part.record_set);
        }
    }
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceResponseTopic {
    pub name: String,
    pub partitions: Vec<ProduceResponsePartition>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProduceResponsePartition {
    pub partition: i32,
    pub error_code: i16,
    pub offset: i64,
    pub log_append_time: i64,
    pub log_start_offset: i64,
}

pub fn parse_produce_response(data: &[u8]) -> Result<Vec<ProduceResponseTopic>, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let _throttle = r.read_i32()?;
    let topic_count = r.read_i32()?;
    let mut topics = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let name = r.read_string()?;
        let part_count = r.read_i32()?;
        let mut partitions = Vec::with_capacity(part_count as usize);
        for _ in 0..part_count {
            let partition = r.read_i32()?;
            let error_code = r.read_i16()?;
            let offset = r.read_i64()?;
            let log_append_time = r.read_i64()?;
            let log_start_offset = r.read_i64()?;
            partitions.push(ProduceResponsePartition {
                partition,
                error_code,
                offset,
                log_append_time,
                log_start_offset,
            });
        }
        topics.push(ProduceResponseTopic { name, partitions });
    }
    Ok(topics)
}

// ─── ListOffsets request/response ──────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct ListOffsetsPartition {
    pub partition: i32,
    pub timestamp: i64, // -2 earliest, -1 latest
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListOffsetsTopic {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartition>,
}

pub fn build_list_offsets_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[ListOffsetsTopic],
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 2, 1, correlation_id, client_id);
    write_i32(&mut buf, -1); // replica_id
    write_i8(&mut buf, 1); // isolation_level: read_committed
    write_i32(&mut buf, topics.len() as i32);
    for topic in topics {
        write_string(&mut buf, &topic.name);
        write_i32(&mut buf, topic.partitions.len() as i32);
        for part in &topic.partitions {
            write_i32(&mut buf, part.partition);
            write_i64(&mut buf, part.timestamp);
        }
    }
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListOffsetsResponsePartition {
    pub partition: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
    pub leader_epoch: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListOffsetsResponseTopic {
    pub name: String,
    pub partitions: Vec<ListOffsetsResponsePartition>,
}

pub fn parse_list_offsets_response(
    data: &[u8],
) -> Result<Vec<ListOffsetsResponseTopic>, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let _throttle = r.read_i32()?;
    let topic_count = r.read_i32()?;
    let mut topics = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let name = r.read_string()?;
        let part_count = r.read_i32()?;
        let mut partitions = Vec::with_capacity(part_count as usize);
        for _ in 0..part_count {
            let partition = r.read_i32()?;
            let error_code = r.read_i16()?;
            let timestamp = r.read_i64()?;
            let offset = r.read_i64()?;
            let leader_epoch = r.read_i32()?;
            partitions.push(ListOffsetsResponsePartition {
                partition,
                error_code,
                timestamp,
                offset,
                leader_epoch,
            });
        }
        topics.push(ListOffsetsResponseTopic { name, partitions });
    }
    Ok(topics)
}

// ─── OffsetFetch request/response ──────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetFetchTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

pub fn build_offset_fetch_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    topics: &[OffsetFetchTopic],
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 9, 3, correlation_id, client_id);
    write_string(&mut buf, group_id);
    write_i32(&mut buf, topics.len() as i32);
    for topic in topics {
        write_string(&mut buf, &topic.name);
        write_i32(&mut buf, topic.partitions.len() as i32);
        for p in &topic.partitions {
            write_i32(&mut buf, *p);
        }
    }
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetFetchResponsePartition {
    pub partition: i32,
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
    pub error_code: i16,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetFetchResponseTopic {
    pub name: String,
    pub partitions: Vec<OffsetFetchResponsePartition>,
}

pub fn parse_offset_fetch_response(
    data: &[u8],
) -> Result<(Vec<OffsetFetchResponseTopic>, i32), KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let _throttle = r.read_i32()?;
    let topic_count = r.read_i32()?;
    let mut topics = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let name = r.read_string()?;
        let part_count = r.read_i32()?;
        let mut partitions = Vec::with_capacity(part_count as usize);
        for _ in 0..part_count {
            let partition = r.read_i32()?;
            let offset = r.read_i64()?;
            let leader_epoch = r.read_i32()?;
            let metadata = r.read_nullable_string()?;
            let error_code = r.read_i16()?;
            partitions.push(OffsetFetchResponsePartition {
                partition,
                offset,
                leader_epoch,
                metadata,
                error_code,
            });
        }
        topics.push(OffsetFetchResponseTopic { name, partitions });
    }
    let error_code = r.read_i16()? as i32;
    Ok((topics, error_code))
}

// ─── JoinGroup request/response ────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct JoinGroupProtocol {
    pub name: String,
    pub metadata: Vec<u8>,
}

pub fn build_join_group_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    member_id: &str,
    protocol_type: &str,
    protocols: &[JoinGroupProtocol],
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 11, 1, correlation_id, client_id);
    write_string(&mut buf, group_id);
    write_i32(&mut buf, session_timeout_ms);
    write_i32(&mut buf, rebalance_timeout_ms);
    write_string(&mut buf, member_id);
    write_string(&mut buf, protocol_type);
    write_i32(&mut buf, protocols.len() as i32);
    for proto in protocols {
        write_string(&mut buf, &proto.name);
        write_bytes(&mut buf, &proto.metadata);
    }
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinGroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinGroupResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinGroupMember>,
}

pub fn parse_join_group_response(data: &[u8]) -> Result<JoinGroupResponse, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let throttle_time_ms = r.read_i32()?;
    let error_code = r.read_i16()?;
    let generation_id = r.read_i32()?;
    let protocol_name = r.read_nullable_string()?;
    let leader = r.read_string()?;
    let member_id = r.read_string()?;
    let member_count = r.read_i32()?;
    let mut members = Vec::with_capacity(member_count as usize);
    for _ in 0..member_count {
        let member_id = r.read_string()?;
        let group_instance_id = r.read_nullable_string()?;
        let metadata = r.read_bytes()?;
        members.push(JoinGroupMember {
            member_id,
            group_instance_id,
            metadata,
        });
    }
    Ok(JoinGroupResponse {
        throttle_time_ms,
        error_code,
        generation_id,
        protocol_name,
        leader,
        member_id,
        members,
    })
}

// ─── SyncGroup request/response ────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct SyncGroupAssignment {
    pub member_id: String,
    pub assignment: Vec<u8>,
}

pub fn build_sync_group_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    protocol_type: Option<&str>,
    protocol_name: Option<&str>,
    assignments: &[SyncGroupAssignment],
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 14, 1, correlation_id, client_id);
    write_string(&mut buf, group_id);
    write_i32(&mut buf, generation_id);
    write_string(&mut buf, member_id);
    write_nullable_string(&mut buf, protocol_type);
    write_nullable_string(&mut buf, protocol_name);
    write_i32(&mut buf, assignments.len() as i32);
    for a in assignments {
        write_string(&mut buf, &a.member_id);
        write_bytes(&mut buf, &a.assignment);
    }
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct SyncGroupResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub assignment: Vec<u8>,
}

pub fn parse_sync_group_response(data: &[u8]) -> Result<SyncGroupResponse, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let throttle_time_ms = r.read_i32()?;
    let error_code = r.read_i16()?;
    let protocol_type = r.read_nullable_string()?;
    let protocol_name = r.read_nullable_string()?;
    let assignment = r.read_bytes()?;
    Ok(SyncGroupResponse {
        throttle_time_ms,
        error_code,
        protocol_type,
        protocol_name,
        assignment,
    })
}

// ─── Heartbeat request/response ────────────────────────────────────────────

pub fn build_heartbeat_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 12, 1, correlation_id, client_id);
    write_string(&mut buf, group_id);
    write_i32(&mut buf, generation_id);
    write_string(&mut buf, member_id);
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct HeartbeatResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

pub fn parse_heartbeat_response(data: &[u8]) -> Result<HeartbeatResponse, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let throttle_time_ms = r.read_i32()?;
    let error_code = r.read_i16()?;
    Ok(HeartbeatResponse {
        throttle_time_ms,
        error_code,
    })
}

// ─── LeaveGroup request/response ───────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct LeaveGroupMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

pub fn build_leave_group_request(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    members: &[LeaveGroupMember],
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 13, 1, correlation_id, client_id);
    write_string(&mut buf, group_id);
    write_i32(&mut buf, members.len() as i32);
    for m in members {
        write_string(&mut buf, &m.member_id);
        write_nullable_string(&mut buf, m.group_instance_id.as_deref());
    }
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeaveGroupResponseMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub error_code: i16,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeaveGroupResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub members: Vec<LeaveGroupResponseMember>,
}

pub fn parse_leave_group_response(data: &[u8]) -> Result<LeaveGroupResponse, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let throttle_time_ms = r.read_i32()?;
    let error_code = r.read_i16()?;
    let member_count = r.read_i32()?;
    let mut members = Vec::with_capacity(member_count as usize);
    for _ in 0..member_count {
        let member_id = r.read_string()?;
        let group_instance_id = r.read_nullable_string()?;
        let err = r.read_i16()?;
        members.push(LeaveGroupResponseMember {
            member_id,
            group_instance_id,
            error_code: err,
        });
    }
    Ok(LeaveGroupResponse {
        throttle_time_ms,
        error_code,
        members,
    })
}

// ─── ApiVersions request/response ──────────────────────────────────────────

pub fn build_api_versions_request(correlation_id: i32, client_id: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 18, 0, correlation_id, client_id);
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_versions: Vec<ApiVersion>,
    pub throttle_time_ms: i32,
}

pub fn parse_api_versions_response(data: &[u8]) -> Result<ApiVersionsResponse, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let error_code = r.read_i16()?;
    let count = r.read_i32()?;
    let mut api_versions = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let api_key = r.read_i16()?;
        let min_version = r.read_i16()?;
        let max_version = r.read_i16()?;
        api_versions.push(ApiVersion {
            api_key,
            min_version,
            max_version,
        });
    }
    let throttle_time_ms = r.read_i32()?;
    Ok(ApiVersionsResponse {
        error_code,
        api_versions,
        throttle_time_ms,
    })
}

// ─── Topic management: CreateTopics ────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct CreateTopicConfig {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateTopicRequest {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: Vec<CreateTopicConfig>,
}

pub fn build_create_topics_request(
    correlation_id: i32,
    client_id: &str,
    topics: &[CreateTopicRequest],
    timeout_ms: i32,
    validate_only: bool,
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 19, 2, correlation_id, client_id);
    write_i32(&mut buf, topics.len() as i32);
    for topic in topics {
        write_string(&mut buf, &topic.name);
        write_i32(&mut buf, topic.num_partitions);
        write_i16(&mut buf, topic.replication_factor);
        // Replicas assignment (empty = default)
        write_i32(&mut buf, 0);
        // Config entries
        write_i32(&mut buf, topic.configs.len() as i32);
        for cfg in &topic.configs {
            write_string(&mut buf, &cfg.name);
            write_string(&mut buf, &cfg.value);
        }
    }
    write_i32(&mut buf, timeout_ms);
    write_i8(&mut buf, if validate_only { 1 } else { 0 });
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateTopicResponse {
    pub name: String,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub topic_id: [u8; 16],
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub configs: Vec<CreateTopicConfig>,
}

pub fn parse_create_topics_response(
    data: &[u8],
) -> Result<Vec<CreateTopicResponse>, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let _throttle = r.read_i32()?;
    let topic_count = r.read_i32()?;
    let mut topics = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let name = r.read_string()?;
        let error_code = r.read_i16()?;
        let error_message = r.read_nullable_string()?;
        let topic_id = r.read_uuid()?;
        let num_partitions = r.read_i32()?;
        let replication_factor = r.read_i16()?;
        let config_count = r.read_i32()?;
        let mut configs = Vec::with_capacity(config_count as usize);
        for _ in 0..config_count {
            let name = r.read_string()?;
            let value = r.read_string()?;
            let _read_only = r.read_i8()?;
            let _config_source = r.read_i8()?;
            let _is_sensitive = r.read_i8()?;
            configs.push(CreateTopicConfig { name, value });
        }
        topics.push(CreateTopicResponse {
            name,
            error_code,
            error_message,
            topic_id,
            num_partitions,
            replication_factor,
            configs,
        });
    }
    Ok(topics)
}

// ─── Topic management: DeleteTopics ────────────────────────────────────────

pub fn build_delete_topics_request(
    correlation_id: i32,
    client_id: &str,
    topic_names: &[&str],
    timeout_ms: i32,
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 20, 1, correlation_id, client_id);
    write_i32(&mut buf, topic_names.len() as i32);
    for name in topic_names {
        write_string(&mut buf, name);
    }
    write_i32(&mut buf, timeout_ms);
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeleteTopicResponse {
    pub name: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

pub fn parse_delete_topics_response(
    data: &[u8],
) -> Result<Vec<DeleteTopicResponse>, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let _throttle = r.read_i32()?;
    let topic_count = r.read_i32()?;
    let mut topics = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let name = r.read_string()?;
        let error_code = r.read_i16()?;
        let error_message = r.read_nullable_string()?;
        topics.push(DeleteTopicResponse {
            name,
            error_code,
            error_message,
        });
    }
    Ok(topics)
}

// ─── Topic management: DescribeTopics ──────────────────────────────────────

pub fn build_describe_topics_request(
    correlation_id: i32,
    client_id: &str,
    topic_names: &[&str],
) -> Vec<u8> {
    let mut buf = Vec::new();
    build_request_header(&mut buf, 75, 0, correlation_id, client_id);
    write_i32(&mut buf, topic_names.len() as i32);
    for name in topic_names {
        write_string(&mut buf, name);
    }
    frame_request(buf)
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeTopicPartition {
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub eligible_leader_replicas: Vec<i32>,
    pub last_known_elr: Vec<i32>,
    pub offline_replicas: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DescribeTopicResponse {
    pub name: String,
    pub topic_id: [u8; 16],
    pub is_internal: bool,
    pub partitions: Vec<DescribeTopicPartition>,
    pub error_code: i16,
    pub error_message: Option<String>,
}

pub fn parse_describe_topics_response(
    data: &[u8],
) -> Result<Vec<DescribeTopicResponse>, KafkaError> {
    let mut r = Reader::new(data);
    let _correlation_id = r.read_i32()?;
    let _throttle = r.read_i32()?;
    let topic_count = r.read_i32()?;
    let mut topics = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let error_code = r.read_i16()?;
        let error_message = r.read_nullable_string()?;
        let name = r.read_string()?;
        let topic_id = r.read_uuid()?;
        let is_internal = r.read_i8()? != 0;
        let part_count = r.read_i32()?;
        let mut partitions = Vec::with_capacity(part_count as usize);
        for _ in 0..part_count {
            let partition_index = r.read_i32()?;
            let leader_id = r.read_i32()?;
            let leader_epoch = r.read_i32()?;
            let replica_count = r.read_i32()?;
            let mut replica_nodes = Vec::with_capacity(replica_count as usize);
            for _ in 0..replica_count {
                replica_nodes.push(r.read_i32()?);
            }
            let isr_count = r.read_i32()?;
            let mut isr_nodes = Vec::with_capacity(isr_count as usize);
            for _ in 0..isr_count {
                isr_nodes.push(r.read_i32()?);
            }
            let elr_count = r.read_i32()?;
            let mut eligible_leader_replicas = Vec::with_capacity(elr_count as usize);
            for _ in 0..elr_count {
                eligible_leader_replicas.push(r.read_i32()?);
            }
            let last_known_count = r.read_i32()?;
            let mut last_known_elr = Vec::with_capacity(last_known_count as usize);
            for _ in 0..last_known_count {
                last_known_elr.push(r.read_i32()?);
            }
            let offline_count = r.read_i32()?;
            let mut offline_replicas = Vec::with_capacity(offline_count as usize);
            for _ in 0..offline_count {
                offline_replicas.push(r.read_i32()?);
            }
            partitions.push(DescribeTopicPartition {
                partition_index,
                leader_id,
                leader_epoch,
                replica_nodes,
                isr_nodes,
                eligible_leader_replicas,
                last_known_elr,
                offline_replicas,
            });
        }
        topics.push(DescribeTopicResponse {
            name,
            topic_id,
            is_internal,
            partitions,
            error_code,
            error_message,
        });
    }
    Ok(topics)
}

// ─── Wire protocol: read full response ─────────────────────────────────────

/// Read a full response from a TCP stream.
pub fn read_response(stream: &mut impl Read) -> Result<Vec<u8>, KafkaError> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).map_err(KafkaError::Io)?;
    let size = i32::from_be_bytes(size_buf) as usize;
    let mut data = vec![0u8; size];
    stream.read_exact(&mut data).map_err(KafkaError::Io)?;
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_metadata_request() {
        let req = build_metadata_request(1, "test", &["my-topic"]);
        assert!(req.len() > 4);
        let size = i32::from_be_bytes([req[0], req[1], req[2], req[3]]) as usize;
        assert_eq!(size + 4, req.len());
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 3);
    }

    #[test]
    fn test_build_fetch_request() {
        let req = build_fetch_request(2, "test", "t", 0, 0, 1024);
        assert!(req.len() > 4);
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 1);
    }

    #[test]
    fn test_reader_i32() {
        let mut r = Reader::new(&[0, 0, 0, 42]);
        assert_eq!(r.read_i32().unwrap(), 42);
    }

    #[test]
    fn test_reader_string() {
        let mut buf = vec![0, 5];
        buf.extend_from_slice(b"hello");
        let mut r = Reader::new(&buf);
        assert_eq!(r.read_string().unwrap(), "hello");
    }

    #[test]
    fn test_varint_roundtrip() {
        let mut buf = Vec::new();
        let values = [0u64, 1, 127, 128, 16383, 16384, u64::MAX];
        for &v in &values {
            buf.clear();
            write_varint(&mut buf, v);
            let mut r = Reader::new(&buf);
            let decoded = r.read_varint().unwrap();
            assert_eq!(decoded, v, "varint mismatch for {}", v);
        }
    }

    #[test]
    fn test_signed_varint_roundtrip() {
        let mut buf = Vec::new();
        let values = [0i64, -1, 1, -128, 127, i64::MIN, i64::MAX];
        for &v in &values {
            buf.clear();
            write_signed_varint(&mut buf, v);
            let mut r = Reader::new(&buf);
            let decoded = r.read_signed_varint().unwrap();
            assert_eq!(decoded, v, "signed varint mismatch for {}", v);
        }
    }

    #[test]
    fn test_uuid_roundtrip() {
        let uuid = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F, 0x10,
        ];
        let mut buf = Vec::new();
        write_uuid(&mut buf, uuid);
        let mut r = Reader::new(&buf);
        assert_eq!(r.read_uuid().unwrap(), uuid);
    }

    #[test]
    fn test_tagged_fields_roundtrip() {
        let mut buf = Vec::new();
        write_varint(&mut buf, 2); // count
        write_varint(&mut buf, 1); // tag 1
        write_varint(&mut buf, 3); // len 3
        buf.extend_from_slice(b"abc");
        write_varint(&mut buf, 5); // tag 5
        write_varint(&mut buf, 2); // len 2
        buf.extend_from_slice(b"xy");

        let mut r = Reader::new(&buf);
        let fields = r.read_tagged_fields().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].tag, 1);
        assert_eq!(&fields[0].data, b"abc");
        assert_eq!(fields[1].tag, 5);
        assert_eq!(&fields[1].data, b"xy");
    }

    #[test]
    fn test_produce_request_response() {
        let topic = ProduceRequestTopic {
            name: "test-topic".to_string(),
            partitions: vec![ProduceRequestPartition {
                partition: 0,
                record_set: vec![0xAB, 0xCD],
            }],
        };
        let req = build_produce_request(7, "client", 1, 5000, &[topic]);
        assert!(req.len() > 4);
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 0); // api_key = Produce

        // Build a synthetic response
        let mut resp = Vec::new();
        write_i32(&mut resp, 7); // correlation_id
        write_i32(&mut resp, 0); // throttle
        write_i32(&mut resp, 1); // 1 topic
        write_string(&mut resp, "test-topic");
        write_i32(&mut resp, 1); // 1 partition
        write_i32(&mut resp, 0); // partition 0
        write_i16(&mut resp, 0); // no error
        write_i64(&mut resp, 42); // offset
        write_i64(&mut resp, -1); // log_append_time
        write_i64(&mut resp, 0); // log_start_offset

        let parsed = parse_produce_response(&resp).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].name, "test-topic");
        assert_eq!(parsed[0].partitions[0].offset, 42);
    }

    #[test]
    fn test_list_offsets_request_response() {
        let topic = ListOffsetsTopic {
            name: "t".to_string(),
            partitions: vec![ListOffsetsPartition {
                partition: 0,
                timestamp: -1, // latest
            }],
        };
        let req = build_list_offsets_request(3, "c", &[topic]);
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 2); // api_key = ListOffsets

        let mut resp = Vec::new();
        write_i32(&mut resp, 3);
        write_i32(&mut resp, 0);
        write_i32(&mut resp, 1);
        write_string(&mut resp, "t");
        write_i32(&mut resp, 1);
        write_i32(&mut resp, 0);
        write_i16(&mut resp, 0);
        write_i64(&mut resp, -1);
        write_i64(&mut resp, 100);
        write_i32(&mut resp, 0);

        let parsed = parse_list_offsets_response(&resp).unwrap();
        assert_eq!(parsed[0].partitions[0].offset, 100);
    }

    #[test]
    fn test_offset_fetch_request_response() {
        let topic = OffsetFetchTopic {
            name: "t".to_string(),
            partitions: vec![0, 1],
        };
        let req = build_offset_fetch_request(5, "c", "my-group", &[topic]);
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 9); // api_key = OffsetFetch

        let mut resp = Vec::new();
        write_i32(&mut resp, 5);
        write_i32(&mut resp, 0);
        write_i32(&mut resp, 1);
        write_string(&mut resp, "t");
        write_i32(&mut resp, 2);
        write_i32(&mut resp, 0);
        write_i64(&mut resp, 50);
        write_i32(&mut resp, 0);
        write_nullable_string(&mut resp, Some("meta"));
        write_i16(&mut resp, 0);
        write_i32(&mut resp, 1);
        write_i64(&mut resp, 60);
        write_i32(&mut resp, 0);
        write_nullable_string(&mut resp, None);
        write_i16(&mut resp, 0);
        write_i16(&mut resp, 0);

        let (topics, err) = parse_offset_fetch_response(&resp).unwrap();
        assert_eq!(err, 0);
        assert_eq!(topics[0].partitions.len(), 2);
        assert_eq!(topics[0].partitions[0].offset, 50);
        assert_eq!(topics[0].partitions[1].offset, 60);
    }

    #[test]
    fn test_join_group_request_response() {
        let proto = JoinGroupProtocol {
            name: "range".to_string(),
            metadata: vec![1, 2, 3],
        };
        let req = build_join_group_request(10, "c", "g1", 30000, 10000, "", "consumer", &[proto]);
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 11); // api_key = JoinGroup

        let mut resp = Vec::new();
        write_i32(&mut resp, 10);
        write_i32(&mut resp, 0);
        write_i16(&mut resp, 0);
        write_i32(&mut resp, 1); // generation_id
        write_nullable_string(&mut resp, Some("range"));
        write_string(&mut resp, "leader-1");
        write_string(&mut resp, "member-1");
        write_i32(&mut resp, 1);
        write_string(&mut resp, "member-1");
        write_nullable_string(&mut resp, None);
        write_bytes(&mut resp, &[4, 5, 6]);

        let parsed = parse_join_group_response(&resp).unwrap();
        assert_eq!(parsed.generation_id, 1);
        assert_eq!(parsed.leader, "leader-1");
        assert_eq!(parsed.members.len(), 1);
    }

    #[test]
    fn test_sync_group_request_response() {
        let assignment = SyncGroupAssignment {
            member_id: "m1".to_string(),
            assignment: vec![7, 8],
        };
        let req =
            build_sync_group_request(11, "c", "g1", 1, "m1", Some("consumer"), Some("range"), &[assignment]);
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 14); // api_key = SyncGroup

        let mut resp = Vec::new();
        write_i32(&mut resp, 11);
        write_i32(&mut resp, 0);
        write_i16(&mut resp, 0);
        write_nullable_string(&mut resp, Some("consumer"));
        write_nullable_string(&mut resp, Some("range"));
        write_bytes(&mut resp, &[9, 10]);

        let parsed = parse_sync_group_response(&resp).unwrap();
        assert_eq!(parsed.assignment, vec![9, 10]);
        assert_eq!(parsed.protocol_name, Some("range".to_string()));
    }

    #[test]
    fn test_heartbeat_request_response() {
        let req = build_heartbeat_request(12, "c", "g1", 1, "m1");
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 12); // api_key = Heartbeat

        let mut resp = Vec::new();
        write_i32(&mut resp, 12);
        write_i32(&mut resp, 0);
        write_i16(&mut resp, 0);

        let parsed = parse_heartbeat_response(&resp).unwrap();
        assert_eq!(parsed.error_code, 0);
    }

    #[test]
    fn test_leave_group_request_response() {
        let member = LeaveGroupMember {
            member_id: "m1".to_string(),
            group_instance_id: Some("inst-1".to_string()),
        };
        let req = build_leave_group_request(13, "c", "g1", &[member]);
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 13); // api_key = LeaveGroup

        let mut resp = Vec::new();
        write_i32(&mut resp, 13);
        write_i32(&mut resp, 0);
        write_i16(&mut resp, 0);
        write_i32(&mut resp, 1);
        write_string(&mut resp, "m1");
        write_nullable_string(&mut resp, Some("inst-1"));
        write_i16(&mut resp, 0);

        let parsed = parse_leave_group_response(&resp).unwrap();
        assert_eq!(parsed.members.len(), 1);
        assert_eq!(parsed.members[0].member_id, "m1");
    }

    #[test]
    fn test_api_versions_request_response() {
        let req = build_api_versions_request(15, "c");
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 18); // api_key = ApiVersions

        let mut resp = Vec::new();
        write_i32(&mut resp, 15);
        write_i16(&mut resp, 0); // error_code
        write_i32(&mut resp, 2); // 2 api versions
        write_i16(&mut resp, 0); // Produce
        write_i16(&mut resp, 0); // min
        write_i16(&mut resp, 9); // max
        write_i16(&mut resp, 1); // Fetch
        write_i16(&mut resp, 0); // min
        write_i16(&mut resp, 13); // max
        write_i32(&mut resp, 0); // throttle

        let parsed = parse_api_versions_response(&resp).unwrap();
        assert_eq!(parsed.api_versions.len(), 2);
        assert_eq!(parsed.api_versions[0].api_key, 0);
        assert_eq!(parsed.api_versions[1].api_key, 1);
    }

    #[test]
    fn test_create_topics_request_response() {
        let topic = CreateTopicRequest {
            name: "new-topic".to_string(),
            num_partitions: 3,
            replication_factor: 1,
            configs: vec![CreateTopicConfig {
                name: "retention.ms".to_string(),
                value: "86400000".to_string(),
            }],
        };
        let req = build_create_topics_request(20, "c", &[topic], 5000, false);
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 19); // api_key = CreateTopics

        let mut resp = Vec::new();
        write_i32(&mut resp, 20);
        write_i32(&mut resp, 0);
        write_i32(&mut resp, 1);
        write_string(&mut resp, "new-topic");
        write_i16(&mut resp, 0); // no error
        write_nullable_string(&mut resp, None);
        write_uuid(&mut resp, [0; 16]);
        write_i32(&mut resp, 3);
        write_i16(&mut resp, 1);
        write_i32(&mut resp, 1); // 1 config
        write_string(&mut resp, "retention.ms");
        write_string(&mut resp, "86400000");
        write_i8(&mut resp, 0);
        write_i8(&mut resp, 0);
        write_i8(&mut resp, 0);

        let parsed = parse_create_topics_response(&resp).unwrap();
        assert_eq!(parsed[0].name, "new-topic");
        assert_eq!(parsed[0].num_partitions, 3);
    }

    #[test]
    fn test_delete_topics_request_response() {
        let req = build_delete_topics_request(21, "c", &["old-topic"], 5000);
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 20); // api_key = DeleteTopics

        let mut resp = Vec::new();
        write_i32(&mut resp, 21);
        write_i32(&mut resp, 0);
        write_i32(&mut resp, 1);
        write_string(&mut resp, "old-topic");
        write_i16(&mut resp, 0);
        write_nullable_string(&mut resp, None);

        let parsed = parse_delete_topics_response(&resp).unwrap();
        assert_eq!(parsed[0].name, "old-topic");
        assert_eq!(parsed[0].error_code, 0);
    }

    #[test]
    fn test_describe_topics_request_response() {
        let req = build_describe_topics_request(22, "c", &["my-topic"]);
        assert_eq!(i16::from_be_bytes([req[4], req[5]]), 75); // api_key = DescribeTopics

        let mut resp = Vec::new();
        write_i32(&mut resp, 22);
        write_i32(&mut resp, 0);
        write_i32(&mut resp, 1);
        write_i16(&mut resp, 0);
        write_nullable_string(&mut resp, None);
        write_string(&mut resp, "my-topic");
        write_uuid(&mut resp, [0xAA; 16]);
        write_i8(&mut resp, 0);
        write_i32(&mut resp, 1);
        write_i32(&mut resp, 0); // partition_index
        write_i32(&mut resp, 1); // leader_id
        write_i32(&mut resp, 0); // leader_epoch
        write_i32(&mut resp, 0); // replicas
        write_i32(&mut resp, 0); // isr
        write_i32(&mut resp, 0); // elr
        write_i32(&mut resp, 0); // last_known_elr
        write_i32(&mut resp, 0); // offline

        let parsed = parse_describe_topics_response(&resp).unwrap();
        assert_eq!(parsed[0].name, "my-topic");
        assert_eq!(parsed[0].partitions[0].leader_id, 1);
    }

    #[test]
    fn test_compact_string_and_bytes() {
        // Compact string: varint(len+1) + data
        let mut buf = Vec::new();
        write_varint(&mut buf, 6); // len = 5+1
        buf.extend_from_slice(b"hello");
        let mut r = Reader::new(&buf);
        assert_eq!(r.read_compact_string().unwrap(), "hello");

        // Compact nullable string (null)
        let mut buf2 = Vec::new();
        write_varint(&mut buf2, 0); // null
        let mut r2 = Reader::new(&buf2);
        assert_eq!(r2.read_nullable_compact_string().unwrap(), None);

        // Compact bytes
        let mut buf3 = Vec::new();
        write_varint(&mut buf3, 4); // len = 3+1
        buf3.extend_from_slice(&[1, 2, 3]);
        let mut r3 = Reader::new(&buf3);
        assert_eq!(r3.read_compact_bytes().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_metadata_response_with_brokers() {
        let mut resp = Vec::new();
        write_i32(&mut resp, 1); // correlation_id
        write_i32(&mut resp, 0); // throttle
        write_i32(&mut resp, 1); // 1 broker
        write_i32(&mut resp, 1); // node_id
        write_string(&mut resp, "localhost");
        write_i32(&mut resp, 9092);
        write_nullable_string(&mut resp, Some("rack-1"));
        write_nullable_string(&mut resp, Some("cluster-1"));
        write_i32(&mut resp, 1); // controller_id
        write_i32(&mut resp, 1); // 1 topic
        write_i16(&mut resp, 0); // error
        write_string(&mut resp, "test");
        write_uuid(&mut resp, [0; 16]);
        write_i8(&mut resp, 0); // not internal
        write_i32(&mut resp, 1); // 1 partition
        write_i16(&mut resp, 0); // error
        write_i32(&mut resp, 0); // id
        write_i32(&mut resp, 1); // leader
        write_i32(&mut resp, 0); // leader_epoch
        write_i32(&mut resp, 1); // 1 replica
        write_i32(&mut resp, 1);
        write_i32(&mut resp, 1); // 1 isr
        write_i32(&mut resp, 1);
        write_i32(&mut resp, 0); // offline

        let parsed = parse_metadata_response(&resp).unwrap();
        assert_eq!(parsed.brokers.len(), 1);
        assert_eq!(parsed.brokers[0].host, "localhost");
        assert_eq!(parsed.brokers[0].port, 9092);
        assert_eq!(parsed.brokers[0].rack, Some("rack-1".to_string()));
        assert_eq!(parsed.cluster_id, Some("cluster-1".to_string()));
        assert_eq!(parsed.controller_id, 1);
        assert_eq!(parsed.topics[0].partitions[0].replicas, vec![1]);
        assert_eq!(parsed.topics[0].partitions[0].isr, vec![1]);
    }
}
