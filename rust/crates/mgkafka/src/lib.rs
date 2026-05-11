//! # mgkafka — Pure Rust Kafka consumer/producer (zero external dependencies)
//!
//! Implements the Kafka wire protocol over raw TCP using only std::net.
//! Replaces C++ librdkafka integration. No C/C++ in dependency tree.

mod coordinator;
mod partition;
pub mod protocol;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use mgstorage::storage::Storage;
use protocol::*;

pub use coordinator::{ConsumerGroup, GroupCoordinator, MemberState, OffsetStore, CommittedOffset};
pub use partition::{
    assign_range, assign_round_robin, elect_leader, is_replica_for, preferred_replica,
    LeaderElectionResult, Member, Partition,
};

// ─── Kafka consumer ────────────────────────────────────────────────────────

pub struct KafkaConsumer {
    broker: String,
    topic: String,
    partition: i32,
    current_offset: i64,
}

impl KafkaConsumer {
    pub fn new(broker: &str, topic: &str) -> Self {
        Self {
            broker: broker.to_string(),
            topic: topic.to_string(),
            partition: 0,
            current_offset: 0,
        }
    }

    /// Connect to broker and discover the topic's partition leader.
    pub fn connect(&mut self) -> Result<(), KafkaError> {
        let mut stream = TcpStream::connect(&self.broker)
            .map_err(|e| KafkaError::Io(e))?;

        // Send Metadata request
        let correlation_id = 1;
        let client_id = "mgkafka";
        let request = build_metadata_request(correlation_id, client_id, &[&self.topic]);
        stream.write_all(&request).map_err(|e| KafkaError::Io(e))?;

        // Read response
        let response = read_response(&mut stream)?;
        let metadata = parse_metadata_response(&response)?;

        // Find partition leader
        for topic in &metadata.topics {
            if topic.name == self.topic {
                for partition in &topic.partitions {
                    if partition.id == self.partition {
                        self.current_offset = 0; // Start from beginning
                        return Ok(());
                    }
                }
                return Err(KafkaError::NoPartition(self.topic.clone()));
            }
        }
        Err(KafkaError::TopicNotFound(self.topic.clone()))
    }

    /// Fetch a batch of messages. Returns empty vec if no messages available.
    pub fn fetch(&mut self, max_bytes: i32) -> Result<Vec<KafkaMessage>, KafkaError> {
        let mut stream = TcpStream::connect(&self.broker)
            .map_err(|e| KafkaError::Io(e))?;

        let correlation_id = 2;
        let request = build_fetch_request(
            correlation_id, "mgkafka",
            &self.topic, self.partition,
            self.current_offset, max_bytes,
        );
        stream.write_all(&request).map_err(|e| KafkaError::Io(e))?;

        let response = read_response(&mut stream)?;
        let (messages, new_offset) = parse_fetch_response(&response)?;

        if new_offset > self.current_offset {
            self.current_offset = new_offset;
        }

        Ok(messages)
    }
}

// ─── Kafka producer ────────────────────────────────────────────────────────

/// A simple Kafka producer that sends record batches to a broker.
pub struct KafkaProducer {
    broker: String,
    client_id: String,
    correlation_id: i32,
}

/// A record to be produced.
#[derive(Clone, Debug, PartialEq)]
pub struct ProducerRecord {
    pub topic: String,
    pub partition: i32,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl KafkaProducer {
    pub fn new(broker: &str) -> Self {
        Self {
            broker: broker.to_string(),
            client_id: "mgkafka-producer".to_string(),
            correlation_id: 1,
        }
    }

    fn next_correlation_id(&mut self) -> i32 {
        let id = self.correlation_id;
        self.correlation_id = self.correlation_id.wrapping_add(1);
        id
    }

    /// Send a batch of records to Kafka. Returns per-topic-partition responses.
    pub fn send_batch(&mut self, records: &[ProducerRecord]) -> Result<Vec<ProduceResponseTopic>, KafkaError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        // Group records by (topic, partition)
        let mut groups: std::collections::HashMap<(String, i32), Vec<&ProducerRecord>> = std::collections::HashMap::new();
        for rec in records {
            groups.entry((rec.topic.clone(), rec.partition)).or_default().push(rec);
        }

        let mut topics = Vec::new();
        for ((topic, partition), recs) in groups {
            let record_set = build_record_batch(&recs);
            topics.push(ProduceRequestTopic {
                name: topic,
                partitions: vec![ProduceRequestPartition {
                    partition,
                    record_set,
                }],
            });
        }

        let mut stream = TcpStream::connect(&self.broker)
            .map_err(|e| KafkaError::Io(e))?;

        let request = build_produce_request(
            self.next_correlation_id(),
            &self.client_id,
            1, // acks = leader only
            5000,
            &topics,
        );
        stream.write_all(&request).map_err(|e| KafkaError::Io(e))?;

        let response = read_response(&mut stream)?;
        parse_produce_response(&response)
    }

    /// Send a single record.
    pub fn send(&mut self, record: &ProducerRecord) -> Result<Vec<ProduceResponseTopic>, KafkaError> {
        self.send_batch(&[record.clone()])
    }
}

// ─── Admin client ──────────────────────────────────────────────────────────

/// Simple Kafka admin client for topic management.
pub struct KafkaAdmin {
    broker: String,
    client_id: String,
    correlation_id: i32,
}

impl KafkaAdmin {
    pub fn new(broker: &str) -> Self {
        Self {
            broker: broker.to_string(),
            client_id: "mgkafka-admin".to_string(),
            correlation_id: 100,
        }
    }

    fn next_correlation_id(&mut self) -> i32 {
        let id = self.correlation_id;
        self.correlation_id = self.correlation_id.wrapping_add(1);
        id
    }

    /// Create topics on the broker.
    pub fn create_topics(
        &mut self,
        topics: &[CreateTopicRequest],
        timeout_ms: i32,
    ) -> Result<Vec<CreateTopicResponse>, KafkaError> {
        let mut stream = TcpStream::connect(&self.broker)
            .map_err(|e| KafkaError::Io(e))?;
        let request = build_create_topics_request(
            self.next_correlation_id(),
            &self.client_id,
            topics,
            timeout_ms,
            false,
        );
        stream.write_all(&request).map_err(|e| KafkaError::Io(e))?;
        let response = read_response(&mut stream)?;
        parse_create_topics_response(&response)
    }

    /// Delete topics from the broker.
    pub fn delete_topics(
        &mut self,
        topic_names: &[&str],
        timeout_ms: i32,
    ) -> Result<Vec<DeleteTopicResponse>, KafkaError> {
        let mut stream = TcpStream::connect(&self.broker)
            .map_err(|e| KafkaError::Io(e))?;
        let request = build_delete_topics_request(
            self.next_correlation_id(),
            &self.client_id,
            topic_names,
            timeout_ms,
        );
        stream.write_all(&request).map_err(|e| KafkaError::Io(e))?;
        let response = read_response(&mut stream)?;
        parse_delete_topics_response(&response)
    }

    /// List offsets for topic partitions.
    pub fn list_offsets(
        &mut self,
        topics: &[ListOffsetsTopic],
    ) -> Result<Vec<ListOffsetsResponseTopic>, KafkaError> {
        let mut stream = TcpStream::connect(&self.broker)
            .map_err(|e| KafkaError::Io(e))?;
        let request = build_list_offsets_request(
            self.next_correlation_id(),
            &self.client_id,
            topics,
        );
        stream.write_all(&request).map_err(|e| KafkaError::Io(e))?;
        let response = read_response(&mut stream)?;
        parse_list_offsets_response(&response)
    }

    /// Fetch cluster metadata.
    pub fn metadata(&mut self, topics: &[&str]) -> Result<TopicMetadataList, KafkaError> {
        let mut stream = TcpStream::connect(&self.broker)
            .map_err(|e| KafkaError::Io(e))?;
        let request = build_metadata_request(
            self.next_correlation_id(),
            &self.client_id,
            topics,
        );
        stream.write_all(&request).map_err(|e| KafkaError::Io(e))?;
        let response = read_response(&mut stream)?;
        parse_metadata_response(&response)
    }
}

// ─── Public types ──────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct KafkaMessage {
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub enum KafkaError {
    Io(std::io::Error),
    Protocol(String),
    TopicNotFound(String),
    NoPartition(String),
}

impl std::fmt::Display for KafkaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KafkaError::Io(e) => write!(f, "I/O error: {}", e),
            KafkaError::Protocol(msg) => write!(f, "protocol error: {}", msg),
            KafkaError::TopicNotFound(t) => write!(f, "topic not found: {}", t),
            KafkaError::NoPartition(t) => write!(f, "no partition for topic: {}", t),
        }
    }
}

// ─── High-level stream runner ──────────────────────────────────────────────

/// Run a Kafka consumer that feeds messages into a Memgraph storage engine.
/// Each message value is treated as a Cypher query string.
pub fn run_consumer(
    storage: Arc<Storage>,
    broker: &str,
    topic: &str,
) -> Result<(), KafkaError> {
    let mut consumer = KafkaConsumer::new(broker, topic);
    consumer.connect()?;

    loop {
        match consumer.fetch(1024 * 1024) {
            Ok(messages) => {
                if messages.is_empty() {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    continue;
                }
                for msg in messages {
                    let query = String::from_utf8_lossy(&msg.value);
                    if let Err(e) = mginterp::execute(&storage, &query) {
                        eprintln!("[kafka] query failed: {}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("[kafka] fetch error: {}", e);
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }
}

// ─── Record batch builder ──────────────────────────────────────────────────

/// Build a Kafka v2 record batch from a slice of producer records.
fn build_record_batch(records: &[&ProducerRecord]) -> Vec<u8> {
    use protocol::{write_i16, write_i32, write_i64, write_i8, write_varint};

    let mut body = Vec::new();

    // Record batch header
    let base_offset: i64 = 0;
    let producer_id: i64 = -1;
    let producer_epoch: i16 = -1;
    let base_sequence: i32 = -1;

    // First pass: build records
    let mut records_buf = Vec::new();
    for (i, rec) in records.iter().enumerate() {
        let mut rec_buf = Vec::new();
        write_varint(&mut rec_buf, 0); // length placeholder (varint of record length)
        write_i8(&mut rec_buf, 0); // attributes
        write_varint(&mut rec_buf, 0); // timestamp delta
        write_varint(&mut rec_buf, i as u64); // offset delta

        // key
        match &rec.key {
            Some(k) => {
                write_varint(&mut rec_buf, k.len() as u64 + 1);
                rec_buf.extend_from_slice(k);
            }
            None => write_varint(&mut rec_buf, 0),
        }

        // value
        write_varint(&mut rec_buf, rec.value.len() as u64 + 1);
        rec_buf.extend_from_slice(&rec.value);

        // headers
        write_varint(&mut rec_buf, rec.headers.len() as u64);
        for (k, v) in &rec.headers {
            write_varint(&mut rec_buf, k.len() as u64 + 1);
            rec_buf.extend_from_slice(k.as_bytes());
            write_varint(&mut rec_buf, v.len() as u64 + 1);
            rec_buf.extend_from_slice(v);
        }

        // Fix record length (first varint = len of rest of record)
        let record_content = rec_buf[rec_buf.len() - (rec_buf.len() - 1)..].to_vec();
        let mut len_buf = Vec::new();
        write_varint(&mut len_buf, record_content.len() as u64 + 1);
        records_buf.extend_from_slice(&len_buf);
        records_buf.extend_from_slice(&record_content);
    }

    let batch_len = 61 + records_buf.len(); // header size + records

    write_i64(&mut body, base_offset);
    write_i32(&mut body, batch_len as i32);
    write_i32(&mut body, 0); // leader_epoch
    write_i8(&mut body, 2); // magic = 2
    write_i32(&mut body, 0); // crc placeholder
    write_i16(&mut body, 0); // attributes
    write_i32(&mut body, (records.len() - 1) as i32); // last_offset_delta
    write_i64(&mut body, 0); // first_timestamp
    write_i64(&mut body, 0); // max_timestamp
    write_i64(&mut body, producer_id);
    write_i16(&mut body, producer_epoch);
    write_i32(&mut body, base_sequence);
    write_i32(&mut body, records.len() as i32);
    body.extend_from_slice(&records_buf);

    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_record_batch_structure() {
        let rec = ProducerRecord {
            topic: "test".to_string(),
            partition: 0,
            key: Some(b"key1".to_vec()),
            value: b"value1".to_vec(),
            headers: vec![("h1".to_string(), b"hv1".to_vec())],
        };
        let batch = build_record_batch(&[&rec]);
        assert!(!batch.is_empty());
        // Should start with base_offset = 0
        assert_eq!(&batch[0..8], &[0, 0, 0, 0, 0, 0, 0, 0]);
        // Magic byte at offset 16 should be 2
        assert_eq!(batch[16], 2);
    }

    #[test]
    fn test_kafka_admin_struct() {
        let admin = KafkaAdmin::new("localhost:9092");
        assert_eq!(admin.broker, "localhost:9092");
    }

    #[test]
    fn test_kafka_producer_struct() {
        let mut producer = KafkaProducer::new("localhost:9092");
        assert_eq!(producer.broker, "localhost:9092");
        assert_eq!(producer.next_correlation_id(), 1);
        assert_eq!(producer.next_correlation_id(), 2);
    }
}
