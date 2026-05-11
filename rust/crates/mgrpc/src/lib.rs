#![allow(unused)]
//! # mgrpc — Internal RPC protocol for cluster communication.
//!
//! Wire format: `[MessageSize(u32 LE)][SLK-framed payload]`
//! Protocol versions V1-V6 with backward compatibility.
//! Used by replication and coordinator.

use std::io::{Read, Write};
use std::net::TcpStream;

use mgslk::{Builder, Reader, SlkDecodeError, SlkLoad, SlkSave, slk_encode};

// ─── Protocol version ──────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(u8)]
pub enum ProtocolVersion {
    V1 = 1,
    V2 = 2,
    V3 = 3,
    V4 = 4,
    V5 = 5,
    V6 = 6,
}

impl ProtocolVersion {
    pub const CURRENT: Self = Self::V6;

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::V1),
            2 => Some(Self::V2),
            3 => Some(Self::V3),
            4 => Some(Self::V4),
            5 => Some(Self::V5),
            6 => Some(Self::V6),
            _ => None,
        }
    }
}

impl SlkSave for ProtocolVersion {
    fn slk_save(&self, builder: &mut Builder) {
        (*self as u8).slk_save(builder);
    }
}

impl SlkLoad for ProtocolVersion {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        let v = u8::slk_load(reader)?;
        Self::from_u8(v).ok_or_else(|| SlkDecodeError::from(format!("unknown protocol version: {}", v)))
    }
}

// ─── Message header ────────────────────────────────────────────────────────

/// Every RPC message starts with this header.
#[derive(Clone, Debug, PartialEq)]
pub struct MessageHeader {
    pub protocol_version: ProtocolVersion,
    pub message_id: u64,
    pub message_version: u64,
}

impl MessageHeader {
    /// Create a header using the current protocol version.
    pub fn new(message_id: u64, message_version: u64) -> Self {
        Self {
            protocol_version: ProtocolVersion::CURRENT,
            message_id,
            message_version,
        }
    }
}

impl SlkSave for MessageHeader {
    fn slk_save(&self, builder: &mut Builder) {
        self.protocol_version.slk_save(builder);
        self.message_id.slk_save(builder);
        self.message_version.slk_save(builder);
    }
}

impl SlkLoad for MessageHeader {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            protocol_version: ProtocolVersion::slk_load(reader)?,
            message_id: u64::slk_load(reader)?,
            message_version: u64::slk_load(reader)?,
        })
    }
}

// ─── Core replication message types ────────────────────────────────────────

/// Heartbeat: periodic health check between main and replica.
#[derive(Clone, Debug, PartialEq)]
pub struct Heartbeat {
    pub main_uuid: String,
    pub timestamp: u64,
    pub epoch_id: u64,
}

impl SlkSave for Heartbeat {
    fn slk_save(&self, builder: &mut Builder) {
        self.main_uuid.slk_save(builder);
        self.timestamp.slk_save(builder);
        self.epoch_id.slk_save(builder);
    }
}

impl SlkLoad for Heartbeat {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            main_uuid: String::slk_load(reader)?,
            timestamp: u64::slk_load(reader)?,
            epoch_id: u64::slk_load(reader)?,
        })
    }
}

/// Snapshot request: transfer full database state.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotRequest {
    pub last_durable_timestamp: u64,
}

impl SlkSave for SnapshotRequest {
    fn slk_save(&self, builder: &mut Builder) {
        self.last_durable_timestamp.slk_save(builder);
    }
}

impl SlkLoad for SnapshotRequest {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            last_durable_timestamp: u64::slk_load(reader)?,
        })
    }
}

/// Snapshot response data.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotData {
    pub data: Vec<u8>,
}

impl SlkSave for SnapshotData {
    fn slk_save(&self, builder: &mut Builder) {
        self.data.slk_save(builder);
    }
}

impl SlkLoad for SnapshotData {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            data: Vec::<u8>::slk_load(reader)?,
        })
    }
}

/// WAL file transfer: send WAL records for replication catch-up.
#[derive(Clone, Debug, PartialEq)]
pub struct WalTransfer {
    pub file_name: String,
    pub data: Vec<u8>,
}

impl SlkSave for WalTransfer {
    fn slk_save(&self, builder: &mut Builder) {
        self.file_name.slk_save(builder);
        self.data.slk_save(builder);
    }
}

impl SlkLoad for WalTransfer {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            file_name: String::slk_load(reader)?,
            data: Vec::<u8>::slk_load(reader)?,
        })
    }
}

/// Request WAL records since a given timestamp, scoped to an epoch.
/// Used by replicas during catch-up after a snapshot or after losing
/// connectivity to the main.
#[derive(Clone, Debug, PartialEq)]
pub struct WalRequest {
    pub epoch_id: u64,
    pub since_timestamp: u64,
}

impl SlkSave for WalRequest {
    fn slk_save(&self, builder: &mut Builder) {
        self.epoch_id.slk_save(builder);
        self.since_timestamp.slk_save(builder);
    }
}

impl SlkLoad for WalRequest {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            epoch_id: u64::slk_load(reader)?,
            since_timestamp: u64::slk_load(reader)?,
        })
    }
}

// ─── Delta streaming ───────────────────────────────────────────────────────

/// Request a stream of deltas starting from a given commit timestamp.
#[derive(Clone, Debug, PartialEq)]
pub struct DeltaStreamRequest {
    pub epoch_id: u64,
    pub start_timestamp: u64,
    pub batch_size_limit: u32,
}

impl SlkSave for DeltaStreamRequest {
    fn slk_save(&self, builder: &mut Builder) {
        self.epoch_id.slk_save(builder);
        self.start_timestamp.slk_save(builder);
        self.batch_size_limit.slk_save(builder);
    }
}

impl SlkLoad for DeltaStreamRequest {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            epoch_id: u64::slk_load(reader)?,
            start_timestamp: u64::slk_load(reader)?,
            batch_size_limit: u32::slk_load(reader)?,
        })
    }
}

/// A batch of deltas streamed from main to replica.
#[derive(Clone, Debug, PartialEq)]
pub struct DeltaBatch {
    pub epoch_id: u64,
    pub commit_timestamp: u64,
    pub sequence_number: u64,
    pub deltas: Vec<mgdurability::DeltaRecord>,
}

impl SlkSave for DeltaBatch {
    fn slk_save(&self, builder: &mut Builder) {
        self.epoch_id.slk_save(builder);
        self.commit_timestamp.slk_save(builder);
        self.sequence_number.slk_save(builder);
        self.deltas.slk_save(builder);
    }
}

impl SlkLoad for DeltaBatch {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            epoch_id: u64::slk_load(reader)?,
            commit_timestamp: u64::slk_load(reader)?,
            sequence_number: u64::slk_load(reader)?,
            deltas: Vec::<mgdurability::DeltaRecord>::slk_load(reader)?,
        })
    }
}

// ─── RPC Client ────────────────────────────────────────────────────────────

/// RPC client connects to a server and sends/receives messages.
pub struct RpcClient {
    stream: TcpStream,
}

impl RpcClient {
    pub fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(Self { stream })
    }

    /// Access the underlying TCP stream (e.g. to set timeouts).
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    /// Send a message: header + body serialized as a single SLK stream.
    pub fn send<T: SlkSave>(&mut self, header: &MessageHeader, body: &T) -> std::io::Result<()> {
        // Encode header then body into one SLK stream, extract raw payload
        let encoded = encode_rpc_message(header, body);
        let total_size = (encoded.len() as u32).to_le_bytes();
        self.stream.write_all(&total_size)?;
        self.stream.write_all(&encoded)?;
        self.stream.flush()
    }

    /// Receive a message from the wire.
    pub fn recv<T: SlkLoad>(&mut self) -> std::io::Result<(MessageHeader, T)> {
        let mut size_buf = [0u8; 4];
        self.stream.read_exact(&mut size_buf)?;
        let msg_size = u32::from_le_bytes(size_buf) as usize;

        let mut payload = vec![0u8; msg_size];
        self.stream.read_exact(&mut payload)?;

        decode_rpc_message(&payload)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{}", e)))
    }

    /// Receive a message and return the header along with the raw payload
    /// (header + body bytes). Use [`decode_body`] to extract the body once
    /// the message_id has been inspected.
    pub fn recv_dispatch(&mut self) -> std::io::Result<(MessageHeader, Vec<u8>)> {
        let mut size_buf = [0u8; 4];
        self.stream.read_exact(&mut size_buf)?;
        let msg_size = u32::from_le_bytes(size_buf) as usize;

        let mut payload = vec![0u8; msg_size];
        self.stream.read_exact(&mut payload)?;

        let mut framed = Vec::new();
        framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        framed.extend_from_slice(&payload);
        let mut reader = Reader::new(&framed);
        let header = MessageHeader::slk_load(&mut reader)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{}", e)))?;
        Ok((header, payload))
    }
}

/// Decode the body of a payload returned by [`RpcClient::recv_dispatch`].
/// Skips the header and returns the next SlkLoad item.
pub fn decode_body<T: SlkLoad>(payload: &[u8]) -> Result<T, SlkDecodeError> {
    let mut framed = Vec::new();
    framed.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    framed.extend_from_slice(payload);
    let mut reader = Reader::new(&framed);
    let _ = MessageHeader::slk_load(&mut reader)?;
    T::slk_load(&mut reader)
}

/// Encode header + body to raw bytes (no SLK segment framing, just the data).
fn encode_rpc_message<H: SlkSave, B: SlkSave>(header: &H, body: &B) -> Vec<u8> {
    // Serialize into SLK, then strip the segment framing
    let (mut builder, collector) = mgslk::Builder::new_collecting();
    header.slk_save(&mut builder);
    body.slk_save(&mut builder);
    builder.finalize();
    let framed = collector.into_vec();
    extract_slk_payload(&framed)
}

/// Decode a raw RPC message (no SLK framing) back into header + body.
fn decode_rpc_message<H: SlkLoad, B: SlkLoad>(data: &[u8]) -> Result<(H, B), SlkDecodeError> {
    // Re-frame as a single SLK segment
    let mut framed = Vec::new();
    framed.extend_from_slice(&(data.len() as u32).to_le_bytes());
    framed.extend_from_slice(data);
    let mut reader = Reader::new(&framed);
    let header = H::slk_load(&mut reader)?;
    let body = B::slk_load(&mut reader)?;
    Ok((header, body))
}

/// Extract the raw payload from a single-segment SLK stream.
fn extract_slk_payload(framed: &[u8]) -> Vec<u8> {
    if framed.len() < 8 {
        return framed.to_vec();
    }
    let seg_size = u32::from_le_bytes([framed[0], framed[1], framed[2], framed[3]]) as usize;
    let end = 4 + seg_size;
    if end <= framed.len() {
        framed[4..end].to_vec()
    } else {
        framed.to_vec()
    }
}

// ─── Additional RPC message types ─────────────────────────────────────────

/// Cluster join request: new node wants to join the cluster.
#[derive(Clone, Debug, PartialEq)]
pub struct ClusterJoinRequest {
    pub node_id: u64,
    pub node_addr: String,
    pub last_epoch: u64,
}

impl SlkSave for ClusterJoinRequest {
    fn slk_save(&self, builder: &mut Builder) {
        self.node_id.slk_save(builder);
        self.node_addr.slk_save(builder);
        self.last_epoch.slk_save(builder);
    }
}

impl SlkLoad for ClusterJoinRequest {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            node_id: u64::slk_load(reader)?,
            node_addr: String::slk_load(reader)?,
            last_epoch: u64::slk_load(reader)?,
        })
    }
}

/// Cluster join response.
#[derive(Clone, Debug, PartialEq)]
pub struct ClusterJoinResponse {
    pub accepted: bool,
    pub current_epoch: u64,
    pub leader_id: u64,
}

impl SlkSave for ClusterJoinResponse {
    fn slk_save(&self, builder: &mut Builder) {
        self.accepted.slk_save(builder);
        self.current_epoch.slk_save(builder);
        self.leader_id.slk_save(builder);
    }
}

impl SlkLoad for ClusterJoinResponse {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            accepted: bool::slk_load(reader)?,
            current_epoch: u64::slk_load(reader)?,
            leader_id: u64::slk_load(reader)?,
        })
    }
}

/// File replication request: transfer a single file.
#[derive(Clone, Debug, PartialEq)]
pub struct FileReplicationRequest {
    pub file_path: String,
    pub offset: u64,
    pub chunk_size: u32,
}

impl SlkSave for FileReplicationRequest {
    fn slk_save(&self, builder: &mut Builder) {
        self.file_path.slk_save(builder);
        self.offset.slk_save(builder);
        self.chunk_size.slk_save(builder);
    }
}

impl SlkLoad for FileReplicationRequest {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            file_path: String::slk_load(reader)?,
            offset: u64::slk_load(reader)?,
            chunk_size: u32::slk_load(reader)?,
        })
    }
}

/// File replication response chunk.
#[derive(Clone, Debug, PartialEq)]
pub struct FileReplicationResponse {
    pub file_path: String,
    pub offset: u64,
    pub data: Vec<u8>,
    pub eof: bool,
}

impl SlkSave for FileReplicationResponse {
    fn slk_save(&self, builder: &mut Builder) {
        self.file_path.slk_save(builder);
        self.offset.slk_save(builder);
        self.data.slk_save(builder);
        self.eof.slk_save(builder);
    }
}

impl SlkLoad for FileReplicationResponse {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            file_path: String::slk_load(reader)?,
            offset: u64::slk_load(reader)?,
            data: Vec::<u8>::slk_load(reader)?,
            eof: bool::slk_load(reader)?,
        })
    }
}

/// Epoch proposal: Raft leader proposes a new epoch.
#[derive(Clone, Debug, PartialEq)]
pub struct EpochProposal {
    pub epoch_id: u64,
    pub proposer_id: u64,
    pub previous_epoch: u64,
}

impl SlkSave for EpochProposal {
    fn slk_save(&self, builder: &mut Builder) {
        self.epoch_id.slk_save(builder);
        self.proposer_id.slk_save(builder);
        self.previous_epoch.slk_save(builder);
    }
}

impl SlkLoad for EpochProposal {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            epoch_id: u64::slk_load(reader)?,
            proposer_id: u64::slk_load(reader)?,
            previous_epoch: u64::slk_load(reader)?,
        })
    }
}

/// Epoch acknowledgement from followers.
#[derive(Clone, Debug, PartialEq)]
pub struct EpochAck {
    pub epoch_id: u64,
    pub node_id: u64,
    pub accepted: bool,
}

impl SlkSave for EpochAck {
    fn slk_save(&self, builder: &mut Builder) {
        self.epoch_id.slk_save(builder);
        self.node_id.slk_save(builder);
        self.accepted.slk_save(builder);
    }
}

impl SlkLoad for EpochAck {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            epoch_id: u64::slk_load(reader)?,
            node_id: u64::slk_load(reader)?,
            accepted: bool::slk_load(reader)?,
        })
    }
}

/// Query routing request: ask coordinator which node handles a query.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRouteRequest {
    pub query_hash: u64,
    pub read_only: bool,
}

impl SlkSave for QueryRouteRequest {
    fn slk_save(&self, builder: &mut Builder) {
        self.query_hash.slk_save(builder);
        self.read_only.slk_save(builder);
    }
}

impl SlkLoad for QueryRouteRequest {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            query_hash: u64::slk_load(reader)?,
            read_only: bool::slk_load(reader)?,
        })
    }
}

/// Query routing response.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRouteResponse {
    pub node_id: u64,
    pub node_addr: String,
}

impl SlkSave for QueryRouteResponse {
    fn slk_save(&self, builder: &mut Builder) {
        self.node_id.slk_save(builder);
        self.node_addr.slk_save(builder);
    }
}

impl SlkLoad for QueryRouteResponse {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            node_id: u64::slk_load(reader)?,
            node_addr: String::slk_load(reader)?,
        })
    }
}

/// Status request: check node health and load.
#[derive(Clone, Debug, PartialEq)]
pub struct StatusRequest {
    pub include_metrics: bool,
}

impl SlkSave for StatusRequest {
    fn slk_save(&self, builder: &mut Builder) {
        self.include_metrics.slk_save(builder);
    }
}

impl SlkLoad for StatusRequest {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            include_metrics: bool::slk_load(reader)?,
        })
    }
}

/// Status response: node health and basic metrics.
#[derive(Clone, Debug, PartialEq)]
pub struct StatusResponse {
    pub node_id: u64,
    pub healthy: bool,
    pub vertex_count: u64,
    pub edge_count: u64,
    pub memory_used_mb: u64,
}

impl SlkSave for StatusResponse {
    fn slk_save(&self, builder: &mut Builder) {
        self.node_id.slk_save(builder);
        self.healthy.slk_save(builder);
        self.vertex_count.slk_save(builder);
        self.edge_count.slk_save(builder);
        self.memory_used_mb.slk_save(builder);
    }
}

impl SlkLoad for StatusResponse {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            node_id: u64::slk_load(reader)?,
            healthy: bool::slk_load(reader)?,
            vertex_count: u64::slk_load(reader)?,
            edge_count: u64::slk_load(reader)?,
            memory_used_mb: u64::slk_load(reader)?,
        })
    }
}

/// Config sync: push configuration updates to nodes.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigSync {
    pub key: String,
    pub value: String,
    pub timestamp: u64,
}

impl SlkSave for ConfigSync {
    fn slk_save(&self, builder: &mut Builder) {
        self.key.slk_save(builder);
        self.value.slk_save(builder);
        self.timestamp.slk_save(builder);
    }
}

impl SlkLoad for ConfigSync {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            key: String::slk_load(reader)?,
            value: String::slk_load(reader)?,
            timestamp: u64::slk_load(reader)?,
        })
    }
}

/// Config sync acknowledgement.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigSyncAck {
    pub node_id: u64,
    pub accepted: bool,
}

impl SlkSave for ConfigSyncAck {
    fn slk_save(&self, builder: &mut Builder) {
        self.node_id.slk_save(builder);
        self.accepted.slk_save(builder);
    }
}

impl SlkLoad for ConfigSyncAck {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            node_id: u64::slk_load(reader)?,
            accepted: bool::slk_load(reader)?,
        })
    }
}

/// RPC message envelope with correlation ID for request/response matching.
#[derive(Clone, Debug, PartialEq)]
pub struct RpcEnvelope<T> {
    pub correlation_id: u64,
    pub payload: T,
}

impl<T: SlkSave> SlkSave for RpcEnvelope<T> {
    fn slk_save(&self, builder: &mut Builder) {
        self.correlation_id.slk_save(builder);
        self.payload.slk_save(builder);
    }
}

impl<T: SlkLoad> SlkLoad for RpcEnvelope<T> {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            correlation_id: u64::slk_load(reader)?,
            payload: T::slk_load(reader)?,
        })
    }
}

/// Message batch for efficient bulk transmission.
#[derive(Clone, Debug, PartialEq)]
pub struct MessageBatch {
    pub messages: Vec<Vec<u8>>,
}

impl SlkSave for MessageBatch {
    fn slk_save(&self, builder: &mut Builder) {
        self.messages.slk_save(builder);
    }
}

impl SlkLoad for MessageBatch {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            messages: Vec::<Vec<u8>>::slk_load(reader)?,
        })
    }
}

/// RPC handler trait for dispatching incoming messages.
pub trait RpcHandler {
    fn handle_heartbeat(&mut self, hb: Heartbeat) -> Result<(), String>;
    fn handle_delta_stream_request(&mut self, req: DeltaStreamRequest) -> Result<DeltaBatch, String>;
    fn handle_snapshot_request(&mut self, req: SnapshotRequest) -> Result<SnapshotData, String>;
    fn handle_status_request(&mut self, req: StatusRequest) -> Result<StatusResponse, String>;
    fn handle_config_sync(&mut self, sync: ConfigSync) -> Result<ConfigSyncAck, String>;
    fn handle_wal_request(&mut self, req: WalRequest) -> Result<WalTransfer, String>;
}

/// Dispatch a raw RPC message to the appropriate handler based on message ID.
pub fn dispatch_message<H: RpcHandler>(
    handler: &mut H,
    msg_id: u64,
    data: &[u8],
) -> Result<Vec<u8>, String> {
    let mut reader = Reader::new(data);
    match msg_id {
        1 => {
            let hb = Heartbeat::slk_load(&mut reader).map_err(|e| e.to_string())?;
            handler.handle_heartbeat(hb)?;
            Ok(Vec::new())
        }
        2 => {
            let req = DeltaStreamRequest::slk_load(&mut reader).map_err(|e| e.to_string())?;
            let batch = handler.handle_delta_stream_request(req)?;
            let (mut builder, collector) = Builder::new_collecting();
            batch.slk_save(&mut builder);
            builder.finalize();
            Ok(collector.into_vec())
        }
        3 => {
            let req = SnapshotRequest::slk_load(&mut reader).map_err(|e| e.to_string())?;
            let data = handler.handle_snapshot_request(req)?;
            let (mut builder, collector) = Builder::new_collecting();
            data.slk_save(&mut builder);
            builder.finalize();
            Ok(collector.into_vec())
        }
        4 => {
            let req = StatusRequest::slk_load(&mut reader).map_err(|e| e.to_string())?;
            let resp = handler.handle_status_request(req)?;
            let (mut builder, collector) = Builder::new_collecting();
            resp.slk_save(&mut builder);
            builder.finalize();
            Ok(collector.into_vec())
        }
        5 => {
            let sync = ConfigSync::slk_load(&mut reader).map_err(|e| e.to_string())?;
            let ack = handler.handle_config_sync(sync)?;
            let (mut builder, collector) = Builder::new_collecting();
            ack.slk_save(&mut builder);
            builder.finalize();
            Ok(collector.into_vec())
        }
        11 => {
            let req = WalRequest::slk_load(&mut reader).map_err(|e| e.to_string())?;
            let transfer = handler.handle_wal_request(req)?;
            let (mut builder, collector) = Builder::new_collecting();
            transfer.slk_save(&mut builder);
            builder.finalize();
            Ok(collector.into_vec())
        }
        _ => Err(format!("unknown message id: {}", msg_id)),
    }
}

// ─── RPC Server ────────────────────────────────────────────────────────────

/// RPC server listens for incoming connections and dispatches messages.
pub struct RpcServer {
    listener: std::net::TcpListener,
}

impl RpcServer {
    pub fn bind(addr: &str) -> std::io::Result<Self> {
        let listener = std::net::TcpListener::bind(addr)?;
        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    pub fn accept(&self) -> std::io::Result<(RpcClient, std::net::SocketAddr)> {
        let (stream, addr) = self.listener.accept()?;
        Ok((RpcClient { stream }, addr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_header_roundtrip() {
        let header = MessageHeader {
            protocol_version: ProtocolVersion::V6,
            message_id: 42,
            message_version: 1,
        };
        let encoded = slk_encode(&header);
        let mut reader = Reader::new(&encoded);
        let decoded = MessageHeader::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_heartbeat_roundtrip() {
        let hb = Heartbeat {
            main_uuid: "550e8400-e29b-41d4-a716-446655440000".into(),
            timestamp: 1234567890,
            epoch_id: 42,
        };
        let encoded = slk_encode(&hb);
        let mut reader = Reader::new(&encoded);
        let decoded = Heartbeat::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, hb);
    }

    #[test]
    fn test_rpc_client_server() {
        use std::thread;

        let server = RpcServer::bind("127.0.0.1:0").unwrap();
        let addr = server.listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut client, _) = server.accept().unwrap();
            let (header, hb): (MessageHeader, Heartbeat) = client.recv().unwrap();
            assert_eq!(hb.main_uuid, "test-uuid");
            // Echo back
            client.send(&header, &hb).unwrap();
        });

        let mut client = RpcClient::connect(&addr.to_string()).unwrap();
        let header = MessageHeader {
            protocol_version: ProtocolVersion::V6,
            message_id: 1,
            message_version: 1,
        };
        let hb = Heartbeat {
            main_uuid: "test-uuid".into(),
            timestamp: 100,
            epoch_id: 0,
        };
        client.send(&header, &hb).unwrap();

        let (_resp_header, resp_hb): (MessageHeader, Heartbeat) = client.recv().unwrap();
        assert_eq!(resp_hb.main_uuid, "test-uuid");

        handle.join().unwrap();
    }

    #[test]
    fn test_delta_batch_roundtrip() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;

        let batch = DeltaBatch {
            epoch_id: 7,
            commit_timestamp: 42,
            sequence_number: 3,
            deltas: vec![
                DeltaRecord::VertexCreate {
                    gid: Gid::from(1u64),
                    timestamp: 100,
                },
                DeltaRecord::VertexSetProperty {
                    gid: Gid::from(1u64),
                    key: mgcore::types::PropertyId::from(0u32),
                    value: mgcore::property_value::PropertyValue::Int(42),
                },
            ],
        };
        let encoded = slk_encode(&batch);
        let mut reader = Reader::new(&encoded);
        let decoded = DeltaBatch::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, batch);
    }

    #[test]
    fn test_delta_stream_request_roundtrip() {
        let req = DeltaStreamRequest {
            epoch_id: 5,
            start_timestamp: 1000,
            batch_size_limit: 256,
        };
        let encoded = slk_encode(&req);
        let mut reader = Reader::new(&encoded);
        let decoded = DeltaStreamRequest::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_cluster_join_request_roundtrip() {
        let req = ClusterJoinRequest {
            node_id: 42,
            node_addr: "192.168.1.1:7687".into(),
            last_epoch: 7,
        };
        let encoded = slk_encode(&req);
        let mut reader = Reader::new(&encoded);
        let decoded = ClusterJoinRequest::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_cluster_join_response_roundtrip() {
        let resp = ClusterJoinResponse {
            accepted: true,
            current_epoch: 8,
            leader_id: 1,
        };
        let encoded = slk_encode(&resp);
        let mut reader = Reader::new(&encoded);
        let decoded = ClusterJoinResponse::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn test_file_replication_request_roundtrip() {
        let req = FileReplicationRequest {
            file_path: "/data/wal/0001.wal".into(),
            offset: 1024,
            chunk_size: 4096,
        };
        let encoded = slk_encode(&req);
        let mut reader = Reader::new(&encoded);
        let decoded = FileReplicationRequest::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_file_replication_response_roundtrip() {
        let resp = FileReplicationResponse {
            file_path: "/data/wal/0001.wal".into(),
            offset: 1024,
            data: vec![1, 2, 3, 4, 5],
            eof: false,
        };
        let encoded = slk_encode(&resp);
        let mut reader = Reader::new(&encoded);
        let decoded = FileReplicationResponse::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn test_epoch_proposal_roundtrip() {
        let prop = EpochProposal {
            epoch_id: 10,
            proposer_id: 1,
            previous_epoch: 9,
        };
        let encoded = slk_encode(&prop);
        let mut reader = Reader::new(&encoded);
        let decoded = EpochProposal::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, prop);
    }

    #[test]
    fn test_epoch_ack_roundtrip() {
        let ack = EpochAck {
            epoch_id: 10,
            node_id: 2,
            accepted: true,
        };
        let encoded = slk_encode(&ack);
        let mut reader = Reader::new(&encoded);
        let decoded = EpochAck::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, ack);
    }

    #[test]
    fn test_query_route_request_roundtrip() {
        let req = QueryRouteRequest {
            query_hash: 0xdeadbeef,
            read_only: true,
        };
        let encoded = slk_encode(&req);
        let mut reader = Reader::new(&encoded);
        let decoded = QueryRouteRequest::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_query_route_response_roundtrip() {
        let resp = QueryRouteResponse {
            node_id: 3,
            node_addr: "10.0.0.3:7687".into(),
        };
        let encoded = slk_encode(&resp);
        let mut reader = Reader::new(&encoded);
        let decoded = QueryRouteResponse::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn test_snapshot_request_roundtrip() {
        let req = SnapshotRequest {
            last_durable_timestamp: 12345,
        };
        let encoded = slk_encode(&req);
        let mut reader = Reader::new(&encoded);
        let decoded = SnapshotRequest::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_wal_transfer_roundtrip() {
        let wt = WalTransfer {
            file_name: "wal-0001.log".into(),
            data: vec![0u8; 100],
        };
        let encoded = slk_encode(&wt);
        let mut reader = Reader::new(&encoded);
        let decoded = WalTransfer::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, wt);
    }

    #[test]
    fn test_status_request_roundtrip() {
        let req = StatusRequest { include_metrics: true };
        let encoded = slk_encode(&req);
        let mut reader = Reader::new(&encoded);
        let decoded = StatusRequest::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_status_response_roundtrip() {
        let resp = StatusResponse {
            node_id: 42,
            healthy: true,
            vertex_count: 1000,
            edge_count: 5000,
            memory_used_mb: 256,
        };
        let encoded = slk_encode(&resp);
        let mut reader = Reader::new(&encoded);
        let decoded = StatusResponse::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, resp);
    }

    #[test]
    fn test_config_sync_roundtrip() {
        let sync = ConfigSync {
            key: "query_timeout".into(),
            value: "300".into(),
            timestamp: 1234567890,
        };
        let encoded = slk_encode(&sync);
        let mut reader = Reader::new(&encoded);
        let decoded = ConfigSync::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, sync);
    }

    #[test]
    fn test_config_sync_ack_roundtrip() {
        let ack = ConfigSyncAck {
            node_id: 7,
            accepted: true,
        };
        let encoded = slk_encode(&ack);
        let mut reader = Reader::new(&encoded);
        let decoded = ConfigSyncAck::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, ack);
    }

    #[test]
    fn test_rpc_envelope_roundtrip() {
        let envelope = RpcEnvelope {
            correlation_id: 12345,
            payload: Heartbeat {
                main_uuid: "test".into(),
                timestamp: 1,
                epoch_id: 2,
            },
        };
        let encoded = slk_encode(&envelope);
        let mut reader = Reader::new(&encoded);
        let decoded = RpcEnvelope::<Heartbeat>::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn test_message_batch_roundtrip() {
        let batch = MessageBatch {
            messages: vec![vec![1, 2, 3], vec![4, 5, 6]],
        };
        let encoded = slk_encode(&batch);
        let mut reader = Reader::new(&encoded);
        let decoded = MessageBatch::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, batch);
    }

    #[test]
    fn test_dispatch_unknown_message() {
        struct DummyHandler;
        impl RpcHandler for DummyHandler {
            fn handle_heartbeat(&mut self, _hb: Heartbeat) -> Result<(), String> { Ok(()) }
            fn handle_delta_stream_request(&mut self, _req: DeltaStreamRequest) -> Result<DeltaBatch, String> {
                Err("not implemented".into())
            }
            fn handle_snapshot_request(&mut self, _req: SnapshotRequest) -> Result<SnapshotData, String> {
                Err("not implemented".into())
            }
            fn handle_status_request(&mut self, _req: StatusRequest) -> Result<StatusResponse, String> {
                Err("not implemented".into())
            }
            fn handle_config_sync(&mut self, _sync: ConfigSync) -> Result<ConfigSyncAck, String> {
                Err("not implemented".into())
            }
            fn handle_wal_request(&mut self, _req: WalRequest) -> Result<WalTransfer, String> {
                Err("not implemented".into())
            }
        }
        let mut handler = DummyHandler;
        let result = dispatch_message(&mut handler, 999, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_wal_request_roundtrip() {
        let req = WalRequest {
            epoch_id: 7,
            since_timestamp: 1234567890,
        };
        let encoded = slk_encode(&req);
        let mut reader = Reader::new(&encoded);
        let decoded = WalRequest::slk_load(&mut reader).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn test_dispatch_wal_request() {
        struct WalHandler;
        impl RpcHandler for WalHandler {
            fn handle_heartbeat(&mut self, _hb: Heartbeat) -> Result<(), String> { Ok(()) }
            fn handle_delta_stream_request(&mut self, _req: DeltaStreamRequest) -> Result<DeltaBatch, String> {
                Err("not implemented".into())
            }
            fn handle_snapshot_request(&mut self, _req: SnapshotRequest) -> Result<SnapshotData, String> {
                Err("not implemented".into())
            }
            fn handle_status_request(&mut self, _req: StatusRequest) -> Result<StatusResponse, String> {
                Err("not implemented".into())
            }
            fn handle_config_sync(&mut self, _sync: ConfigSync) -> Result<ConfigSyncAck, String> {
                Err("not implemented".into())
            }
            fn handle_wal_request(&mut self, req: WalRequest) -> Result<WalTransfer, String> {
                Ok(WalTransfer {
                    file_name: format!("wal-since-{}.log", req.since_timestamp),
                    data: vec![req.epoch_id as u8; 4],
                })
            }
        }
        let mut handler = WalHandler;
        let req = WalRequest { epoch_id: 9, since_timestamp: 100 };
        let payload = slk_encode(&req);
        let response = dispatch_message(&mut handler, 11, &payload).unwrap();
        let mut reader = Reader::new(&response);
        let transfer = WalTransfer::slk_load(&mut reader).unwrap();
        assert_eq!(transfer.file_name, "wal-since-100.log");
        assert_eq!(transfer.data, vec![9, 9, 9, 9]);
    }
}
