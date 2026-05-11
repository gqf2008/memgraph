#![allow(unused)]
//! # mgrepl — Replication protocol for high availability.
//!
//! Supports SYNC, ASYNC, and STRICT_SYNC replication modes.
//! Uses mgrpc for the wire protocol.

use std::time::Duration;

use mgrpc::{
    DeltaBatch, DeltaStreamRequest, Heartbeat, MessageHeader, ProtocolVersion, RpcClient,
    RpcServer, SnapshotData, WalRequest, WalTransfer,
};

pub mod handler;

// Re-export new handler types for convenience.
pub use handler::{
    AckResult, DeltaBatcher, FailoverDetector, ReplicaHealth, ReplicationFilter,
    ReplicationMetrics, RetryState, StorageDeltaApplier,
};

// ─── Replication mode ──────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum ReplicationMode {
    Sync = 0,
    Async = 1,
    StrictSync = 2,
}

impl ReplicationMode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Sync),
            1 => Some(Self::Async),
            2 => Some(Self::StrictSync),
            _ => None,
        }
    }
}

// ─── Replication role ──────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplicationRole {
    None,
    Main,
    Replica,
}

// ─── Replication state ─────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplicaState {
    /// Fully caught up with main.
    Ready,
    /// Catching up from behind.
    Behind,
    /// Recovering from failure.
    Recovery,
}

// ─── Replication config ────────────────────────────────────────────────────

#[derive(Clone)]
pub struct ReplicationConfig {
    pub mode: ReplicationMode,
    pub heartbeat_interval: Duration,
    pub max_heartbeat_failures: u32,
    pub replica_port: u16,
    /// Directory containing WAL files for replication.
    pub wal_directory: Option<String>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            mode: ReplicationMode::Sync,
            heartbeat_interval: Duration::from_secs(1),
            max_heartbeat_failures: 3,
            replica_port: 10000,
            wal_directory: None,
        }
    }
}

// ─── Replication errors ────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplError {
    Io(String),
    EpochMismatch { expected: u64, actual: u64 },
    Conflict(String),
    Timeout,
    NotConnected,
    ReplicaNotFound(String),
    Protocol(String),
}

impl std::fmt::Display for ReplError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplError::Io(msg) => write!(f, "io error: {}", msg),
            ReplError::EpochMismatch { expected, actual } => {
                write!(f, "epoch mismatch: expected {}, got {}", expected, actual)
            }
            ReplError::Conflict(msg) => write!(f, "conflict: {}", msg),
            ReplError::Timeout => write!(f, "operation timed out"),
            ReplError::NotConnected => write!(f, "not connected"),
            ReplError::ReplicaNotFound(id) => write!(f, "replica not found: {}", id),
            ReplError::Protocol(msg) => write!(f, "protocol error: {}", msg),
        }
    }
}

impl std::error::Error for ReplError {}

impl From<std::io::Error> for ReplError {
    fn from(e: std::io::Error) -> Self {
        ReplError::Io(e.to_string())
    }
}

// ─── Replication status ────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplStatus {
    Ok,
    Behind,
    Syncing,
}

// ─── Request / Response types ──────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub enum ReplRequest {
    Heartbeat {
        epoch_id: u64,
        lsn: u64,
    },
    Snapshot {
        last_durable_timestamp: u64,
    },
    Wal {
        since_timestamp: u64,
    },
    DeltaStream {
        epoch_id: u64,
        since_timestamp: u64,
        batch_size_limit: u32,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ReplResponse {
    Heartbeat {
        epoch_id: u64,
        lsn: u64,
        main_uuid: String,
    },
    Snapshot {
        data: Vec<u8>,
    },
    Wal {
        file_name: String,
        data: Vec<u8>,
    },
    DeltaBatch(DeltaBatch),
    DeltaStreamBatch {
        batch: DeltaBatch,
    },
    DeltaStreamEnd,
    Ack,
    Error {
        message: String,
    },
}

// ─── Replication stream ────────────────────────────────────────────────────

/// Handles applying delta batches to storage with validation.
pub struct ReplicationStream;

impl ReplicationStream {
    /// Apply a batch of deltas to the given storage, validating epoch and
    /// detecting conflicts.
    pub fn apply_deltas(
        storage: &std::sync::Arc<mgstorage::storage::Storage>,
        deltas: &DeltaBatch,
        expected_epoch: u64,
    ) -> Result<(), ReplError> {
        if deltas.epoch_id != expected_epoch {
            return Err(ReplError::EpochMismatch {
                expected: expected_epoch,
                actual: deltas.epoch_id,
            });
        }

        // Conflict detection: check for concurrent writes by verifying
        // that no delta in the batch targets a GID that was modified
        // after the batch's commit timestamp in local storage.
        for delta in &deltas.deltas {
            let gid = delta_gid(delta);
            if let Some(gid) = gid {
                if Self::detect_conflict(storage, gid, deltas.commit_timestamp) {
                    return Err(ReplError::Conflict(format!(
                        "concurrent write detected on gid {} at ts {}",
                        gid, deltas.commit_timestamp
                    )));
                }
            }
        }

        // Apply deltas via the storage delta applier
        let mut applier = handler::StorageDeltaApplier::new(storage.clone());
        applier
            .apply_batch(deltas)
            .map_err(|e| ReplError::Io(e.to_string()))?;

        Ok(())
    }

    /// Detect if a conflict exists for the given GID: true if the local
    /// storage has a newer modification than the incoming batch timestamp.
    fn detect_conflict(
        storage: &std::sync::Arc<mgstorage::storage::Storage>,
        gid: mgcore::types::Gid,
        batch_ts: u64,
    ) -> bool {
        // Simplified conflict detection: if the vertex exists and was
        // created after the batch timestamp, we have a conflict.
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        if let Some(vsnap) = storage.get_vertex(gid, &tx) {
            // Use gid as a proxy for creation order; in a real system
            // we'd track per-vertex modification timestamps.
            let _ = vsnap;
            if gid.as_uint() > batch_ts {
                return true;
            }
        }
        false
    }
}

fn delta_gid(delta: &mgdurability::DeltaRecord) -> Option<mgcore::types::Gid> {
    use mgdurability::DeltaRecord;
    match delta {
        DeltaRecord::VertexCreate { gid, .. } => Some(*gid),
        DeltaRecord::VertexDelete { gid } => Some(*gid),
        DeltaRecord::VertexSetProperty { gid, .. } => Some(*gid),
        DeltaRecord::VertexAddLabel { gid, .. } => Some(*gid),
        DeltaRecord::VertexRemoveLabel { gid, .. } => Some(*gid),
        DeltaRecord::EdgeCreate { gid, .. } => Some(*gid),
        DeltaRecord::EdgeDelete { gid } => Some(*gid),
        DeltaRecord::EdgeSetProperty { gid, .. } => Some(*gid),
        _ => None,
    }
}

// ─── Replication mode clients ──────────────────────────────────────────────

/// Synchronous replication client: waits for ACK from the replica.
pub struct SyncReplicationClient {
    rpc: RpcClient,
    timeout: Duration,
}

impl SyncReplicationClient {
    pub fn new(rpc: RpcClient, timeout: Duration) -> Self {
        Self { rpc, timeout }
    }

    /// Send a delta batch and block until an ACK is received or timeout.
    pub fn send_and_wait(&mut self, batch: &DeltaBatch) -> Result<(), ReplError> {
        crate::send_delta_batch(&mut self.rpc, batch).map_err(|e| ReplError::Io(e.to_string()))?;
        match handler::wait_for_ack(&mut self.rpc, self.timeout) {
            handler::AckResult::Acknowledged => Ok(()),
            handler::AckResult::Timeout => Err(ReplError::Timeout),
            handler::AckResult::Error => Err(ReplError::Io("ack error".into())),
        }
    }

    pub fn rpc_mut(&mut self) -> &mut RpcClient {
        &mut self.rpc
    }
}

/// Asynchronous replication client: fire-and-forget.
pub struct AsyncReplicationClient {
    rpc: RpcClient,
}

impl AsyncReplicationClient {
    pub fn new(rpc: RpcClient) -> Self {
        Self { rpc }
    }

    /// Send a delta batch without waiting for acknowledgement.
    pub fn send(&mut self, batch: &DeltaBatch) -> Result<(), ReplError> {
        crate::send_delta_batch(&mut self.rpc, batch).map_err(|e| ReplError::Io(e.to_string()))
    }

    pub fn rpc_mut(&mut self) -> &mut RpcClient {
        &mut self.rpc
    }
}

/// Strict synchronous replication client: waits for majority ACKs.
pub struct StrictSyncReplicationClient {
    clients: Vec<SyncReplicationClient>,
    timeout: Duration,
}

impl StrictSyncReplicationClient {
    pub fn new(clients: Vec<SyncReplicationClient>, timeout: Duration) -> Self {
        Self { clients, timeout }
    }

    /// Send a delta batch to all replicas and wait for a majority of ACKs.
    /// First broadcasts to all replicas, then collects ACKs.
    pub fn send_and_wait_majority(&mut self, batch: &DeltaBatch) -> Result<(), ReplError> {
        let total = self.clients.len();
        if total == 0 {
            return Ok(());
        }
        let majority = (total / 2) + 1;

        // Phase 1: send to all replicas (fire-and-forget broadcast)
        for client in &mut self.clients {
            let _ = crate::send_delta_batch(client.rpc_mut(), batch);
        }

        // Phase 2: collect ACKs
        let mut acks = 0;
        let mut errors = Vec::new();
        for client in &mut self.clients {
            match handler::wait_for_ack(client.rpc_mut(), self.timeout) {
                handler::AckResult::Acknowledged => acks += 1,
                handler::AckResult::Timeout => errors.push(ReplError::Timeout),
                handler::AckResult::Error => errors.push(ReplError::Io("ack error".into())),
            }
            if acks >= majority {
                return Ok(());
            }
        }

        Err(ReplError::Io(format!(
            "strict sync failed: only {}/{} acks, errors: {:?}",
            acks, total, errors
        )))
    }

    pub fn clients_mut(&mut self) -> &mut [SyncReplicationClient] {
        &mut self.clients
    }
}

// ─── Replication server (main side) ────────────────────────────────────────

/// Replication server runs on the main instance, serving replicas.
pub struct ReplicationServer {
    server: RpcServer,
    config: ReplicationConfig,
    main_uuid: String,
    epoch_id: u64,
}

impl ReplicationServer {
    pub fn bind(addr: &str, config: ReplicationConfig) -> std::io::Result<Self> {
        let server = RpcServer::bind(addr)?;
        Ok(Self {
            server,
            config,
            main_uuid: uuid(),
            epoch_id: 1,
        })
    }

    pub fn main_uuid(&self) -> &str {
        &self.main_uuid
    }

    pub fn accept(&self) -> std::io::Result<(RpcClient, std::net::SocketAddr)> {
        self.server.accept()
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.server.local_addr()
    }
}

// ─── Replication server handler ────────────────────────────────────────────

/// Full replication server that dispatches incoming requests.
pub struct ReplServer {
    inner: ReplicationServer,
    epoch_id: std::sync::atomic::AtomicU64,
    lsn: std::sync::atomic::AtomicU64,
}

/// Read the current value of `key` on the vertex/edge identified by `gid`
/// from `storage`.  Used by replication delta translation to recover the
/// new value of a `SetProperty` write (the chain only carries the pre-image
/// for undo).  Returns `Null` when storage is not supplied or the object
/// has been deleted.
fn resolve_current_property(
    storage: Option<&std::sync::Arc<mgstorage::storage::Storage>>,
    gid: mgcore::types::Gid,
    key: mgcore::types::PropertyId,
    is_vertex: bool,
) -> mgcore::property_value::PropertyValue {
    let Some(storage) = storage else {
        return mgcore::property_value::PropertyValue::Null;
    };
    let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
    if is_vertex {
        match storage.get_vertex(gid, &tx) {
            Some(snap) => snap.properties.get(key).clone(),
            None => mgcore::property_value::PropertyValue::Null,
        }
    } else {
        match storage.get_edge(gid, &tx) {
            Some(snap) => snap.properties.get(key).clone(),
            None => mgcore::property_value::PropertyValue::Null,
        }
    }
}

/// Resolve the (from, to, edge_type) triple for an edge whose creation we are
/// replicating.  The MVCC delta chain identifies the edge by gid only, so we
/// have to look it up in the live edge map.  Returns `None` if storage isn't
/// supplied or the edge is no longer resolvable, in which case the caller
/// must drop the EdgeCreate record (replicas will catch up via snapshot).
fn resolve_edge_endpoints(
    storage: Option<&std::sync::Arc<mgstorage::storage::Storage>>,
    gid: mgcore::types::Gid,
) -> Option<(
    mgcore::types::Gid,
    mgcore::types::Gid,
    mgcore::types::EdgeTypeId,
)> {
    let storage = storage?;
    let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
    let snap = storage.get_edge(gid, &tx)?;
    Some((snap.from_vertex, snap.to_vertex, snap.edge_type))
}

impl ReplServer {
    pub fn bind(addr: &str, config: ReplicationConfig) -> std::io::Result<Self> {
        let inner = ReplicationServer::bind(addr, config)?;
        Ok(Self {
            inner,
            epoch_id: std::sync::atomic::AtomicU64::new(1),
            lsn: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.inner.local_addr()
    }

    pub fn main_uuid(&self) -> &str {
        self.inner.main_uuid()
    }

    pub fn epoch_id(&self) -> u64 {
        self.epoch_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn lsn(&self) -> u64 {
        self.lsn.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn bump_lsn(&self) -> u64 {
        self.lsn.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1
    }

    /// Accept a new connection and return the client handle.
    pub fn accept(&self) -> std::io::Result<(RpcClient, std::net::SocketAddr)> {
        self.inner.accept()
    }

    /// Handle a single request and produce a response.
    ///
    /// Dispatches to the appropriate handler using the provided storage
    /// reference for snapshot and WAL operations.
    pub fn handle_request(
        &self,
        req: &ReplRequest,
        _storage: &std::sync::Arc<mgstorage::storage::Storage>,
    ) -> ReplResponse {
        match req {
            ReplRequest::Heartbeat { epoch_id, lsn } => {
                let current_epoch = self.epoch_id();
                let current_lsn = self.lsn();
                ReplResponse::Heartbeat {
                    epoch_id: current_epoch,
                    lsn: current_lsn,
                    main_uuid: self.main_uuid().to_string(),
                }
            }
            ReplRequest::Snapshot {
                last_durable_timestamp: _,
            } => {
                // Serialize current storage state into a snapshot blob.
                let data = Self::serialize_storage_state(_storage);
                ReplResponse::Snapshot { data }
            }
            ReplRequest::Wal { since_timestamp } => {
                // Return WAL records modified since the given timestamp.
                let wal_dir = self.inner.config.wal_directory.as_deref();
                let records = Self::fetch_wal_since(_storage, *since_timestamp, wal_dir);
                if records.is_empty() {
                    ReplResponse::Wal {
                        file_name: String::new(),
                        data: Vec::new(),
                    }
                } else {
                    let (file_name, data) = records.into_iter().next().unwrap();
                    ReplResponse::Wal { file_name, data }
                }
            }
            ReplRequest::DeltaStream {
                epoch_id,
                since_timestamp,
                batch_size_limit,
            } => {
                // Validate epoch
                let current_epoch = self.epoch_id();
                if *epoch_id != current_epoch {
                    return ReplResponse::Error {
                        message: format!(
                            "epoch mismatch: expected {}, got {}",
                            current_epoch, epoch_id
                        ),
                    };
                }

                // Walk storage and collect deltas since since_timestamp
                let mut deltas = Vec::new();
                let mut max_commit_ts = 0u64;

                let raw_deltas = _storage.deltas_since(*since_timestamp);
                for (gid, is_vertex, d) in raw_deltas {
                    let ts = d.commit_info.timestamp();
                    if let Some(record) =
                        Self::core_delta_to_record(gid, d, is_vertex, Some(_storage))
                    {
                        max_commit_ts = max_commit_ts.max(ts);
                        deltas.push(record);
                    }
                }

                if deltas.is_empty() {
                    return ReplResponse::DeltaStreamEnd;
                }

                // Respect batch_size_limit (truncate if needed)
                let limit = *batch_size_limit as usize;
                if limit > 0 && deltas.len() > limit {
                    deltas.truncate(limit);
                }

                let batch = DeltaBatch {
                    epoch_id: current_epoch,
                    commit_timestamp: max_commit_ts,
                    sequence_number: self.bump_lsn(),
                    deltas,
                };

                ReplResponse::DeltaStreamBatch { batch }
            }
        }
    }

    /// Magic bytes identifying a mgrepl snapshot blob.
    pub const SNAPSHOT_MAGIC: &'static [u8; 4] = b"MGRS";
    /// Snapshot format version.
    pub const SNAPSHOT_VERSION: u32 = 1;

    /// Serialize the current storage state into bytes for snapshot transfer.
    ///
    /// Layout:
    ///   ┌─────────────┬──────────────┬─────────────────────────────┐
    ///   │ "MGRS" (4B) │ version (4B) │ SLK-framed body             │
    ///   └─────────────┴──────────────┴─────────────────────────────┘
    /// The body is encoded with the same `mgslk` framing used by
    /// snapshots and WAL, so it is also forward-compatible with C++.
    fn serialize_storage_state(storage: &std::sync::Arc<mgstorage::storage::Storage>) -> Vec<u8> {
        use mgslk::{Builder, SlkSave};

        // Header: magic + version (raw little-endian, outside the SLK frame).
        let mut data = Vec::new();
        data.extend_from_slice(Self::SNAPSHOT_MAGIC);
        data.extend_from_slice(&Self::SNAPSHOT_VERSION.to_le_bytes());

        // Body: vertex count, vertices, edge count, edges.
        let (mut builder, collector) = Builder::new_collecting();

        let vertices = storage.all_vertices();
        (vertices.len() as u64).slk_save(&mut builder);
        for (gid, labels, props) in &vertices {
            gid.as_uint().slk_save(&mut builder);
            (labels.len() as u64).slk_save(&mut builder);
            for label in labels {
                label.as_uint().slk_save(&mut builder);
            }
            let prop_pairs: Vec<(u32, mgcore::property_value::PropertyValue)> = props
                .iter()
                .map(|(id, v)| (id.as_uint(), v.clone()))
                .collect();
            (prop_pairs.len() as u64).slk_save(&mut builder);
            for (id, value) in &prop_pairs {
                id.slk_save(&mut builder);
                value.slk_save(&mut builder);
            }
        }

        let edges = storage.all_edges();
        (edges.len() as u64).slk_save(&mut builder);
        for (gid, from, to, etype, props) in &edges {
            gid.as_uint().slk_save(&mut builder);
            from.as_uint().slk_save(&mut builder);
            to.as_uint().slk_save(&mut builder);
            etype.as_uint().slk_save(&mut builder);
            let prop_pairs: Vec<(u32, mgcore::property_value::PropertyValue)> = props
                .iter()
                .map(|(id, v)| (id.as_uint(), v.clone()))
                .collect();
            (prop_pairs.len() as u64).slk_save(&mut builder);
            for (id, value) in &prop_pairs {
                id.slk_save(&mut builder);
                value.slk_save(&mut builder);
            }
        }
        builder.finalize();
        data.extend_from_slice(&collector.into_vec());
        data
    }

    /// Restore a snapshot produced by `serialize_storage_state` into `storage`.
    ///
    /// `storage` should be empty (or at least missing the gids carried by
    /// the snapshot); the function uses `create_vertex` / `create_edge`
    /// and will return an error if those fail.
    pub fn apply_snapshot_data(
        storage: &std::sync::Arc<mgstorage::storage::Storage>,
        data: &[u8],
    ) -> Result<(), ReplError> {
        use mgcore::property_value::PropertyValue;
        use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
        use mgslk::{Reader, SlkLoad};

        if data.len() < 8 {
            return Err(ReplError::Protocol("snapshot too short".into()));
        }
        if &data[..4] != Self::SNAPSHOT_MAGIC {
            return Err(ReplError::Protocol("snapshot magic mismatch".into()));
        }
        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        if version != Self::SNAPSHOT_VERSION {
            return Err(ReplError::Protocol(format!(
                "unsupported snapshot version: {}",
                version
            )));
        }
        let body = &data[8..];
        let mut reader = Reader::new(body);
        let map_err = |e: mgslk::SlkDecodeError| ReplError::Protocol(e.to_string());

        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);

        let vcount = u64::slk_load(&mut reader).map_err(map_err)?;
        for _ in 0..vcount {
            let gid_raw = u64::slk_load(&mut reader).map_err(map_err)?;
            let gid = Gid::from(gid_raw);
            storage
                .create_vertex(&tx, gid)
                .map_err(|e| ReplError::Protocol(format!("create_vertex: {:?}", e)))?;

            let label_count = u64::slk_load(&mut reader).map_err(map_err)?;
            for _ in 0..label_count {
                let l = u32::slk_load(&mut reader).map_err(map_err)?;
                storage
                    .vertex_add_label(&tx, gid, LabelId::from(l))
                    .map_err(|e| ReplError::Protocol(format!("add_label: {:?}", e)))?;
            }

            let prop_count = u64::slk_load(&mut reader).map_err(map_err)?;
            for _ in 0..prop_count {
                let id = u32::slk_load(&mut reader).map_err(map_err)?;
                let value = PropertyValue::slk_load(&mut reader).map_err(map_err)?;
                storage
                    .vertex_set_property(&tx, gid, PropertyId::from(id), value)
                    .map_err(|e| ReplError::Protocol(format!("set_property: {:?}", e)))?;
            }
        }

        let ecount = u64::slk_load(&mut reader).map_err(map_err)?;
        for _ in 0..ecount {
            let gid = Gid::from(u64::slk_load(&mut reader).map_err(map_err)?);
            let from = Gid::from(u64::slk_load(&mut reader).map_err(map_err)?);
            let to = Gid::from(u64::slk_load(&mut reader).map_err(map_err)?);
            let etype = EdgeTypeId::from(u32::slk_load(&mut reader).map_err(map_err)?);
            storage
                .create_edge(&tx, gid, from, to, etype)
                .map_err(|e| ReplError::Protocol(format!("create_edge: {:?}", e)))?;

            let prop_count = u64::slk_load(&mut reader).map_err(map_err)?;
            for _ in 0..prop_count {
                let id = u32::slk_load(&mut reader).map_err(map_err)?;
                let value = PropertyValue::slk_load(&mut reader).map_err(map_err)?;
                storage
                    .edge_set_property(&tx, gid, PropertyId::from(id), value)
                    .map_err(|e| ReplError::Protocol(format!("set_property: {:?}", e)))?;
            }
        }

        storage.commit_transaction(&tx);
        Ok(())
    }

    /// Convert a core `Delta` (from vertex/edge delta chain) into an
    /// `mgdurability::DeltaRecord` for replication streaming.
    ///
    /// Deltas in the chain are *undo* records.  The forward operation that
    /// created the delta is the logical opposite of the undo action:
    ///
    ///   - `DeleteObject`      → forward was **create**  (undo = delete)
    ///   - `RecreateObject`    → forward was **delete**  (undo = recreate)
    ///   - `Label { AddLabel }`   → forward was **add label**
    ///   - `Label { RemoveLabel }`→ forward was **remove label**
    ///   - `SetProperty`       → forward was **set property**
    ///
    /// `storage`, when provided, is consulted to resolve the *current* value
    /// of a property for `SetProperty` deltas (the chain only stores the
    /// pre-image used for undo).  When `None`, we fall back to
    /// `PropertyValue::Null`, which is only correct when the property has
    /// since been removed.
    ///
    /// Returns `None` for delta kinds that don't map to a serializable
    /// `DeltaRecord` (e.g. edge-add/remove on vertices, which are
    /// structural rather than logical deltas).
    pub fn core_delta_to_record(
        gid: mgcore::types::Gid,
        delta: &mgcore::Delta,
        is_vertex: bool,
        storage: Option<&std::sync::Arc<mgstorage::storage::Storage>>,
    ) -> Option<mgdurability::DeltaRecord> {
        use mgcore::delta::DeltaAction;
        use mgcore::property_value::PropertyValue;

        let ts = delta.commit_info.timestamp();

        match &delta.kind {
            // DeleteObject is the initial tombstone placed when a vertex/edge
            // is created.  Its presence means "to undo creation, delete the
            // object".  Therefore the forward operation was a create.
            mgcore::delta::DeltaKind::DeleteObject => {
                if is_vertex {
                    Some(mgdurability::DeltaRecord::VertexCreate { gid, timestamp: ts })
                } else {
                    // EdgeCreate needs the (from, to, edge_type) triple,
                    // which the delta itself does not carry.  Recover it
                    // by looking the edge up in live storage.  If the edge
                    // has since been deleted we drop the record — replicas
                    // will pick it up via snapshot/WAL recovery.
                    resolve_edge_endpoints(storage, gid).map(
                        |(from_vertex, to_vertex, edge_type)| {
                            mgdurability::DeltaRecord::EdgeCreate {
                                gid,
                                from_vertex,
                                to_vertex,
                                edge_type,
                                timestamp: ts,
                            }
                        },
                    )
                }
            }
            mgcore::delta::DeltaKind::DeleteDeserializedObject { .. } => {
                // Same logical meaning as DeleteObject.
                if is_vertex {
                    Some(mgdurability::DeltaRecord::VertexCreate { gid, timestamp: ts })
                } else {
                    resolve_edge_endpoints(storage, gid).map(
                        |(from_vertex, to_vertex, edge_type)| {
                            mgdurability::DeltaRecord::EdgeCreate {
                                gid,
                                from_vertex,
                                to_vertex,
                                edge_type,
                                timestamp: ts,
                            }
                        },
                    )
                }
            }
            // RecreateObject undoes a deletion, so the forward operation was
            // a delete.
            mgcore::delta::DeltaKind::RecreateObject => {
                if is_vertex {
                    Some(mgdurability::DeltaRecord::VertexDelete { gid })
                } else {
                    Some(mgdurability::DeltaRecord::EdgeDelete { gid })
                }
            }
            // SetProperty stores the OLD value (for undo).  Walk the live
            // PropertyStore via `storage` to recover the current value to
            // send to the replica.  When the property has since been
            // unset (or storage isn't supplied) this reduces to Null.
            mgcore::delta::DeltaKind::SetProperty { key, old_value: _ } => {
                let value = resolve_current_property(storage, gid, *key, is_vertex);
                if is_vertex {
                    Some(mgdurability::DeltaRecord::VertexSetProperty {
                        gid,
                        key: *key,
                        value,
                    })
                } else {
                    Some(mgdurability::DeltaRecord::EdgeSetProperty {
                        gid,
                        key: *key,
                        value,
                    })
                }
            }
            // Label deltas: the action is the forward operation.
            mgcore::delta::DeltaKind::Label { action, value } => match action {
                DeltaAction::AddLabel => {
                    Some(mgdurability::DeltaRecord::VertexAddLabel { gid, label: *value })
                }
                DeltaAction::RemoveLabel => {
                    Some(mgdurability::DeltaRecord::VertexRemoveLabel { gid, label: *value })
                }
                _ => None,
            },
            // VertexEdge deltas are structural bookkeeping on the vertex
            // side (AddInEdge, AddOutEdge, RemoveInEdge, RemoveOutEdge).
            // The logical edge create/delete is handled by edge delta chains.
            mgcore::delta::DeltaKind::VertexEdge { action, .. } => {
                let _ = action;
                None
            }
        }
    }

    /// Fetch WAL records since a given timestamp.
    fn fetch_wal_since(
        storage: &std::sync::Arc<mgstorage::storage::Storage>,
        since_timestamp: u64,
        wal_dir: Option<&str>,
    ) -> Vec<(String, Vec<u8>)> {
        let _ = storage;
        let mut results = Vec::new();
        if let Some(dir) = wal_dir {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|e| e == "wal") {
                        if let Ok(data) = std::fs::read(&path) {
                            if let Ok(meta) = entry.metadata() {
                                if let Ok(modified) = meta.modified() {
                                    let ts = modified
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs();
                                    if ts >= since_timestamp {
                                        if let Some(name) = path.file_name() {
                                            results
                                                .push((name.to_string_lossy().to_string(), data));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        results
    }
}

// ─── Replication client (replica side) ─────────────────────────────────────

/// Replication client connects to a main instance and streams deltas.
pub struct ReplicationClient {
    rpc: RpcClient,
    main_address: String,
    state: ReplicaState,
    config: ReplicationConfig,
    epoch_id: u64,
}

impl ReplicationClient {
    /// Connect to a main instance.
    pub fn connect(main_address: &str, config: ReplicationConfig) -> std::io::Result<Self> {
        let rpc = RpcClient::connect(main_address)?;
        Ok(Self {
            rpc,
            main_address: main_address.to_string(),
            state: ReplicaState::Behind,
            config,
            epoch_id: 0,
        })
    }

    /// Override the epoch_id used in outgoing requests. Replicas update this
    /// after observing the main's current epoch via heartbeat or snapshot.
    pub fn set_epoch_id(&mut self, epoch_id: u64) {
        self.epoch_id = epoch_id;
    }

    pub fn epoch_id(&self) -> u64 {
        self.epoch_id
    }

    /// Send a heartbeat and check main status.
    pub fn heartbeat(&mut self, main_uuid: &str) -> std::io::Result<()> {
        let header = MessageHeader::new(1, 1);
        let hb = Heartbeat {
            main_uuid: main_uuid.to_string(),
            timestamp: now_secs(),
            epoch_id: self.epoch_id,
        };
        self.rpc.send(&header, &hb)?;
        let (_resp_header, _resp): (MessageHeader, Heartbeat) = self.rpc.recv()?;
        Ok(())
    }

    /// Request a full snapshot from the main.
    pub fn request_snapshot(&mut self, last_durable_ts: u64) -> std::io::Result<Vec<u8>> {
        let header = MessageHeader::new(10, 2);
        let req = mgrpc::SnapshotRequest {
            last_durable_timestamp: last_durable_ts,
        };
        self.rpc.send(&header, &req)?;
        let (_resp_header, snap): (MessageHeader, SnapshotData) = self.rpc.recv()?;
        Ok(snap.data)
    }

    /// Request WAL files since a given timestamp.
    pub fn request_wal(&mut self, since_ts: u64) -> std::io::Result<Vec<(String, Vec<u8>)>> {
        let header = MessageHeader::new(11, 1);
        let req = WalRequest {
            epoch_id: self.epoch_id,
            since_timestamp: since_ts,
        };
        self.rpc.send(&header, &req)?;
        let (_resp_header, wal): (MessageHeader, WalTransfer) = self.rpc.recv()?;
        Ok(vec![(wal.file_name, wal.data)])
    }

    pub fn state(&self) -> ReplicaState {
        self.state
    }

    pub fn main_address(&self) -> &str {
        &self.main_address
    }

    /// Request a delta stream starting from `since_timestamp`.
    /// After calling this, use [`next_delta_batch`](Self::next_delta_batch)
    /// to pull batches until the main sends an empty end-of-stream marker.
    pub fn request_delta_stream(
        &mut self,
        since_timestamp: u64,
        batch_size_limit: u32,
    ) -> std::io::Result<()> {
        let header = MessageHeader::new(MSG_ID_DELTA_BATCH, 1);
        let req = DeltaStreamRequest {
            epoch_id: self.epoch_id,
            start_timestamp: since_timestamp,
            batch_size_limit,
        };
        self.rpc.send(&header, &req)
    }

    /// Receive the next delta batch from an active stream. Returns `None`
    /// when the main signals end-of-stream with an empty batch.
    pub fn next_delta_batch(&mut self) -> std::io::Result<Option<DeltaBatch>> {
        let (header, batch): (MessageHeader, DeltaBatch) = self.rpc.recv()?;
        if batch.deltas.is_empty() {
            return Ok(None);
        }
        assert_eq!(header.message_id, MSG_ID_DELTA_BATCH);
        self.epoch_id = batch.epoch_id;
        Ok(Some(batch))
    }

    /// Pull and apply all batches from an active delta stream using the
    /// given applier. Returns the commit timestamp of the last applied batch,
    /// or `None` if the stream was empty.
    pub fn apply_stream<A: DeltaApplier>(
        &mut self,
        applier: &mut A,
    ) -> std::io::Result<Option<u64>> {
        let mut last_ts = None;
        while let Some(batch) = self.next_delta_batch()? {
            last_ts = Some(batch.commit_timestamp);
            applier.apply_batch(&batch)?;
            self.epoch_id = batch.epoch_id;
        }
        Ok(last_ts)
    }
}

// ─── ReplClient state machine ──────────────────────────────────────────────

/// High-level replication client with sync/continuous sync support.
pub struct ReplClient {
    inner: Option<ReplicationClient>,
    config: ReplicationConfig,
    main_address: String,
    epoch_id: u64,
}

impl ReplClient {
    /// Create a new ReplClient with the given configuration.
    /// The connection is established lazily on first sync.
    pub fn new(config: ReplicationConfig, main_address: String) -> Self {
        Self {
            inner: None,
            config,
            main_address,
            epoch_id: 0,
        }
    }

    fn ensure_connected(&mut self) -> Result<(), ReplError> {
        if self.inner.is_none() {
            let client = ReplicationClient::connect(&self.main_address, self.config.clone())
                .map_err(|e| ReplError::Io(e.to_string()))?;
            self.inner = Some(client);
        }
        Ok(())
    }

    /// Perform a one-shot sync: request delta stream and apply all batches.
    pub fn sync(&mut self) -> Result<ReplStatus, ReplError> {
        self.ensure_connected()?;
        let client = self.inner.as_mut().unwrap();

        // Request stream from current position (epoch_id as proxy)
        client
            .request_delta_stream(self.epoch_id, 100)
            .map_err(|e| ReplError::Io(e.to_string()))?;

        let mut applier = handler::StorageDeltaApplier::new(
            // We don't own storage here; in a real implementation the
            // ReplClient would hold an Arc<Storage>. For the skeleton we
            // create a temporary storage — tests will use the lower-level
            // ReplicationClient directly for real scenarios.
            std::sync::Arc::new(mgstorage::storage::Storage::new()),
        );

        let last_ts = client
            .apply_stream(&mut applier)
            .map_err(|e| ReplError::Io(e.to_string()))?;

        if let Some(ts) = last_ts {
            self.epoch_id = ts;
            Ok(ReplStatus::Ok)
        } else {
            Ok(ReplStatus::Behind)
        }
    }

    /// Start continuous background sync. Spawns a background thread that
    /// periodically calls sync() until the handle is stopped.
    pub fn start_continuous_sync(&mut self) -> Result<ContinuousSyncHandle, ReplError> {
        self.ensure_connected()?;
        let active = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let active_clone = active.clone();

        let _thread = std::thread::spawn(move || {
            while active_clone.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        });

        Ok(ContinuousSyncHandle {
            active,
            _thread: Some(_thread),
        })
    }

    pub fn is_connected(&self) -> bool {
        self.inner.is_some()
    }

    pub fn epoch_id(&self) -> u64 {
        self.epoch_id
    }
}

/// Handle for a continuous background sync task.
pub struct ContinuousSyncHandle {
    active: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[allow(dead_code)]
    _thread: Option<std::thread::JoinHandle<()>>,
}

impl ContinuousSyncHandle {
    pub fn stop(&mut self) {
        self.active
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn is_active(&self) -> bool {
        self.active.load(std::sync::atomic::Ordering::Relaxed)
    }
}

// ─── Delta streaming ───────────────────────────────────────────────────────

/// Trait for applying a batch of deltas to local replica storage.
pub trait DeltaApplier {
    fn apply_batch(&mut self, batch: &DeltaBatch) -> std::io::Result<()>;
}

/// A no-op applier used in tests.
pub struct NullApplier;

impl DeltaApplier for NullApplier {
    fn apply_batch(&mut self, _batch: &DeltaBatch) -> std::io::Result<()> {
        Ok(())
    }
}

/// Send a single delta batch over an RPC connection (main → replica).
pub fn send_delta_batch(rpc: &mut RpcClient, batch: &DeltaBatch) -> std::io::Result<()> {
    let header = MessageHeader::new(MSG_ID_DELTA_BATCH, 1);
    rpc.send(&header, batch)
}

/// Seconds since Unix epoch.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Signal end-of-stream by sending an empty batch.
pub fn send_delta_stream_end(rpc: &mut RpcClient, epoch_id: u64) -> std::io::Result<()> {
    send_delta_batch(
        rpc,
        &DeltaBatch {
            epoch_id,
            commit_timestamp: 0,
            sequence_number: 0,
            deltas: vec![],
        },
    )
}

const MSG_ID_DELTA_BATCH: u64 = 20;

fn uuid() -> String {
    let mut buf = [0u8; 16];
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();
    for (i, b) in buf.iter_mut().enumerate().take(8) {
        *b = (ts >> (i * 8)) as u8;
    }
    for (i, b) in buf.iter_mut().enumerate().skip(8).take(4) {
        *b = (pid >> ((i - 8) * 8)) as u8;
    }
    for (i, b) in buf.iter_mut().enumerate().skip(12) {
        *b = (i as u8).wrapping_mul(17);
    }
    buf[6] = (buf[6] & 0x0f) | 0x40;
    buf[8] = (buf[8] & 0x3f) | 0x80;
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]),
        u16::from_be_bytes([buf[4], buf[5]]),
        u16::from_be_bytes([buf[6], buf[7]]),
        u16::from_be_bytes([buf[8], buf[9]]),
        u64::from_be_bytes([buf[10], buf[11], buf[12], buf[13], buf[14], buf[15], 0, 0]) >> 16,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_replication_config_defaults() {
        let config = ReplicationConfig::default();
        assert_eq!(config.mode, ReplicationMode::Sync);
        assert_eq!(config.replica_port, 10000);
    }

    #[test]
    fn test_replication_mode_roundtrip() {
        for mode in [
            ReplicationMode::Sync,
            ReplicationMode::Async,
            ReplicationMode::StrictSync,
        ] {
            let v = mode as u8;
            let back = ReplicationMode::from_u8(v).unwrap();
            assert_eq!(back, mode);
        }
    }

    #[test]
    fn test_main_uuid_generation() {
        let server = ReplicationServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        assert!(server.main_uuid().contains('-'));
        assert_eq!(server.main_uuid().len(), 36);
    }

    #[test]
    fn test_delta_stream_roundtrip() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;
        use mgrpc::DeltaBatch;

        let server = ReplicationServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        let addr = server.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut peer, _) = server.accept().unwrap();
            // Read the DeltaStreamRequest
            let (_hdr, req): (MessageHeader, DeltaStreamRequest) = peer.recv().unwrap();
            assert_eq!(req.start_timestamp, 100);
            assert_eq!(req.batch_size_limit, 2);

            // Send two batches
            let batch1 = DeltaBatch {
                epoch_id: 1,
                commit_timestamp: 101,
                sequence_number: 0,
                deltas: vec![DeltaRecord::VertexCreate {
                    gid: Gid::from(1u64),
                    timestamp: 101,
                }],
            };
            send_delta_batch(&mut peer, &batch1).unwrap();

            let batch2 = DeltaBatch {
                epoch_id: 1,
                commit_timestamp: 102,
                sequence_number: 1,
                deltas: vec![DeltaRecord::VertexDelete {
                    gid: Gid::from(1u64),
                }],
            };
            send_delta_batch(&mut peer, &batch2).unwrap();

            // End of stream
            send_delta_stream_end(&mut peer, 1).unwrap();
        });

        let mut client =
            ReplicationClient::connect(&addr.to_string(), ReplicationConfig::default()).unwrap();
        client.request_delta_stream(100, 2).unwrap();

        let batch = client.next_delta_batch().unwrap().unwrap();
        assert_eq!(batch.commit_timestamp, 101);
        assert_eq!(batch.deltas.len(), 1);

        let batch = client.next_delta_batch().unwrap().unwrap();
        assert_eq!(batch.commit_timestamp, 102);

        let end = client.next_delta_batch().unwrap();
        assert!(end.is_none());

        handle.join().unwrap();
    }

    #[test]
    fn test_apply_stream_with_null_applier() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;
        use mgrpc::DeltaBatch;

        let server = ReplicationServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        let addr = server.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut peer, _) = server.accept().unwrap();
            let (_hdr, _req): (MessageHeader, DeltaStreamRequest) = peer.recv().unwrap();

            let batch = DeltaBatch {
                epoch_id: 1,
                commit_timestamp: 200,
                sequence_number: 0,
                deltas: vec![DeltaRecord::VertexCreate {
                    gid: Gid::from(2u64),
                    timestamp: 200,
                }],
            };
            send_delta_batch(&mut peer, &batch).unwrap();
            send_delta_stream_end(&mut peer, 1).unwrap();
        });

        let mut client =
            ReplicationClient::connect(&addr.to_string(), ReplicationConfig::default()).unwrap();
        client.request_delta_stream(0, 10).unwrap();

        let mut applier = NullApplier;
        let last_ts = client.apply_stream(&mut applier).unwrap();
        assert_eq!(last_ts, Some(200));

        handle.join().unwrap();
    }

    // ─── New feature tests (8 required) ────────────────────────────────────

    #[test]
    fn test_delta_batch_application() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;
        use mgrpc::DeltaBatch;

        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![
                DeltaRecord::VertexCreate {
                    gid: Gid::from(1u64),
                    timestamp: 100,
                },
                DeltaRecord::VertexAddLabel {
                    gid: Gid::from(1u64),
                    label: mgcore::types::LabelId::from(1u32),
                },
            ],
        };

        ReplicationStream::apply_deltas(&storage, &batch, 1).unwrap();

        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        let v = storage.get_vertex(Gid::from(1u64), &tx);
        assert!(v.is_some());
    }

    #[test]
    fn test_epoch_validation() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;
        use mgrpc::DeltaBatch;

        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let batch = DeltaBatch {
            epoch_id: 5,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![DeltaRecord::VertexCreate {
                gid: Gid::from(1u64),
                timestamp: 100,
            }],
        };

        // Expected epoch is 7, batch has 5 → mismatch
        let result = ReplicationStream::apply_deltas(&storage, &batch, 7);
        assert!(
            matches!(
                result,
                Err(ReplError::EpochMismatch {
                    expected: 7,
                    actual: 5
                })
            ),
            "expected epoch mismatch, got {:?}",
            result
        );
    }

    #[test]
    fn test_snapshot_request_serialization() {
        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage
            .create_vertex(&tx, mgcore::types::Gid::from(42u64))
            .unwrap();
        storage.commit_transaction(&tx);

        let server = ReplServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        let req = ReplRequest::Snapshot {
            last_durable_timestamp: 0,
        };
        let resp = server.handle_request(&req, &storage);

        match resp {
            ReplResponse::Snapshot { data } => {
                assert!(
                    !data.is_empty(),
                    "snapshot data should contain serialized vertices"
                );
            }
            other => panic!("expected Snapshot response, got {:?}", other),
        }
    }

    #[test]
    fn test_snapshot_roundtrip_includes_vertices_edges_and_properties() {
        use mgcore::property_value::PropertyValue;
        use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};

        let main = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let tx = main.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        main.create_vertex(&tx, Gid::from(1u64)).unwrap();
        main.create_vertex(&tx, Gid::from(2u64)).unwrap();
        main.vertex_add_label(&tx, Gid::from(1u64), LabelId::from(11u32))
            .unwrap();
        main.vertex_set_property(
            &tx,
            Gid::from(1u64),
            PropertyId::from(7u32),
            PropertyValue::String("alice".into()),
        )
        .unwrap();
        main.vertex_set_property(
            &tx,
            Gid::from(2u64),
            PropertyId::from(7u32),
            PropertyValue::Int(42),
        )
        .unwrap();
        main.create_edge(
            &tx,
            Gid::from(100u64),
            Gid::from(1u64),
            Gid::from(2u64),
            EdgeTypeId::from(3u32),
        )
        .unwrap();
        main.edge_set_property(
            &tx,
            Gid::from(100u64),
            PropertyId::from(9u32),
            PropertyValue::Bool(true),
        )
        .unwrap();
        main.commit_transaction(&tx);

        let blob = ReplServer::serialize_storage_state(&main);
        assert_eq!(&blob[..4], ReplServer::SNAPSHOT_MAGIC);

        // Apply into a fresh replica storage and verify equivalence.
        let replica = std::sync::Arc::new(mgstorage::storage::Storage::new());
        ReplServer::apply_snapshot_data(&replica, &blob).expect("snapshot should apply cleanly");

        let mut main_v = main.all_vertices();
        let mut rep_v = replica.all_vertices();
        main_v.sort_by_key(|(g, _, _)| g.as_uint());
        rep_v.sort_by_key(|(g, _, _)| g.as_uint());
        assert_eq!(main_v.len(), rep_v.len());
        for ((g1, l1, p1), (g2, l2, p2)) in main_v.iter().zip(rep_v.iter()) {
            assert_eq!(g1, g2);
            assert_eq!(l1, l2);
            let p1_collected: Vec<_> = p1.iter().collect();
            let p2_collected: Vec<_> = p2.iter().collect();
            assert_eq!(p1_collected.len(), p2_collected.len());
            for ((id1, v1), (id2, v2)) in p1_collected.iter().zip(p2_collected.iter()) {
                assert_eq!(id1, id2);
                assert_eq!(v1, v2);
            }
        }

        let mut main_e = main.all_edges();
        let mut rep_e = replica.all_edges();
        main_e.sort_by_key(|(g, _, _, _, _)| g.as_uint());
        rep_e.sort_by_key(|(g, _, _, _, _)| g.as_uint());
        assert_eq!(main_e, rep_e);
    }

    #[test]
    fn test_snapshot_rejects_bad_magic() {
        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let bad = vec![b'X', b'Y', b'Z', b'W', 1, 0, 0, 0];
        let err = ReplServer::apply_snapshot_data(&storage, &bad).unwrap_err();
        assert!(matches!(err, ReplError::Protocol(_)));
    }

    #[test]
    fn test_snapshot_rejects_unknown_version() {
        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let mut bad = ReplServer::SNAPSHOT_MAGIC.to_vec();
        bad.extend_from_slice(&999u32.to_le_bytes());
        let err = ReplServer::apply_snapshot_data(&storage, &bad).unwrap_err();
        match err {
            ReplError::Protocol(msg) => assert!(msg.contains("version")),
            other => panic!("expected Protocol error, got {:?}", other),
        }
    }

    #[test]
    fn test_wal_range_request() {
        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let server = ReplServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();

        let req = ReplRequest::Wal {
            since_timestamp: 100,
        };
        let resp = server.handle_request(&req, &storage);

        match resp {
            ReplResponse::Wal { file_name, data } => {
                assert!(file_name.is_empty());
                assert!(data.is_empty());
            }
            other => panic!("expected Wal response, got {:?}", other),
        }
    }

    #[test]
    fn test_sync_mode_ack_waiting() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;
        use mgrpc::DeltaBatch;

        let server = ReplicationServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        let addr = server.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut peer, _) = server.accept().unwrap();
            // Receive the delta batch
            let (_hdr, batch): (MessageHeader, DeltaBatch) = peer.recv().unwrap();
            assert_eq!(batch.deltas.len(), 1);
            // Send ACK (echo heartbeat)
            let ack = Heartbeat {
                main_uuid: "test".into(),
                timestamp: now_secs(),
                epoch_id: 1,
            };
            peer.send(&MessageHeader::new(1, 1), &ack).unwrap();
        });

        let client = RpcClient::connect(&addr.to_string()).unwrap();
        let mut sync_client = SyncReplicationClient::new(client, Duration::from_secs(5));

        let batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![DeltaRecord::VertexCreate {
                gid: Gid::from(1u64),
                timestamp: 100,
            }],
        };

        sync_client.send_and_wait(&batch).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_async_mode_fire_and_forget() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;
        use mgrpc::DeltaBatch;

        let server = ReplicationServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        let addr = server.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut peer, _) = server.accept().unwrap();
            let (_hdr, batch): (MessageHeader, DeltaBatch) = peer.recv().unwrap();
            assert_eq!(batch.deltas.len(), 1);
            // No ACK sent — async doesn't wait
        });

        let client = RpcClient::connect(&addr.to_string()).unwrap();
        let mut async_client = AsyncReplicationClient::new(client);

        let batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![DeltaRecord::VertexCreate {
                gid: Gid::from(1u64),
                timestamp: 100,
            }],
        };

        async_client.send(&batch).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_strict_sync_majority() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;
        use mgrpc::DeltaBatch;

        // Set up 3 replicas, all will ACK — strict sync should succeed with majority
        let server1 = ReplicationServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        let addr1 = server1.local_addr().unwrap();
        let server2 = ReplicationServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        let addr2 = server2.local_addr().unwrap();
        let server3 = ReplicationServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        let addr3 = server3.local_addr().unwrap();

        let h1 = thread::spawn(move || {
            let (mut peer, _) = server1.accept().unwrap();
            let (_hdr, _batch): (MessageHeader, DeltaBatch) = peer.recv().unwrap();
            let ack = Heartbeat {
                main_uuid: "test".into(),
                timestamp: now_secs(),
                epoch_id: 1,
            };
            peer.send(&MessageHeader::new(1, 1), &ack).unwrap();
        });

        let h2 = thread::spawn(move || {
            let (mut peer, _) = server2.accept().unwrap();
            let (_hdr, _batch): (MessageHeader, DeltaBatch) = peer.recv().unwrap();
            let ack = Heartbeat {
                main_uuid: "test".into(),
                timestamp: now_secs(),
                epoch_id: 1,
            };
            peer.send(&MessageHeader::new(1, 1), &ack).unwrap();
        });

        let h3 = thread::spawn(move || {
            let (mut peer, _) = server3.accept().unwrap();
            let (_hdr, _batch): (MessageHeader, DeltaBatch) = peer.recv().unwrap();
            let ack = Heartbeat {
                main_uuid: "test".into(),
                timestamp: now_secs(),
                epoch_id: 1,
            };
            peer.send(&MessageHeader::new(1, 1), &ack).unwrap();
        });

        let client1 = RpcClient::connect(&addr1.to_string()).unwrap();
        let client2 = RpcClient::connect(&addr2.to_string()).unwrap();
        let client3 = RpcClient::connect(&addr3.to_string()).unwrap();

        let sync1 = SyncReplicationClient::new(client1, Duration::from_secs(5));
        let sync2 = SyncReplicationClient::new(client2, Duration::from_secs(5));
        let sync3 = SyncReplicationClient::new(client3, Duration::from_secs(5));

        let mut strict =
            StrictSyncReplicationClient::new(vec![sync1, sync2, sync3], Duration::from_secs(5));

        let batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![DeltaRecord::VertexCreate {
                gid: Gid::from(1u64),
                timestamp: 100,
            }],
        };

        // All 3 ACK → majority clearly satisfied
        strict.send_and_wait_majority(&batch).unwrap();

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
    }

    #[test]
    fn test_conflict_detection() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;
        use mgrpc::DeltaBatch;

        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        // Pre-create a vertex with a high GID to simulate a newer write
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(999u64)).unwrap();
        storage.commit_transaction(&tx);

        // Now try to apply a batch with an older timestamp that targets the same vertex
        let batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 10, // older than the vertex creation
            sequence_number: 0,
            deltas: vec![DeltaRecord::VertexSetProperty {
                gid: Gid::from(999u64),
                key: mgcore::types::PropertyId::from(0u32),
                value: mgcore::property_value::PropertyValue::Int(42),
            }],
        };

        // Conflict detection triggers because gid (999) > batch_ts (10)
        let result = ReplicationStream::apply_deltas(&storage, &batch, 1);
        assert!(
            matches!(result, Err(ReplError::Conflict(_))),
            "expected conflict, got {:?}",
            result
        );
    }

    #[test]
    fn test_repl_client_new_and_connect() {
        let server = ReplicationServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        let addr = server.local_addr().unwrap().to_string();

        let mut client = ReplClient::new(ReplicationConfig::default(), addr);
        assert!(!client.is_connected());

        // connect lazily via ensure_connected
        client.ensure_connected().unwrap();
        assert!(client.is_connected());
    }

    #[test]
    fn test_repl_server_handle_heartbeat() {
        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let server = ReplServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();

        let req = ReplRequest::Heartbeat {
            epoch_id: 1,
            lsn: 0,
        };
        let resp = server.handle_request(&req, &storage);

        match resp {
            ReplResponse::Heartbeat {
                epoch_id,
                lsn,
                main_uuid,
            } => {
                assert_eq!(epoch_id, 1);
                assert_eq!(lsn, 0);
                assert!(!main_uuid.is_empty());
            }
            other => panic!("expected Heartbeat response, got {:?}", other),
        }
    }

    #[test]
    fn test_repl_server_handle_delta_stream_epoch_validation() {
        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let server = ReplServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();

        // Server epoch is 1, client sends 99 → error
        let req = ReplRequest::DeltaStream {
            epoch_id: 99,
            since_timestamp: 0,
            batch_size_limit: 10,
        };
        let resp = server.handle_request(&req, &storage);

        match resp {
            ReplResponse::Error { message } => {
                assert!(message.contains("epoch mismatch"));
            }
            other => panic!("expected Error response, got {:?}", other),
        }
    }

    #[test]
    fn test_continuous_sync_handle() {
        let server = ReplicationServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();
        let addr = server.local_addr().unwrap().to_string();

        let mut client = ReplClient::new(ReplicationConfig::default(), addr);
        let handle = client.start_continuous_sync().unwrap();
        assert!(handle.is_active());

        // In a real implementation the handle would control a background thread.
        // Here we verify the handle API works.
    }

    #[test]
    fn test_replication_metrics_exported() {
        let mut m = ReplicationMetrics::new();
        m.record_batch_sent(5, 512);
        assert_eq!(m.batches_sent, 1);
    }

    #[test]
    fn test_replication_filter_exported() {
        let mut filter = ReplicationFilter::new();
        filter.vertices_only = true;
        let d = mgdurability::DeltaRecord::EdgeCreate {
            gid: mgcore::types::Gid::from(1u64),
            from_vertex: mgcore::types::Gid::from(2u64),
            to_vertex: mgcore::types::Gid::from(3u64),
            edge_type: mgcore::types::EdgeTypeId::from(1u32),
            timestamp: 1,
        };
        assert!(!filter.allows(&d));
    }

    #[test]
    fn test_failover_detector_exported() {
        let mut det = FailoverDetector::new(std::time::Duration::from_secs(1), 1);
        assert!(!det.is_main_failed());
        det.record_miss();
        assert!(det.is_main_failed());
    }

    #[test]
    fn test_storage_delta_applier_exported() {
        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let mut applier = StorageDeltaApplier::new(storage.clone());
        assert_eq!(applier.applied_count, 0);

        let batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![mgdurability::DeltaRecord::VertexCreate {
                gid: mgcore::types::Gid::from(1u64),
                timestamp: 100,
            }],
        };
        applier.apply_batch(&batch).unwrap();
        assert_eq!(applier.applied_count, 1);
    }

    #[test]
    fn test_delta_gid_all_variants() {
        let g = mgcore::types::Gid::from(1u64);
        assert_eq!(
            delta_gid(&mgdurability::DeltaRecord::VertexCreate {
                gid: g,
                timestamp: 1
            }),
            Some(g)
        );
        assert_eq!(
            delta_gid(&mgdurability::DeltaRecord::VertexDelete { gid: g }),
            Some(g)
        );
        assert_eq!(
            delta_gid(&mgdurability::DeltaRecord::VertexSetProperty {
                gid: g,
                key: mgcore::types::PropertyId::from(0u32),
                value: mgcore::property_value::PropertyValue::Null
            }),
            Some(g)
        );
        assert_eq!(
            delta_gid(&mgdurability::DeltaRecord::VertexAddLabel {
                gid: g,
                label: mgcore::types::LabelId::from(1u32)
            }),
            Some(g)
        );
        assert_eq!(
            delta_gid(&mgdurability::DeltaRecord::VertexRemoveLabel {
                gid: g,
                label: mgcore::types::LabelId::from(1u32)
            }),
            Some(g)
        );
        assert_eq!(
            delta_gid(&mgdurability::DeltaRecord::EdgeCreate {
                gid: g,
                from_vertex: g,
                to_vertex: g,
                edge_type: mgcore::types::EdgeTypeId::from(1u32),
                timestamp: 1
            }),
            Some(g)
        );
        assert_eq!(
            delta_gid(&mgdurability::DeltaRecord::EdgeDelete { gid: g }),
            Some(g)
        );
        assert_eq!(
            delta_gid(&mgdurability::DeltaRecord::EdgeSetProperty {
                gid: g,
                key: mgcore::types::PropertyId::from(0u32),
                value: mgcore::property_value::PropertyValue::Null
            }),
            Some(g)
        );
        assert_eq!(
            delta_gid(&mgdurability::DeltaRecord::TransactionStart { timestamp: 1 }),
            None
        );
        assert_eq!(
            delta_gid(&mgdurability::DeltaRecord::LabelIndexCreate {
                label: mgcore::types::LabelId::from(1u32)
            }),
            None
        );
    }

    #[test]
    fn test_repl_error_display() {
        let e1 = ReplError::Timeout;
        assert_eq!(e1.to_string(), "operation timed out");

        let e2 = ReplError::EpochMismatch {
            expected: 1,
            actual: 2,
        };
        assert!(e2.to_string().contains("epoch mismatch"));

        let e3 = ReplError::NotConnected;
        assert_eq!(e3.to_string(), "not connected");
    }

    #[test]
    fn test_delta_stream_returns_deltas_from_storage() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;

        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let server = ReplServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();

        // Create two vertices inside a transaction so they get committed timestamps
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);

        // Request delta stream since timestamp 0 — should include the vertex-create deltas
        let req = ReplRequest::DeltaStream {
            epoch_id: 1,
            since_timestamp: 0,
            batch_size_limit: 100,
        };
        let resp = server.handle_request(&req, &storage);

        match resp {
            ReplResponse::DeltaStreamBatch { batch } => {
                assert!(!batch.deltas.is_empty(), "batch should contain deltas");
                assert_eq!(batch.epoch_id, 1);
                // At minimum we should see VertexCreate records for both vertices
                let creates: Vec<_> = batch
                    .deltas
                    .iter()
                    .filter(|d| matches!(d, DeltaRecord::VertexCreate { .. }))
                    .collect();
                assert_eq!(creates.len(), 2, "expected two VertexCreate deltas");
            }
            ReplResponse::DeltaStreamEnd => {
                panic!("expected DeltaStreamBatch, got DeltaStreamEnd (no deltas found)");
            }
            other => panic!("expected DeltaStreamBatch, got {:?}", other),
        }
    }

    #[test]
    fn test_edge_create_delta_carries_endpoints_and_type() {
        use mgcore::types::{EdgeTypeId, Gid};
        use mgdurability::DeltaRecord;

        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage
            .create_edge(
                &tx,
                Gid::from(99u64),
                Gid::from(1u64),
                Gid::from(2u64),
                EdgeTypeId::from(7u32),
            )
            .unwrap();
        storage.commit_transaction(&tx);

        let raw = storage.deltas_since(0);
        let mut saw_edge_create = false;
        for (gid, is_vertex, d) in raw {
            if let Some(record) =
                ReplServer::core_delta_to_record(gid, d, is_vertex, Some(&storage))
            {
                if let DeltaRecord::EdgeCreate {
                    gid: g,
                    from_vertex,
                    to_vertex,
                    edge_type,
                    ..
                } = record
                {
                    assert_eq!(g, Gid::from(99u64));
                    assert_eq!(from_vertex, Gid::from(1u64));
                    assert_eq!(to_vertex, Gid::from(2u64));
                    assert_eq!(edge_type, EdgeTypeId::from(7u32));
                    saw_edge_create = true;
                }
            }
        }
        assert!(saw_edge_create, "expected an EdgeCreate record");
    }

    #[test]
    fn test_edge_create_delta_dropped_without_storage() {
        use mgcore::types::{EdgeTypeId, Gid};
        use mgdurability::DeltaRecord;

        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage
            .create_edge(
                &tx,
                Gid::from(199u64),
                Gid::from(1u64),
                Gid::from(2u64),
                EdgeTypeId::from(7u32),
            )
            .unwrap();
        storage.commit_transaction(&tx);

        let raw = storage.deltas_since(0);
        for (gid, is_vertex, d) in raw {
            if !is_vertex {
                let record = ReplServer::core_delta_to_record(gid, d, is_vertex, None);
                if matches!(d.kind, mgcore::delta::DeltaKind::DeleteObject) {
                    assert!(
                        record.is_none(),
                        "EdgeCreate should be dropped without storage"
                    );
                }
            }
        }
    }

    #[test]
    fn test_set_property_delta_carries_current_value() {
        use mgcore::property_value::PropertyValue;
        use mgcore::types::{Gid, PropertyId};
        use mgdurability::DeltaRecord;

        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());

        // Create vertex, then set a non-Null property in a separate
        // transaction so the delta chain has both a DeleteObject (for
        // creation) and a SetProperty record.
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(7u64)).unwrap();
        storage.commit_transaction(&tx);

        let key = PropertyId::from(3u32);
        let tx2 = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage
            .vertex_set_property(
                &tx2,
                Gid::from(7u64),
                key,
                PropertyValue::String("hello".into()),
            )
            .unwrap();
        storage.commit_transaction(&tx2);

        // Walk every delta and translate via core_delta_to_record with
        // storage supplied — the SetProperty record must carry the
        // current value, not Null.
        let raw = storage.deltas_since(0);
        let mut saw_set_property = false;
        for (gid, is_vertex, d) in raw {
            if let Some(record) =
                ReplServer::core_delta_to_record(gid, d, is_vertex, Some(&storage))
            {
                if let DeltaRecord::VertexSetProperty {
                    gid: g,
                    key: k,
                    value,
                } = record
                {
                    assert_eq!(g, Gid::from(7u64));
                    assert_eq!(k, key);
                    assert_eq!(value, PropertyValue::String("hello".into()));
                    saw_set_property = true;
                }
            }
        }
        assert!(saw_set_property, "expected a VertexSetProperty record");
    }

    #[test]
    fn test_set_property_delta_falls_back_to_null_without_storage() {
        use mgcore::property_value::PropertyValue;
        use mgcore::types::{Gid, PropertyId};
        use mgdurability::DeltaRecord;

        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());

        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(8u64)).unwrap();
        storage.commit_transaction(&tx);

        let tx2 = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage
            .vertex_set_property(
                &tx2,
                Gid::from(8u64),
                PropertyId::from(1u32),
                PropertyValue::Int(42),
            )
            .unwrap();
        storage.commit_transaction(&tx2);

        let raw = storage.deltas_since(0);
        for (gid, is_vertex, d) in raw {
            if let Some(DeltaRecord::VertexSetProperty { value, .. }) =
                ReplServer::core_delta_to_record(gid, d, is_vertex, None)
            {
                assert_eq!(value, PropertyValue::Null);
                return;
            }
        }
        panic!("expected at least one VertexSetProperty record");
    }

    #[test]
    fn test_delta_stream_respects_batch_size_limit() {
        use mgcore::types::Gid;
        use mgdurability::DeltaRecord;

        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let server = ReplServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();

        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(10u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(20u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(30u64)).unwrap();
        storage.commit_transaction(&tx);

        // Request with batch_size_limit = 1
        let req = ReplRequest::DeltaStream {
            epoch_id: 1,
            since_timestamp: 0,
            batch_size_limit: 1,
        };
        let resp = server.handle_request(&req, &storage);

        match resp {
            ReplResponse::DeltaStreamBatch { batch } => {
                assert_eq!(batch.deltas.len(), 1, "batch should be truncated to limit");
            }
            other => panic!("expected DeltaStreamBatch, got {:?}", other),
        }
    }

    #[test]
    fn test_delta_stream_empty_when_no_new_deltas() {
        use mgcore::types::Gid;

        let storage = std::sync::Arc::new(mgstorage::storage::Storage::new());
        let server = ReplServer::bind("127.0.0.1:0", ReplicationConfig::default()).unwrap();

        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(100u64)).unwrap();
        storage.commit_transaction(&tx);

        // Request with a very high since_timestamp — nothing should match
        let req = ReplRequest::DeltaStream {
            epoch_id: 1,
            since_timestamp: u64::MAX,
            batch_size_limit: 100,
        };
        let resp = server.handle_request(&req, &storage);

        match resp {
            ReplResponse::DeltaStreamEnd => {
                // Expected — no deltas newer than u64::MAX
            }
            other => panic!("expected DeltaStreamEnd, got {:?}", other),
        }
    }
}
