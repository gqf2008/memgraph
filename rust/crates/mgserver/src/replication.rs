//! Replication integration — wires mgrepl into mgserver.
//!
//! Handles three roles:
//!   * **Main**    — accepts replica connections, streams deltas on commit.
//!   * **Replica** — connects to main, applies incoming deltas.
//!   * **None**    — no replication (default).

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use tracing::{error, info, warn};

use mgdurability::DeltaRecord;
use mgrepl::{
    ReplServer, ReplicationClient, ReplicationConfig, ReplicationMode, ReplicationRole,
    SyncReplicationClient,
};
use mgrpc::{
    decode_body, DeltaBatch, DeltaStreamRequest, MessageHeader, RpcClient, WalRequest, WalTransfer,
};
use mgstorage::storage::Storage;
use mgstorage::{WalAppender, WalRecord};

/// Shared state for the main replication server.
pub struct MainReplicationState {
    server: ReplServer,
    storage: Arc<Storage>,
    /// Connected replica clients, keyed by their socket address.
    replicas: Mutex<Vec<(SocketAddr, SyncReplicationClient)>>,
    /// Delta batches waiting to be sent (used for async mode + background thread).
    pending: Mutex<VecDeque<DeltaBatch>>,
    epoch_id: u64,
    sequence: AtomicU64,
    /// Path to the active WAL file. Replicas request this for catch-up after
    /// snapshot recovery. None if WAL is not yet attached.
    wal_path: RwLock<Option<PathBuf>>,
}

impl MainReplicationState {
    pub fn bind(
        addr: &str,
        config: ReplicationConfig,
        storage: Arc<Storage>,
    ) -> std::io::Result<Arc<Self>> {
        let server = ReplServer::bind(addr, config)?;
        info!("Replication server listening on {}", server.local_addr()?);
        Ok(Arc::new(Self {
            server,
            storage,
            replicas: Mutex::new(Vec::new()),
            pending: Mutex::new(VecDeque::new()),
            epoch_id: 1,
            sequence: AtomicU64::new(0),
            wal_path: RwLock::new(None),
        }))
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.server.local_addr()
    }

    /// Set the path of the active WAL file so replicas can pull it during
    /// catch-up. Called by mgserver after attaching the WAL writer.
    pub fn set_wal_path(&self, path: PathBuf) {
        if let Ok(mut guard) = self.wal_path.write() {
            *guard = Some(path);
        }
    }

    fn current_wal_path(&self) -> Option<PathBuf> {
        self.wal_path.read().ok().and_then(|g| g.clone())
    }

    /// Accept a new replica connection and spawn a handler thread that
    /// serves delta-stream requests.
    pub fn accept_replica(self: &Arc<Self>) {
        match self.server.accept() {
            Ok((rpc, addr)) => {
                info!("Replica connected from {}", addr);
                let state = self.clone();
                thread::spawn(move || {
                    serve_replica(rpc, state);
                });
            }
            Err(e) => {
                warn!("Failed to accept replica: {}", e);
            }
        }
    }

    /// Send a delta batch to all connected replicas.
    /// For Sync mode, blocks until ACK. For Async mode, fire-and-forget.
    pub fn broadcast(&self, batch: &DeltaBatch, mode: ReplicationMode) {
        let mut replicas = self.replicas.lock().unwrap();
        let mut to_remove = Vec::new();

        for (idx, (addr, client)) in replicas.iter_mut().enumerate() {
            let result = match mode {
                ReplicationMode::Sync => client.send_and_wait(batch),
                ReplicationMode::StrictSync => client.send_and_wait(batch),
                ReplicationMode::Async => {
                    // AsyncReplicationClient::send expects mutable RpcClient
                    // For simplicity, use sync client's rpc_mut for fire-and-forget
                    let _ = mgrepl::send_delta_batch(client.rpc_mut(), batch);
                    Ok(())
                }
            };
            if let Err(e) = result {
                warn!("Replication failed to {}: {}", addr, e);
                to_remove.push(idx);
            }
        }

        // Remove dead replicas in reverse order to maintain indices
        for idx in to_remove.into_iter().rev() {
            let removed = replicas.remove(idx);
            warn!("Removed dead replica {}", removed.0);
        }
    }

    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }
}

/// Serve a single replica connection: dispatch on the request's message_id and
/// respond accordingly. Currently handles:
///   * 11 — `WalRequest`: send back the active WAL file as a `WalTransfer`.
///   * 20 — `DeltaStreamRequest`: walk storage and stream `DeltaBatch`es.
fn serve_replica(mut rpc: RpcClient, state: Arc<MainReplicationState>) {
    const MSG_WAL_REQUEST: u64 = 11;
    const MSG_DELTA_STREAM: u64 = 20;
    const EPOCH_ID: u64 = 1;

    let (header, payload) = match rpc.recv_dispatch() {
        Ok(v) => v,
        Err(e) => {
            warn!("Replica request recv error: {}", e);
            return;
        }
    };

    match header.message_id {
        MSG_DELTA_STREAM => {
            let req: DeltaStreamRequest = match decode_body(&payload) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Failed to decode DeltaStreamRequest: {}", e);
                    return;
                }
            };
            serve_delta_stream(rpc, state.storage.clone(), req, EPOCH_ID);
        }
        MSG_WAL_REQUEST => {
            let req: WalRequest = match decode_body(&payload) {
                Ok(v) => v,
                Err(e) => {
                    warn!("Failed to decode WalRequest: {}", e);
                    return;
                }
            };
            serve_wal_request(rpc, &state, req);
        }
        other => {
            warn!("Unknown replica message_id: {}", other);
        }
    }
}

fn serve_delta_stream(
    mut rpc: RpcClient,
    storage: Arc<Storage>,
    req: DeltaStreamRequest,
    epoch_id: u64,
) {
    if req.epoch_id != epoch_id {
        let _ = mgrepl::send_delta_stream_end(&mut rpc, epoch_id);
        return;
    }

    let mut since_ts = req.start_timestamp;
    loop {
        let mut deltas = Vec::new();
        let mut max_commit_ts = 0u64;

        let raw_deltas = storage.deltas_since(since_ts);
        for (gid, is_vertex, d) in raw_deltas {
            let ts = d.commit_info.timestamp();
            if let Some(record) =
                ReplServer::core_delta_to_record(gid, d, is_vertex, Some(&storage))
            {
                max_commit_ts = max_commit_ts.max(ts);
                deltas.push(record);
            }
        }

        if deltas.is_empty() {
            let _ = mgrepl::send_delta_stream_end(&mut rpc, epoch_id);
            break;
        }

        let batch = DeltaBatch {
            epoch_id,
            commit_timestamp: max_commit_ts,
            sequence_number: 0,
            deltas,
        };

        since_ts = max_commit_ts + 1;

        if let Err(e) = mgrepl::send_delta_batch(&mut rpc, &batch) {
            warn!("Failed to send batch to replica: {}", e);
            break;
        }
    }
}

fn serve_wal_request(mut rpc: RpcClient, state: &MainReplicationState, req: WalRequest) {
    if req.epoch_id != state.epoch_id {
        warn!(
            "Replica WalRequest epoch_id mismatch: got {}, expected {}",
            req.epoch_id, state.epoch_id
        );
        let empty = WalTransfer {
            file_name: String::new(),
            data: Vec::new(),
        };
        let header = MessageHeader::new(11, 1);
        let _ = rpc.send(&header, &empty);
        return;
    }

    let path = match state.current_wal_path() {
        Some(p) => p,
        None => {
            warn!("Replica requested WAL but no WAL path is configured");
            let empty = WalTransfer {
                file_name: String::new(),
                data: Vec::new(),
            };
            let header = MessageHeader::new(11, 1);
            let _ = rpc.send(&header, &empty);
            return;
        }
    };

    let data = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Failed to read WAL file {}: {}", path.display(), e);
            let empty = WalTransfer {
                file_name: String::new(),
                data: Vec::new(),
            };
            let header = MessageHeader::new(11, 1);
            let _ = rpc.send(&header, &empty);
            return;
        }
    };

    let file_name = path
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_else(|| "wal.mgwal".to_string());

    info!(
        "Sending WAL '{}' ({} bytes) to replica (since_ts={})",
        file_name,
        data.len(),
        req.since_timestamp
    );
    let transfer = WalTransfer { file_name, data };
    let header = MessageHeader::new(11, 1);
    if let Err(e) = rpc.send(&header, &transfer) {
        warn!("Failed to send WAL to replica: {}", e);
    }
}

/// Replica-side replication state.
pub struct ReplicaReplicationState {
    client: Arc<Mutex<ReplicationClient>>,
    main_addr: String,
}

impl ReplicaReplicationState {
    pub fn connect(main_addr: &str, config: ReplicationConfig) -> std::io::Result<Arc<Self>> {
        let client = ReplicationClient::connect(main_addr, config)?;
        info!("Connected to main replication server at {}", main_addr);
        Ok(Arc::new(Self {
            client: Arc::new(Mutex::new(client)),
            main_addr: main_addr.to_string(),
        }))
    }

    /// Start a background thread that continuously pulls deltas from main
    /// and applies them to the local storage.
    pub fn start_sync_loop(self: &Arc<Self>, storage: Arc<Storage>) {
        let client = self.client.clone();
        let main_addr = self.main_addr.clone();
        thread::spawn(move || {
            info!("[replica] Starting delta sync loop from {}", main_addr);
            let mut applier = mgrepl::StorageDeltaApplier::new(storage);
            loop {
                let result = {
                    let mut c = client.lock().unwrap();
                    // Request stream starting from timestamp 0 (full catch-up)
                    if let Err(e) = c.request_delta_stream(0, 1000) {
                        warn!("[replica] Failed to request delta stream: {}", e);
                        thread::sleep(Duration::from_secs(5));
                        continue;
                    }
                    c.apply_stream(&mut applier)
                };
                match result {
                    Ok(Some(ts)) => info!("[replica] Applied deltas up to timestamp {}", ts),
                    Ok(None) => info!("[replica] Stream empty, retrying..."),
                    Err(e) => {
                        warn!("[replica] Delta stream error: {}", e);
                        thread::sleep(Duration::from_secs(5));
                    }
                }
            }
        });
    }
}

/// Top-level replication manager attached to ServerContext.
pub struct ReplicationManager {
    pub role: ReplicationRole,
    pub config: ReplicationConfig,
    pub main_state: Option<Arc<MainReplicationState>>,
    pub replica_state: Option<Arc<ReplicaReplicationState>>,
}

impl ReplicationManager {
    pub fn none() -> Self {
        Self {
            role: ReplicationRole::None,
            config: ReplicationConfig::default(),
            main_state: None,
            replica_state: None,
        }
    }

    pub fn new_main(
        bind_addr: &str,
        config: ReplicationConfig,
        storage: Arc<Storage>,
    ) -> std::io::Result<Self> {
        let state = MainReplicationState::bind(bind_addr, config.clone(), storage)?;
        // Start acceptor thread
        let state_clone = state.clone();
        thread::spawn(move || {
            info!("[repl] Accepting replica connections...");
            loop {
                state_clone.accept_replica();
            }
        });
        Ok(Self {
            role: ReplicationRole::Main,
            config,
            main_state: Some(state),
            replica_state: None,
        })
    }

    pub fn new_replica(
        main_addr: &str,
        config: ReplicationConfig,
        storage: Arc<Storage>,
    ) -> std::io::Result<Self> {
        let state = ReplicaReplicationState::connect(main_addr, config.clone())?;
        state.start_sync_loop(storage);
        Ok(Self {
            role: ReplicationRole::Replica,
            config,
            main_state: None,
            replica_state: Some(state),
        })
    }

    /// Convert a WalRecord to a DeltaRecord for replication.
    fn wal_to_delta(record: &WalRecord) -> Option<DeltaRecord> {
        match record {
            WalRecord::VertexCreate { gid, timestamp } => Some(DeltaRecord::VertexCreate {
                gid: *gid,
                timestamp: *timestamp,
            }),
            WalRecord::VertexDelete { gid } => Some(DeltaRecord::VertexDelete { gid: *gid }),
            WalRecord::VertexAddLabel { gid, label } => Some(DeltaRecord::VertexAddLabel {
                gid: *gid,
                label: *label,
            }),
            WalRecord::VertexRemoveLabel { gid, label } => Some(DeltaRecord::VertexRemoveLabel {
                gid: *gid,
                label: *label,
            }),
            WalRecord::VertexSetProperty { gid, key, value } => {
                Some(DeltaRecord::VertexSetProperty {
                    gid: *gid,
                    key: *key,
                    value: value.clone(),
                })
            }
            WalRecord::EdgeCreate {
                gid,
                from_vertex,
                to_vertex,
                edge_type,
                timestamp,
            } => Some(DeltaRecord::EdgeCreate {
                gid: *gid,
                from_vertex: *from_vertex,
                to_vertex: *to_vertex,
                edge_type: *edge_type,
                timestamp: *timestamp,
            }),
            WalRecord::EdgeDelete { gid } => Some(DeltaRecord::EdgeDelete { gid: *gid }),
            WalRecord::EdgeSetProperty { gid, key, value } => Some(DeltaRecord::EdgeSetProperty {
                gid: *gid,
                key: *key,
                value: value.clone(),
            }),
            WalRecord::EdgeChangeType { gid, old_type, new_type } => Some(DeltaRecord::EdgeChangeType {
                gid: *gid,
                old_type: *old_type,
                new_type: *new_type,
            }),
            WalRecord::EdgeSetFrom { gid, old_from, new_from } => Some(DeltaRecord::EdgeSetFrom {
                gid: *gid,
                old_from: *old_from,
                new_from: *new_from,
            }),
            WalRecord::EdgeSetTo { gid, old_to, new_to } => Some(DeltaRecord::EdgeSetTo {
                gid: *gid,
                old_to: *old_to,
                new_to: *new_to,
            }),
            WalRecord::TransactionStart { timestamp } => Some(DeltaRecord::TransactionStart { timestamp: *timestamp }),
            WalRecord::TransactionEnd { .. } => None,
        }
    }

    /// Called when a transaction commits successfully.
    /// Sends accumulated deltas to all replicas.
    pub fn on_commit(&self, deltas: Vec<DeltaRecord>, commit_timestamp: u64) {
        if deltas.is_empty() {
            return;
        }
        if let Some(ref main) = self.main_state {
            let batch = DeltaBatch {
                epoch_id: main.epoch_id,
                commit_timestamp,
                sequence_number: main.next_sequence(),
                deltas,
            };
            main.broadcast(&batch, self.config.mode);
        }
    }
}

/// WalAppender implementation that wraps an existing appender and triggers replication.
pub struct ReplicatingWalWriter {
    inner: Box<dyn WalAppender>,
    repl: Arc<Mutex<Option<ReplicationManager>>>,
    pending: Vec<DeltaRecord>,
}

impl ReplicatingWalWriter {
    pub fn new(inner: Box<dyn WalAppender>, repl: Arc<Mutex<Option<ReplicationManager>>>) -> Self {
        Self {
            inner,
            repl,
            pending: Vec::new(),
        }
    }
}

impl WalAppender for ReplicatingWalWriter {
    fn append(&mut self, record: WalRecord) {
        // 1. Accumulate deltas for replication before handing to inner
        match &record {
            WalRecord::TransactionEnd {
                commit_timestamp, ..
            } => {
                let deltas = std::mem::take(&mut self.pending);
                if let Ok(guard) = self.repl.lock() {
                    if let Some(ref repl) = *guard {
                        repl.on_commit(deltas, *commit_timestamp);
                    }
                }
            }
            _ => {
                if let Some(d) = ReplicationManager::wal_to_delta(&record) {
                    self.pending.push(d);
                }
            }
        }
        // 2. Write to local WAL (via inner appender)
        self.inner.append(record);
    }

    fn sync(&mut self) -> Result<(), std::io::Error> {
        self.inner.sync()
    }

    fn reset(&mut self) -> Result<(), std::io::Error> {
        self.inner.reset()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgrepl::ReplicationClient;
    use std::io::Write;

    fn temp_wal_with_data(name: &str, payload: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mgserver_repl_test_{}_{}.mgwal",
            name,
            std::process::id()
        ));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(payload).unwrap();
        f.flush().unwrap();
        path
    }

    #[test]
    fn test_replica_can_pull_wal_via_request_wal() {
        let storage = Arc::new(Storage::new());
        let state =
            MainReplicationState::bind("127.0.0.1:0", ReplicationConfig::default(), storage)
                .expect("bind main replication server");
        let addr = state.local_addr().unwrap().to_string();

        let payload = b"MGwl\x00\x01wal-bytes-for-test";
        let wal_path = temp_wal_with_data("pull", payload);
        state.set_wal_path(wal_path.clone());

        // Spawn an acceptor that handles exactly one connection.
        let state_acceptor = state.clone();
        let acceptor = thread::spawn(move || {
            state_acceptor.accept_replica();
        });

        // Connect a replication client and request WAL.
        let mut client = ReplicationClient::connect(&addr, ReplicationConfig::default())
            .expect("connect replication client");
        client.set_epoch_id(1);
        let files = client.request_wal(0).expect("request_wal");

        acceptor.join().expect("acceptor thread");

        assert_eq!(files.len(), 1);
        let (file_name, data) = files.into_iter().next().unwrap();
        assert_eq!(file_name, wal_path.file_name().unwrap().to_string_lossy());
        assert_eq!(data, payload);

        let _ = std::fs::remove_file(&wal_path);
    }

    #[test]
    fn test_replica_request_wal_without_path_returns_empty() {
        let storage = Arc::new(Storage::new());
        let state =
            MainReplicationState::bind("127.0.0.1:0", ReplicationConfig::default(), storage)
                .expect("bind main replication server");
        let addr = state.local_addr().unwrap().to_string();
        // No set_wal_path() call.

        let state_acceptor = state.clone();
        let acceptor = thread::spawn(move || {
            state_acceptor.accept_replica();
        });

        let mut client = ReplicationClient::connect(&addr, ReplicationConfig::default()).unwrap();
        client.set_epoch_id(1);
        let files = client.request_wal(0).expect("request_wal");

        acceptor.join().unwrap();

        assert_eq!(files.len(), 1);
        let (file_name, data) = files.into_iter().next().unwrap();
        assert!(file_name.is_empty());
        assert!(data.is_empty());
    }

    #[test]
    fn test_wal_request_epoch_mismatch_returns_empty() {
        let storage = Arc::new(Storage::new());
        let state =
            MainReplicationState::bind("127.0.0.1:0", ReplicationConfig::default(), storage)
                .expect("bind main replication server");
        let addr = state.local_addr().unwrap().to_string();

        let payload = b"some-wal-payload";
        let wal_path = temp_wal_with_data("epoch", payload);
        state.set_wal_path(wal_path.clone());

        let state_acceptor = state.clone();
        let acceptor = thread::spawn(move || {
            state_acceptor.accept_replica();
        });

        let mut client = ReplicationClient::connect(&addr, ReplicationConfig::default()).unwrap();
        client.set_epoch_id(99); // wrong epoch
        let files = client.request_wal(0).expect("request_wal");

        acceptor.join().unwrap();

        assert_eq!(files.len(), 1);
        let (file_name, data) = files.into_iter().next().unwrap();
        assert!(
            file_name.is_empty(),
            "epoch mismatch should yield empty file_name"
        );
        assert!(data.is_empty());

        let _ = std::fs::remove_file(&wal_path);
    }
}
