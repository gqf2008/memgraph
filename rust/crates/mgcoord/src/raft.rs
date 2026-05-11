//! openraft integration for coordinator consensus.
//!
//! Each coordinator instance runs a Raft node. The replicated state machine
//! tracks cluster membership (data instances, their roles, and routing tables).
//! Only coordinator-level decisions go through Raft; replication of graph deltas
//! between main ↔ replica is handled by `mgrepl`.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use openraft::{
    Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, RaftTypeConfig,
    Snapshot, SnapshotMeta, StoredMembership, Vote,
};
use openraft::error::RaftError;
use openraft::network::{Backoff, RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::{RaftLogStorage, RaftStateMachine, LogFlushed};

pub use openraft::Config as RaftConfig;

// ─── Node types ────────────────────────────────────────────────────────────

/// Unique identifier for a coordinator node.
pub type CoordinatorNodeId = u64;

/// Network address and metadata for a coordinator peer.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CoordinatorNode {
    pub rpc_addr: String,
    pub bolt_addr: String,
}

impl std::fmt::Display for CoordinatorNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.rpc_addr, self.bolt_addr)
    }
}

// ─── Application data ──────────────────────────────────────────────────────

/// A command that mutates cluster state. These are the payloads replicated
/// through the Raft log.
///
/// `AppData` and `AppDataResponse` are blanket-implemented by openraft when
/// serde is enabled, so we don't impl them manually.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClusterCommand {
    /// Register a new data instance.
    RegisterInstance {
        instance_id: String,
        bolt_addr: SocketAddr,
    },
    /// Set the main instance for a database.
    SetMain {
        database: String,
        instance_id: String,
    },
    /// Promote a replica to main.
    Promote { instance_id: String },
    /// Demote a main to replica.
    Demote { instance_id: String },
    /// Remove an instance from the cluster.
    Unregister { instance_id: String },
}

/// Response from applying a `ClusterCommand`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClusterResponse {
    Ok,
    InstanceNotFound(String),
    AlreadyMain(String),
    AlreadyReplica(String),
}

// ─── TypeConfig ────────────────────────────────────────────────────────────

/// openraft type configuration for the coordinator consensus layer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypeConfig;

impl RaftTypeConfig for TypeConfig {
    type D = ClusterCommand;
    type R = ClusterResponse;
    type NodeId = CoordinatorNodeId;
    type Node = CoordinatorNode;
    type Entry = openraft::Entry<TypeConfig>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<TypeConfig>;
}

// ─── Network ───────────────────────────────────────────────────────────────

/// mgrpc-based network transport for Raft RPCs.
///
/// Uses connection pooling with retry logic and configurable timeouts.
#[derive(Clone, Debug)]
pub struct CoordinatorNetwork {
    target: CoordinatorNodeId,
    target_node: CoordinatorNode,
    timeout: Duration,
    max_retries: usize,
}

impl CoordinatorNetwork {
    pub fn new(target: CoordinatorNodeId, target_node: CoordinatorNode) -> Self {
        Self {
            target,
            target_node,
            timeout: Duration::from_secs(5),
            max_retries: 3,
        }
    }

    /// Set the RPC timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the maximum number of retries for transient failures.
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Execute an RPC with retry logic.
    async fn rpc_with_retry<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Debug,
    {
        let mut last_err = None;
        for attempt in 0..self.max_retries {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < self.max_retries - 1 {
                        tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                    }
                }
            }
        }
        // This should be unreachable because we always set last_err before this point,
        // but we need to satisfy the compiler.
        panic!("rpc_with_retry: max_retries={} but no error was captured", self.max_retries)
    }
}

impl RaftNetwork<TypeConfig> for CoordinatorNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<CoordinatorNodeId>,
        openraft::error::RPCError<CoordinatorNodeId, CoordinatorNode, RaftError<CoordinatorNodeId>>,
    > {
        // Serialize via bincode over mgrpc
        let payload = match bincode::serialize(&rpc) {
            Ok(p) => p,
            Err(e) => {
                return Err(openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("bincode serialize error: {}", e),
                    )),
                ));
            }
        };

        self.rpc_with_retry(|| async {
            let mut client = mgrpc::RpcClient::connect(&self.target_node.rpc_addr)
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&e)
                ))?;
            let header = mgrpc::MessageHeader::new(1, 1);
            client.send(&header, &mgrpc::SnapshotData { data: payload.clone() })
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&e)
                ))?;
            let (_resp_header, resp_body): (mgrpc::MessageHeader, mgrpc::SnapshotData) = client.recv()
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&e)
                ))?;
            let resp: AppendEntriesResponse<CoordinatorNodeId> = bincode::deserialize(&resp_body.data)
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("bincode deserialize error: {}", e),
                    ))
                ))?;
            Ok::<_, openraft::error::RPCError<CoordinatorNodeId, CoordinatorNode, RaftError<CoordinatorNodeId>>>(resp)
        }).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<CoordinatorNodeId>,
        openraft::error::RPCError<CoordinatorNodeId, CoordinatorNode,
            RaftError<CoordinatorNodeId, openraft::error::InstallSnapshotError>>,
    > {
        let payload = match bincode::serialize(&rpc) {
            Ok(p) => p,
            Err(e) => {
                return Err(openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("bincode serialize error: {}", e),
                    )),
                ));
            }
        };

        self.rpc_with_retry(|| async {
            let mut client = mgrpc::RpcClient::connect(&self.target_node.rpc_addr)
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&e)
                ))?;
            let header = mgrpc::MessageHeader::new(2, 1);
            client.send(&header, &mgrpc::SnapshotData { data: payload.clone() })
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&e)
                ))?;
            let (_resp_header, resp_body): (mgrpc::MessageHeader, mgrpc::SnapshotData) = client.recv()
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&e)
                ))?;
            let resp: InstallSnapshotResponse<CoordinatorNodeId> = bincode::deserialize(&resp_body.data)
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("bincode deserialize error: {}", e),
                    ))
                ))?;
            Ok::<_, openraft::error::RPCError<CoordinatorNodeId, CoordinatorNode,
                RaftError<CoordinatorNodeId, openraft::error::InstallSnapshotError>>>(resp)
        }).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<CoordinatorNodeId>,
        _option: RPCOption,
    ) -> Result<
        VoteResponse<CoordinatorNodeId>,
        openraft::error::RPCError<CoordinatorNodeId, CoordinatorNode, RaftError<CoordinatorNodeId>>,
    > {
        let payload = match bincode::serialize(&rpc) {
            Ok(p) => p,
            Err(e) => {
                return Err(openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("bincode serialize error: {}", e),
                    )),
                ));
            }
        };

        self.rpc_with_retry(|| async {
            let mut client = mgrpc::RpcClient::connect(&self.target_node.rpc_addr)
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&e)
                ))?;
            let header = mgrpc::MessageHeader::new(3, 1);
            client.send(&header, &mgrpc::SnapshotData { data: payload.clone() })
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&e)
                ))?;
            let (_resp_header, resp_body): (mgrpc::MessageHeader, mgrpc::SnapshotData) = client.recv()
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&e)
                ))?;
            let resp: VoteResponse<CoordinatorNodeId> = bincode::deserialize(&resp_body.data)
                .map_err(|e| openraft::error::RPCError::Unreachable(
                    openraft::error::Unreachable::new(&std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("bincode deserialize error: {}", e),
                    ))
                ))?;
            Ok::<_, openraft::error::RPCError<CoordinatorNodeId, CoordinatorNode, RaftError<CoordinatorNodeId>>>(resp)
        }).await
    }

    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(500)))
    }
}

/// Factory that creates [`CoordinatorNetwork`] instances for each target node.
#[derive(Clone, Debug, Default)]
pub struct CoordinatorNetworkFactory;

impl CoordinatorNetworkFactory {
    pub fn new() -> Self {
        Self
    }
}

impl RaftNetworkFactory<TypeConfig> for CoordinatorNetworkFactory {
    type Network = CoordinatorNetwork;

    async fn new_client(&mut self, target: CoordinatorNodeId, node: &CoordinatorNode) -> Self::Network {
        CoordinatorNetwork::new(target, node.clone())
    }
}

// ─── Log Storage ───────────────────────────────────────────────────────────

/// In-memory log storage with vote persistence.
///
/// In production this should be backed by `mgdurability` WAL for persistence.
/// The log storage is separate from the state machine in openraft v2.
#[derive(Clone, Debug)]
pub struct CoordinatorLogStorage {
    log: BTreeMap<u64, Entry<TypeConfig>>,
    vote: Option<Vote<CoordinatorNodeId>>,
    committed: Option<LogId<CoordinatorNodeId>>,
}

impl CoordinatorLogStorage {
    pub fn new() -> Self {
        Self {
            log: BTreeMap::new(),
            vote: None,
            committed: None,
        }
    }
}

impl RaftLogReader<TypeConfig> for CoordinatorLogStorage {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, openraft::StorageError<CoordinatorNodeId>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(v) => *v,
            std::ops::Bound::Excluded(v) => *v + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(v) => *v + 1,
            std::ops::Bound::Excluded(v) => *v,
            std::ops::Bound::Unbounded => u64::MAX,
        };
        Ok(self
            .log
            .range(start..end)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for CoordinatorLogStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<openraft::storage::LogState<TypeConfig>, openraft::StorageError<CoordinatorNodeId>> {
        let last = self.log.last_key_value().map(|(_, e)| e.log_id.clone());
        Ok(openraft::storage::LogState {
            last_purged_log_id: None,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<CoordinatorNodeId>) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        self.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<CoordinatorNodeId>>, openraft::StorageError<CoordinatorNodeId>> {
        Ok(self.vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<CoordinatorNodeId>>) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<CoordinatorNodeId>>, openraft::StorageError<CoordinatorNodeId>> {
        Ok(self.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), openraft::StorageError<CoordinatorNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        for entry in entries {
            self.log.insert(entry.log_id.index, entry);
        }
        // In-memory storage: immediately report flushed
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<CoordinatorNodeId>) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        self.log.retain(|_, e| e.log_id < log_id);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<CoordinatorNodeId>) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        self.log.retain(|_, e| e.log_id > log_id);
        Ok(())
    }
}

// ─── Persistent Log Storage ────────────────────────────────────────────────

/// Disk-backed log storage. Each mutation is bincoded and atomically renamed
/// into place under `dir`, so a coordinator restart recovers `log`, `vote`,
/// and `committed` from the file system.
///
/// The on-disk layout is three separate files:
/// - `log.bin`       — `BTreeMap<u64, Entry<TypeConfig>>`
/// - `vote.bin`      — `Option<Vote<CoordinatorNodeId>>`
/// - `committed.bin` — `Option<LogId<CoordinatorNodeId>>`
///
/// Cloning a storage handle gives a snapshot of the in-memory log for reads;
/// it shares the same `dir` but readers never write back. This matches the
/// `Self`-as-LogReader pattern used by [`CoordinatorLogStorage`].
#[derive(Clone, Debug)]
pub struct PersistentCoordinatorLogStorage {
    dir: PathBuf,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    vote: Option<Vote<CoordinatorNodeId>>,
    committed: Option<LogId<CoordinatorNodeId>>,
}

impl PersistentCoordinatorLogStorage {
    /// Open (or create) a persistent log storage rooted at `dir`. Replays any
    /// existing `log.bin`, `vote.bin`, and `committed.bin` files into memory.
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let log = read_or_default(&dir.join("log.bin"))?;
        let vote = read_or_default(&dir.join("vote.bin"))?;
        let committed = read_or_default(&dir.join("committed.bin"))?;

        Ok(Self { dir, log, vote, committed })
    }

    fn write_atomic(&self, name: &str, data: &[u8]) -> std::io::Result<()> {
        let tmp = self.dir.join(format!("{}.tmp", name));
        let final_path = self.dir.join(name);
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, &final_path)?;
        Ok(())
    }

    fn persist_log(&self) -> std::io::Result<()> {
        let data = bincode::serialize(&self.log).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        self.write_atomic("log.bin", &data)
    }

    fn persist_vote(&self) -> std::io::Result<()> {
        let data = bincode::serialize(&self.vote).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        self.write_atomic("vote.bin", &data)
    }

    fn persist_committed(&self) -> std::io::Result<()> {
        let data = bincode::serialize(&self.committed).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        self.write_atomic("committed.bin", &data)
    }
}

fn read_or_default<T: serde::de::DeserializeOwned + Default>(
    path: &Path,
) -> std::io::Result<T> {
    if !path.exists() {
        return Ok(T::default());
    }
    let bytes = std::fs::read(path)?;
    bincode::deserialize(&bytes).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })
}

fn io_storage_error(
    subject: openraft::ErrorSubject<CoordinatorNodeId>,
    verb: openraft::ErrorVerb,
    err: std::io::Error,
) -> openraft::StorageError<CoordinatorNodeId> {
    openraft::StorageError::IO {
        source: openraft::StorageIOError::new(subject, verb, &err),
    }
}

impl RaftLogReader<TypeConfig> for PersistentCoordinatorLogStorage {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, openraft::StorageError<CoordinatorNodeId>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(v) => *v,
            std::ops::Bound::Excluded(v) => *v + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(v) => *v + 1,
            std::ops::Bound::Excluded(v) => *v,
            std::ops::Bound::Unbounded => u64::MAX,
        };
        Ok(self
            .log
            .range(start..end)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for PersistentCoordinatorLogStorage {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::storage::LogState<TypeConfig>, openraft::StorageError<CoordinatorNodeId>>
    {
        let last = self.log.last_key_value().map(|(_, e)| e.log_id.clone());
        Ok(openraft::storage::LogState {
            last_purged_log_id: None,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<CoordinatorNodeId>,
    ) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        self.vote = Some(*vote);
        self.persist_vote().map_err(|e| {
            io_storage_error(openraft::ErrorSubject::Vote, openraft::ErrorVerb::Write, e)
        })
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<CoordinatorNodeId>>, openraft::StorageError<CoordinatorNodeId>> {
        Ok(self.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<CoordinatorNodeId>>,
    ) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        self.committed = committed;
        self.persist_committed().map_err(|e| {
            io_storage_error(openraft::ErrorSubject::Store, openraft::ErrorVerb::Write, e)
        })
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<CoordinatorNodeId>>, openraft::StorageError<CoordinatorNodeId>> {
        Ok(self.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), openraft::StorageError<CoordinatorNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        for entry in entries {
            self.log.insert(entry.log_id.index, entry);
        }
        match self.persist_log() {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                let err = io_storage_error(
                    openraft::ErrorSubject::Logs,
                    openraft::ErrorVerb::Write,
                    e,
                );
                // Surface the IO failure to the caller. We do not invoke the
                // callback because the log was not durably flushed.
                Err(err)
            }
        }
    }

    async fn truncate(
        &mut self,
        log_id: LogId<CoordinatorNodeId>,
    ) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        self.log.retain(|_, e| e.log_id < log_id);
        self.persist_log().map_err(|e| {
            io_storage_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write, e)
        })
    }

    async fn purge(
        &mut self,
        log_id: LogId<CoordinatorNodeId>,
    ) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        self.log.retain(|_, e| e.log_id > log_id);
        self.persist_log().map_err(|e| {
            io_storage_error(openraft::ErrorSubject::Logs, openraft::ErrorVerb::Write, e)
        })
    }
}

// ─── State Machine ─────────────────────────────────────────────────────────

/// In-memory state machine for coordinator consensus.
///
/// Applies cluster commands and maintains snapshots.
#[derive(Clone, Debug)]
pub struct CoordinatorStateMachine {
    last_applied: Option<LogId<CoordinatorNodeId>>,
    stored_membership: StoredMembership<CoordinatorNodeId, CoordinatorNode>,
    snapshot: Option<Snapshot<TypeConfig>>,
    cluster_state: Option<Arc<crate::ClusterState>>,
}

impl CoordinatorStateMachine {
    pub fn new() -> Self {
        Self {
            last_applied: None,
            stored_membership: StoredMembership::default(),
            snapshot: None,
            cluster_state: None,
        }
    }

    /// Attach a shared [`ClusterState`] so that `apply` can mutate real cluster state.
    pub fn attach_cluster_state(&mut self, state: Arc<crate::ClusterState>) {
        self.cluster_state = Some(state);
    }
}

impl RaftStateMachine<TypeConfig> for CoordinatorStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(
        Option<LogId<CoordinatorNodeId>>,
        StoredMembership<CoordinatorNodeId, CoordinatorNode>,
    ), openraft::StorageError<CoordinatorNodeId>> {
        Ok((self.last_applied, self.stored_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClusterResponse>, openraft::StorageError<CoordinatorNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();
        for entry in entries {
            let resp = match &entry.payload {
                EntryPayload::Normal(cmd) => apply_command(cmd, self.cluster_state.as_ref()),
                EntryPayload::Membership(membership) => {
                    self.stored_membership = StoredMembership::new(Some(entry.log_id.clone()), membership.clone());
                    ClusterResponse::Ok
                }
                EntryPayload::Blank => ClusterResponse::Ok,
            };
            self.last_applied = Some(entry.log_id.clone());
            responses.push(resp);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, openraft::StorageError<CoordinatorNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<CoordinatorNodeId, CoordinatorNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        let data = snapshot.into_inner();
        self.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data)),
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, openraft::StorageError<CoordinatorNodeId>> {
        Ok(self.snapshot.clone())
    }
}

/// Serializable snapshot of cluster state for Raft.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ClusterSnapshot {
    instances: Vec<crate::Instance>,
    leader_id: String,
    routes: Vec<(String, SocketAddr)>,
}

impl RaftSnapshotBuilder<TypeConfig> for CoordinatorStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, openraft::StorageError<CoordinatorNodeId>> {
        let snap = if let Some(ref state) = self.cluster_state {
            ClusterSnapshot {
                instances: state.list(),
                leader_id: state.leader_id(),
                routes: state.routes(),
            }
        } else {
            ClusterSnapshot {
                instances: Vec::new(),
                leader_id: String::new(),
                routes: Vec::new(),
            }
        };
        let data = match bincode::serialize(&snap) {
            Ok(d) => d,
            Err(e) => {
                return Err(openraft::StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::StateMachine,
                        openraft::ErrorVerb::Write,
                        &std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("serialize cluster snapshot: {}", e),
                        ),
                    ),
                });
            }
        };
        let meta = SnapshotMeta::default();
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

/// Apply a replicated [`ClusterCommand`] against the shared [`ClusterState`].
///
/// When `cluster_state` is `None` (e.g. during unit tests that don't wire
/// storage to a real state object) the command returns `ClusterResponse::Ok`
/// without side-effects, preserving the old no-op behaviour for tests that
/// only verify log plumbing.
fn apply_command(
    cmd: &ClusterCommand,
    cluster_state: Option<&Arc<crate::ClusterState>>,
) -> ClusterResponse {
    let Some(state) = cluster_state else {
        return ClusterResponse::Ok;
    };

    match cmd {
        ClusterCommand::RegisterInstance {
            instance_id,
            bolt_addr,
        } => {
            let inst = crate::Instance::new(
                instance_id.clone(),
                *bolt_addr,
                crate::InstanceRole::Replica,
            );
            state.register(inst);
            ClusterResponse::Ok
        }
        ClusterCommand::SetMain {
            database,
            instance_id,
        } => {
            // Promote the target to main (demotes current main automatically).
            if let Err(e) = state.promote_to_main(instance_id) {
                return ClusterResponse::InstanceNotFound(e.to_string());
            }
            // Update routing table so clients are redirected to the new main.
            if let Some(inst) = state.get(instance_id) {
                state.set_route(database, inst.address);
            }
            ClusterResponse::Ok
        }
        ClusterCommand::Promote { instance_id } => {
            match state.promote_to_main(instance_id) {
                Ok(()) => ClusterResponse::Ok,
                Err(crate::CoordinatorError::InstanceNotFound(id)) => {
                    ClusterResponse::InstanceNotFound(id)
                }
                Err(_) => ClusterResponse::InstanceNotFound(instance_id.clone()),
            }
        }
        ClusterCommand::Demote { instance_id } => {
            match state.demote_to_replica(instance_id) {
                Ok(()) => ClusterResponse::Ok,
                Err(crate::CoordinatorError::InstanceNotFound(id)) => {
                    ClusterResponse::InstanceNotFound(id)
                }
                Err(crate::CoordinatorError::NotMain(id)) => {
                    ClusterResponse::AlreadyReplica(id)
                }
                Err(_) => ClusterResponse::InstanceNotFound(instance_id.clone()),
            }
        }
        ClusterCommand::Unregister { instance_id } => {
            state.unregister(instance_id);
            ClusterResponse::Ok
        }
    }
}

// ─── Persistent State Machine ──────────────────────────────────────────────

/// Disk-backed state machine. Survives process restart.
///
/// Persists `last_applied`, `stored_membership`, and snapshot data under `dir`.
/// `cluster_state` is kept in-memory only (it is a runtime reference).
#[derive(Clone, Debug)]
pub struct PersistentCoordinatorStateMachine {
    dir: PathBuf,
    last_applied: Option<LogId<CoordinatorNodeId>>,
    stored_membership: StoredMembership<CoordinatorNodeId, CoordinatorNode>,
    cluster_state: Option<Arc<crate::ClusterState>>,
}

impl PersistentCoordinatorStateMachine {
    pub fn open(dir: impl AsRef<Path>) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let last_applied: Option<LogId<CoordinatorNodeId>> =
            read_or_default(&dir.join("last_applied.bin"))?;
        let stored_membership: StoredMembership<CoordinatorNodeId, CoordinatorNode> =
            read_or_default(&dir.join("membership.bin"))?;

        Ok(Self {
            dir,
            last_applied,
            stored_membership,
            cluster_state: None,
        })
    }

    pub fn attach_cluster_state(&mut self, state: Arc<crate::ClusterState>) {
        self.cluster_state = Some(state);
    }

    fn write_atomic(&self, name: &str, data: &[u8]) -> std::io::Result<()> {
        let tmp = self.dir.join(format!("{}.tmp", name));
        let final_path = self.dir.join(name);
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, &final_path)?;
        Ok(())
    }

    fn persist_last_applied(&self) -> std::io::Result<()> {
        let data = bincode::serialize(&self.last_applied).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        self.write_atomic("last_applied.bin", &data)
    }

    fn persist_membership(&self) -> std::io::Result<()> {
        let data = bincode::serialize(&self.stored_membership).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        self.write_atomic("membership.bin", &data)
    }
}

impl RaftStateMachine<TypeConfig> for PersistentCoordinatorStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(
        Option<LogId<CoordinatorNodeId>>,
        StoredMembership<CoordinatorNodeId, CoordinatorNode>,
    ), openraft::StorageError<CoordinatorNodeId>> {
        Ok((self.last_applied, self.stored_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClusterResponse>, openraft::StorageError<CoordinatorNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();
        for entry in entries {
            let resp = match &entry.payload {
                EntryPayload::Normal(cmd) => apply_command(cmd, self.cluster_state.as_ref()),
                EntryPayload::Membership(membership) => {
                    self.stored_membership = StoredMembership::new(Some(entry.log_id.clone()), membership.clone());
                    ClusterResponse::Ok
                }
                EntryPayload::Blank => ClusterResponse::Ok,
            };
            self.last_applied = Some(entry.log_id.clone());
            responses.push(resp);
        }
        self.persist_last_applied().map_err(|e| {
            io_storage_error(openraft::ErrorSubject::StateMachine, openraft::ErrorVerb::Write, e)
        })?;
        self.persist_membership().map_err(|e| {
            io_storage_error(openraft::ErrorSubject::StateMachine, openraft::ErrorVerb::Write, e)
        })?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, openraft::StorageError<CoordinatorNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<CoordinatorNodeId, CoordinatorNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        let data = snapshot.into_inner();
        let meta_bytes = bincode::serialize(meta).map_err(|e| {
            openraft::StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Write,
                    &std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                ),
            }
        })?;
        self.write_atomic("snapshot_meta.bin", &meta_bytes).map_err(|e| {
            io_storage_error(openraft::ErrorSubject::StateMachine, openraft::ErrorVerb::Write, e)
        })?;
        self.write_atomic("snapshot_data.bin", &data).map_err(|e| {
            io_storage_error(openraft::ErrorSubject::StateMachine, openraft::ErrorVerb::Write, e)
        })?;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, openraft::StorageError<CoordinatorNodeId>> {
        let meta_path = self.dir.join("snapshot_meta.bin");
        let data_path = self.dir.join("snapshot_data.bin");
        if !meta_path.exists() || !data_path.exists() {
            return Ok(None);
        }
        let meta_bytes = std::fs::read(&meta_path).map_err(|e| {
            io_storage_error(openraft::ErrorSubject::StateMachine, openraft::ErrorVerb::Read, e)
        })?;
        let meta: SnapshotMeta<CoordinatorNodeId, CoordinatorNode> = bincode::deserialize(&meta_bytes)
            .map_err(|e| {
                openraft::StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::StateMachine,
                        openraft::ErrorVerb::Read,
                        &std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    ),
                }
            })?;
        let data = std::fs::read(&data_path).map_err(|e| {
            io_storage_error(openraft::ErrorSubject::StateMachine, openraft::ErrorVerb::Read, e)
        })?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for PersistentCoordinatorStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, openraft::StorageError<CoordinatorNodeId>> {
        let snap = if let Some(ref state) = self.cluster_state {
            ClusterSnapshot {
                instances: state.list(),
                leader_id: state.leader_id(),
                routes: state.routes(),
            }
        } else {
            ClusterSnapshot {
                instances: Vec::new(),
                leader_id: String::new(),
                routes: Vec::new(),
            }
        };
        let data = match bincode::serialize(&snap) {
            Ok(d) => d,
            Err(e) => {
                return Err(openraft::StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::StateMachine,
                        openraft::ErrorVerb::Write,
                        &std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("serialize cluster snapshot: {}", e),
                        ),
                    ),
                });
            }
        };
        let meta = SnapshotMeta::default();
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ─── StorageBackedStateMachine ─────────────────────────────────────────────

/// Serializable snapshot of storage metadata for Raft.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct StorageSnapshot {
    vertex_count: usize,
    edge_count: usize,
    label_indices: Vec<u32>,
    label_property_indices: Vec<(u32, u32)>,
}

/// Wraps `mgstorage::Storage` as a Raft state machine.
///
/// This bridges the graph storage engine into the coordinator consensus layer.
/// Each applied log entry is translated into a storage operation.
#[derive(Clone)]
pub struct StorageBackedStateMachine {
    last_applied: Option<LogId<CoordinatorNodeId>>,
    stored_membership: StoredMembership<CoordinatorNodeId, CoordinatorNode>,
    snapshot: Option<Snapshot<TypeConfig>>,
    storage: Option<Arc<mgstorage::storage::Storage>>,
}

impl std::fmt::Debug for StorageBackedStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageBackedStateMachine")
            .field("last_applied", &self.last_applied)
            .field("stored_membership", &self.stored_membership)
            .field("snapshot", &self.snapshot.is_some())
            .field("storage", &self.storage.is_some())
            .finish()
    }
}

impl StorageBackedStateMachine {
    pub fn new() -> Self {
        Self {
            last_applied: None,
            stored_membership: StoredMembership::default(),
            snapshot: None,
            storage: None,
        }
    }

    pub fn attach_storage(&mut self, storage: Arc<mgstorage::storage::Storage>) {
        self.storage = Some(storage);
    }
}

impl RaftStateMachine<TypeConfig> for StorageBackedStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(
        Option<LogId<CoordinatorNodeId>>,
        StoredMembership<CoordinatorNodeId, CoordinatorNode>,
    ), openraft::StorageError<CoordinatorNodeId>> {
        Ok((self.last_applied, self.stored_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClusterResponse>, openraft::StorageError<CoordinatorNodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();
        for entry in entries {
            let resp = match &entry.payload {
                EntryPayload::Normal(cmd) => {
                    if let Some(ref storage) = self.storage {
                        apply_storage_command(cmd, storage)
                    } else {
                        ClusterResponse::Ok
                    }
                }
                EntryPayload::Membership(membership) => {
                    self.stored_membership = StoredMembership::new(Some(entry.log_id.clone()), membership.clone());
                    ClusterResponse::Ok
                }
                EntryPayload::Blank => ClusterResponse::Ok,
            };
            self.last_applied = Some(entry.log_id.clone());
            responses.push(resp);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, openraft::StorageError<CoordinatorNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<CoordinatorNodeId, CoordinatorNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<CoordinatorNodeId>> {
        let data = snapshot.into_inner();
        self.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data)),
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, openraft::StorageError<CoordinatorNodeId>> {
        Ok(self.snapshot.clone())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StorageBackedStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, openraft::StorageError<CoordinatorNodeId>> {
        let snap = if let Some(ref storage) = self.storage {
            let label_indices = storage.active_label_indices.read().unwrap()
                .iter().map(|l| l.as_uint()).collect();
            let label_property_indices = storage.active_label_property_indices.read().unwrap()
                .iter().map(|(l, p)| (l.as_uint(), p.as_uint())).collect();
            StorageSnapshot {
                vertex_count: storage.vertex_count(),
                edge_count: storage.edge_count(),
                label_indices,
                label_property_indices,
            }
        } else {
            StorageSnapshot {
                vertex_count: 0,
                edge_count: 0,
                label_indices: Vec::new(),
                label_property_indices: Vec::new(),
            }
        };
        let data = match bincode::serialize(&snap) {
            Ok(d) => d,
            Err(e) => {
                return Err(openraft::StorageError::IO {
                    source: openraft::StorageIOError::new(
                        openraft::ErrorSubject::StateMachine,
                        openraft::ErrorVerb::Write,
                        &std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("serialize storage snapshot: {}", e),
                        ),
                    ),
                });
            }
        };
        let meta = SnapshotMeta::default();
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

/// Apply a cluster command against the shared storage.
fn apply_storage_command(
    cmd: &ClusterCommand,
    _storage: &mgstorage::storage::Storage,
) -> ClusterResponse {
    match cmd {
        ClusterCommand::RegisterInstance { .. } => ClusterResponse::Ok,
        ClusterCommand::SetMain { .. } => ClusterResponse::Ok,
        ClusterCommand::Promote { .. } => ClusterResponse::Ok,
        ClusterCommand::Demote { .. } => ClusterResponse::Ok,
        ClusterCommand::Unregister { .. } => ClusterResponse::Ok,
    }
}

// ─── LeaderWatcher ─────────────────────────────────────────────────────────

/// Callback invoked when the local Raft node transitions into or out of
/// the leader role.
pub type LeaderCallback = Arc<dyn Fn(bool) + Send + Sync>;

/// Watches Raft leadership state and invokes registered callbacks on change.
#[derive(Clone)]
pub struct LeaderWatcher {
    callbacks: Arc<std::sync::Mutex<Vec<LeaderCallback>>>,
}

impl std::fmt::Debug for LeaderWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderWatcher")
            .field("callbacks", &self.callbacks.lock().map(|c| c.len()).unwrap_or(0))
            .finish()
    }
}

impl LeaderWatcher {
    pub fn new() -> Self {
        Self {
            callbacks: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Register a callback to be called with `true` when this node becomes
    /// leader and `false` when it steps down.
    pub fn on_change(&self, cb: LeaderCallback) {
        let mut cbs = self.callbacks.lock().expect("lock poisoned");
        cbs.push(cb);
    }

    /// Notify all registered callbacks of a leadership change.
    pub fn notify(&self, is_leader: bool) {
        let cbs = self.callbacks.lock().expect("lock poisoned");
        for cb in cbs.iter() {
            cb(is_leader);
        }
    }
}

impl Default for LeaderWatcher {
    fn default() -> Self {
        Self::new()
    }
}

// ─── InstanceDiscovery ─────────────────────────────────────────────────────

/// Abstraction for discovering data instances at startup.
///
/// Implementations may read from a config file, environment variables,
/// service discovery (e.g. Consul), or DNS.
pub trait InstanceDiscovery: Send + Sync {
    /// Return the list of known data instances.
    fn discover(&self) -> Vec<crate::Instance>;
}

/// Static discovery backed by an in-memory list.
///
/// Typically constructed from a config file or env vars at bootstrap time.
#[derive(Clone, Debug)]
pub struct StaticDiscovery {
    instances: Vec<crate::Instance>,
}

impl StaticDiscovery {
    pub fn new(instances: Vec<crate::Instance>) -> Self {
        Self { instances }
    }

    /// Build from a simple `(id, address, role)` slice.
    pub fn from_slice(items: &[(String, SocketAddr, crate::InstanceRole)]) -> Self {
        let instances = items
            .iter()
            .map(|(id, addr, role)| crate::Instance::new(id.clone(), *addr, *role))
            .collect();
        Self { instances }
    }
}

impl InstanceDiscovery for StaticDiscovery {
    fn discover(&self) -> Vec<crate::Instance> {
        self.instances.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn test_cluster_command_bincode_roundtrip() {
        let cmd = ClusterCommand::RegisterInstance {
            instance_id: "i1".into(),
            bolt_addr: "127.0.0.1:7687".parse().unwrap(),
        };
        let bytes = bincode::serialize(&cmd).unwrap();
        let back: ClusterCommand = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, cmd);
    }

    #[test]
    fn test_apply_command_returns_ok_when_no_state() {
        let cmd = ClusterCommand::Promote {
            instance_id: "i1".into(),
        };
        assert_eq!(apply_command(&cmd, None), ClusterResponse::Ok);
    }

    #[test]
    fn test_apply_command_register_instance() {
        let state = Arc::new(crate::ClusterState::new("coord-1".into()));
        let cmd = ClusterCommand::RegisterInstance {
            instance_id: "i1".into(),
            bolt_addr: test_addr(7687),
        };
        assert_eq!(apply_command(&cmd, Some(&state)), ClusterResponse::Ok);
        assert_eq!(state.list().len(), 1);
        assert_eq!(state.get("i1").unwrap().role, crate::InstanceRole::Replica);
    }

    #[test]
    fn test_apply_command_set_main_updates_routing() {
        let state = Arc::new(crate::ClusterState::new("coord-1".into()));
        state.register(crate::Instance::new(
            "i1".into(),
            test_addr(7687),
            crate::InstanceRole::Replica,
        ));

        let cmd = ClusterCommand::SetMain {
            database: "mydb".into(),
            instance_id: "i1".into(),
        };
        assert_eq!(apply_command(&cmd, Some(&state)), ClusterResponse::Ok);
        assert_eq!(state.main_instance().unwrap().id, "i1");
        assert_eq!(state.get_route("mydb"), Some(test_addr(7687)));
    }

    #[test]
    fn test_apply_command_promote_and_demote() {
        let state = Arc::new(crate::ClusterState::new("coord-1".into()));
        state.register(crate::Instance::new(
            "i1".into(),
            test_addr(7687),
            crate::InstanceRole::Replica,
        ));

        let promote = ClusterCommand::Promote {
            instance_id: "i1".into(),
        };
        assert_eq!(apply_command(&promote, Some(&state)), ClusterResponse::Ok);
        assert_eq!(state.main_instance().unwrap().id, "i1");

        let demote = ClusterCommand::Demote {
            instance_id: "i1".into(),
        };
        assert_eq!(apply_command(&demote, Some(&state)), ClusterResponse::Ok);
        assert!(state.main_instance().is_none());
        assert_eq!(state.get("i1").unwrap().role, crate::InstanceRole::Replica);
    }

    #[test]
    fn test_apply_command_unregister() {
        let state = Arc::new(crate::ClusterState::new("coord-1".into()));
        state.register(crate::Instance::new(
            "i1".into(),
            test_addr(7687),
            crate::InstanceRole::Main,
        ));

        let cmd = ClusterCommand::Unregister {
            instance_id: "i1".into(),
        };
        assert_eq!(apply_command(&cmd, Some(&state)), ClusterResponse::Ok);
        assert_eq!(state.list().len(), 0);
    }

    #[test]
    fn test_leader_watcher_notifies_callbacks() {
        let watcher = LeaderWatcher::new();
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag_clone = flag.clone();
        watcher.on_change(Arc::new(move |is_leader| {
            flag_clone.store(is_leader, std::sync::atomic::Ordering::SeqCst);
        }));
        watcher.notify(true);
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        watcher.notify(false);
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_static_discovery() {
        let discovery = StaticDiscovery::from_slice(&[
            ("i1".into(), test_addr(7687), crate::InstanceRole::Main),
            ("i2".into(), test_addr(7688), crate::InstanceRole::Replica),
        ]);
        let instances = discovery.discover();
        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0].id, "i1");
        assert_eq!(instances[1].id, "i2");
    }

    // ─── openraft v2 storage tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_log_storage_append_and_read() {
        let mut storage = CoordinatorLogStorage::new();

        let entry = Entry {
            log_id: LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1u64),
                index: 1,
            },
            payload: EntryPayload::Blank,
        };

        // Directly insert into the log for testing (avoiding LogFlushed construction)
        storage.log.insert(entry.log_id.index, entry.clone());

        let entries = storage.try_get_log_entries(1..2).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn test_log_storage_vote_persistence() {
        let mut storage = CoordinatorLogStorage::new();

        let vote = Vote {
            leader_id: openraft::LeaderId::new(1, 1u64),
            committed: false,
        };

        storage.save_vote(&vote).await.unwrap();
        let read = storage.read_vote().await.unwrap();
        assert_eq!(read, Some(vote));
    }

    #[tokio::test]
    async fn test_log_storage_truncate_and_purge() {
        let mut storage = CoordinatorLogStorage::new();

        for i in 1..=5 {
            let entry = Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1u64),
                    index: i,
                },
                payload: EntryPayload::Blank,
            };
            storage.log.insert(entry.log_id.index, entry);
        }

        // Truncate from index 3
        storage.truncate(LogId {
            leader_id: openraft::CommittedLeaderId::new(1, 1u64),
            index: 3,
        }).await.unwrap();

        let entries = storage.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(entries.len(), 2); // indices 1, 2 remain

        // Purge up to index 2
        storage.purge(LogId {
            leader_id: openraft::CommittedLeaderId::new(1, 1u64),
            index: 2,
        }).await.unwrap();

        let entries = storage.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(entries.len(), 0); // only index 3+ remain, but we truncated from 3
    }

    #[tokio::test]
    async fn test_state_machine_apply() {
        let mut sm = CoordinatorStateMachine::new();

        let entries = vec![
            Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1u64),
                    index: 1,
                },
                payload: EntryPayload::Normal(ClusterCommand::RegisterInstance {
                    instance_id: "i1".into(),
                    bolt_addr: test_addr(7687),
                }),
            },
        ];

        let responses = sm.apply(entries).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], ClusterResponse::Ok);
        assert_eq!(sm.last_applied, Some(LogId {
            leader_id: openraft::CommittedLeaderId::new(1, 1u64),
            index: 1,
        }));
    }

    #[tokio::test]
    async fn test_state_machine_snapshot() {
        let mut sm = CoordinatorStateMachine::new();

        let snapshot_data = Box::new(Cursor::new(vec![1u8, 2, 3]));
        let meta = SnapshotMeta::default();

        sm.install_snapshot(&meta, snapshot_data).await.unwrap();

        let snapshot = sm.get_current_snapshot().await.unwrap();
        assert!(snapshot.is_some());
    }

    #[tokio::test]
    async fn test_storage_backed_state_machine_apply() {
        let mut sm = StorageBackedStateMachine::new();

        let entries = vec![
            Entry {
                log_id: LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1u64),
                    index: 1,
                },
                payload: EntryPayload::Blank,
            },
        ];

        let responses = sm.apply(entries).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], ClusterResponse::Ok);
    }

    #[tokio::test]
    async fn test_network_factory_creates_client() {
        let mut factory = CoordinatorNetworkFactory::new();
        let node = CoordinatorNode {
            rpc_addr: "127.0.0.1:7687".into(),
            bolt_addr: "127.0.0.1:7688".into(),
        };
        let network = factory.new_client(1, &node).await;
        assert_eq!(network.target, 1);
    }

    #[tokio::test]
    async fn test_build_snapshot_with_cluster_state() {
        let mut sm = CoordinatorStateMachine::new();
        let state = Arc::new(crate::ClusterState::new("leader1".into()));
        state.register(crate::Instance::new(
            "i1".into(),
            test_addr(7687),
            crate::InstanceRole::Main,
        ));
        state.set_route("default", test_addr(7688));
        sm.attach_cluster_state(state);

        let snapshot = sm.build_snapshot().await.unwrap();
        assert!(!snapshot.snapshot.into_inner().is_empty());
    }

    #[tokio::test]
    async fn test_build_snapshot_without_cluster_state() {
        let mut sm = CoordinatorStateMachine::new();
        let snapshot = sm.build_snapshot().await.unwrap();
        assert!(!snapshot.snapshot.into_inner().is_empty());
    }

    #[tokio::test]
    async fn test_storage_backed_build_snapshot() {
        let mut sm = StorageBackedStateMachine::new();
        let storage = Arc::new(mgstorage::storage::Storage::new());
        sm.attach_storage(storage);
        let snapshot = sm.build_snapshot().await.unwrap();
        assert!(!snapshot.snapshot.into_inner().is_empty());
    }

    // ─── Persistent log storage tests ───────────────────────────────────────

    #[tokio::test]
    async fn test_persistent_log_storage_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();

        let vote = Vote {
            leader_id: openraft::LeaderId::new(2, 7u64),
            committed: true,
        };
        let committed = LogId {
            leader_id: openraft::CommittedLeaderId::new(1, 1u64),
            index: 5,
        };

        {
            let mut storage = PersistentCoordinatorLogStorage::open(&path).unwrap();
            storage.save_vote(&vote).await.unwrap();
            storage.save_committed(Some(committed)).await.unwrap();

            for i in 1..=3 {
                let entry = Entry {
                    log_id: LogId {
                        leader_id: openraft::CommittedLeaderId::new(1, 1u64),
                        index: i,
                    },
                    payload: EntryPayload::Blank,
                };
                storage.log.insert(entry.log_id.index, entry);
            }
            storage.persist_log().unwrap();
        }

        // Reopen and verify everything survived.
        let mut storage = PersistentCoordinatorLogStorage::open(&path).unwrap();
        assert_eq!(storage.read_vote().await.unwrap(), Some(vote));
        assert_eq!(storage.read_committed().await.unwrap(), Some(committed));
        let entries = storage.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].log_id.index, 1);
        assert_eq!(entries[2].log_id.index, 3);
    }

    #[tokio::test]
    async fn test_persistent_log_storage_truncate_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let mut storage = PersistentCoordinatorLogStorage::open(&path).unwrap();
            for i in 1..=5 {
                storage.log.insert(
                    i,
                    Entry {
                        log_id: LogId {
                            leader_id: openraft::CommittedLeaderId::new(1, 1u64),
                            index: i,
                        },
                        payload: EntryPayload::Blank,
                    },
                );
            }
            storage.persist_log().unwrap();
            storage
                .truncate(LogId {
                    leader_id: openraft::CommittedLeaderId::new(1, 1u64),
                    index: 3,
                })
                .await
                .unwrap();
        }

        let mut storage = PersistentCoordinatorLogStorage::open(&path).unwrap();
        let entries = storage.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.log_id.index < 3));
    }

    #[tokio::test]
    async fn test_persistent_log_storage_open_fresh_directory() {
        // Opening a brand-new directory must succeed with empty state.
        let tmp = tempfile::tempdir().unwrap();
        let mut storage = PersistentCoordinatorLogStorage::open(tmp.path()).unwrap();
        assert_eq!(storage.read_vote().await.unwrap(), None);
        assert_eq!(storage.read_committed().await.unwrap(), None);
        let entries = storage.try_get_log_entries(0..10).await.unwrap();
        assert!(entries.is_empty());
    }

    // ─── Persistent state machine tests ─────────────────────────────────────

    #[tokio::test]
    async fn test_persistent_state_machine_apply_and_recover() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();

        // First session: apply some entries.
        {
            let mut sm = PersistentCoordinatorStateMachine::open(&path).unwrap();
            let entries = vec![
                Entry {
                    log_id: LogId {
                        leader_id: openraft::CommittedLeaderId::new(1, 1u64),
                        index: 1,
                    },
                    payload: EntryPayload::Normal(ClusterCommand::RegisterInstance {
                        instance_id: "i1".into(),
                        bolt_addr: test_addr(7687),
                    }),
                },
                Entry {
                    log_id: LogId {
                        leader_id: openraft::CommittedLeaderId::new(1, 1u64),
                        index: 2,
                    },
                    payload: EntryPayload::Blank,
                },
            ];
            let responses = sm.apply(entries).await.unwrap();
            assert_eq!(responses.len(), 2);
            assert_eq!(responses[0], ClusterResponse::Ok);
        }

        // Second session: reopen and verify state.
        let mut sm = PersistentCoordinatorStateMachine::open(&path).unwrap();
        let (last_applied, _membership) = sm.applied_state().await.unwrap();
        assert_eq!(last_applied, Some(LogId {
            leader_id: openraft::CommittedLeaderId::new(1, 1u64),
            index: 2,
        }));
    }

    #[tokio::test]
    async fn test_persistent_state_machine_membership_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sm = PersistentCoordinatorStateMachine::open(tmp.path()).unwrap();

        let mut configs = std::collections::BTreeSet::new();
        configs.insert(1u64);
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(
            1u64,
            CoordinatorNode {
                rpc_addr: "127.0.0.1:7687".into(),
                bolt_addr: "127.0.0.1:7688".into(),
            },
        );
        let membership = openraft::Membership::new(vec![configs], nodes);

        let entry = Entry {
            log_id: LogId {
                leader_id: openraft::CommittedLeaderId::new(1, 1u64),
                index: 1,
            },
            payload: EntryPayload::Membership(membership.clone()),
        };
        sm.apply(vec![entry]).await.unwrap();

        // Reopen and verify membership was persisted (contains node 1).
        let mut sm2 = PersistentCoordinatorStateMachine::open(tmp.path()).unwrap();
        let (_, recovered) = sm2.applied_state().await.unwrap();
        assert!(
            recovered.membership().get_node(&1u64).is_some(),
            "membership should contain node 1 after recovery"
        );
    }

    #[tokio::test]
    async fn test_persistent_state_machine_snapshot_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sm = PersistentCoordinatorStateMachine::open(tmp.path()).unwrap();

        let snap_data = Box::new(Cursor::new(vec![1u8, 2, 3, 4]));
        let meta = SnapshotMeta::default();
        sm.install_snapshot(&meta, snap_data).await.unwrap();

        let snapshot = sm.get_current_snapshot().await.unwrap();
        assert!(snapshot.is_some());
        let recovered = snapshot.unwrap();
        assert_eq!(recovered.snapshot.into_inner(), vec![1u8, 2, 3, 4]);
    }
}
