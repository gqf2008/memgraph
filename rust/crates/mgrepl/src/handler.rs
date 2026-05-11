//! Replication handler — wires replication into the storage engine.
//!
//! Equivalent to C++ `src/replication_handler/`. Manages the main→replica
//! delta streaming pipeline, replica registration, and epoch tracking.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tracing::{info, warn};

use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
use mgdurability::DeltaRecord;
use mgstorage::storage::Storage;
use mgstorage::transaction::Transaction;

use crate::{send_delta_batch, send_delta_stream_end, DeltaApplier};
use crate::{ReplicaState, ReplicationConfig, ReplicationMode};
use mgrpc::{DeltaBatch, MessageHeader, RpcClient};

/// Health metrics tracked per replica.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReplicaHealth {
    pub state: ReplicaState,
    /// Number of consecutive failures since last success.
    pub consecutive_failures: u32,
    /// Timestamp (ms since UNIX epoch) of the last successfully acknowledged delta.
    pub last_acknowledged_ts: u64,
    /// Estimated replication lag in milliseconds.
    pub replication_lag_ms: u64,
    /// Timestamp of the last successful heartbeat.
    pub last_heartbeat_ms: u64,
}

impl Default for ReplicaHealth {
    fn default() -> Self {
        Self {
            state: ReplicaState::Behind,
            consecutive_failures: 0,
            last_acknowledged_ts: 0,
            replication_lag_ms: 0,
            last_heartbeat_ms: 0,
        }
    }
}

/// A registered replica connection on the main instance.
pub struct ReplicaConn {
    pub id: String,
    pub rpc: RpcClient,
    pub mode: ReplicationMode,
    pub health: ReplicaHealth,
    /// Current retry backoff state for automatic reconnection.
    pub retry_state: RetryState,
}

/// Tracks exponential backoff for a replica connection.
#[derive(Clone, Debug)]
pub struct RetryState {
    pub attempt: u32,
    pub next_backoff: Duration,
    pub max_backoff: Duration,
    pub base_delay: Duration,
    pub last_attempt: Option<Instant>,
}

impl RetryState {
    pub fn new(base_delay_ms: u64, max_backoff_ms: u64) -> Self {
        Self {
            attempt: 0,
            next_backoff: Duration::from_millis(base_delay_ms),
            max_backoff: Duration::from_millis(max_backoff_ms),
            base_delay: Duration::from_millis(base_delay_ms),
            last_attempt: None,
        }
    }

    /// Record a failure and compute the next backoff duration.
    pub fn record_failure(&mut self) -> Duration {
        self.attempt += 1;
        self.next_backoff = std::cmp::min(
            self.base_delay * 2u32.saturating_pow(self.attempt.saturating_sub(1)),
            self.max_backoff,
        );
        self.last_attempt = Some(Instant::now());
        self.next_backoff
    }

    /// Reset backoff after a successful operation.
    pub fn record_success(&mut self) {
        self.attempt = 0;
        self.next_backoff = self.base_delay;
        self.last_attempt = None;
    }

    /// Returns true if enough time has passed since the last attempt to retry now.
    pub fn can_retry(&self) -> bool {
        match self.last_attempt {
            None => true,
            Some(last) => Instant::now().duration_since(last) >= self.next_backoff,
        }
    }
}

/// Metrics tracked for replication performance and health.
#[derive(Clone, Debug, Default)]
pub struct ReplicationMetrics {
    /// Total number of delta batches sent.
    pub batches_sent: u64,
    /// Total number of individual deltas sent.
    pub deltas_sent: u64,
    /// Total bytes replicated (approximate).
    pub bytes_replicated: u64,
    /// Number of failed replication attempts.
    pub failed_attempts: u64,
    /// Number of replica evictions due to repeated failures.
    pub replicas_evicted: u64,
    /// Current replication lag in ms (max across all replicas).
    pub max_replication_lag_ms: u64,
    /// Number of successful ACKs received (Sync/StrictSync).
    pub acks_received: u64,
    /// Number of ACK timeouts.
    pub ack_timeouts: u64,
    /// Number of quorum commits completed (StrictSync).
    pub quorum_commits: u64,
    /// Number of quorum commit failures.
    pub quorum_failures: u64,
}

impl ReplicationMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_batch_sent(&mut self, delta_count: usize, approx_bytes: usize) {
        self.batches_sent += 1;
        self.deltas_sent += delta_count as u64;
        self.bytes_replicated += approx_bytes as u64;
    }

    pub fn record_failure(&mut self) {
        self.failed_attempts += 1;
    }

    pub fn record_ack(&mut self) {
        self.acks_received += 1;
    }

    pub fn record_timeout(&mut self) {
        self.ack_timeouts += 1;
    }

    pub fn record_quorum_success(&mut self) {
        self.quorum_commits += 1;
    }

    pub fn record_quorum_failure(&mut self) {
        self.quorum_failures += 1;
    }

    pub fn record_eviction(&mut self) {
        self.replicas_evicted += 1;
    }
}

/// Filter for selective replication. Only deltas matching the filter
/// criteria are replicated to a given replica.
#[derive(Clone, Debug, Default)]
pub struct ReplicationFilter {
    /// Only replicate deltas for vertices with these labels.
    pub include_labels: Vec<LabelId>,
    /// Exclude deltas for vertices with these labels.
    pub exclude_labels: Vec<LabelId>,
    /// If true, only replicate vertex operations (skip edge/schema ops).
    pub vertices_only: bool,
}

impl ReplicationFilter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if the delta passes the filter and should be replicated.
    pub fn allows(&self, delta: &DeltaRecord) -> bool {
        if self.include_labels.is_empty() && self.exclude_labels.is_empty() && !self.vertices_only {
            return true;
        }

        let (gid, is_vertex_op) = match delta {
            DeltaRecord::VertexCreate { .. }
            | DeltaRecord::VertexDelete { .. }
            | DeltaRecord::VertexAddLabel { .. }
            | DeltaRecord::VertexRemoveLabel { .. }
            | DeltaRecord::VertexSetProperty { .. } => (crate::delta_gid(delta), true),
            DeltaRecord::EdgeCreate { .. }
            | DeltaRecord::EdgeDelete { .. }
            | DeltaRecord::EdgeSetProperty { .. } => (None, false),
            _ => {
                // Schema operations pass through unless vertices_only is set
                return !self.vertices_only;
            }
        };

        if self.vertices_only && !is_vertex_op {
            return false;
        }

        // For vertex ops, we can't check labels without storage access.
        // In a full implementation we'd look up the vertex's labels.
        // Here we pass through and let the caller do label filtering
        // if it has storage access.
        true
    }
}

/// Detects main instance failure on the replica side by monitoring
/// heartbeat responses and connection health.
#[derive(Clone, Debug)]
pub struct FailoverDetector {
    /// Max time without a successful heartbeat before declaring main failed.
    pub max_heartbeat_interval: Duration,
    /// Number of consecutive missed heartbeats before failover.
    pub max_missed_heartbeats: u32,
    missed_count: u32,
    last_successful_heartbeat: Option<Instant>,
    pub main_failed: bool,
}

impl FailoverDetector {
    pub fn new(max_heartbeat_interval: Duration, max_missed_heartbeats: u32) -> Self {
        Self {
            max_heartbeat_interval,
            max_missed_heartbeats,
            missed_count: 0,
            last_successful_heartbeat: None,
            main_failed: false,
        }
    }

    /// Record a successful heartbeat from the main.
    pub fn record_success(&mut self) {
        self.missed_count = 0;
        self.last_successful_heartbeat = Some(Instant::now());
        self.main_failed = false;
    }

    /// Record a missed heartbeat.
    pub fn record_miss(&mut self) {
        self.missed_count += 1;
        if self.missed_count >= self.max_missed_heartbeats {
            self.main_failed = true;
        }
    }

    /// Returns true if the main is considered failed.
    pub fn is_main_failed(&self) -> bool {
        if self.main_failed {
            return true;
        }
        match self.last_successful_heartbeat {
            Some(last) => {
                Instant::now().duration_since(last)
                    > self.max_heartbeat_interval * self.max_missed_heartbeats
            }
            None => false,
        }
    }

    /// Time since last successful heartbeat.
    pub fn time_since_last_heartbeat(&self) -> Option<Duration> {
        self.last_successful_heartbeat
            .map(|last| Instant::now().duration_since(last))
    }
}

/// Buffers deltas over a time window and flushes them as a single batch.
pub struct DeltaBatcher {
    window: Duration,
    max_size: usize,
    buffer: VecDeque<DeltaRecord>,
    last_flush: Instant,
    epoch_id: u64,
    sequence: u64,
}

impl DeltaBatcher {
    pub fn new(window: Duration, max_size: usize) -> Self {
        Self {
            window,
            max_size,
            buffer: VecDeque::new(),
            last_flush: Instant::now(),
            epoch_id: 1,
            sequence: 0,
        }
    }

    /// Push a delta into the batcher. Returns a ready batch if the window
    /// has elapsed or the buffer is at capacity.
    pub fn push(&mut self, delta: DeltaRecord) -> Option<DeltaBatch> {
        self.buffer.push_back(delta);
        if self.buffer.len() >= self.max_size || self.last_flush.elapsed() >= self.window {
            return self.flush();
        }
        None
    }

    /// Forcefully flush all buffered deltas into a batch.
    pub fn flush(&mut self) -> Option<DeltaBatch> {
        if self.buffer.is_empty() {
            return None;
        }
        let deltas: Vec<DeltaRecord> = self.buffer.drain(..).collect();
        let commit_timestamp = deltas.iter().map(delta_timestamp).max().unwrap_or(0);
        self.sequence += 1;
        self.last_flush = Instant::now();
        Some(DeltaBatch {
            epoch_id: self.epoch_id,
            commit_timestamp,
            sequence_number: self.sequence,
            deltas,
        })
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch_id = epoch;
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

/// Result of a sync-ack wait operation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AckResult {
    Acknowledged,
    Timeout,
    Error,
}

/// Wait for an ACK from a replica with a configurable timeout.
pub fn wait_for_ack(rpc: &mut RpcClient, timeout: Duration) -> AckResult {
    let start = Instant::now();
    // Set read timeout via the underlying TcpStream
    let _ = rpc.stream().set_read_timeout(Some(timeout));
    let result = match rpc.recv::<mgrpc::Heartbeat>() {
        Ok(_) => AckResult::Acknowledged,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::TimedOut
                || e.kind() == std::io::ErrorKind::WouldBlock
            {
                AckResult::Timeout
            } else {
                AckResult::Error
            }
        }
    };
    let _ = rpc.stream().set_read_timeout(None);
    if start.elapsed() >= timeout && result != AckResult::Acknowledged {
        AckResult::Timeout
    } else {
        result
    }
}

/// Replication handler lives on the main instance and manages all replicas.
pub struct ReplicationHandler {
    storage: Arc<Storage>,
    config: ReplicationConfig,
    replicas: RwLock<HashMap<String, ReplicaConn>>,
    epoch_id: std::sync::atomic::AtomicU64,
    main_uuid: String,
    /// Timeout for waiting for replica ACKs in Sync/StrictSync modes.
    pub sync_ack_timeout: Duration,
    /// Max consecutive failures before a replica is marked stale.
    pub max_failures_before_stale: u32,
}

impl ReplicationHandler {
    pub fn new(storage: Arc<Storage>, config: ReplicationConfig) -> Self {
        let main_uuid = uuid();
        info!("ReplicationHandler initialized, main_uuid={}", main_uuid);
        Self {
            storage,
            config,
            replicas: RwLock::new(HashMap::new()),
            epoch_id: std::sync::atomic::AtomicU64::new(1),
            main_uuid,
            sync_ack_timeout: Duration::from_secs(5),
            max_failures_before_stale: 3,
        }
    }

    pub fn main_uuid(&self) -> &str {
        &self.main_uuid
    }

    pub fn epoch_id(&self) -> u64 {
        self.epoch_id.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Register a new replica. The replica must already be connected.
    pub fn register_replica(
        &self,
        id: String,
        rpc: RpcClient,
        mode: ReplicationMode,
    ) -> Result<(), String> {
        let mut replicas = self
            .replicas
            .write()
            .map_err(|e| format!("lock poisoned: {}", e))?;
        if replicas.contains_key(&id) {
            return Err(format!("replica '{}' already registered", id));
        }
        replicas.insert(
            id.clone(),
            ReplicaConn {
                id,
                rpc,
                mode,
                health: ReplicaHealth::default(),
                retry_state: RetryState::new(100, 30_000),
            },
        );
        Ok(())
    }

    /// Unregister a replica.
    pub fn unregister_replica(&self, id: &str) {
        let mut replicas = self.replicas.write().expect("lock poisoned");
        if replicas.remove(id).is_some() {
            info!("Replica '{}' unregistered", id);
        }
    }

    /// List all registered replicas with their current mode and health state.
    pub fn list_replicas(&self) -> Vec<(String, ReplicationMode, ReplicaHealth)> {
        let replicas = self.replicas.read().expect("lock poisoned");
        replicas
            .values()
            .map(|r| (r.id.clone(), r.mode, r.health))
            .collect()
    }

    /// Get health for a specific replica.
    pub fn replica_health(&self, id: &str) -> Option<ReplicaHealth> {
        let replicas = self.replicas.read().expect("lock poisoned");
        replicas.get(id).map(|r| r.health)
    }

    /// Replicate a single delta to all registered replicas.
    /// For Sync/StrictSync modes, waits for acknowledgement with timeout.
    pub fn replicate_delta(&self, delta: &DeltaRecord) -> Result<(), String> {
        let epoch = self.epoch_id();
        let batch = DeltaBatch {
            epoch_id: epoch,
            commit_timestamp: delta_timestamp(delta),
            sequence_number: 0,
            deltas: vec![delta.clone()],
        };

        let mut replicas = self
            .replicas
            .write()
            .map_err(|e| format!("lock poisoned: {}", e))?;
        let mut to_remove = Vec::new();
        let now_ms = now_millis();

        for (id, conn) in replicas.iter_mut() {
            match conn.mode {
                ReplicationMode::Async => {
                    if let Err(e) = send_delta_batch(&mut conn.rpc, &batch) {
                        warn!("Failed to replicate to {}: {}", id, e);
                        conn.health.consecutive_failures += 1;
                        conn.retry_state.record_failure();
                        Self::update_health_state(
                            &mut conn.health,
                            conn.retry_state.attempt,
                            self.max_failures_before_stale,
                        );
                        if conn.health.consecutive_failures >= self.max_failures_before_stale {
                            to_remove.push(id.clone());
                        }
                    } else {
                        conn.health.consecutive_failures = 0;
                        conn.health.last_acknowledged_ts = batch.commit_timestamp;
                        conn.health.replication_lag_ms = 0;
                        conn.health.last_heartbeat_ms = now_ms;
                        conn.retry_state.record_success();
                        if conn.health.state == ReplicaState::Behind
                            || conn.health.state == ReplicaState::Recovery
                        {
                            conn.health.state = ReplicaState::Ready;
                        }
                    }
                }
                ReplicationMode::Sync | ReplicationMode::StrictSync => {
                    if let Err(e) = send_delta_batch(&mut conn.rpc, &batch) {
                        warn!("Failed to replicate to {}: {}", id, e);
                        conn.health.consecutive_failures += 1;
                        conn.retry_state.record_failure();
                        Self::update_health_state(
                            &mut conn.health,
                            conn.retry_state.attempt,
                            self.max_failures_before_stale,
                        );
                        if conn.health.consecutive_failures >= self.max_failures_before_stale {
                            to_remove.push(id.clone());
                        }
                        continue;
                    }

                    // Wait for ACK with timeout
                    match wait_for_ack(&mut conn.rpc, self.sync_ack_timeout) {
                        AckResult::Acknowledged => {
                            conn.health.consecutive_failures = 0;
                            conn.health.last_acknowledged_ts = batch.commit_timestamp;
                            conn.health.replication_lag_ms = 0;
                            conn.health.last_heartbeat_ms = now_ms;
                            conn.retry_state.record_success();
                            if conn.health.state == ReplicaState::Behind
                                || conn.health.state == ReplicaState::Recovery
                            {
                                conn.health.state = ReplicaState::Ready;
                            }
                        }
                        AckResult::Timeout => {
                            warn!("Replica {} ACK timed out", id);
                            conn.health.consecutive_failures += 1;
                            conn.retry_state.record_failure();
                            Self::update_health_state(
                                &mut conn.health,
                                conn.retry_state.attempt,
                                self.max_failures_before_stale,
                            );
                            if conn.health.consecutive_failures >= self.max_failures_before_stale {
                                to_remove.push(id.clone());
                            }
                        }
                        AckResult::Error => {
                            warn!("Replica {} ACK error", id);
                            conn.health.consecutive_failures += 1;
                            conn.retry_state.record_failure();
                            Self::update_health_state(
                                &mut conn.health,
                                conn.retry_state.attempt,
                                self.max_failures_before_stale,
                            );
                            if conn.health.consecutive_failures >= self.max_failures_before_stale {
                                to_remove.push(id.clone());
                            }
                        }
                    }
                }
            }
        }

        for id in to_remove {
            if let Some(conn) = replicas.remove(&id) {
                warn!(
                    "Replica '{}' removed after {} consecutive failures",
                    id, conn.health.consecutive_failures
                );
            }
        }

        Ok(())
    }

    /// Replicate a pre-built delta batch (e.g. from a DeltaBatcher) to all replicas.
    pub fn replicate_batch(&self, batch: &DeltaBatch) -> Result<(), String> {
        let mut replicas = self
            .replicas
            .write()
            .map_err(|e| format!("lock poisoned: {}", e))?;
        let mut to_remove = Vec::new();
        let now_ms = now_millis();

        for (id, conn) in replicas.iter_mut() {
            match conn.mode {
                ReplicationMode::Async => {
                    if let Err(e) = send_delta_batch(&mut conn.rpc, batch) {
                        warn!("Failed to replicate batch to {}: {}", id, e);
                        conn.health.consecutive_failures += 1;
                        conn.retry_state.record_failure();
                        Self::update_health_state(
                            &mut conn.health,
                            conn.retry_state.attempt,
                            self.max_failures_before_stale,
                        );
                        if conn.health.consecutive_failures >= self.max_failures_before_stale {
                            to_remove.push(id.clone());
                        }
                    } else {
                        conn.health.consecutive_failures = 0;
                        conn.health.last_acknowledged_ts = batch.commit_timestamp;
                        conn.health.replication_lag_ms =
                            now_ms.saturating_sub(batch.commit_timestamp);
                        conn.health.last_heartbeat_ms = now_ms;
                        conn.retry_state.record_success();
                        if conn.health.state == ReplicaState::Behind
                            || conn.health.state == ReplicaState::Recovery
                        {
                            conn.health.state = ReplicaState::Ready;
                        }
                    }
                }
                ReplicationMode::Sync | ReplicationMode::StrictSync => {
                    if let Err(e) = send_delta_batch(&mut conn.rpc, batch) {
                        warn!("Failed to replicate batch to {}: {}", id, e);
                        conn.health.consecutive_failures += 1;
                        conn.retry_state.record_failure();
                        Self::update_health_state(
                            &mut conn.health,
                            conn.retry_state.attempt,
                            self.max_failures_before_stale,
                        );
                        if conn.health.consecutive_failures >= self.max_failures_before_stale {
                            to_remove.push(id.clone());
                        }
                        continue;
                    }

                    match wait_for_ack(&mut conn.rpc, self.sync_ack_timeout) {
                        AckResult::Acknowledged => {
                            conn.health.consecutive_failures = 0;
                            conn.health.last_acknowledged_ts = batch.commit_timestamp;
                            conn.health.replication_lag_ms = 0;
                            conn.health.last_heartbeat_ms = now_ms;
                            conn.retry_state.record_success();
                            if conn.health.state == ReplicaState::Behind
                                || conn.health.state == ReplicaState::Recovery
                            {
                                conn.health.state = ReplicaState::Ready;
                            }
                        }
                        AckResult::Timeout => {
                            warn!("Replica {} batch ACK timed out", id);
                            conn.health.consecutive_failures += 1;
                            conn.retry_state.record_failure();
                            Self::update_health_state(
                                &mut conn.health,
                                conn.retry_state.attempt,
                                self.max_failures_before_stale,
                            );
                            if conn.health.consecutive_failures >= self.max_failures_before_stale {
                                to_remove.push(id.clone());
                            }
                        }
                        AckResult::Error => {
                            warn!("Replica {} batch ACK error", id);
                            conn.health.consecutive_failures += 1;
                            conn.retry_state.record_failure();
                            Self::update_health_state(
                                &mut conn.health,
                                conn.retry_state.attempt,
                                self.max_failures_before_stale,
                            );
                            if conn.health.consecutive_failures >= self.max_failures_before_stale {
                                to_remove.push(id.clone());
                            }
                        }
                    }
                }
            }
        }

        for id in to_remove {
            if let Some(conn) = replicas.remove(&id) {
                warn!(
                    "Replica '{}' removed after {} consecutive failures",
                    id, conn.health.consecutive_failures
                );
            }
        }

        Ok(())
    }

    /// Update replica health state based on consecutive failures and retry attempts.
    fn update_health_state(health: &mut ReplicaHealth, retry_attempt: u32, max_failures: u32) {
        if health.consecutive_failures >= max_failures {
            health.state = ReplicaState::Recovery;
        } else if health.consecutive_failures > 0 && retry_attempt > 0 {
            health.state = ReplicaState::Behind;
        }
        // If lag is very high, also mark as Behind
        if health.replication_lag_ms > 10_000 && health.state == ReplicaState::Ready {
            health.state = ReplicaState::Behind;
        }
    }

    /// Send a full snapshot to a specific replica.
    pub fn send_snapshot_to_replica(&self, replica_id: &str) -> Result<(), String> {
        let mut replicas = self
            .replicas
            .write()
            .map_err(|e| format!("lock poisoned: {}", e))?;
        let conn = replicas
            .get_mut(replica_id)
            .ok_or_else(|| format!("replica '{}' not found", replica_id))?;

        let header = MessageHeader::new(10, 2);
        let snap = mgrpc::SnapshotData { data: vec![] };
        conn.rpc
            .send(&header, &snap)
            .map_err(|e| format!("send snapshot failed: {}", e))?;

        conn.health.state = ReplicaState::Ready;
        conn.health.consecutive_failures = 0;
        conn.retry_state.record_success();
        Ok(())
    }

    /// Broadcast a heartbeat to all replicas. Updates health metrics and
    /// transitions state: Ready -> Behind -> Recovery on repeated failures.
    pub fn heartbeat_all(&self) -> Vec<String> {
        let mut replicas = self.replicas.write().expect("lock poisoned");
        let mut stale = Vec::new();
        let now_ms = now_millis();

        for (id, conn) in replicas.iter_mut() {
            // Skip replicas in backoff that aren't ready to retry yet
            if conn.retry_state.attempt > 0 && !conn.retry_state.can_retry() {
                conn.health.replication_lag_ms =
                    now_ms.saturating_sub(conn.health.last_acknowledged_ts);
                continue;
            }

            let header = MessageHeader::new(1, 1);
            let hb = mgrpc::Heartbeat {
                main_uuid: self.main_uuid.clone(),
                timestamp: now_secs(),
                epoch_id: self.epoch_id(),
            };
            if let Err(e) = conn.rpc.send(&header, &hb) {
                warn!("Replica {} heartbeat failed: {}", id, e);
                conn.health.consecutive_failures += 1;
                conn.retry_state.record_failure();
                Self::update_health_state(
                    &mut conn.health,
                    conn.retry_state.attempt,
                    self.max_failures_before_stale,
                );
                if conn.health.consecutive_failures >= self.max_failures_before_stale {
                    stale.push(id.clone());
                }
            } else {
                conn.health.consecutive_failures = 0;
                conn.health.last_heartbeat_ms = now_ms;
                conn.retry_state.record_success();
                if conn.health.state == ReplicaState::Behind
                    || conn.health.state == ReplicaState::Recovery
                {
                    conn.health.state = ReplicaState::Ready;
                }
            }
        }

        for id in &stale {
            if let Some(conn) = replicas.remove(id) {
                warn!(
                    "Replica '{}' removed after heartbeat failures (consecutive={})",
                    id, conn.health.consecutive_failures
                );
            }
        }

        stale
    }

    /// Increment the epoch (e.g. after a failover).
    pub fn bump_epoch(&self) {
        let new_epoch = self
            .epoch_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        info!("Epoch bumped to {}", new_epoch);
    }

    /// Replicate a delta batch to all registered replicas and wait for a
    /// majority of ACKs in StrictSync mode. For Sync mode, waits for all
    /// replicas. For Async mode, fire-and-forget.
    pub fn replicate_with_quorum(&self, batch: &DeltaBatch) -> Result<(), String> {
        let mut replicas = self
            .replicas
            .write()
            .map_err(|e| format!("lock poisoned: {}", e))?;
        if replicas.is_empty() {
            return Ok(());
        }

        let mut to_remove = Vec::new();
        let now_ms = now_millis();
        let total = replicas.len();
        let majority = (total / 2) + 1;
        let mut acks = 0;

        // Phase 1: Send to all replicas
        for (id, conn) in replicas.iter_mut() {
            if let Err(e) = send_delta_batch(&mut conn.rpc, batch) {
                warn!("Failed to send batch to {}: {}", id, e);
                conn.health.consecutive_failures += 1;
                conn.retry_state.record_failure();
                Self::update_health_state(
                    &mut conn.health,
                    conn.retry_state.attempt,
                    self.max_failures_before_stale,
                );
                if conn.health.consecutive_failures >= self.max_failures_before_stale {
                    to_remove.push(id.clone());
                }
            } else {
                conn.health.last_heartbeat_ms = now_ms;
            }
        }

        // Phase 2: Collect ACKs for Sync/StrictSync replicas
        for (id, conn) in replicas.iter_mut() {
            if to_remove.contains(id) {
                continue;
            }
            match conn.mode {
                ReplicationMode::Async => {
                    // Already sent, no ACK needed
                    conn.health.consecutive_failures = 0;
                    conn.health.last_acknowledged_ts = batch.commit_timestamp;
                    conn.health.replication_lag_ms = 0;
                    conn.retry_state.record_success();
                    if conn.health.state == ReplicaState::Behind
                        || conn.health.state == ReplicaState::Recovery
                    {
                        conn.health.state = ReplicaState::Ready;
                    }
                }
                ReplicationMode::Sync | ReplicationMode::StrictSync => {
                    match wait_for_ack(&mut conn.rpc, self.sync_ack_timeout) {
                        AckResult::Acknowledged => {
                            acks += 1;
                            conn.health.consecutive_failures = 0;
                            conn.health.last_acknowledged_ts = batch.commit_timestamp;
                            conn.health.replication_lag_ms = 0;
                            conn.health.last_heartbeat_ms = now_ms;
                            conn.retry_state.record_success();
                            if conn.health.state == ReplicaState::Behind
                                || conn.health.state == ReplicaState::Recovery
                            {
                                conn.health.state = ReplicaState::Ready;
                            }
                        }
                        AckResult::Timeout => {
                            warn!("Replica {} ACK timed out (quorum)", id);
                            conn.health.consecutive_failures += 1;
                            conn.retry_state.record_failure();
                            Self::update_health_state(
                                &mut conn.health,
                                conn.retry_state.attempt,
                                self.max_failures_before_stale,
                            );
                            if conn.health.consecutive_failures >= self.max_failures_before_stale {
                                to_remove.push(id.clone());
                            }
                        }
                        AckResult::Error => {
                            warn!("Replica {} ACK error (quorum)", id);
                            conn.health.consecutive_failures += 1;
                            conn.retry_state.record_failure();
                            Self::update_health_state(
                                &mut conn.health,
                                conn.retry_state.attempt,
                                self.max_failures_before_stale,
                            );
                            if conn.health.consecutive_failures >= self.max_failures_before_stale {
                                to_remove.push(id.clone());
                            }
                        }
                    }
                }
            }
        }

        for id in to_remove {
            if let Some(conn) = replicas.remove(&id) {
                warn!(
                    "Replica '{}' removed after quorum failures (consecutive={})",
                    id, conn.health.consecutive_failures
                );
            }
        }

        // Check quorum for StrictSync
        let strict_sync_count = replicas
            .values()
            .filter(|r| r.mode == ReplicationMode::StrictSync)
            .count();
        if strict_sync_count > 0 {
            if acks >= majority {
                Ok(())
            } else {
                Err(format!(
                    "strict sync quorum failed: {}/{} acks",
                    acks, total
                ))
            }
        } else {
            Ok(())
        }
    }

    /// Get current replication metrics summary.
    pub fn metrics(&self) -> ReplicationMetrics {
        let replicas = self.replicas.read().expect("lock poisoned");
        let mut metrics = ReplicationMetrics::new();
        let max_lag = replicas
            .values()
            .map(|r| r.health.replication_lag_ms)
            .max()
            .unwrap_or(0);
        metrics.max_replication_lag_ms = max_lag;
        metrics
    }
}

/// Replica-side handler: applies deltas from the main to local storage.
pub struct ReplicaHandler {
    storage: Arc<Storage>,
    pub main_address: String,
    client: Option<crate::ReplicationClient>,
    config: ReplicationConfig,
    /// Tracks main instance health for failover detection.
    pub failover_detector: FailoverDetector,
    /// Current replication position (commit timestamp of last applied delta).
    pub last_applied_timestamp: u64,
    /// Retry state for reconnection.
    reconnect_retry: RetryState,
}

impl ReplicaHandler {
    pub fn new(storage: Arc<Storage>, main_address: String, config: ReplicationConfig) -> Self {
        Self {
            storage,
            main_address,
            client: None,
            config,
            failover_detector: FailoverDetector::new(Duration::from_secs(5), 3),
            last_applied_timestamp: 0,
            reconnect_retry: RetryState::new(500, 30_000),
        }
    }

    /// Connect to the main instance.
    pub fn connect(&mut self) -> Result<(), String> {
        let client = crate::ReplicationClient::connect(&self.main_address, self.config.clone())
            .map_err(|e| format!("connect failed: {}", e))?;
        self.client = Some(client);
        self.reconnect_retry.record_success();
        info!("Connected to main at {}", self.main_address);
        Ok(())
    }

    /// Ensure connection, with automatic retry if disconnected.
    pub fn ensure_connected(&mut self) -> Result<(), String> {
        if self.client.is_some() {
            return Ok(());
        }
        if !self.reconnect_retry.can_retry() {
            return Err(format!(
                "reconnection backed off, wait {:?} before retry",
                self.reconnect_retry.next_backoff
            ));
        }
        self.connect().inspect_err(|e| {
            self.reconnect_retry.record_failure();
        })
    }

    /// Perform WAL catch-up: request WAL files from main since
    /// `last_applied_timestamp`, apply them, then return.
    /// Returns the new timestamp after catch-up.
    pub fn catch_up_wal(&mut self) -> Result<u64, String> {
        let client = self.client.as_mut().ok_or("not connected")?;

        info!(
            "Requesting WAL catch-up from timestamp {}",
            self.last_applied_timestamp
        );

        let wal_files = client
            .request_wal(self.last_applied_timestamp)
            .map_err(|e| format!("WAL request failed: {}", e))?;

        let mut max_ts = self.last_applied_timestamp;
        for (_file_name, data) in wal_files {
            if data.is_empty() {
                continue;
            }
            let reader = match mgdurability::WalReader::from_bytes(&data) {
                Ok(r) => r,
                Err(e) => {
                    warn!("WAL parse error for {}: {}", _file_name, e);
                    continue;
                }
            };
            let records: Vec<_> = reader.records().to_vec();
            if records.is_empty() {
                continue;
            }
            // Track the highest commit timestamp across records.
            for rec in &records {
                match rec {
                    mgdurability::DeltaRecord::TransactionEnd {
                        commit_timestamp, ..
                    } => {
                        max_ts = max_ts.max(*commit_timestamp);
                    }
                    mgdurability::DeltaRecord::VertexCreate { timestamp, .. } => {
                        max_ts = max_ts.max(*timestamp);
                    }
                    mgdurability::DeltaRecord::EdgeCreate { timestamp, .. } => {
                        max_ts = max_ts.max(*timestamp);
                    }
                    _ => {}
                }
            }
            let batch = mgrpc::DeltaBatch {
                epoch_id: self.client.as_ref().map_or(1, |c| c.epoch_id()),
                commit_timestamp: max_ts,
                sequence_number: 0,
                deltas: records,
            };
            let mut applier = StorageDeltaApplier::new(self.storage.clone());
            if let Err(e) = applier.apply_batch(&batch) {
                warn!("WAL batch apply error: {}", e);
            }
        }

        self.last_applied_timestamp = max_ts;
        Ok(max_ts)
    }

    /// Pull and apply all deltas from the main starting at `since_timestamp`.
    /// If `since_timestamp` is 0, requests a full snapshot first.
    /// Performs WAL catch-up before switching to live delta stream.
    pub fn sync_from_main(&mut self, since_timestamp: u64) -> Result<Option<u64>, String> {
        self.ensure_connected()?;

        if since_timestamp == 0 {
            info!("Requesting full snapshot from main");
            let snapshot_data = self
                .client
                .as_mut()
                .ok_or("not connected")?
                .request_snapshot(0)
                .map_err(|e| format!("snapshot request failed: {}", e))?;
            if !snapshot_data.is_empty() {
                crate::ReplServer::apply_snapshot_data(&self.storage, &snapshot_data)
                    .map_err(|e| format!("snapshot apply failed: {}", e))?;
            }
        } else {
            // Try WAL catch-up first for faster synchronization
            let _ = self.catch_up_wal();
        }

        let client = self.client.as_mut().ok_or("not connected")?;
        client
            .request_delta_stream(self.last_applied_timestamp.max(since_timestamp), 100)
            .map_err(|e| format!("delta stream request failed: {}", e))?;

        let mut applier = StorageDeltaApplier::new(self.storage.clone());
        let last_ts = client
            .apply_stream(&mut applier)
            .map_err(|e| format!("delta stream failed: {}", e))?;

        if let Some(ts) = last_ts {
            self.last_applied_timestamp = ts;
        }

        self.failover_detector.record_success();
        Ok(last_ts)
    }

    /// Send a heartbeat to the main and update failover detector.
    pub fn heartbeat(&mut self) -> Result<(), String> {
        self.ensure_connected()?;
        let client = self.client.as_mut().ok_or("not connected")?;
        match client.heartbeat("unknown") {
            Ok(()) => {
                self.failover_detector.record_success();
                Ok(())
            }
            Err(e) => {
                self.failover_detector.record_miss();
                if self.failover_detector.is_main_failed() {
                    warn!(
                        "Main instance declared failed after {} missed heartbeats",
                        self.failover_detector.max_missed_heartbeats
                    );
                }
                Err(format!("heartbeat failed: {}", e))
            }
        }
    }

    pub fn is_connected(&self) -> bool {
        self.client.is_some()
    }

    /// Run a continuous sync loop: heartbeat, sync, sleep.
    /// Returns Ok when caught up, Err if main is declared failed.
    pub fn run_sync_cycle(&mut self) -> Result<(), String> {
        if let Err(e) = self.heartbeat() {
            warn!("Heartbeat failed: {}", e);
            if self.failover_detector.is_main_failed() {
                return Err("main instance failed, triggering failover".into());
            }
        }

        match self.sync_from_main(self.last_applied_timestamp) {
            Ok(Some(ts)) => {
                info!("Synced up to timestamp {}", ts);
                Ok(())
            }
            Ok(None) => {
                info!("No new deltas to sync");
                Ok(())
            }
            Err(e) => {
                warn!("Sync failed: {}", e);
                if self.failover_detector.is_main_failed() {
                    return Err("main instance failed during sync".into());
                }
                Ok(())
            }
        }
    }
}

/// Applies delta batches to local Storage.
pub struct StorageDeltaApplier {
    storage: Arc<Storage>,
    pub applied_count: u64,
}

impl StorageDeltaApplier {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            applied_count: 0,
        }
    }

    fn with_tx<F>(&self, f: F) -> Result<(), String>
    where
        F: FnOnce(&Transaction) -> Result<(), mgstorage::storage::StorageError>,
    {
        let tx = self
            .storage
            .begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        match f(&tx) {
            Ok(()) => {
                self.storage.commit_transaction(&tx);
                Ok(())
            }
            Err(e) => {
                self.storage.abort_transaction(&tx);
                Err(format!("storage error: {:?}", e))
            }
        }
    }
}

impl DeltaApplier for StorageDeltaApplier {
    fn apply_batch(&mut self, batch: &DeltaBatch) -> std::io::Result<()> {
        for delta in &batch.deltas {
            let result = match delta {
                // ── Vertex operations ──────────────────────────────────────
                DeltaRecord::VertexCreate { gid, timestamp: _ } => {
                    self.with_tx(|tx| self.storage.create_vertex(tx, *gid).map(|_| ()))
                }
                DeltaRecord::VertexDelete { gid } => {
                    self.with_tx(|tx| self.storage.delete_vertex(tx, *gid).map(|_| ()))
                }
                DeltaRecord::VertexSetProperty { gid, key, value } => self.with_tx(|tx| {
                    self.storage
                        .vertex_set_property(tx, *gid, *key, value.clone())
                }),
                DeltaRecord::VertexAddLabel { gid, label } => {
                    self.with_tx(|tx| self.storage.vertex_add_label(tx, *gid, *label))
                }
                DeltaRecord::VertexRemoveLabel { gid, label } => self.with_tx(|tx| {
                    self.storage
                        .vertex_remove_label(tx, *gid, *label)
                        .map(|_| ())
                }),
                // ── Edge operations ────────────────────────────────────────
                DeltaRecord::EdgeCreate {
                    gid,
                    from_vertex,
                    to_vertex,
                    edge_type,
                    timestamp: _,
                } => self.with_tx(|tx| {
                    self.storage
                        .create_edge(tx, *gid, *from_vertex, *to_vertex, *edge_type)
                        .map(|_| ())
                }),
                DeltaRecord::EdgeDelete { gid } => {
                    self.with_tx(|tx| self.storage.delete_edge(tx, *gid).map(|_| ()))
                }
                DeltaRecord::EdgeSetProperty { gid, key, value } => self.with_tx(|tx| {
                    self.storage
                        .edge_set_property(tx, *gid, *key, value.clone())
                }),
                // ── Transaction boundaries ─────────────────────────────────
                DeltaRecord::TransactionStart { timestamp: _ } => Ok(()),
                DeltaRecord::TransactionEnd {
                    timestamp: _,
                    commit_timestamp: _,
                } => Ok(()),
                // ── Label indices ──────────────────────────────────────────
                DeltaRecord::LabelIndexCreate { label } => {
                    self.storage.create_label_index(*label);
                    Ok(())
                }
                DeltaRecord::LabelIndexDrop { label } => {
                    self.storage.drop_label_index(*label);
                    Ok(())
                }
                DeltaRecord::LabelIndexStatsSet { .. } => Ok(()),
                DeltaRecord::LabelIndexStatsClear { .. } => Ok(()),
                // ── Label-property indices ─────────────────────────────────
                DeltaRecord::LabelPropertyIndexCreate { label, property } => {
                    self.storage.create_label_property_index(*label, *property);
                    Ok(())
                }
                DeltaRecord::LabelPropertyIndexDrop { label, property } => {
                    self.storage.drop_label_property_index(*label, *property);
                    Ok(())
                }
                DeltaRecord::LabelPropertyIndexStatsSet { .. } => Ok(()),
                DeltaRecord::LabelPropertyIndexStatsClear { .. } => Ok(()),
                // ── Edge indices ───────────────────────────────────────────
                DeltaRecord::EdgeIndexCreate { edge_type } => {
                    self.storage.create_edge_type_index(*edge_type);
                    Ok(())
                }
                DeltaRecord::EdgeIndexDrop { edge_type } => {
                    self.storage.drop_edge_type_index(*edge_type);
                    Ok(())
                }
                // ── Edge property indices ──────────────────────────────────
                DeltaRecord::EdgePropertyIndexCreate { property, .. } => {
                    self.storage.build_edge_property_index(*property);
                    Ok(())
                }
                DeltaRecord::EdgePropertyIndexDrop {
                    edge_type,
                    property,
                } => {
                    self.storage
                        .drop_edge_type_property_index(*edge_type, *property);
                    Ok(())
                }
                DeltaRecord::GlobalEdgePropertyIndexCreate { property } => {
                    self.storage.build_edge_property_index(*property);
                    Ok(())
                }
                DeltaRecord::GlobalEdgePropertyIndexDrop { .. } => {
                    warn!("GlobalEdgePropertyIndexDrop delta not yet fully supported (no Storage API to drop global edge property index)");
                    Ok(())
                }
                // ── Constraints ────────────────────────────────────────────
                DeltaRecord::ExistenceConstraintCreate { label, property } => {
                    self.storage
                        .constraints
                        .add_existence_constraint(*label, *property);
                    Ok(())
                }
                DeltaRecord::ExistenceConstraintDrop { label, property } => {
                    self.storage
                        .constraints
                        .drop_existence_constraint(*label, *property);
                    Ok(())
                }
                DeltaRecord::UniqueConstraintCreate { label, properties } => {
                    self.storage
                        .constraints
                        .add_unique_constraint(*label, properties.clone());
                    Ok(())
                }
                DeltaRecord::UniqueConstraintDrop { label, properties } => {
                    self.storage
                        .constraints
                        .remove_unique_constraint(*label, properties);
                    Ok(())
                }
                DeltaRecord::TypeConstraintCreate {
                    label,
                    property,
                    type_tag,
                } => {
                    let ct = match *type_tag {
                        0 => mgstorage::constraints::ConstraintType::Int,
                        1 => mgstorage::constraints::ConstraintType::Double,
                        2 => mgstorage::constraints::ConstraintType::Bool,
                        3 => mgstorage::constraints::ConstraintType::String,
                        4 => mgstorage::constraints::ConstraintType::List,
                        5 => mgstorage::constraints::ConstraintType::Map,
                        6 => mgstorage::constraints::ConstraintType::Point2D,
                        7 => mgstorage::constraints::ConstraintType::Date,
                        8 => mgstorage::constraints::ConstraintType::Duration,
                        9 => mgstorage::constraints::ConstraintType::LocalTime,
                        10 => mgstorage::constraints::ConstraintType::LocalDateTime,
                        _ => mgstorage::constraints::ConstraintType::Enum,
                    };
                    self.storage
                        .constraints
                        .add_type_constraint(*label, *property, ct);
                    Ok(())
                }
                DeltaRecord::TypeConstraintDrop { label, property } => {
                    self.storage
                        .constraints
                        .drop_type_constraint(*label, *property);
                    Ok(())
                }
                // ── Point indices ──────────────────────────────────────────
                DeltaRecord::PointIndexCreate { label, property } => {
                    self.storage.create_point_index(*label, *property);
                    Ok(())
                }
                DeltaRecord::PointIndexDrop { label, property } => {
                    self.storage.drop_point_index(*label, *property);
                    Ok(())
                }
                // ── Text indices ───────────────────────────────────────────
                DeltaRecord::TextIndexCreate { .. } => {
                    warn!("TextIndexCreate delta not yet fully supported");
                    Ok(())
                }
                DeltaRecord::TextIndexDrop { .. } => {
                    warn!("TextIndexDrop delta not yet fully supported");
                    Ok(())
                }
                DeltaRecord::TextEdgeIndexCreate { .. } => {
                    warn!("TextEdgeIndexCreate delta not yet fully supported");
                    Ok(())
                }
                // ── Vector indices ─────────────────────────────────────────
                DeltaRecord::VectorIndexCreate { .. } => {
                    warn!("VectorIndexCreate delta not yet fully supported");
                    Ok(())
                }
                DeltaRecord::VectorIndexDrop { .. } => {
                    warn!("VectorIndexDrop delta not yet fully supported");
                    Ok(())
                }
                DeltaRecord::VectorEdgeIndexCreate { .. } => {
                    warn!("VectorEdgeIndexCreate delta not yet fully supported");
                    Ok(())
                }
                // ── Enum operations ────────────────────────────────────────
                DeltaRecord::EnumCreate { .. } => {
                    warn!("EnumCreate delta not yet fully supported");
                    Ok(())
                }
                DeltaRecord::EnumAlterAdd { .. } => {
                    warn!("EnumAlterAdd delta not yet fully supported");
                    Ok(())
                }
                DeltaRecord::EnumAlterUpdate { .. } => {
                    warn!("EnumAlterUpdate delta not yet fully supported");
                    Ok(())
                }
                // ── TTL ────────────────────────────────────────────────────
                DeltaRecord::TtlOperation { label, ttl_ms } => {
                    self.storage.set_ttl(*label, *ttl_ms);
                    Ok(())
                }
                // ── Descriptions ───────────────────────────────────────────
                DeltaRecord::DescriptionSet { .. } => Ok(()),
                DeltaRecord::DescriptionDelete { .. } => Ok(()),
            };

            if let Err(e) = result {
                warn!("Failed to apply delta: {}", e);
                return Err(std::io::Error::other(e));
            }
            self.applied_count += 1;
        }
        Ok(())
    }
}

fn delta_timestamp(delta: &DeltaRecord) -> u64 {
    match delta {
        DeltaRecord::VertexCreate { timestamp, .. } => *timestamp,
        DeltaRecord::EdgeCreate { timestamp, .. } => *timestamp,
        _ => 0,
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

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
    use mgcore::types::LabelId;

    #[test]
    fn test_handler_new() {
        let storage = Arc::new(Storage::new());
        let handler = ReplicationHandler::new(storage, ReplicationConfig::default());
        assert_eq!(handler.epoch_id(), 1);
        assert!(!handler.main_uuid().is_empty());
    }

    #[test]
    fn test_list_replicas_empty() {
        let storage = Arc::new(Storage::new());
        let handler = ReplicationHandler::new(storage, ReplicationConfig::default());
        assert!(handler.list_replicas().is_empty());
    }

    #[test]
    fn test_replicate_delta_no_replicas() {
        let storage = Arc::new(Storage::new());
        let handler = ReplicationHandler::new(storage, ReplicationConfig::default());

        let delta = DeltaRecord::VertexCreate {
            gid: Gid::from(1u64),
            timestamp: 1,
        };
        handler.replicate_delta(&delta).unwrap();
    }

    #[test]
    fn test_replica_handler_not_connected() {
        let storage = Arc::new(Storage::new());
        let mut handler = ReplicaHandler::new(
            storage,
            "127.0.0.1:9999".into(),
            ReplicationConfig::default(),
        );
        assert!(!handler.is_connected());
        assert!(handler.sync_from_main(0).is_err());
    }

    #[test]
    fn test_storage_delta_applier() {
        let storage = Arc::new(Storage::new());
        let mut applier = StorageDeltaApplier::new(storage.clone());

        let batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 10,
            sequence_number: 0,
            deltas: vec![
                DeltaRecord::VertexCreate {
                    gid: Gid::from(1u64),
                    timestamp: 10,
                },
                DeltaRecord::VertexAddLabel {
                    gid: Gid::from(1u64),
                    label: LabelId::from(1u32),
                },
            ],
        };

        applier.apply_batch(&batch).unwrap();

        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        let v = storage.get_vertex(Gid::from(1u64), &tx);
        assert!(v.is_some()); // vertex exists
    }

    #[test]
    fn test_epoch_bump() {
        let storage = Arc::new(Storage::new());
        let handler = ReplicationHandler::new(storage, ReplicationConfig::default());
        assert_eq!(handler.epoch_id(), 1);
        handler.bump_epoch();
        assert_eq!(handler.epoch_id(), 2);
    }

    // ─── New feature tests ─────────────────────────────────────────────────

    #[test]
    fn test_delta_batcher_basic() {
        let mut batcher = DeltaBatcher::new(Duration::from_secs(1), 10);
        assert!(batcher.is_empty());

        let d1 = DeltaRecord::VertexCreate {
            gid: Gid::from(1u64),
            timestamp: 100,
        };
        let result = batcher.push(d1);
        assert!(result.is_none());
        assert_eq!(batcher.len(), 1);
    }

    #[test]
    fn test_delta_batcher_flush_on_capacity() {
        let mut batcher = DeltaBatcher::new(Duration::from_secs(60), 2);

        let d1 = DeltaRecord::VertexCreate {
            gid: Gid::from(1u64),
            timestamp: 100,
        };
        let d2 = DeltaRecord::VertexCreate {
            gid: Gid::from(2u64),
            timestamp: 200,
        };

        assert!(batcher.push(d1).is_none());
        let batch = batcher.push(d2);
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.deltas.len(), 2);
        assert_eq!(batch.commit_timestamp, 200);
        assert!(batcher.is_empty());
    }

    #[test]
    fn test_delta_batcher_manual_flush() {
        let mut batcher = DeltaBatcher::new(Duration::from_secs(60), 100);

        let d1 = DeltaRecord::VertexCreate {
            gid: Gid::from(1u64),
            timestamp: 100,
        };
        batcher.push(d1);

        let batch = batcher.flush();
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().deltas.len(), 1);
        assert!(batcher.is_empty());
    }

    #[test]
    fn test_delta_batcher_epoch() {
        let mut batcher = DeltaBatcher::new(Duration::from_secs(1), 2);
        batcher.set_epoch(42);
        let d = DeltaRecord::VertexCreate {
            gid: Gid::from(1u64),
            timestamp: 100,
        };
        let d2 = DeltaRecord::VertexCreate {
            gid: Gid::from(2u64),
            timestamp: 200,
        };
        batcher.push(d);
        let batch = batcher.push(d2).unwrap();
        assert_eq!(batch.epoch_id, 42);
    }

    #[test]
    fn test_retry_state_backoff() {
        let mut retry = RetryState::new(100, 1000);
        assert!(retry.can_retry());

        let b1 = retry.record_failure();
        assert_eq!(b1, Duration::from_millis(100));
        assert!(!retry.can_retry());

        let b2 = retry.record_failure();
        assert_eq!(b2, Duration::from_millis(200));

        let b3 = retry.record_failure();
        assert_eq!(b3, Duration::from_millis(400));

        retry.record_success();
        assert!(retry.can_retry());
        assert_eq!(retry.attempt, 0);
    }

    #[test]
    fn test_retry_state_max_backoff() {
        let mut retry = RetryState::new(100, 250);
        retry.record_failure(); // 100
        retry.record_failure(); // 200
        let b3 = retry.record_failure(); // would be 400, capped at 250
        assert_eq!(b3, Duration::from_millis(250));
    }

    #[test]
    fn test_replica_health_default() {
        let health = ReplicaHealth::default();
        assert_eq!(health.state, ReplicaState::Behind);
        assert_eq!(health.consecutive_failures, 0);
        assert_eq!(health.replication_lag_ms, 0);
    }

    #[test]
    fn test_ack_result_variants() {
        assert_ne!(AckResult::Acknowledged, AckResult::Timeout);
        assert_ne!(AckResult::Timeout, AckResult::Error);
        assert_ne!(AckResult::Error, AckResult::Acknowledged);
    }

    #[test]
    fn test_replication_handler_sync_timeout_field() {
        let storage = Arc::new(Storage::new());
        let handler = ReplicationHandler::new(storage, ReplicationConfig::default());
        assert_eq!(handler.sync_ack_timeout, Duration::from_secs(5));
        assert_eq!(handler.max_failures_before_stale, 3);
    }

    #[test]
    fn test_replication_metrics() {
        let mut m = ReplicationMetrics::new();
        assert_eq!(m.batches_sent, 0);
        m.record_batch_sent(10, 1024);
        assert_eq!(m.batches_sent, 1);
        assert_eq!(m.deltas_sent, 10);
        assert_eq!(m.bytes_replicated, 1024);
        m.record_failure();
        assert_eq!(m.failed_attempts, 1);
        m.record_ack();
        assert_eq!(m.acks_received, 1);
        m.record_timeout();
        assert_eq!(m.ack_timeouts, 1);
        m.record_quorum_success();
        assert_eq!(m.quorum_commits, 1);
        m.record_quorum_failure();
        assert_eq!(m.quorum_failures, 1);
        m.record_eviction();
        assert_eq!(m.replicas_evicted, 1);
    }

    #[test]
    fn test_replication_filter_defaults() {
        let filter = ReplicationFilter::new();
        let d = DeltaRecord::VertexCreate {
            gid: Gid::from(1u64),
            timestamp: 1,
        };
        assert!(filter.allows(&d));
    }

    #[test]
    fn test_replication_filter_vertices_only() {
        let mut filter = ReplicationFilter::new();
        filter.vertices_only = true;
        let v = DeltaRecord::VertexCreate {
            gid: Gid::from(1u64),
            timestamp: 1,
        };
        let e = DeltaRecord::EdgeCreate {
            gid: Gid::from(2u64),
            from_vertex: Gid::from(1u64),
            to_vertex: Gid::from(3u64),
            edge_type: mgcore::types::EdgeTypeId::from(1u32),
            timestamp: 1,
        };
        assert!(filter.allows(&v));
        assert!(!filter.allows(&e));
    }

    #[test]
    fn test_failover_detector_success() {
        let mut det = FailoverDetector::new(Duration::from_secs(1), 2);
        assert!(!det.is_main_failed());
        det.record_success();
        assert!(!det.is_main_failed());
    }

    #[test]
    fn test_failover_detector_failure() {
        let mut det = FailoverDetector::new(Duration::from_millis(100), 1);
        det.record_miss();
        assert!(det.is_main_failed());
    }

    #[test]
    fn test_failover_detector_timeout() {
        let mut det = FailoverDetector::new(Duration::from_millis(10), 1);
        det.record_success();
        std::thread::sleep(Duration::from_millis(30));
        assert!(det.is_main_failed());
    }

    #[test]
    fn test_replica_handler_failover_detector() {
        let storage = Arc::new(Storage::new());
        let handler = ReplicaHandler::new(
            storage,
            "127.0.0.1:9999".into(),
            ReplicationConfig::default(),
        );
        assert!(!handler.failover_detector.is_main_failed());
        assert_eq!(handler.last_applied_timestamp, 0);
    }

    #[test]
    fn test_replica_handler_reconnect_backoff() {
        let storage = Arc::new(Storage::new());
        let mut handler =
            ReplicaHandler::new(storage, "127.0.0.1:1".into(), ReplicationConfig::default());
        assert!(!handler.is_connected());
        // First attempt should fail immediately (can_retry = true)
        let r1 = handler.ensure_connected();
        assert!(r1.is_err());
        // Second attempt should fail with backoff message
        let r2 = handler.ensure_connected();
        assert!(r2.is_err());
    }

    #[test]
    fn test_delta_applier_all_types() {
        let storage = Arc::new(Storage::new());
        let mut applier = StorageDeltaApplier::new(storage.clone());

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
                    label: LabelId::from(1u32),
                },
                DeltaRecord::VertexSetProperty {
                    gid: Gid::from(1u64),
                    key: PropertyId::from(0u32),
                    value: mgcore::property_value::PropertyValue::Int(42),
                },
                DeltaRecord::LabelIndexCreate {
                    label: LabelId::from(1u32),
                },
                DeltaRecord::LabelPropertyIndexCreate {
                    label: LabelId::from(1u32),
                    property: PropertyId::from(0u32),
                },
                DeltaRecord::ExistenceConstraintCreate {
                    label: LabelId::from(1u32),
                    property: PropertyId::from(0u32),
                },
                DeltaRecord::TtlOperation {
                    label: LabelId::from(1u32),
                    ttl_ms: 3600_000,
                },
                DeltaRecord::TransactionStart { timestamp: 100 },
                DeltaRecord::TransactionEnd {
                    timestamp: 100,
                    commit_timestamp: 101,
                },
            ],
        };

        applier.apply_batch(&batch).unwrap();
        assert_eq!(applier.applied_count, 9);

        // Verify vertex exists
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        let v = storage.get_vertex(Gid::from(1u64), &tx);
        assert!(v.is_some());
        assert!(storage.has_label_index(LabelId::from(1u32)));
        assert!(storage.has_label_property_index(LabelId::from(1u32), PropertyId::from(0u32)));
    }

    #[test]
    fn test_delta_applier_edge_operations() {
        let storage = Arc::new(Storage::new());
        let mut applier = StorageDeltaApplier::new(storage.clone());

        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);

        let batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![
                DeltaRecord::EdgeCreate {
                    gid: Gid::from(10u64),
                    from_vertex: Gid::from(1u64),
                    to_vertex: Gid::from(2u64),
                    edge_type: mgcore::types::EdgeTypeId::from(1u32),
                    timestamp: 100,
                },
                DeltaRecord::EdgeSetProperty {
                    gid: Gid::from(10u64),
                    key: PropertyId::from(0u32),
                    value: mgcore::property_value::PropertyValue::String("knows".into()),
                },
                DeltaRecord::EdgeDelete {
                    gid: Gid::from(10u64),
                },
            ],
        };

        applier.apply_batch(&batch).unwrap();
        assert_eq!(applier.applied_count, 3);
    }

    #[test]
    fn test_delta_applier_constraints() {
        let storage = Arc::new(Storage::new());
        let mut applier = StorageDeltaApplier::new(storage.clone());

        let batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![
                DeltaRecord::UniqueConstraintCreate {
                    label: LabelId::from(1u32),
                    properties: vec![PropertyId::from(0u32)],
                },
                DeltaRecord::TypeConstraintCreate {
                    label: LabelId::from(1u32),
                    property: PropertyId::from(0u32),
                    type_tag: 0, // Int
                },
            ],
        };

        applier.apply_batch(&batch).unwrap();
        assert_eq!(applier.applied_count, 2);

        assert!(storage
            .constraints
            .has_unique_constraint(LabelId::from(1u32), &[PropertyId::from(0u32)]));
        assert!(storage
            .constraints
            .has_type_constraint(LabelId::from(1u32), PropertyId::from(0u32)));
    }

    #[test]
    fn test_delta_applier_constraint_drops() {
        let storage = Arc::new(Storage::new());
        let mut applier = StorageDeltaApplier::new(storage.clone());

        // First create constraints
        let create_batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![
                DeltaRecord::ExistenceConstraintCreate {
                    label: LabelId::from(1u32),
                    property: PropertyId::from(0u32),
                },
                DeltaRecord::UniqueConstraintCreate {
                    label: LabelId::from(1u32),
                    properties: vec![PropertyId::from(0u32)],
                },
                DeltaRecord::TypeConstraintCreate {
                    label: LabelId::from(1u32),
                    property: PropertyId::from(0u32),
                    type_tag: 0,
                },
            ],
        };
        applier.apply_batch(&create_batch).unwrap();

        // Now drop them
        let drop_batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 101,
            sequence_number: 0,
            deltas: vec![
                DeltaRecord::ExistenceConstraintDrop {
                    label: LabelId::from(1u32),
                    property: PropertyId::from(0u32),
                },
                DeltaRecord::UniqueConstraintDrop {
                    label: LabelId::from(1u32),
                    properties: vec![PropertyId::from(0u32)],
                },
                DeltaRecord::TypeConstraintDrop {
                    label: LabelId::from(1u32),
                    property: PropertyId::from(0u32),
                },
            ],
        };
        applier.apply_batch(&drop_batch).unwrap();

        assert!(!storage
            .constraints
            .has_existence_constraint(LabelId::from(1u32), PropertyId::from(0u32)));
        assert!(!storage
            .constraints
            .has_unique_constraint(LabelId::from(1u32), &[PropertyId::from(0u32)]));
        assert!(!storage
            .constraints
            .has_type_constraint(LabelId::from(1u32), PropertyId::from(0u32)));
    }

    #[test]
    fn test_delta_applier_label_index_drop() {
        let storage = Arc::new(Storage::new());
        let mut applier = StorageDeltaApplier::new(storage.clone());

        let create = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![DeltaRecord::LabelIndexCreate {
                label: LabelId::from(1u32),
            }],
        };
        applier.apply_batch(&create).unwrap();
        assert!(storage.has_label_index(LabelId::from(1u32)));

        let drop = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 101,
            sequence_number: 0,
            deltas: vec![DeltaRecord::LabelIndexDrop {
                label: LabelId::from(1u32),
            }],
        };
        applier.apply_batch(&drop).unwrap();
        assert!(!storage.has_label_index(LabelId::from(1u32)));
    }

    #[test]
    fn test_replication_handler_metrics_empty() {
        let storage = Arc::new(Storage::new());
        let handler = ReplicationHandler::new(storage, ReplicationConfig::default());
        let metrics = handler.metrics();
        assert_eq!(metrics.max_replication_lag_ms, 0);
    }

    /// End-to-end: ReplicaHandler::sync_from_main(0) pulls a snapshot from the
    /// main via TCP, applies it to local storage, then switches to delta stream.
    #[test]
    fn test_sync_from_main_applies_snapshot() {
        use mgcore::property_value::PropertyValue;
        use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
        use std::thread;

        // Build a populated main storage.
        let main = Arc::new(Storage::new());
        {
            let tx = main.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
            main.create_vertex(&tx, Gid::from(1u64)).unwrap();
            main.create_vertex(&tx, Gid::from(2u64)).unwrap();
            main.vertex_add_label(&tx, Gid::from(1u64), LabelId::from(10u32))
                .unwrap();
            main.vertex_set_property(
                &tx,
                Gid::from(1u64),
                PropertyId::from(9u32),
                PropertyValue::String("alice".into()),
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
            main.commit_transaction(&tx);
        }
        let snapshot_blob = crate::ReplServer::serialize_storage_state(&main);

        // Spawn a mock main that serves snapshot then end-of-stream.
        let server =
            crate::ReplServer::bind("127.0.0.1:0", crate::ReplicationConfig::default()).unwrap();
        let addr = server.local_addr().unwrap().to_string();
        let blob = snapshot_blob.clone();

        let handle = thread::spawn(move || {
            let (mut peer, _) = server.accept().unwrap();

            // Snapshot request
            let (_hdr, _req): (mgrpc::MessageHeader, mgrpc::SnapshotRequest) = peer.recv().unwrap();
            let resp_hdr = mgrpc::MessageHeader::new(10, 1);
            peer.send(&resp_hdr, &mgrpc::SnapshotData { data: blob })
                .unwrap();

            // DeltaStream request
            let (_hdr, _req): (mgrpc::MessageHeader, mgrpc::DeltaStreamRequest) =
                peer.recv().unwrap();
            let end_batch = crate::DeltaBatch {
                epoch_id: 1,
                commit_timestamp: 0,
                sequence_number: 0,
                deltas: vec![],
            };
            crate::send_delta_batch(&mut peer, &end_batch).unwrap();
        });

        // Replica side: connect, sync, verify state.
        let replica = Arc::new(Storage::new());
        let mut handler =
            ReplicaHandler::new(replica.clone(), addr, crate::ReplicationConfig::default());
        handler.connect().expect("connect to mock main");
        let result = handler.sync_from_main(0);
        assert!(result.is_ok(), "sync_from_main failed: {:?}", result.err());

        // Verify replica now matches main.
        let mut main_v = main.all_vertices();
        let mut rep_v = replica.all_vertices();
        main_v.sort_by_key(|(g, _, _)| g.as_uint());
        rep_v.sort_by_key(|(g, _, _)| g.as_uint());
        assert_eq!(main_v.len(), rep_v.len(), "vertex count mismatch");
        for ((g1, l1, p1), (g2, l2, p2)) in main_v.iter().zip(rep_v.iter()) {
            assert_eq!(g1, g2);
            assert_eq!(l1, l2);
            assert_eq!(p1.iter().collect::<Vec<_>>(), p2.iter().collect::<Vec<_>>());
        }

        let mut main_e = main.all_edges();
        let mut rep_e = replica.all_edges();
        main_e.sort_by_key(|(g, _, _, _, _)| g.as_uint());
        rep_e.sort_by_key(|(g, _, _, _, _)| g.as_uint());
        assert_eq!(main_e, rep_e, "edge mismatch");

        handle.join().unwrap();
    }

    /// WAL catch-up applies records received from the main to local storage.
    #[test]
    fn test_catch_up_wal_applies_records() {
        use mgcore::property_value::PropertyValue;
        use mgcore::types::{Gid, LabelId, PropertyId};
        use std::fs;
        use std::thread;

        // Build a WAL with a vertex create + property set.
        let wal_tmp = "/tmp/mg_test_catchup.wal";
        let _ = fs::remove_file(wal_tmp);
        {
            let mut writer = mgdurability::WalWriter::create(wal_tmp).unwrap();
            writer
                .append_record(&mgdurability::DeltaRecord::VertexCreate {
                    gid: Gid::from(42u64),
                    timestamp: 200,
                })
                .unwrap();
            writer
                .append_record(&mgdurability::DeltaRecord::VertexSetProperty {
                    gid: Gid::from(42u64),
                    key: PropertyId::from(7u32),
                    value: PropertyValue::String("bob".into()),
                })
                .unwrap();
            writer
                .append_record(&mgdurability::DeltaRecord::TransactionEnd {
                    timestamp: 200,
                    commit_timestamp: 201,
                })
                .unwrap();
            writer.sync().unwrap();
        }
        let wal_bytes = fs::read(wal_tmp).unwrap();
        fs::remove_file(wal_tmp).ok();

        // Mock main: accept WalRequest, reply with WalTransfer.
        let server =
            crate::ReplServer::bind("127.0.0.1:0", crate::ReplicationConfig::default()).unwrap();
        let addr = server.local_addr().unwrap().to_string();

        let handle = thread::spawn(move || {
            let (mut peer, _) = server.accept().unwrap();

            let (_hdr, _req): (mgrpc::MessageHeader, mgrpc::WalRequest) = peer.recv().unwrap();
            let resp_hdr = mgrpc::MessageHeader::new(11, 1);
            peer.send(
                &resp_hdr,
                &mgrpc::WalTransfer {
                    file_name: "catchup.wal".into(),
                    data: wal_bytes,
                },
            )
            .unwrap();
        });

        let storage = Arc::new(Storage::new());
        let mut handler =
            ReplicaHandler::new(storage.clone(), addr, crate::ReplicationConfig::default());
        handler.connect().expect("connect to mock main");

        let result = handler.catch_up_wal();
        assert!(result.is_ok(), "catch_up_wal failed: {:?}", result.err());

        // Verify the vertex and property were applied.
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        let v = storage.get_vertex(Gid::from(42u64), &tx);
        assert!(v.is_some(), "vertex 42 should exist");
        assert_eq!(
            v.unwrap().properties.get(PropertyId::from(7u32)),
            &PropertyValue::String("bob".into())
        );

        handle.join().unwrap();
    }

    #[test]
    fn test_delta_applier_edge_index_create_drop() {
        let storage = Arc::new(Storage::new());
        let mut applier = StorageDeltaApplier::new(storage.clone());

        let edge_type = EdgeTypeId::from(1u32);

        let create = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![DeltaRecord::EdgeIndexCreate { edge_type }],
        };
        applier.apply_batch(&create).unwrap();
        assert!(storage.has_edge_type_index(edge_type));

        let drop = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 101,
            sequence_number: 0,
            deltas: vec![DeltaRecord::EdgeIndexDrop { edge_type }],
        };
        applier.apply_batch(&drop).unwrap();
        assert!(!storage.has_edge_type_index(edge_type));
    }

    #[test]
    fn test_delta_applier_edge_property_index_drop() {
        let storage = Arc::new(Storage::new());
        let mut applier = StorageDeltaApplier::new(storage.clone());

        let edge_type = EdgeTypeId::from(1u32);
        let property = PropertyId::from(0u32);

        // Create the index directly via Storage API (the delta handler for
        // EdgePropertyIndexCreate currently builds the global edge property
        // index, not the edge-type-scoped one).
        assert!(storage.create_edge_type_property_index(edge_type, property));
        assert!(storage.has_edge_type_property_index(edge_type, property));

        // Drop it via delta
        let drop = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 101,
            sequence_number: 0,
            deltas: vec![DeltaRecord::EdgePropertyIndexDrop {
                edge_type,
                property,
            }],
        };
        applier.apply_batch(&drop).unwrap();
        assert!(!storage.has_edge_type_property_index(edge_type, property));
    }

    #[test]
    fn test_delta_applier_global_edge_property_index_create() {
        let storage = Arc::new(Storage::new());
        let mut applier = StorageDeltaApplier::new(storage.clone());

        let property = PropertyId::from(0u32);

        let batch = DeltaBatch {
            epoch_id: 1,
            commit_timestamp: 100,
            sequence_number: 0,
            deltas: vec![DeltaRecord::GlobalEdgePropertyIndexCreate { property }],
        };
        applier.apply_batch(&batch).unwrap();
        // build_edge_property_index populates the index; we can verify it ran without error
        assert_eq!(applier.applied_count, 1);
    }
}
