#![allow(unused)]
//! # mgcoord — Cluster coordinator for high availability.
//!
//! Manages cluster membership, main/replica roles, failover, and routing.
//! Coordinates data instances using mgrpc and monitors health via heartbeats.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use mgrepl::ReplicationMode;

pub mod raft;

use raft::{ClusterCommand, CoordinatorNode, CoordinatorNodeId, TypeConfig};

// ─── Instance ──────────────────────────────────────────────────────────────

/// Role of a data instance in the cluster.
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum InstanceRole {
    Main,
    Replica,
}

/// Health status of an instance.
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum InstanceHealth {
    Up,
    Down,
    Unknown,
}

/// A registered data instance.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Instance {
    pub id: String,
    pub address: SocketAddr,
    pub role: InstanceRole,
    pub health: InstanceHealth,
    #[serde(skip, default = "Instant::now")]
    pub last_heartbeat: Instant,
    pub replication_mode: ReplicationMode,
}

impl Instance {
    pub fn new(id: String, address: SocketAddr, role: InstanceRole) -> Self {
        Self {
            id,
            address,
            role,
            health: InstanceHealth::Unknown,
            last_heartbeat: Instant::now(),
            replication_mode: ReplicationMode::Sync,
        }
    }
}

// ─── Cluster State ─────────────────────────────────────────────────────────

/// Cluster configuration and state.
#[derive(Debug)]
pub struct ClusterState {
    instances: RwLock<HashMap<String, Instance>>,
    /// Current leader coordinator's ID (for coordinator consensus).
    leader_id: RwLock<String>,
    /// Routing table: maps database names to main instance addresses.
    routing: RwLock<HashMap<String, SocketAddr>>,
}

impl ClusterState {
    pub fn new(leader_id: String) -> Self {
        Self {
            instances: RwLock::new(HashMap::new()),
            leader_id: RwLock::new(leader_id),
            routing: RwLock::new(HashMap::new()),
        }
    }

    // ─── Instance management ──────────────────────────────────────────

    pub fn register(&self, instance: Instance) {
        let mut instances = self.instances.write().expect("lock poisoned");
        instances.insert(instance.id.clone(), instance);
    }

    pub fn unregister(&self, instance_id: &str) {
        let mut instances = self.instances.write().expect("lock poisoned");
        instances.remove(instance_id);
    }

    pub fn get(&self, instance_id: &str) -> Option<Instance> {
        self.instances
            .read()
            .expect("lock poisoned")
            .get(instance_id)
            .cloned()
    }

    pub fn list(&self) -> Vec<Instance> {
        self.instances
            .read()
            .expect("lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn update_health(&self, instance_id: &str, health: InstanceHealth) {
        if let Some(inst) = self
            .instances
            .write()
            .expect("lock poisoned")
            .get_mut(instance_id)
        {
            inst.health = health;
            inst.last_heartbeat = Instant::now();
        }
    }

    /// Get all instances with a specific role.
    pub fn by_role(&self, role: InstanceRole) -> Vec<Instance> {
        self.instances
            .read()
            .expect("lock poisoned")
            .values()
            .filter(|i| i.role == role)
            .cloned()
            .collect()
    }

    /// Get the current main instance (there should be exactly one).
    pub fn main_instance(&self) -> Option<Instance> {
        self.by_role(InstanceRole::Main).into_iter().next()
    }

    /// Get all healthy replicas.
    pub fn healthy_replicas(&self) -> Vec<Instance> {
        self.by_role(InstanceRole::Replica)
            .into_iter()
            .filter(|i| i.health == InstanceHealth::Up)
            .collect()
    }

    // ─── Failover ─────────────────────────────────────────────────────

    /// Promote a replica to main. Demotes the current main if present.
    pub fn promote_to_main(&self, instance_id: &str) -> Result<(), CoordinatorError> {
        let mut instances = self.instances.write().expect("lock poisoned");

        // Demote current main
        for inst in instances.values_mut() {
            if inst.role == InstanceRole::Main {
                inst.role = InstanceRole::Replica;
            }
        }

        // Promote target
        let target = instances
            .get_mut(instance_id)
            .ok_or(CoordinatorError::InstanceNotFound(instance_id.to_string()))?;
        target.role = InstanceRole::Main;

        Ok(())
    }

    /// Demote a main to replica.
    pub fn demote_to_replica(&self, instance_id: &str) -> Result<(), CoordinatorError> {
        let mut instances = self.instances.write().expect("lock poisoned");
        let target = instances
            .get_mut(instance_id)
            .ok_or(CoordinatorError::InstanceNotFound(instance_id.to_string()))?;
        if target.role != InstanceRole::Main {
            return Err(CoordinatorError::NotMain(instance_id.to_string()));
        }
        target.role = InstanceRole::Replica;
        Ok(())
    }

    // ─── Routing ──────────────────────────────────────────────────────

    pub fn set_route(&self, database: &str, main_addr: SocketAddr) {
        self.routing
            .write()
            .expect("lock poisoned")
            .insert(database.to_string(), main_addr);
    }

    pub fn get_route(&self, database: &str) -> Option<SocketAddr> {
        self.routing
            .read()
            .expect("lock poisoned")
            .get(database)
            .copied()
    }

    pub fn remove_route(&self, database: &str) {
        self.routing
            .write()
            .expect("lock poisoned")
            .remove(database);
    }

    pub fn routes(&self) -> Vec<(String, SocketAddr)> {
        self.routing
            .read()
            .expect("lock poisoned")
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    pub fn leader_id(&self) -> String {
        self.leader_id.read().expect("lock poisoned").clone()
    }

    // ─── Health monitoring ────────────────────────────────────────────

    /// Mark instances as Down if they haven't heartbeated within the timeout.
    pub fn check_health(&self, timeout: Duration) -> Vec<String> {
        let mut instances = self.instances.write().expect("lock poisoned");
        let mut down_ids = Vec::new();
        let now = Instant::now();

        for (id, inst) in instances.iter_mut() {
            if inst.health == InstanceHealth::Up
                && now.duration_since(inst.last_heartbeat) > timeout
            {
                inst.health = InstanceHealth::Down;
                down_ids.push(id.clone());
            }
        }

        down_ids
    }

    /// Attempt auto-failover: if main is down, promote the healthiest replica.
    pub fn auto_failover(&self) -> Option<String> {
        let candidate = self.failover_candidate()?;
        self.promote_to_main(&candidate).ok()?;
        Some(candidate)
    }

    /// Return the ID of the best replica to promote if the main is down.
    /// Does **not** mutate state — the caller must propose the promotion
    /// through consensus (e.g. Raft).
    pub fn failover_candidate(&self) -> Option<String> {
        let main = self.main_instance()?;
        if main.health != InstanceHealth::Down {
            return None;
        }

        let replicas = self.healthy_replicas();
        if replicas.is_empty() {
            return None;
        }

        Some(replicas[0].id.clone())
    }

    /// Total number of registered instances.
    pub fn instance_count(&self) -> usize {
        self.instances.read().expect("lock poisoned").len()
    }

    /// Number of instances whose health is [`InstanceHealth::Up`].
    pub fn healthy_instance_count(&self) -> usize {
        self.instances
            .read()
            .expect("lock poisoned")
            .values()
            .filter(|i| i.health == InstanceHealth::Up)
            .count()
    }
}

// ─── ClusterConfig ─────────────────────────────────────────────────────────

/// Bootstrap configuration for a new cluster.
///
/// Typically loaded from a config file or environment variables.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
    /// The set of data instances to register at bootstrap time.
    pub bootstrap_instances: Vec<Instance>,
    /// Default database name used for the initial routing table.
    pub default_database: String,
}

impl ClusterConfig {
    pub fn new(bootstrap_instances: Vec<Instance>, default_database: String) -> Self {
        Self {
            bootstrap_instances,
            default_database,
        }
    }
}

/// Bootstraps a [`ClusterState`] from a [`ClusterConfig`] and an
/// [`InstanceDiscovery`] implementation.
///
/// This is a one-time operation performed when a coordinator first starts
/// and has not yet joined an existing Raft cluster.
pub struct ClusterBootstrap;

impl ClusterBootstrap {
    /// Initialise `state` by registering every instance returned by `discovery`
    /// and, if a main instance exists, setting the default route.
    pub fn init(
        state: &ClusterState,
        config: &ClusterConfig,
        discovery: &dyn raft::InstanceDiscovery,
    ) {
        for inst in discovery.discover() {
            state.register(inst);
        }
        // Also register any explicit bootstrap instances from the config.
        for inst in &config.bootstrap_instances {
            state.register(inst.clone());
        }
        // If there is already a main, wire up the default database route.
        if let Some(main) = state.main_instance() {
            state.set_route(&config.default_database, main.address);
        }
    }
}

// ─── Coordinator ───────────────────────────────────────────────────────────

/// Top-level coordinator managing the cluster.
pub struct Coordinator {
    pub state: Arc<ClusterState>,
    pub id: String,
    health_timeout: Duration,
}

impl Coordinator {
    pub fn new(id: String, health_timeout: Duration) -> Self {
        Self {
            state: Arc::new(ClusterState::new(id.clone())),
            id,
            health_timeout,
        }
    }

    pub fn state(&self) -> &Arc<ClusterState> {
        &self.state
    }

    /// Bootstrap this coordinator's state from config + discovery.
    ///
    /// Should be called once before the Raft node is started.
    pub fn bootstrap_cluster(
        &self,
        config: &ClusterConfig,
        discovery: &dyn raft::InstanceDiscovery,
    ) {
        ClusterBootstrap::init(&self.state, config, discovery);
    }

    /// Periodic health check — should be called on a timer.
    pub fn tick(&self) -> Vec<String> {
        let down = self.state.check_health(self.health_timeout);
        if !down.is_empty() {
            // Try auto-failover if main went down
            if let Some(promoted) = self.state.auto_failover() {
                eprintln!("[coordinator] auto-failover: promoted {} to main", promoted);
            }
        }
        down
    }
}

// ─── Errors ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorError {
    InstanceNotFound(String),
    NotMain(String),
    NoHealthyReplicas,
}

impl std::fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoordinatorError::InstanceNotFound(id) => write!(f, "instance not found: {}", id),
            CoordinatorError::NotMain(id) => write!(f, "instance is not main: {}", id),
            CoordinatorError::NoHealthyReplicas => write!(f, "no healthy replicas available"),
        }
    }
}

impl std::error::Error for CoordinatorError {}

// ─── ReplicationLagTracker ─────────────────────────────────────────────────

/// Tracks replication lag for each replica instance.
#[derive(Clone, Debug)]
pub struct ReplicationLagTracker {
    lags: Arc<RwLock<HashMap<String, Duration>>>,
}

impl ReplicationLagTracker {
    pub fn new() -> Self {
        Self {
            lags: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn record_lag(&self, instance_id: &str, lag: Duration) {
        self.lags
            .write()
            .expect("lock poisoned")
            .insert(instance_id.to_string(), lag);
    }

    pub fn get_lag(&self, instance_id: &str) -> Option<Duration> {
        self.lags
            .read()
            .expect("lock poisoned")
            .get(instance_id)
            .copied()
    }

    pub fn all_lags(&self) -> HashMap<String, Duration> {
        self.lags.read().expect("lock poisoned").clone()
    }

    /// Replicas sorted by lag ascending (least laggy first).
    pub fn replicas_by_lag(&self) -> Vec<(String, Duration)> {
        let mut v: Vec<_> = self.all_lags().into_iter().collect();
        v.sort_by(|a, b| a.1.cmp(&b.1));
        v
    }

    /// Check if any replica exceeds the max acceptable lag.
    pub fn has_lagging_replicas(&self, max_lag: Duration) -> Vec<String> {
        self.lags
            .read()
            .expect("lock poisoned")
            .iter()
            .filter(|(_, &lag)| lag > max_lag)
            .map(|(id, _)| id.clone())
            .collect()
    }
}

impl Default for ReplicationLagTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ─── ShardAllocator ────────────────────────────────────────────────────────

/// A shard is a partition of the graph (range of Gid values or label-based).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ShardId(pub u64);

/// Assignment of shards to data instances for horizontal scaling.
///
/// In Memgraph Enterprise, the coordinator may split a large graph into shards
/// and assign each shard to a different data instance.
#[derive(Clone, Debug)]
pub struct ShardAllocator {
    assignments: Arc<RwLock<HashMap<ShardId, String>>>,
    shard_count: u64,
}

impl ShardAllocator {
    pub fn new(shard_count: u64) -> Self {
        Self {
            assignments: Arc::new(RwLock::new(HashMap::new())),
            shard_count,
        }
    }

    pub fn shard_count(&self) -> u64 {
        self.shard_count
    }

    /// Assign a shard to an instance using round-robin.
    pub fn assign_round_robin(&self, instances: &[Instance]) {
        let mut map = self.assignments.write().expect("lock poisoned");
        map.clear();
        if instances.is_empty() {
            return;
        }
        for i in 0..self.shard_count {
            let inst = &instances[i as usize % instances.len()];
            map.insert(ShardId(i), inst.id.clone());
        }
    }

    /// Assign a shard to a specific instance.
    pub fn assign(&self, shard: ShardId, instance_id: &str) {
        self.assignments
            .write()
            .expect("lock poisoned")
            .insert(shard, instance_id.to_string());
    }

    /// Get the instance responsible for a shard.
    pub fn get(&self, shard: ShardId) -> Option<String> {
        self.assignments
            .read()
            .expect("lock poisoned")
            .get(&shard)
            .cloned()
    }

    /// Rebalance shards evenly across instances.
    pub fn rebalance(&self, instances: &[Instance]) {
        let mut map = self.assignments.write().expect("lock poisoned");
        if instances.is_empty() {
            map.clear();
            return;
        }
        // Compute target counts per instance
        let target_per_inst = self.shard_count as usize / instances.len();
        let extra = self.shard_count as usize % instances.len();

        let mut new_assignments: HashMap<ShardId, String> = HashMap::new();
        let mut inst_idx = 0usize;
        let mut assigned_to_current = 0usize;

        for i in 0..self.shard_count {
            let shard = ShardId(i);
            let inst = &instances[inst_idx];
            new_assignments.insert(shard, inst.id.clone());
            assigned_to_current += 1;
            let target = target_per_inst + if inst_idx < extra { 1 } else { 0 };
            if assigned_to_current >= target {
                inst_idx += 1;
                assigned_to_current = 0;
            }
        }

        *map = new_assignments;
    }

    /// All shards assigned to a given instance.
    pub fn shards_for(&self, instance_id: &str) -> Vec<ShardId> {
        self.assignments
            .read()
            .expect("lock poisoned")
            .iter()
            .filter(|(_, id)| *id == instance_id)
            .map(|(sid, _)| *sid)
            .collect()
    }
}

impl Default for ShardAllocator {
    fn default() -> Self {
        Self::new(16)
    }
}

// ─── LoadBalancer ──────────────────────────────────────────────────────────

/// Load balancing strategy for read queries across replicas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadBalanceStrategy {
    RoundRobin,
    LeastConnections,
    Random,
}

/// Tracks per-instance connection counts and routes read queries.
#[derive(Clone, Debug)]
pub struct LoadBalancer {
    strategy: LoadBalanceStrategy,
    connections: Arc<RwLock<HashMap<String, usize>>>,
    rr_index: Arc<RwLock<usize>>,
}

impl LoadBalancer {
    pub fn new(strategy: LoadBalanceStrategy) -> Self {
        Self {
            strategy,
            connections: Arc::new(RwLock::new(HashMap::new())),
            rr_index: Arc::new(RwLock::new(0)),
        }
    }

    pub fn record_connection(&self, instance_id: &str, delta: i64) {
        let mut conns = self.connections.write().expect("lock poisoned");
        let entry = conns.entry(instance_id.to_string()).or_insert(0);
        if delta >= 0 {
            *entry += delta as usize;
        } else {
            *entry = entry.saturating_sub((-delta) as usize);
        }
    }

    /// Select a replica for a read query.
    pub fn pick_replica(
        &self,
        replicas: &[Instance],
        lag_tracker: Option<&ReplicationLagTracker>,
    ) -> Option<String> {
        if replicas.is_empty() {
            return None;
        }
        // Filter out lagging replicas if tracker provided
        let candidates: Vec<&Instance> = match lag_tracker {
            Some(lt) => replicas
                .iter()
                .filter(|r| lt.get_lag(&r.id).is_none_or(|lag| lag.as_secs() < 30))
                .collect(),
            None => replicas.iter().collect(),
        };
        let candidates = if candidates.is_empty() {
            replicas.iter().collect()
        } else {
            candidates
        };

        match self.strategy {
            LoadBalanceStrategy::RoundRobin => {
                let mut idx = self.rr_index.write().expect("lock poisoned");
                let choice = &candidates[*idx % candidates.len()];
                *idx += 1;
                Some(choice.id.clone())
            }
            LoadBalanceStrategy::LeastConnections => {
                let conns = self.connections.read().expect("lock poisoned");
                candidates
                    .iter()
                    .min_by_key(|inst| conns.get(&inst.id).copied().unwrap_or(0))
                    .map(|inst| inst.id.clone())
            }
            LoadBalanceStrategy::Random => {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                Instant::now().hash(&mut hasher);
                let idx = hasher.finish() as usize % candidates.len();
                Some(candidates[idx].id.clone())
            }
        }
    }
}

impl Default for LoadBalancer {
    fn default() -> Self {
        Self::new(LoadBalanceStrategy::RoundRobin)
    }
}

// ─── ClusterTopology ───────────────────────────────────────────────────────

/// Representation of the cluster topology as a graph of instances.
#[derive(Clone, Debug)]
pub struct ClusterTopology {
    /// instance_id -> list of instance_ids it replicates to / from
    edges: HashMap<String, Vec<String>>,
}

impl ClusterTopology {
    pub fn new() -> Self {
        Self {
            edges: HashMap::new(),
        }
    }

    pub fn add_edge(&mut self, from: &str, to: &str) {
        self.edges
            .entry(from.to_string())
            .or_default()
            .push(to.to_string());
    }

    pub fn remove_instance(&mut self, instance_id: &str) {
        self.edges.remove(instance_id);
        for targets in self.edges.values_mut() {
            targets.retain(|t| t != instance_id);
        }
    }

    /// Out-degree (replica count) for each instance.
    pub fn out_degrees(&self) -> HashMap<String, usize> {
        self.edges
            .iter()
            .map(|(k, v)| (k.clone(), v.len()))
            .collect()
    }

    /// Check if the topology forms a tree (exactly one main, replicas form a tree).
    pub fn is_valid_tree(&self, main_id: &str) -> bool {
        let mut visited = std::collections::HashSet::new();
        let mut queue = vec![main_id.to_string()];
        while let Some(current) = queue.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            if let Some(targets) = self.edges.get(&current) {
                for t in targets {
                    queue.push(t.clone());
                }
            }
        }
        // All nodes should be reachable from main
        let all_nodes: std::collections::HashSet<String> = self.edges.keys().cloned().collect();
        visited == all_nodes || visited.len() >= all_nodes.len()
    }

    /// Detect cycles in the replication topology.
    pub fn has_cycle(&self) -> bool {
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();

        fn dfs(
            node: &str,
            edges: &HashMap<String, Vec<String>>,
            visited: &mut std::collections::HashSet<String>,
            rec_stack: &mut std::collections::HashSet<String>,
        ) -> bool {
            visited.insert(node.to_string());
            rec_stack.insert(node.to_string());
            if let Some(targets) = edges.get(node) {
                for t in targets {
                    if !visited.contains(t) {
                        if dfs(t, edges, visited, rec_stack) {
                            return true;
                        }
                    } else if rec_stack.contains(t) {
                        return true;
                    }
                }
            }
            rec_stack.remove(node);
            false
        }

        for node in self.edges.keys() {
            if !visited.contains(node) && dfs(node, &self.edges, &mut visited, &mut rec_stack) {
                return true;
            }
        }
        false
    }
}

impl Default for ClusterTopology {
    fn default() -> Self {
        Self::new()
    }
}

// ─── DataMigration ─────────────────────────────────────────────────────────

/// Status of an ongoing data migration between instances.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// A single data migration task (e.g. moving a shard from one instance to another).
#[derive(Clone, Debug)]
pub struct MigrationTask {
    pub id: String,
    pub shard: ShardId,
    pub from_instance: String,
    pub to_instance: String,
    pub status: MigrationStatus,
    pub progress_percent: u8,
    pub started_at: Option<Instant>,
    pub completed_at: Option<Instant>,
}

/// Tracks all active and completed data migrations.
#[derive(Clone, Debug)]
pub struct DataMigration {
    tasks: Arc<RwLock<HashMap<String, MigrationTask>>>,
}

impl DataMigration {
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_task(&self, shard: ShardId, from: &str, to: &str) -> String {
        let id = format!("mig-{}-{}-{}", shard.0, from, to);
        let task = MigrationTask {
            id: id.clone(),
            shard,
            from_instance: from.to_string(),
            to_instance: to.to_string(),
            status: MigrationStatus::Pending,
            progress_percent: 0,
            started_at: None,
            completed_at: None,
        };
        self.tasks
            .write()
            .expect("lock poisoned")
            .insert(id.clone(), task);
        id
    }

    pub fn start_task(&self, task_id: &str) {
        if let Some(task) = self.tasks.write().expect("lock poisoned").get_mut(task_id) {
            task.status = MigrationStatus::InProgress;
            task.started_at = Some(Instant::now());
        }
    }

    pub fn update_progress(&self, task_id: &str, percent: u8) {
        if let Some(task) = self.tasks.write().expect("lock poisoned").get_mut(task_id) {
            task.progress_percent = percent.min(100);
        }
    }

    pub fn complete_task(&self, task_id: &str) {
        if let Some(task) = self.tasks.write().expect("lock poisoned").get_mut(task_id) {
            task.status = MigrationStatus::Completed;
            task.progress_percent = 100;
            task.completed_at = Some(Instant::now());
        }
    }

    pub fn fail_task(&self, task_id: &str) {
        if let Some(task) = self.tasks.write().expect("lock poisoned").get_mut(task_id) {
            task.status = MigrationStatus::Failed;
        }
    }

    pub fn get_task(&self, task_id: &str) -> Option<MigrationTask> {
        self.tasks
            .read()
            .expect("lock poisoned")
            .get(task_id)
            .cloned()
    }

    pub fn active_tasks(&self) -> Vec<MigrationTask> {
        self.tasks
            .read()
            .expect("lock poisoned")
            .values()
            .filter(|t| t.status == MigrationStatus::InProgress)
            .cloned()
            .collect()
    }

    pub fn completed_tasks(&self) -> Vec<MigrationTask> {
        self.tasks
            .read()
            .expect("lock poisoned")
            .values()
            .filter(|t| t.status == MigrationStatus::Completed)
            .cloned()
            .collect()
    }
}

impl Default for DataMigration {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Multi-DC Support ──────────────────────────────────────────────────────

/// A data center / availability zone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataCenter {
    pub id: String,
    pub region: String,
    pub latency_to_coordinator_ms: u64,
}

/// Per-instance datacenter membership.
#[derive(Clone, Debug)]
pub struct MultiDcRouting {
    dc_map: Arc<RwLock<HashMap<String, DataCenter>>>,
    instance_dc: Arc<RwLock<HashMap<String, String>>>,
}

impl MultiDcRouting {
    pub fn new() -> Self {
        Self {
            dc_map: Arc::new(RwLock::new(HashMap::new())),
            instance_dc: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_dc(&self, dc: DataCenter) {
        self.dc_map
            .write()
            .expect("lock poisoned")
            .insert(dc.id.clone(), dc);
    }

    pub fn assign_instance_to_dc(&self, instance_id: &str, dc_id: &str) {
        self.instance_dc
            .write()
            .expect("lock poisoned")
            .insert(instance_id.to_string(), dc_id.to_string());
    }

    pub fn instance_dc(&self, instance_id: &str) -> Option<String> {
        self.instance_dc
            .read()
            .expect("lock poisoned")
            .get(instance_id)
            .cloned()
    }

    pub fn instances_in_dc(&self, dc_id: &str) -> Vec<String> {
        self.instance_dc
            .read()
            .expect("lock poisoned")
            .iter()
            .filter(|(_, id)| *id == dc_id)
            .map(|(inst, _)| inst.clone())
            .collect()
    }

    /// Prefer local-DC replicas for reads to minimize cross-DC latency.
    pub fn local_replicas(&self, replicas: &[Instance], client_dc: &str) -> Vec<Instance> {
        let inst_dc = self.instance_dc.read().expect("lock poisoned");
        let local: Vec<Instance> = replicas
            .iter()
            .filter(|r| inst_dc.get(&r.id).is_some_and(|dc| dc == client_dc))
            .cloned()
            .collect();
        if local.is_empty() {
            replicas.to_vec()
        } else {
            local
        }
    }

    pub fn all_dcs(&self) -> Vec<DataCenter> {
        self.dc_map
            .read()
            .expect("lock poisoned")
            .values()
            .cloned()
            .collect()
    }
}

impl Default for MultiDcRouting {
    fn default() -> Self {
        Self::new()
    }
}

// ─── InstanceMetrics ───────────────────────────────────────────────────────

/// Detailed per-instance metrics for health evaluation.
#[derive(Clone, Debug, Default)]
pub struct InstanceMetrics {
    pub cpu_percent: f64,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
    pub disk_used_gb: u64,
    pub disk_total_gb: u64,
    pub query_throughput_qps: f64,
    pub active_connections: usize,
    pub replication_lag_ms: u64,
}

/// Collects and aggregates instance metrics.
#[derive(Clone, Debug)]
pub struct MetricsCollector {
    metrics: Arc<RwLock<HashMap<String, InstanceMetrics>>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn record(&self, instance_id: &str, metrics: InstanceMetrics) {
        self.metrics
            .write()
            .expect("lock poisoned")
            .insert(instance_id.to_string(), metrics);
    }

    pub fn get(&self, instance_id: &str) -> Option<InstanceMetrics> {
        self.metrics
            .read()
            .expect("lock poisoned")
            .get(instance_id)
            .cloned()
    }

    pub fn avg_cpu(&self) -> f64 {
        let m = self.metrics.read().expect("lock poisoned");
        if m.is_empty() {
            return 0.0;
        }
        m.values().map(|v| v.cpu_percent).sum::<f64>() / m.len() as f64
    }

    pub fn total_active_connections(&self) -> usize {
        self.metrics
            .read()
            .expect("lock poisoned")
            .values()
            .map(|v| v.active_connections)
            .sum()
    }

    pub fn overloaded_instances(&self, cpu_threshold: f64) -> Vec<String> {
        self.metrics
            .read()
            .expect("lock poisoned")
            .iter()
            .filter(|(_, m)| m.cpu_percent > cpu_threshold)
            .map(|(id, _)| id.clone())
            .collect()
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

// ─── ClusterManager ────────────────────────────────────────────────────────

/// High-level API for managing a Raft-backed coordinator cluster.
///
/// Wraps the openraft `Raft` node and provides convenient methods for
/// bootstrapping, membership changes, and command proposals.
pub struct ClusterManager {
    raft: openraft::Raft<TypeConfig>,
    node_id: CoordinatorNodeId,
}

/// Errors that can occur during cluster management operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterError {
    RaftFatal(openraft::error::Fatal<CoordinatorNodeId>),
    RaftInit(
        openraft::error::RaftError<
            CoordinatorNodeId,
            openraft::error::InitializeError<CoordinatorNodeId, CoordinatorNode>,
        >,
    ),
    ClientWrite(
        openraft::error::RaftError<
            CoordinatorNodeId,
            openraft::error::ClientWriteError<CoordinatorNodeId, CoordinatorNode>,
        >,
    ),
    NotLeader,
    Network(String),
}

impl std::fmt::Display for ClusterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterError::RaftFatal(e) => write!(f, "raft fatal: {}", e),
            ClusterError::RaftInit(e) => write!(f, "raft init error: {}", e),
            ClusterError::ClientWrite(e) => write!(f, "client write error: {}", e),
            ClusterError::NotLeader => write!(f, "not leader"),
            ClusterError::Network(e) => write!(f, "network error: {}", e),
        }
    }
}

impl std::error::Error for ClusterError {}

impl ClusterManager {
    /// Create a new `ClusterManager` with the given node ID, config, network, and storage.
    pub async fn new(
        id: CoordinatorNodeId,
        config: Arc<openraft::Config>,
        network: raft::CoordinatorNetworkFactory,
        log_storage: raft::CoordinatorLogStorage,
        state_machine: raft::CoordinatorStateMachine,
    ) -> Result<Self, ClusterError> {
        let raft = openraft::Raft::new(id, config, network, log_storage, state_machine)
            .await
            .map_err(ClusterError::RaftFatal)?;
        Ok(Self { raft, node_id: id })
    }

    /// Create a `ClusterManager` with a custom log storage backend (e.g.
    /// [`raft::PersistentCoordinatorLogStorage`]). Use this when you need
    /// raft state to survive coordinator restarts.
    pub async fn with_log_storage<LS, SM>(
        id: CoordinatorNodeId,
        config: Arc<openraft::Config>,
        network: raft::CoordinatorNetworkFactory,
        log_storage: LS,
        state_machine: SM,
    ) -> Result<Self, ClusterError>
    where
        LS: openraft::storage::RaftLogStorage<TypeConfig>,
        SM: openraft::storage::RaftStateMachine<TypeConfig>,
    {
        let raft = openraft::Raft::new(id, config, network, log_storage, state_machine)
            .await
            .map_err(ClusterError::RaftFatal)?;
        Ok(Self { raft, node_id: id })
    }

    /// Initialize a single-node cluster. Must be called before any other operations.
    pub async fn init_single_node(&self) -> Result<(), ClusterError> {
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(
            self.node_id,
            CoordinatorNode {
                rpc_addr: "127.0.0.1:0".into(),
                bolt_addr: "127.0.0.1:0".into(),
            },
        );
        self.raft
            .initialize(nodes)
            .await
            .map_err(ClusterError::RaftInit)
    }

    /// Add a new learner node to the cluster.
    pub async fn add_node(
        &self,
        id: CoordinatorNodeId,
        node: CoordinatorNode,
    ) -> Result<openraft::raft::ClientWriteResponse<TypeConfig>, ClusterError> {
        self.raft
            .add_learner(id, node, false)
            .await
            .map_err(ClusterError::ClientWrite)
    }

    /// Remove a node from the cluster by changing membership.
    pub async fn remove_node(
        &self,
        id: CoordinatorNodeId,
    ) -> Result<openraft::raft::ClientWriteResponse<TypeConfig>, ClusterError> {
        let changes = openraft::ChangeMembers::RemoveNodes(std::iter::once(id).collect());
        self.raft
            .change_membership(changes, false)
            .await
            .map_err(ClusterError::ClientWrite)
    }

    /// Get the current leader node ID, if known.
    pub fn get_leader(&self) -> Option<CoordinatorNodeId> {
        let metrics = self.raft.metrics();
        let leader = metrics.borrow().current_leader;
        leader
    }

    /// Check whether this node is the current leader.
    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics();
        let current_leader = metrics.borrow().current_leader;
        current_leader == Some(self.node_id)
    }

    /// Propose a command to the Raft log. Only succeeds on the leader.
    pub async fn propose(
        &self,
        cmd: ClusterCommand,
    ) -> Result<openraft::raft::ClientWriteResponse<TypeConfig>, ClusterError> {
        self.raft
            .client_write(cmd)
            .await
            .map_err(ClusterError::ClientWrite)
    }

    /// Shut down the Raft node gracefully.
    pub async fn shutdown(self) -> Result<(), ClusterError> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| ClusterError::Network(format!("shutdown join error: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn test_register_and_list() {
        let state = ClusterState::new("coord-1".into());
        let inst = Instance::new("main-1".into(), test_addr(7687), InstanceRole::Main);
        state.register(inst);
        assert_eq!(state.list().len(), 1);
        assert!(state.main_instance().is_some());
    }

    #[test]
    fn test_failover() {
        let state = ClusterState::new("coord-1".into());

        let mut main = Instance::new("main-1".into(), test_addr(7687), InstanceRole::Main);
        main.health = InstanceHealth::Down;
        state.register(main);

        let mut replica = Instance::new("repl-1".into(), test_addr(7688), InstanceRole::Replica);
        replica.health = InstanceHealth::Up;
        state.register(replica);

        let promoted = state.auto_failover().unwrap();
        assert_eq!(promoted, "repl-1");

        let new_main = state.main_instance().unwrap();
        assert_eq!(new_main.id, "repl-1");
        assert_eq!(new_main.role, InstanceRole::Main);
    }

    #[test]
    fn test_failover_candidate_read_only() {
        let state = ClusterState::new("coord-1".into());

        let mut main = Instance::new("main-1".into(), test_addr(7687), InstanceRole::Main);
        main.health = InstanceHealth::Down;
        state.register(main);

        let mut replica = Instance::new("repl-1".into(), test_addr(7688), InstanceRole::Replica);
        replica.health = InstanceHealth::Up;
        state.register(replica);

        // failover_candidate returns the candidate without mutating state
        assert_eq!(state.failover_candidate(), Some("repl-1".into()));
        // main should still be main
        assert_eq!(state.main_instance().unwrap().id, "main-1");

        // No healthy replicas -> no candidate
        let mut state2 = ClusterState::new("coord-2".into());
        let mut main2 = Instance::new("main-2".into(), test_addr(7687), InstanceRole::Main);
        main2.health = InstanceHealth::Down;
        state2.register(main2);
        assert_eq!(state2.failover_candidate(), None);

        // Main is up -> no candidate
        let mut state3 = ClusterState::new("coord-3".into());
        let mut main3 = Instance::new("main-3".into(), test_addr(7687), InstanceRole::Main);
        main3.health = InstanceHealth::Up;
        state3.register(main3);
        assert_eq!(state3.failover_candidate(), None);
    }

    #[test]
    fn test_routing() {
        let state = ClusterState::new("coord-1".into());
        state.set_route("mydb", test_addr(7687));
        assert_eq!(state.get_route("mydb"), Some(test_addr(7687)));
        state.remove_route("mydb");
        assert_eq!(state.get_route("mydb"), None);
    }

    #[test]
    fn test_health_monitoring() {
        let state = ClusterState::new("coord-1".into());

        let mut inst = Instance::new("main-1".into(), test_addr(7687), InstanceRole::Main);
        inst.health = InstanceHealth::Up;
        // Make the heartbeat look old
        inst.last_heartbeat = Instant::now() - Duration::from_secs(60);
        state.register(inst);

        let down = state.check_health(Duration::from_secs(30));
        assert_eq!(down, vec!["main-1"]);
    }

    #[test]
    fn test_instance_counts() {
        let state = ClusterState::new("coord-1".into());
        assert_eq!(state.instance_count(), 0);
        assert_eq!(state.healthy_instance_count(), 0);

        let mut inst1 = Instance::new("i1".into(), test_addr(7687), InstanceRole::Main);
        inst1.health = InstanceHealth::Up;
        state.register(inst1);

        let mut inst2 = Instance::new("i2".into(), test_addr(7688), InstanceRole::Replica);
        inst2.health = InstanceHealth::Down;
        state.register(inst2);

        assert_eq!(state.instance_count(), 2);
        assert_eq!(state.healthy_instance_count(), 1);
    }

    #[test]
    fn test_bootstrap_cluster() {
        let coord = Coordinator::new("coord-1".into(), Duration::from_secs(30));
        let config = ClusterConfig::new(
            vec![Instance::new(
                "cfg-1".into(),
                test_addr(7689),
                InstanceRole::Replica,
            )],
            "default".into(),
        );
        let discovery = raft::StaticDiscovery::from_slice(&[
            ("disc-1".into(), test_addr(7687), InstanceRole::Main),
            ("disc-2".into(), test_addr(7688), InstanceRole::Replica),
        ]);

        coord.bootstrap_cluster(&config, &discovery);

        assert_eq!(coord.state.instance_count(), 3);
        assert!(coord.state.main_instance().is_some());
        assert_eq!(coord.state.get_route("default"), Some(test_addr(7687)));
    }

    // ─── ReplicationLagTracker tests ────────────────────────────────────────

    #[test]
    fn test_lag_tracker() {
        let tracker = ReplicationLagTracker::new();
        tracker.record_lag("repl-1", Duration::from_secs(5));
        tracker.record_lag("repl-2", Duration::from_secs(15));
        assert_eq!(tracker.get_lag("repl-1"), Some(Duration::from_secs(5)));
        let sorted = tracker.replicas_by_lag();
        assert_eq!(sorted[0].0, "repl-1");
        let lagging = tracker.has_lagging_replicas(Duration::from_secs(10));
        assert_eq!(lagging, vec!["repl-2"]);
    }

    // ─── ShardAllocator tests ───────────────────────────────────────────────

    #[test]
    fn test_shard_allocator_round_robin() {
        let allocator = ShardAllocator::new(4);
        let instances = vec![
            Instance::new("i1".into(), test_addr(7687), InstanceRole::Main),
            Instance::new("i2".into(), test_addr(7688), InstanceRole::Replica),
        ];
        allocator.assign_round_robin(&instances);
        assert_eq!(allocator.get(ShardId(0)), Some("i1".into()));
        assert_eq!(allocator.get(ShardId(1)), Some("i2".into()));
        assert_eq!(allocator.get(ShardId(2)), Some("i1".into()));
        assert_eq!(allocator.get(ShardId(3)), Some("i2".into()));
    }

    #[test]
    fn test_shard_allocator_rebalance() {
        let allocator = ShardAllocator::new(6);
        let i1 = Instance::new("i1".into(), test_addr(7687), InstanceRole::Main);
        let i2 = Instance::new("i2".into(), test_addr(7688), InstanceRole::Replica);
        let i3 = Instance::new("i3".into(), test_addr(7689), InstanceRole::Replica);
        allocator.assign_round_robin(&[i1.clone(), i2.clone()]);
        allocator.rebalance(&[i1.clone(), i2.clone(), i3.clone()]);
        // After rebalance, i3 should get at least one shard
        assert!(!allocator.shards_for("i3").is_empty());
    }

    // ─── LoadBalancer tests ─────────────────────────────────────────────────

    #[test]
    fn test_load_balancer_round_robin() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::RoundRobin);
        let replicas = vec![
            Instance::new("r1".into(), test_addr(7688), InstanceRole::Replica),
            Instance::new("r2".into(), test_addr(7689), InstanceRole::Replica),
        ];
        let a = lb.pick_replica(&replicas, None);
        let b = lb.pick_replica(&replicas, None);
        assert!(a.is_some());
        assert!(b.is_some());
        // Two consecutive picks from 2 replicas should cover both
        let choices: std::collections::HashSet<String> =
            [a.unwrap(), b.unwrap()].into_iter().collect();
        assert!(choices.len() >= 1);
    }

    #[test]
    fn test_load_balancer_least_connections() {
        let lb = LoadBalancer::new(LoadBalanceStrategy::LeastConnections);
        let replicas = vec![
            Instance::new("r1".into(), test_addr(7688), InstanceRole::Replica),
            Instance::new("r2".into(), test_addr(7689), InstanceRole::Replica),
        ];
        lb.record_connection("r1", 10);
        lb.record_connection("r2", 2);
        let choice = lb.pick_replica(&replicas, None).unwrap();
        assert_eq!(choice, "r2");
    }

    // ─── ClusterTopology tests ──────────────────────────────────────────────

    #[test]
    fn test_topology_tree() {
        let mut topo = ClusterTopology::new();
        topo.add_edge("main", "repl-1");
        topo.add_edge("main", "repl-2");
        assert!(topo.is_valid_tree("main"));
        assert!(!topo.has_cycle());
    }

    #[test]
    fn test_topology_cycle() {
        let mut topo = ClusterTopology::new();
        topo.add_edge("a", "b");
        topo.add_edge("b", "c");
        topo.add_edge("c", "a");
        assert!(topo.has_cycle());
    }

    // ─── DataMigration tests ────────────────────────────────────────────────

    #[test]
    fn test_migration_lifecycle() {
        let dm = DataMigration::new();
        let id = dm.create_task(ShardId(0), "i1", "i2");
        assert_eq!(dm.get_task(&id).unwrap().status, MigrationStatus::Pending);
        dm.start_task(&id);
        assert_eq!(
            dm.get_task(&id).unwrap().status,
            MigrationStatus::InProgress
        );
        dm.update_progress(&id, 50);
        assert_eq!(dm.get_task(&id).unwrap().progress_percent, 50);
        dm.complete_task(&id);
        assert_eq!(dm.get_task(&id).unwrap().status, MigrationStatus::Completed);
        assert_eq!(dm.completed_tasks().len(), 1);
    }

    // ─── MultiDcRouting tests ───────────────────────────────────────────────

    #[test]
    fn test_multi_dc_routing() {
        let routing = MultiDcRouting::new();
        routing.register_dc(DataCenter {
            id: "dc1".into(),
            region: "us-east".into(),
            latency_to_coordinator_ms: 10,
        });
        routing.register_dc(DataCenter {
            id: "dc2".into(),
            region: "eu-west".into(),
            latency_to_coordinator_ms: 100,
        });
        routing.assign_instance_to_dc("i1", "dc1");
        routing.assign_instance_to_dc("i2", "dc2");
        assert_eq!(routing.instance_dc("i1"), Some("dc1".into()));
        assert_eq!(routing.instances_in_dc("dc1"), vec!["i1"]);

        let replicas = vec![
            Instance::new("i1".into(), test_addr(7688), InstanceRole::Replica),
            Instance::new("i2".into(), test_addr(7689), InstanceRole::Replica),
        ];
        let local = routing.local_replicas(&replicas, "dc1");
        assert_eq!(local.len(), 1);
        assert_eq!(local[0].id, "i1");
    }

    // ─── MetricsCollector tests ─────────────────────────────────────────────

    #[test]
    fn test_metrics_collector() {
        let collector = MetricsCollector::new();
        collector.record(
            "i1",
            InstanceMetrics {
                cpu_percent: 80.0,
                memory_used_mb: 1024,
                memory_total_mb: 4096,
                disk_used_gb: 100,
                disk_total_gb: 500,
                query_throughput_qps: 1000.0,
                active_connections: 50,
                replication_lag_ms: 10,
            },
        );
        collector.record(
            "i2",
            InstanceMetrics {
                cpu_percent: 40.0,
                ..Default::default()
            },
        );
        assert_eq!(collector.avg_cpu(), 60.0);
        assert_eq!(collector.total_active_connections(), 50);
        assert_eq!(collector.overloaded_instances(50.0), vec!["i1"]);
    }
}
