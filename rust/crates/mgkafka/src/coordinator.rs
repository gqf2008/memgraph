//! Consumer group coordinator and offset management.
//!
//! Thread-safe in-memory implementation of the Kafka consumer group coordinator
//! protocol, supporting multiple groups, multiple members per group, automatic
//! rebalancing, heartbeat tracking, and offset storage.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::partition::{assign_range, assign_round_robin, Member, Partition};

// ─── Error types ───────────────────────────────────────────────────────────

/// Errors that can occur during coordinator operations.
#[derive(Clone, Debug, PartialEq)]
pub enum CoordinatorError {
    /// The group does not exist.
    UnknownGroup(String),
    /// The member is not part of the group.
    UnknownMember(String),
    /// The generation ID does not match the group's current generation.
    InvalidGeneration(i32),
    /// The member is not the leader of the group.
    NotLeader,
    /// The group is in a state that does not allow the requested operation.
    InvalidState(MemberState),
    /// Rebalance is in progress.
    RebalanceInProgress,
    /// The session has timed out.
    SessionTimeout,
}

impl std::fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoordinatorError::UnknownGroup(g) => write!(f, "unknown group: {}", g),
            CoordinatorError::UnknownMember(m) => write!(f, "unknown member: {}", m),
            CoordinatorError::InvalidGeneration(g) => {
                write!(f, "invalid generation id: {}", g)
            }
            CoordinatorError::NotLeader => write!(f, "not the group leader"),
            CoordinatorError::InvalidState(s) => write!(f, "invalid group state: {:?}", s),
            CoordinatorError::RebalanceInProgress => write!(f, "rebalance in progress"),
            CoordinatorError::SessionTimeout => write!(f, "session timeout"),
        }
    }
}

impl std::error::Error for CoordinatorError {}

// ─── Member state ──────────────────────────────────────────────────────────

/// State of a consumer group member.
#[derive(Clone, Debug, PartialEq)]
pub enum MemberState {
    /// Group is stable and consuming.
    Stable,
    /// Group is preparing for rebalance (members joining).
    PreparingRebalance,
    /// Group is completing rebalance (waiting for sync).
    CompletingRebalance,
    /// Group is empty/dead.
    Dead,
}

/// Protocol subscription for a member.
#[derive(Clone, Debug, PartialEq)]
pub struct MemberSubscription {
    pub protocols: Vec<String>,
    pub topics: Vec<String>,
}

/// Metadata for a single member in a consumer group.
#[derive(Clone, Debug)]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub subscription: MemberSubscription,
    pub assignment: Vec<u8>,
    pub last_heartbeat: Instant,
    pub rebalance_timeout_ms: i32,
    pub session_timeout_ms: i32,
}

// ─── Consumer group ────────────────────────────────────────────────────────

/// A consumer group as tracked by the coordinator.
#[derive(Clone, Debug)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub generation_id: i32,
    pub protocol_type: String,
    pub protocol_name: Option<String>,
    pub leader: Option<String>,
    pub members: HashMap<String, GroupMember>,
    pub state: MemberState,
    pub created_at: Instant,
}

impl ConsumerGroup {
    fn new(group_id: String, protocol_type: String) -> Self {
        Self {
            group_id,
            generation_id: 0,
            protocol_type,
            protocol_name: None,
            leader: None,
            members: HashMap::new(),
            state: MemberState::Stable,
            created_at: Instant::now(),
        }
    }

    /// Check if the group is currently rebalancing.
    pub fn is_rebalancing(&self) -> bool {
        matches!(
            self.state,
            MemberState::PreparingRebalance | MemberState::CompletingRebalance
        )
    }

    /// Get a list of member IDs sorted for deterministic leader election.
    fn sorted_member_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.members.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Elect a new leader from current members (deterministic: first sorted).
    fn elect_leader(&mut self) {
        let ids = self.sorted_member_ids();
        self.leader = ids.first().cloned();
    }

    /// Select the common protocol among all members.
    /// Uses the leader's preference order for determinism.
    fn select_protocol(&self) -> Option<String> {
        if self.members.is_empty() {
            return None;
        }
        // Use the leader's protocol list as the reference order.
        let leader_id = self.leader.as_ref()?;
        let leader_protocols = self.members.get(leader_id)?.subscription.protocols.clone();
        if leader_protocols.is_empty() {
            return None;
        }
        // Collect all supported protocols per member
        let all_protocols: Vec<Vec<String>> = self
            .members
            .values()
            .map(|m| m.subscription.protocols.clone())
            .collect();
        // Find first protocol in leader's preference that all members support
        for proto in &leader_protocols {
            if all_protocols.iter().all(|p| p.contains(proto)) {
                return Some(proto.clone());
            }
        }
        None
    }
}

// ─── Join result ───────────────────────────────────────────────────────────

/// Result of a successful join_group call.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinGroupResult {
    pub generation_id: i32,
    pub member_id: String,
    pub leader: String,
    pub protocol_name: Option<String>,
    pub members: Vec<(String, Vec<u8>)>, // (member_id, metadata) — only populated for leader
}

// ─── Sync result ───────────────────────────────────────────────────────────

/// Result of a successful sync_group call.
#[derive(Clone, Debug, PartialEq)]
pub struct SyncGroupResult {
    pub assignment: Vec<u8>,
    pub protocol_name: Option<String>,
    pub protocol_type: Option<String>,
}

// ─── Group coordinator ─────────────────────────────────────────────────────

/// Thread-safe in-memory group coordinator.
pub struct GroupCoordinator {
    groups: RwLock<HashMap<String, Arc<Mutex<ConsumerGroup>>>>,
    /// Default session timeout for members (milliseconds).
    default_session_timeout_ms: i32,
    /// Default rebalance timeout (milliseconds).
    default_rebalance_timeout_ms: i32,
}

impl GroupCoordinator {
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            default_session_timeout_ms: 10_000,
            default_rebalance_timeout_ms: 30_000,
        }
    }

    /// Create with custom timeout defaults.
    pub fn with_timeouts(session_timeout_ms: i32, rebalance_timeout_ms: i32) -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            default_session_timeout_ms: session_timeout_ms,
            default_rebalance_timeout_ms: rebalance_timeout_ms,
        }
    }

    // ── Internal helpers ───────────────────────────────────────────────────

    fn get_or_create_group(
        &self,
        group_id: &str,
        protocol_type: &str,
    ) -> Arc<Mutex<ConsumerGroup>> {
        let mut groups = self.groups.write().unwrap();
        groups
            .entry(group_id.to_string())
            .or_insert_with(|| {
                Arc::new(Mutex::new(ConsumerGroup::new(
                    group_id.to_string(),
                    protocol_type.to_string(),
                )))
            })
            .clone()
    }

    fn get_group_arc(&self, group_id: &str) -> Option<Arc<Mutex<ConsumerGroup>>> {
        let groups = self.groups.read().unwrap();
        groups.get(group_id).cloned()
    }

    fn remove_group(&self, group_id: &str) {
        let mut groups = self.groups.write().unwrap();
        groups.remove(group_id);
    }

    /// Generate a unique member ID.
    fn generate_member_id(&self, client_id: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("{}-{:x}", client_id, n)
    }

    // ── Group lifecycle ────────────────────────────────────────────────────

    /// Handle a consumer joining a group.
    ///
    /// If `member_id` is empty, a new member ID is assigned.
    /// Triggers a rebalance (PreparingRebalance state) when a new member joins
    /// or when an existing member rejoins with a stale generation.
    pub fn join_group(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        client_id: &str,
        client_host: &str,
        protocol_type: &str,
        protocols: Vec<String>,
        topics: Vec<String>,
        session_timeout_ms: Option<i32>,
        rebalance_timeout_ms: Option<i32>,
    ) -> Result<JoinGroupResult, CoordinatorError> {
        let session_timeout = session_timeout_ms.unwrap_or(self.default_session_timeout_ms);
        let rebalance_timeout = rebalance_timeout_ms.unwrap_or(self.default_rebalance_timeout_ms);

        let group_arc = self.get_or_create_group(group_id, protocol_type);
        let mut group = group_arc.lock().unwrap();

        // Validate protocol type matches
        if !group.protocol_type.is_empty() && group.protocol_type != protocol_type {
            return Err(CoordinatorError::InvalidState(MemberState::Dead));
        }

        // If member_id is provided but not found, it's a rejoin with unknown member
        let is_new_member = member_id
            .map(|mid| mid.is_empty() || !group.members.contains_key(mid))
            .unwrap_or(true);

        let assigned_member_id = if is_new_member {
            self.generate_member_id(client_id)
        } else {
            member_id.unwrap().to_string()
        };

        // Transition to PreparingRebalance and bump generation when a member
        // joins a stable group. If already rebalancing, stay in the same
        // generation so all members sync to the same epoch.
        if group.state != MemberState::PreparingRebalance {
            group.state = MemberState::PreparingRebalance;
            group.generation_id += 1;
        }

        // Insert or update member
        let member = GroupMember {
            member_id: assigned_member_id.clone(),
            client_id: client_id.to_string(),
            client_host: client_host.to_string(),
            subscription: MemberSubscription {
                protocols: protocols.clone(),
                topics: topics.clone(),
            },
            assignment: Vec::new(),
            last_heartbeat: Instant::now(),
            rebalance_timeout_ms: rebalance_timeout,
            session_timeout_ms: session_timeout,
        };

        let was_leader = group
            .leader
            .as_ref()
            .map(|l| l == &assigned_member_id)
            .unwrap_or(false);
        group.members.insert(assigned_member_id.clone(), member);

        // Elect leader if none or if the only member
        if group.leader.is_none() || group.members.len() == 1 {
            group.elect_leader();
        }

        // If this is a rejoin of the leader, keep them as leader
        if was_leader {
            group.leader = Some(assigned_member_id.clone());
        }

        // Select protocol
        group.protocol_name = group.select_protocol();

        // Build member list for leader (metadata bytes are empty here —
        // in a real implementation they'd be the subscription metadata)
        let members_for_leader: Vec<(String, Vec<u8>)> = if group
            .leader
            .as_ref()
            .map(|l| l == &assigned_member_id)
            .unwrap_or(false)
        {
            group
                .members
                .values()
                .map(|m| (m.member_id.clone(), Vec::new()))
                .collect()
        } else {
            Vec::new()
        };

        let leader_id = group.leader.clone().unwrap_or_default();

        Ok(JoinGroupResult {
            generation_id: group.generation_id,
            member_id: assigned_member_id,
            leader: leader_id,
            protocol_name: group.protocol_name.clone(),
            members: members_for_leader,
        })
    }

    /// Synchronize group state and assign partitions to members.
    ///
    /// Only the leader may provide assignments. Non-leaders receive their
    /// pre-stored assignment. After successful sync, the group transitions
    /// to `Stable`.
    pub fn sync_group(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        assignments: Option<Vec<(String, Vec<u8>)>>,
    ) -> Result<SyncGroupResult, CoordinatorError> {
        let group_arc = self
            .get_group_arc(group_id)
            .ok_or_else(|| CoordinatorError::UnknownGroup(group_id.to_string()))?;
        let mut group = group_arc.lock().unwrap();

        if group.generation_id != generation_id {
            return Err(CoordinatorError::InvalidGeneration(group.generation_id));
        }

        if !group.members.contains_key(member_id) {
            return Err(CoordinatorError::UnknownMember(member_id.to_string()));
        }

        // Only leader can provide assignments
        let is_leader = group
            .leader
            .as_ref()
            .map(|l| l == member_id)
            .unwrap_or(false);

        if let Some(new_assignments) = assignments {
            if !is_leader {
                return Err(CoordinatorError::NotLeader);
            }
            // Store assignments for all members
            for (mid, assignment) in new_assignments {
                if let Some(m) = group.members.get_mut(&mid) {
                    m.assignment = assignment;
                }
            }
        }

        // Transition to Stable after sync
        group.state = MemberState::Stable;

        // Return this member's assignment
        let member_assignment = group
            .members
            .get(member_id)
            .map(|m| m.assignment.clone())
            .unwrap_or_default();

        Ok(SyncGroupResult {
            assignment: member_assignment,
            protocol_name: group.protocol_name.clone(),
            protocol_type: Some(group.protocol_type.clone()),
        })
    }

    /// Record a heartbeat from a member.
    ///
    /// Updates the member's last heartbeat timestamp. If the generation ID
    /// does not match, returns an error to trigger rejoin.
    pub fn heartbeat(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
    ) -> Result<(), CoordinatorError> {
        let group_arc = self
            .get_group_arc(group_id)
            .ok_or_else(|| CoordinatorError::UnknownGroup(group_id.to_string()))?;
        let mut group = group_arc.lock().unwrap();

        if group.generation_id != generation_id {
            return Err(CoordinatorError::InvalidGeneration(group.generation_id));
        }

        let member = group
            .members
            .get_mut(member_id)
            .ok_or_else(|| CoordinatorError::UnknownMember(member_id.to_string()))?;

        member.last_heartbeat = Instant::now();
        Ok(())
    }

    /// Remove a member from a group.
    ///
    /// Triggers a rebalance (new generation) if the group still has members.
    /// If the group becomes empty, it transitions to `Dead`.
    pub fn leave_group(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> Result<(), CoordinatorError> {
        let group_arc = self
            .get_group_arc(group_id)
            .ok_or_else(|| CoordinatorError::UnknownGroup(group_id.to_string()))?;
        let mut group = group_arc.lock().unwrap();

        if !group.members.contains_key(member_id) {
            return Err(CoordinatorError::UnknownMember(member_id.to_string()));
        }

        group.members.remove(member_id);

        if group.members.is_empty() {
            group.state = MemberState::Dead;
            group.leader = None;
            group.generation_id += 1;
        } else {
            // Trigger rebalance
            group.state = MemberState::PreparingRebalance;
            group.generation_id += 1;

            // Re-elect leader if the leaving member was the leader
            if group.leader.as_deref() == Some(member_id) {
                group.elect_leader();
            }
        }

        Ok(())
    }

    /// Check for timed-out members and remove them, triggering rebalances.
    ///
    /// Call this periodically (e.g., from a background task) to enforce
    /// session timeouts.
    pub fn check_timeouts(&self) -> Vec<(String, String)> {
        let mut timed_out = Vec::new();
        let group_ids: Vec<String> = {
            let groups = self.groups.read().unwrap();
            groups.keys().cloned().collect()
        };

        for group_id in group_ids {
            if let Some(group_arc) = self.get_group_arc(&group_id) {
                let mut group = group_arc.lock().unwrap();
                let now = Instant::now();
                let to_remove: Vec<String> = group
                    .members
                    .values()
                    .filter(|m| {
                        let elapsed = now.duration_since(m.last_heartbeat);
                        elapsed > Duration::from_millis(m.session_timeout_ms as u64)
                    })
                    .map(|m| m.member_id.clone())
                    .collect();

                for mid in to_remove {
                    group.members.remove(&mid);
                    timed_out.push((group_id.clone(), mid));
                }

                if !timed_out.is_empty() {
                    if group.members.is_empty() {
                        group.state = MemberState::Dead;
                        group.leader = None;
                    } else {
                        group.state = MemberState::PreparingRebalance;
                        group.elect_leader();
                    }
                    group.generation_id += 1;
                }
            }
        }

        timed_out
    }

    /// Get a snapshot of a group's state.
    pub fn get_group(&self, group_id: &str) -> Option<ConsumerGroup> {
        let group_arc = self.get_group_arc(group_id)?;
        let group = group_arc.lock().unwrap();
        Some(group.clone())
    }

    /// List all known group IDs.
    pub fn list_groups(&self) -> Vec<String> {
        let groups = self.groups.read().unwrap();
        groups.keys().cloned().collect()
    }

    /// Delete a group entirely.
    pub fn delete_group(&self, group_id: &str) {
        self.remove_group(group_id);
    }

    /// Get the number of tracked groups.
    pub fn group_count(&self) -> usize {
        let groups = self.groups.read().unwrap();
        groups.len()
    }
}

impl Default for GroupCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Automatic partition assignment helper ─────────────────────────────────

/// Assign partitions to group members using a named strategy.
///
/// Supported strategies: "range" and "roundrobin".
/// Returns a map of member_id -> list of assigned (topic, partition).
pub fn assign_partitions(
    strategy: &str,
    members: &[Member],
    partitions: &[Partition],
) -> Vec<(String, Partition)> {
    match strategy {
        "roundrobin" => assign_round_robin(members, partitions),
        _ => assign_range(members, partitions),
    }
}

// ─── Offset store ──────────────────────────────────────────────────────────

/// A committed offset for a specific topic-partition.
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedOffset {
    pub offset: i64,
    pub metadata: Option<String>,
    pub leader_epoch: i32,
}

/// Thread-safe in-memory offset store.
pub struct OffsetStore {
    offsets: Mutex<HashMap<(String, String, i32), CommittedOffset>>,
}

impl OffsetStore {
    pub fn new() -> Self {
        Self {
            offsets: Mutex::new(HashMap::new()),
        }
    }

    /// Commit an offset for a group/topic/partition.
    pub fn commit(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        metadata: Option<String>,
        leader_epoch: i32,
    ) {
        let mut offsets = self.offsets.lock().unwrap();
        let key = (group_id.to_string(), topic.to_string(), partition);
        offsets.insert(
            key,
            CommittedOffset {
                offset,
                metadata,
                leader_epoch,
            },
        );
    }

    /// Fetch a committed offset.
    pub fn fetch(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> Option<CommittedOffset> {
        let offsets = self.offsets.lock().unwrap();
        offsets
            .get(&(group_id.to_string(), topic.to_string(), partition))
            .cloned()
    }

    /// Delete offsets for a group.
    pub fn delete_group(&self, group_id: &str) {
        let mut offsets = self.offsets.lock().unwrap();
        offsets.retain(|key, _| key.0 != group_id);
    }

    /// List all committed offsets for a group.
    pub fn list_group_offsets(&self, group_id: &str) -> Vec<((String, i32), CommittedOffset)> {
        let offsets = self.offsets.lock().unwrap();
        offsets
            .iter()
            .filter(|((g, _, _), _)| g == group_id)
            .map(|((_, topic, partition), offset)| ((topic.clone(), *partition), offset.clone()))
            .collect()
    }

    /// Fetch offsets for multiple partitions at once.
    pub fn fetch_many(
        &self,
        group_id: &str,
        topic_partitions: &[(String, i32)],
    ) -> Vec<((String, i32), Option<CommittedOffset>)> {
        let offsets = self.offsets.lock().unwrap();
        topic_partitions
            .iter()
            .map(|(topic, partition)| {
                let key = (group_id.to_string(), topic.clone(), *partition);
                let off = offsets.get(&key).cloned();
                ((topic.clone(), *partition), off)
            })
            .collect()
    }
}

impl Default for OffsetStore {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_join_group_creates_group() {
        let coord = GroupCoordinator::new();
        let result = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        assert_eq!(result.generation_id, 1);
        assert!(!result.member_id.is_empty());
        assert_eq!(result.leader, result.member_id); // first member is leader
        assert_eq!(result.protocol_name, Some("range".to_string()));
        assert!(!result.members.is_empty()); // leader gets member list
    }

    #[test]
    fn test_join_group_multiple_members() {
        let coord = GroupCoordinator::new();

        let r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        let r2 = coord
            .join_group(
                "g1",
                None,
                "client-2",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        assert_eq!(r1.leader, r1.member_id); // leader stays
        assert_eq!(r2.leader, r1.member_id); // second member sees same leader
        assert_eq!(r2.generation_id, 1); // same generation — still in same rebalance
        assert!(r2.members.is_empty()); // non-leader gets empty members list
    }

    #[test]
    fn test_join_group_with_provided_member_id() {
        let coord = GroupCoordinator::new();

        let r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        // Rejoin with same member ID
        let r2 = coord
            .join_group(
                "g1",
                Some(&r1.member_id),
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        assert_eq!(r2.member_id, r1.member_id);
        assert_eq!(r2.generation_id, 1); // same rebalance epoch
    }

    #[test]
    fn test_sync_group_leader_and_follower() {
        let coord = GroupCoordinator::new();

        let r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        let r2 = coord
            .join_group(
                "g1",
                None,
                "client-2",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        // After r2 joined, generation bumped to 2. Use current generation.
        let current_gen = coord.get_group("g1").unwrap().generation_id;

        // Leader provides assignments for all members
        let assignments = vec![
            (r1.member_id.clone(), vec![1, 2]),
            (r2.member_id.clone(), vec![3, 4]),
        ];

        let sync1 = coord
            .sync_group("g1", current_gen, &r1.member_id, Some(assignments))
            .unwrap();
        assert_eq!(sync1.assignment, vec![1, 2]);
        assert_eq!(sync1.protocol_name, Some("range".to_string()));

        // Follower gets their pre-stored assignment
        let sync2 = coord
            .sync_group("g1", current_gen, &r2.member_id, None)
            .unwrap();
        assert_eq!(sync2.assignment, vec![3, 4]);

        // Group should now be Stable
        let group = coord.get_group("g1").unwrap();
        assert_eq!(group.state, MemberState::Stable);
    }

    #[test]
    fn test_sync_group_non_leader_cannot_assign() {
        let coord = GroupCoordinator::new();

        let _r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        let r2 = coord
            .join_group(
                "g1",
                None,
                "client-2",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        let result = coord.sync_group(
            "g1",
            r2.generation_id,
            &r2.member_id,
            Some(vec![(r2.member_id.clone(), vec![])]),
        );
        assert_eq!(result, Err(CoordinatorError::NotLeader));
    }

    #[test]
    fn test_sync_group_invalid_generation() {
        let coord = GroupCoordinator::new();

        let r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        let result = coord.sync_group("g1", r1.generation_id - 1, &r1.member_id, None);
        assert_eq!(
            result,
            Err(CoordinatorError::InvalidGeneration(r1.generation_id))
        );
    }

    #[test]
    fn test_heartbeat_updates_liveness() {
        let coord = GroupCoordinator::new();

        let r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        coord
            .heartbeat("g1", r1.generation_id, &r1.member_id)
            .unwrap();

        // Heartbeat with wrong generation fails
        let result = coord.heartbeat("g1", r1.generation_id + 1, &r1.member_id);
        assert_eq!(
            result,
            Err(CoordinatorError::InvalidGeneration(r1.generation_id))
        );

        // Heartbeat for unknown member fails
        let result = coord.heartbeat("g1", r1.generation_id, "unknown-member");
        assert_eq!(
            result,
            Err(CoordinatorError::UnknownMember("unknown-member".to_string()))
        );
    }

    #[test]
    fn test_leave_group_triggers_rebalance() {
        let coord = GroupCoordinator::new();

        let r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        let r2 = coord
            .join_group(
                "g1",
                None,
                "client-2",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        let current_gen = coord.get_group("g1").unwrap().generation_id;
        coord
            .sync_group(
                "g1",
                current_gen,
                &r1.member_id,
                Some(vec![
                    (r1.member_id.clone(), vec![1]),
                    (r2.member_id.clone(), vec![2]),
                ]),
            )
            .unwrap();

        // Leave group
        coord.leave_group("g1", &r1.member_id).unwrap();

        let group = coord.get_group("g1").unwrap();
        assert_eq!(group.members.len(), 1);
        assert_eq!(group.leader, Some(r2.member_id.clone()));
        assert_eq!(group.state, MemberState::PreparingRebalance);
        assert_eq!(group.generation_id, 2); // bumped once by leave
    }

    #[test]
    fn test_leave_group_last_member_makes_dead() {
        let coord = GroupCoordinator::new();

        let r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        coord.leave_group("g1", &r1.member_id).unwrap();

        let group = coord.get_group("g1").unwrap();
        assert_eq!(group.state, MemberState::Dead);
        assert!(group.leader.is_none());
    }

    #[test]
    fn test_leave_group_unknown_group() {
        let coord = GroupCoordinator::new();
        let result = coord.leave_group("nonexistent", "m1");
        assert_eq!(
            result,
            Err(CoordinatorError::UnknownGroup("nonexistent".to_string()))
        );
    }

    #[test]
    fn test_leave_group_unknown_member() {
        let coord = GroupCoordinator::new();

        let _ = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        let result = coord.leave_group("g1", "unknown");
        assert_eq!(result, Err(CoordinatorError::UnknownMember("unknown".to_string())));
    }

    #[test]
    fn test_timeout_check_removes_stale_members() {
        let coord = GroupCoordinator::with_timeouts(50, 100);

        let r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                Some(50),
                Some(100),
            )
            .unwrap();

        let _r2 = coord
            .join_group(
                "g1",
                None,
                "client-2",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                Some(50),
                Some(100),
            )
            .unwrap();

        // Don't heartbeat — wait for timeout
        std::thread::sleep(Duration::from_millis(120));

        let timed_out = coord.check_timeouts();
        assert_eq!(timed_out.len(), 2);
        assert!(timed_out.iter().any(|(_, m)| m == &r1.member_id));

        let group = coord.get_group("g1").unwrap();
        assert!(group.members.is_empty());
        assert_eq!(group.state, MemberState::Dead);
    }

    #[test]
    fn test_timeout_check_keeps_active_members() {
        let coord = GroupCoordinator::with_timeouts(500, 1000);

        let r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                Some(500),
                Some(1000),
            )
            .unwrap();

        // Heartbeat keeps member alive
        coord
            .heartbeat("g1", r1.generation_id, &r1.member_id)
            .unwrap();

        std::thread::sleep(Duration::from_millis(50));

        let timed_out = coord.check_timeouts();
        assert!(timed_out.is_empty());

        let group = coord.get_group("g1").unwrap();
        assert_eq!(group.members.len(), 1);
    }

    #[test]
    fn test_multiple_groups_isolation() {
        let coord = GroupCoordinator::new();

        let g1 = coord
            .join_group(
                "group-a",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        let g2 = coord
            .join_group(
                "group-b",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        assert_eq!(coord.group_count(), 2);
        let mut groups = coord.list_groups();
        groups.sort();
        assert_eq!(groups, vec!["group-a", "group-b"]);

        // Generations are independent
        assert_eq!(g1.generation_id, 1);
        assert_eq!(g2.generation_id, 1);
    }

    #[test]
    fn test_protocol_selection_common_protocol() {
        let coord = GroupCoordinator::new();

        let r1 = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string(), "roundrobin".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        let r2 = coord
            .join_group(
                "g1",
                None,
                "client-2",
                "127.0.0.1",
                "consumer",
                vec!["roundrobin".to_string(), "range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        // "range" is common to both and appears first in client-1's list
        assert_eq!(r1.protocol_name, Some("range".to_string()));
        assert_eq!(r2.protocol_name, Some("range".to_string()));
    }

    #[test]
    fn test_delete_group() {
        let coord = GroupCoordinator::new();

        let _ = coord
            .join_group(
                "g1",
                None,
                "client-1",
                "127.0.0.1",
                "consumer",
                vec!["range".to_string()],
                vec!["topic1".to_string()],
                None,
                None,
            )
            .unwrap();

        assert_eq!(coord.group_count(), 1);
        coord.delete_group("g1");
        assert_eq!(coord.group_count(), 0);
        assert!(coord.get_group("g1").is_none());
    }

    #[test]
    fn test_thread_safety_concurrent_joins() {
        let coord = Arc::new(GroupCoordinator::new());
        let mut handles = Vec::new();

        for i in 0..10 {
            let c = coord.clone();
            handles.push(thread::spawn(move || {
                c.join_group(
                    "g1",
                    None,
                    &format!("client-{}", i),
                    "127.0.0.1",
                    "consumer",
                    vec!["range".to_string()],
                    vec!["topic1".to_string()],
                    None,
                    None,
                )
                .unwrap()
            }));
        }

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert_eq!(results.len(), 10);

        let group = coord.get_group("g1").unwrap();
        assert_eq!(group.members.len(), 10);
    }

    #[test]
    fn test_thread_safety_concurrent_heartbeats() {
        let coord = Arc::new(GroupCoordinator::new());
        let mut member_ids = Vec::new();

        for i in 0..5 {
            let r = coord
                .join_group(
                    "g1",
                    None,
                    &format!("client-{}", i),
                    "127.0.0.1",
                    "consumer",
                    vec!["range".to_string()],
                    vec!["topic1".to_string()],
                    None,
                    None,
                )
                .unwrap();
            member_ids.push(r.member_id);
        }

        let gen = coord.get_group("g1").unwrap().generation_id;
        let mut handles = Vec::new();

        for mid in member_ids {
            let c = coord.clone();
            handles.push(thread::spawn(move || {
                c.heartbeat("g1", gen, &mid).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let group = coord.get_group("g1").unwrap();
        assert_eq!(group.members.len(), 5);
    }

    // ─── OffsetStore tests ─────────────────────────────────────────────────

    #[test]
    fn test_offset_store_commit_fetch() {
        let store = OffsetStore::new();
        store.commit("g1", "t1", 0, 100, Some("meta".to_string()), 0);

        let off = store.fetch("g1", "t1", 0).unwrap();
        assert_eq!(off.offset, 100);
        assert_eq!(off.metadata, Some("meta".to_string()));

        // Update
        store.commit("g1", "t1", 0, 200, None, 1);
        let off = store.fetch("g1", "t1", 0).unwrap();
        assert_eq!(off.offset, 200);
    }

    #[test]
    fn test_offset_store_fetch_many() {
        let store = OffsetStore::new();
        store.commit("g1", "t1", 0, 10, None, 0);
        store.commit("g1", "t1", 1, 20, None, 0);
        store.commit("g1", "t2", 0, 30, None, 0);

        let results = store.fetch_many(
            "g1",
            &[
                ("t1".to_string(), 0),
                ("t1".to_string(), 1),
                ("t1".to_string(), 2), // missing
                ("t2".to_string(), 0),
            ],
        );

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].1.as_ref().unwrap().offset, 10);
        assert_eq!(results[1].1.as_ref().unwrap().offset, 20);
        assert!(results[2].1.is_none());
        assert_eq!(results[3].1.as_ref().unwrap().offset, 30);
    }

    #[test]
    fn test_offset_store_delete_group() {
        let store = OffsetStore::new();
        store.commit("g1", "t1", 0, 100, None, 0);
        store.commit("g2", "t1", 0, 200, None, 0);

        store.delete_group("g1");
        assert!(store.fetch("g1", "t1", 0).is_none());
        assert!(store.fetch("g2", "t1", 0).is_some());
    }

    #[test]
    fn test_offset_store_list() {
        let store = OffsetStore::new();
        store.commit("g1", "t1", 0, 10, None, 0);
        store.commit("g1", "t1", 1, 20, None, 0);
        store.commit("g1", "t2", 0, 30, None, 0);
        store.commit("g2", "t1", 0, 99, None, 0);

        let g1_offsets = store.list_group_offsets("g1");
        assert_eq!(g1_offsets.len(), 3);
    }

    #[test]
    fn test_offset_store_thread_safety() {
        let store = Arc::new(OffsetStore::new());
        let mut handles = Vec::new();

        for i in 0..10 {
            let s = store.clone();
            handles.push(thread::spawn(move || {
                s.commit("g1", "t1", i, i as i64, None, 0);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let offsets = store.list_group_offsets("g1");
        assert_eq!(offsets.len(), 10);
    }

    #[test]
    fn test_assign_partitions_helper() {
        let members = vec![
            Member {
                member_id: "m1".to_string(),
            },
            Member {
                member_id: "m2".to_string(),
            },
        ];
        let partitions = vec![
            Partition {
                topic: "t".to_string(),
                partition: 0,
            },
            Partition {
                topic: "t".to_string(),
                partition: 1,
            },
            Partition {
                topic: "t".to_string(),
                partition: 2,
            },
            Partition {
                topic: "t".to_string(),
                partition: 3,
            },
        ];

        let range = assign_partitions("range", &members, &partitions);
        assert_eq!(range.len(), 4);

        let rr = assign_partitions("roundrobin", &members, &partitions);
        assert_eq!(rr.len(), 4);
        assert_eq!(rr[0].0, "m1");
        assert_eq!(rr[1].0, "m2");
        assert_eq!(rr[2].0, "m1");
        assert_eq!(rr[3].0, "m2");
    }
}
