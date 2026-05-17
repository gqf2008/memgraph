//! Partition assignment strategies and replica management.

/// A partition with its topic and index.
#[derive(Clone, Debug, PartialEq)]
pub struct Partition {
    pub topic: String,
    pub partition: i32,
}

/// A consumer member in a group.
#[derive(Clone, Debug, PartialEq)]
pub struct Member {
    pub member_id: String,
}

/// Assign partitions to members using round-robin strategy.
/// Iterates over all (topic, partition) pairs and assigns them
/// cyclically to members.
pub fn assign_round_robin(
    members: &[Member],
    partitions: &[Partition],
) -> Vec<(String, Partition)> {
    let mut assignments = Vec::with_capacity(partitions.len());
    if members.is_empty() {
        return assignments;
    }
    for (i, part) in partitions.iter().enumerate() {
        let member_idx = i % members.len();
        assignments.push((members[member_idx].member_id.clone(), part.clone()));
    }
    assignments
}

/// Assign partitions using the range strategy.
/// Partitions are sorted by topic then partition index, and each
/// member gets a contiguous range.
pub fn assign_range(members: &[Member], partitions: &[Partition]) -> Vec<(String, Partition)> {
    let mut assignments = Vec::with_capacity(partitions.len());
    if members.is_empty() || partitions.is_empty() {
        return assignments;
    }

    // Group partitions by topic
    let mut by_topic: std::collections::BTreeMap<String, Vec<Partition>> =
        std::collections::BTreeMap::new();
    for p in partitions {
        by_topic.entry(p.topic.clone()).or_default().push(p.clone());
    }

    for (_topic, mut topic_parts) in by_topic {
        topic_parts.sort_by_key(|p| p.partition);
        let n = topic_parts.len();
        let m = members.len();
        let parts_per_member = n / m;
        let extra = n % m;
        let mut start = 0usize;
        for (member_idx, member) in members.iter().enumerate() {
            let count = parts_per_member + if member_idx < extra { 1 } else { 0 };
            let end = (start + count).min(n);
            for part in &topic_parts[start..end] {
                assignments.push((member.member_id.clone(), part.clone()));
            }
            start = end;
        }
    }
    assignments
}

/// Result of a leader election attempt.
#[derive(Clone, Debug, PartialEq)]
pub enum LeaderElectionResult {
    Elected(i32),       // new leader node_id
    NoValidReplica,     // no ISR member is available
    AlreadyLeader(i32), // current leader is still valid
    NotEnoughReplicas,  // can't form majority
}

/// Controller epoch for fencing stale leaders.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ControllerEpoch(u64);

impl ControllerEpoch {
    pub fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    pub fn increment(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }

    pub fn get(&self) -> u64 {
        self.0
    }

    pub fn is_newer_than(&self, other: ControllerEpoch) -> bool {
        self.0 > other.0
    }
}

/// Leader election state for a single partition.
#[derive(Clone, Debug)]
pub struct PartitionLeaderState {
    pub partition: Partition,
    pub current_leader: i32,
    pub leader_epoch: i32,
    pub controller_epoch: ControllerEpoch,
    pub isr: Vec<i32>,
    pub last_election_time: std::time::SystemTime,
}

/// Attempt to elect a new leader for a partition using a majority-based
/// quorum algorithm. The election prefers the preferred replica first,
/// then falls back to any ISR member that can form a majority.
///
/// Election rules:
/// 1. If ISR is empty, return NoValidReplica
/// 2. If current leader is in ISR and alive, return AlreadyLeader
/// 3. Prefer preferred_replica if it is alive and in ISR
/// 4. Otherwise, pick any ISR member that has majority support
pub fn elect_leader(
    current_leader: i32,
    isr: &[i32],
    live_brokers: &[i32],
) -> LeaderElectionResult {
    if isr.is_empty() {
        return LeaderElectionResult::NoValidReplica;
    }

    // Check if current leader is still alive and in ISR
    let leader_alive = live_brokers.contains(&current_leader);
    if isr.contains(&current_leader) && leader_alive {
        return LeaderElectionResult::AlreadyLeader(current_leader);
    }

    // Filter ISR to only live brokers
    let live_isr: Vec<i32> = isr
        .iter()
        .copied()
        .filter(|b| live_brokers.contains(b))
        .collect();

    if live_isr.is_empty() {
        return LeaderElectionResult::NoValidReplica;
    }

    // Check if we have a majority of the original ISR
    let majority = (isr.len() / 2) + 1;
    if live_isr.len() < majority {
        return LeaderElectionResult::NotEnoughReplicas;
    }

    // Prefer the preferred replica (first in ISR) if it's alive
    if let Some(preferred) = isr.first().copied() {
        if live_isr.contains(&preferred) && preferred != current_leader {
            return LeaderElectionResult::Elected(preferred);
        }
    }

    // Fall back to any live ISR member
    LeaderElectionResult::Elected(live_isr[0])
}

/// Perform a full leader election with controller epoch fencing.
/// The new leader receives an incremented leader epoch to prevent
/// stale leaders from accepting writes (fencing).
pub fn elect_leader_with_epoch(
    state: &mut PartitionLeaderState,
    live_brokers: &[i32],
) -> LeaderElectionResult {
    let result = elect_leader(state.current_leader, &state.isr, live_brokers);
    match result {
        LeaderElectionResult::Elected(new_leader) => {
            state.current_leader = new_leader;
            state.leader_epoch += 1;
            state.controller_epoch.increment();
            state.last_election_time = std::time::SystemTime::now();
            LeaderElectionResult::Elected(new_leader)
        }
        LeaderElectionResult::AlreadyLeader(_) => {
            result
        }
        _ => result,
    }
}

/// Replica awareness: determine if a broker hosts a replica for a partition.
pub fn is_replica_for(broker_id: i32, replicas: &[i32]) -> bool {
    replicas.contains(&broker_id)
}

/// Determine preferred replica (the first replica in the list).
pub fn preferred_replica(replicas: &[i32]) -> Option<i32> {
    replicas.first().copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_members(ids: &[&str]) -> Vec<Member> {
        ids.iter()
            .map(|id| Member {
                member_id: id.to_string(),
            })
            .collect()
    }

    fn make_partitions(topic: &str, count: i32) -> Vec<Partition> {
        (0..count)
            .map(|i| Partition {
                topic: topic.to_string(),
                partition: i,
            })
            .collect()
    }

    #[test]
    fn test_round_robin_basic() {
        let members = make_members(&["m1", "m2"]);
        let partitions = make_partitions("t", 4);
        let assignments = assign_round_robin(&members, &partitions);
        assert_eq!(assignments.len(), 4);
        assert_eq!(assignments[0].0, "m1");
        assert_eq!(assignments[1].0, "m2");
        assert_eq!(assignments[2].0, "m1");
        assert_eq!(assignments[3].0, "m2");
    }

    #[test]
    fn test_round_robin_empty_members() {
        let partitions = make_partitions("t", 2);
        let assignments = assign_round_robin(&[], &partitions);
        assert!(assignments.is_empty());
    }

    #[test]
    fn test_range_basic() {
        let members = make_members(&["m1", "m2"]);
        let partitions = make_partitions("t", 4);
        let assignments = assign_range(&members, &partitions);
        assert_eq!(assignments.len(), 4);
        // m1 gets first half: p0, p1
        assert_eq!(assignments[0].0, "m1");
        assert_eq!(assignments[0].1.partition, 0);
        assert_eq!(assignments[1].0, "m1");
        assert_eq!(assignments[1].1.partition, 1);
        // m2 gets second half: p2, p3
        assert_eq!(assignments[2].0, "m2");
        assert_eq!(assignments[2].1.partition, 2);
        assert_eq!(assignments[3].0, "m2");
        assert_eq!(assignments[3].1.partition, 3);
    }

    #[test]
    fn test_range_uneven() {
        let members = make_members(&["m1", "m2", "m3"]);
        let partitions = make_partitions("t", 7);
        let assignments = assign_range(&members, &partitions);
        assert_eq!(assignments.len(), 7);
        // m1 gets 3, m2 gets 2, m3 gets 2
        let m1_count = assignments.iter().filter(|a| a.0 == "m1").count();
        let m2_count = assignments.iter().filter(|a| a.0 == "m2").count();
        let m3_count = assignments.iter().filter(|a| a.0 == "m3").count();
        assert_eq!(m1_count, 3);
        assert_eq!(m2_count, 2);
        assert_eq!(m3_count, 2);
    }

    #[test]
    fn test_leader_election_already_leader() {
        let result = elect_leader(1, &[1, 2, 3], &[1, 2, 3]);
        assert_eq!(result, LeaderElectionResult::AlreadyLeader(1));
    }

    #[test]
    fn test_leader_election_new_leader() {
        // Leader 1 is down, ISR is [2, 3], both alive — preferred is 2
        let result = elect_leader(1, &[2, 3, 1], &[2, 3]);
        assert_eq!(result, LeaderElectionResult::Elected(2));
    }

    #[test]
    fn test_leader_election_leader_not_in_isr() {
        // Current leader 1 is not in ISR at all
        let result = elect_leader(1, &[2, 3], &[2, 3]);
        assert_eq!(result, LeaderElectionResult::Elected(2));
    }

    #[test]
    fn test_leader_election_no_isr() {
        let result = elect_leader(1, &[], &[]);
        assert_eq!(result, LeaderElectionResult::NoValidReplica);
    }

    #[test]
    fn test_leader_election_no_live_brokers() {
        let result = elect_leader(1, &[1, 2, 3], &[]);
        assert_eq!(result, LeaderElectionResult::NoValidReplica);
    }

    #[test]
    fn test_leader_election_no_majority() {
        // ISR = 3 requires 2 live for majority, only 1 is alive
        let result = elect_leader(1, &[1, 2, 3], &[2]);
        assert_eq!(result, LeaderElectionResult::NotEnoughReplicas);
    }

    #[test]
    fn test_leader_election_preferred_takes_priority() {
        // Leader 5 is dead, live ISR = [3, 7, 10], preferred is 3
        let result = elect_leader(5, &[3, 7, 10], &[3, 7, 10]);
        assert_eq!(result, LeaderElectionResult::Elected(3));
    }

    #[test]
    fn test_leader_election_with_epoch_increments() {
        let mut state = PartitionLeaderState {
            partition: Partition { topic: "t".into(), partition: 0 },
            current_leader: 1,
            leader_epoch: 5,
            controller_epoch: ControllerEpoch::new(3),
            isr: vec![2, 3, 1],
            last_election_time: std::time::SystemTime::now(),
        };
        let result = elect_leader_with_epoch(&mut state, &[2, 3]);
        assert_eq!(result, LeaderElectionResult::Elected(2));
        assert_eq!(state.current_leader, 2);
        assert_eq!(state.leader_epoch, 6);
        assert_eq!(state.controller_epoch.get(), 4);
    }

    #[test]
    fn test_controller_epoch_is_newer() {
        let older = ControllerEpoch::new(1);
        let newer = ControllerEpoch::new(5);
        assert!(newer.is_newer_than(older));
        assert!(!older.is_newer_than(newer));
        assert!(!older.is_newer_than(older));
    }

    #[test]
    fn test_replica_awareness() {
        assert!(is_replica_for(2, &[1, 2, 3]));
        assert!(!is_replica_for(5, &[1, 2, 3]));
        assert_eq!(preferred_replica(&[10, 20, 30]), Some(10));
        assert_eq!(preferred_replica(&[]), None);
    }
}
