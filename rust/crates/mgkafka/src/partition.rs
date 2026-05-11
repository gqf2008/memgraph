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
}

/// Attempt to elect a new leader for a partition.
/// In a real implementation this would consult ZooKeeper/KRaft.
/// This stub picks the first replica in the ISR that is not the current leader.
pub fn elect_leader(
    current_leader: i32,
    isr: &[i32],
    _live_brokers: &[i32],
) -> LeaderElectionResult {
    if isr.is_empty() {
        return LeaderElectionResult::NoValidReplica;
    }
    if isr.contains(&current_leader) {
        return LeaderElectionResult::AlreadyLeader(current_leader);
    }
    // Pick first available ISR member
    LeaderElectionResult::Elected(isr[0])
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
        let result = elect_leader(1, &[2, 3], &[2, 3]);
        assert_eq!(result, LeaderElectionResult::Elected(2));
    }

    #[test]
    fn test_leader_election_no_isr() {
        let result = elect_leader(1, &[], &[]);
        assert_eq!(result, LeaderElectionResult::NoValidReplica);
    }

    #[test]
    fn test_replica_awareness() {
        assert!(is_replica_for(2, &[1, 2, 3]));
        assert!(!is_replica_for(5, &[1, 2, 3]));
        assert_eq!(preferred_replica(&[10, 20, 30]), Some(10));
        assert_eq!(preferred_replica(&[]), None);
    }
}
