//! Consumer group coordinator stub and offset management.

use std::collections::HashMap;

/// State of a consumer group member.
#[derive(Clone, Debug, PartialEq)]
pub enum MemberState {
    Stable,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
}

/// A consumer group as tracked by the coordinator.
#[derive(Clone, Debug, PartialEq)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub generation_id: i32,
    pub protocol: Option<String>,
    pub leader: Option<String>,
    pub members: Vec<String>,
    pub state: MemberState,
}

/// In-memory group coordinator stub.
pub struct GroupCoordinator {
    groups: HashMap<String, ConsumerGroup>,
}

impl GroupCoordinator {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    /// Register or update a group.
    pub fn register_group(&mut self, group: ConsumerGroup) {
        self.groups.insert(group.group_id.clone(), group);
    }

    /// Get a group by ID.
    pub fn get_group(&self, group_id: &str) -> Option<&ConsumerGroup> {
        self.groups.get(group_id)
    }

    /// Remove a group.
    pub fn remove_group(&mut self, group_id: &str) {
        self.groups.remove(group_id);
    }

    /// Add a member to a group (creating the group if necessary).
    pub fn join_group(&mut self, group_id: &str, member_id: &str) -> i32 {
        let group = self.groups.entry(group_id.to_string()).or_insert_with(|| ConsumerGroup {
            group_id: group_id.to_string(),
            generation_id: 0,
            protocol: None,
            leader: None,
            members: Vec::new(),
            state: MemberState::PreparingRebalance,
        });
        if !group.members.contains(&member_id.to_string()) {
            group.members.push(member_id.to_string());
        }
        if group.leader.is_none() {
            group.leader = Some(member_id.to_string());
        }
        group.generation_id += 1;
        group.state = MemberState::Stable;
        group.generation_id
    }

    /// Remove a member from a group.
    pub fn leave_group(&mut self, group_id: &str, member_id: &str) {
        if let Some(group) = self.groups.get_mut(group_id) {
            group.members.retain(|m| m != member_id);
            if group.leader.as_deref() == Some(member_id) {
                group.leader = group.members.first().cloned();
            }
            if group.members.is_empty() {
                group.state = MemberState::Dead;
            }
        }
    }

    /// List all known group IDs.
    pub fn list_groups(&self) -> Vec<&str> {
        self.groups.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for GroupCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// A committed offset for a specific topic-partition.
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedOffset {
    pub offset: i64,
    pub metadata: Option<String>,
    pub leader_epoch: i32,
}

/// In-memory offset store.
pub struct OffsetStore {
    offsets: HashMap<(String, String, i32), CommittedOffset>,
}

impl OffsetStore {
    pub fn new() -> Self {
        Self {
            offsets: HashMap::new(),
        }
    }

    /// Commit an offset for a group/topic/partition.
    pub fn commit(
        &mut self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        metadata: Option<String>,
        leader_epoch: i32,
    ) {
        let key = (group_id.to_string(), topic.to_string(), partition);
        self.offsets.insert(
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
        &mut self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> Option<&CommittedOffset> {
        self.offsets
            .get(&(group_id.to_string(), topic.to_string(), partition))
    }

    /// Delete offsets for a group.
    pub fn delete_group(&mut self, group_id: &str) {
        self.offsets
            .retain(|key, _| key.0 != group_id);
    }

    /// List all committed offsets for a group.
    pub fn list_group_offsets(&self, group_id: &str) -> Vec<((String, i32), CommittedOffset)> {
        self.offsets
            .iter()
            .filter(|((g, _, _), _)| g == group_id)
            .map(|((_, topic, partition), offset)| {
                ((topic.clone(), *partition), offset.clone())
            })
            .collect()
    }
}

impl Default for OffsetStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_coordinator_join_leave() {
        let mut coord = GroupCoordinator::new();
        let gen = coord.join_group("g1", "m1");
        assert_eq!(gen, 1);
        let group = coord.get_group("g1").unwrap();
        assert_eq!(group.members, vec!["m1"]);
        assert_eq!(group.leader, Some("m1".to_string()));

        let gen2 = coord.join_group("g1", "m2");
        assert_eq!(gen2, 2);
        let group = coord.get_group("g1").unwrap();
        assert_eq!(group.members, vec!["m1", "m2"]);

        coord.leave_group("g1", "m1");
        let group = coord.get_group("g1").unwrap();
        assert_eq!(group.members, vec!["m2"]);
        assert_eq!(group.leader, Some("m2".to_string()));
    }

    #[test]
    fn test_group_coordinator_empty_group_dead() {
        let mut coord = GroupCoordinator::new();
        coord.join_group("g1", "m1");
        coord.leave_group("g1", "m1");
        let group = coord.get_group("g1").unwrap();
        assert_eq!(group.state, MemberState::Dead);
    }

    #[test]
    fn test_offset_store_commit_fetch() {
        let mut store = OffsetStore::new();
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
    fn test_offset_store_delete_group() {
        let mut store = OffsetStore::new();
        store.commit("g1", "t1", 0, 100, None, 0);
        store.commit("g2", "t1", 0, 200, None, 0);
        store.delete_group("g1");
        assert!(store.fetch("g1", "t1", 0).is_none());
        assert!(store.fetch("g2", "t1", 0).is_some());
    }

    #[test]
    fn test_offset_store_list() {
        let mut store = OffsetStore::new();
        store.commit("g1", "t1", 0, 10, None, 0);
        store.commit("g1", "t1", 1, 20, None, 0);
        store.commit("g1", "t2", 0, 30, None, 0);
        store.commit("g2", "t1", 0, 99, None, 0);

        let g1_offsets = store.list_group_offsets("g1");
        assert_eq!(g1_offsets.len(), 3);
    }
}
