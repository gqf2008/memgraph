//! Articulation points (cut vertices) and related connectivity algorithms.

use std::collections::{HashMap, HashSet};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Find all articulation points using DFS-based low-link values.
pub fn articulation_points(storage: &Storage) -> Vec<Gid> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all.is_empty() {
        return Vec::new();
    }

    let mut visited: HashSet<Gid> = HashSet::new();
    let mut disc: HashMap<Gid, usize> = HashMap::new();
    let mut low: HashMap<Gid, usize> = HashMap::new();
    let mut parent: HashMap<Gid, Option<Gid>> = HashMap::new();
    let mut result: HashSet<Gid> = HashSet::new();
    let mut time = 0usize;

    for &v in &all {
        if !visited.contains(&v) {
            parent.insert(v, None);
            dfs(
                v,
                storage,
                &mut visited,
                &mut disc,
                &mut low,
                &mut parent,
                &mut result,
                &mut time,
            );
        }
    }

    result.into_iter().collect()
}

fn dfs(
    u: Gid,
    storage: &Storage,
    visited: &mut HashSet<Gid>,
    disc: &mut HashMap<Gid, usize>,
    low: &mut HashMap<Gid, usize>,
    parent: &mut HashMap<Gid, Option<Gid>>,
    result: &mut HashSet<Gid>,
    time: &mut usize,
) {
    visited.insert(u);
    *time += 1;
    disc.insert(u, *time);
    low.insert(u, *time);

    let mut children = 0usize;
    let mut neighbors = Vec::new();
    for (_, other, _) in storage.vertex_out_edges(u, None) {
        neighbors.push(other);
    }
    for (_, other, _) in storage.vertex_in_edges(u, None) {
        neighbors.push(other);
    }

    for &v in &neighbors {
        if !visited.contains(&v) {
            children += 1;
            parent.insert(v, Some(u));
            dfs(v, storage, visited, disc, low, parent, result, time);

            let low_v = low[&v];
            let low_u = low.get_mut(&u).unwrap();
            *low_u = (*low_u).min(low_v);

            // u is articulation point if:
            // (1) u is root and has 2+ children
            // (2) u is not root and low[v] >= disc[u]
            if parent.get(&u) != Some(&None) && low_v >= disc[&u] {
                result.insert(u);
            }
        } else if parent.get(&u) != Some(&Some(v)) {
            let disc_v = disc[&v];
            let low_u = low.get_mut(&u).unwrap();
            *low_u = (*low_u).min(disc_v);
        }
    }

    // Root is articulation point if it has 2+ children
    if parent.get(&u) == Some(&None) && children >= 2 {
        result.insert(u);
    }
}

/// Check if removing a single vertex disconnects the graph.
pub fn is_biconnected(storage: &Storage) -> bool {
    articulation_points(storage).is_empty() && storage.all_vertices().len() > 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::delta::IsolationLevel;
    use mgcore::types::{EdgeTypeId, Gid};
    use mgstorage::storage::Storage;

    fn build_graph(storage: &Storage, edge_pairs: &[(u64, u64)]) {
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let mut created = std::collections::HashSet::new();
        let mut next_edge = 100u64;
        for &(from_u, to_u) in edge_pairs {
            let from = Gid::from(from_u);
            let to = Gid::from(to_u);
            if created.insert(from) {
                storage.create_vertex(&tx, from).unwrap();
            }
            if created.insert(to) {
                storage.create_vertex(&tx, to).unwrap();
            }
            storage
                .create_edge(&tx, Gid::from(next_edge), from, to, EdgeTypeId::from(0u32))
                .unwrap();
            next_edge += 1;
        }
        storage.commit_transaction(&tx);
    }

    #[test]
    fn test_articulation_points_line() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let ap = articulation_points(&storage);
        // Nodes 2 and 3 are articulation points in a line
        assert_eq!(ap.len(), 2);
        assert!(ap.contains(&Gid::from(2u64)));
        assert!(ap.contains(&Gid::from(3u64)));
    }

    #[test]
    fn test_articulation_points_cycle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4), (4, 1)]);
        let ap = articulation_points(&storage);
        // Cycle has no articulation points
        assert!(ap.is_empty());
    }

    #[test]
    fn test_articulation_points_star() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (1, 3), (1, 4), (1, 5)]);
        let ap = articulation_points(&storage);
        // Center node is the only articulation point
        assert_eq!(ap.len(), 1);
        assert!(ap.contains(&Gid::from(1u64)));
    }

    #[test]
    fn test_articulation_points_two_cycles() {
        let storage = Storage::new();
        // Two cycles sharing a single node (3)
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (3, 4), (4, 5), (5, 3)]);
        let ap = articulation_points(&storage);
        // Node 3 is the only articulation point
        assert_eq!(ap.len(), 1);
        assert!(ap.contains(&Gid::from(3u64)));
    }

    #[test]
    fn test_is_biconnected() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        assert!(is_biconnected(&storage));

        let storage2 = Storage::new();
        build_graph(&storage2, &[(1, 2), (2, 3)]);
        assert!(!is_biconnected(&storage2));
    }
}
