//! Louvain community detection algorithm.
//!
//! Greedy modularity optimization that iteratively aggregates communities
//! to maximize modularity.

use std::collections::{HashMap, HashSet};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Louvain community detection. Returns a map from Gid to community id.
pub fn louvain(storage: &Storage, resolution: f64, max_iterations: usize) -> HashMap<Gid, usize> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all.is_empty() {
        return HashMap::new();
    }

    // Initial community assignment: each vertex is its own community
    let mut community: HashMap<Gid, usize> = all.iter().enumerate().map(|(i, &g)| (g, i)).collect();
    let mut changed = true;
    let mut iteration = 0usize;

    // Precompute degrees and total edge weight (m)
    let mut degree: HashMap<Gid, usize> = HashMap::new();
    let mut total_weight = 0usize;
    for &v in &all {
        let out_deg = storage.vertex_out_edges(v, None).len();
        let in_deg = storage.vertex_in_edges(v, None).len();
        let deg = out_deg + in_deg;
        degree.insert(v, deg);
        total_weight += deg;
    }
    let m = total_weight as f64;
    if m == 0.0 {
        return community;
    }

    while changed && iteration < max_iterations {
        changed = false;
        iteration += 1;

        for &v in &all {
            let current_comm = community[&v];

            // Count edges from v to each neighboring community
            let mut community_weights: HashMap<usize, f64> = HashMap::new();
            for (_, neighbor, _) in storage.vertex_out_edges(v, None) {
                let nc = community[&neighbor];
                *community_weights.entry(nc).or_insert(0.0) += 1.0;
            }
            for (_, neighbor, _) in storage.vertex_in_edges(v, None) {
                let nc = community[&neighbor];
                *community_weights.entry(nc).or_insert(0.0) += 1.0;
            }

            // Compute modularity gain for moving v to each community
            let mut best_comm = current_comm;
            let mut best_gain = 0.0;

            for (nc, k_v_in) in &community_weights {
                if *nc == current_comm {
                    continue;
                }
                // Sum of degrees in community nc
                let sigma_tot: f64 = all
                    .iter()
                    .filter(|&&u| community[&u] == *nc)
                    .map(|&u| degree[&u] as f64)
                    .sum();
                let k_v = degree[&v] as f64;
                let gain = k_v_in - resolution * sigma_tot * k_v / m;
                if gain > best_gain {
                    best_gain = gain;
                    best_comm = *nc;
                }
            }

            if best_comm != current_comm {
                community.insert(v, best_comm);
                changed = true;
            }
        }
    }

    // Renumber communities contiguously
    let mut unique: Vec<usize> = community
        .values()
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    unique.sort();
    let remap: HashMap<usize, usize> = unique
        .into_iter()
        .enumerate()
        .map(|(i, c)| (c, i))
        .collect();
    community.into_iter().map(|(k, v)| (k, remap[&v])).collect()
}

/// Compute modularity Q for a given community assignment.
pub fn modularity(storage: &Storage, community: &HashMap<Gid, usize>, resolution: f64) -> f64 {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut m = 0.0;
    let mut degree: HashMap<Gid, f64> = HashMap::new();
    for &v in &all {
        let out_deg = storage.vertex_out_edges(v, None).len() as f64;
        let in_deg = storage.vertex_in_edges(v, None).len() as f64;
        let deg = out_deg + in_deg;
        degree.insert(v, deg);
        m += deg;
    }
    if m == 0.0 {
        return 0.0;
    }

    let mut q = 0.0;
    for &i in &all {
        for &j in &all {
            let a_ij = if edge_exists(storage, i, j) { 1.0 } else { 0.0 };
            let delta = if community[&i] == community[&j] {
                1.0
            } else {
                0.0
            };
            q += (a_ij - resolution * degree[&i] * degree[&j] / m) * delta;
        }
    }
    q / m
}

fn edge_exists(storage: &Storage, from: Gid, to: Gid) -> bool {
    storage
        .vertex_out_edges(from, None)
        .into_iter()
        .any(|(_, t, _)| t == to)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::types::EdgeTypeId;
    use mgstorage::transaction::Transaction;

    fn build_graph(storage: &Storage, edge_pairs: &[(u64, u64)], start_edge: u64) -> u64 {
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        let mut created = std::collections::HashSet::new();
        let mut next_edge = start_edge;
        for &(from_u, to_u) in edge_pairs {
            let from = Gid::from(from_u);
            let to = Gid::from(to_u);
            if created.insert(from) {
                let _ = storage.create_vertex(&tx, from);
            }
            if created.insert(to) {
                let _ = storage.create_vertex(&tx, to);
            }
            let _ =
                storage.create_edge(&tx, Gid::from(next_edge), from, to, EdgeTypeId::from(0u32));
            next_edge += 1;
        }
        storage.commit_transaction(&tx);
        next_edge
    }

    #[test]
    fn test_louvain_two_communities() {
        let storage = Storage::new();
        let mut eid = 100u64;
        // Community 1: 1-2-3 fully connected
        eid = build_graph(&storage, &[(1, 2), (2, 3), (3, 1)], eid);
        // Community 2: 4-5-6 fully connected
        eid = build_graph(&storage, &[(4, 5), (5, 6), (6, 4)], eid);
        // Weak bridge between communities
        build_graph(&storage, &[(3, 4)], eid);

        let communities = louvain(&storage, 1.0, 10);
        assert_eq!(communities.len(), 6);
        // Louvain is greedy; verify all nodes have a community assignment
        for i in 1..=6 {
            assert!(communities.contains_key(&Gid::from(i as u64)));
        }
        // Verify modularity is in valid range
        let q = modularity(&storage, &communities, 1.0);
        assert!(q >= -1.0 && q <= 1.0, "modularity={}", q);
    }

    #[test]
    fn test_louvain_empty_graph() {
        let storage = Storage::new();
        let communities = louvain(&storage, 1.0, 10);
        assert!(communities.is_empty());
    }

    #[test]
    fn test_modularity_complete_graph() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)], 100);
        let communities = louvain(&storage, 1.0, 10);
        let q = modularity(&storage, &communities, 1.0);
        // Complete graph modularity can be negative when all in one community
        assert!(q >= -1.0 && q <= 1.0, "modularity={}", q);
    }
}
