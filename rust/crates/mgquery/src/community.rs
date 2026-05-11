// Community detection algorithms.

use std::collections::{HashMap, HashSet};

use mgcore::delta::IsolationLevel;
use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Label propagation community detection.
/// Returns a map of vertex GID -> community label.
pub fn label_propagation(storage: &Storage, max_iter: usize) -> HashMap<Gid, u64> {
    let all_v = storage.all_vertices();
    let gids: Vec<Gid> = all_v.iter().map(|(g, _, _)| *g).collect();
    if gids.is_empty() { return HashMap::new(); }

    // Initialize: each vertex is its own community
    let mut labels: HashMap<Gid, u64> = gids.iter().enumerate()
        .map(|(i, g)| (*g, i as u64))
        .collect();

    for _ in 0..max_iter {
        let mut changed = false;
        for gid in &gids {
            // Collect neighbor labels
            let mut neighbor_labels: Vec<u64> = Vec::new();
            for (_, other, _) in storage.vertex_out_edges(*gid, None) {
                if let Some(&label) = labels.get(&other) { neighbor_labels.push(label); }
            }
            for (_, other, _) in storage.vertex_in_edges(*gid, None) {
                if let Some(&label) = labels.get(&other) { neighbor_labels.push(label); }
            }
            if neighbor_labels.is_empty() { continue; }
            // Most common neighbor label
            let mut counts: HashMap<u64, usize> = HashMap::new();
            for &l in &neighbor_labels {
                *counts.entry(l).or_insert(0) += 1;
            }
            let most_common = counts.into_iter().max_by_key(|(_, c)| *c).map(|(l, _)| l).unwrap();
            if labels.get(gid) != Some(&most_common) {
                labels.insert(*gid, most_common);
                changed = true;
            }
        }
        if !changed { break; }
    }
    labels
}

/// Weakly connected components via union-find.
pub fn connected_components(storage: &Storage) -> HashMap<Gid, Gid> {
    let all_v = storage.all_vertices();
    let gids: Vec<Gid> = all_v.iter().map(|(g, _, _)| *g).collect();
    if gids.is_empty() { return HashMap::new(); }

    // Initialize: each vertex is its own parent
    let mut parent: HashMap<Gid, Gid> = gids.iter().map(|g| (*g, *g)).collect();

    fn find(parent: &mut HashMap<Gid, Gid>, x: Gid) -> Gid {
        let p = parent[&x];
        if p != x {
            let root = find(parent, p);
            parent.insert(x, root);
            root
        } else {
            x
        }
    }

    fn union(parent: &mut HashMap<Gid, Gid>, a: Gid, b: Gid) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra != rb {
            parent.insert(ra, rb);
        }
    }

    for (gid, _, _) in &all_v {
        for (_, other, _) in storage.vertex_out_edges(*gid, None) {
            union(&mut parent, *gid, other);
        }
        for (_, other, _) in storage.vertex_in_edges(*gid, None) {
            union(&mut parent, *gid, other);
        }
    }

    // Resolve components
    let mut components = HashMap::new();
    for gid in &gids {
        components.insert(*gid, find(&mut parent, *gid));
    }
    components
}

/// Louvain modularity optimization (single-pass simplified).
/// Returns a map of vertex GID -> community id.
pub fn louvain(storage: &Storage) -> HashMap<Gid, u64> {
    let all_v = storage.all_vertices();
    let gids: Vec<Gid> = all_v.iter().map(|(g, _, _)| *g).collect();
    if gids.is_empty() { return HashMap::new(); }

    let mut communities: HashMap<Gid, u64> = gids.iter().enumerate().map(|(i, g)| (*g, i as u64)).collect();
    let mut changed = true;
    let mut iteration = 0;
    let max_iter = 100;

    // Precompute degrees and edge list
    let mut degrees: HashMap<Gid, usize> = HashMap::new();
    let mut edge_count: HashMap<(Gid, Gid), usize> = HashMap::new();
    let mut total_edges = 0usize;

    for gid in &gids {
        let mut d = 0usize;
        for (_, other, _) in storage.vertex_out_edges(*gid, None) {
            d += 1;
            let a = *gid;
            let b = other;
            let key = if a <= b { (a, b) } else { (b, a) };
            *edge_count.entry(key).or_insert(0) += 1;
            total_edges += 1;
        }
        for (_, other, _) in storage.vertex_in_edges(*gid, None) {
            d += 1;
            let a = *gid;
            let b = other;
            let key = if a <= b { (a, b) } else { (b, a) };
            *edge_count.entry(key).or_insert(0) += 1;
            total_edges += 1;
        }
        degrees.insert(*gid, d);
    }

    let m = total_edges as f64;
    if m == 0.0 { return communities; }

    while changed && iteration < max_iter {
        changed = false;
        iteration += 1;
        for gid in &gids {
            let current_comm = communities[gid];
            let mut neighbor_comms: HashMap<u64, f64> = HashMap::new();

            // Count edges to each neighbor community
            for (_, other, _) in storage.vertex_out_edges(*gid, None) {
                let comm = communities[&other];
                *neighbor_comms.entry(comm).or_insert(0.0) += 1.0;
            }
            for (_, other, _) in storage.vertex_in_edges(*gid, None) {
                let comm = communities[&other];
                *neighbor_comms.entry(comm).or_insert(0.0) += 1.0;
            }

            let mut best_comm = current_comm;
            let mut best_gain = 0.0;
            let k_i = degrees[gid] as f64;
            let sum_in = neighbor_comms.get(&current_comm).copied().unwrap_or(0.0);

            for (comm, k_i_in) in neighbor_comms {
                // Compute modularity gain for moving to comm
                let sum_tot = gids.iter()
                    .filter(|g| communities[*g] == comm && **g != *gid)
                    .map(|g| degrees[g] as f64)
                    .sum::<f64>();
                let gain = if comm == current_comm {
                    0.0
                } else {
                    (k_i_in - sum_in) / m - (k_i * (sum_tot + k_i)) / (m * m) + (k_i * k_i) / (m * m)
                };
                if gain > best_gain {
                    best_gain = gain;
                    best_comm = comm;
                }
            }

            if best_comm != current_comm {
                communities.insert(*gid, best_comm);
                changed = true;
            }
        }
    }

    communities
}

/// Eigenvector centrality (power iteration).
pub fn eigenvector_centrality(storage: &Storage, max_iter: usize, epsilon: f64) -> HashMap<Gid, f64> {
    let all: Vec<Gid> = storage.all_vertices().into_iter().map(|(g, _, _)| g).collect();
    let n = all.len() as f64;
    if n == 0.0 { return HashMap::new(); }

    let mut centrality: HashMap<Gid, f64> = all.iter().map(|g| (*g, 1.0 / n)).collect();

    for _ in 0..max_iter {
        let mut new_centrality: HashMap<Gid, f64> = HashMap::new();
        for gid in &all {
            let mut sum = 0.0;
            for (_, other, _) in storage.vertex_out_edges(*gid, None) {
                let deg = storage.vertex_out_degree(other).max(1) + storage.vertex_in_degree(other).max(1);
                sum += centrality[&other] / deg as f64;
            }
            for (_, other, _) in storage.vertex_in_edges(*gid, None) {
                let deg = storage.vertex_out_degree(other).max(1) + storage.vertex_in_degree(other).max(1);
                sum += centrality[&other] / deg as f64;
            }
            new_centrality.insert(*gid, sum);
        }

        // Normalize
        let norm: f64 = new_centrality.values().map(|v| v * v).sum::<f64>().sqrt();
        if norm > 0.0 {
            for v in new_centrality.values_mut() {
                *v /= norm;
            }
        }

        let delta: f64 = all.iter().map(|g| (new_centrality[g] - centrality[g]).abs()).sum();
        centrality = new_centrality;
        if delta < epsilon { break; }
    }

    centrality
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgstorage::storage::Storage;

    #[test]
    fn test_label_propagation_basic() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let a = storage.create_vertex(&tx, mgcore::types::Gid::from(1u64)).unwrap();
        let b = storage.create_vertex(&tx, mgcore::types::Gid::from(2u64)).unwrap();
        let c = storage.create_vertex(&tx, mgcore::types::Gid::from(3u64)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(10u64), a, b, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(11u64), b, c, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.commit_transaction(&tx);

        let communities = label_propagation(&storage, 10);
        assert_eq!(communities.len(), 3);
    }

    #[test]
    fn test_connected_components_basic() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let a = storage.create_vertex(&tx, mgcore::types::Gid::from(1u64)).unwrap();
        let b = storage.create_vertex(&tx, mgcore::types::Gid::from(2u64)).unwrap();
        let c = storage.create_vertex(&tx, mgcore::types::Gid::from(3u64)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(10u64), a, b, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.commit_transaction(&tx);

        let components = connected_components(&storage);
        assert_eq!(components[&a], components[&b]);
        // c is isolated
        assert_eq!(components[&c], c);
    }

    #[test]
    fn test_louvain_basic() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let a = storage.create_vertex(&tx, mgcore::types::Gid::from(1u64)).unwrap();
        let b = storage.create_vertex(&tx, mgcore::types::Gid::from(2u64)).unwrap();
        let c = storage.create_vertex(&tx, mgcore::types::Gid::from(3u64)).unwrap();
        let d = storage.create_vertex(&tx, mgcore::types::Gid::from(4u64)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(10u64), a, b, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(11u64), b, c, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(12u64), c, d, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.commit_transaction(&tx);

        let communities = louvain(&storage);
        assert_eq!(communities.len(), 4);
    }

    #[test]
    fn test_eigenvector_centrality_basic() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let a = storage.create_vertex(&tx, mgcore::types::Gid::from(1u64)).unwrap();
        let b = storage.create_vertex(&tx, mgcore::types::Gid::from(2u64)).unwrap();
        let c = storage.create_vertex(&tx, mgcore::types::Gid::from(3u64)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(10u64), a, b, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(11u64), b, c, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.commit_transaction(&tx);

        let ec = eigenvector_centrality(&storage, 100, 1e-6);
        assert_eq!(ec.len(), 3);
        // b should have highest centrality (degree 2)
        assert!(ec[&b] >= ec[&a]);
        assert!(ec[&b] >= ec[&c]);
    }

    #[test]
    fn test_diameter_line() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let a = storage.create_vertex(&tx, mgcore::types::Gid::from(1u64)).unwrap();
        let b = storage.create_vertex(&tx, mgcore::types::Gid::from(2u64)).unwrap();
        let c = storage.create_vertex(&tx, mgcore::types::Gid::from(3u64)).unwrap();
        let d = storage.create_vertex(&tx, mgcore::types::Gid::from(4u64)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(10u64), a, b, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(11u64), b, c, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.create_edge(&tx, mgcore::types::Gid::from(12u64), c, d, mgcore::types::EdgeTypeId::from(1u32)).unwrap();
        storage.commit_transaction(&tx);

        assert_eq!(diameter(&storage), 3);
    }

    #[test]
    fn test_diameter_empty() {
        let storage = Storage::new();
        assert_eq!(diameter(&storage), 0);
    }
}

/// Graph diameter (longest shortest path). Returns 0 for empty/single-vertex graphs.
pub fn diameter(storage: &Storage) -> usize {
    let all: Vec<Gid> = storage.all_vertices().into_iter().map(|(g, _, _)| g).collect();
    if all.len() <= 1 { return 0; }

    let mut max_dist = 0usize;
    for s in &all {
        let mut visited: HashMap<Gid, usize> = HashMap::new();
        let mut queue = std::collections::VecDeque::new();
        visited.insert(*s, 0);
        queue.push_back(*s);

        while let Some(v) = queue.pop_front() {
            let d = visited[&v];
            for (_, other, _) in storage.vertex_out_edges(v, None) {
                if !visited.contains_key(&other) {
                    visited.insert(other, d + 1);
                    queue.push_back(other);
                }
            }
            for (_, other, _) in storage.vertex_in_edges(v, None) {
                if !visited.contains_key(&other) {
                    visited.insert(other, d + 1);
                    queue.push_back(other);
                }
            }
        }

        for gid in &all {
            if let Some(&d) = visited.get(gid) {
                max_dist = max_dist.max(d);
            }
        }
    }
    max_dist
}
