//! Katz centrality: a variant of eigenvector centrality that accounts for all path lengths.

use std::collections::HashMap;

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Katz centrality with attenuation factor `alpha`.
/// `alpha` must be smaller than the inverse of the largest eigenvalue of the adjacency matrix.
/// Returns a map from Gid to centrality score.
pub fn katz_centrality(
    storage: &Storage,
    alpha: f64,
    max_iter: usize,
    epsilon: f64,
) -> HashMap<Gid, f64> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let n = all.len() as f64;
    if n == 0.0 {
        return HashMap::new();
    }

    let mut centrality: HashMap<Gid, f64> = all.iter().map(|g| (*g, 1.0)).collect();

    for _ in 0..max_iter {
        let mut new_centrality: HashMap<Gid, f64> = HashMap::new();

        for &gid in &all {
            let mut sum = 0.0;
            // Sum centrality of all neighbors
            for (_, other, _) in storage.vertex_out_edges(gid, None) {
                sum += centrality.get(&other).copied().unwrap_or(0.0);
            }
            for (_, other, _) in storage.vertex_in_edges(gid, None) {
                sum += centrality.get(&other).copied().unwrap_or(0.0);
            }
            new_centrality.insert(gid, alpha * sum + 1.0);
        }

        let delta: f64 = all
            .iter()
            .map(|g| (new_centrality[g] - centrality[g]).abs())
            .sum();

        centrality = new_centrality;
        if delta < epsilon {
            break;
        }
    }

    centrality
}

/// Count all paths of length exactly `k` from each vertex.
/// Returns a map from Gid -> number of paths of length k.
pub fn count_paths_of_length(storage: &Storage, k: usize) -> HashMap<Gid, usize> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all.is_empty() {
        return HashMap::new();
    }

    // dp[i][v] = number of paths of length i starting from v
    let mut dp: HashMap<Gid, usize> = all.iter().map(|g| (*g, 1)).collect(); // length 0

    for _ in 0..k {
        let mut new_dp: HashMap<Gid, usize> = HashMap::new();
        for &gid in &all {
            let mut count = 0usize;
            for (_, other, _) in storage.vertex_out_edges(gid, None) {
                count += dp.get(&other).copied().unwrap_or(0);
            }
            for (_, other, _) in storage.vertex_in_edges(gid, None) {
                count += dp.get(&other).copied().unwrap_or(0);
            }
            new_dp.insert(gid, count);
        }
        dp = new_dp;
    }

    dp
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
    fn test_katz_line() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let katz = katz_centrality(&storage, 0.1, 100, 1e-6);
        assert_eq!(katz.len(), 4);
        // Middle nodes should have higher centrality
        assert!(katz[&Gid::from(2u64)] > katz[&Gid::from(1u64)]);
        assert!(katz[&Gid::from(3u64)] > katz[&Gid::from(4u64)]);
    }

    #[test]
    fn test_katz_star() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (1, 3), (1, 4), (1, 5)]);
        let katz = katz_centrality(&storage, 0.1, 100, 1e-6);
        // Center node should have highest centrality
        assert!(katz[&Gid::from(1u64)] > katz[&Gid::from(2u64)]);
    }

    #[test]
    fn test_count_paths_of_length() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        // Length 1 paths from each node
        let p1 = count_paths_of_length(&storage, 1);
        assert_eq!(p1[&Gid::from(1u64)], 2); // 1->2, 1<-3
        assert_eq!(p1[&Gid::from(2u64)], 2);
        assert_eq!(p1[&Gid::from(3u64)], 2);
    }

    #[test]
    fn test_katz_empty() {
        let storage = Storage::new();
        let katz = katz_centrality(&storage, 0.1, 100, 1e-6);
        assert!(katz.is_empty());
    }
}
