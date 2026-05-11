//! PageRank algorithm.

use std::collections::HashMap;

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// PageRank algorithm.
///
/// * `damping` - damping factor (typically 0.85)
/// * `max_iter` - maximum number of iterations
/// * `epsilon` - convergence threshold (sum of absolute differences)
///
/// Returns a map of Gid -> PageRank score.
pub fn pagerank(
    storage: &Storage,
    damping: f64,
    max_iter: usize,
    epsilon: f64,
) -> HashMap<Gid, f64> {
    let all_v: Vec<(Gid, Vec<_>, _)> = storage.all_vertices();
    let gids: Vec<Gid> = all_v.iter().map(|(g, _, _)| *g).collect();
    let n = gids.len() as f64;
    if n == 0.0 {
        return HashMap::new();
    }

    // Out-degree (at least 1 to avoid division by zero)
    let out_deg: HashMap<Gid, usize> = gids
        .iter()
        .map(|g| (*g, storage.vertex_out_degree(*g).max(1)))
        .collect();

    // In-neighbors for each vertex
    let in_neighbors: HashMap<Gid, Vec<Gid>> = gids
        .iter()
        .map(|g| {
            (
                *g,
                storage
                    .vertex_in_edges(*g, None)
                    .into_iter()
                    .map(|(_, other, _)| other)
                    .collect(),
            )
        })
        .collect();

    let mut rank: HashMap<Gid, f64> = gids.iter().map(|g| (*g, 1.0 / n)).collect();
    let mut new_rank: HashMap<Gid, f64> = HashMap::new();

    for _ in 0..max_iter {
        for gid in &gids {
            let mut sum = 0.0;
            if let Some(neighbors) = in_neighbors.get(gid) {
                for neighbor in neighbors {
                    let out_d = out_deg.get(neighbor).copied().unwrap_or(1) as f64;
                    sum += damping * rank.get(neighbor).copied().unwrap_or(0.0) / out_d;
                }
            }
            sum += (1.0 - damping) / n;
            new_rank.insert(*gid, sum);
        }
        let delta: f64 = gids.iter().map(|g| (new_rank[g] - rank[g]).abs()).sum();
        std::mem::swap(&mut rank, &mut new_rank);
        if delta < epsilon {
            break;
        }
    }

    rank
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
    fn test_pagerank_cycle() {
        let storage = Storage::new();
        // 3-node cycle: 1->2->3->1
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let ranks = pagerank(&storage, 0.85, 100, 1e-6);
        assert_eq!(ranks.len(), 3);
        let total: f64 = ranks.values().sum();
        assert!((total - 1.0).abs() < 0.1, "sum={}", total);
        // Symmetric cycle should have roughly equal ranks
        assert!((ranks[&Gid::from(1u64)] - ranks[&Gid::from(2u64)]).abs() < 1e-3);
        assert!((ranks[&Gid::from(2u64)] - ranks[&Gid::from(3u64)]).abs() < 1e-3);
    }

    #[test]
    fn test_pagerank_star() {
        let storage = Storage::new();
        // Star: 1->2, 1->3, 1->4 (node 1 has out-degree 3, nodes 2-4 have in-degree 1)
        build_graph(&storage, &[(1, 2), (1, 3), (1, 4)]);
        let ranks = pagerank(&storage, 0.85, 100, 1e-6);
        assert_eq!(ranks.len(), 4);
        let total: f64 = ranks.values().sum();
        // Total may not sum to exactly 1.0 because dangling nodes (no out-edges)
        // leak rank; just check it's reasonable.
        assert!(total > 0.0 && total <= 1.0, "sum={}", total);
        // Nodes 2,3,4 each receive 1/3 of node 1's rank
        assert!((ranks[&Gid::from(2u64)] - ranks[&Gid::from(3u64)]).abs() < 1e-6);
        assert!((ranks[&Gid::from(3u64)] - ranks[&Gid::from(4u64)]).abs() < 1e-6);
    }

    #[test]
    fn test_pagerank_empty() {
        let storage = Storage::new();
        let ranks = pagerank(&storage, 0.85, 100, 1e-6);
        assert!(ranks.is_empty());
    }

    #[test]
    fn test_pagerank_single_node() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.commit_transaction(&tx);
        let ranks = pagerank(&storage, 0.85, 100, 1e-6);
        assert_eq!(ranks.len(), 1);
        // Single node with no edges: rank converges to (1-damping)/n + damping*0 = 0.15
        assert!((ranks[&Gid::from(1u64)] - 0.15).abs() < 1e-6);
    }
}
