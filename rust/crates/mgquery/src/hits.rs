//! HITS (Hyperlink-Induced Topic Search) algorithm.
//! Computes hub and authority scores for each vertex.

use std::collections::{HashMap, HashSet};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// HITS algorithm: computes (authority, hub) scores for each vertex.
/// `max_iter`: maximum iterations. `epsilon`: convergence threshold.
pub fn hits(storage: &Storage, max_iter: usize, epsilon: f64) -> (HashMap<Gid, f64>, HashMap<Gid, f64>) {
    let all: Vec<Gid> = storage.all_vertices().into_iter().map(|(g, _, _)| g).collect();
    if all.is_empty() {
        return (HashMap::new(), HashMap::new());
    }

    let mut auth: HashMap<Gid, f64> = all.iter().map(|g| (*g, 1.0)).collect();
    let mut hub: HashMap<Gid, f64> = all.iter().map(|g| (*g, 1.0)).collect();

    // Precompute neighbor lists
    let mut out_neighbors: HashMap<Gid, Vec<Gid>> = HashMap::new();
    let mut in_neighbors: HashMap<Gid, Vec<Gid>> = HashMap::new();
    for &gid in &all {
        let mut out = Vec::new();
        for (_, other, _) in storage.vertex_out_edges(gid, None) { out.push(other); }
        out_neighbors.insert(gid, out);

        let mut inn = Vec::new();
        for (_, other, _) in storage.vertex_in_edges(gid, None) { inn.push(other); }
        in_neighbors.insert(gid, inn);
    }

    for _ in 0..max_iter {
        let mut new_auth: HashMap<Gid, f64> = HashMap::new();
        let mut new_hub: HashMap<Gid, f64> = HashMap::new();

        // Authority update: auth(v) = sum of hub(u) for all u linking to v
        for &gid in &all {
            let sum: f64 = in_neighbors[&gid].iter()
                .map(|u| hub.get(u).copied().unwrap_or(0.0))
                .sum();
            new_auth.insert(gid, sum);
        }

        // Hub update: hub(v) = sum of auth(u) for all v linking to u
        for &gid in &all {
            let sum: f64 = out_neighbors[&gid].iter()
                .map(|u| auth.get(u).copied().unwrap_or(0.0))
                .sum();
            new_hub.insert(gid, sum);
        }

        // Normalize
        let auth_norm: f64 = new_auth.values().map(|v| v * v).sum::<f64>().sqrt();
        let hub_norm: f64 = new_hub.values().map(|v| v * v).sum::<f64>().sqrt();

        if auth_norm > 0.0 {
            for v in new_auth.values_mut() { *v /= auth_norm; }
        }
        if hub_norm > 0.0 {
            for v in new_hub.values_mut() { *v /= hub_norm; }
        }

        let delta: f64 = all.iter()
            .map(|g| (new_auth[g] - auth[g]).abs() + (new_hub[g] - hub[g]).abs())
            .sum();

        auth = new_auth;
        hub = new_hub;

        if delta < epsilon { break; }
    }

    (auth, hub)
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
            if created.insert(from) { storage.create_vertex(&tx, from).unwrap(); }
            if created.insert(to) { storage.create_vertex(&tx, to).unwrap(); }
            storage.create_edge(&tx, Gid::from(next_edge), from, to, EdgeTypeId::from(0u32)).unwrap();
            next_edge += 1;
        }
        storage.commit_transaction(&tx);
    }

    #[test]
    fn test_hits_simple() {
        let storage = Storage::new();
        // Simple directed graph: 1 -> 2, 3 -> 2
        build_graph(&storage, &[(1, 2), (3, 2)]);
        let (auth, hub) = hits(&storage, 100, 1e-6);
        assert_eq!(auth.len(), 3);
        assert_eq!(hub.len(), 3);
        // Node 2 has highest authority (two incoming links)
        assert!(auth[&Gid::from(2u64)] >= auth[&Gid::from(1u64)]);
        assert!(auth[&Gid::from(2u64)] >= auth[&Gid::from(3u64)]);
    }

    #[test]
    fn test_hits_cycle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let (auth, hub) = hits(&storage, 100, 1e-6);
        // Symmetric cycle should have roughly equal scores
        assert!((auth[&Gid::from(1u64)] - auth[&Gid::from(2u64)]).abs() < 1e-3);
        assert!((hub[&Gid::from(1u64)] - hub[&Gid::from(2u64)]).abs() < 1e-3);
    }

    #[test]
    fn test_hits_empty() {
        let storage = Storage::new();
        let (auth, hub) = hits(&storage, 100, 1e-6);
        assert!(auth.is_empty());
        assert!(hub.is_empty());
    }
}
