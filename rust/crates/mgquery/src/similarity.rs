//! Node similarity algorithms: Jaccard, cosine, common neighbors.

use std::collections::{HashMap, HashSet};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Get the set of neighbors (both in and out) for a vertex.
fn neighbors(storage: &Storage, gid: Gid) -> HashSet<Gid> {
    let mut set = HashSet::new();
    for (_, other, _) in storage.vertex_out_edges(gid, None) {
        set.insert(other);
    }
    for (_, other, _) in storage.vertex_in_edges(gid, None) {
        set.insert(other);
    }
    set
}

/// Jaccard similarity coefficient between two vertices' neighbor sets.
/// Returns 0.0 if either vertex has no neighbors.
pub fn jaccard_similarity(storage: &Storage, a: Gid, b: Gid) -> f64 {
    let na = neighbors(storage, a);
    let nb = neighbors(storage, b);
    if na.is_empty() && nb.is_empty() {
        return 1.0;
    }
    let intersection: HashSet<Gid> = na.intersection(&nb).copied().collect();
    let union: HashSet<Gid> = na.union(&nb).copied().collect();
    if union.is_empty() {
        0.0
    } else {
        intersection.len() as f64 / union.len() as f64
    }
}

/// Cosine similarity of adjacency vectors for two vertices.
/// Treats the graph as undirected. Returns 0.0 if either has no neighbors.
pub fn cosine_similarity(storage: &Storage, a: Gid, b: Gid) -> f64 {
    let na = neighbors(storage, a);
    let nb = neighbors(storage, b);
    if na.is_empty() || nb.is_empty() {
        return 0.0;
    }
    let intersection: HashSet<Gid> = na.intersection(&nb).copied().collect();
    let dot = intersection.len() as f64;
    let norm_a = (na.len() as f64).sqrt();
    let norm_b = (nb.len() as f64).sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

/// Common neighbors of two vertices.
pub fn common_neighbors(storage: &Storage, a: Gid, b: Gid) -> Vec<Gid> {
    let na = neighbors(storage, a);
    let nb = neighbors(storage, b);
    na.intersection(&nb).copied().collect()
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
    fn test_jaccard_similarity_identical() {
        let storage = Storage::new();
        // Triangle: 1-2-3-1, nodes 1 and 2 share neighbors {3}
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let sim = jaccard_similarity(&storage, Gid::from(1u64), Gid::from(2u64));
        // N(1) = {2,3}, N(2) = {1,3}, intersection = {3}, union = {1,2,3} -> 1/3
        assert!((sim - 1.0 / 3.0).abs() < 1e-6);
    }

    #[test]
    fn test_jaccard_similarity_disjoint() {
        let storage = Storage::new();
        // Two disconnected edges
        build_graph(&storage, &[(1, 2), (3, 4)]);
        let sim = jaccard_similarity(&storage, Gid::from(1u64), Gid::from(3u64));
        assert_eq!(sim, 0.0);
    }

    #[test]
    fn test_jaccard_similarity_empty() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);
        let sim = jaccard_similarity(&storage, Gid::from(1u64), Gid::from(2u64));
        // Both have no neighbors -> return 1.0
        assert_eq!(sim, 1.0);
    }

    #[test]
    fn test_cosine_similarity() {
        let storage = Storage::new();
        // Triangle: N(1)={2,3}, N(2)={1,3}, intersection={3}, |N1|=2, |N2|=2
        // cos = 1 / sqrt(2*2) = 0.5
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let sim = cosine_similarity(&storage, Gid::from(1u64), Gid::from(2u64));
        assert!((sim - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (1, 3)]);
        let sim = cosine_similarity(&storage, Gid::from(1u64), Gid::from(1u64));
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_empty() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);
        let sim = cosine_similarity(&storage, Gid::from(1u64), Gid::from(2u64));
        assert_eq!(sim, 0.0);
    }

    #[test]
    fn test_common_neighbors() {
        let storage = Storage::new();
        // 1 connected to 2 and 3; 4 connected to 2 and 3
        build_graph(&storage, &[(1, 2), (1, 3), (4, 2), (4, 3)]);
        let common = common_neighbors(&storage, Gid::from(1u64), Gid::from(4u64));
        assert_eq!(common.len(), 2);
        assert!(common.contains(&Gid::from(2u64)));
        assert!(common.contains(&Gid::from(3u64)));
    }

    #[test]
    fn test_common_neighbors_none() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (3, 4)]);
        let common = common_neighbors(&storage, Gid::from(1u64), Gid::from(3u64));
        assert!(common.is_empty());
    }
}
