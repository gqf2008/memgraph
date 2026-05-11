//! Link prediction algorithms for graph edges.

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

/// Adamic-Adar index: sum over common neighbors of 1 / log(degree).
/// Higher scores indicate a stronger likelihood of a future edge.
pub fn adamic_adar(storage: &Storage, a: Gid, b: Gid) -> f64 {
    let na = neighbors(storage, a);
    let nb = neighbors(storage, b);
    let common: HashSet<Gid> = na.intersection(&nb).copied().collect();
    common.iter().map(|&c| {
        let deg = neighbors(storage, c).len() as f64;
        if deg > 1.0 { 1.0 / deg.ln() } else { 0.0 }
    }).sum()
}

/// Preferential Attachment score: |N(a)| * |N(b)|.
pub fn preferential_attachment(storage: &Storage, a: Gid, b: Gid) -> f64 {
    let na = neighbors(storage, a).len() as f64;
    let nb = neighbors(storage, b).len() as f64;
    na * nb
}

/// Resource Allocation index: sum over common neighbors of 1 / degree.
pub fn resource_allocation(storage: &Storage, a: Gid, b: Gid) -> f64 {
    let na = neighbors(storage, a);
    let nb = neighbors(storage, b);
    let common: HashSet<Gid> = na.intersection(&nb).copied().collect();
    common.iter().map(|&c| {
        let deg = neighbors(storage, c).len() as f64;
        if deg > 0.0 { 1.0 / deg } else { 0.0 }
    }).sum()
}

/// Common Neighbors count.
pub fn common_neighbors_count(storage: &Storage, a: Gid, b: Gid) -> usize {
    let na = neighbors(storage, a);
    let nb = neighbors(storage, b);
    na.intersection(&nb).count()
}

/// Soundarajan-Hopcroft score (community-aware common neighbors).
/// Vertices in the same community (same component id) score higher.
pub fn soundarajan_hopcroft(
    storage: &Storage,
    a: Gid,
    b: Gid,
    community: &HashMap<Gid, usize>,
) -> usize {
    let na = neighbors(storage, a);
    let nb = neighbors(storage, b);
    let ca = community.get(&a);
    let cb = community.get(&b);
    if ca != cb {
        return 0;
    }
    na.intersection(&nb)
        .filter(|&&c| community.get(&c) == ca)
        .count()
}

/// Link prediction for all non-adjacent vertex pairs.
/// Returns pairs sorted by Adamic-Adar score descending.
pub fn predict_links(storage: &Storage, top_k: usize) -> Vec<(Gid, Gid, f64)> {
    let all: Vec<Gid> = storage.all_vertices().into_iter().map(|(g, _, _)| g).collect();
    let mut scores = Vec::new();
    for i in 0..all.len() {
        for j in (i + 1)..all.len() {
            let a = all[i];
            let b = all[j];
            // Skip already connected pairs
            if are_connected(storage, a, b) {
                continue;
            }
            let score = adamic_adar(storage, a, b);
            if score > 0.0 {
                scores.push((a, b, score));
            }
        }
    }
    scores.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
    scores.into_iter().take(top_k).collect()
}

fn are_connected(storage: &Storage, a: Gid, b: Gid) -> bool {
    for (_, other, _) in storage.vertex_out_edges(a, None) {
        if other == b { return true; }
    }
    for (_, other, _) in storage.vertex_in_edges(a, None) {
        if other == b { return true; }
    }
    false
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
    fn test_adamic_adar() {
        let storage = Storage::new();
        // Square with diagonal: 1-2-3-4-1, plus 1-3
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4), (4, 1), (1, 3)]);
        // Nodes 2 and 4 share neighbors {1, 3}
        let score = adamic_adar(&storage, Gid::from(2u64), Gid::from(4u64));
        assert!(score > 0.0);
    }

    #[test]
    fn test_preferential_attachment() {
        let storage = Storage::new();
        // Star: 1 connected to 2,3,4,5
        build_graph(&storage, &[(1, 2), (1, 3), (1, 4), (1, 5)]);
        let pa = preferential_attachment(&storage, Gid::from(2u64), Gid::from(3u64));
        // Both have degree 1 -> 1*1 = 1
        assert_eq!(pa, 1.0);
    }

    #[test]
    fn test_resource_allocation() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let ra = resource_allocation(&storage, Gid::from(1u64), Gid::from(2u64));
        // Already connected; but RA still computes via common neighbor 3
        assert!(ra > 0.0);
    }

    #[test]
    fn test_common_neighbors_count() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        assert_eq!(common_neighbors_count(&storage, Gid::from(1u64), Gid::from(2u64)), 1);
    }

    #[test]
    fn test_soundarajan_hopcroft_same_community() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (4, 5)]);
        let mut comm = HashMap::new();
        comm.insert(Gid::from(1u64), 0);
        comm.insert(Gid::from(2u64), 0);
        comm.insert(Gid::from(3u64), 0);
        comm.insert(Gid::from(4u64), 1);
        comm.insert(Gid::from(5u64), 1);
        // 1 and 2 are in same community, share neighbor 3 (also same community)
        let score = soundarajan_hopcroft(&storage, Gid::from(1u64), Gid::from(2u64), &comm);
        assert_eq!(score, 1);
    }

    #[test]
    fn test_soundarajan_hopcroft_different_community() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let mut comm = HashMap::new();
        comm.insert(Gid::from(1u64), 0);
        comm.insert(Gid::from(2u64), 1);
        comm.insert(Gid::from(3u64), 0);
        let score = soundarajan_hopcroft(&storage, Gid::from(1u64), Gid::from(2u64), &comm);
        assert_eq!(score, 0);
    }

    #[test]
    fn test_predict_links() {
        let storage = Storage::new();
        // Square: 1-2-3-4-1 (no diagonal)
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4), (4, 1)]);
        let predictions = predict_links(&storage, 10);
        // Pair (1,3) and (2,4) are not connected
        assert!(predictions.len() >= 1);
    }
}
