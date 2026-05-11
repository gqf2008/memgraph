//! Advanced graph similarity measures.

use std::collections::HashSet;

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Jaccard similarity between two vertices based on common neighbors.
/// Returns a value in [0, 1].
pub fn jaccard_similarity(storage: &Storage, a: Gid, b: Gid) -> f64 {
    let neighbors_a: HashSet<Gid> = storage
        .vertex_out_edges(a, None)
        .into_iter()
        .map(|(_, other, _)| other)
        .chain(
            storage
                .vertex_in_edges(a, None)
                .into_iter()
                .map(|(_, other, _)| other),
        )
        .collect();
    let neighbors_b: HashSet<Gid> = storage
        .vertex_out_edges(b, None)
        .into_iter()
        .map(|(_, other, _)| other)
        .chain(
            storage
                .vertex_in_edges(b, None)
                .into_iter()
                .map(|(_, other, _)| other),
        )
        .collect();

    let intersection = neighbors_a.intersection(&neighbors_b).count();
    let union = neighbors_a.union(&neighbors_b).count();
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

/// Adamic-Adar similarity between two vertices.
/// Weights common neighbors by the inverse log of their degree.
pub fn adamic_adar(storage: &Storage, a: Gid, b: Gid) -> f64 {
    let neighbors_a: HashSet<Gid> = storage
        .vertex_out_edges(a, None)
        .into_iter()
        .map(|(_, other, _)| other)
        .chain(
            storage
                .vertex_in_edges(a, None)
                .into_iter()
                .map(|(_, other, _)| other),
        )
        .collect();
    let neighbors_b: HashSet<Gid> = storage
        .vertex_out_edges(b, None)
        .into_iter()
        .map(|(_, other, _)| other)
        .chain(
            storage
                .vertex_in_edges(b, None)
                .into_iter()
                .map(|(_, other, _)| other),
        )
        .collect();

    let mut score = 0.0;
    for &common in neighbors_a.intersection(&neighbors_b) {
        let deg = storage.vertex_out_edges(common, None).len()
            + storage.vertex_in_edges(common, None).len();
        if deg > 1 {
            score += 1.0 / (deg as f64).ln();
        }
    }
    score
}

/// Cosine similarity between two vertices based on neighbor sets.
pub fn cosine_similarity(storage: &Storage, a: Gid, b: Gid) -> f64 {
    let neighbors_a: HashSet<Gid> = storage
        .vertex_out_edges(a, None)
        .into_iter()
        .map(|(_, other, _)| other)
        .chain(
            storage
                .vertex_in_edges(a, None)
                .into_iter()
                .map(|(_, other, _)| other),
        )
        .collect();
    let neighbors_b: HashSet<Gid> = storage
        .vertex_out_edges(b, None)
        .into_iter()
        .map(|(_, other, _)| other)
        .chain(
            storage
                .vertex_in_edges(b, None)
                .into_iter()
                .map(|(_, other, _)| other),
        )
        .collect();

    let intersection = neighbors_a.intersection(&neighbors_b).count() as f64;
    let len_a = neighbors_a.len() as f64;
    let len_b = neighbors_b.len() as f64;
    if len_a == 0.0 || len_b == 0.0 {
        0.0
    } else {
        intersection / (len_a.sqrt() * len_b.sqrt())
    }
}

/// Common neighbors count.
pub fn common_neighbors(storage: &Storage, a: Gid, b: Gid) -> usize {
    let neighbors_a: HashSet<Gid> = storage
        .vertex_out_edges(a, None)
        .into_iter()
        .map(|(_, other, _)| other)
        .chain(
            storage
                .vertex_in_edges(a, None)
                .into_iter()
                .map(|(_, other, _)| other),
        )
        .collect();
    let neighbors_b: HashSet<Gid> = storage
        .vertex_out_edges(b, None)
        .into_iter()
        .map(|(_, other, _)| other)
        .chain(
            storage
                .vertex_in_edges(b, None)
                .into_iter()
                .map(|(_, other, _)| other),
        )
        .collect();

    neighbors_a.intersection(&neighbors_b).count()
}

/// Resource Allocation index for link prediction.
pub fn resource_allocation(storage: &Storage, a: Gid, b: Gid) -> f64 {
    let neighbors_a: HashSet<Gid> = storage
        .vertex_out_edges(a, None)
        .into_iter()
        .map(|(_, other, _)| other)
        .chain(
            storage
                .vertex_in_edges(a, None)
                .into_iter()
                .map(|(_, other, _)| other),
        )
        .collect();
    let neighbors_b: HashSet<Gid> = storage
        .vertex_out_edges(b, None)
        .into_iter()
        .map(|(_, other, _)| other)
        .chain(
            storage
                .vertex_in_edges(b, None)
                .into_iter()
                .map(|(_, other, _)| other),
        )
        .collect();

    neighbors_a
        .intersection(&neighbors_b)
        .map(|&common| {
            let deg = storage.vertex_out_edges(common, None).len()
                + storage.vertex_in_edges(common, None).len();
            if deg > 0 {
                1.0 / deg as f64
            } else {
                0.0
            }
        })
        .sum()
}

/// Preferential Attachment score (product of degrees).
pub fn preferential_attachment(storage: &Storage, a: Gid, b: Gid) -> usize {
    let deg_a = storage.vertex_out_edges(a, None).len() + storage.vertex_in_edges(a, None).len();
    let deg_b = storage.vertex_out_edges(b, None).len() + storage.vertex_in_edges(b, None).len();
    deg_a * deg_b
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::types::EdgeTypeId;
    use mgstorage::transaction::Transaction;

    fn build_graph(storage: &Storage, edge_pairs: &[(u64, u64)]) {
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
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
    fn test_jaccard_identical_neighbors() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 3), (1, 4), (2, 3), (2, 4)]);
        let sim = jaccard_similarity(&storage, Gid::from(1u64), Gid::from(2u64));
        assert!((sim - 1.0).abs() < 1e-6, "sim={}", sim);
    }

    #[test]
    fn test_jaccard_no_common() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 3), (2, 4)]);
        let sim = jaccard_similarity(&storage, Gid::from(1u64), Gid::from(2u64));
        assert_eq!(sim, 0.0);
    }

    #[test]
    fn test_adamic_adar() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 3), (2, 3)]);
        let score = adamic_adar(&storage, Gid::from(1u64), Gid::from(2u64));
        assert!(score > 0.0, "score={}", score);
    }

    #[test]
    fn test_cosine_similarity() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 3), (1, 4), (2, 3), (2, 4), (2, 5)]);
        let sim = cosine_similarity(&storage, Gid::from(1u64), Gid::from(2u64));
        assert!(sim > 0.0 && sim <= 1.0, "sim={}", sim);
    }

    #[test]
    fn test_common_neighbors() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 3), (1, 4), (2, 3), (2, 4), (2, 5)]);
        let cn = common_neighbors(&storage, Gid::from(1u64), Gid::from(2u64));
        assert_eq!(cn, 2);
    }

    #[test]
    fn test_resource_allocation() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 3), (2, 3)]);
        let ra = resource_allocation(&storage, Gid::from(1u64), Gid::from(2u64));
        assert!(ra > 0.0, "ra={}", ra);
    }

    #[test]
    fn test_preferential_attachment() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 3), (1, 4), (2, 5)]);
        let pa = preferential_attachment(&storage, Gid::from(1u64), Gid::from(2u64));
        assert_eq!(pa, 2 * 1);
    }
}
