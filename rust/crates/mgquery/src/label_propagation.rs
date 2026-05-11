//! Label propagation community detection (Raghavan et al., 2007).
//!
//! Fast near-linear time algorithm that propagates the most frequent
//! neighbor label to each vertex until convergence.

use std::collections::{HashMap, HashSet};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Label propagation community detection.
/// Returns a map from Gid to community id.
pub fn label_propagation(storage: &Storage, max_iterations: usize) -> HashMap<Gid, usize> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all.is_empty() {
        return HashMap::new();
    }

    // Each vertex starts with its own unique label
    let mut labels: HashMap<Gid, usize> = all.iter().enumerate().map(|(i, &g)| (g, i)).collect();
    let mut changed = true;
    let mut iteration = 0usize;

    while changed && iteration < max_iterations {
        changed = false;
        iteration += 1;

        for &v in &all {
            let mut neighbor_labels: HashMap<usize, usize> = HashMap::new();

            for (_, neighbor, _) in storage.vertex_out_edges(v, None) {
                *neighbor_labels.entry(labels[&neighbor]).or_insert(0) += 1;
            }
            for (_, neighbor, _) in storage.vertex_in_edges(v, None) {
                *neighbor_labels.entry(labels[&neighbor]).or_insert(0) += 1;
            }

            if neighbor_labels.is_empty() {
                continue;
            }

            // Pick the most frequent label (break ties by smallest label id)
            let (&best_label, _) = neighbor_labels
                .iter()
                .max_by_key(|&(label, count)| (*count, !*label)) // count desc, then label asc
                .unwrap();

            if labels[&v] != best_label {
                labels.insert(v, best_label);
                changed = true;
            }
        }
    }

    // Renumber communities contiguously
    let mut unique: Vec<usize> = labels
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
    labels.into_iter().map(|(k, v)| (k, remap[&v])).collect()
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
    fn test_label_propagation_cliques() {
        let storage = Storage::new();
        let mut eid = 100u64;
        // Clique 1: 1-2-3
        eid = build_graph(&storage, &[(1, 2), (2, 3), (3, 1)], eid);
        // Clique 2: 4-5-6
        eid = build_graph(&storage, &[(4, 5), (5, 6), (6, 4)], eid);
        // Bridge
        build_graph(&storage, &[(3, 4)], eid);

        let communities = label_propagation(&storage, 100);
        assert_eq!(communities.len(), 6);
        // Within each clique, labels should converge (or at least not crash)
        assert_eq!(communities[&Gid::from(1u64)], communities[&Gid::from(2u64)]);
        assert_eq!(communities[&Gid::from(2u64)], communities[&Gid::from(3u64)]);
        assert_eq!(communities[&Gid::from(4u64)], communities[&Gid::from(5u64)]);
        assert_eq!(communities[&Gid::from(5u64)], communities[&Gid::from(6u64)]);
    }

    #[test]
    fn test_label_propagation_empty() {
        let storage = Storage::new();
        let communities = label_propagation(&storage, 10);
        assert!(communities.is_empty());
    }
}
