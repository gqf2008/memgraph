//! K-core decomposition: finding the maximal subgraph where each vertex has degree >= k.

use std::collections::{HashMap, HashSet};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Compute the core number for each vertex (maximum k for which the vertex belongs to the k-core).
pub fn core_decomposition(storage: &Storage) -> HashMap<Gid, usize> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all.is_empty() {
        return HashMap::new();
    }

    let mut degrees: HashMap<Gid, usize> = all
        .iter()
        .map(|&g| {
            let deg = storage.vertex_out_degree(g) + storage.vertex_in_degree(g);
            (g, deg)
        })
        .collect();

    let mut core_numbers: HashMap<Gid, usize> = HashMap::new();
    let mut removed: HashSet<Gid> = HashSet::new();

    for _ in 0..all.len() {
        // Find vertex with minimum degree among remaining
        let mut min_deg = usize::MAX;
        let mut next = None;
        for &gid in &all {
            if removed.contains(&gid) {
                continue;
            }
            let d = degrees[&gid];
            if d < min_deg {
                min_deg = d;
                next = Some(gid);
            }
        }

        let gid = next.unwrap();
        removed.insert(gid);
        core_numbers.insert(gid, min_deg);

        // Decrease degree of remaining neighbors (only those with degree > current)
        let mut neighbors = HashSet::new();
        for (_, other, _) in storage.vertex_out_edges(gid, None) {
            if !removed.contains(&other) {
                neighbors.insert(other);
            }
        }
        for (_, other, _) in storage.vertex_in_edges(gid, None) {
            if !removed.contains(&other) {
                neighbors.insert(other);
            }
        }
        let current_deg = degrees[&gid];
        for nbr in neighbors {
            if let Some(d) = degrees.get_mut(&nbr) {
                if *d > current_deg {
                    *d -= 1;
                }
            }
        }
    }

    core_numbers
}

/// Get the k-core: all vertices with core number >= k.
pub fn k_core(storage: &Storage, k: usize) -> Vec<Gid> {
    let cores = core_decomposition(storage);
    cores
        .into_iter()
        .filter(|(_, c)| *c >= k)
        .map(|(g, _)| g)
        .collect()
}

/// Degeneracy of the graph: the maximum core number.
pub fn degeneracy(storage: &Storage) -> usize {
    core_decomposition(storage)
        .values()
        .copied()
        .max()
        .unwrap_or(0)
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
    fn test_core_decomposition_triangle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let cores = core_decomposition(&storage);
        // Triangle: each vertex has degree 2, so core number = 2
        assert_eq!(cores[&Gid::from(1u64)], 2);
        assert_eq!(cores[&Gid::from(2u64)], 2);
        assert_eq!(cores[&Gid::from(3u64)], 2);
    }

    #[test]
    fn test_core_decomposition_triangle_with_tail() {
        let storage = Storage::new();
        // Triangle 1-2-3-1, plus tail 3-4
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (3, 4)]);
        let cores = core_decomposition(&storage);
        // Node 4 has degree 1, so core number = 1
        assert_eq!(cores[&Gid::from(4u64)], 1);
        // Nodes 1,2,3 still form a triangle with effective core 2
        assert_eq!(cores[&Gid::from(1u64)], 2);
        assert_eq!(cores[&Gid::from(2u64)], 2);
        assert_eq!(cores[&Gid::from(3u64)], 2);
    }

    #[test]
    fn test_k_core() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (3, 4)]);
        let k2 = k_core(&storage, 2);
        assert_eq!(k2.len(), 3);
        assert!(k2.contains(&Gid::from(1u64)));
        assert!(k2.contains(&Gid::from(2u64)));
        assert!(k2.contains(&Gid::from(3u64)));
    }

    #[test]
    fn test_degeneracy() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        assert_eq!(degeneracy(&storage), 2);
    }
}
