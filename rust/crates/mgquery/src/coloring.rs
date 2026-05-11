//! Graph coloring algorithms: greedy coloring and DSatur.

use std::collections::{HashMap, HashSet};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Greedy graph coloring (Welsh-Powell heuristic: sort by degree descending).
/// Returns a map from Gid to color number (starting from 0).
pub fn greedy_coloring(storage: &Storage) -> HashMap<Gid, usize> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all.is_empty() {
        return HashMap::new();
    }

    // Sort by degree descending
    let mut sorted: Vec<(Gid, usize)> = all
        .iter()
        .map(|&g| {
            let deg = storage.vertex_out_degree(g) + storage.vertex_in_degree(g);
            (g, deg)
        })
        .collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    let mut coloring: HashMap<Gid, usize> = HashMap::new();

    for (gid, _) in sorted {
        let mut neighbor_colors = HashSet::new();
        for (_, other, _) in storage.vertex_out_edges(gid, None) {
            if let Some(&c) = coloring.get(&other) {
                neighbor_colors.insert(c);
            }
        }
        for (_, other, _) in storage.vertex_in_edges(gid, None) {
            if let Some(&c) = coloring.get(&other) {
                neighbor_colors.insert(c);
            }
        }

        let mut color = 0usize;
        while neighbor_colors.contains(&color) {
            color += 1;
        }
        coloring.insert(gid, color);
    }

    coloring
}

/// DSatur (Degree of Saturation) coloring algorithm.
/// At each step, color the vertex with the highest saturation degree
/// (number of differently colored neighbors), breaking ties by degree.
pub fn dsatur_coloring(storage: &Storage) -> HashMap<Gid, usize> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all.is_empty() {
        return HashMap::new();
    }

    let mut coloring: HashMap<Gid, usize> = HashMap::new();
    let mut uncolored: HashSet<Gid> = all.iter().copied().collect();

    fn saturation_degree(gid: Gid, storage: &Storage, coloring: &HashMap<Gid, usize>) -> usize {
        let mut colors = HashSet::new();
        for (_, other, _) in storage.vertex_out_edges(gid, None) {
            if let Some(&c) = coloring.get(&other) {
                colors.insert(c);
            }
        }
        for (_, other, _) in storage.vertex_in_edges(gid, None) {
            if let Some(&c) = coloring.get(&other) {
                colors.insert(c);
            }
        }
        colors.len()
    }

    fn degree(gid: Gid, storage: &Storage) -> usize {
        storage.vertex_out_degree(gid) + storage.vertex_in_degree(gid)
    }

    while !uncolored.is_empty() {
        // Find vertex with max saturation, then max degree
        let mut next = None;
        let mut max_sat = 0usize;
        let mut max_deg = 0usize;
        for &gid in &uncolored {
            let sat = saturation_degree(gid, storage, &coloring);
            let deg = degree(gid, storage);
            if sat > max_sat || (sat == max_sat && deg > max_deg) {
                max_sat = sat;
                max_deg = deg;
                next = Some(gid);
            }
        }

        let gid = next.unwrap();
        uncolored.remove(&gid);

        let mut used = HashSet::new();
        for (_, other, _) in storage.vertex_out_edges(gid, None) {
            if let Some(&c) = coloring.get(&other) {
                used.insert(c);
            }
        }
        for (_, other, _) in storage.vertex_in_edges(gid, None) {
            if let Some(&c) = coloring.get(&other) {
                used.insert(c);
            }
        }

        let mut color = 0usize;
        while used.contains(&color) {
            color += 1;
        }
        coloring.insert(gid, color);
    }

    coloring
}

/// Number of colors used in a coloring.
pub fn chromatic_number(coloring: &HashMap<Gid, usize>) -> usize {
    coloring.values().copied().max().map(|m| m + 1).unwrap_or(0)
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

    fn is_valid_coloring(storage: &Storage, coloring: &HashMap<Gid, usize>) -> bool {
        for (gid, &c) in coloring {
            for (_, other, _) in storage.vertex_out_edges(*gid, None) {
                if let Some(&co) = coloring.get(&other) {
                    if co == c {
                        return false;
                    }
                }
            }
        }
        true
    }

    #[test]
    fn test_greedy_triangle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let coloring = greedy_coloring(&storage);
        assert_eq!(coloring.len(), 3);
        assert!(is_valid_coloring(&storage, &coloring));
        assert_eq!(chromatic_number(&coloring), 3);
    }

    #[test]
    fn test_greedy_bipartite() {
        let storage = Storage::new();
        // Square (even cycle): 2-colorable
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4), (4, 1)]);
        let coloring = greedy_coloring(&storage);
        assert!(is_valid_coloring(&storage, &coloring));
        assert_eq!(chromatic_number(&coloring), 2);
    }

    #[test]
    fn test_dsatur_triangle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let coloring = dsatur_coloring(&storage);
        assert_eq!(coloring.len(), 3);
        assert!(is_valid_coloring(&storage, &coloring));
        assert_eq!(chromatic_number(&coloring), 3);
    }

    #[test]
    fn test_dsatur_bipartite() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4), (4, 1)]);
        let coloring = dsatur_coloring(&storage);
        assert!(is_valid_coloring(&storage, &coloring));
        assert_eq!(chromatic_number(&coloring), 2);
    }

    #[test]
    fn test_empty_graph() {
        let storage = Storage::new();
        let coloring = greedy_coloring(&storage);
        assert!(coloring.is_empty());
        assert_eq!(chromatic_number(&coloring), 0);
    }
}
