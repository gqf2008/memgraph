//! Clique detection algorithms: maximal cliques (Bron-Kerbosch).

use std::collections::{HashMap, HashSet};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Bron-Kerbosch algorithm with pivot (Tomita variant) for finding all maximal cliques.
pub fn maximal_cliques(storage: &Storage) -> Vec<Vec<Gid>> {
    let all: Vec<Gid> = storage.all_vertices().into_iter().map(|(g, _, _)| g).collect();
    let mut adj: HashMap<Gid, HashSet<Gid>> = HashMap::new();
    for &gid in &all {
        let mut set = HashSet::new();
        for (_, other, _) in storage.vertex_out_edges(gid, None) {
            set.insert(other);
        }
        for (_, other, _) in storage.vertex_in_edges(gid, None) {
            set.insert(other);
        }
        adj.insert(gid, set);
    }

    let mut cliques = Vec::new();
    let r = HashSet::new();
    let p: HashSet<Gid> = all.iter().copied().collect();
    let x = HashSet::new();

    bron_kerbosch_pivot(&adj, r, p, x, &mut cliques);
    cliques
}

fn bron_kerbosch_pivot(
    adj: &HashMap<Gid, HashSet<Gid>>,
    r: HashSet<Gid>,
    p: HashSet<Gid>,
    x: HashSet<Gid>,
    cliques: &mut Vec<Vec<Gid>>,
) {
    if p.is_empty() && x.is_empty() {
        let mut clique: Vec<Gid> = r.iter().copied().collect();
        clique.sort();
        cliques.push(clique);
        return;
    }

    // Pivot: choose u in P union X with max |P intersect N(u)|
    let union_px: Vec<Gid> = p.union(&x).copied().collect();
    let mut pivot = union_px[0];
    let mut max_count = 0usize;
    for &u in &union_px {
        let count = p.intersection(&adj[&u]).count();
        if count > max_count {
            max_count = count;
            pivot = u;
        }
    }

    let candidates: Vec<Gid> = p.difference(&adj[&pivot]).copied().collect();
    for v in candidates {
        let mut r2 = r.clone();
        r2.insert(v);
        let p2: HashSet<Gid> = p.intersection(&adj[&v]).copied().collect();
        let x2: HashSet<Gid> = x.intersection(&adj[&v]).copied().collect();
        bron_kerbosch_pivot(adj, r2, p2, x2, cliques);
        // P = P \ {v}; X = X union {v}
    }
}

/// Find all maximal cliques containing a specific vertex.
pub fn cliques_containing(storage: &Storage, vertex: Gid) -> Vec<Vec<Gid>> {
    let all = maximal_cliques(storage);
    all.into_iter()
        .filter(|c| c.contains(&vertex))
        .collect()
}

/// Size of the largest maximal clique (clique number).
pub fn clique_number(storage: &Storage) -> usize {
    maximal_cliques(storage)
        .into_iter()
        .map(|c| c.len())
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
            if created.insert(from) { storage.create_vertex(&tx, from).unwrap(); }
            if created.insert(to) { storage.create_vertex(&tx, to).unwrap(); }
            storage.create_edge(&tx, Gid::from(next_edge), from, to, EdgeTypeId::from(0u32)).unwrap();
            next_edge += 1;
        }
        storage.commit_transaction(&tx);
    }

    #[test]
    fn test_maximal_cliques_triangle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let cliques = maximal_cliques(&storage);
        assert_eq!(cliques.len(), 1);
        assert_eq!(cliques[0].len(), 3);
    }

    #[test]
    fn test_maximal_cliques_two_triangles() {
        let storage = Storage::new();
        // Two triangles sharing an edge: 1-2-3-1 and 2-3-4-2
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (2, 4), (4, 3)]);
        let cliques = maximal_cliques(&storage);
        // Both triangles are maximal cliques of size 3
        assert_eq!(cliques.len(), 2);
        for c in &cliques {
            assert_eq!(c.len(), 3);
        }
    }

    #[test]
    fn test_maximal_cliques_line() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let cliques = maximal_cliques(&storage);
        // Each edge is a maximal clique of size 2
        assert_eq!(cliques.len(), 3);
        for c in &cliques {
            assert_eq!(c.len(), 2);
        }
    }

    #[test]
    fn test_clique_number() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (3, 4)]);
        assert_eq!(clique_number(&storage), 3);
    }

    #[test]
    fn test_cliques_containing() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let cliques = cliques_containing(&storage, Gid::from(1u64));
        assert_eq!(cliques.len(), 1);
        assert!(cliques[0].contains(&Gid::from(1u64)));
    }
}
