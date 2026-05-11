//! Graph embedding algorithms: node2vec-style random walks, DeepWalk.

use std::collections::{HashMap, HashSet};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// A random walk starting from `start`, with `length` steps.
/// `p` = return parameter (lower = more likely to return to previous node).
/// `q` = in-out parameter (lower = more like BFS, higher = more like DFS).
pub fn biased_random_walk(
    storage: &Storage,
    start: Gid,
    length: usize,
    p: f64,
    q: f64,
) -> Vec<Gid> {
    let mut walk = vec![start];
    if length == 0 { return walk; }

    let mut rng = fast_rng();

    for _ in 0..length {
        let current = *walk.last().unwrap();
        let prev = if walk.len() >= 2 { Some(walk[walk.len() - 2]) } else { None };

        let neighbors: Vec<Gid> = {
            let mut nbrs = Vec::new();
            for (_, other, _) in storage.vertex_out_edges(current, None) { nbrs.push(other); }
            for (_, other, _) in storage.vertex_in_edges(current, None) { nbrs.push(other); }
            nbrs
        };

        if neighbors.is_empty() { break; }

        let mut weights: Vec<f64> = Vec::with_capacity(neighbors.len());
        for &nbr in &neighbors {
            let w = if let Some(prev_node) = prev {
                if nbr == prev_node {
                    1.0 / p
                } else if are_neighbors(storage, prev_node, nbr) {
                    1.0
                } else {
                    1.0 / q
                }
            } else {
                1.0 / neighbors.len() as f64
            };
            weights.push(w);
        }

        let total: f64 = weights.iter().sum();
        let r: f64 = rng.next_f64() * total;
        let mut cum = 0.0;
        let mut chosen = neighbors[0];
        for (i, &w) in weights.iter().enumerate() {
            cum += w;
            if r <= cum {
                chosen = neighbors[i];
                break;
            }
        }
        walk.push(chosen);
    }

    walk
}

fn are_neighbors(storage: &Storage, a: Gid, b: Gid) -> bool {
    for (_, other, _) in storage.vertex_out_edges(a, None) {
        if other == b { return true; }
    }
    for (_, other, _) in storage.vertex_in_edges(a, None) {
        if other == b { return true; }
    }
    false
}

/// Generate `num_walks` random walks of `walk_length` from each vertex.
pub fn generate_walks(
    storage: &Storage,
    num_walks: usize,
    walk_length: usize,
    p: f64,
    q: f64,
) -> Vec<Vec<Gid>> {
    let all: Vec<Gid> = storage.all_vertices().into_iter().map(|(g, _, _)| g).collect();
    let mut walks = Vec::with_capacity(all.len() * num_walks);
    for &start in &all {
        for _ in 0..num_walks {
            walks.push(biased_random_walk(storage, start, walk_length, p, q));
        }
    }
    walks
}

/// Simple random walk (unbiased) from `start` with `length` steps.
pub fn simple_random_walk(storage: &Storage, start: Gid, length: usize) -> Vec<Gid> {
    biased_random_walk(storage, start, length, 1.0, 1.0)
}

/// Fast deterministic pseudo-RNG (xorshift64*) for reproducible walks in tests.
struct FastRng {
    state: u64,
}

fn fast_rng() -> FastRng {
    FastRng { state: 0x123456789abcdef0 }
}

impl FastRng {
    fn next_u64(&mut self) -> u64 {
        self.state ^= self.state >> 12;
        self.state ^= self.state << 25;
        self.state ^= self.state >> 27;
        self.state.wrapping_mul(0x2545f4914f6cdd1d)
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
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
    fn test_simple_random_walk() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let walk = simple_random_walk(&storage, Gid::from(1u64), 5);
        assert_eq!(walk[0], Gid::from(1u64));
        assert_eq!(walk.len(), 6); // start + 5 steps
    }

    #[test]
    fn test_biased_random_walk() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let walk = biased_random_walk(&storage, Gid::from(1u64), 5, 1.0, 1.0);
        assert_eq!(walk[0], Gid::from(1u64));
        assert_eq!(walk.len(), 6);
    }

    #[test]
    fn test_generate_walks() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let walks = generate_walks(&storage, 2, 3, 1.0, 1.0);
        assert_eq!(walks.len(), 6); // 3 nodes * 2 walks
        for w in &walks {
            assert_eq!(w.len(), 4); // start + 3 steps
        }
    }

    #[test]
    fn test_random_walk_isolated() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.commit_transaction(&tx);
        let walk = simple_random_walk(&storage, Gid::from(1u64), 5);
        assert_eq!(walk, vec![Gid::from(1u64)]);
    }
}
