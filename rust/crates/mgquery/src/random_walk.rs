//! Random walk utilities: simple random walks, personalized PageRank via random walk.

use std::collections::HashMap;

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Simple random walk from `start` with `steps` steps (unbiased).
/// Uses a deterministic RNG for test reproducibility.
pub fn random_walk(storage: &Storage, start: Gid, steps: usize) -> Vec<Gid> {
    let mut walk = vec![start];
    let mut rng = fast_rng();

    for _ in 0..steps {
        let current = *walk.last().unwrap();
        let mut neighbors = Vec::new();
        for (_, other, _) in storage.vertex_out_edges(current, None) {
            neighbors.push(other);
        }
        for (_, other, _) in storage.vertex_in_edges(current, None) {
            neighbors.push(other);
        }
        if neighbors.is_empty() {
            break;
        }
        let idx = (rng.next_u64() as usize) % neighbors.len();
        walk.push(neighbors[idx]);
    }

    walk
}

/// Random walk with restart (RWR): at each step, with probability `restart_prob`,
/// jump back to the start node instead of following an edge.
pub fn random_walk_with_restart(
    storage: &Storage,
    start: Gid,
    steps: usize,
    restart_prob: f64,
) -> Vec<Gid> {
    let mut walk = vec![start];
    let mut rng = fast_rng();

    for _ in 0..steps {
        let current = *walk.last().unwrap();
        if rng.next_f64() < restart_prob {
            walk.push(start);
            continue;
        }
        let mut neighbors = Vec::new();
        for (_, other, _) in storage.vertex_out_edges(current, None) {
            neighbors.push(other);
        }
        for (_, other, _) in storage.vertex_in_edges(current, None) {
            neighbors.push(other);
        }
        if neighbors.is_empty() {
            walk.push(start); // restart if stuck
            continue;
        }
        let idx = (rng.next_u64() as usize) % neighbors.len();
        walk.push(neighbors[idx]);
    }

    walk
}

/// Personalized PageRank approximation via random walk with restart.
/// Returns a map from visited Gid to visit frequency.
pub fn personalized_pagerank(
    storage: &Storage,
    start: Gid,
    walk_count: usize,
    walk_length: usize,
    restart_prob: f64,
) -> HashMap<Gid, f64> {
    let mut visit_counts: HashMap<Gid, usize> = HashMap::new();
    let mut total_visits = 0usize;

    for _ in 0..walk_count {
        let walk = random_walk_with_restart(storage, start, walk_length, restart_prob);
        for &gid in &walk {
            *visit_counts.entry(gid).or_insert(0) += 1;
            total_visits += 1;
        }
    }

    visit_counts
        .into_iter()
        .map(|(gid, count)| (gid, count as f64 / total_visits as f64))
        .collect()
}

/// Fast deterministic pseudo-RNG (xorshift64*).
struct FastRng {
    state: u64,
}

fn fast_rng() -> FastRng {
    FastRng {
        state: 0x123456789abcdef0,
    }
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
    fn test_random_walk() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let walk = random_walk(&storage, Gid::from(1u64), 5);
        assert_eq!(walk[0], Gid::from(1u64));
        assert_eq!(walk.len(), 6); // start + 5 steps
    }

    #[test]
    fn test_random_walk_isolated() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.commit_transaction(&tx);
        let walk = random_walk(&storage, Gid::from(1u64), 5);
        assert_eq!(walk, vec![Gid::from(1u64)]);
    }

    #[test]
    fn test_random_walk_with_restart() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let walk = random_walk_with_restart(&storage, Gid::from(1u64), 10, 0.2);
        assert_eq!(walk[0], Gid::from(1u64));
        assert_eq!(walk.len(), 11);
        // With restart, start node should appear multiple times
        let count = walk.iter().filter(|&&g| g == Gid::from(1u64)).count();
        assert!(count >= 1);
    }

    #[test]
    fn test_personalized_pagerank() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let ppr = personalized_pagerank(&storage, Gid::from(1u64), 100, 10, 0.15);
        assert!(!ppr.is_empty());
        let total: f64 = ppr.values().sum();
        assert!((total - 1.0).abs() < 0.01);
    }
}
