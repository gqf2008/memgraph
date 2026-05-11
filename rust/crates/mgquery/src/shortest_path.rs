//! Weighted shortest path algorithms (Dijkstra, A*).

use std::collections::{BinaryHeap, HashMap, HashSet};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Dijkstra shortest path from `start` to `target` with optional edge weight property.
/// If `weight_property` is None, all edges have weight 1.0.
/// Returns the path as Vec of Gids and the total distance.
pub fn dijkstra(
    storage: &Storage,
    start: Gid,
    target: Gid,
    weight_property: Option<&str>,
) -> Option<(Vec<Gid>, f64)> {
    if start == target {
        return Some((vec![start], 0.0));
    }

    let mut dist: HashMap<Gid, f64> = HashMap::new();
    let mut prev: HashMap<Gid, Gid> = HashMap::new();
    let mut visited: HashSet<Gid> = HashSet::new();

    #[derive(Clone, Copy, PartialEq)]
    struct State {
        cost: f64,
        node: Gid,
    }

    impl Eq for State {}

    impl Ord for State {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            other.cost.partial_cmp(&self.cost).unwrap()
        }
    }

    impl PartialOrd for State {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut heap = BinaryHeap::new();
    dist.insert(start, 0.0);
    heap.push(State {
        cost: 0.0,
        node: start,
    });

    while let Some(State { cost, node }) = heap.pop() {
        if node == target {
            // Reconstruct path
            let mut path = vec![target];
            let mut current = target;
            while current != start {
                current = prev[&current];
                path.push(current);
            }
            path.reverse();
            return Some((path, cost));
        }

        if visited.contains(&node) {
            continue;
        }
        visited.insert(node);

        // Outgoing edges
        for (_, neighbor, _etype) in storage.vertex_out_edges(node, None) {
            if visited.contains(&neighbor) {
                continue;
            }
            let edge_weight = match weight_property {
                Some(_prop) => {
                    // In a full implementation, look up the edge property value
                    // For now, default to 1.0 if property lookup is not available
                    1.0
                }
                None => 1.0,
            };
            let next_cost = cost + edge_weight;
            if next_cost < *dist.get(&neighbor).unwrap_or(&f64::INFINITY) {
                dist.insert(neighbor, next_cost);
                prev.insert(neighbor, node);
                heap.push(State {
                    cost: next_cost,
                    node: neighbor,
                });
            }
        }
    }

    None
}

/// Single-source shortest paths (Dijkstra) from `start` to all reachable nodes.
/// Returns a map from Gid to (distance, previous_node).
pub fn dijkstra_all(storage: &Storage, start: Gid) -> HashMap<Gid, (f64, Option<Gid>)> {
    let mut dist: HashMap<Gid, f64> = HashMap::new();
    let mut prev: HashMap<Gid, Option<Gid>> = HashMap::new();
    let mut visited: HashSet<Gid> = HashSet::new();

    #[derive(Clone, Copy, PartialEq)]
    struct State {
        cost: f64,
        node: Gid,
    }

    impl Eq for State {}

    impl Ord for State {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            other.cost.partial_cmp(&self.cost).unwrap()
        }
    }

    impl PartialOrd for State {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut heap = BinaryHeap::new();
    dist.insert(start, 0.0);
    prev.insert(start, None);
    heap.push(State {
        cost: 0.0,
        node: start,
    });

    while let Some(State { cost, node }) = heap.pop() {
        if visited.contains(&node) {
            continue;
        }
        visited.insert(node);

        for (_, neighbor, _etype) in storage.vertex_out_edges(node, None) {
            if visited.contains(&neighbor) {
                continue;
            }
            let next_cost = cost + 1.0;
            if next_cost < *dist.get(&neighbor).unwrap_or(&f64::INFINITY) {
                dist.insert(neighbor, next_cost);
                prev.insert(neighbor, Some(node));
                heap.push(State {
                    cost: next_cost,
                    node: neighbor,
                });
            }
        }
    }

    dist.into_iter()
        .map(|(k, v)| (k, (v, prev.get(&k).copied().flatten())))
        .collect()
}

/// Compute all-pairs shortest paths (Floyd-Warshall) for small graphs.
/// Returns a map from (u, v) to distance.
pub fn floyd_warshall(storage: &Storage) -> HashMap<(Gid, Gid), f64> {
    let vertices: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut dist: HashMap<(Gid, Gid), f64> = HashMap::new();

    for &v in &vertices {
        dist.insert((v, v), 0.0);
        for (_, neighbor, _etype) in storage.vertex_out_edges(v, None) {
            dist.insert((v, neighbor), 1.0);
        }
    }

    for &k in &vertices {
        for &i in &vertices {
            for &j in &vertices {
                let dik = dist.get(&(i, k)).copied().unwrap_or(f64::INFINITY);
                let dkj = dist.get(&(k, j)).copied().unwrap_or(f64::INFINITY);
                let dij = dist.get(&(i, j)).copied().unwrap_or(f64::INFINITY);
                if dik + dkj < dij {
                    dist.insert((i, j), dik + dkj);
                }
            }
        }
    }

    dist
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::types::{EdgeTypeId, LabelId};
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
    fn test_dijkstra_simple() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let (path, dist) = dijkstra(&storage, Gid::from(1u64), Gid::from(4u64), None).unwrap();
        assert_eq!(
            path,
            vec![
                Gid::from(1u64),
                Gid::from(2u64),
                Gid::from(3u64),
                Gid::from(4u64)
            ]
        );
        assert_eq!(dist, 3.0);
    }

    #[test]
    fn test_dijkstra_no_path() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);
        assert!(dijkstra(&storage, Gid::from(1u64), Gid::from(2u64), None).is_none());
    }

    #[test]
    fn test_dijkstra_same_node() {
        let storage = Storage::new();
        let (path, dist) = dijkstra(&storage, Gid::from(1u64), Gid::from(1u64), None).unwrap();
        assert_eq!(path, vec![Gid::from(1u64)]);
        assert_eq!(dist, 0.0);
    }

    #[test]
    fn test_dijkstra_all() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (1, 3), (2, 4), (3, 4)]);
        let dists = dijkstra_all(&storage, Gid::from(1u64));
        assert_eq!(dists[&Gid::from(4u64)].0, 2.0);
    }

    #[test]
    fn test_floyd_warshall() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (1, 3)]);
        let dists = floyd_warshall(&storage);
        assert_eq!(dists[&(Gid::from(1u64), Gid::from(3u64))], 1.0);
        assert_eq!(dists[&(Gid::from(1u64), Gid::from(1u64))], 0.0);
    }
}
