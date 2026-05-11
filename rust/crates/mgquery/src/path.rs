//! Shortest path algorithms.

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// BFS shortest path from `start` to `end`.
/// Returns the path as a Vec of Gids (including both start and end), or `None` if no path exists.
pub fn shortest_path(storage: &Storage, start: Gid, end: Gid) -> Option<Vec<Gid>> {
    if start == end {
        return Some(vec![start]);
    }

    let mut visited: HashMap<Gid, Gid> = HashMap::new();
    let mut queue: VecDeque<Gid> = VecDeque::new();
    queue.push_back(start);
    visited.insert(start, start);

    while let Some(current) = queue.pop_front() {
        let mut neighbors = Vec::new();
        for (_, other, _) in storage.vertex_out_edges(current, None) {
            neighbors.push(other);
        }
        for (_, other, _) in storage.vertex_in_edges(current, None) {
            neighbors.push(other);
        }

        for next in neighbors {
            if visited.contains_key(&next) {
                continue;
            }
            visited.insert(next, current);
            if next == end {
                // Reconstruct path
                let mut path = vec![end];
                let mut node = end;
                while node != start {
                    node = visited[&node];
                    path.push(node);
                }
                path.reverse();
                return Some(path);
            }
            queue.push_back(next);
        }
    }

    None
}

/// All-pairs shortest path distances (undirected, unweighted).
/// Uses repeated BFS for each vertex. Returns a map from (u, v) pairs to distance.
pub fn all_pairs_shortest_path(storage: &Storage) -> HashMap<(Gid, Gid), usize> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut result = HashMap::new();

    for s in &all {
        let mut visited: HashMap<Gid, usize> = HashMap::new();
        let mut queue = VecDeque::new();
        visited.insert(*s, 0);
        queue.push_back(*s);

        while let Some(v) = queue.pop_front() {
            let d = visited[&v];
            let mut neighbors = Vec::new();
            for (_, other, _) in storage.vertex_out_edges(v, None) {
                neighbors.push(other);
            }
            for (_, other, _) in storage.vertex_in_edges(v, None) {
                neighbors.push(other);
            }
            for w in neighbors {
                if let std::collections::hash_map::Entry::Vacant(e) = visited.entry(w) {
                    e.insert(d + 1);
                    queue.push_back(w);
                }
            }
        }

        for (&target, &dist) in &visited {
            result.insert((*s, target), dist);
        }
    }

    result
}

/// Dijkstra shortest path on a weighted graph.
/// `weight_prop` is the PropertyId of the edge weight (expected to be PropertyValue::Double).
/// Returns the path and total weight, or None if no path exists.
pub fn shortest_path_weighted(
    storage: &Storage,
    start: Gid,
    end: Gid,
    weight_prop: mgcore::types::PropertyId,
) -> Option<(Vec<Gid>, f64)> {
    if start == end {
        return Some((vec![start], 0.0));
    }

    #[derive(Clone, Copy, PartialEq)]
    struct State {
        cost: f64,
        node: Gid,
    }

    impl Eq for State {}

    impl Ord for State {
        fn cmp(&self, other: &Self) -> Ordering {
            // Reverse for min-heap via BinaryHeap (which is a max-heap)
            other
                .cost
                .partial_cmp(&self.cost)
                .unwrap_or(Ordering::Equal)
        }
    }

    impl PartialOrd for State {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut dist: HashMap<Gid, f64> = HashMap::new();
    let mut prev: HashMap<Gid, Gid> = HashMap::new();
    let mut heap = std::collections::BinaryHeap::new();

    dist.insert(start, 0.0);
    heap.push(State {
        cost: 0.0,
        node: start,
    });

    while let Some(State { cost, node }) = heap.pop() {
        if node == end {
            // Reconstruct path
            let mut path = vec![end];
            let mut n = end;
            while n != start {
                n = prev[&n];
                path.push(n);
            }
            path.reverse();
            return Some((path, cost));
        }

        if cost > *dist.get(&node).unwrap_or(&f64::INFINITY) {
            continue;
        }

        // Outgoing edges
        for (_, other, _) in storage.vertex_out_edges(node, None) {
            let weight = get_edge_weight(storage, node, other, weight_prop);
            let next_cost = cost + weight;
            if next_cost < *dist.get(&other).unwrap_or(&f64::INFINITY) {
                dist.insert(other, next_cost);
                prev.insert(other, node);
                heap.push(State {
                    cost: next_cost,
                    node: other,
                });
            }
        }

        // Incoming edges (treat graph as undirected for shortest path)
        for (_, other, _) in storage.vertex_in_edges(node, None) {
            let weight = get_edge_weight(storage, other, node, weight_prop);
            let next_cost = cost + weight;
            if next_cost < *dist.get(&other).unwrap_or(&f64::INFINITY) {
                dist.insert(other, next_cost);
                prev.insert(other, node);
                heap.push(State {
                    cost: next_cost,
                    node: other,
                });
            }
        }
    }

    None
}

/// Helper: try to read a weight from an edge's properties.
/// Since we don't have direct edge property access by edge gid in the public API,
/// we approximate by looking at the first edge between `from` and `to`.
fn get_edge_weight(
    storage: &Storage,
    from: Gid,
    to: Gid,
    weight_prop: mgcore::types::PropertyId,
) -> f64 {
    // We can't directly query edge properties from the public Storage API
    // without the edge gid. As a fallback, we return 1.0 for unweighted cases.
    // In a real implementation, the Storage API would expose edge properties
    // by edge gid or by (from, to) pair.
    // For now, we use the all_edges() method to find the property.
    for (_edge_gid, e_from, e_to, _etype, props) in storage.all_edges() {
        if (e_from == from && e_to == to) || (e_from == to && e_to == from) {
            match props.get(weight_prop) {
                mgcore::property_value::PropertyValue::Double(v) => return *v,
                mgcore::property_value::PropertyValue::Int(v) => return *v as f64,
                _ => continue,
            }
        }
    }
    1.0
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
    fn test_shortest_path_basic() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let path = shortest_path(&storage, Gid::from(1u64), Gid::from(4u64));
        assert_eq!(
            path,
            Some(vec![
                Gid::from(1u64),
                Gid::from(2u64),
                Gid::from(3u64),
                Gid::from(4u64)
            ])
        );
    }

    #[test]
    fn test_shortest_path_same_node() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2)]);
        let path = shortest_path(&storage, Gid::from(1u64), Gid::from(1u64));
        assert_eq!(path, Some(vec![Gid::from(1u64)]));
    }

    #[test]
    fn test_shortest_path_no_path() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);
        let path = shortest_path(&storage, Gid::from(1u64), Gid::from(2u64));
        assert_eq!(path, None);
    }

    #[test]
    fn test_all_pairs_shortest_path() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let dists = all_pairs_shortest_path(&storage);
        assert_eq!(dists.get(&(Gid::from(1u64), Gid::from(4u64))), Some(&3));
        assert_eq!(dists.get(&(Gid::from(4u64), Gid::from(1u64))), Some(&3));
        assert_eq!(dists.get(&(Gid::from(1u64), Gid::from(1u64))), Some(&0));
    }

    #[test]
    fn test_all_pairs_empty() {
        let storage = Storage::new();
        let dists = all_pairs_shortest_path(&storage);
        assert!(dists.is_empty());
    }

    #[test]
    fn test_shortest_path_weighted_basic() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        for i in 1..=4 {
            storage.create_vertex(&tx, Gid::from(i)).unwrap();
        }
        // Path 1->2->3->4 with weight 1 each
        storage
            .create_edge(
                &tx,
                Gid::from(100u64),
                Gid::from(1u64),
                Gid::from(2u64),
                EdgeTypeId::from(0u32),
            )
            .unwrap();
        storage
            .create_edge(
                &tx,
                Gid::from(101u64),
                Gid::from(2u64),
                Gid::from(3u64),
                EdgeTypeId::from(0u32),
            )
            .unwrap();
        storage
            .create_edge(
                &tx,
                Gid::from(102u64),
                Gid::from(3u64),
                Gid::from(4u64),
                EdgeTypeId::from(0u32),
            )
            .unwrap();
        storage.commit_transaction(&tx);

        let result = shortest_path_weighted(
            &storage,
            Gid::from(1u64),
            Gid::from(4u64),
            mgcore::types::PropertyId::from(0u32),
        );
        // Without explicit weights set on edges, fallback weight is 1.0 per edge
        assert!(result.is_some());
        let (path, weight) = result.unwrap();
        assert_eq!(path.len(), 4);
        assert!((weight - 3.0).abs() < 1e-6);
    }

    #[test]
    fn test_shortest_path_weighted_no_path() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);

        let result = shortest_path_weighted(
            &storage,
            Gid::from(1u64),
            Gid::from(2u64),
            mgcore::types::PropertyId::from(0u32),
        );
        assert_eq!(result, None);
    }
}
