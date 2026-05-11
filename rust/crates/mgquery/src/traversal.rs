//! Graph traversal algorithms: BFS levels, DFS, topological sort, cycle detection.

use std::collections::{HashMap, HashSet, VecDeque};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// BFS returning levels (each inner Vec is one distance from start).
/// `max_depth` limits how many levels to explore (0 means only start).
pub fn bfs(storage: &Storage, start: Gid, max_depth: usize) -> Vec<Vec<Gid>> {
    let mut levels: Vec<Vec<Gid>> = Vec::new();
    let mut visited: HashSet<Gid> = HashSet::new();
    let mut queue = VecDeque::new();

    visited.insert(start);
    queue.push_back((start, 0usize));
    levels.push(vec![start]);

    while let Some((current, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        let mut neighbors = Vec::new();
        for (_, other, _) in storage.vertex_out_edges(current, None) {
            neighbors.push(other);
        }
        for (_, other, _) in storage.vertex_in_edges(current, None) {
            neighbors.push(other);
        }
        for next in neighbors {
            if visited.insert(next) {
                if depth + 1 >= levels.len() {
                    levels.push(Vec::new());
                }
                levels[depth + 1].push(next);
                queue.push_back((next, depth + 1));
            }
        }
    }

    levels
}

/// DFS traversal from `start`. Returns vertices in discovery order.
pub fn dfs(storage: &Storage, start: Gid) -> Vec<Gid> {
    let mut visited = Vec::new();
    let mut stack = vec![start];
    let mut seen: HashSet<Gid> = HashSet::new();
    seen.insert(start);

    while let Some(current) = stack.pop() {
        visited.push(current);
        let mut neighbors = Vec::new();
        for (_, other, _) in storage.vertex_out_edges(current, None) {
            neighbors.push(other);
        }
        for (_, other, _) in storage.vertex_in_edges(current, None) {
            neighbors.push(other);
        }
        for next in neighbors {
            if seen.insert(next) {
                stack.push(next);
            }
        }
    }

    visited
}

/// Topological sort (Kahn's algorithm) for directed acyclic graphs.
/// Returns `None` if the graph contains a cycle.
pub fn topological_sort(storage: &Storage) -> Option<Vec<Gid>> {
    let all_v = storage.all_vertices();
    let gids: Vec<Gid> = all_v.iter().map(|(g, _, _)| *g).collect();
    if gids.is_empty() {
        return Some(Vec::new());
    }

    let mut in_degree: HashMap<Gid, usize> = gids.iter().map(|g| (*g, 0)).collect();
    for (gid, _, _) in &all_v {
        for (_, other, _) in storage.vertex_out_edges(*gid, None) {
            *in_degree.get_mut(&other).unwrap() += 1;
        }
    }

    let mut queue: VecDeque<Gid> = in_degree
        .iter()
        .filter(|(_, &d)| d == 0)
        .map(|(&g, _)| g)
        .collect();
    let mut sorted = Vec::new();

    while let Some(v) = queue.pop_front() {
        sorted.push(v);
        for (_, other, _) in storage.vertex_out_edges(v, None) {
            let d = in_degree.get_mut(&other).unwrap();
            *d -= 1;
            if *d == 0 {
                queue.push_back(other);
            }
        }
    }

    if sorted.len() == gids.len() {
        Some(sorted)
    } else {
        None
    }
}

/// Detect a cycle in a directed graph.
/// Returns one cycle as a Vec of Gids if found, or None.
pub fn cycle_detection(storage: &Storage) -> Option<Vec<Gid>> {
    let all: Vec<Gid> = storage.all_vertices().into_iter().map(|(g, _, _)| g).collect();
    let mut visited: HashSet<Gid> = HashSet::new();
    let mut rec_stack: HashSet<Gid> = HashSet::new();
    let mut parent: HashMap<Gid, Gid> = HashMap::new();

    fn dfs(
        v: Gid,
        storage: &Storage,
        visited: &mut HashSet<Gid>,
        rec_stack: &mut HashSet<Gid>,
        parent: &mut HashMap<Gid, Gid>,
    ) -> Option<Gid> {
        visited.insert(v);
        rec_stack.insert(v);
        for (_, w, _) in storage.vertex_out_edges(v, None) {
            if !visited.contains(&w) {
                parent.insert(w, v);
                if let Some(cycle_node) = dfs(w, storage, visited, rec_stack, parent) {
                    return Some(cycle_node);
                }
            } else if rec_stack.contains(&w) {
                parent.insert(w, v);
                return Some(w);
            }
        }
        rec_stack.remove(&v);
        None
    }

    for v in &all {
        if !visited.contains(v) {
            if let Some(cycle_node) = dfs(*v, storage, &mut visited, &mut rec_stack, &mut parent) {
                // Reconstruct cycle
                let mut cycle = vec![cycle_node];
                let mut node = parent[&cycle_node];
                while node != cycle_node {
                    cycle.push(node);
                    node = parent[&node];
                }
                cycle.push(cycle_node);
                cycle.reverse();
                return Some(cycle);
            }
        }
    }

    None
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
    fn test_bfs_levels() {
        let storage = Storage::new();
        // Line: 1-2-3-4
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let levels = bfs(&storage, Gid::from(1u64), 10);
        assert_eq!(levels.len(), 4);
        assert_eq!(levels[0], vec![Gid::from(1u64)]);
        assert_eq!(levels[1], vec![Gid::from(2u64)]);
        assert_eq!(levels[2], vec![Gid::from(3u64)]);
        assert_eq!(levels[3], vec![Gid::from(4u64)]);
    }

    #[test]
    fn test_bfs_max_depth() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let levels = bfs(&storage, Gid::from(1u64), 1);
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0], vec![Gid::from(1u64)]);
        assert_eq!(levels[1], vec![Gid::from(2u64)]);
    }

    #[test]
    fn test_bfs_triangle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let levels = bfs(&storage, Gid::from(1u64), 10);
        // In an undirected triangle from node 1:
        // level 0: {1}, level 1: {2, 3}
        assert_eq!(levels.len(), 2);
        assert_eq!(levels[0], vec![Gid::from(1u64)]);
        assert_eq!(levels[1].len(), 2);
        assert!(levels[1].contains(&Gid::from(2u64)));
        assert!(levels[1].contains(&Gid::from(3u64)));
    }

    #[test]
    fn test_dfs_basic() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let order = dfs(&storage, Gid::from(1u64));
        assert_eq!(order.len(), 4);
        assert_eq!(order[0], Gid::from(1u64));
        // DFS should visit all nodes
        assert!(order.contains(&Gid::from(2u64)));
        assert!(order.contains(&Gid::from(3u64)));
        assert!(order.contains(&Gid::from(4u64)));
    }

    #[test]
    fn test_dfs_triangle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let order = dfs(&storage, Gid::from(1u64));
        assert_eq!(order.len(), 3);
        assert_eq!(order[0], Gid::from(1u64));
    }

    #[test]
    fn test_topological_sort_dag() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (1, 3), (2, 4), (3, 4)]);
        let sorted = topological_sort(&storage).unwrap();
        assert_eq!(sorted.len(), 4);
        let pos1 = sorted.iter().position(|g| *g == Gid::from(1u64)).unwrap();
        let pos2 = sorted.iter().position(|g| *g == Gid::from(2u64)).unwrap();
        let pos4 = sorted.iter().position(|g| *g == Gid::from(4u64)).unwrap();
        assert!(pos1 < pos2);
        assert!(pos2 < pos4);
    }

    #[test]
    fn test_topological_sort_cycle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        assert!(topological_sort(&storage).is_none());
    }

    #[test]
    fn test_topological_sort_empty() {
        let storage = Storage::new();
        let sorted = topological_sort(&storage).unwrap();
        assert!(sorted.is_empty());
    }

    #[test]
    fn test_cycle_detection_found() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let cycle = cycle_detection(&storage);
        assert!(cycle.is_some());
        let c = cycle.unwrap();
        // Cycle should contain at least the cycle nodes
        assert!(c.contains(&Gid::from(1u64)));
        assert!(c.contains(&Gid::from(2u64)));
        assert!(c.contains(&Gid::from(3u64)));
    }

    #[test]
    fn test_cycle_detection_none() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3)]);
        assert!(cycle_detection(&storage).is_none());
    }

    #[test]
    fn test_cycle_detection_self_loop() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage
            .create_edge(&tx, Gid::from(100u64), Gid::from(1u64), Gid::from(1u64), EdgeTypeId::from(0u32))
            .unwrap();
        storage.commit_transaction(&tx);
        let cycle = cycle_detection(&storage);
        assert!(cycle.is_some());
        let c = cycle.unwrap();
        assert!(c.contains(&Gid::from(1u64)));
    }
}
