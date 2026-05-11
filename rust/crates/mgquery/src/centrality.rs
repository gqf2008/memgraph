//! Centrality measures: degree, betweenness, closeness.

use std::collections::{HashMap, VecDeque};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Degree centrality: fraction of vertices a node is connected to (undirected).
/// Returns a map of Gid -> centrality score.
pub fn degree_centrality(storage: &Storage) -> HashMap<Gid, f64> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let n = all.len();
    if n <= 1 {
        return all.iter().map(|g| (*g, 0.0)).collect();
    }

    let mut result = HashMap::new();
    for gid in &all {
        let deg = storage.vertex_out_degree(*gid) + storage.vertex_in_degree(*gid);
        result.insert(*gid, deg as f64 / (n as f64 - 1.0));
    }
    result
}

/// Betweenness centrality (Brandes' algorithm, unweighted, undirected).
/// Returns a map of Gid -> centrality score.
pub fn betweenness_centrality(storage: &Storage) -> HashMap<Gid, f64> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut cb: HashMap<Gid, f64> = all.iter().map(|g| (*g, 0.0)).collect();

    for s in &all {
        let mut stack: Vec<Gid> = Vec::new();
        let mut pred: HashMap<Gid, Vec<Gid>> = all.iter().map(|g| (*g, Vec::new())).collect();
        let mut sigma: HashMap<Gid, usize> = all.iter().map(|g| (*g, 0usize)).collect();
        let mut dist: HashMap<Gid, i64> = all.iter().map(|g| (*g, -1i64)).collect();
        let mut queue = VecDeque::new();

        sigma.insert(*s, 1);
        dist.insert(*s, 0);
        queue.push_back(*s);

        while let Some(v) = queue.pop_front() {
            stack.push(v);
            let mut neighbors = Vec::new();
            for (_, other, _) in storage.vertex_out_edges(v, None) {
                neighbors.push(other);
            }
            for (_, other, _) in storage.vertex_in_edges(v, None) {
                neighbors.push(other);
            }
            for w in neighbors {
                if *dist.get(&w).unwrap_or(&-1) < 0 {
                    queue.push_back(w);
                    dist.insert(w, dist[&v] + 1);
                }
                if dist[&w] == dist[&v] + 1 {
                    sigma.insert(w, sigma[&w] + sigma[&v]);
                    pred.get_mut(&w).unwrap().push(v);
                }
            }
        }

        let mut delta: HashMap<Gid, f64> = all.iter().map(|g| (*g, 0.0)).collect();
        while let Some(w) = stack.pop() {
            for v in &pred[&w] {
                let sv = sigma[v] as f64;
                let sw = sigma[&w] as f64;
                if sw > 0.0 {
                    let contrib = (sv / sw) * (1.0 + delta[&w]);
                    *delta.get_mut(v).unwrap() += contrib;
                }
            }
            if w != *s {
                *cb.get_mut(&w).unwrap() += delta[&w];
            }
        }
    }

    // Normalize for undirected graphs: divide by 2
    for v in &all {
        *cb.get_mut(v).unwrap() /= 2.0;
    }

    cb
}

/// Closeness centrality for each vertex (undirected, unweighted).
/// Uses the standard definition: (reachable - 1) / (n - 1) * (reachable - 1) / sum_dist.
/// Returns 0.0 for isolated or unreachable vertices.
pub fn closeness_centrality(storage: &Storage) -> HashMap<Gid, f64> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let n = all.len() as f64;
    if n <= 1.0 {
        return all.iter().map(|g| (*g, 0.0)).collect();
    }

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

        let sum_dist: usize = all
            .iter()
            .filter(|g| *g != s)
            .map(|g| visited.get(g).copied().unwrap_or(0))
            .sum();
        let reachable = visited.len() as f64;
        let cc = if sum_dist == 0 || reachable <= 1.0 {
            0.0
        } else {
            (reachable - 1.0) / (n - 1.0) * (reachable - 1.0) / sum_dist as f64
        };
        result.insert(*s, cc);
    }

    result
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
    fn test_degree_centrality() {
        let storage = Storage::new();
        // Triangle: each node has degree 2
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let dc = degree_centrality(&storage);
        assert_eq!(dc.len(), 3);
        // n=3, max degree = 2, so centrality = 2/2 = 1.0 for each
        assert!((dc[&Gid::from(1u64)] - 1.0).abs() < 1e-6);
        assert!((dc[&Gid::from(2u64)] - 1.0).abs() < 1e-6);
        assert!((dc[&Gid::from(3u64)] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_degree_centrality_star() {
        let storage = Storage::new();
        // Star: 1 connected to 2,3,4
        build_graph(&storage, &[(1, 2), (1, 3), (1, 4)]);
        let dc = degree_centrality(&storage);
        // n=4, center has degree 3 -> 3/3 = 1.0, leaves have degree 1 -> 1/3
        assert!((dc[&Gid::from(1u64)] - 1.0).abs() < 1e-6);
        assert!((dc[&Gid::from(2u64)] - 1.0 / 3.0).abs() < 1e-6);
    }

    #[test]
    fn test_betweenness_centrality_line() {
        let storage = Storage::new();
        // Line graph: 1-2-3-4
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let bc = betweenness_centrality(&storage);
        assert_eq!(bc.len(), 4);
        // Middle nodes should have higher betweenness
        assert!(bc[&Gid::from(2u64)] > bc[&Gid::from(1u64)]);
        assert!(bc[&Gid::from(3u64)] > bc[&Gid::from(1u64)]);
        // End nodes have zero betweenness in an undirected line
        assert_eq!(bc[&Gid::from(1u64)], 0.0);
        assert_eq!(bc[&Gid::from(4u64)], 0.0);
    }

    #[test]
    fn test_betweenness_centrality_star() {
        let storage = Storage::new();
        // Star: 1 connected to 2,3,4,5
        build_graph(&storage, &[(1, 2), (1, 3), (1, 4), (1, 5)]);
        let bc = betweenness_centrality(&storage);
        // Center node lies on all shortest paths between leaves
        assert!(bc[&Gid::from(1u64)] > 0.0);
        // Leaves have zero betweenness
        assert_eq!(bc[&Gid::from(2u64)], 0.0);
    }

    #[test]
    fn test_closeness_centrality_line() {
        let storage = Storage::new();
        // Line: 1-2-3-4
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let cc = closeness_centrality(&storage);
        assert_eq!(cc.len(), 4);
        // Middle nodes should have higher closeness
        assert!(cc[&Gid::from(2u64)] > cc[&Gid::from(1u64)]);
        assert!(cc[&Gid::from(3u64)] > cc[&Gid::from(4u64)]);
    }

    #[test]
    fn test_closeness_centrality_isolated() {
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);
        let cc = closeness_centrality(&storage);
        assert_eq!(cc[&Gid::from(1u64)], 0.0);
        assert_eq!(cc[&Gid::from(2u64)], 0.0);
    }
}
