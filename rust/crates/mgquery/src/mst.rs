//! Minimum Spanning Tree algorithms: Kruskal and Prim.

use std::collections::{HashMap, HashSet, VecDeque};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// Edge with weight for MST algorithms.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WeightedEdge {
    pub from: Gid,
    pub to: Gid,
    pub weight: f64,
}

/// Kruskal's MST algorithm (undirected, uses union-find).
/// Returns the list of edges in the MST and total weight.
pub fn kruskal(edges: Vec<WeightedEdge>) -> (Vec<WeightedEdge>, f64) {
    let mut sorted = edges;
    sorted.sort_by(|a, b| a.weight.partial_cmp(&b.weight).unwrap_or(std::cmp::Ordering::Equal));

    let mut parent: HashMap<Gid, Gid> = HashMap::new();

    fn find(parent: &mut HashMap<Gid, Gid>, x: Gid) -> Gid {
        let p = *parent.get(&x).unwrap_or(&x);
        if p != x {
            let root = find(parent, p);
            parent.insert(x, root);
            root
        } else {
            x
        }
    }

    fn union(parent: &mut HashMap<Gid, Gid>, a: Gid, b: Gid) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra != rb {
            parent.insert(ra, rb);
        }
    }

    let mut mst = Vec::new();
    let mut total = 0.0;

    for e in sorted {
        if find(&mut parent, e.from) != find(&mut parent, e.to) {
            union(&mut parent, e.from, e.to);
            total += e.weight;
            mst.push(e);
        }
    }

    (mst, total)
}

/// Prim's MST algorithm starting from `start`.
/// Uses a min-heap. Returns edges in the MST and total weight.
pub fn prim(storage: &Storage, start: Gid) -> (Vec<WeightedEdge>, f64) {
    let mut in_mst = HashSet::new();
    let mut mst = Vec::new();
    let mut total = 0.0;

    // Priority queue: (weight, from, to)
    use std::cmp::Ordering;
    use std::collections::BinaryHeap;

    #[derive(Clone, Copy, PartialEq)]
    struct State {
        weight: f64,
        from: Gid,
        to: Gid,
    }

    impl Eq for State {}
    impl Ord for State {
        fn cmp(&self, other: &Self) -> Ordering {
            other.weight.partial_cmp(&self.weight).unwrap_or(Ordering::Equal)
        }
    }
    impl PartialOrd for State {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut heap = BinaryHeap::new();
    in_mst.insert(start);

    // Add initial edges from start
    for (_, other, _) in storage.vertex_out_edges(start, None) {
        if other != start {
            heap.push(State { weight: 1.0, from: start, to: other });
        }
    }
    for (_, other, _) in storage.vertex_in_edges(start, None) {
        if other != start {
            heap.push(State { weight: 1.0, from: start, to: other });
        }
    }

    while let Some(State { weight, from, to }) = heap.pop() {
        if in_mst.contains(&to) { continue; }
        in_mst.insert(to);
        mst.push(WeightedEdge { from, to, weight });
        total += weight;

        for (_, other, _) in storage.vertex_out_edges(to, None) {
            if !in_mst.contains(&other) {
                heap.push(State { weight: 1.0, from: to, to: other });
            }
        }
        for (_, other, _) in storage.vertex_in_edges(to, None) {
            if !in_mst.contains(&other) {
                heap.push(State { weight: 1.0, from: to, to: other });
            }
        }
    }

    (mst, total)
}

/// Extract all undirected edges from the graph with uniform weight 1.0.
pub fn extract_edges(storage: &Storage) -> Vec<WeightedEdge> {
    let mut seen = HashSet::new();
    let mut edges = Vec::new();
    for (gid, _, _) in storage.all_vertices() {
        for (_, other, _) in storage.vertex_out_edges(gid, None) {
            let key = if gid <= other { (gid, other) } else { (other, gid) };
            if seen.insert(key) {
                edges.push(WeightedEdge { from: gid, to: other, weight: 1.0 });
            }
        }
    }
    edges
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
    fn test_kruskal_line() {
        // 4-node line: 1-2-3-4 -> MST has 3 edges
        let edges = vec![
            WeightedEdge { from: Gid::from(1u64), to: Gid::from(2u64), weight: 1.0 },
            WeightedEdge { from: Gid::from(2u64), to: Gid::from(3u64), weight: 1.0 },
            WeightedEdge { from: Gid::from(3u64), to: Gid::from(4u64), weight: 1.0 },
        ];
        let (mst, total) = kruskal(edges);
        assert_eq!(mst.len(), 3);
        assert_eq!(total, 3.0);
    }

    #[test]
    fn test_kruskal_triangle() {
        // Triangle with one heavy edge: 1-2(1), 2-3(1), 1-3(10)
        let edges = vec![
            WeightedEdge { from: Gid::from(1u64), to: Gid::from(2u64), weight: 1.0 },
            WeightedEdge { from: Gid::from(2u64), to: Gid::from(3u64), weight: 1.0 },
            WeightedEdge { from: Gid::from(1u64), to: Gid::from(3u64), weight: 10.0 },
        ];
        let (mst, total) = kruskal(edges);
        assert_eq!(mst.len(), 2);
        assert_eq!(total, 2.0);
    }

    #[test]
    fn test_prim_line() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let (mst, total) = prim(&storage, Gid::from(1u64));
        assert_eq!(mst.len(), 3);
        assert_eq!(total, 3.0);
    }

    #[test]
    fn test_prim_triangle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let (mst, total) = prim(&storage, Gid::from(1u64));
        assert_eq!(mst.len(), 2);
        assert_eq!(total, 2.0);
    }

    #[test]
    fn test_extract_edges() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let edges = extract_edges(&storage);
        assert_eq!(edges.len(), 3);
    }
}
