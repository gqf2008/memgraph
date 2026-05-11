//! Graph analytics and statistics collection for the storage layer.
//!
//! Provides degree distributions, density, connected component counts,
//! and other global graph metrics.

use std::collections::{HashMap, HashSet};

use crate::storage::Storage;
use mgcore::types::{EdgeTypeId, Gid, LabelId};

/// Global graph statistics snapshot.
#[derive(Clone, Debug, Default)]
pub struct GraphStats {
    pub vertex_count: usize,
    pub edge_count: usize,
    pub avg_degree: f64,
    pub max_in_degree: usize,
    pub max_out_degree: usize,
    pub density: f64,
    pub label_distribution: HashMap<LabelId, usize>,
    pub edge_type_distribution: HashMap<EdgeTypeId, usize>,
    pub connected_component_count: usize,
    pub isolated_vertex_count: usize,
}

/// Compute global graph statistics.
pub fn compute_graph_stats(storage: &Storage) -> GraphStats {
    let all_v = storage.all_vertices();
    let n = all_v.len();
    if n == 0 {
        return GraphStats::default();
    }

    let mut edge_count = 0usize;
    let mut total_degree = 0usize;
    let mut max_in = 0usize;
    let mut max_out = 0usize;
    let mut label_dist: HashMap<LabelId, usize> = HashMap::new();
    let mut edge_type_dist: HashMap<EdgeTypeId, usize> = HashMap::new();
    let mut isolated = 0usize;

    for (gid, labels, _props) in &all_v {
        let out_deg = storage.vertex_out_degree(*gid);
        let in_deg = storage.vertex_in_degree(*gid);
        edge_count += out_deg;
        total_degree += out_deg + in_deg;
        max_in = max_in.max(in_deg);
        max_out = max_out.max(out_deg);
        if out_deg == 0 && in_deg == 0 {
            isolated += 1;
        }
        for label in labels {
            *label_dist.entry(*label).or_insert(0) += 1;
        }
    }

    for (_gid, _from, _to, etype, _props) in storage.all_edges() {
        *edge_type_dist.entry(etype).or_insert(0) += 1;
    }

    let density = if n > 1 {
        edge_count as f64 / (n * (n - 1)) as f64
    } else {
        0.0
    };

    let components = count_weak_components(storage);

    GraphStats {
        vertex_count: n,
        edge_count,
        avg_degree: total_degree as f64 / n as f64,
        max_in_degree: max_in,
        max_out_degree: max_out,
        density,
        label_distribution: label_dist,
        edge_type_distribution: edge_type_dist,
        connected_component_count: components,
        isolated_vertex_count: isolated,
    }
}

fn count_weak_components(storage: &Storage) -> usize {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut visited = HashSet::new();
    let mut count = 0usize;

    for start in &all {
        if visited.contains(start) {
            continue;
        }
        count += 1;
        let mut stack = vec![*start];
        while let Some(current) = stack.pop() {
            if !visited.insert(current) {
                continue;
            }
            for (_, other, _) in storage.vertex_out_edges(current, None) {
                if !visited.contains(&other) {
                    stack.push(other);
                }
            }
            for (_, other, _) in storage.vertex_in_edges(current, None) {
                if !visited.contains(&other) {
                    stack.push(other);
                }
            }
        }
    }

    count
}

/// Degree distribution histogram.
pub fn degree_histogram(storage: &Storage, max_bucket: usize) -> Vec<(usize, usize)> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut hist: HashMap<usize, usize> = HashMap::new();

    for gid in &all {
        let deg = storage.vertex_out_degree(*gid) + storage.vertex_in_degree(*gid);
        let bucket = deg.min(max_bucket);
        *hist.entry(bucket).or_insert(0) += 1;
    }

    let mut result: Vec<_> = hist.into_iter().collect();
    result.sort_by_key(|(k, _)| *k);
    result
}

/// Power-law exponent estimate (linear regression on log-log degree distribution).
pub fn power_law_exponent(storage: &Storage) -> Option<f64> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut degree_counts: HashMap<usize, usize> = HashMap::new();

    for gid in &all {
        let deg = storage.vertex_out_degree(*gid) + storage.vertex_in_degree(*gid);
        if deg > 0 {
            *degree_counts.entry(deg).or_insert(0) += 1;
        }
    }

    if degree_counts.len() < 2 {
        return None;
    }

    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_x2 = 0.0;
    let n = degree_counts.len() as f64;

    for (&deg, &count) in &degree_counts {
        let x = (deg as f64).ln();
        let y = (count as f64).ln();
        sum_x += x;
        sum_y += y;
        sum_xy += x * y;
        sum_x2 += x * x;
    }

    let denom = n * sum_x2 - sum_x * sum_x;
    if denom.abs() < 1e-10 {
        return None;
    }

    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    Some(-slope)
}

/// Reciprocity: fraction of edges that are reciprocated (bidirectional).
/// For a directed graph, returns the ratio of bidirectional edge pairs to total edges.
pub fn reciprocity(storage: &Storage) -> f64 {
    let edges = storage.all_edges();
    let m = edges.len();
    if m == 0 {
        return 0.0;
    }

    let edge_set: HashSet<(Gid, Gid)> = edges
        .iter()
        .map(|(_, from, to, _, _)| (*from, *to))
        .collect();

    let mut reciprocal = 0usize;
    for (_, from, to, _, _) in &edges {
        if edge_set.contains(&(*to, *from)) {
            reciprocal += 1;
        }
    }
    reciprocal as f64 / m as f64
}

/// Average clustering coefficient across all vertices (undirected).
pub fn average_clustering_coefficient(storage: &Storage) -> f64 {
    let all_v = storage.all_vertices();
    let mut total_cc = 0.0;
    let mut count = 0usize;

    for (gid, _, _) in &all_v {
        let mut neighbors = HashSet::new();
        for (_, other, _) in storage.vertex_out_edges(*gid, None) {
            neighbors.insert(other);
        }
        for (_, other, _) in storage.vertex_in_edges(*gid, None) {
            neighbors.insert(other);
        }
        let n = neighbors.len();
        if n < 2 {
            continue;
        }
        let neighbor_vec: Vec<Gid> = neighbors.iter().copied().collect();
        let mut edges_between = 0usize;
        for i in 0..neighbor_vec.len() {
            for j in (i + 1)..neighbor_vec.len() {
                let a = neighbor_vec[i];
                let b = neighbor_vec[j];
                let mut connected = false;
                for (_, target, _) in storage.vertex_out_edges(a, None) {
                    if target == b {
                        connected = true;
                        break;
                    }
                }
                if !connected {
                    for (_, target, _) in storage.vertex_in_edges(a, None) {
                        if target == b {
                            connected = true;
                            break;
                        }
                    }
                }
                if connected {
                    edges_between += 1;
                }
            }
        }
        let possible = n * (n - 1) / 2;
        total_cc += edges_between as f64 / possible as f64;
        count += 1;
    }

    if count == 0 {
        0.0
    } else {
        total_cc / count as f64
    }
}

/// Triangle density: number of triangles per possible triple of vertices.
pub fn triangle_density(storage: &Storage) -> f64 {
    let n = storage.vertex_count();
    if n < 3 {
        return 0.0;
    }
    let possible = n * (n - 1) * (n - 2) / 6;
    if possible == 0 {
        return 0.0;
    }

    let mut triangles = 0usize;
    let all_v = storage.all_vertices();
    for (gid, _, _) in &all_v {
        let mut neighbors = Vec::new();
        for (_, other, _) in storage.vertex_out_edges(*gid, None) {
            neighbors.push(other);
        }
        for (_, other, _) in storage.vertex_in_edges(*gid, None) {
            neighbors.push(other);
        }
        let neighbor_set: HashSet<Gid> = neighbors.iter().copied().collect();
        for i in 0..neighbors.len() {
            for j in (i + 1)..neighbors.len() {
                let a = neighbors[i];
                let b = neighbors[j];
                if a == b {
                    continue;
                }
                if neighbor_set.contains(&a) && neighbor_set.contains(&b) {
                    // Check if a and b are connected
                    let mut connected = false;
                    for (_, target, _) in storage.vertex_out_edges(a, None) {
                        if target == b {
                            connected = true;
                            break;
                        }
                    }
                    if !connected {
                        for (_, target, _) in storage.vertex_in_edges(a, None) {
                            if target == b {
                                connected = true;
                                break;
                            }
                        }
                    }
                    if connected {
                        triangles += 1;
                    }
                }
            }
        }
    }
    // Each triangle counted 3 times (once per vertex)
    (triangles / 3) as f64 / possible as f64
}

/// In-degree and out-degree correlation (Pearson).
pub fn degree_correlation(storage: &Storage) -> f64 {
    let all_v: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let n = all_v.len();
    if n == 0 {
        return 0.0;
    }

    let mut sum_in = 0.0;
    let mut sum_out = 0.0;
    let mut sum_in_out = 0.0;
    let mut sum_in2 = 0.0;
    let mut sum_out2 = 0.0;

    for gid in &all_v {
        let in_d = storage.vertex_in_degree(*gid) as f64;
        let out_d = storage.vertex_out_degree(*gid) as f64;
        sum_in += in_d;
        sum_out += out_d;
        sum_in_out += in_d * out_d;
        sum_in2 += in_d * in_d;
        sum_out2 += out_d * out_d;
    }

    let n_f = n as f64;
    let num = n_f * sum_in_out - sum_in * sum_out;
    let den = ((n_f * sum_in2 - sum_in * sum_in) * (n_f * sum_out2 - sum_out * sum_out)).sqrt();
    if den == 0.0 {
        0.0
    } else {
        num / den
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use mgcore::delta::IsolationLevel;
    use mgcore::types::{EdgeTypeId, Gid, LabelId};

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
    fn test_compute_graph_stats() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let stats = compute_graph_stats(&storage);
        assert_eq!(stats.vertex_count, 3);
        assert_eq!(stats.edge_count, 3);
        assert_eq!(stats.isolated_vertex_count, 0);
        assert_eq!(stats.connected_component_count, 1);
    }

    #[test]
    fn test_degree_histogram() {
        let storage = Storage::new();
        // Star: center has degree 4, leaves have degree 1
        build_graph(&storage, &[(1, 2), (1, 3), (1, 4), (1, 5)]);
        let hist = degree_histogram(&storage, 10);
        // Leaves have out-degree 0 + in-degree 1 = 1; center has out-degree 4 + in-degree 0 = 4
        let bucket_1 = hist
            .iter()
            .find(|(k, _)| *k == 1)
            .map(|(_, v)| *v)
            .unwrap_or(0);
        let bucket_4 = hist
            .iter()
            .find(|(k, _)| *k == 4)
            .map(|(_, v)| *v)
            .unwrap_or(0);
        assert_eq!(bucket_1, 4);
        assert_eq!(bucket_4, 1);
    }

    #[test]
    fn test_power_law_exponent() {
        let storage = Storage::new();
        // Star graph: center degree 4, leaves degree 1 → two distinct degrees
        build_graph(&storage, &[(1, 2), (1, 3), (1, 4), (1, 5)]);
        let exp = power_law_exponent(&storage);
        assert!(exp.is_some());
    }

    #[test]
    fn test_reciprocity() {
        let storage = Storage::new();
        // Triangle: all edges reciprocated (each direction exists)
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (2, 1), (3, 2), (1, 3)]);
        let r = reciprocity(&storage);
        assert_eq!(r, 1.0);

        // Line: no reciprocated edges
        let storage2 = Storage::new();
        build_graph(&storage2, &[(1, 2), (2, 3)]);
        assert_eq!(reciprocity(&storage2), 0.0);
    }

    #[test]
    fn test_average_clustering_coefficient() {
        let storage = Storage::new();
        // Triangle: all CC = 1.0, avg = 1.0
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let acc = average_clustering_coefficient(&storage);
        assert!((acc - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_triangle_density() {
        let storage = Storage::new();
        // Triangle on 3 nodes: 1 triangle / 1 possible triple = 1.0
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let td = triangle_density(&storage);
        assert!((td - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_degree_correlation() {
        let storage = Storage::new();
        // Regular graph where in-degree == out-degree for all nodes
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let corr = degree_correlation(&storage);
        // In a directed cycle, each node has in-degree 1 and out-degree 1
        assert!(corr.is_finite());
    }
}
