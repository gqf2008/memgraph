//! Graph algorithms: shortest path, centrality, PageRank, traversals, similarity, community detection.
//! Uses public Storage API (no private field access).

pub mod articulation_points;
pub mod centrality;
pub mod clique;
pub mod coloring;
pub mod community;
pub mod embedding;
pub mod hits;
pub mod k_core;
pub mod katz;
pub mod label_propagation;
pub mod link_prediction;
pub mod louvain;
pub mod mst;
pub mod pagerank;
pub mod path;
pub mod random_walk;
pub mod shortest_path;
pub mod similarity;
pub mod similarity_advanced;
pub mod traversal;

use std::collections::{HashMap, HashSet, VecDeque};

use mgcore::types::Gid;
use mgstorage::storage::Storage;

pub fn bfs(storage: &Storage, start: Gid) -> Vec<Gid> {
    let mut visited = Vec::new();
    let mut queue = VecDeque::new();
    queue.push_back(start);
    while let Some(current) = queue.pop_front() {
        if visited.contains(&current) {
            continue;
        }
        visited.push(current);
        let _degree_in = storage.vertex_in_degree(current);
        let _degree_out = storage.vertex_out_degree(current);
        for (_edge_gid, other, _etype) in storage.vertex_out_edges(current, None) {
            if !visited.contains(&other) {
                queue.push_back(other);
            }
        }
        for (_edge_gid, other, _etype) in storage.vertex_in_edges(current, None) {
            if !visited.contains(&other) {
                queue.push_back(other);
            }
        }
    }
    visited
}

pub fn pagerank(
    storage: &Storage,
    damping: f64,
    max_iter: usize,
    epsilon: f64,
) -> HashMap<Gid, f64> {
    let all_v: Vec<(Gid, Vec<_>, _)> = storage.all_vertices();
    let gids: Vec<Gid> = all_v.iter().map(|(g, _, _)| *g).collect();
    let n = gids.len() as f64;
    if n == 0.0 {
        return HashMap::new();
    }

    let out_deg: HashMap<Gid, usize> = gids
        .iter()
        .map(|g| (*g, storage.vertex_out_degree(*g).max(1)))
        .collect();

    let in_neighbors: HashMap<Gid, Vec<Gid>> = gids
        .iter()
        .map(|g| {
            (
                *g,
                storage
                    .vertex_in_edges(*g, None)
                    .into_iter()
                    .map(|(_, other, _)| other)
                    .collect(),
            )
        })
        .collect();

    let mut rank: HashMap<Gid, f64> = gids.iter().map(|g| (*g, 1.0 / n)).collect();
    let mut new_rank: HashMap<Gid, f64> = HashMap::new();

    for _ in 0..max_iter {
        for gid in &gids {
            let mut sum = 0.0;
            if let Some(neighbors) = in_neighbors.get(gid) {
                for neighbor in neighbors {
                    let out_d = out_deg.get(neighbor).copied().unwrap_or(1) as f64;
                    sum += damping * rank.get(neighbor).copied().unwrap_or(0.0) / out_d;
                }
            }
            sum += (1.0 - damping) / n;
            new_rank.insert(*gid, sum);
        }
        let delta: f64 = gids.iter().map(|g| (new_rank[g] - rank[g]).abs()).sum();
        std::mem::swap(&mut rank, &mut new_rank);
        if delta < epsilon {
            break;
        }
    }
    rank
}

pub fn triangle_count(storage: &Storage) -> usize {
    let all_v = storage.all_vertices();
    let mut count = 0usize;
    for (gid, _labels, _props) in &all_v {
        let neighbors: HashSet<Gid> = storage
            .vertex_out_edges(*gid, None)
            .into_iter()
            .map(|(_, other, _)| other)
            .chain(
                storage
                    .vertex_in_edges(*gid, None)
                    .into_iter()
                    .map(|(_, other, _)| other),
            )
            .collect();
        for n in &neighbors {
            if *n <= *gid {
                continue;
            }
            let n2: HashSet<Gid> = storage
                .vertex_out_edges(*n, None)
                .into_iter()
                .map(|(_, other, _)| other)
                .chain(
                    storage
                        .vertex_in_edges(*n, None)
                        .into_iter()
                        .map(|(_, other, _)| other),
                )
                .collect();
            let common = neighbors.intersection(&n2).count();
            count += common;
        }
    }
    count / 3 // each triangle counted 3 times (once per vertex)
}

/// BFS shortest path from `start` to `target`. Returns the path as a Vec of Gids
/// (including both start and target), or `None` if no path exists.
pub fn shortest_path(storage: &Storage, start: Gid, target: Gid) -> Option<Vec<Gid>> {
    if start == target {
        return Some(vec![start]);
    }
    let mut visited: HashMap<Gid, Gid> = HashMap::new();
    let mut queue: VecDeque<Gid> = VecDeque::new();
    queue.push_back(start);
    visited.insert(start, start);

    while let Some(current) = queue.pop_front() {
        let neighbors: Vec<Gid> = storage
            .vertex_out_edges(current, None)
            .into_iter()
            .map(|(_, other, _)| other)
            .chain(
                storage
                    .vertex_in_edges(current, None)
                    .into_iter()
                    .map(|(_, other, _)| other),
            )
            .collect();
        for next in neighbors {
            if visited.contains_key(&next) {
                continue;
            }
            visited.insert(next, current);
            if next == target {
                // Reconstruct path
                let mut path = vec![target];
                let mut node = target;
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

/// Degree centrality: return vertices sorted by degree (in + out).
pub fn degree_centrality(storage: &Storage) -> Vec<(Gid, usize)> {
    let mut result: Vec<(Gid, usize)> = storage
        .all_vertices()
        .into_iter()
        .map(|(gid, _, _)| {
            let deg = storage.vertex_out_edges(gid, None).len()
                + storage.vertex_in_edges(gid, None).len();
            (gid, deg)
        })
        .collect();
    result.sort_by(|a, b| b.1.cmp(&a.1));
    result
}

/// Weakly connected components. Returns a map from Gid to component id.
pub fn wcc(storage: &Storage) -> HashMap<Gid, usize> {
    let all = storage.all_vertices();
    let mut visited: HashSet<Gid> = HashSet::new();
    let mut component_map = HashMap::new();
    let mut component_id = 0usize;

    for (start, _, _) in &all {
        if visited.contains(start) {
            continue;
        }
        let mut stack = vec![*start];
        while let Some(current) = stack.pop() {
            if visited.contains(&current) {
                continue;
            }
            visited.insert(current);
            component_map.insert(current, component_id);
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
        component_id += 1;
    }
    component_map
}

/// Local clustering coefficient for each vertex.
pub fn clustering_coefficient(storage: &Storage) -> HashMap<Gid, f64> {
    let all = storage.all_vertices();
    let mut result = HashMap::new();
    for (gid, _, _) in &all {
        let mut neighbors = HashSet::new();
        for (_, other, _) in storage.vertex_out_edges(*gid, None) {
            neighbors.insert(other);
        }
        for (_, other, _) in storage.vertex_in_edges(*gid, None) {
            neighbors.insert(other);
        }
        let n = neighbors.len();
        if n < 2 {
            result.insert(*gid, 0.0);
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
        result.insert(*gid, edges_between as f64 / possible as f64);
    }
    result
}

/// Betweenness centrality (Brandes' algorithm, unweighted, undirected).
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
                let contrib = (sv / sw) * (1.0 + delta[&w]);
                *delta.get_mut(v).unwrap() += contrib;
            }
            if w != *s {
                *cb.get_mut(&w).unwrap() += delta[&w];
            }
        }
    }

    cb
}

/// Closeness centrality for each vertex (undirected, unweighted).
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

/// Strongly connected components (Tarjan's algorithm).
/// Returns a map from Gid to component id.
pub fn scc(storage: &Storage) -> HashMap<Gid, usize> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut index = 0usize;
    let mut stack: Vec<Gid> = Vec::new();
    let mut on_stack: HashSet<Gid> = HashSet::new();
    let mut indices: HashMap<Gid, usize> = HashMap::new();
    let mut lowlinks: HashMap<Gid, usize> = HashMap::new();
    let mut sccs: Vec<Vec<Gid>> = Vec::new();

    fn strongconnect(
        v: Gid,
        storage: &Storage,
        index: &mut usize,
        stack: &mut Vec<Gid>,
        on_stack: &mut HashSet<Gid>,
        indices: &mut HashMap<Gid, usize>,
        lowlinks: &mut HashMap<Gid, usize>,
        sccs: &mut Vec<Vec<Gid>>,
    ) {
        indices.insert(v, *index);
        lowlinks.insert(v, *index);
        *index += 1;
        stack.push(v);
        on_stack.insert(v);

        for (_, w, _) in storage.vertex_out_edges(v, None) {
            if !indices.contains_key(&w) {
                strongconnect(w, storage, index, stack, on_stack, indices, lowlinks, sccs);
                let lw = lowlinks[&w];
                let lv = lowlinks.get_mut(&v).unwrap();
                *lv = (*lv).min(lw);
            } else if on_stack.contains(&w) {
                let lw = indices[&w];
                let lv = lowlinks.get_mut(&v).unwrap();
                *lv = (*lv).min(lw);
            }
        }

        if lowlinks[&v] == indices[&v] {
            let mut component = Vec::new();
            loop {
                let w = stack.pop().unwrap();
                on_stack.remove(&w);
                component.push(w);
                if w == v {
                    break;
                }
            }
            sccs.push(component);
        }
    }

    for v in &all {
        if !indices.contains_key(v) {
            strongconnect(
                *v,
                storage,
                &mut index,
                &mut stack,
                &mut on_stack,
                &mut indices,
                &mut lowlinks,
                &mut sccs,
            );
        }
    }

    let mut result = HashMap::new();
    for (cid, component) in sccs.iter().enumerate() {
        for v in component {
            result.insert(*v, cid);
        }
    }
    result
}

/// Detect if the graph contains any cycle (for directed graphs).
pub fn has_cycle(storage: &Storage) -> bool {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut visited: HashSet<Gid> = HashSet::new();
    let mut rec_stack: HashSet<Gid> = HashSet::new();

    fn dfs(
        v: Gid,
        storage: &Storage,
        visited: &mut HashSet<Gid>,
        rec_stack: &mut HashSet<Gid>,
    ) -> bool {
        visited.insert(v);
        rec_stack.insert(v);
        for (_, w, _) in storage.vertex_out_edges(v, None) {
            if !visited.contains(&w) {
                if dfs(w, storage, visited, rec_stack) {
                    return true;
                }
            } else if rec_stack.contains(&w) {
                return true;
            }
        }
        rec_stack.remove(&v);
        false
    }

    for v in &all {
        if !visited.contains(v) && dfs(*v, storage, &mut visited, &mut rec_stack) {
            return true;
        }
    }
    false
}

/// Topological sort (Kahn's algorithm). Returns None if cycle exists.
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

/// Find all bridges (cut edges) in an undirected graph.
/// Returns a list of edge pairs (u, v) that are bridges.
pub fn bridges(storage: &Storage) -> Vec<(Gid, Gid)> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut visited: HashSet<Gid> = HashSet::new();
    let mut disc: HashMap<Gid, usize> = HashMap::new();
    let mut low: HashMap<Gid, usize> = HashMap::new();
    let mut parent: HashMap<Gid, Option<Gid>> = HashMap::new();
    let mut result: Vec<(Gid, Gid)> = Vec::new();
    let mut time = 0usize;

    fn dfs(
        u: Gid,
        storage: &Storage,
        visited: &mut HashSet<Gid>,
        disc: &mut HashMap<Gid, usize>,
        low: &mut HashMap<Gid, usize>,
        parent: &mut HashMap<Gid, Option<Gid>>,
        result: &mut Vec<(Gid, Gid)>,
        time: &mut usize,
    ) {
        visited.insert(u);
        *time += 1;
        disc.insert(u, *time);
        low.insert(u, *time);

        let mut neighbors = Vec::new();
        for (_, other, _) in storage.vertex_out_edges(u, None) {
            neighbors.push(other);
        }
        for (_, other, _) in storage.vertex_in_edges(u, None) {
            neighbors.push(other);
        }

        for &v in &neighbors {
            if !visited.contains(&v) {
                parent.insert(v, Some(u));
                dfs(v, storage, visited, disc, low, parent, result, time);
                let low_v = low[&v];
                let low_u = low.get_mut(&u).unwrap();
                *low_u = (*low_u).min(low_v);
                if low_v > disc[&u] {
                    result.push((u, v));
                }
            } else if parent.get(&u) != Some(&Some(v)) {
                let disc_v = disc[&v];
                let low_u = low.get_mut(&u).unwrap();
                *low_u = (*low_u).min(disc_v);
            }
        }
    }

    for v in &all {
        if !visited.contains(v) {
            parent.insert(*v, None);
            dfs(
                *v,
                storage,
                &mut visited,
                &mut disc,
                &mut low,
                &mut parent,
                &mut result,
                &mut time,
            );
        }
    }
    result
}

/// Check if the undirected graph is bipartite.
pub fn is_bipartite(storage: &Storage) -> bool {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let mut colors: HashMap<Gid, bool> = HashMap::new();

    for start in &all {
        if colors.contains_key(start) {
            continue;
        }
        let mut queue = VecDeque::new();
        queue.push_back(*start);
        colors.insert(*start, true);

        while let Some(u) = queue.pop_front() {
            let color_u = colors[&u];
            let mut neighbors = Vec::new();
            for (_, other, _) in storage.vertex_out_edges(u, None) {
                neighbors.push(other);
            }
            for (_, other, _) in storage.vertex_in_edges(u, None) {
                neighbors.push(other);
            }
            for v in neighbors {
                if let Some(&color_v) = colors.get(&v) {
                    if color_v == color_u {
                        return false;
                    }
                } else {
                    colors.insert(v, !color_u);
                    queue.push_back(v);
                }
            }
        }
    }
    true
}

/// Graph diameter: longest shortest path between any pair of vertices (undirected, unweighted).
/// Returns 0 if fewer than 2 vertices.
pub fn diameter(storage: &Storage) -> usize {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all.len() < 2 {
        return 0;
    }
    let mut max_dist = 0usize;
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
                    max_dist = max_dist.max(d + 1);
                }
            }
        }
    }
    max_dist
}

/// Eccentricity for each vertex: maximum distance to any other vertex (undirected, unweighted).
pub fn eccentricity(storage: &Storage) -> HashMap<Gid, usize> {
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
        let mut max_dist = 0usize;
        while let Some(v) = queue.pop_front() {
            let d = visited[&v];
            max_dist = max_dist.max(d);
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
        result.insert(*s, max_dist);
    }
    result
}

/// Average path length (undirected, unweighted). Returns 0.0 if fewer than 2 vertices.
pub fn average_path_length(storage: &Storage) -> f64 {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let n = all.len();
    if n < 2 {
        return 0.0;
    }
    let mut total_dist = 0usize;
    let mut count = 0usize;
    for s in &all {
        let mut visited: HashMap<Gid, usize> = HashMap::new();
        let mut queue = VecDeque::new();
        visited.insert(*s, 0);
        queue.push_back(*s);
        while let Some(v) = queue.pop_front() {
            let d = visited[&v];
            if v != *s {
                total_dist += d;
                count += 1;
            }
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
    }
    if count == 0 {
        0.0
    } else {
        total_dist as f64 / count as f64
    }
}

/// Eigenvector centrality: vertices are important if connected to other important vertices.
/// Iterative power method until convergence.
pub fn eigenvector_centrality(
    storage: &Storage,
    max_iter: usize,
    epsilon: f64,
) -> HashMap<Gid, f64> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let n = all.len();
    if n == 0 {
        return HashMap::new();
    }

    let neighbors: HashMap<Gid, Vec<Gid>> = all
        .iter()
        .map(|g| {
            let mut nbrs = Vec::new();
            for (_, other, _) in storage.vertex_out_edges(*g, None) {
                nbrs.push(other);
            }
            for (_, other, _) in storage.vertex_in_edges(*g, None) {
                nbrs.push(other);
            }
            (*g, nbrs)
        })
        .collect();

    let mut scores: HashMap<Gid, f64> = all.iter().map(|g| (*g, 1.0 / n as f64)).collect();

    for _ in 0..max_iter {
        let mut new_scores = HashMap::new();
        let mut norm = 0.0;
        for g in &all {
            let mut sum = 0.0;
            if let Some(nbrs) = neighbors.get(g) {
                for nbr in nbrs {
                    sum += scores.get(nbr).copied().unwrap_or(0.0);
                }
            }
            new_scores.insert(*g, sum);
            norm += sum * sum;
        }
        norm = norm.sqrt();
        if norm < 1e-10 {
            break;
        }
        for g in &all {
            *new_scores.get_mut(g).unwrap() /= norm;
        }
        let delta: f64 = all.iter().map(|g| (new_scores[g] - scores[g]).abs()).sum();
        scores = new_scores;
        if delta < epsilon {
            break;
        }
    }
    scores
}

/// Graph radius: minimum eccentricity among all vertices.
/// Returns 0 for empty or single-vertex graphs.
pub fn radius(storage: &Storage) -> usize {
    let ecc = eccentricity(storage);
    if ecc.is_empty() {
        0
    } else {
        *ecc.values().min().unwrap_or(&0)
    }
}

/// Harmonic centrality: sum of reciprocal distances to all other reachable vertices.
pub fn harmonic_centrality(storage: &Storage) -> HashMap<Gid, f64> {
    let all: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let n = all.len();
    if n == 0 {
        return HashMap::new();
    }

    let mut result = HashMap::new();
    for s in &all {
        let mut visited: HashMap<Gid, usize> = HashMap::new();
        let mut queue = VecDeque::new();
        visited.insert(*s, 0);
        queue.push_back(*s);
        let mut sum = 0.0;
        while let Some(v) = queue.pop_front() {
            let d = visited[&v];
            if v != *s && d > 0 {
                sum += 1.0 / d as f64;
            }
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
        result.insert(*s, sum);
    }
    result
}

/// Graph density: ratio of actual edges to possible edges.
/// For a directed graph with n vertices, max edges = n*(n-1).
/// Returns 0.0 for graphs with fewer than 2 vertices.
pub fn graph_density(storage: &Storage) -> f64 {
    let n = storage.vertex_count();
    if n < 2 {
        return 0.0;
    }
    let m = storage.edge_count() as f64;
    let max_edges = (n * (n - 1)) as f64;
    if max_edges == 0.0 {
        0.0
    } else {
        m / max_edges
    }
}

/// Global clustering coefficient (transitivity): ratio of closed triplets to all triplets.
/// Returns 0.0 for graphs with fewer than 3 vertices.
pub fn global_clustering_coefficient(storage: &Storage) -> f64 {
    let all_v = storage.all_vertices();
    let mut closed_triplets = 0usize;
    let mut all_triplets = 0usize;
    for (gid, _, _) in &all_v {
        let mut neighbors = Vec::new();
        for (_, other, _) in storage.vertex_out_edges(*gid, None) {
            neighbors.push(other);
        }
        for (_, other, _) in storage.vertex_in_edges(*gid, None) {
            neighbors.push(other);
        }
        let k = neighbors.len();
        if k < 2 {
            continue;
        }
        all_triplets += k * (k - 1) / 2;
        let _neighbor_set: HashSet<Gid> = neighbors.iter().copied().collect();
        for i in 0..neighbors.len() {
            for j in (i + 1)..neighbors.len() {
                let a = neighbors[i];
                let b = neighbors[j];
                if a == b {
                    continue;
                }
                // Check if a and b are connected in either direction
                let mut connected = false;
                for (_, other, _) in storage.vertex_out_edges(a, None) {
                    if other == b {
                        connected = true;
                        break;
                    }
                }
                if !connected {
                    for (_, other, _) in storage.vertex_in_edges(a, None) {
                        if other == b {
                            connected = true;
                            break;
                        }
                    }
                }
                if connected {
                    closed_triplets += 1;
                }
            }
        }
    }
    if all_triplets == 0 {
        0.0
    } else {
        closed_triplets as f64 / all_triplets as f64
    }
}

/// K-core decomposition: returns the coreness (maximum k for which vertex belongs to k-core)
/// for each vertex. A k-core is a maximal subgraph where every vertex has degree >= k.
pub fn coreness(storage: &Storage) -> HashMap<Gid, usize> {
    let all_v: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all_v.is_empty() {
        return HashMap::new();
    }

    // Compute initial degrees (undirected: in + out, counting unique neighbors)
    let mut degrees: HashMap<Gid, HashSet<Gid>> = HashMap::new();
    for &gid in &all_v {
        let mut neighbors = HashSet::new();
        for (_, other, _) in storage.vertex_out_edges(gid, None) {
            neighbors.insert(other);
        }
        for (_, other, _) in storage.vertex_in_edges(gid, None) {
            neighbors.insert(other);
        }
        degrees.insert(gid, neighbors);
    }

    let mut coreness_map: HashMap<Gid, usize> = HashMap::new();
    let mut remaining: HashSet<Gid> = all_v.iter().copied().collect();
    let mut current_k = 0usize;

    while !remaining.is_empty() {
        let mut changed = true;
        while changed {
            changed = false;
            let to_remove: Vec<Gid> = remaining
                .iter()
                .filter(|&&gid| degrees.get(&gid).map(|n| n.len()).unwrap_or(0) <= current_k)
                .copied()
                .collect();
            for gid in to_remove {
                if !remaining.contains(&gid) {
                    continue;
                }
                remaining.remove(&gid);
                coreness_map.insert(gid, current_k);
                changed = true;
                // Reduce degree of neighbors
                let neighbors = degrees.remove(&gid).unwrap_or_default();
                for neighbor in neighbors {
                    if remaining.contains(&neighbor) {
                        if let Some(set) = degrees.get_mut(&neighbor) {
                            set.remove(&gid);
                        }
                    }
                }
            }
        }
        current_k += 1;
    }

    coreness_map
}

/// Degree assortativity: Pearson correlation coefficient of degrees at either end of edges.
/// Returns 0.0 for graphs with fewer than 2 edges.
pub fn degree_assortativity(storage: &Storage) -> f64 {
    let edges = storage.all_edges();
    let m = edges.len();
    if m < 2 {
        return 0.0;
    }

    let mut sum_xy = 0.0f64;
    let mut sum_x = 0.0f64;
    let mut sum_y = 0.0f64;
    let mut sum_x2 = 0.0f64;
    let mut sum_y2 = 0.0f64;

    for (_, from, to, _, _) in &edges {
        let deg_from = storage.vertex_out_degree(*from) + storage.vertex_in_degree(*from);
        let deg_to = storage.vertex_out_degree(*to) + storage.vertex_in_degree(*to);
        let dx = deg_from as f64;
        let dy = deg_to as f64;
        sum_xy += dx * dy;
        sum_x += dx;
        sum_y += dy;
        sum_x2 += dx * dx;
        sum_y2 += dy * dy;
    }

    let m_f = m as f64;
    let num = m_f * sum_xy - sum_x * sum_y;
    let den = ((m_f * sum_x2 - sum_x * sum_x) * (m_f * sum_y2 - sum_y * sum_y)).sqrt();
    if den == 0.0 {
        0.0
    } else {
        num / den
    }
}

/// Rich-club coefficient for a given degree threshold k.
/// Measures the fraction of edges that exist between nodes with degree > k,
/// compared to the maximum possible edges between them.
/// Returns a map from k to rich-club coefficient.
pub fn rich_club_coefficient(storage: &Storage, max_k: usize) -> HashMap<usize, f64> {
    let all_v: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all_v.is_empty() {
        return HashMap::new();
    }

    let degrees: HashMap<Gid, usize> = all_v
        .iter()
        .map(|&g| {
            let mut neighbors = HashSet::new();
            for (_, other, _) in storage.vertex_out_edges(g, None) {
                neighbors.insert(other);
            }
            for (_, other, _) in storage.vertex_in_edges(g, None) {
                neighbors.insert(other);
            }
            (g, neighbors.len())
        })
        .collect();

    let max_deg = degrees.values().copied().max().unwrap_or(0);
    let mut result = HashMap::new();

    for k in 0..=max_k.min(max_deg) {
        let rich_nodes: Vec<Gid> = all_v
            .iter()
            .filter(|&&g| degrees.get(&g).copied().unwrap_or(0) > k)
            .copied()
            .collect();
        let n = rich_nodes.len();
        if n < 2 {
            result.insert(k, 0.0);
            continue;
        }
        let rich_set: HashSet<Gid> = rich_nodes.iter().copied().collect();
        let mut actual_edges = 0usize;
        for &g in &rich_nodes {
            for (_, other, _) in storage.vertex_out_edges(g, None) {
                if rich_set.contains(&other) && other != g {
                    actual_edges += 1;
                }
            }
        }
        let max_possible = n * (n - 1);
        let phi = if max_possible == 0 {
            0.0
        } else {
            actual_edges as f64 / max_possible as f64
        };
        result.insert(k, phi);
    }

    result
}

/// Small-world coefficient (sigma): ratio of clustering coefficient ratio to path length ratio,
/// compared to an equivalent random graph. Uses a simple approximation for the random graph.
/// Returns > 1 if the graph exhibits small-world characteristics.
pub fn small_world_coefficient(storage: &Storage) -> f64 {
    let n = storage.vertex_count();
    if n < 3 {
        return 0.0;
    }
    let m = storage.edge_count();

    let cc = global_clustering_coefficient(storage);
    let apl = average_path_length(storage);
    if apl == 0.0 {
        return 0.0;
    }

    // Random graph approximations (Erdos-Renyi with same n and m)
    let p = if n > 1 {
        m as f64 / (n * (n - 1)) as f64
    } else {
        0.0
    };
    let cc_random = if n > 2 { p } else { 0.0 };
    let apl_random = if p > 0.0 && n > 1 {
        (n as f64).ln() / (n as f64 * p).ln()
    } else {
        0.0
    };

    if cc_random == 0.0 || apl_random == 0.0 {
        return 0.0;
    }
    (cc / cc_random) / (apl / apl_random)
}

/// Periphery: vertices with maximum eccentricity.
pub fn periphery(storage: &Storage) -> Vec<Gid> {
    let ecc = eccentricity(storage);
    if ecc.is_empty() {
        return Vec::new();
    }
    let max_ecc = *ecc.values().max().unwrap_or(&0);
    ecc.into_iter()
        .filter(|(_, e)| *e == max_ecc)
        .map(|(g, _)| g)
        .collect()
}

/// Center: vertices with minimum eccentricity.
pub fn center(storage: &Storage) -> Vec<Gid> {
    let ecc = eccentricity(storage);
    if ecc.is_empty() {
        return Vec::new();
    }
    let min_ecc = *ecc.values().min().unwrap_or(&0);
    ecc.into_iter()
        .filter(|(_, e)| *e == min_ecc)
        .map(|(g, _)| g)
        .collect()
}

/// Modularity of a given community partition.
/// `partition` maps each vertex Gid to a community id.
/// Returns the modularity score Q (Newman-Girvan modularity).
pub fn modularity(storage: &Storage, partition: &HashMap<Gid, usize>) -> f64 {
    let all_v: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    let n = all_v.len();
    if n < 2 {
        return 0.0;
    }
    let m = storage.edge_count() as f64;
    if m == 0.0 {
        return 0.0;
    }

    // Compute degrees
    let degrees: HashMap<Gid, usize> = all_v
        .iter()
        .map(|&g| {
            let mut deg = 0usize;
            for (_, _, _) in storage.vertex_out_edges(g, None) {
                deg += 1;
            }
            for (_, _, _) in storage.vertex_in_edges(g, None) {
                deg += 1;
            }
            (g, deg)
        })
        .collect();

    let mut q = 0.0;
    for &i in &all_v {
        for &j in &all_v {
            let same_comm = partition.get(&i) == partition.get(&j);
            if !same_comm {
                continue;
            }
            let a_ij = if has_edge(storage, i, j) { 1.0 } else { 0.0 };
            let k_i = degrees.get(&i).copied().unwrap_or(0) as f64;
            let k_j = degrees.get(&j).copied().unwrap_or(0) as f64;
            q += a_ij - (k_i * k_j) / (2.0 * m);
        }
    }
    q / (2.0 * m)
}

fn has_edge(storage: &Storage, from: Gid, to: Gid) -> bool {
    for (_, other, _) in storage.vertex_out_edges(from, None) {
        if other == to {
            return true;
        }
    }
    for (_, other, _) in storage.vertex_out_edges(to, None) {
        if other == from {
            return true;
        }
    }
    false
}

/// Conductance for each community in a partition.
/// Conductance = cut_edges / min(community_volume, rest_volume).
/// Returns a map from community id to conductance value.
pub fn conductance(storage: &Storage, partition: &HashMap<Gid, usize>) -> HashMap<usize, f64> {
    let all_v: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all_v.is_empty() {
        return HashMap::new();
    }

    // Build community sets
    let mut communities: HashMap<usize, HashSet<Gid>> = HashMap::new();
    for (&gid, &cid) in partition {
        communities.entry(cid).or_default().insert(gid);
    }

    let mut result = HashMap::new();
    for (&cid, members) in &communities {
        let mut cut_edges = 0usize;
        let mut comm_vol = 0usize;
        for &gid in members {
            for (_, other, _) in storage.vertex_out_edges(gid, None) {
                comm_vol += 1;
                if !members.contains(&other) {
                    cut_edges += 1;
                }
            }
        }
        let rest_vol = storage.edge_count().saturating_sub(comm_vol);
        let min_vol = comm_vol.min(rest_vol);
        let phi = if min_vol == 0 {
            0.0
        } else {
            cut_edges as f64 / min_vol as f64
        };
        result.insert(cid, phi);
    }
    result
}

/// Normalized cut for each community in a partition.
/// Ncut = cut_edges / comm_vol + cut_edges / rest_vol.
pub fn normalized_cut(storage: &Storage, partition: &HashMap<Gid, usize>) -> HashMap<usize, f64> {
    let all_v: Vec<Gid> = storage
        .all_vertices()
        .into_iter()
        .map(|(g, _, _)| g)
        .collect();
    if all_v.is_empty() {
        return HashMap::new();
    }

    let mut communities: HashMap<usize, HashSet<Gid>> = HashMap::new();
    for (&gid, &cid) in partition {
        communities.entry(cid).or_default().insert(gid);
    }

    let total_edges = storage.edge_count();
    let mut result = HashMap::new();
    for (&cid, members) in &communities {
        let mut cut_edges = 0usize;
        let mut comm_vol = 0usize;
        for &gid in members {
            for (_, other, _) in storage.vertex_out_edges(gid, None) {
                comm_vol += 1;
                if !members.contains(&other) {
                    cut_edges += 1;
                }
            }
        }
        let rest_vol = total_edges.saturating_sub(comm_vol);
        let ncut = if comm_vol == 0 || rest_vol == 0 {
            0.0
        } else {
            cut_edges as f64 / comm_vol as f64 + cut_edges as f64 / rest_vol as f64
        };
        result.insert(cid, ncut);
    }
    result
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
    fn test_bfs() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let path = bfs(&storage, Gid::from(1u64));
        assert!(path.contains(&Gid::from(1u64)));
        assert!(path.contains(&Gid::from(3u64)));
    }

    #[test]
    fn test_pagerank() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let ranks = pagerank(&storage, 0.85, 100, 1e-6);
        assert_eq!(ranks.len(), 3);
        let total: f64 = ranks.values().sum();
        assert!((total - 1.0).abs() < 0.1, "sum={}", total);
    }

    #[test]
    fn test_triangle_count() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        assert_eq!(triangle_count(&storage), 1);
    }

    #[test]
    fn test_shortest_path() {
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
        let tx = storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage.commit_transaction(&tx);
        let path = shortest_path(&storage, Gid::from(1u64), Gid::from(2u64));
        assert_eq!(path, None);
    }

    #[test]
    fn test_degree_centrality() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (1, 3), (2, 3)]);
        let deg = degree_centrality(&storage);
        assert_eq!(deg.len(), 3);
        // Node 2 has in-degree 1 (from 1) + out-degree 1 (to 3) = 2
        // Node 1 has out-degree 2 (to 2, 3)
        // Node 3 has in-degree 2 (from 1, 2)
        assert!(deg[0].1 >= 2);
    }

    #[test]
    fn test_wcc() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (4, 5)]);
        let components = wcc(&storage);
        assert_eq!(components.len(), 5);
        assert_eq!(components[&Gid::from(1u64)], components[&Gid::from(2u64)]);
        assert_eq!(components[&Gid::from(2u64)], components[&Gid::from(3u64)]);
        assert_ne!(components[&Gid::from(3u64)], components[&Gid::from(4u64)]);
        assert_eq!(components[&Gid::from(4u64)], components[&Gid::from(5u64)]);
    }

    #[test]
    fn test_clustering_coefficient() {
        let storage = Storage::new();
        // Triangle: 1-2-3-1, plus 1-4 (dangling)
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (1, 4)]);
        let cc = clustering_coefficient(&storage);
        assert_eq!(cc.len(), 4);
        // Node 1 has neighbors {2,3,4}; edges between them: 2-3 only => 1/3
        assert!((cc[&Gid::from(1u64)] - 1.0 / 3.0).abs() < 1e-6);
        // Nodes 2,3 have neighbors {1,2,3} (closed triangle) => CC = 1.0
        assert!((cc[&Gid::from(2u64)] - 1.0).abs() < 1e-6);
        assert!((cc[&Gid::from(3u64)] - 1.0).abs() < 1e-6);
        // Node 4 has only one neighbor, CC = 0
        assert_eq!(cc[&Gid::from(4u64)], 0.0);
    }

    #[test]
    fn test_betweenness_centrality() {
        let storage = Storage::new();
        // Line graph: 1-2-3-4
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let bc = betweenness_centrality(&storage);
        assert_eq!(bc.len(), 4);
        // Middle nodes should have higher betweenness
        assert!(bc[&Gid::from(2u64)] > bc[&Gid::from(1u64)]);
        assert!(bc[&Gid::from(3u64)] > bc[&Gid::from(1u64)]);
    }

    #[test]
    fn test_closeness_centrality() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let cc = closeness_centrality(&storage);
        assert_eq!(cc.len(), 4);
        // Middle nodes should have higher closeness
        assert!(cc[&Gid::from(2u64)] > cc[&Gid::from(1u64)]);
        assert!(cc[&Gid::from(3u64)] > cc[&Gid::from(4u64)]);
    }

    #[test]
    fn test_scc() {
        let storage = Storage::new();
        // Two SCCs: {1,2,3} cycle and {4,5} cycle
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (4, 5), (5, 4)]);
        let comps = scc(&storage);
        assert_eq!(comps.len(), 5);
        assert_eq!(comps[&Gid::from(1u64)], comps[&Gid::from(2u64)]);
        assert_eq!(comps[&Gid::from(2u64)], comps[&Gid::from(3u64)]);
        assert_eq!(comps[&Gid::from(4u64)], comps[&Gid::from(5u64)]);
        assert_ne!(comps[&Gid::from(3u64)], comps[&Gid::from(4u64)]);
    }

    #[test]
    fn test_has_cycle() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        assert!(has_cycle(&storage));

        let storage2 = Storage::new();
        build_graph(&storage2, &[(1, 2), (2, 3)]);
        assert!(!has_cycle(&storage2));
    }

    #[test]
    fn test_topological_sort() {
        let storage = Storage::new();
        build_graph(&storage, &[(1, 2), (1, 3), (2, 4), (3, 4)]);
        let sorted = topological_sort(&storage).unwrap();
        assert_eq!(sorted.len(), 4);
        // 1 must come before 2, 3, 4
        let pos1 = sorted.iter().position(|g| *g == Gid::from(1u64)).unwrap();
        let pos2 = sorted.iter().position(|g| *g == Gid::from(2u64)).unwrap();
        let pos4 = sorted.iter().position(|g| *g == Gid::from(4u64)).unwrap();
        assert!(pos1 < pos2);
        assert!(pos2 < pos4);

        // Cycle -> None
        let storage2 = Storage::new();
        build_graph(&storage2, &[(1, 2), (2, 3), (3, 1)]);
        assert!(topological_sort(&storage2).is_none());
    }

    #[test]
    fn test_bridges() {
        let storage = Storage::new();
        // Line: 1-2-3, all edges are bridges
        build_graph(&storage, &[(1, 2), (2, 3)]);
        let b = bridges(&storage);
        assert_eq!(b.len(), 2);

        // Triangle: no bridges
        let storage2 = Storage::new();
        build_graph(&storage2, &[(1, 2), (2, 3), (3, 1)]);
        let b2 = bridges(&storage2);
        assert_eq!(b2.len(), 0);
    }

    #[test]
    fn test_is_bipartite() {
        let storage = Storage::new();
        // Even cycle (4 nodes): bipartite
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4), (4, 1)]);
        assert!(is_bipartite(&storage));

        // Triangle: not bipartite
        let storage2 = Storage::new();
        build_graph(&storage2, &[(1, 2), (2, 3), (3, 1)]);
        assert!(!is_bipartite(&storage2));
    }

    #[test]
    fn test_diameter() {
        let storage = Storage::new();
        // Line: 1-2-3-4, diameter = 3
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        assert_eq!(diameter(&storage), 3);

        // Triangle: diameter = 1
        let storage2 = Storage::new();
        build_graph(&storage2, &[(1, 2), (2, 3), (3, 1)]);
        assert_eq!(diameter(&storage2), 1);

        // Empty graph
        let storage3 = Storage::new();
        assert_eq!(diameter(&storage3), 0);
    }

    #[test]
    fn test_eccentricity() {
        let storage = Storage::new();
        // Line: 1-2-3-4
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let ecc = eccentricity(&storage);
        // Endpoints have eccentricity 3, middle have 2
        assert_eq!(ecc[&Gid::from(1u64)], 3);
        assert_eq!(ecc[&Gid::from(4u64)], 3);
        assert_eq!(ecc[&Gid::from(2u64)], 2);
        assert_eq!(ecc[&Gid::from(3u64)], 2);
    }

    #[test]
    fn test_average_path_length() {
        let storage = Storage::new();
        // Triangle: 3 edges, each pair distance 1, avg = 1.0
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        let avg = average_path_length(&storage);
        assert!((avg - 1.0).abs() < 1e-6);

        // Line 1-2-3: distances (1,2,1) = 4/3
        let storage2 = Storage::new();
        build_graph(&storage2, &[(1, 2), (2, 3)]);
        let avg2 = average_path_length(&storage2);
        assert!((avg2 - 4.0 / 3.0).abs() < 1e-6);
    }

    #[test]
    fn test_eigenvector_centrality() {
        let storage = Storage::new();
        // Star: center (1) connected to 2,3,4
        build_graph(&storage, &[(1, 2), (1, 3), (1, 4)]);
        let ev = eigenvector_centrality(&storage, 100, 1e-6);
        assert_eq!(ev.len(), 4);
        // Center should have higher centrality than leaves
        assert!(ev[&Gid::from(1u64)] > ev[&Gid::from(2u64)]);
    }

    #[test]
    fn test_radius() {
        let storage = Storage::new();
        // Line: 1-2-3-4, radius = 2 (center vertices 2,3)
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        assert_eq!(radius(&storage), 2);

        // Triangle: radius = 1
        let storage2 = Storage::new();
        build_graph(&storage2, &[(1, 2), (2, 3), (3, 1)]);
        assert_eq!(radius(&storage2), 1);
    }

    #[test]
    fn test_harmonic_centrality() {
        let storage = Storage::new();
        // Star: center connected to leaves
        build_graph(&storage, &[(1, 2), (1, 3), (1, 4)]);
        let hc = harmonic_centrality(&storage);
        // Center reaches 3 leaves at dist 1 = 3.0
        assert!((hc[&Gid::from(1u64)] - 3.0).abs() < 1e-6);
        // Leaf reaches center at dist 1, other leaves at dist 2 (via center)
        let leaf = hc[&Gid::from(2u64)];
        assert!((leaf - (1.0 + 0.5 + 0.5)).abs() < 1e-6);
    }

    #[test]
    fn test_graph_density() {
        let storage = Storage::new();
        assert_eq!(graph_density(&storage), 0.0);

        // Triangle: 3 edges / (3*2) = 0.5
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        assert!((graph_density(&storage) - 0.5).abs() < 1e-6);

        // Complete directed graph on 4 nodes: 12 edges / (4*3) = 1.0
        let storage2 = Storage::new();
        build_graph(
            &storage2,
            &[
                (1, 2),
                (1, 3),
                (1, 4),
                (2, 1),
                (2, 3),
                (2, 4),
                (3, 1),
                (3, 2),
                (3, 4),
                (4, 1),
                (4, 2),
                (4, 3),
            ],
        );
        assert!((graph_density(&storage2) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_global_clustering_coefficient() {
        let storage = Storage::new();
        assert_eq!(global_clustering_coefficient(&storage), 0.0);

        // Triangle: each node has 2 neighbors connected by 1 edge => 3 closed / 3 total = 1.0
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1)]);
        assert!((global_clustering_coefficient(&storage) - 1.0).abs() < 1e-6);

        // Square (no diagonals): no triangles => 0.0
        let storage2 = Storage::new();
        build_graph(&storage2, &[(1, 2), (2, 3), (3, 4), (4, 1)]);
        assert_eq!(global_clustering_coefficient(&storage2), 0.0);
    }

    #[test]
    fn test_coreness() {
        let storage = Storage::new();
        // k-core decomposition of a triangle with a dangling node
        // 1-2-3 forms a triangle (2-core), 4 attached to 1 (1-core)
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (1, 4)]);
        let c = coreness(&storage);
        assert_eq!(c[&Gid::from(1u64)], 2);
        assert_eq!(c[&Gid::from(2u64)], 2);
        assert_eq!(c[&Gid::from(3u64)], 2);
        assert_eq!(c[&Gid::from(4u64)], 1);
    }

    #[test]
    fn test_degree_assortativity() {
        let storage = Storage::new();
        assert_eq!(degree_assortativity(&storage), 0.0);

        // Path graph: degrees 1-2-2-1, edges connect high to low -> negative assortativity
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let r = degree_assortativity(&storage);
        assert!(
            r < 0.0,
            "path graph should have negative assortativity, got {}",
            r
        );

        // Regular graph (bidirectional ring): all nodes degree 2 -> assortativity ~0
        let storage2 = Storage::new();
        build_graph(
            &storage2,
            &[
                (1, 2),
                (2, 3),
                (3, 4),
                (4, 1),
                (2, 1),
                (3, 2),
                (4, 3),
                (1, 4),
            ],
        );
        let r2 = degree_assortativity(&storage2);
        assert!(
            r2.abs() < 0.5,
            "ring graph should have near-zero assortativity, got {}",
            r2
        );
    }

    #[test]
    fn test_rich_club_coefficient() {
        let storage = Storage::new();
        // Bidirectional clique of 4 nodes: all have degree 6, rich-club should be 1.0 for k=2
        build_graph(
            &storage,
            &[
                (1, 2),
                (1, 3),
                (1, 4),
                (2, 1),
                (2, 3),
                (2, 4),
                (3, 1),
                (3, 2),
                (3, 4),
                (4, 1),
                (4, 2),
                (4, 3),
            ],
        );
        let rc = rich_club_coefficient(&storage, 5);
        assert!(rc.contains_key(&2));
        assert!(
            (rc[&2] - 1.0).abs() < 1e-6,
            "clique should have phi=1.0, got {}",
            rc[&2]
        );
    }

    #[test]
    fn test_small_world_coefficient() {
        let storage = Storage::new();
        assert_eq!(small_world_coefficient(&storage), 0.0);

        // Graph with triangles (for clustering) and shortcuts (for short paths)
        // Two triangles {1,2,3} and {4,5,6} connected by shortcut 3-4
        build_graph(
            &storage,
            &[
                (1, 2),
                (2, 3),
                (3, 1),
                (2, 1),
                (3, 2),
                (1, 3),
                (4, 5),
                (5, 6),
                (6, 4),
                (5, 4),
                (6, 5),
                (4, 6),
                (3, 4),
                (4, 3),
            ],
        );
        let sigma = small_world_coefficient(&storage);
        assert!(
            sigma > 0.0,
            "small-world coefficient should be positive, got {}",
            sigma
        );
    }

    #[test]
    fn test_periphery() {
        let storage = Storage::new();
        // Line: 1-2-3-4, periphery = {1, 4}
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let p = periphery(&storage);
        assert_eq!(p.len(), 2);
        assert!(p.contains(&Gid::from(1u64)));
        assert!(p.contains(&Gid::from(4u64)));
    }

    #[test]
    fn test_center() {
        let storage = Storage::new();
        // Line: 1-2-3-4, center = {2, 3}
        build_graph(&storage, &[(1, 2), (2, 3), (3, 4)]);
        let c = center(&storage);
        assert_eq!(c.len(), 2);
        assert!(c.contains(&Gid::from(2u64)));
        assert!(c.contains(&Gid::from(3u64)));
    }

    #[test]
    fn test_modularity() {
        let storage = Storage::new();
        // Two triangles: {1,2,3} and {4,5,6}
        build_graph(&storage, &[(1, 2), (2, 3), (3, 1), (4, 5), (5, 6), (6, 4)]);
        let mut partition = HashMap::new();
        partition.insert(Gid::from(1u64), 0);
        partition.insert(Gid::from(2u64), 0);
        partition.insert(Gid::from(3u64), 0);
        partition.insert(Gid::from(4u64), 1);
        partition.insert(Gid::from(5u64), 1);
        partition.insert(Gid::from(6u64), 1);
        let q = modularity(&storage, &partition);
        assert!(
            q > 0.3,
            "two disconnected triangles should have positive modularity, got {}",
            q
        );
    }

    #[test]
    fn test_conductance() {
        let storage = Storage::new();
        // Two triangles connected by one edge: 3-4
        build_graph(
            &storage,
            &[(1, 2), (2, 3), (3, 1), (3, 4), (4, 5), (5, 6), (6, 4)],
        );
        let mut partition = HashMap::new();
        partition.insert(Gid::from(1u64), 0);
        partition.insert(Gid::from(2u64), 0);
        partition.insert(Gid::from(3u64), 0);
        partition.insert(Gid::from(4u64), 1);
        partition.insert(Gid::from(5u64), 1);
        partition.insert(Gid::from(6u64), 1);
        let cond = conductance(&storage, &partition);
        assert!(cond.contains_key(&0));
        assert!(cond.contains_key(&1));
        // Conductance should be positive (one edge crossing)
        assert!(*cond.get(&0).unwrap() > 0.0);
    }

    #[test]
    fn test_normalized_cut() {
        let storage = Storage::new();
        // Two triangles connected by one edge
        build_graph(
            &storage,
            &[(1, 2), (2, 3), (3, 1), (3, 4), (4, 5), (5, 6), (6, 4)],
        );
        let mut partition = HashMap::new();
        partition.insert(Gid::from(1u64), 0);
        partition.insert(Gid::from(2u64), 0);
        partition.insert(Gid::from(3u64), 0);
        partition.insert(Gid::from(4u64), 1);
        partition.insert(Gid::from(5u64), 1);
        partition.insert(Gid::from(6u64), 1);
        let nc = normalized_cut(&storage, &partition);
        assert!(nc.contains_key(&0));
        assert!(nc.contains_key(&1));
        assert!(*nc.get(&0).unwrap() > 0.0);
    }
}
