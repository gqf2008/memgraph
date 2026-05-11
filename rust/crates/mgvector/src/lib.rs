#![allow(unused)]
//! # mgvector — Pure Rust HNSW vector index (replaces C++ usearch)
//!
//! Hierarchical Navigable Small World (HNSW) for approximate nearest neighbor search.
//! No C/C++ dependencies.
//!
//! Features:
//! - Cosine, Euclidean, Dot, Manhattan distance metrics
//! - Batch insert for efficient bulk loading
//! - Save/load index to disk
//! - Brute-force fallback for small indices
//! - Parallel search with rayon
//! - Vector normalization helpers

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;

// ─── Distance functions ────────────────────────────────────────────────────

/// Distance metric for vector comparison.
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum Distance {
    /// Cosine distance: 1 - cos(a, b). Range [0, 2]. Lower is closer.
    Cosine,
    /// Euclidean (L2) distance. Range [0, ∞). Lower is closer.
    Euclidean,
    /// Inner product (for normalized vectors, equivalent to cosine).
    Dot,
    /// Manhattan (L1) distance. Range [0, ∞). Lower is closer.
    Manhattan,
    /// Hamming distance for binary vectors. Count of differing positions.
    Hamming,
}

impl Distance {
    pub fn compute(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Distance::Cosine => {
                let (dot, norm_a, norm_b) = dot_and_norms(a, b);
                if norm_a == 0.0 || norm_b == 0.0 { return 1.0; }
                1.0 - (dot / (norm_a * norm_b)).clamp(-1.0, 1.0)
            }
            Distance::Euclidean => {
                a.iter().zip(b).map(|(x, y)| (x - y).powi(2)).sum::<f32>().sqrt()
            }
            Distance::Dot => {
                -dot_and_norms(a, b).0 // negative for "lower is closer"
            }
            Distance::Manhattan => {
                a.iter().zip(b).map(|(x, y)| (x - y).abs()).sum()
            }
            Distance::Hamming => {
                a.iter().zip(b).filter(|(x, y)| x != y).count() as f32
            }
        }
    }

    /// Whether smaller distances mean closer vectors.
    pub fn is_minimizing(&self) -> bool {
        matches!(self, Distance::Cosine | Distance::Euclidean | Distance::Manhattan | Distance::Hamming)
    }
}

fn dot_and_norms(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    (dot, norm_a.sqrt(), norm_b.sqrt())
}

/// Normalize a vector to unit length (L2 norm = 1).
pub fn normalize(vector: &mut [f32]) {
    let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in vector.iter_mut() {
            *x /= norm;
        }
    }
}

/// Normalize a vector and return a new allocation.
pub fn normalized(vector: &[f32]) -> Vec<f32> {
    let mut v = vector.to_vec();
    normalize(&mut v);
    v
}

// ─── Index configuration ───────────────────────────────────────────────────

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HnswConfig {
    /// Max connections per node per layer.
    pub m: usize,
    /// Max layer 0 connections (typically 2 * M).
    pub m0: usize,
    /// Beam width during construction search.
    pub ef_construction: usize,
    /// Beam width during query search.
    pub ef_search: usize,
    /// Level generation multiplier (1 / ln(M)).
    pub level_multiplier: f64,
    /// Max number of elements.
    pub max_elements: usize,
    /// Use brute-force search when index has fewer than this many elements.
    pub brute_force_threshold: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        let m = 16;
        Self {
            m,
            m0: 2 * m,
            ef_construction: 200,
            ef_search: 50,
            level_multiplier: 1.0 / (m as f64).ln(),
            max_elements: 1_000_000,
            brute_force_threshold: 100,
        }
    }
}

// ─── Internal types ────────────────────────────────────────────────────────

pub type NodeId = usize;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Node {
    id: NodeId,
    vector: Vec<f32>,
    /// Max layer this node belongs to. 0 = base layer only.
    max_layer: usize,
    /// Neighbors per layer: layers[0] = base, layers[1..] = higher.
    layers: Vec<Vec<NodeId>>,
    deleted: bool,
}

/// Entry in a search result heap. Reversed for min-heap behavior.
#[derive(Clone, PartialEq)]
struct SearchEntry {
    id: NodeId,
    distance: f32,
}

impl Eq for SearchEntry {}
impl PartialOrd for SearchEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.distance.partial_cmp(&self.distance)
    }
}
impl Ord for SearchEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

// ─── HNSW Index ────────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize)]
pub struct HnswIndex {
    config: HnswConfig,
    distance: Distance,
    dimension: usize,
    nodes: HashMap<NodeId, Node>,
    next_id: NodeId,
    /// Entry point (node with highest max layer).
    entry_point: Option<NodeId>,
    /// Current max level in the graph.
    max_level: usize,
    /// Number of deleted nodes (for stats).
    deleted_count: usize,
}

impl HnswIndex {
    pub fn new(dimension: usize, distance: Distance, config: HnswConfig) -> Self {
        Self {
            config,
            distance,
            dimension,
            nodes: HashMap::new(),
            next_id: 0,
            entry_point: None,
            max_level: 0,
            deleted_count: 0,
        }
    }

    /// Insert a vector. Returns the assigned node ID.
    pub fn insert(&mut self, vector: &[f32]) -> NodeId {
        assert_eq!(vector.len(), self.dimension);
        let id = self.next_id;
        self.next_id += 1;

        let level = self.random_level();

        if self.entry_point.is_none() {
            self.entry_point = Some(id);
            self.max_level = level;
            self.nodes.insert(id, Node {
                id,
                vector: vector.to_vec(),
                max_layer: level,
                layers: vec![Vec::new(); level + 1],
                deleted: false,
            });
            return id;
        }

        let ep = self.entry_point.unwrap();
        let mut curr_ep = ep;
        let mut curr_dist = self.distance.compute(vector, &self.nodes[&ep].vector);

        // Search from top level down to level+1
        for lc in (level + 1..=self.max_level).rev() {
            let (new_ep, new_dist) = self.search_layer(vector, curr_ep, 1, lc);
            curr_ep = new_ep;
            curr_dist = new_dist;
        }

        // Insert node
        self.nodes.insert(id, Node {
            id,
            vector: vector.to_vec(),
            max_layer: level,
            layers: vec![Vec::new(); level + 1],
            deleted: false,
        });

        // Insert at each layer from level down to 0
        let m_max = self.config.m;
        for lc in (0..=level.min(self.max_level)).rev() {
            let neighbors = self.search_layer_beam(vector, curr_ep, self.config.ef_construction, lc);
            let max_conn = if lc == 0 { self.config.m0 } else { m_max };
            let selected = self.select_neighbors(&neighbors, max_conn);
            for n in &selected {
                self.nodes.get_mut(&id).unwrap().layers[lc].push(n.id);
            }
        }

        // Add reverse connections from neighbors → new node
        let node_layers: Vec<Vec<NodeId>> = self.nodes[&id].layers.clone();
        for lc in (0..=level.min(self.max_level)).rev() {
            for neighbor_id in &node_layers[lc] {
                if let Some(neighbor) = self.nodes.get_mut(neighbor_id) {
                    if neighbor.layers.len() > lc {
                        neighbor.layers[lc].push(id);
                        // Prune if too many connections
                        if neighbor.layers[lc].len() > self.config.m0 * 2 {
                            self.prune_neighbors(*neighbor_id, lc);
                        }
                    }
                }
            }
        }

        // Update entry point if this node is at a higher level
        if level > self.max_level {
            self.max_level = level;
            self.entry_point = Some(id);
        }

        id
    }

    /// Batch insert multiple vectors. More efficient than repeated `insert`.
    ///
    /// Uses a two-phase bulk-loading algorithm:
    /// 1. Create all nodes with assigned levels (no connections).
    /// 2. Build connections level-by-level from top to bottom.
    /// 3. Refinement pass at level 0 improves neighbor quality.
    ///
    /// This avoids the O(n) repeated greedy searches of sequential insert
    /// and produces a higher-quality graph for batch-loaded data.
    pub fn insert_batch(&mut self, vectors: &[Vec<f32>]) -> Vec<NodeId> {
        if vectors.is_empty() {
            return Vec::new();
        }

        // Phase 1: Create all nodes without connections.
        self.nodes.reserve(vectors.len());
        let mut new_nodes = Vec::with_capacity(vectors.len());
        for vector in vectors {
            let id = self.next_id;
            self.next_id += 1;
            let level = self.random_level();
            self.nodes.insert(id, Node {
                id,
                vector: vector.clone(),
                max_layer: level,
                layers: vec![Vec::new(); level + 1],
                deleted: false,
            });
            if level > self.max_level {
                self.max_level = level;
                self.entry_point = Some(id);
            }
            new_nodes.push((id, level));
        }

        if self.entry_point.is_none() {
            return new_nodes.into_iter().map(|(id, _)| id).collect();
        }

        // Phase 2: Build connections level by level from top to bottom.
        for lc in (0..=self.max_level).rev() {
            let nodes_at_level: Vec<NodeId> = new_nodes
                .iter()
                .filter(|(_, level)| *level >= lc)
                .map(|(id, _)| *id)
                .collect();
            if nodes_at_level.is_empty() {
                continue;
            }

            for &id in &nodes_at_level {
                let vector = &self.nodes[&id].vector;
                let ep = self.entry_point.unwrap();
                let mut curr_ep = ep;
                for higher in (lc + 1..=self.max_level).rev() {
                    let (new_ep, _) = self.search_layer(vector, curr_ep, 1, higher);
                    curr_ep = new_ep;
                }
                let neighbors = self.search_layer_beam(vector, curr_ep, self.config.ef_construction, lc);
                let max_conn = if lc == 0 { self.config.m0 } else { self.config.m };
                let selected = self.select_neighbors(&neighbors, max_conn);
                for n in &selected {
                    self.nodes.get_mut(&id).unwrap().layers[lc].push(n.id);
                }
            }

            // Reverse connections.
            for &id in &nodes_at_level {
                if lc >= self.nodes[&id].layers.len() {
                    continue;
                }
                let my_neighbors: Vec<NodeId> = self.nodes[&id].layers[lc].clone();
                for &neighbor_id in &my_neighbors {
                    if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                        if neighbor.layers.len() > lc && !neighbor.layers[lc].contains(&id) {
                            neighbor.layers[lc].push(id);
                            if neighbor.layers[lc].len() > self.config.m0 * 2 {
                                self.prune_neighbors(neighbor_id, lc);
                            }
                        }
                    }
                }
            }
        }

        // Phase 3: Refinement pass at level 0.
        // Re-search for better neighbors now that the graph is more complete.
        let level0_nodes: Vec<NodeId> = new_nodes.iter().map(|(id, _)| *id).collect();
        for &id in &level0_nodes {
            let vector = &self.nodes[&id].vector;
            let ep = self.entry_point.unwrap();
            let mut curr_ep = ep;
            for higher in (1..=self.max_level).rev() {
                let (new_ep, _) = self.search_layer(vector, curr_ep, 1, higher);
                curr_ep = new_ep;
            }
            let neighbors = self.search_layer_beam(vector, curr_ep, self.config.ef_construction, 0);
            let selected = self.select_neighbors(&neighbors, self.config.m0);
            let new_neighbors: Vec<NodeId> = selected.into_iter().map(|e| e.id).collect();
            self.nodes.get_mut(&id).unwrap().layers[0] = new_neighbors;
        }

        // Rebuild reverse connections at level 0 after refinement.
        for &id in &level0_nodes {
            if self.nodes[&id].layers.is_empty() {
                continue;
            }
            let my_neighbors: Vec<NodeId> = self.nodes[&id].layers[0].clone();
            for &neighbor_id in &my_neighbors {
                if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                    if !neighbor.layers[0].contains(&id) {
                        neighbor.layers[0].push(id);
                        if neighbor.layers[0].len() > self.config.m0 * 2 {
                            self.prune_neighbors(neighbor_id, 0);
                        }
                    }
                }
            }
        }

        // Update entry point to the highest-level node overall.
        let highest = self
            .nodes
            .values()
            .filter(|n| !n.deleted)
            .max_by_key(|n| n.max_layer)
            .map(|n| n.id);
        self.entry_point = highest;
        self.max_level = highest
            .and_then(|id| self.nodes.get(&id))
            .map_or(0, |n| n.max_layer);

        new_nodes.into_iter().map(|(id, _)| id).collect()
    }

    /// Batch insert vectors with caller-supplied IDs (e.g., Gid mapping).
    ///
    /// Unlike `insert_batch` which auto-assigns NodeIds, this method lets the
    /// caller provide external identifiers. Returns a map from the caller's
    /// ID to the internal NodeId.
    pub fn insert_batch_with_ids<T: Clone + Eq + std::hash::Hash + std::fmt::Debug>(
        &mut self,
        vectors: &[(Vec<f32>, T)],
    ) -> std::collections::HashMap<T, NodeId> {
        if vectors.is_empty() {
            return std::collections::HashMap::new();
        }

        // Phase 1: Create all nodes without connections.
        self.nodes.reserve(vectors.len());
        let mut new_nodes = Vec::with_capacity(vectors.len());
        let mut id_map = std::collections::HashMap::with_capacity(vectors.len());

        for (vector, external_id) in vectors {
            let id = self.next_id;
            self.next_id += 1;
            let level = self.random_level();
            self.nodes.insert(id, Node {
                id,
                vector: vector.clone(),
                max_layer: level,
                layers: vec![Vec::new(); level + 1],
                deleted: false,
            });
            if level > self.max_level {
                self.max_level = level;
                self.entry_point = Some(id);
            }
            new_nodes.push((id, level));
            id_map.insert(external_id.clone(), id);
        }

        if self.entry_point.is_none() {
            return id_map;
        }

        // Phase 2: Build connections level by level from top to bottom.
        for lc in (0..=self.max_level).rev() {
            let nodes_at_level: Vec<NodeId> = new_nodes
                .iter()
                .filter(|(_, level)| *level >= lc)
                .map(|(id, _)| *id)
                .collect();
            if nodes_at_level.is_empty() {
                continue;
            }

            for &id in &nodes_at_level {
                let vector = &self.nodes[&id].vector;
                let ep = self.entry_point.unwrap();
                let mut curr_ep = ep;
                for higher in (lc + 1..=self.max_level).rev() {
                    let (new_ep, _) = self.search_layer(vector, curr_ep, 1, higher);
                    curr_ep = new_ep;
                }
                let neighbors = self.search_layer_beam(vector, curr_ep, self.config.ef_construction, lc);
                let max_conn = if lc == 0 { self.config.m0 } else { self.config.m };
                let selected = self.select_neighbors(&neighbors, max_conn);
                for n in &selected {
                    self.nodes.get_mut(&id).unwrap().layers[lc].push(n.id);
                }
            }

            // Reverse connections.
            for &id in &nodes_at_level {
                if lc >= self.nodes[&id].layers.len() {
                    continue;
                }
                let my_neighbors: Vec<NodeId> = self.nodes[&id].layers[lc].clone();
                for &neighbor_id in &my_neighbors {
                    if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                        if neighbor.layers.len() > lc && !neighbor.layers[lc].contains(&id) {
                            neighbor.layers[lc].push(id);
                            if neighbor.layers[lc].len() > self.config.m0 * 2 {
                                self.prune_neighbors(neighbor_id, lc);
                            }
                        }
                    }
                }
            }
        }

        // Phase 3: Refinement pass at level 0.
        let level0_nodes: Vec<NodeId> = new_nodes.iter().map(|(id, _)| *id).collect();
        for &id in &level0_nodes {
            let vector = &self.nodes[&id].vector;
            let ep = self.entry_point.unwrap();
            let mut curr_ep = ep;
            for higher in (1..=self.max_level).rev() {
                let (new_ep, _) = self.search_layer(vector, curr_ep, 1, higher);
                curr_ep = new_ep;
            }
            let neighbors = self.search_layer_beam(vector, curr_ep, self.config.ef_construction, 0);
            let selected = self.select_neighbors(&neighbors, self.config.m0);
            let new_neighbors: Vec<NodeId> = selected.into_iter().map(|e| e.id).collect();
            self.nodes.get_mut(&id).unwrap().layers[0] = new_neighbors;
        }

        // Rebuild reverse connections at level 0 after refinement.
        for &id in &level0_nodes {
            if self.nodes[&id].layers.is_empty() {
                continue;
            }
            let my_neighbors: Vec<NodeId> = self.nodes[&id].layers[0].clone();
            for &neighbor_id in &my_neighbors {
                if let Some(neighbor) = self.nodes.get_mut(&neighbor_id) {
                    if !neighbor.layers[0].contains(&id) {
                        neighbor.layers[0].push(id);
                        if neighbor.layers[0].len() > self.config.m0 * 2 {
                            self.prune_neighbors(neighbor_id, 0);
                        }
                    }
                }
            }
        }

        // Update entry point to the highest-level node overall.
        let highest = self
            .nodes
            .values()
            .filter(|n| !n.deleted)
            .max_by_key(|n| n.max_layer)
            .map(|n| n.id);
        self.entry_point = highest;
        self.max_level = highest
            .and_then(|id| self.nodes.get(&id))
            .map_or(0, |n| n.max_layer);

        id_map
    }

    /// Search for k nearest neighbors.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(NodeId, f32)> {
        if self.entry_point.is_none() {
            return Vec::new();
        }

        let active_count = self.len();
        if active_count <= self.config.brute_force_threshold {
            return self.brute_force_search(query, k);
        }

        let mut ep = self.entry_point.unwrap();
        let mut ep_dist = self.distance.compute(query, &self.nodes[&ep].vector);

        // Descend through layers
        for lc in (1..=self.max_level).rev() {
            let (new_ep, new_dist) = self.search_layer(query, ep, 1, lc);
            ep = new_ep;
            ep_dist = new_dist;
        }

        // Search base layer with ef_search
        let results = self.search_layer_beam(query, ep, self.config.ef_search.max(k), 0);
        results.into_iter()
            .filter(|e| self.nodes.get(&e.id).map_or(false, |n| !n.deleted))
            .take(k)
            .map(|e| (e.id, e.distance))
            .collect()
    }

    /// Brute-force search (exact, O(n)). Used for small indices or verification.
    pub fn brute_force_search(&self, query: &[f32], k: usize) -> Vec<(NodeId, f32)> {
        let mut results: Vec<(NodeId, f32)> = self.nodes
            .values()
            .filter(|n| !n.deleted)
            .map(|n| (n.id, self.distance.compute(query, &n.vector)))
            .collect();
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        results.truncate(k);
        results
    }

    /// Parallel batch search. Returns results for each query.
    #[cfg(feature = "rayon")]
    pub fn search_batch(&self, queries: &[Vec<f32>], k: usize) -> Vec<Vec<(NodeId, f32)>> {
        use rayon::prelude::*;
        queries.par_iter().map(|q| self.search(q, k)).collect()
    }

    /// Mark a node as deleted.
    pub fn delete(&mut self, id: NodeId) -> bool {
        if let Some(node) = self.nodes.get_mut(&id) {
            if !node.deleted {
                node.deleted = true;
                self.deleted_count += 1;
                return true;
            }
        }
        false
    }

    /// Physically remove deleted nodes and rebuild connections.
    pub fn compact(&mut self) {
        let deleted_ids: Vec<NodeId> = self.nodes
            .values()
            .filter(|n| n.deleted)
            .map(|n| n.id)
            .collect();

        for id in deleted_ids {
            self.nodes.remove(&id);
        }

        // Rebuild all connections (simplified: clear and re-insert)
        let all_nodes: Vec<(NodeId, Vec<f32>)> = self.nodes
            .values()
            .map(|n| (n.id, n.vector.clone()))
            .collect();

        self.nodes.clear();
        self.entry_point = None;
        self.max_level = 0;
        self.next_id = 0;
        self.deleted_count = 0;

        for (_old_id, vector) in all_nodes {
            self.insert(&vector);
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.iter().filter(|(_, n)| !n.deleted).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Get the vector for a node (if not deleted).
    pub fn get(&self, id: NodeId) -> Option<&[f32]> {
        self.nodes.get(&id).filter(|n| !n.deleted).map(|n| n.vector.as_slice())
    }

    /// Get index statistics.
    pub fn stats(&self) -> IndexStats {
        let total_nodes = self.nodes.len();
        let active = self.len();
        let mut total_connections = 0usize;
        for node in self.nodes.values() {
            for layer in &node.layers {
                total_connections += layer.len();
            }
        }
        IndexStats {
            total_nodes,
            active_nodes: active,
            deleted_nodes: self.deleted_count,
            max_level: self.max_level,
            avg_connections: if total_nodes > 0 {
                total_connections as f64 / total_nodes as f64
            } else {
                0.0
            },
            dimension: self.dimension,
            distance_metric: format!("{:?}", self.distance),
        }
    }

    /// Save index to a file.
    pub fn save(&self, path: impl AsRef<std::path::Path>) -> Result<(), std::io::Error> {
        let encoded = bincode::serialize(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(path, encoded)?;
        Ok(())
    }

    /// Load index from a file.
    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self, std::io::Error> {
        let data = std::fs::read(path)?;
        let index = bincode::deserialize(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(index)
    }

    // ─── Internal ───────────────────────────────────────────────────────

    fn random_level(&self) -> usize {
        let r: f64 = rand::random::<f64>();
        (-r.ln() * self.config.level_multiplier) as usize
    }

    fn search_layer(&self, query: &[f32], entry: NodeId, _ef: usize, level: usize) -> (NodeId, f32) {
        let mut curr = entry;
        let mut curr_dist = self.distance.compute(query, &self.nodes[&entry].vector);
        let mut improved = true;

        while improved {
            improved = false;
            if let Some(neighbors) = self.nodes[&curr].layers.get(level) {
                for &n in neighbors {
                    if self.nodes[&n].deleted { continue; }
                    let n_dist = self.distance.compute(query, &self.nodes[&n].vector);
                    if n_dist < curr_dist {
                        curr = n;
                        curr_dist = n_dist;
                        improved = true;
                    }
                }
            }
        }

        (curr, curr_dist)
    }

    fn search_layer_beam(&self, query: &[f32], entry: NodeId, ef: usize, level: usize,
    ) -> Vec<SearchEntry> {
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut candidates: BinaryHeap<SearchEntry> = BinaryHeap::new();
        let mut results: BinaryHeap<SearchEntry> = BinaryHeap::new();

        let dist = self.distance.compute(query, &self.nodes[&entry].vector);
        candidates.push(SearchEntry { id: entry, distance: dist });
        results.push(SearchEntry { id: entry, distance: dist });
        visited.insert(entry);

        while let Some(c) = candidates.pop() {
            if let Some(worst) = results.peek() {
                if results.len() >= ef && c.distance > worst.distance {
                    break;
                }
            }

            if let Some(neighbors) = self.nodes[&c.id].layers.get(level) {
                for &n in neighbors {
                    if visited.insert(n) && !self.nodes[&n].deleted {
                        let n_dist = self.distance.compute(query, &self.nodes[&n].vector);
                        if results.len() < ef || n_dist < results.peek().unwrap().distance {
                            candidates.push(SearchEntry { id: n, distance: n_dist });
                            results.push(SearchEntry { id: n, distance: n_dist });
                            if results.len() > ef {
                                results.pop();
                            }
                        }
                    }
                }
            }
        }

        let mut r: Vec<SearchEntry> = results.into_vec();
        r.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal));
        r
    }

    fn select_neighbors(&self, candidates: &[SearchEntry], max_conn: usize,
    ) -> Vec<SearchEntry> {
        let mut sorted = candidates.to_vec();
        sorted.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal));
        sorted.truncate(max_conn);
        sorted
    }

    /// Prune excess connections from a node at a given layer.
    fn prune_neighbors(&mut self, node_id: NodeId, level: usize,
    ) {
        if let Some(node) = self.nodes.get(&node_id) {
            if node.layers.len() <= level { return; }
            if node.layers[level].len() <= self.config.m0 { return; }

            let vector = node.vector.clone();
            let neighbors: Vec<NodeId> = node.layers[level].clone();

            let mut entries: Vec<SearchEntry> = neighbors
                .iter()
                .filter_map(|nid| {
                    self.nodes.get(nid).map(|n| {
                        SearchEntry {
                            id: *nid,
                            distance: self.distance.compute(&vector, &n.vector),
                        }
                    })
                })
                .collect();

            entries.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal));
            entries.truncate(self.config.m0);

            let pruned: Vec<NodeId> = entries.into_iter().map(|e| e.id).collect();
            if let Some(node) = self.nodes.get_mut(&node_id) {
                node.layers[level] = pruned;
            }
        }
    }
}

impl fmt::Debug for HnswIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HnswIndex")
            .field("dimension", &self.dimension)
            .field("nodes", &self.nodes.len())
            .field("max_level", &self.max_level)
            .field("entry_point", &self.entry_point)
            .finish()
    }
}

/// Index statistics.
#[derive(Clone, Debug)]
pub struct IndexStats {
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub deleted_nodes: usize,
    pub max_level: usize,
    pub avg_connections: f64,
    pub dimension: usize,
    pub distance_metric: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_search() {
        let mut idx = HnswIndex::new(3, Distance::Euclidean, HnswConfig::default());
        let v1 = idx.insert(&[1.0, 0.0, 0.0]);
        let v2 = idx.insert(&[0.0, 1.0, 0.0]);
        let v3 = idx.insert(&[0.0, 0.0, 1.0]);

        assert_eq!(idx.len(), 3);
        assert_eq!(idx.dimension(), 3);

        let results = idx.search(&[0.9, 0.1, 0.0], 1);
        assert!(!results.is_empty());
        assert_eq!(results[0].0, v1);
    }

    #[test]
    fn test_cosine_distance() {
        let mut idx = HnswIndex::new(2, Distance::Cosine, HnswConfig {
            m: 4,
            m0: 8,
            ..Default::default()
        });
        idx.insert(&[1.0, 0.0]);
        idx.insert(&[0.0, 1.0]);
        idx.insert(&[0.7, 0.7]);

        let results = idx.search(&[1.0, 0.0], 2);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_delete() {
        let mut idx = HnswIndex::new(2, Distance::Euclidean, HnswConfig::default());
        let id = idx.insert(&[1.0, 2.0]);
        assert_eq!(idx.len(), 1);
        assert!(idx.delete(id));
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn test_empty_search() {
        let idx = HnswIndex::new(2, Distance::Euclidean, HnswConfig::default());
        let results = idx.search(&[1.0, 0.0], 5);
        assert!(results.is_empty());
    }

    #[test]
    fn test_dot_distance() {
        let mut idx = HnswIndex::new(2, Distance::Dot, HnswConfig {
            m: 4,
            m0: 8,
            ..Default::default()
        });
        idx.insert(&[1.0, 0.0]);
        idx.insert(&[0.0, 1.0]);
        idx.insert(&[0.5, 0.5]);

        let results = idx.search(&[1.0, 0.0], 2);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_manhattan_distance() {
        let mut idx = HnswIndex::new(2, Distance::Manhattan, HnswConfig::default());
        idx.insert(&[0.0, 0.0]);
        idx.insert(&[1.0, 1.0]);
        idx.insert(&[2.0, 2.0]);

        let results = idx.search(&[0.1, 0.1], 1);
        assert_eq!(results[0].0, 0); // closest to origin
    }

    #[test]
    fn test_multi_insert_search() {
        let mut idx = HnswIndex::new(10, Distance::Euclidean, HnswConfig {
            m: 8,
            m0: 16,
            ef_construction: 50,
            ef_search: 20,
            ..Default::default()
        });

        let mut inserted = Vec::new();
        for i in 0..100 {
            let vec: Vec<f32> = (0..10).map(|j| ((i * 7 + j * 13) % 100) as f32 / 100.0).collect();
            let id = idx.insert(&vec);
            inserted.push((id, vec));
        }

        assert_eq!(idx.len(), 100);

        let mut exact_matches = 0;
        for (id, vec) in &inserted {
            let results = idx.search(vec, 5);
            if !results.is_empty() && results[0].0 == *id {
                exact_matches += 1;
            }
        }
        assert!(exact_matches >= 30, "expected at least 30 exact matches, got {}", exact_matches);
    }

    #[test]
    fn test_get_vector() {
        let mut idx = HnswIndex::new(3, Distance::Euclidean, HnswConfig::default());
        let v = vec![1.0f32, 2.0, 3.0];
        let id = idx.insert(&v);

        let retrieved = idx.get(id).unwrap();
        assert_eq!(retrieved, v.as_slice());

        idx.delete(id);
        assert!(idx.get(id).is_none());
    }

    #[test]
    fn test_delete_and_search() {
        let mut idx = HnswIndex::new(3, Distance::Euclidean, HnswConfig::default());
        let id1 = idx.insert(&[1.0, 0.0, 0.0]);
        let id2 = idx.insert(&[0.0, 1.0, 0.0]);
        let id3 = idx.insert(&[0.0, 0.0, 1.0]);

        assert_eq!(idx.len(), 3);
        idx.delete(id2);
        assert_eq!(idx.len(), 2);

        let results = idx.search(&[0.0, 1.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert!(!results.iter().any(|(id, _)| *id == id2));
    }

    #[test]
    fn test_batch_insert() {
        let mut idx = HnswIndex::new(4, Distance::Euclidean, HnswConfig::default());
        let vectors: Vec<Vec<f32>> = (0..50)
            .map(|i| vec![i as f32, (i * 2) as f32, (i * 3) as f32, (i * 5) as f32])
            .collect();
        let ids = idx.insert_batch(&vectors);
        assert_eq!(ids.len(), 50);
        assert_eq!(idx.len(), 50);

        // Verify search quality: most vectors should find themselves as nearest neighbor
        let mut exact_matches = 0;
        for (i, vec) in vectors.iter().enumerate() {
            let results = idx.search(vec, 1);
            if !results.is_empty() && results[0].0 == ids[i] {
                exact_matches += 1;
            }
        }
        assert!(exact_matches >= 35, "expected at least 35 exact matches, got {}", exact_matches);
    }

    #[test]
    fn test_batch_insert_on_existing_index() {
        let mut idx = HnswIndex::new(4, Distance::Euclidean, HnswConfig::default());
        // Pre-populate with some vectors
        for i in 0..10 {
            idx.insert(&vec![i as f32, 0.0, 0.0, 0.0]);
        }
        // Batch insert more
        let batch: Vec<Vec<f32>> = (10..60)
            .map(|i| vec![i as f32, (i * 2) as f32, (i * 3) as f32, (i * 5) as f32])
            .collect();
        let ids = idx.insert_batch(&batch);
        assert_eq!(ids.len(), 50);
        assert_eq!(idx.len(), 60);

        // Search should find both old and new vectors
        let results = idx.search(&[5.0, 0.0, 0.0, 0.0], 3);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_brute_force_search() {
        let mut idx = HnswIndex::new(2, Distance::Euclidean, HnswConfig {
            brute_force_threshold: 1000, // always use brute force
            ..Default::default()
        });
        idx.insert(&[0.0, 0.0]);
        idx.insert(&[1.0, 1.0]);
        idx.insert(&[2.0, 2.0]);

        let results = idx.search(&[0.1, 0.1], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0); // origin is closest
    }

    #[test]
    fn test_compact() {
        let mut idx = HnswIndex::new(2, Distance::Euclidean, HnswConfig::default());
        let id1 = idx.insert(&[1.0, 0.0]);
        let id2 = idx.insert(&[0.0, 1.0]);
        let id3 = idx.insert(&[1.0, 1.0]);

        idx.delete(id2);
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.stats().deleted_nodes, 1);

        idx.compact();
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.stats().deleted_nodes, 0);

        // Should still be searchable
        let results = idx.search(&[0.0, 1.0], 2);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_stats() {
        let mut idx = HnswIndex::new(3, Distance::Cosine, HnswConfig::default());
        idx.insert(&[1.0, 0.0, 0.0]);
        idx.insert(&[0.0, 1.0, 0.0]);

        let stats = idx.stats();
        assert_eq!(stats.active_nodes, 2);
        assert_eq!(stats.dimension, 3);
        assert_eq!(stats.distance_metric, "Cosine");
        assert!(stats.avg_connections >= 0.0);
    }

    #[test]
    fn test_normalize() {
        let mut v = vec![3.0f32, 4.0];
        normalize(&mut v);
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5, "norm = {}", norm);
    }

    #[test]
    fn test_save_load() {
        let tmp = format!("/tmp/mgvector_test_save_{}", std::process::id());
        {
            let mut idx = HnswIndex::new(3, Distance::Euclidean, HnswConfig::default());
            idx.insert(&[1.0, 0.0, 0.0]);
            idx.insert(&[0.0, 1.0, 0.0]);
            idx.save(&tmp).unwrap();
        }

        let idx = HnswIndex::load(&tmp).unwrap();
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.dimension(), 3);

        let results = idx.search(&[0.9, 0.1, 0.0], 1);
        assert_eq!(results.len(), 1);

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_hamming_distance() {
        let mut idx = HnswIndex::new(4, Distance::Hamming, HnswConfig::default());
        idx.insert(&[0.0, 0.0, 0.0, 0.0]);
        idx.insert(&[1.0, 0.0, 0.0, 0.0]);
        idx.insert(&[1.0, 1.0, 0.0, 0.0]);

        let results = idx.search(&[1.0, 0.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        // First result should be exact match (distance 0)
        assert_eq!(results[0].1, 0.0);
    }
}
