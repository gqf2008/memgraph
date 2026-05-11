//! Built-in query procedures exposed via the Cypher `CALL` clause.
//!
//! Equivalent to C++ built-in procedures in `query_modules/`.
//! Procedures are registered in a `ProcedureRegistry` and invoked by name.

use std::collections::HashMap;

use mgcore::property_value::PropertyValue;
use mgcore::types::Gid;
use mgstorage::storage::Storage;

/// A built-in procedure: takes arguments and storage, returns rows.
pub type BuiltInProc = fn(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String>;

/// Registry of built-in procedures.
pub struct ProcedureRegistry {
    procs: HashMap<String, BuiltInProc>,
}

impl ProcedureRegistry {
    pub fn new() -> Self {
        let mut procs = HashMap::new();
        procs.insert("db.labels".to_string(), db_labels as BuiltInProc);
        procs.insert("db.relationshipTypes".to_string(), db_relationship_types as BuiltInProc);
        procs.insert("db.schema".to_string(), db_schema as BuiltInProc);
        procs.insert("db.schema.nodeTypeProperties".to_string(), db_schema_node_type_properties as BuiltInProc);
        procs.insert("db.schema.relTypeProperties".to_string(), db_schema_rel_type_properties as BuiltInProc);
        procs.insert("db.stats".to_string(), db_stats as BuiltInProc);
        procs.insert("db.indexes".to_string(), db_indexes as BuiltInProc);
        procs.insert("db.constraints".to_string(), db_constraints as BuiltInProc);
        procs.insert("algo.pagerank".to_string(), algo_pagerank as BuiltInProc);
        procs.insert("algo.wcc".to_string(), algo_wcc as BuiltInProc);
        procs.insert("algo.bfs".to_string(), algo_bfs as BuiltInProc);
        procs.insert("algo.triangle_count".to_string(), algo_triangle_count as BuiltInProc);
        procs.insert("algo.betweenness_centrality".to_string(), algo_betweenness_centrality as BuiltInProc);
        procs.insert("algo.closeness_centrality".to_string(), algo_closeness_centrality as BuiltInProc);
        procs.insert("algo.eigenvector_centrality".to_string(), algo_eigenvector_centrality as BuiltInProc);
        procs.insert("algo.shortest_path".to_string(), algo_shortest_path as BuiltInProc);
        procs.insert("algo.clustering_coefficient".to_string(), algo_clustering_coefficient as BuiltInProc);
        procs.insert("algo.scc".to_string(), algo_scc as BuiltInProc);
        procs.insert("algo.topological_sort".to_string(), algo_topological_sort as BuiltInProc);
        procs.insert("algo.degree_centrality".to_string(), algo_degree_centrality as BuiltInProc);
        procs.insert("algo.bridges".to_string(), algo_bridges as BuiltInProc);
        procs.insert("algo.is_bipartite".to_string(), algo_is_bipartite as BuiltInProc);
        procs.insert("algo.diameter".to_string(), algo_diameter as BuiltInProc);
        procs.insert("algo.graph_density".to_string(), algo_graph_density as BuiltInProc);
        procs.insert("algo.has_cycle".to_string(), algo_has_cycle as BuiltInProc);
        procs.insert("algo.harmonic_centrality".to_string(), algo_harmonic_centrality as BuiltInProc);
        procs.insert("algo.radius".to_string(), algo_radius as BuiltInProc);
        procs.insert("algo.eccentricity".to_string(), algo_eccentricity as BuiltInProc);
        procs.insert("algo.average_path_length".to_string(), algo_average_path_length as BuiltInProc);
        procs.insert("algo.coreness".to_string(), algo_coreness as BuiltInProc);
        procs.insert("algo.degree_assortativity".to_string(), algo_degree_assortativity as BuiltInProc);
        procs.insert("algo.global_clustering_coefficient".to_string(), algo_global_clustering_coefficient as BuiltInProc);
        procs.insert("algo.center".to_string(), algo_center as BuiltInProc);
        procs.insert("algo.periphery".to_string(), algo_periphery as BuiltInProc);
        procs.insert("algo.small_world_coefficient".to_string(), algo_small_world_coefficient as BuiltInProc);
        procs.insert("algo.articulation_points".to_string(), algo_articulation_points as BuiltInProc);
        procs.insert("algo.is_biconnected".to_string(), algo_is_biconnected as BuiltInProc);
        procs.insert("algo.connected_components".to_string(), algo_connected_components as BuiltInProc);
        procs.insert("algo.cycle_detection".to_string(), algo_cycle_detection as BuiltInProc);
        procs.insert("algo.all_pairs_shortest_path".to_string(), algo_all_pairs_shortest_path as BuiltInProc);
        procs.insert("algo.maximal_cliques".to_string(), algo_maximal_cliques as BuiltInProc);
        procs.insert("algo.clique_number".to_string(), algo_clique_number as BuiltInProc);
        procs.insert("algo.greedy_coloring".to_string(), algo_greedy_coloring as BuiltInProc);
        procs.insert("algo.dsatur_coloring".to_string(), algo_dsatur_coloring as BuiltInProc);
        procs.insert("algo.degeneracy".to_string(), algo_degeneracy as BuiltInProc);
        procs.insert("algo.predict_links".to_string(), algo_predict_links as BuiltInProc);
        procs.insert("algo.rich_club_coefficient".to_string(), algo_rich_club_coefficient as BuiltInProc);
        procs.insert("algo.katz_centrality".to_string(), algo_katz_centrality as BuiltInProc);
        procs.insert("algo.label_propagation".to_string(), algo_label_propagation as BuiltInProc);
        procs.insert("algo.louvain".to_string(), algo_louvain as BuiltInProc);
        procs.insert("algo.hits".to_string(), algo_hits as BuiltInProc);
        procs.insert("algo.core_decomposition".to_string(), algo_core_decomposition as BuiltInProc);
        procs.insert("algo.k_core".to_string(), algo_k_core as BuiltInProc);
        procs.insert("algo.floyd_warshall".to_string(), algo_floyd_warshall as BuiltInProc);
        procs.insert("algo.modularity".to_string(), algo_modularity as BuiltInProc);
        procs.insert("algo.conductance".to_string(), algo_conductance as BuiltInProc);
        procs.insert("algo.normalized_cut".to_string(), algo_normalized_cut as BuiltInProc);
        procs.insert("db.propertyKeys".to_string(), db_property_keys as BuiltInProc);
        procs.insert("db.createTextIndex".to_string(), db_create_text_index as BuiltInProc);
        procs.insert("db.searchTextIndex".to_string(), db_search_text_index as BuiltInProc);
        procs.insert("db.createVectorIndex".to_string(), db_create_vector_index as BuiltInProc);
        procs.insert("db.searchVectorIndex".to_string(), db_search_vector_index as BuiltInProc);
        procs.insert("db.createPointIndex".to_string(), db_create_point_index as BuiltInProc);
        procs.insert("db.dropPointIndex".to_string(), db_drop_point_index as BuiltInProc);
        procs.insert("db.withinBBox".to_string(), db_within_bbox as BuiltInProc);
        procs.insert("db.nearest".to_string(), db_nearest as BuiltInProc);
        procs.insert("db.analytics".to_string(), db_analytics as BuiltInProc);
        procs.insert("db.degreeHistogram".to_string(), db_degree_histogram as BuiltInProc);
        procs.insert("db.degree_histogram".to_string(), db_degree_histogram as BuiltInProc);
        Self { procs }
    }

    pub fn get(&self, name: &str) -> Option<BuiltInProc> {
        self.procs.get(name).copied()
    }

    pub fn register(&mut self, name: String, proc: BuiltInProc) {
        self.procs.insert(name, proc);
    }

    pub fn list(&self) -> Vec<String> {
        self.procs.keys().cloned().collect()
    }
}

impl Default for ProcedureRegistry {
    fn default() -> Self { Self::new() }
}

// ─── db.labels ─────────────────────────────────────────────────────────────

fn db_labels(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let labels = storage.schema_info.all_labels();
    Ok(labels.into_iter().map(|l| {
        let mut row = HashMap::new();
        row.insert("label".to_string(), PropertyValue::String(format!("{}", l.as_uint())));
        row
    }).collect())
}

// ─── db.relationshipTypes ──────────────────────────────────────────────────

fn db_relationship_types(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let types = storage.schema_info.all_edge_types();
    Ok(types.into_iter().map(|t| {
        let mut row = HashMap::new();
        row.insert("relationshipType".to_string(), PropertyValue::String(format!("{}", t.as_uint())));
        row
    }).collect())
}

// ─── db.schema ─────────────────────────────────────────────────────────────

fn db_schema(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let labels = storage.schema_info.all_labels();
    let edge_types = storage.schema_info.all_edge_types();

    let mut row = HashMap::new();
    row.insert("nodeCount".to_string(), PropertyValue::Int(storage.vertex_count() as i64));
    row.insert("relationshipCount".to_string(), PropertyValue::Int(storage.edge_count() as i64));
    row.insert("labelCount".to_string(), PropertyValue::Int(labels.len() as i64));
    row.insert("relTypeCount".to_string(), PropertyValue::Int(edge_types.len() as i64));
    Ok(vec![row])
}

// ─── db.schema.nodeTypeProperties ──────────────────────────────────────────

fn db_schema_node_type_properties(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut rows = Vec::new();
    for label in storage.schema_info.all_labels() {
        let props = storage.schema_info.label_properties(label);
        for prop in props {
            let mut row = HashMap::new();
            row.insert("nodeType".to_string(), PropertyValue::String(format!("{}", label.as_uint())));
            row.insert("propertyName".to_string(), PropertyValue::String(format!("{}", prop.as_uint())));
            rows.push(row);
        }
    }
    Ok(rows)
}

// ─── db.schema.relTypeProperties ───────────────────────────────────────────

fn db_schema_rel_type_properties(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut rows = Vec::new();
    for etype in storage.schema_info.all_edge_types() {
        let props = storage.schema_info.edge_type_properties(etype);
        for prop in props {
            let mut row = HashMap::new();
            row.insert("relType".to_string(), PropertyValue::String(format!("{}", etype.as_uint())));
            row.insert("propertyName".to_string(), PropertyValue::String(format!("{}", prop.as_uint())));
            rows.push(row);
        }
    }
    Ok(rows)
}

// ─── db.stats ──────────────────────────────────────────────────────────────

fn db_stats(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let snap = storage.metrics.snapshot();
    let mut row = HashMap::new();
    row.insert("verticesCreated".to_string(), PropertyValue::Int(snap.vertices_created as i64));
    row.insert("verticesDeleted".to_string(), PropertyValue::Int(snap.vertices_deleted as i64));
    row.insert("edgesCreated".to_string(), PropertyValue::Int(snap.edges_created as i64));
    row.insert("edgesDeleted".to_string(), PropertyValue::Int(snap.edges_deleted as i64));
    row.insert("transactionsCommitted".to_string(), PropertyValue::Int(snap.transactions_committed as i64));
    row.insert("transactionsAborted".to_string(), PropertyValue::Int(snap.transactions_aborted as i64));
    Ok(vec![row])
}

// ─── db.indexes ────────────────────────────────────────────────────────────

fn db_indexes(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut rows = Vec::new();
    let labels = storage.active_label_indices.read().map_err(|e| format!("{}", e))?;
    for label in labels.iter() {
        let mut row = HashMap::new();
        row.insert("label".to_string(), PropertyValue::String(format!("{}", label.as_uint())));
        row.insert("type".to_string(), PropertyValue::String("label".into()));
        rows.push(row);
    }
    let lp = storage.active_label_property_indices.read().map_err(|e| format!("{}", e))?;
    for (label, prop) in lp.iter() {
        let mut row = HashMap::new();
        row.insert("label".to_string(), PropertyValue::String(format!("{}", label.as_uint())));
        row.insert("property".to_string(), PropertyValue::String(format!("{}", prop.as_uint())));
        row.insert("type".to_string(), PropertyValue::String("label+property".into()));
        rows.push(row);
    }
    // Vector indices
    let vector_indices = storage.vector_indices.read().map_err(|e| format!("{}", e))?;
    for (label, entry) in vector_indices.iter() {
        let mut row = HashMap::new();
        row.insert("label".to_string(), PropertyValue::String(format!("{}", label.as_uint())));
        row.insert("property".to_string(), PropertyValue::String(format!("{}", entry.property.as_uint())));
        row.insert("type".to_string(), PropertyValue::String("vector".into()));
        row.insert("dimension".to_string(), PropertyValue::Int(entry.dimension as i64));
        row.insert("distance".to_string(), PropertyValue::String(format!("{:?}", entry.distance)));
        rows.push(row);
    }
    drop(vector_indices);
    // Text indices
    let text_indices = storage.text_indices.read().map_err(|e| format!("{}", e))?;
    for (label, entry) in text_indices.iter() {
        let mut row = HashMap::new();
        row.insert("label".to_string(), PropertyValue::String(format!("{}", label.as_uint())));
        let prop_names: Vec<String> = entry.properties.iter().map(|(pid, _)| format!("{}", pid.as_uint())).collect();
        row.insert("property".to_string(), PropertyValue::String(prop_names.join(",")));
        row.insert("type".to_string(), PropertyValue::String("text".into()));
        rows.push(row);
    }
    drop(text_indices);
    // Point indices
    let point_indices = storage.active_point_indices.read().map_err(|e| format!("{}", e))?;
    for (label, prop) in point_indices.iter() {
        let mut row = HashMap::new();
        row.insert("label".to_string(), PropertyValue::String(format!("{}", label.as_uint())));
        row.insert("property".to_string(), PropertyValue::String(format!("{}", prop.as_uint())));
        row.insert("type".to_string(), PropertyValue::String("point".into()));
        rows.push(row);
    }
    Ok(rows)
}

// ─── db.constraints ────────────────────────────────────────────────────────

fn db_constraints(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut rows = Vec::new();
    for c in &storage.constraints.list() {
        let mut row = HashMap::new();
        row.insert("type".to_string(), PropertyValue::String(format!("{:?}", c.kind)));
        row.insert("label".to_string(), PropertyValue::String(format!("{}", c.label.as_uint())));
        row.insert("property".to_string(), PropertyValue::String(format!("{}", c.property.as_uint())));
        rows.push(row);
    }
    Ok(rows)
}

// ─── Graph algorithm built-ins ─────────────────────────────────────────────

fn algo_pagerank(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let ranks = mgquery::pagerank(storage, 0.85, 100, 1e-6);
    Ok(ranks.into_iter().map(|(gid, rank)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("rank".to_string(), PropertyValue::Double(rank));
        row
    }).collect())
}

fn algo_wcc(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let components = mgquery::wcc(storage);
    Ok(components.into_iter().map(|(gid, cid)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("component".to_string(), PropertyValue::Int(cid as i64));
        row
    }).collect())
}

fn algo_bfs(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let start_gid = match args.get("startNode") {
        Some(PropertyValue::Int(n)) => Gid::from(*n as u64),
        _ => return Err("startNode argument required (int)".into()),
    };
    let path = mgquery::bfs(storage, start_gid);
    Ok(path.into_iter().enumerate().map(|(i, gid)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("depth".to_string(), PropertyValue::Int(i as i64));
        row
    }).collect())
}

fn algo_triangle_count(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let count = mgquery::triangle_count(storage);
    let mut row = HashMap::new();
    row.insert("triangles".to_string(), PropertyValue::Int(count as i64));
    Ok(vec![row])
}

fn algo_betweenness_centrality(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let scores = mgquery::betweenness_centrality(storage);
    Ok(scores.into_iter().map(|(gid, score)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("score".to_string(), PropertyValue::Double(score));
        row
    }).collect())
}

fn algo_closeness_centrality(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let scores = mgquery::closeness_centrality(storage);
    Ok(scores.into_iter().map(|(gid, score)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("score".to_string(), PropertyValue::Double(score));
        row
    }).collect())
}

fn algo_eigenvector_centrality(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let scores = mgquery::eigenvector_centrality(storage, 100, 1e-6);
    Ok(scores.into_iter().map(|(gid, score)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("score".to_string(), PropertyValue::Double(score));
        row
    }).collect())
}

fn algo_shortest_path(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let start = match args.get("startNode") {
        Some(PropertyValue::Int(n)) => Gid::from(*n as u64),
        _ => return Err("startNode argument required (int)".into()),
    };
    let end = match args.get("endNode") {
        Some(PropertyValue::Int(n)) => Gid::from(*n as u64),
        _ => return Err("endNode argument required (int)".into()),
    };
    match mgquery::shortest_path(storage, start, end) {
        Some(path) => Ok(path.into_iter().enumerate().map(|(i, gid)| {
            let mut row = HashMap::new();
            row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
            row.insert("hop".to_string(), PropertyValue::Int(i as i64));
            row
        }).collect()),
        None => Ok(vec![]),
    }
}

fn algo_clustering_coefficient(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let coeffs = mgquery::clustering_coefficient(storage);
    Ok(coeffs.into_iter().map(|(gid, coeff)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("coefficient".to_string(), PropertyValue::Double(coeff));
        row
    }).collect())
}

fn algo_scc(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let components = mgquery::scc(storage);
    Ok(components.into_iter().map(|(gid, cid)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("component".to_string(), PropertyValue::Int(cid as i64));
        row
    }).collect())
}

fn algo_topological_sort(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    match mgquery::topological_sort(storage) {
        Some(order) => Ok(order.into_iter().enumerate().map(|(i, gid)| {
            let mut row = HashMap::new();
            row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
            row.insert("order".to_string(), PropertyValue::Int(i as i64));
            row
        }).collect()),
        None => Err("graph contains cycles".into()),
    }
}

fn algo_degree_centrality(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let scores = mgquery::degree_centrality(storage);
    Ok(scores.into_iter().map(|(gid, degree)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("degree".to_string(), PropertyValue::Int(degree as i64));
        row
    }).collect())
}

fn algo_bridges(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let bridges = mgquery::bridges(storage);
    Ok(bridges.into_iter().map(|(from, to)| {
        let mut row = HashMap::new();
        row.insert("from".to_string(), PropertyValue::Int(from.as_int()));
        row.insert("to".to_string(), PropertyValue::Int(to.as_int()));
        row
    }).collect())
}

fn algo_is_bipartite(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("bipartite".to_string(), PropertyValue::Bool(mgquery::is_bipartite(storage)));
    Ok(vec![row])
}

fn algo_diameter(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("diameter".to_string(), PropertyValue::Int(mgquery::diameter(storage) as i64));
    Ok(vec![row])
}

fn algo_graph_density(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("density".to_string(), PropertyValue::Double(mgquery::graph_density(storage)));
    Ok(vec![row])
}

fn algo_has_cycle(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("hasCycle".to_string(), PropertyValue::Bool(mgquery::has_cycle(storage)));
    Ok(vec![row])
}

fn algo_harmonic_centrality(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let scores = mgquery::harmonic_centrality(storage);
    Ok(scores.into_iter().map(|(gid, score)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("score".to_string(), PropertyValue::Double(score));
        row
    }).collect())
}

fn algo_radius(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("radius".to_string(), PropertyValue::Int(mgquery::radius(storage) as i64));
    Ok(vec![row])
}

fn algo_eccentricity(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let scores = mgquery::eccentricity(storage);
    Ok(scores.into_iter().map(|(gid, ecc)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("eccentricity".to_string(), PropertyValue::Int(ecc as i64));
        row
    }).collect())
}

fn algo_average_path_length(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("averagePathLength".to_string(), PropertyValue::Double(mgquery::average_path_length(storage)));
    Ok(vec![row])
}

fn algo_coreness(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let scores = mgquery::coreness(storage);
    Ok(scores.into_iter().map(|(gid, k)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("coreness".to_string(), PropertyValue::Int(k as i64));
        row
    }).collect())
}

fn algo_degree_assortativity(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("assortativity".to_string(), PropertyValue::Double(mgquery::degree_assortativity(storage)));
    Ok(vec![row])
}

fn algo_global_clustering_coefficient(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("globalClusteringCoefficient".to_string(), PropertyValue::Double(mgquery::global_clustering_coefficient(storage)));
    Ok(vec![row])
}

fn algo_center(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let centers = mgquery::center(storage);
    Ok(centers.into_iter().map(|gid| {
        let mut row = HashMap::new();
        row.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
        row
    }).collect())
}

fn algo_periphery(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let periph = mgquery::periphery(storage);
    Ok(periph.into_iter().map(|gid| {
        let mut row = HashMap::new();
        row.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
        row
    }).collect())
}

fn algo_small_world_coefficient(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("sigma".to_string(), PropertyValue::Double(mgquery::small_world_coefficient(storage)));
    Ok(vec![row])
}

fn algo_articulation_points(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let points = mgquery::articulation_points::articulation_points(storage);
    Ok(points.into_iter().map(|gid| {
        let mut row = HashMap::new();
        row.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
        row
    }).collect())
}

fn algo_is_biconnected(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("biconnected".to_string(), PropertyValue::Bool(mgquery::articulation_points::is_biconnected(storage)));
    Ok(vec![row])
}

fn algo_connected_components(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let components = mgquery::community::connected_components(storage);
    Ok(components.into_iter().map(|(gid, cid)| {
        let mut row = HashMap::new();
        row.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("component".to_string(), PropertyValue::Int(cid.as_int()));
        row
    }).collect())
}

fn algo_cycle_detection(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    match mgquery::traversal::cycle_detection(storage) {
        Some(cycle) => Ok(cycle.into_iter().map(|gid| {
            let mut row = HashMap::new();
            row.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
            row
        }).collect()),
        None => Ok(vec![]),
    }
}

fn algo_all_pairs_shortest_path(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let apsp = mgquery::path::all_pairs_shortest_path(storage);
    Ok(apsp.into_iter().map(|((a, b), dist)| {
        let mut row = HashMap::new();
        row.insert("source".to_string(), PropertyValue::Int(a.as_int()));
        row.insert("target".to_string(), PropertyValue::Int(b.as_int()));
        row.insert("distance".to_string(), PropertyValue::Int(dist as i64));
        row
    }).collect())
}

fn algo_maximal_cliques(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let cliques = mgquery::clique::maximal_cliques(storage);
    Ok(cliques.into_iter().enumerate().map(|(i, clique)| {
        let mut row = HashMap::new();
        row.insert("cliqueId".to_string(), PropertyValue::Int(i as i64));
        row.insert("nodes".to_string(), PropertyValue::List(clique.into_iter().map(|g| PropertyValue::Int(g.as_int())).collect()));
        row
    }).collect())
}

fn algo_clique_number(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("cliqueNumber".to_string(), PropertyValue::Int(mgquery::clique::clique_number(storage) as i64));
    Ok(vec![row])
}

fn algo_greedy_coloring(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let coloring = mgquery::coloring::greedy_coloring(storage);
    Ok(coloring.into_iter().map(|(gid, color)| {
        let mut row = HashMap::new();
        row.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("color".to_string(), PropertyValue::Int(color as i64));
        row
    }).collect())
}

fn algo_dsatur_coloring(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let coloring = mgquery::coloring::dsatur_coloring(storage);
    Ok(coloring.into_iter().map(|(gid, color)| {
        let mut row = HashMap::new();
        row.insert("nodeId".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("color".to_string(), PropertyValue::Int(color as i64));
        row
    }).collect())
}

fn algo_degeneracy(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let mut row = HashMap::new();
    row.insert("degeneracy".to_string(), PropertyValue::Int(mgquery::k_core::degeneracy(storage) as i64));
    Ok(vec![row])
}

fn algo_predict_links(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let predictions = mgquery::link_prediction::predict_links(storage, 10);
    Ok(predictions.into_iter().map(|(a, b, score)| {
        let mut row = HashMap::new();
        row.insert("nodeA".to_string(), PropertyValue::Int(a.as_int()));
        row.insert("nodeB".to_string(), PropertyValue::Int(b.as_int()));
        row.insert("score".to_string(), PropertyValue::Double(score));
        row
    }).collect())
}

fn algo_rich_club_coefficient(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let max_k = match args.get("maxK") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => 10,
    };
    let coeffs = mgquery::rich_club_coefficient(storage, max_k);
    Ok(coeffs.into_iter().map(|(k, phi)| {
        let mut row = HashMap::new();
        row.insert("k".to_string(), PropertyValue::Int(k as i64));
        row.insert("coefficient".to_string(), PropertyValue::Double(phi));
        row
    }).collect())
}

fn algo_katz_centrality(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let alpha = match args.get("alpha") {
        Some(PropertyValue::Double(n)) => *n,
        Some(PropertyValue::Int(n)) => *n as f64,
        _ => 0.002,
    };
    let max_iter = match args.get("maxIterations") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => 100,
    };
    let epsilon = match args.get("epsilon") {
        Some(PropertyValue::Double(n)) => *n,
        _ => 1e-6,
    };
    let scores = mgquery::katz::katz_centrality(storage, alpha, max_iter, epsilon);
    Ok(scores.into_iter().map(|(gid, score)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("score".to_string(), PropertyValue::Double(score));
        row
    }).collect())
}

fn algo_label_propagation(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let max_iter = match args.get("maxIterations") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => 100,
    };
    let communities = mgquery::label_propagation::label_propagation(storage, max_iter);
    Ok(communities.into_iter().map(|(gid, cid)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("community".to_string(), PropertyValue::Int(cid as i64));
        row
    }).collect())
}

fn algo_louvain(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let resolution = match args.get("resolution") {
        Some(PropertyValue::Double(n)) => *n,
        Some(PropertyValue::Int(n)) => *n as f64,
        _ => 1.0,
    };
    let max_iter = match args.get("maxIterations") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => 100,
    };
    let communities = mgquery::louvain::louvain(storage, resolution, max_iter);
    Ok(communities.into_iter().map(|(gid, cid)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("community".to_string(), PropertyValue::Int(cid as i64));
        row
    }).collect())
}

fn algo_hits(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let max_iter = match args.get("maxIterations") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => 100,
    };
    let epsilon = match args.get("epsilon") {
        Some(PropertyValue::Double(n)) => *n,
        _ => 1e-6,
    };
    let (auth, hub) = mgquery::hits::hits(storage, max_iter, epsilon);
    Ok(auth.into_iter().map(|(gid, score)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("authority".to_string(), PropertyValue::Double(score));
        row.insert("hub".to_string(), PropertyValue::Double(hub.get(&gid).copied().unwrap_or(0.0)));
        row
    }).collect())
}

fn algo_core_decomposition(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let cores = mgquery::k_core::core_decomposition(storage);
    Ok(cores.into_iter().map(|(gid, k)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("coreness".to_string(), PropertyValue::Int(k as i64));
        row
    }).collect())
}

fn algo_k_core(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let k = match args.get("k") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => return Err("k argument required (int)".into()),
    };
    let nodes = mgquery::k_core::k_core(storage, k);
    Ok(nodes.into_iter().map(|gid| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row
    }).collect())
}

fn algo_floyd_warshall(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let dists = mgquery::shortest_path::floyd_warshall(storage);
    Ok(dists.into_iter().map(|((a, b), dist)| {
        let mut row = HashMap::new();
        row.insert("source".to_string(), PropertyValue::Int(a.as_int()));
        row.insert("target".to_string(), PropertyValue::Int(b.as_int()));
        row.insert("distance".to_string(), PropertyValue::Double(dist));
        row
    }).collect())
}

fn algo_modularity(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let partition = mgquery::wcc(storage);
    let q = mgquery::modularity(storage, &partition);
    let mut row = HashMap::new();
    row.insert("modularity".to_string(), PropertyValue::Double(q));
    Ok(vec![row])
}

fn algo_conductance(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let partition = mgquery::wcc(storage);
    let cond = mgquery::conductance(storage, &partition);
    Ok(cond.into_iter().map(|(cid, phi)| {
        let mut row = HashMap::new();
        row.insert("community".to_string(), PropertyValue::Int(cid as i64));
        row.insert("conductance".to_string(), PropertyValue::Double(phi));
        row
    }).collect())
}

fn algo_normalized_cut(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let partition = mgquery::wcc(storage);
    let nc = mgquery::normalized_cut(storage, &partition);
    Ok(nc.into_iter().map(|(cid, val)| {
        let mut row = HashMap::new();
        row.insert("community".to_string(), PropertyValue::Int(cid as i64));
        row.insert("normalizedCut".to_string(), PropertyValue::Double(val));
        row
    }).collect())
}

fn db_property_keys(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let keys = storage.schema_info.all_property_keys();
    Ok(keys.into_iter().map(|prop| {
        let mut row = HashMap::new();
        row.insert("propertyKey".to_string(), PropertyValue::String(format!("{}", prop.as_uint())));
        row
    }).collect())
}

// ─── db.createTextIndex ────────────────────────────────────────────────────

pub fn db_create_text_index(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let label_name = match args.get("label") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("label argument required".into()),
    };
    let prop_list = match args.get("properties") {
        Some(PropertyValue::List(l)) => l.iter().filter_map(|v| match v {
            PropertyValue::String(s) => Some(s.clone()),
            _ => None,
        }).collect::<Vec<_>>(),
        _ => vec!["value".into()],
    };

    let catalog = crate::eval::active_catalog().unwrap_or_else(|| {
        // Fallback: create a temporary catalog just for name resolution
        static TMP: std::sync::OnceLock<mgcatalog::Catalog> = std::sync::OnceLock::new();
        TMP.get_or_init(mgcatalog::Catalog::new)
    });
    let label_id = catalog.label(&label_name);
    static IDX_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let unique = IDX_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path = format!("/tmp/mg_text_idx_{}_{}", label_name, unique);
    let _ = std::fs::remove_dir_all(&path);

    match mgstorage::text_index::TextIndex::create(&path, &prop_list) {
        Ok(index) => {
            let mut indexed_props = Vec::new();
            for name in &prop_list {
                let pid = catalog.property(name);
                indexed_props.push((pid, name.clone()));
            }
            let entry = mgstorage::storage::TextIndexEntry::new(std::sync::Arc::new(index), indexed_props.clone());
            storage.text_indices.write().unwrap().insert(label_id, entry);

            // Index existing vertices with this label
            let all = storage.all_vertices();
            let text_indices = storage.text_indices.read().unwrap();
            let entry = text_indices.get(&label_id).unwrap();
            for (gid, labels, props) in all {
                if labels.contains(&label_id) {
                    let mut text_values = Vec::new();
                    for (pid, field_name) in &indexed_props {
                        let prop_val = props.get(*pid);
                        if !prop_val.is_null() {
                            let s = match prop_val {
                                PropertyValue::String(s) => s.clone(),
                                PropertyValue::Int(n) => n.to_string(),
                                PropertyValue::Double(n) => n.to_string(),
                                PropertyValue::Bool(b) => b.to_string(),
                                _ => continue,
                            };
                            text_values.push((field_name.clone(), s));
                        }
                    }
                    let _ = entry.index.index_vertex(gid, &text_values);
                }
            }
            drop(text_indices);
            Ok(vec![])
        }
        Err(e) => Err(format!("Failed to create text index: {}", e)),
    }
}

// ─── db.searchTextIndex ────────────────────────────────────────────────────

pub fn db_search_text_index(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let label_name = match args.get("label") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("label argument required".into()),
    };
    let query = match args.get("query") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("query argument required".into()),
    };
    let limit = match args.get("limit") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => 10,
    };

    let catalog = crate::eval::active_catalog().unwrap_or_else(|| {
        static TMP: std::sync::OnceLock<mgcatalog::Catalog> = std::sync::OnceLock::new();
        TMP.get_or_init(mgcatalog::Catalog::new)
    });
    let label_id = catalog.label(&label_name);

    let text_indices = storage.text_indices.read().unwrap();
    let entry = match text_indices.get(&label_id) {
        Some(e) => e,
        None => return Ok(vec![]),
    };

    let results = match entry.index.search(&query, limit) {
        Ok(r) => r,
        Err(e) => return Err(format!("Text search failed: {}", e)),
    };
    drop(text_indices);

    Ok(results.into_iter().map(|(gid, score)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("score".to_string(), PropertyValue::Double(score as f64));
        row
    }).collect())
}

// ─── db.createVectorIndex ──────────────────────────────────────────────────

pub fn db_create_vector_index(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let label_name = match args.get("label") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("label argument required".into()),
    };
    let prop_name = match args.get("property") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("property argument required".into()),
    };
    let dimension = match args.get("dimension") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => return Err("dimension argument required (int)".into()),
    };
    let distance = match args.get("distance") {
        Some(PropertyValue::String(s)) => match s.as_str() {
            "cosine" => mgvector::Distance::Cosine,
            "euclidean" => mgvector::Distance::Euclidean,
            "dot" => mgvector::Distance::Dot,
            "manhattan" => mgvector::Distance::Manhattan,
            _ => return Err("distance must be one of: cosine, euclidean, dot, manhattan".into()),
        },
        _ => mgvector::Distance::Cosine,
    };

    let catalog = crate::eval::active_catalog().unwrap_or_else(|| {
        static TMP: std::sync::OnceLock<mgcatalog::Catalog> = std::sync::OnceLock::new();
        TMP.get_or_init(mgcatalog::Catalog::new)
    });
    let label_id = catalog.label(&label_name);
    let prop_id = catalog.property(&prop_name);

    let index = std::sync::Arc::new(std::sync::RwLock::new(
        mgvector::HnswIndex::new(dimension, distance, mgvector::HnswConfig::default())
    ));
    let entry = mgstorage::storage::VectorIndexEntry::new(index.clone(), prop_id, dimension, distance);

    // Backfill existing vertices with this label
    let all = storage.all_vertices();
    for (gid, labels, props) in all {
        if labels.contains(&label_id) {
            let prop_value = props.get(prop_id);
            if !prop_value.is_null() {
                if let PropertyValue::List(items) = prop_value {
                    let mut vec = Vec::with_capacity(items.len());
                    for item in items {
                        match item {
                            PropertyValue::Int(i) => vec.push(*i as f32),
                            PropertyValue::Double(d) => vec.push(*d as f32),
                            _ => { vec.clear(); break; }
                        }
                    }
                    if vec.len() == dimension {
                        let mut idx = index.write().unwrap();
                        let node_id = idx.insert(&vec);
                        entry.gid_to_node.write().unwrap().insert(gid, node_id);
                    }
                }
            }
        }
    }

    storage.vector_indices.write().unwrap().insert(label_id, entry);
    Ok(vec![])
}

// ─── db.searchVectorIndex ──────────────────────────────────────────────────

pub fn db_search_vector_index(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let label_name = match args.get("label") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("label argument required".into()),
    };
    let prop_name = match args.get("property") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("property argument required".into()),
    };
    let query_vec = match args.get("vector") {
        Some(PropertyValue::List(l)) => {
            let mut vec = Vec::with_capacity(l.len());
            for v in l {
                match v {
                    PropertyValue::Int(i) => vec.push(*i as f32),
                    PropertyValue::Double(d) => vec.push(*d as f32),
                    _ => return Err("vector must contain only numbers".into()),
                }
            }
            vec
        }
        _ => return Err("vector argument required (list of numbers)".into()),
    };
    let k = match args.get("k") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => 10,
    };

    let catalog = crate::eval::active_catalog().unwrap_or_else(|| {
        static TMP: std::sync::OnceLock<mgcatalog::Catalog> = std::sync::OnceLock::new();
        TMP.get_or_init(mgcatalog::Catalog::new)
    });
    let label_id = catalog.label(&label_name);
    let prop_id = catalog.property(&prop_name);

    let vector_indices = storage.vector_indices.read().unwrap();
    let entry = match vector_indices.get(&label_id) {
        Some(e) if e.property == prop_id => e,
        _ => return Ok(vec![]),
    };

    let index = entry.index.read().unwrap();
    let results = index.search(&query_vec, k);
    drop(index);
    drop(vector_indices);

    Ok(results.into_iter().map(|(node_id, distance)| {
        let mut row = HashMap::new();
        // Map NodeId back to Gid via gid_to_node reverse lookup
        let gid = storage.vector_indices.read().unwrap()
            .get(&label_id)
            .and_then(|e| e.gid_to_node.read().unwrap().iter().find(|(_, nid)| **nid == node_id).map(|(gid, _)| *gid))
            .unwrap_or_else(|| Gid::from(node_id as u64));
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("distance".to_string(), PropertyValue::Double(distance as f64));
        row
    }).collect())
}

// ─── db.createPointIndex ───────────────────────────────────────────────────

pub fn db_create_point_index(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let label_name = match args.get("label") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("label argument required".into()),
    };
    let prop_name = match args.get("property") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("property argument required".into()),
    };

    let catalog = crate::eval::active_catalog().unwrap_or_else(|| {
        static TMP: std::sync::OnceLock<mgcatalog::Catalog> = std::sync::OnceLock::new();
        TMP.get_or_init(mgcatalog::Catalog::new)
    });
    let label_id = catalog.label(&label_name);
    let prop_id = catalog.property(&prop_name);

    storage.create_point_index(label_id, prop_id);

    // Backfill existing vertices
    let all = storage.all_vertices();
    for (gid, labels, props) in all {
        if labels.contains(&label_id) {
            let prop_value = props.get(prop_id);
            match prop_value {
                PropertyValue::Point2D(p) => storage.point_index.insert_2d(label_id, prop_id, gid, *p),
                PropertyValue::Point3D(p) => storage.point_index.insert_3d(label_id, prop_id, gid, *p),
                _ => {}
            }
        }
    }

    Ok(vec![])
}

// ─── db.dropPointIndex ─────────────────────────────────────────────────────

pub fn db_drop_point_index(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let label_name = match args.get("label") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("label argument required".into()),
    };
    let prop_name = match args.get("property") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("property argument required".into()),
    };

    let catalog = crate::eval::active_catalog().unwrap_or_else(|| {
        static TMP: std::sync::OnceLock<mgcatalog::Catalog> = std::sync::OnceLock::new();
        TMP.get_or_init(mgcatalog::Catalog::new)
    });
    let label_id = catalog.label(&label_name);
    let prop_id = catalog.property(&prop_name);

    storage.drop_point_index(label_id, prop_id);
    Ok(vec![])
}

// ─── db.withinBBox ─────────────────────────────────────────────────────────

pub fn db_within_bbox(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let label_name = match args.get("label") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("label argument required".into()),
    };
    let prop_name = match args.get("property") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("property argument required".into()),
    };
    let lower_left = match args.get("lowerLeft") {
        Some(PropertyValue::List(l)) if l.len() == 2 => {
            let x = match &l[0] {
                PropertyValue::Int(i) => *i as f64,
                PropertyValue::Double(d) => *d,
                _ => return Err("lowerLeft must be a list of 2 numbers".into()),
            };
            let y = match &l[1] {
                PropertyValue::Int(i) => *i as f64,
                PropertyValue::Double(d) => *d,
                _ => return Err("lowerLeft must be a list of 2 numbers".into()),
            };
            mgcore::point::Point2D::new(mgcore::point::Crs::Cartesian2D, x, y)
        }
        _ => return Err("lowerLeft argument required ([x, y])".into()),
    };
    let upper_right = match args.get("upperRight") {
        Some(PropertyValue::List(l)) if l.len() == 2 => {
            let x = match &l[0] {
                PropertyValue::Int(i) => *i as f64,
                PropertyValue::Double(d) => *d,
                _ => return Err("upperRight must be a list of 2 numbers".into()),
            };
            let y = match &l[1] {
                PropertyValue::Int(i) => *i as f64,
                PropertyValue::Double(d) => *d,
                _ => return Err("upperRight must be a list of 2 numbers".into()),
            };
            mgcore::point::Point2D::new(mgcore::point::Crs::Cartesian2D, x, y)
        }
        _ => return Err("upperRight argument required ([x, y])".into()),
    };

    let catalog = crate::eval::active_catalog().unwrap_or_else(|| {
        static TMP: std::sync::OnceLock<mgcatalog::Catalog> = std::sync::OnceLock::new();
        TMP.get_or_init(mgcatalog::Catalog::new)
    });
    let label_id = catalog.label(&label_name);
    let prop_id = catalog.property(&prop_name);

    if !storage.has_point_index(label_id, prop_id) {
        return Ok(vec![]);
    }

    let gids = storage.point_index.within_bbox_2d(label_id, prop_id, lower_left, upper_right);
    Ok(gids.into_iter().map(|gid| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row
    }).collect())
}

// ─── db.nearest ────────────────────────────────────────────────────────────

pub fn db_nearest(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let label_name = match args.get("label") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("label argument required".into()),
    };
    let prop_name = match args.get("property") {
        Some(PropertyValue::String(s)) => s.clone(),
        _ => return Err("property argument required".into()),
    };
    let query_point = match args.get("point") {
        Some(PropertyValue::List(l)) if l.len() == 2 => {
            let x = match &l[0] {
                PropertyValue::Int(i) => *i as f64,
                PropertyValue::Double(d) => *d,
                _ => return Err("point must be a list of 2 numbers".into()),
            };
            let y = match &l[1] {
                PropertyValue::Int(i) => *i as f64,
                PropertyValue::Double(d) => *d,
                _ => return Err("point must be a list of 2 numbers".into()),
            };
            mgcore::point::Point2D::new(mgcore::point::Crs::Cartesian2D, x, y)
        }
        _ => return Err("point argument required ([x, y])".into()),
    };
    let k = match args.get("k") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => 10,
    };

    let catalog = crate::eval::active_catalog().unwrap_or_else(|| {
        static TMP: std::sync::OnceLock<mgcatalog::Catalog> = std::sync::OnceLock::new();
        TMP.get_or_init(mgcatalog::Catalog::new)
    });
    let label_id = catalog.label(&label_name);
    let prop_id = catalog.property(&prop_name);

    if !storage.has_point_index(label_id, prop_id) {
        return Ok(vec![]);
    }

    let results = storage.point_index.nearest_2d(label_id, prop_id, query_point, k);
    Ok(results.into_iter().map(|(gid, distance)| {
        let mut row = HashMap::new();
        row.insert("node".to_string(), PropertyValue::Int(gid.as_int()));
        row.insert("distance".to_string(), PropertyValue::Double(distance));
        row
    }).collect())
}

fn db_analytics(
    storage: &Storage,
    _args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let stats = mgstorage::analytics::compute_graph_stats(storage);
    let avg_cc = mgstorage::analytics::average_clustering_coefficient(storage);
    let reciprocity = mgstorage::analytics::reciprocity(storage);
    let triangle_density = mgstorage::analytics::triangle_density(storage);
    let degree_corr = mgstorage::analytics::degree_correlation(storage);
    let mut row = HashMap::new();
    row.insert("vertexCount".to_string(), PropertyValue::Int(stats.vertex_count as i64));
    row.insert("edgeCount".to_string(), PropertyValue::Int(stats.edge_count as i64));
    row.insert("avgDegree".to_string(), PropertyValue::Double(stats.avg_degree));
    row.insert("maxInDegree".to_string(), PropertyValue::Int(stats.max_in_degree as i64));
    row.insert("maxOutDegree".to_string(), PropertyValue::Int(stats.max_out_degree as i64));
    row.insert("density".to_string(), PropertyValue::Double(stats.density));
    row.insert("connectedComponents".to_string(), PropertyValue::Int(stats.connected_component_count as i64));
    row.insert("isolatedVertices".to_string(), PropertyValue::Int(stats.isolated_vertex_count as i64));
    row.insert("avgClusteringCoefficient".to_string(), PropertyValue::Double(avg_cc));
    row.insert("reciprocity".to_string(), PropertyValue::Double(reciprocity));
    row.insert("triangleDensity".to_string(), PropertyValue::Double(triangle_density));
    row.insert("degreeCorrelation".to_string(), PropertyValue::Double(degree_corr));
    Ok(vec![row])
}

fn db_degree_histogram(
    storage: &Storage,
    args: &HashMap<String, PropertyValue>,
) -> Result<Vec<HashMap<String, PropertyValue>>, String> {
    let max_bucket = match args.get("maxBucket") {
        Some(PropertyValue::Int(n)) => *n as usize,
        _ => 20,
    };
    let hist = mgstorage::analytics::degree_histogram(storage, max_bucket);
    Ok(hist.into_iter().map(|(degree, count)| {
        let mut row = HashMap::new();
        row.insert("degree".to_string(), PropertyValue::Int(degree as i64));
        row.insert("count".to_string(), PropertyValue::Int(count as i64));
        row
    }).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_builtins() {
        let reg = ProcedureRegistry::new();
        assert!(reg.get("db.labels").is_some());
        assert!(reg.get("db.schema").is_some());
        assert!(reg.get("db.stats").is_some());
        assert!(reg.get("nonexistent").is_none());
    }

    #[test]
    fn test_db_labels_empty() {
        let storage = Storage::new();
        let rows = db_labels(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_db_schema_empty() {
        let storage = Storage::new();
        let rows = db_schema(&storage, &HashMap::new()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("nodeCount"), Some(&PropertyValue::Int(0)));
    }

    #[test]
    fn test_db_stats_empty() {
        let storage = Storage::new();
        let rows = db_stats(&storage, &HashMap::new()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("verticesCreated"), Some(&PropertyValue::Int(0)));
    }

    #[test]
    fn test_db_indexes_empty() {
        let storage = Storage::new();
        let rows = db_indexes(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_db_constraints_empty() {
        let storage = Storage::new();
        let rows = db_constraints(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_registry_new_algorithms() {
        let reg = ProcedureRegistry::new();
        assert!(reg.get("algo.rich_club_coefficient").is_some());
        assert!(reg.get("algo.katz_centrality").is_some());
        assert!(reg.get("algo.label_propagation").is_some());
        assert!(reg.get("algo.louvain").is_some());
        assert!(reg.get("algo.hits").is_some());
        assert!(reg.get("algo.core_decomposition").is_some());
        assert!(reg.get("algo.k_core").is_some());
        assert!(reg.get("algo.floyd_warshall").is_some());
        assert!(reg.get("algo.modularity").is_some());
        assert!(reg.get("algo.conductance").is_some());
        assert!(reg.get("algo.normalized_cut").is_some());
    }

    #[test]
    fn test_rich_club_empty() {
        let storage = Storage::new();
        let rows = algo_rich_club_coefficient(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_katz_empty() {
        let storage = Storage::new();
        let rows = algo_katz_centrality(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_label_propagation_empty() {
        let storage = Storage::new();
        let rows = algo_label_propagation(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_louvain_empty() {
        let storage = Storage::new();
        let rows = algo_louvain(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_hits_empty() {
        let storage = Storage::new();
        let rows = algo_hits(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_core_decomposition_empty() {
        let storage = Storage::new();
        let rows = algo_core_decomposition(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_k_core_empty() {
        let storage = Storage::new();
        let mut args = HashMap::new();
        args.insert("k".to_string(), PropertyValue::Int(2));
        let rows = algo_k_core(&storage, &args).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_floyd_warshall_empty() {
        let storage = Storage::new();
        let rows = algo_floyd_warshall(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_modularity_empty() {
        let storage = Storage::new();
        let rows = algo_modularity(&storage, &HashMap::new()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("modularity"), Some(&PropertyValue::Double(0.0)));
    }

    #[test]
    fn test_conductance_empty() {
        let storage = Storage::new();
        let rows = algo_conductance(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_normalized_cut_empty() {
        let storage = Storage::new();
        let rows = algo_normalized_cut(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_db_analytics_empty() {
        let storage = Storage::new();
        let rows = db_analytics(&storage, &HashMap::new()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("vertexCount"), Some(&PropertyValue::Int(0)));
        assert_eq!(rows[0].get("edgeCount"), Some(&PropertyValue::Int(0)));
    }

    #[test]
    fn test_db_degree_histogram_empty() {
        let storage = Storage::new();
        let rows = db_degree_histogram(&storage, &HashMap::new()).unwrap();
        assert!(rows.is_empty());
    }
}
