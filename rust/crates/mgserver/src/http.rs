//! HTTP REST API server for mgserver.
//!
//! Provides REST endpoints for health, metrics, vertices, edges,
//! Cypher queries, schema, and storage stats.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{Empty, Full};
use hyper::{Method, Request, Response, StatusCode};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use mgcatalog::Catalog;
use mgcore::delta::IsolationLevel;
use mgcore::property_store::PropertyStore;
use mgcore::property_value::PropertyValue;
use mgcore::types::{Gid, LabelId};
use mginterp::execute_with_catalog_auth_dbms_and_params_timeout;
use mgstorage::storage::{Storage, VertexSnapshot, EdgeSnapshot};

use crate::admin::AdminState;

/// HTTP body type used for all responses.
type Body = Full<Bytes>;

/// HTTP server handle.
pub struct HttpServer {
    storage: Arc<Storage>,
    admin: Arc<AdminState>,
    catalog: Arc<Catalog>,
    cluster_state: Option<Arc<mgcoord::ClusterState>>,
    cluster_manager: Option<Arc<tokio::sync::RwLock<Option<mgcoord::ClusterManager>>>>,
}

impl HttpServer {
    pub fn new(
        storage: Arc<Storage>,
        admin: Arc<AdminState>,
        catalog: Arc<Catalog>,
        cluster_state: Option<Arc<mgcoord::ClusterState>>,
        cluster_manager: Option<Arc<tokio::sync::RwLock<Option<mgcoord::ClusterManager>>>>,
    ) -> Self {
        Self {
            storage,
            admin,
            catalog,
            cluster_state,
            cluster_manager,
        }
    }

    /// Start the HTTP server on the given address.
    pub async fn run(self, addr: SocketAddr) {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => {
                tracing::info!("HTTP server listening on http://{}", addr);
                l
            }
            Err(e) => {
                tracing::error!("Failed to bind HTTP server to {}: {}", addr, e);
                return;
            }
        };

        let state = Arc::new(ServerState {
            storage: self.storage,
            admin: self.admin,
            catalog: self.catalog,
            cluster_state: self.cluster_state,
            cluster_manager: self.cluster_manager,
        });

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("HTTP accept error: {}", e);
                    continue;
                }
            };

            let io = TokioIo::new(stream);
            let state = state.clone();

            tokio::spawn(async move {
                let svc = service_fn(move |req| {
                    let state = state.clone();
                    async move { handle_request(req, state).await }
                });

                if let Err(err) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, svc)
                    .await
                {
                    tracing::debug!("HTTP connection error: {}", err);
                }
            });
        }
    }
}

struct ServerState {
    storage: Arc<Storage>,
    admin: Arc<AdminState>,
    catalog: Arc<Catalog>,
    cluster_state: Option<Arc<mgcoord::ClusterState>>,
    cluster_manager: Option<Arc<tokio::sync::RwLock<Option<mgcoord::ClusterManager>>>>,
}

async fn handle_request<B>(
    req: Request<B>,
    state: Arc<ServerState>,
) -> Result<Response<Body>, Infallible>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin + 'static,
    B::Error: std::fmt::Display + Send + Sync,
{
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let result = match (method, path.as_str()) {
        (Method::GET, "/health") => handle_health(&state).await,
        (Method::GET, "/metrics") => handle_metrics(&state).await,
        (Method::GET, "/api/v1/vertices") => handle_list_vertices(&state).await,
        (Method::GET, path) if path.starts_with("/api/v1/vertices/") && path.ends_with("/edges") => {
            handle_list_vertex_edges(&state, path).await
        }
        (Method::GET, path) if path.starts_with("/api/v1/vertices/") => {
            handle_get_vertex(&state, path).await
        }
        (Method::POST, "/api/v1/vertices") => handle_create_vertex(&state, req).await,
        (Method::POST, "/api/v1/vertices/batch") => handle_create_vertices_batch(&state, req).await,
        (Method::GET, "/api/v1/edges") => handle_list_edges(&state).await,
        (Method::GET, path) if path.starts_with("/api/v1/edges/") => {
            handle_get_edge(&state, path).await
        }
        (Method::POST, "/api/v1/query") => handle_query(&state, req).await,
        (Method::GET, "/api/v1/schema") => handle_schema(&state).await,
        (Method::GET, "/api/v1/stats") => handle_stats(&state).await,
        (Method::DELETE, path) if path.starts_with("/api/v1/vertices/") => {
            handle_delete_vertex(&state, path).await
        }
        (Method::DELETE, path) if path.starts_with("/api/v1/edges/") => {
            handle_delete_edge(&state, path).await
        }
        (Method::PUT, path) if path.starts_with("/api/v1/vertices/") => {
            handle_update_vertex(&state, path, req).await
        }
        (Method::PUT, path) if path.starts_with("/api/v1/edges/") => {
            handle_update_edge(&state, path, req).await
        }
        (Method::POST, "/api/v1/edges") => handle_create_edge(&state, req).await,
        (Method::POST, "/api/v1/edges/batch") => handle_create_edges_batch(&state, req).await,
        (Method::GET, path) if path.starts_with("/api/v1/labels/") && path.ends_with("/vertices") => {
            handle_list_vertices_by_label(&state, path).await
        }
        (Method::GET, "/api/v1/analytics") => handle_analytics(&state).await,
        (Method::GET, "/api/v1/labels") => handle_list_labels(&state).await,
        (Method::GET, "/api/v1/edge-types") => handle_list_edge_types(&state).await,
        (Method::GET, "/api/v1/properties") => handle_list_properties(&state).await,
        (Method::POST, "/api/v1/vertices/batch-delete") => handle_delete_vertices_batch(&state, req).await,
        (Method::POST, "/api/v1/edges/batch-delete") => handle_delete_edges_batch(&state, req).await,
        (Method::GET, "/api/v1/cluster/status") => handle_cluster_status(&state).await,
        (Method::GET, "/api/v1/cluster/instances") => handle_cluster_instances(&state).await,
        _ => Ok(error_response(StatusCode::NOT_FOUND, "not found")),
    };

    match result {
        Ok(resp) => Ok(resp),
        Err(e) => Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e))),
    }
}

// -- Endpoint handlers -------------------------------------------------------

async fn handle_health(state: &ServerState) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let health = state.admin.health_check();
    let body = serde_json::json!({
        "healthy": health.healthy,
        "active_connections": health.active_connections,
        "active_queries": health.active_queries,
        "uptime_secs": health.uptime_secs,
    });
    Ok(json_response(&body))
}

async fn handle_metrics(state: &ServerState) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let prom = state.admin.metrics().to_prometheus();
    Ok(text_response(&prom))
}

async fn handle_list_vertices(state: &ServerState) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let all = state.storage.all_vertices();
    let arr: Vec<serde_json::Value> = all
        .into_iter()
        .map(|(gid, labels, props)| vertex_to_json(gid, &labels, &props, &state.catalog))
        .collect();
    Ok(json_response(&serde_json::Value::Array(arr)))
}

async fn handle_get_vertex(
    state: &ServerState,
    path: &str,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let gid_str = path.strip_prefix("/api/v1/vertices/").unwrap_or("");
    let gid = match gid_str.parse::<u64>() {
        Ok(id) => Gid::from(id),
        Err(_) => return Ok(error_response(StatusCode::BAD_REQUEST, "invalid gid")),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    match state.storage.get_vertex(gid, &tx) {
        Some(snap) => Ok(json_response(&vertex_snapshot_to_json(&snap, &state.catalog))),
        None => Ok(error_response(StatusCode::NOT_FOUND, "vertex not found")),
    }
}

async fn handle_create_vertex<B>(
    state: &ServerState,
    req: Request<B>,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: std::fmt::Display + Send + Sync,
{
    let body = read_body(req).await?;
    let req_json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &format!("invalid JSON: {}", e))),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let gid = state.storage.allocate_gid();

    if let Err(e) = state.storage.create_vertex(&tx, gid) {
        return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
    }

    // Add labels
    if let Some(labels) = req_json.get("labels").and_then(|v| v.as_array()) {
        for label_val in labels {
            if let Some(name) = label_val.as_str() {
                let label_id = state.catalog.label(name);
                if let Err(e) = state.storage.vertex_add_label(&tx, gid, label_id) {
                    return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
                }
            }
        }
    }

    // Set properties
    if let Some(props) = req_json.get("properties").and_then(|v| v.as_object()) {
        for (key, val) in props {
            let prop_id = state.catalog.property(key);
            let pv = json_to_property_value(val);
            if let Err(e) = state.storage.vertex_set_property(&tx, gid, prop_id, pv) {
                return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
            }
        }
    }

    state.storage.commit_transaction(&tx);

    let tx2 = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    match state.storage.get_vertex(gid, &tx2) {
        Some(snap) => Ok(json_response(&vertex_snapshot_to_json(&snap, &state.catalog))),
        None => Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to read created vertex")),
    }
}

async fn handle_create_vertices_batch<B>(
    state: &ServerState,
    req: Request<B>,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: std::fmt::Display + Send + Sync,
{
    let body = read_body(req).await?;
    let req_json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &format!("invalid JSON: {}", e))),
    };

    let items = match req_json.get("vertices").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Ok(error_response(StatusCode::BAD_REQUEST, "missing 'vertices' array")),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let mut created = Vec::new();

    for item in items {
        let gid = state.storage.allocate_gid();
        if let Err(e) = state.storage.create_vertex(&tx, gid) {
            return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
        }

        if let Some(labels) = item.get("labels").and_then(|v| v.as_array()) {
            for label_val in labels {
                if let Some(name) = label_val.as_str() {
                    let label_id = state.catalog.label(name);
                    if let Err(e) = state.storage.vertex_add_label(&tx, gid, label_id) {
                        return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
                    }
                }
            }
        }

        if let Some(props) = item.get("properties").and_then(|v| v.as_object()) {
            for (key, val) in props {
                let prop_id = state.catalog.property(key);
                let pv = json_to_property_value(val);
                if let Err(e) = state.storage.vertex_set_property(&tx, gid, prop_id, pv) {
                    return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
                }
            }
        }

        created.push(gid.as_uint());
    }

    state.storage.commit_transaction(&tx);
    Ok(json_response(&serde_json::json!({ "created": created })))
}

async fn handle_list_edges(state: &ServerState) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let all = state.storage.all_edges();
    let arr: Vec<serde_json::Value> = all
        .into_iter()
        .map(|(gid, from, to, etype, props)| edge_to_json(gid, from, to, etype, &props, &state.catalog))
        .collect();
    Ok(json_response(&serde_json::Value::Array(arr)))
}

async fn handle_get_edge(
    state: &ServerState,
    path: &str,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let gid_str = path.strip_prefix("/api/v1/edges/").unwrap_or("");
    let gid = match gid_str.parse::<u64>() {
        Ok(id) => Gid::from(id),
        Err(_) => return Ok(error_response(StatusCode::BAD_REQUEST, "invalid gid")),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    match state.storage.get_edge(gid, &tx) {
        Some(snap) => Ok(json_response(&edge_snapshot_to_json(&snap, &state.catalog))),
        None => Ok(error_response(StatusCode::NOT_FOUND, "edge not found")),
    }
}

async fn handle_query<B>(
    state: &ServerState,
    req: Request<B>,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: std::fmt::Display + Send + Sync,
{
    let body = read_body(req).await?;
    let req_json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &format!("invalid JSON: {}", e))),
    };

    let query = match req_json.get("query").and_then(|v| v.as_str()) {
        Some(q) => q,
        None => return Ok(error_response(StatusCode::BAD_REQUEST, "missing 'query' field")),
    };

    // Parse optional query parameters
    let mut params: std::collections::HashMap<String, PropertyValue> = std::collections::HashMap::new();
    if let Some(params_json) = req_json.get("params").and_then(|v| v.as_object()) {
        for (key, val) in params_json {
            params.insert(key.clone(), json_to_property_value(val));
        }
    }

    let timeout = if state.admin.config().query_timeout_ms > 0 {
        Some(std::time::Duration::from_millis(state.admin.config().query_timeout_ms))
    } else {
        None
    };
    match execute_with_catalog_auth_dbms_and_params_timeout(
        &state.storage, query, Some(&state.catalog), &params, None, None, timeout,
    ) {
        Ok(result) => {
            let rows: Vec<serde_json::Value> = result.rows.iter().map(|row| {
                let mut obj = serde_json::Map::new();
                for col in &result.columns {
                    if let Some(val) = row.get(col) {
                        obj.insert(col.clone(), property_value_to_json(val));
                    }
                }
                serde_json::Value::Object(obj)
            }).collect();
            Ok(json_response(&serde_json::json!({
                "columns": result.columns,
                "rows": rows,
                "error": null,
            })))
        }
        Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &format!("{}", e))),
    }
}

async fn handle_schema(state: &ServerState) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let (labels, properties, edge_types) = state.catalog.dump_mappings();
    let label_names: Vec<String> = labels.into_iter().map(|(name, _)| name).collect();
    let prop_names: Vec<String> = properties.into_iter().map(|(name, _)| name).collect();
    let edge_type_names: Vec<String> = edge_types.into_iter().map(|(name, _)| name).collect();

    let indices = {
        let li = state.storage.active_label_indices.read().unwrap();
        let lpi = state.storage.active_label_property_indices.read().unwrap();
        let mut idxs = Vec::new();
        for label in li.iter() {
            idxs.push(serde_json::json!({
                "type": "label",
                "label": state.catalog.label_name(*label),
            }));
        }
        for (label, prop) in lpi.iter() {
            idxs.push(serde_json::json!({
                "type": "label+property",
                "label": state.catalog.label_name(*label),
                "property": state.catalog.property_name(*prop),
            }));
        }
        idxs
    };

    Ok(json_response(&serde_json::json!({
        "labels": label_names,
        "properties": prop_names,
        "edge_types": edge_type_names,
        "indices": indices,
    })))
}

async fn handle_stats(state: &ServerState) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let li = state.storage.active_label_indices.read().unwrap();
    let lpi = state.storage.active_label_property_indices.read().unwrap();
    Ok(json_response(&serde_json::json!({
        "vertex_count": state.storage.vertex_count(),
        "edge_count": state.storage.edge_count(),
        "index_count": li.len() + lpi.len(),
    })))
}

async fn handle_delete_vertex(
    state: &ServerState,
    path: &str,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let gid_str = path.strip_prefix("/api/v1/vertices/").unwrap_or("");
    let gid = match gid_str.parse::<u64>() {
        Ok(id) => Gid::from(id),
        Err(_) => return Ok(error_response(StatusCode::BAD_REQUEST, "invalid gid")),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    match state.storage.delete_vertex_and_edges(&tx, gid) {
        Ok((_vertex_count, _edge_count)) => {
            state.storage.commit_transaction(&tx);
            Ok(json_response(&serde_json::json!({ "deleted": true, "gid": gid.as_uint() })))
        }
        Err(e) => Ok(error_response(StatusCode::NOT_FOUND, &format!("vertex not found: {}", e))),
    }
}

async fn handle_delete_edge(
    state: &ServerState,
    path: &str,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let gid_str = path.strip_prefix("/api/v1/edges/").unwrap_or("");
    let gid = match gid_str.parse::<u64>() {
        Ok(id) => Gid::from(id),
        Err(_) => return Ok(error_response(StatusCode::BAD_REQUEST, "invalid gid")),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    match state.storage.delete_edge(&tx, gid) {
        Ok(()) => {
            state.storage.commit_transaction(&tx);
            Ok(json_response(&serde_json::json!({ "deleted": true, "gid": gid.as_uint() })))
        }
        Err(e) => Ok(error_response(StatusCode::NOT_FOUND, &format!("edge not found: {}", e))),
    }
}

async fn handle_update_vertex<B>(
    state: &ServerState,
    path: &str,
    req: Request<B>,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: std::fmt::Display + Send + Sync,
{
    let gid_str = path.strip_prefix("/api/v1/vertices/").unwrap_or("");
    let gid = match gid_str.parse::<u64>() {
        Ok(id) => Gid::from(id),
        Err(_) => return Ok(error_response(StatusCode::BAD_REQUEST, "invalid gid")),
    };

    let body = read_body(req).await?;
    let req_json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &format!("invalid JSON: {}", e))),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);

    // Update labels if provided
    if let Some(labels) = req_json.get("labels").and_then(|v| v.as_array()) {
        // Clear existing labels and set new ones
        let existing = state.storage.get_vertex(gid, &tx);
        if existing.is_none() {
            return Ok(error_response(StatusCode::NOT_FOUND, "vertex not found"));
        }
        for label_val in labels {
            if let Some(name) = label_val.as_str() {
                let label_id = state.catalog.label(name);
                let _ = state.storage.vertex_add_label(&tx, gid, label_id);
            }
        }
    }

    // Update properties if provided
    if let Some(props) = req_json.get("properties").and_then(|v| v.as_object()) {
        for (key, val) in props {
            let prop_id = state.catalog.property(key);
            let pv = json_to_property_value(val);
            if let Err(e) = state.storage.vertex_set_property(&tx, gid, prop_id, pv) {
                return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
            }
        }
    }

    state.storage.commit_transaction(&tx);

    let tx2 = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    match state.storage.get_vertex(gid, &tx2) {
        Some(snap) => Ok(json_response(&vertex_snapshot_to_json(&snap, &state.catalog))),
        None => Ok(error_response(StatusCode::NOT_FOUND, "vertex not found")),
    }
}

async fn handle_update_edge<B>(
    state: &ServerState,
    path: &str,
    req: Request<B>,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: std::fmt::Display + Send + Sync,
{
    let gid_str = path.strip_prefix("/api/v1/edges/").unwrap_or("");
    let gid = match gid_str.parse::<u64>() {
        Ok(id) => Gid::from(id),
        Err(_) => return Ok(error_response(StatusCode::BAD_REQUEST, "invalid gid")),
    };

    let body = read_body(req).await?;
    let req_json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &format!("invalid JSON: {}", e))),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);

    // Update properties if provided
    if let Some(props) = req_json.get("properties").and_then(|v| v.as_object()) {
        for (key, val) in props {
            let prop_id = state.catalog.property(key);
            let pv = json_to_property_value(val);
            if let Err(e) = state.storage.edge_set_property(&tx, gid, prop_id, pv) {
                return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
            }
        }
    }

    state.storage.commit_transaction(&tx);

    let tx2 = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    match state.storage.get_edge(gid, &tx2) {
        Some(snap) => Ok(json_response(&edge_snapshot_to_json(&snap, &state.catalog))),
        None => Ok(error_response(StatusCode::NOT_FOUND, "edge not found")),
    }
}

async fn handle_create_edge<B>(
    state: &ServerState,
    req: Request<B>,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: std::fmt::Display + Send + Sync,
{
    let body = read_body(req).await?;
    let req_json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &format!("invalid JSON: {}", e))),
    };

    let from_gid = req_json.get("from_vertex").and_then(|v| v.as_u64())
        .map(Gid::from)
        .ok_or_else(|| "missing from_vertex")?;
    let to_gid = req_json.get("to_vertex").and_then(|v| v.as_u64())
        .map(Gid::from)
        .ok_or_else(|| "missing to_vertex")?;
    let edge_type_name = req_json.get("edge_type").and_then(|v| v.as_str())
        .unwrap_or("REL");
    let etype = state.catalog.edge_type(edge_type_name);

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let gid = state.storage.allocate_gid();

    if let Err(e) = state.storage.create_edge(&tx, gid, from_gid, to_gid, etype) {
        return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
    }

    // Set properties if provided
    if let Some(props) = req_json.get("properties").and_then(|v| v.as_object()) {
        for (key, val) in props {
            let prop_id = state.catalog.property(key);
            let pv = json_to_property_value(val);
            if let Err(e) = state.storage.edge_set_property(&tx, gid, prop_id, pv) {
                return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
            }
        }
    }

    state.storage.commit_transaction(&tx);

    let tx2 = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    match state.storage.get_edge(gid, &tx2) {
        Some(snap) => Ok(json_response(&edge_snapshot_to_json(&snap, &state.catalog))),
        None => Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, "failed to read created edge")),
    }
}

async fn handle_create_edges_batch<B>(
    state: &ServerState,
    req: Request<B>,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: std::fmt::Display + Send + Sync,
{
    let body = read_body(req).await?;
    let req_json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &format!("invalid JSON: {}", e))),
    };

    let items = match req_json.get("edges").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Ok(error_response(StatusCode::BAD_REQUEST, "missing 'edges' array")),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let mut created = Vec::new();

    for item in items {
        let from_gid = item.get("from_vertex").and_then(|v| v.as_u64())
            .map(Gid::from)
            .ok_or_else(|| "missing from_vertex")?;
        let to_gid = item.get("to_vertex").and_then(|v| v.as_u64())
            .map(Gid::from)
            .ok_or_else(|| "missing to_vertex")?;
        let edge_type_name = item.get("edge_type").and_then(|v| v.as_str())
            .unwrap_or("REL");
        let etype = state.catalog.edge_type(edge_type_name);

        let gid = state.storage.allocate_gid();
        if let Err(e) = state.storage.create_edge(&tx, gid, from_gid, to_gid, etype) {
            return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
        }

        if let Some(props) = item.get("properties").and_then(|v| v.as_object()) {
            for (key, val) in props {
                let prop_id = state.catalog.property(key);
                let pv = json_to_property_value(val);
                if let Err(e) = state.storage.edge_set_property(&tx, gid, prop_id, pv) {
                    return Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{}", e)));
                }
            }
        }

        created.push(gid.as_uint());
    }

    state.storage.commit_transaction(&tx);
    Ok(json_response(&serde_json::json!({ "created": created })))
}

async fn handle_list_vertex_edges(
    state: &ServerState,
    path: &str,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let gid_str = path.strip_prefix("/api/v1/vertices/").and_then(|p| p.strip_suffix("/edges")).unwrap_or("");
    let gid = match gid_str.parse::<u64>() {
        Ok(id) => Gid::from(id),
        Err(_) => return Ok(error_response(StatusCode::BAD_REQUEST, "invalid gid")),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let mut edges = Vec::new();

    for (edge_gid, other, etype) in state.storage.vertex_out_edges(gid, None) {
        edges.push(serde_json::json!({
            "gid": edge_gid.as_uint(),
            "direction": "out",
            "other_vertex": other.as_uint(),
            "edge_type": state.catalog.edge_type_name(etype),
        }));
    }
    for (edge_gid, other, etype) in state.storage.vertex_in_edges(gid, None) {
        edges.push(serde_json::json!({
            "gid": edge_gid.as_uint(),
            "direction": "in",
            "other_vertex": other.as_uint(),
            "edge_type": state.catalog.edge_type_name(etype),
        }));
    }

    Ok(json_response(&serde_json::Value::Array(edges)))
}

async fn handle_list_vertices_by_label(
    state: &ServerState,
    path: &str,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let label_name = path.strip_prefix("/api/v1/labels/").and_then(|p| p.strip_suffix("/vertices")).unwrap_or("");
    if label_name.is_empty() {
        return Ok(error_response(StatusCode::BAD_REQUEST, "invalid label"));
    }
    let label_id = state.catalog.label(label_name);

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let all = state.storage.all_vertices();
    let arr: Vec<serde_json::Value> = all
        .into_iter()
        .filter(|(_, labels, _)| labels.contains(&label_id))
        .map(|(gid, labels, props)| vertex_to_json(gid, &labels, &props, &state.catalog))
        .collect();

    Ok(json_response(&serde_json::Value::Array(arr)))
}

async fn handle_analytics(
    state: &ServerState,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    use mgstorage::analytics;
    let stats = analytics::compute_graph_stats(&state.storage);
    let body = serde_json::json!({
        "vertex_count": stats.vertex_count,
        "edge_count": stats.edge_count,
        "avg_degree": stats.avg_degree,
        "max_in_degree": stats.max_in_degree,
        "max_out_degree": stats.max_out_degree,
        "density": stats.density,
        "connected_component_count": stats.connected_component_count,
        "isolated_vertex_count": stats.isolated_vertex_count,
        "average_clustering": analytics::average_clustering_coefficient(&state.storage),
        "reciprocity": analytics::reciprocity(&state.storage),
    });
    Ok(json_response(&body))
}

async fn handle_list_labels(
    state: &ServerState,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let (labels, _, _) = state.catalog.dump_mappings();
    let names: Vec<String> = labels.into_iter().map(|(name, _)| name).collect();
    Ok(json_response(&serde_json::json!({ "labels": names })))
}

async fn handle_list_edge_types(
    state: &ServerState,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let (_, _, edge_types) = state.catalog.dump_mappings();
    let names: Vec<String> = edge_types.into_iter().map(|(name, _)| name).collect();
    Ok(json_response(&serde_json::json!({ "edge_types": names })))
}

async fn handle_list_properties(
    state: &ServerState,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let (_, properties, _) = state.catalog.dump_mappings();
    let names: Vec<String> = properties.into_iter().map(|(name, _)| name).collect();
    Ok(json_response(&serde_json::json!({ "properties": names })))
}

async fn handle_delete_vertices_batch<B>(
    state: &ServerState,
    req: Request<B>,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: std::fmt::Display + Send + Sync,
{
    let body = read_body(req).await?;
    let req_json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &format!("invalid JSON: {}", e))),
    };
    let gids = match req_json.get("gids").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Ok(error_response(StatusCode::BAD_REQUEST, "missing 'gids' array")),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for gid_val in gids {
        if let Some(gid_num) = gid_val.as_u64() {
            let gid = Gid::from(gid_num);
            match state.storage.delete_vertex_and_edges(&tx, gid) {
                Ok(_) => deleted.push(gid_num),
                Err(e) => errors.push(format!("{}: {}", gid_num, e)),
            }
        }
    }

    state.storage.commit_transaction(&tx);
    Ok(json_response(&serde_json::json!({
        "deleted": deleted,
        "deleted_count": deleted.len(),
        "errors": errors,
    })))
}

async fn handle_delete_edges_batch<B>(
    state: &ServerState,
    req: Request<B>,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: std::fmt::Display + Send + Sync,
{
    let body = read_body(req).await?;
    let req_json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &format!("invalid JSON: {}", e))),
    };
    let gids = match req_json.get("gids").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Ok(error_response(StatusCode::BAD_REQUEST, "missing 'gids' array")),
    };

    let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for gid_val in gids {
        if let Some(gid_num) = gid_val.as_u64() {
            let gid = Gid::from(gid_num);
            match state.storage.delete_edge(&tx, gid) {
                Ok(_) => deleted.push(gid_num),
                Err(e) => errors.push(format!("{}: {}", gid_num, e)),
            }
        }
    }

    state.storage.commit_transaction(&tx);
    Ok(json_response(&serde_json::json!({
        "deleted": deleted,
        "deleted_count": deleted.len(),
        "errors": errors,
    })))
}

async fn handle_cluster_status(
    state: &ServerState,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let mut body = serde_json::Map::new();

    if let Some(ref cluster_state) = state.cluster_state {
        body.insert("instance_count".to_string(), serde_json::json!(cluster_state.instance_count()));
        body.insert("healthy_instance_count".to_string(), serde_json::json!(cluster_state.healthy_instance_count()));

        let routes: Vec<serde_json::Value> = cluster_state
            .routes()
            .into_iter()
            .map(|(db, addr)| serde_json::json!({"database": db, "address": addr.to_string()}))
            .collect();
        body.insert("routes".to_string(), serde_json::Value::Array(routes));
    } else {
        body.insert("enabled".to_string(), serde_json::Value::Bool(false));
    }

    if let Some(ref cm_ref) = state.cluster_manager {
        let guard = cm_ref.read().await;
        if let Some(ref cm) = *guard {
            body.insert("raft_enabled".to_string(), serde_json::Value::Bool(true));
            body.insert("is_leader".to_string(), serde_json::json!(cm.is_leader()));
            body.insert("leader_id".to_string(), serde_json::json!(cm.get_leader()));
        }
    }

    Ok(json_response(&serde_json::Value::Object(body)))
}

async fn handle_cluster_instances(
    state: &ServerState,
) -> Result<Response<Body>, Box<dyn std::error::Error + Send + Sync>> {
    let instances = match state.cluster_state {
        Some(ref cs) => cs
            .list()
            .into_iter()
            .map(|inst| {
                serde_json::json!({
                    "id": inst.id,
                    "address": inst.address.to_string(),
                    "role": format!("{:?}", inst.role),
                    "health": format!("{:?}", inst.health),
                    "replication_mode": format!("{:?}", inst.replication_mode),
                })
            })
            .collect(),
        None => Vec::new(),
    };

    Ok(json_response(&serde_json::Value::Array(instances)))
}

// -- JSON serialization helpers ----------------------------------------------

fn property_value_to_json(val: &PropertyValue) -> serde_json::Value {
    match val {
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::Bool(b) => serde_json::Value::Bool(*b),
        PropertyValue::Int(i) => serde_json::Value::Number((*i).into()),
        PropertyValue::Double(d) => {
            serde_json::Number::from_f64(*d)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        PropertyValue::String(s) => serde_json::Value::String(s.clone()),
        PropertyValue::List(items) => {
            serde_json::Value::Array(items.iter().map(property_value_to_json).collect())
        }
        PropertyValue::Map(m) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in m {
                obj.insert(k.clone(), property_value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        PropertyValue::Vertex(v) => {
            serde_json::json!({"gid": v.gid.as_uint(), "labels": v.labels.iter().map(|l| l.as_uint()).collect::<Vec<_>>() })
        }
        PropertyValue::Edge(e) => {
            serde_json::json!({"gid": e.gid.as_uint(), "edge_type": e.edge_type.as_uint(), "from": e.from_vertex.as_uint(), "to": e.to_vertex.as_uint() })
        }
        PropertyValue::Path(p) => {
            serde_json::json!({
                "vertices": p.vertices.iter().map(|v| v.gid.as_uint()).collect::<Vec<_>>(),
                "edges": p.edges.iter().map(|e| e.gid.as_uint()).collect::<Vec<_>>(),
            })
        }
        PropertyValue::Date(d) => serde_json::Value::String(format!("{}", d)),
        PropertyValue::LocalTime(t) => serde_json::Value::String(format!("{}", t)),
        PropertyValue::LocalDateTime(dt) => serde_json::Value::String(format!("{}", dt)),
        PropertyValue::ZonedDateTime(zdt) => serde_json::Value::String(format!("{}", zdt)),
        PropertyValue::Duration(dur) => serde_json::Value::String(format!("{}", dur)),
        PropertyValue::Point2D(p) => serde_json::Value::String(format!("{}", p)),
        PropertyValue::Point3D(p) => serde_json::Value::String(format!("{}", p)),
        PropertyValue::Enum { enum_type, value } => {
            serde_json::json!({"enum_type": enum_type, "value": value})
        }
    }
}

fn json_to_property_value(val: &serde_json::Value) -> PropertyValue {
    match val {
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Bool(b) => PropertyValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Double(f)
            } else {
                PropertyValue::Null
            }
        }
        serde_json::Value::String(s) => PropertyValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            PropertyValue::List(arr.iter().map(json_to_property_value).collect())
        }
        serde_json::Value::Object(obj) => {
            let mut entries = Vec::new();
            for (k, v) in obj {
                entries.push((k.clone(), json_to_property_value(v)));
            }
            PropertyValue::Map(entries)
        }
    }
}

fn property_store_to_json(store: &PropertyStore, catalog: &Catalog) -> serde_json::Map<String, serde_json::Value> {
    let mut obj = serde_json::Map::new();
    for (prop_id, val) in store.iter() {
        let name = catalog.property_name(prop_id);
        obj.insert(name, property_value_to_json(val));
    }
    obj
}

fn vertex_to_json(gid: Gid, labels: &[LabelId], props: &PropertyStore, catalog: &Catalog) -> serde_json::Value {
    let label_names: Vec<String> = labels.iter().map(|l| catalog.label_name(*l)).collect();
    serde_json::json!({
        "gid": gid.as_uint(),
        "labels": label_names,
        "properties": property_store_to_json(props, catalog),
    })
}

fn vertex_snapshot_to_json(snap: &VertexSnapshot, catalog: &Catalog) -> serde_json::Value {
    vertex_to_json(snap.gid, &snap.labels, &snap.properties, catalog)
}

fn edge_to_json(gid: Gid, from: Gid, to: Gid, etype: mgcore::types::EdgeTypeId, props: &PropertyStore, catalog: &Catalog) -> serde_json::Value {
    serde_json::json!({
        "gid": gid.as_uint(),
        "from_vertex": from.as_uint(),
        "to_vertex": to.as_uint(),
        "edge_type": catalog.edge_type_name(etype),
        "properties": property_store_to_json(props, catalog),
    })
}

fn edge_snapshot_to_json(snap: &EdgeSnapshot, catalog: &Catalog) -> serde_json::Value {
    edge_to_json(snap.gid, snap.from_vertex, snap.to_vertex, snap.edge_type, &snap.properties, catalog)
}

// -- Response builders -------------------------------------------------------

fn json_response(value: &serde_json::Value) -> Response<Body> {
    let body = Bytes::from(value.to_string());
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Full::new(body))
        .unwrap()
}

fn text_response(text: &str) -> Response<Body> {
    let body = Bytes::from(text.to_string());
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(body))
        .unwrap()
}

fn error_response(status: StatusCode, msg: &str) -> Response<Body> {
    let body = Bytes::from(format!("{{\"error\":\"{}\"}}", msg));
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(body))
        .unwrap()
}

async fn read_body<B>(req: Request<B>) -> Result<String, Box<dyn std::error::Error + Send + Sync>>
where
    B: http_body::Body<Data = Bytes> + Send + Sync + Unpin,
    B::Error: std::fmt::Display + Send + Sync,
{
    use http_body_util::BodyExt;
    let mut body = req.into_body();
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| e.to_string())?;
        if let Some(data) = frame.data_ref() {
            bytes.extend_from_slice(data);
        }
    }
    String::from_utf8(bytes).map_err(|e| e.to_string().into())
}

// -- Tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_test_state() -> Arc<ServerState> {
        let storage = Arc::new(Storage::new());
        let catalog = Arc::new(Catalog::new());
        let admin = AdminState::new();
        Arc::new(ServerState {
            storage,
            admin,
            catalog,
            cluster_state: None,
            cluster_manager: None,
        })
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let state = setup_test_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let state = setup_test_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/metrics")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("memgraph_queries_total"));
    }

    #[tokio::test]
    async fn test_list_vertices_empty() {
        let state = setup_test_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/vertices")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert_eq!(body, "[]");
    }

    #[tokio::test]
    async fn test_create_and_get_vertex() {
        let state = setup_test_state();

        // Create vertex
        let create_req = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/vertices")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"labels":["Person"],"properties":{"name":"Alice","age":30}}"#
            )))
            .unwrap();
        let resp = handle_request(create_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("Alice"));
        assert!(body.contains("Person"));

        // List vertices
        let list_req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/vertices")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(list_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("Alice"));

        // Get specific vertex (gid=1 is first allocated)
        let get_req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/vertices/1")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(get_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("Alice"));

        // Get non-existent vertex
        let get_missing = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/vertices/9999")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(get_missing, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_query_endpoint() {
        let state = setup_test_state();

        // Create a vertex first
        let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = state.storage.allocate_gid();
        state.storage.create_vertex(&tx, gid).unwrap();
        state.storage.vertex_add_label(&tx, gid, state.catalog.label("Person")).unwrap();
        state.storage.commit_transaction(&tx);

        let query_req = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/query")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"query":"MATCH (n:Person) RETURN n"}"#
            )))
            .unwrap();
        let resp = handle_request(query_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("columns"));
        assert!(body.contains("rows"));
    }

    #[tokio::test]
    async fn test_stats_endpoint() {
        let state = setup_test_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/stats")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("vertex_count"));
        assert!(body.contains("edge_count"));
    }

    #[tokio::test]
    async fn test_schema_endpoint() {
        let state = setup_test_state();
        // Pre-populate catalog
        state.catalog.label("Person");
        state.catalog.property("name");
        state.catalog.edge_type("KNOWS");

        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/schema")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("Person"));
        assert!(body.contains("name"));
        assert!(body.contains("KNOWS"));
    }

    #[tokio::test]
    async fn test_bad_query_returns_400() {
        let state = setup_test_state();
        let query_req = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/query")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"query":"INVALID CYPHER!!!"}"#
            )))
            .unwrap();
        let resp = handle_request(query_req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_not_found_returns_404() {
        let state = setup_test_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/nonexistent")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_vertex() {
        let state = setup_test_state();

        // Create vertex
        let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = state.storage.allocate_gid();
        state.storage.create_vertex(&tx, gid).unwrap();
        state.storage.vertex_set_property(&tx, gid, state.catalog.property("name"), PropertyValue::String("Alice".into())).unwrap();
        state.storage.commit_transaction(&tx);

        // Delete vertex
        let del_req = Request::builder()
            .method(Method::DELETE)
            .uri(format!("/api/v1/vertices/{}", gid.as_uint()))
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(del_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify deleted
        let get_req = Request::builder()
            .method(Method::GET)
            .uri(format!("/api/v1/vertices/{}", gid.as_uint()))
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(get_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_edge() {
        let state = setup_test_state();

        let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v1 = state.storage.allocate_gid();
        let v2 = state.storage.allocate_gid();
        state.storage.create_vertex(&tx, v1).unwrap();
        state.storage.create_vertex(&tx, v2).unwrap();
        let e1 = state.storage.allocate_gid();
        let etype = state.catalog.edge_type("KNOWS");
        state.storage.create_edge(&tx, e1, v1, v2, etype).unwrap();
        state.storage.commit_transaction(&tx);

        let del_req = Request::builder()
            .method(Method::DELETE)
            .uri(format!("/api/v1/edges/{}", e1.as_uint()))
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(del_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let get_req = Request::builder()
            .method(Method::GET)
            .uri(format!("/api/v1/edges/{}", e1.as_uint()))
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(get_req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_update_vertex() {
        let state = setup_test_state();

        let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = state.storage.allocate_gid();
        state.storage.create_vertex(&tx, gid).unwrap();
        state.storage.vertex_set_property(&tx, gid, state.catalog.property("name"), PropertyValue::String("Alice".into())).unwrap();
        state.storage.commit_transaction(&tx);

        let put_req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/api/v1/vertices/{}", gid.as_uint()))
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"properties":{"name":"Alicia","age":31}}"#
            )))
            .unwrap();
        let resp = handle_request(put_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("Alicia"));
        assert!(body.contains("31"));
    }

    #[tokio::test]
    async fn test_update_edge() {
        let state = setup_test_state();

        let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v1 = state.storage.allocate_gid();
        let v2 = state.storage.allocate_gid();
        state.storage.create_vertex(&tx, v1).unwrap();
        state.storage.create_vertex(&tx, v2).unwrap();
        let e1 = state.storage.allocate_gid();
        let etype = state.catalog.edge_type("KNOWS");
        state.storage.create_edge(&tx, e1, v1, v2, etype).unwrap();
        state.storage.edge_set_property(&tx, e1, state.catalog.property("since"), PropertyValue::Int(2020)).unwrap();
        state.storage.commit_transaction(&tx);

        let put_req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/api/v1/edges/{}", e1.as_uint()))
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"properties":{"since":2024}}"#
            )))
            .unwrap();
        let resp = handle_request(put_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("2024"));
    }

    #[tokio::test]
    async fn test_create_edge_and_get() {
        let state = setup_test_state();

        // Create two vertices
        let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v1 = state.storage.allocate_gid();
        let v2 = state.storage.allocate_gid();
        state.storage.create_vertex(&tx, v1).unwrap();
        state.storage.create_vertex(&tx, v2).unwrap();
        let e1 = state.storage.allocate_gid();
        let etype = state.catalog.edge_type("KNOWS");
        state.storage.create_edge(&tx, e1, v1, v2, etype).unwrap();
        state.storage.commit_transaction(&tx);

        // List edges
        let list_req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/edges")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(list_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("KNOWS"));

        // Get specific edge
        let get_req = Request::builder()
            .method(Method::GET)
            .uri(&format!("/api/v1/edges/{}", e1.as_uint()))
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(get_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("KNOWS"));

        // Missing edge
        let get_missing = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/edges/9999")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(get_missing, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_create_edge_via_rest() {
        let state = setup_test_state();

        // Create two vertices
        let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v1 = state.storage.allocate_gid();
        let v2 = state.storage.allocate_gid();
        state.storage.create_vertex(&tx, v1).unwrap();
        state.storage.create_vertex(&tx, v2).unwrap();
        state.storage.commit_transaction(&tx);

        let create_req = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/edges")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(
                format!(r#"{{"from_vertex":{},"to_vertex":{},"edge_type":"KNOWS","properties":{{"since":2020}}}}"#, v1.as_uint(), v2.as_uint())
            )))
            .unwrap();
        let resp = handle_request(create_req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("KNOWS"));
    }

    #[tokio::test]
    async fn test_list_vertex_edges() {
        let state = setup_test_state();

        let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v1 = state.storage.allocate_gid();
        let v2 = state.storage.allocate_gid();
        state.storage.create_vertex(&tx, v1).unwrap();
        state.storage.create_vertex(&tx, v2).unwrap();
        let e1 = state.storage.allocate_gid();
        let etype = state.catalog.edge_type("KNOWS");
        state.storage.create_edge(&tx, e1, v1, v2, etype).unwrap();
        state.storage.commit_transaction(&tx);

        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("/api/v1/vertices/{}/edges", v1.as_uint()))
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("KNOWS"));
        assert!(body.contains("out"));
    }

    #[tokio::test]
    async fn test_list_vertices_by_label() {
        let state = setup_test_state();

        let tx = state.storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        let gid = state.storage.allocate_gid();
        state.storage.create_vertex(&tx, gid).unwrap();
        state.storage.vertex_add_label(&tx, gid, state.catalog.label("Person")).unwrap();
        state.storage.commit_transaction(&tx);

        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/labels/Person/vertices")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("Person"));
    }

    #[tokio::test]
    async fn test_analytics_endpoint() {
        let state = setup_test_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/analytics")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("vertex_count"));
        assert!(body.contains("density"));
    }

    #[tokio::test]
    async fn test_cluster_status_disabled() {
        let state = setup_test_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/cluster/status")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("enabled") || body.contains("raft_enabled"));
    }

    #[tokio::test]
    async fn test_cluster_instances_empty() {
        let state = setup_test_state();
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/cluster/instances")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert_eq!(body, "[]");
    }

    #[tokio::test]
    async fn test_cluster_status_with_state() {
        let storage = Arc::new(Storage::new());
        let catalog = Arc::new(Catalog::new());
        let admin = AdminState::new();
        let cluster_state = Arc::new(mgcoord::ClusterState::new("coord-1".into()));
        cluster_state.register(mgcoord::Instance::new(
            "i1".into(),
            std::net::SocketAddr::from(([127, 0, 0, 1], 7687)),
            mgcoord::InstanceRole::Main,
        ));
        let state = Arc::new(ServerState {
            storage,
            admin,
            catalog,
            cluster_state: Some(cluster_state),
            cluster_manager: None,
        });

        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/cluster/status")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(req, state.clone()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("instance_count"));

        let instances_req = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/cluster/instances")
            .body(Empty::<Bytes>::new())
            .unwrap();
        let resp = handle_request(instances_req, state).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_body_to_string(resp).await;
        assert!(body.contains("i1"));
    }

    // Helper to read response body in tests
    async fn read_body_to_string(resp: Response<Body>) -> String {
        use http_body_util::BodyExt;
        let body = resp.into_body();
        let bytes = body.collect()
            .await
            .map(|c| c.to_bytes())
            .unwrap_or_default();
        String::from_utf8(bytes.to_vec()).unwrap_or_default()
    }
}
