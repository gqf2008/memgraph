//! Async HTTP + WebSocket server for Memgraph monitoring and management.
//!
//! Uses `hyper` 1.x for async request handling. Routes:
//! - GET  /health              → health check
//! - GET  /version             → server version
//! - GET  /stats               → vertex/edge counts, storage info
//! - GET  /stats/detailed      → detailed statistics
//! - GET  /schema              → labels, edge types, indices
//! - GET  /metrics             → Prometheus format
//! - POST /query               → execute Cypher query
//! - POST /explain             → explain query plan
//! - POST /profile             → profile query execution
//! - GET  /databases           → list databases
//! - GET  /sessions            → active sessions
//! - GET  /queries             → active queries
//! - GET  /config              → server configuration
//! - POST /config              → update configuration
//! - GET  /logs                → recent log entries
//! - POST /backup              → trigger backup
//! - GET  /indices             → list indices
//! - POST /indices             → create index
//! - DELETE /indices           → drop index
//! - GET  /constraints         → list constraints
//! - POST /constraints         → create constraint
//! - DELETE /constraints       → drop constraint
//! - POST /graph/import        → import JSONL data
//! - GET  /graph/export        → export graph as JSONL

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::body::{Bytes, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use mgcore::property_value::PropertyValue;
use mgstorage::storage::Storage;

use crate::HttpApi;
use crate::websocket::{LogBroadcaster, WebSocketServer};

/// HTTP response body type.
type BoxBody = hyper::body::Incoming;

/// Start both HTTP and WebSocket servers (if enabled in flags).
pub async fn run_servers(
    addr: SocketAddr,
    storage: Arc<Storage>,
    flags: mgflags::Flags,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let broadcaster = Arc::new(LogBroadcaster::new(1024));

    // Start WebSocket server if enabled
    if flags.websocket_enabled && flags.websocket_port != 0 {
        if let Some(ws_server) = crate::websocket::server_from_flags(&flags, broadcaster.clone()) {
            tokio::spawn(async move {
                if let Err(e) = ws_server.run().await {
                    tracing::error!("WebSocket server error: {}", e);
                }
            });
        }
    }

    // Start HTTP server (this blocks)
    run_server(addr, storage, flags).await
}

/// Start the HTTP server.
pub async fn run_server(
    addr: SocketAddr,
    storage: Arc<Storage>,
    flags: mgflags::Flags,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("HTTP server listening on http://{}", addr);

    let api = Arc::new(HttpApi::new(storage, flags));

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let api = api.clone();

        tokio::spawn(async move {
            let svc = service_fn(move |req| {
                let api = api.clone();
                async move { handle_request(req, api).await }
            });

            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                tracing::warn!("HTTP connection error: {}", err);
            }
        });
    }
}

async fn handle_request(
    req: Request<Incoming>,
    api: Arc<HttpApi>,
) -> Result<Response<Incoming>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let result = match (method.as_str(), path.as_str()) {
        // Health & metadata
        ("GET", "/health") => Ok(json_response(&api.health_check())),
        ("GET", "/version") => Ok(json_response(&api.version())),

        // Statistics
        ("GET", "/stats") => Ok(json_response(&api.get_stats())),
        ("GET", "/stats/detailed") => Ok(json_response(&api.get_detailed_stats())),
        ("GET", "/schema") => Ok(json_response(&api.get_schema())),
        ("GET", "/metrics") => Ok(text_response(&api.get_metrics())),

        // Query execution
        ("POST", "/query") => {
            let body = match read_body(req).await {
                Ok(b) => b,
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e)),
            };
            match serde_json::from_str::<QueryRequest>(&body) {
                Ok(qr) => {
                    let params = qr.parameters.unwrap_or_default();
                    match api.execute_query(&qr.query, Some(params)) {
                        Ok(result) => Ok(json_response(&result)),
                        Err(e) => Ok(error_response(StatusCode::INTERNAL_SERVER_ERROR, &e)),
                    }
                }
                Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &format!("{}", e))),
            }
        }
        ("POST", "/explain") => {
            let body = match read_body(req).await {
                Ok(b) => b,
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e)),
            };
            match serde_json::from_str::<QueryOnlyRequest>(&body) {
                Ok(qr) => match api.explain_query(&qr.query) {
                    Ok(result) => Ok(json_response(&result)),
                    Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &e)),
                },
                Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &format!("{}", e))),
            }
        }
        ("POST", "/profile") => {
            let body = match read_body(req).await {
                Ok(b) => b,
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e)),
            };
            match serde_json::from_str::<QueryOnlyRequest>(&body) {
                Ok(qr) => match api.profile_query(&qr.query) {
                    Ok(result) => Ok(json_response(&result)),
                    Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &e)),
                },
                Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &format!("{}", e))),
            }
        }

        // Admin
        ("GET", "/databases") => Ok(json_response(&api.list_databases())),
        ("GET", "/sessions") => Ok(json_response(&api.list_sessions())),
        ("GET", "/queries") => Ok(json_response(&api.list_queries())),
        ("GET", "/config") => Ok(json_response(&api.get_config())),
        ("POST", "/config") => {
            let body = match read_body(req).await {
                Ok(b) => b,
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e)),
            };
            match serde_json::from_str::<ConfigUpdateRequest>(&body) {
                Ok(req) => match api.update_config(&req.key, req.value) {
                    Ok(()) => Ok(json_response(&serde_json::json!({"status": "updated"}))),
                    Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &e)),
                },
                Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &format!("{}", e))),
            }
        }
        ("GET", "/logs") => {
            let limit = req.uri().query()
                .and_then(|q| {
                    q.split('&').find(|p| p.starts_with("limit="))
                        .and_then(|p| p.strip_prefix("limit="))
                        .and_then(|v| v.parse::<usize>().ok())
                })
                .unwrap_or(100);
            Ok(json_response(&api.get_logs(limit)))
        }
        ("POST", "/backup") => Ok(json_response(&api.trigger_backup())),

        // Index management
        ("GET", "/indices") => Ok(json_response(&api.list_indices())),
        ("POST", "/indices") => {
            let body = match read_body(req).await {
                Ok(b) => b,
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e)),
            };
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(json) => match api.create_index(&json) {
                    Ok(result) => Ok(json_response(&result)),
                    Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &e)),
                },
                Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &format!("{}", e))),
            }
        }
        ("DELETE", "/indices") => {
            let body = match read_body(req).await {
                Ok(b) => b,
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e)),
            };
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(json) => match api.drop_index(&json) {
                    Ok(result) => Ok(json_response(&result)),
                    Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &e)),
                },
                Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &format!("{}", e))),
            }
        }

        // Constraint management
        ("GET", "/constraints") => Ok(json_response(&api.list_constraints())),
        ("POST", "/constraints") => {
            let body = match read_body(req).await {
                Ok(b) => b,
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e)),
            };
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(json) => match api.create_constraint(&json) {
                    Ok(result) => Ok(json_response(&result)),
                    Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &e)),
                },
                Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &format!("{}", e))),
            }
        }
        ("DELETE", "/constraints") => {
            let body = match read_body(req).await {
                Ok(b) => b,
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e)),
            };
            match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(json) => match api.drop_constraint(&json) {
                    Ok(result) => Ok(json_response(&result)),
                    Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &e)),
                },
                Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &format!("{}", e))),
            }
        }

        // Graph import / export
        ("POST", "/graph/import") => {
            let body = match read_body(req).await {
                Ok(b) => b,
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, &e)),
            };
            match import_jsonl(&api.storage, &body) {
                Ok(count) => Ok(json_response(&serde_json::json!({
                    "status": "imported",
                    "count": count,
                }))),
                Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, &e)),
            }
        }
        ("GET", "/graph/export") => {
            let jsonl = export_graph_jsonl(&api.storage);
            Ok(text_response(&jsonl))
        }

        _ => Ok(error_response(StatusCode::NOT_FOUND, "not found")),
    };

    result
}

#[derive(serde::Deserialize)]
struct QueryRequest {
    query: String,
    parameters: Option<std::collections::HashMap<String, serde_json::Value>>,
}

#[derive(serde::Deserialize)]
struct QueryOnlyRequest {
    query: String,
}

#[derive(serde::Deserialize)]
struct ConfigUpdateRequest {
    key: String,
    value: serde_json::Value,
}

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
                obj.insert(format!("{}", k), property_value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::String(format!("{:?}", val)),
    }
}

fn json_response(value: &serde_json::Value) -> Response<Incoming> {
    let body = Bytes::from(value.to_string());
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Incoming::new(hyper::body::Frame::data(body)))
        .unwrap()
}

fn text_response(text: &str) -> Response<Incoming> {
    let body = Bytes::from(text.to_string());
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/plain")
        .body(Incoming::new(hyper::body::Frame::data(body)))
        .unwrap()
}

fn error_response(status: StatusCode, msg: &str) -> Response<Incoming> {
    let body = Bytes::from(format!("{{\"error\":\"{}\"}}", msg));
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Incoming::new(hyper::body::Frame::data(body)))
        .unwrap()
}

async fn read_body(req: Request<Incoming>) -> Result<String, String> {
    use hyper::body::Body;
    let mut body = req.into_body();
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| e.to_string())?;
        if let Some(data) = frame.data_ref() {
            bytes.extend_from_slice(data);
        }
    }
    String::from_utf8(bytes).map_err(|e| e.to_string())
}

/// Import vertices/edges from JSONL string.
fn import_jsonl(storage: &Storage, jsonl: &str) -> Result<usize, String> {
    let mut count = 0;
    for line in jsonl.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let obj: serde_json::Value = serde_json::from_str(line).map_err(|e| format!("{}", e))?;
        if let Some(query) = json_to_create(&obj) {
            mginterp::execute(storage, &query).map_err(|e| format!("{}", e))?;
            count += 1;
        }
    }
    Ok(count)
}

fn json_to_create(obj: &serde_json::Value) -> Option<String> {
    let typ = obj.get("type").and_then(|v| v.as_str())?;
    match typ {
        "node" | "vertex" => {
            let label = obj.get("label").and_then(|v| v.as_str()).unwrap_or("Node");
            let props = obj.get("properties").and_then(|v| v.as_object())
                .map(|m| {
                    let parts: Vec<String> = m.iter().map(|(k, v)| {
                        match v {
                            serde_json::Value::String(s) => format!("{}: \"{}\"", k, s),
                            serde_json::Value::Number(n) => format!("{}: {}", k, n),
                            serde_json::Value::Bool(b) => format!("{}: {}", k, b),
                            _ => format!("{}: null", k),
                        }
                    }).collect();
                    format!("{{{}}}", parts.join(", "))
                })
                .unwrap_or_default();
            Some(format!("CREATE (n:{} {})", label, props))
        }
        "edge" | "relationship" => {
            let from = obj.get("from").and_then(|v| v.as_str())?;
            let to = obj.get("to").and_then(|v| v.as_str())?;
            let etype = obj.get("edge_type").and_then(|v| v.as_str()).unwrap_or("REL");
            Some(format!(
                "MATCH (a {{key: \"{}\"}}), (b {{key: \"{}\"}}) CREATE (a)-[:{}]->(b)",
                from, to, etype
            ))
        }
        _ => None,
    }
}

/// Export all vertices and edges as JSONL.
fn export_graph_jsonl(storage: &Storage) -> String {
    let mut lines = Vec::new();

    // Export vertices
    let vertices = storage.all_vertices();
    for (gid, labels, properties) in vertices {
        let label_str = labels.first()
            .map(|l| format!("{}", l.as_uint()))
            .unwrap_or_else(|| "Node".to_string());

        let props: serde_json::Map<String, serde_json::Value> = properties.iter()
            .filter_map(|(k, v)| {
                property_value_to_json(v).map(|jv| (format!("{}", k.as_uint()), jv))
            })
            .collect();

        let obj = serde_json::json!({
            "type": "node",
            "id": gid.as_uint(),
            "label": label_str,
            "properties": props,
        });
        lines.push(obj.to_string());
    }

    // Export edges
    let edges = storage.all_edges();
    for (gid, from, to, edge_type, properties) in edges {
        let props: serde_json::Map<String, serde_json::Value> = properties.iter()
            .filter_map(|(k, v)| {
                property_value_to_json(v).map(|jv| (format!("{}", k.as_uint()), jv))
            })
            .collect();

        let obj = serde_json::json!({
            "type": "edge",
            "id": gid.as_uint(),
            "from": from.as_uint(),
            "to": to.as_uint(),
            "edge_type": format!("{}", edge_type.as_uint()),
            "properties": props,
        });
        lines.push(obj.to_string());
    }

    lines.join("\n")
}
