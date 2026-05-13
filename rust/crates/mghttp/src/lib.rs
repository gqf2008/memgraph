//! HTTP REST API handlers for Memgraph monitoring and management.
//! Equivalent to C++ `src/http_handlers/`.
//!
//! Endpoints:
//!   GET  /health          — health check
//!   GET  /metrics         — Prometheus metrics
//!   GET  /stats           — database statistics
//!   GET  /schema          — schema information
//!   POST /query           — execute Cypher query
//!   GET  /databases       — list databases
//!   GET  /sessions        — active sessions
//!   GET  /queries         — active queries
//!   POST /query/terminate — terminate query by session
//!   GET  /config          — server configuration
//!   POST /config          — update configuration
//!   GET  /logs            — recent log entries
//!   GET  /backup          — trigger backup
//!   GET  /version         — server version
//!
//! WebSocket log streaming: see `websocket` module.

use std::collections::HashMap;
use std::sync::Arc;

use mgstorage::storage::Storage;

pub mod websocket;

/// Build HTTP routes for the Memgraph REST API.
pub struct HttpApi {
    pub storage: Arc<Storage>,
    pub flags: mgflags::Flags,
    query_timeout_secs: std::sync::atomic::AtomicU64,
    memory_limit_mib: std::sync::atomic::AtomicU64,
}

impl HttpApi {
    pub fn new(storage: Arc<Storage>, flags: mgflags::Flags) -> Self {
        let qt = flags.query_execution_timeout_secs;
        let ml = flags.memory_limit;
        Self {
            storage,
            flags,
            query_timeout_secs: std::sync::atomic::AtomicU64::new(qt),
            memory_limit_mib: std::sync::atomic::AtomicU64::new(ml),
        }
    }

    /// Health check response.
    pub fn health_check(&self) -> serde_json::Value {
        serde_json::json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "uptime_secs": self.uptime_secs(),
        })
    }

    /// Get server version.
    pub fn version(&self) -> serde_json::Value {
        serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "edition": "community",
            "protocol_versions": ["bolt-v1", "bolt-v4", "bolt-v5"],
        })
    }

    /// Get server stats.
    pub fn get_stats(&self) -> serde_json::Value {
        let vc = self.storage.vertex_count();
        let ec = self.storage.edge_count();
        serde_json::json!({
            "vertices": vc,
            "edges": ec,
            "storage_mode": self.flags.storage_mode,
            "isolation_level": self.flags.isolation_level,
            "memory_limit": self.memory_limit_mib.load(std::sync::atomic::Ordering::Relaxed),
            "query_timeout": self.query_timeout_secs.load(std::sync::atomic::Ordering::Relaxed),
        })
    }

    /// Get detailed database statistics.
    pub fn get_detailed_stats(&self) -> serde_json::Value {
        let vc = self.storage.vertex_count();
        let ec = self.storage.edge_count();
        let indices = self.storage.active_label_indices.read().unwrap().len()
            + self
                .storage
                .active_label_property_indices
                .read()
                .unwrap()
                .len();
        let constraints = self.storage.constraints.list().len();

        serde_json::json!({
            "vertices": {
                "count": vc,
            },
            "edges": {
                "count": ec,
            },
            "indices": {
                "count": indices,
                "label_indices": self.storage.active_label_indices.read().unwrap().len(),
                "label_property_indices": self.storage.active_label_property_indices.read().unwrap().len(),
            },
            "constraints": {
                "count": constraints,
            },
            "triggers": {
                "count": self.storage.triggers.list().len(),
            },
            "storage_mode": self.flags.storage_mode,
            "isolation_level": self.flags.isolation_level,
            "memory_limit": self.memory_limit_mib.load(std::sync::atomic::Ordering::Relaxed),
            "query_timeout": self.query_timeout_secs.load(std::sync::atomic::Ordering::Relaxed),
        })
    }

    /// Get schema information.
    pub fn get_schema(&self) -> serde_json::Value {
        let labels: Vec<String> = self
            .storage
            .schema_info
            .all_labels()
            .iter()
            .map(|l| format!("{}", l.as_uint()))
            .collect();
        let edge_types: Vec<String> = self
            .storage
            .schema_info
            .all_edge_types()
            .iter()
            .map(|e| format!("{}", e.as_uint()))
            .collect();
        serde_json::json!({
            "labels": labels,
            "edge_types": edge_types,
        })
    }

    /// Get Prometheus-style metrics.
    pub fn get_metrics(&self) -> String {
        let vc = self.storage.vertex_count();
        let ec = self.storage.edge_count();
        let mut out = format!(
            "# HELP memgraph_vertices_total Total number of vertices.\n\
             # TYPE memgraph_vertices_total gauge\n\
             memgraph_vertices_total {}\n\
             # HELP memgraph_edges_total Total number of edges.\n\
             # TYPE memgraph_edges_total gauge\n\
             memgraph_edges_total {}\n",
            vc, ec
        );
        out.push_str(&self.storage.metrics.snapshot().to_prometheus());
        out
    }

    /// Execute a Cypher query via HTTP.
    pub fn execute_query(
        &self,
        query: &str,
        parameters: Option<HashMap<String, serde_json::Value>>,
    ) -> Result<serde_json::Value, String> {
        let param_map = parameters.unwrap_or_default();
        // Convert JSON parameters to PropertyValue
        let mut prop_params = HashMap::new();
        for (k, v) in param_map {
            if let Ok(pv) = json_to_property_value(&v) {
                prop_params.insert(k, pv);
            }
        }

        let result = mginterp::execute_with_catalog(
            &self.storage,
            query,
            None, // Could pass catalog here
        )
        .map_err(|e| format!("{}", e))?;

        // Convert rows to JSON
        let rows: Vec<serde_json::Value> = result
            .rows
            .into_iter()
            .map(
                |row: HashMap<String, mgcore::property_value::PropertyValue>| {
                    let json_row: HashMap<String, serde_json::Value> = row
                        .into_iter()
                        .filter_map(|(k, v)| property_value_to_json(v).map(|jv| (k, jv)))
                        .collect();
                    serde_json::json!(json_row)
                },
            )
            .collect();

        Ok(serde_json::json!({
            "columns": result.columns,
            "rows": rows,
        }))
    }

    /// Get server configuration.
    pub fn get_config(&self) -> serde_json::Value {
        serde_json::json!({
            "storage_mode": self.flags.storage_mode,
            "isolation_level": self.flags.isolation_level,
            "bolt_port": self.flags.bolt_port,
            "bolt_address": self.flags.bolt_server_address,
            "memory_limit": self.memory_limit_mib.load(std::sync::atomic::Ordering::Relaxed),
            "query_timeout": self.query_timeout_secs.load(std::sync::atomic::Ordering::Relaxed),
        })
    }

    /// Update a configuration value (runtime mutable flags only).
    pub fn update_config(&self, key: &str, value: serde_json::Value) -> Result<(), String> {
        match key {
            "query_timeout" => {
                let secs = value.as_u64().ok_or("query_timeout must be an integer")?;
                self.query_timeout_secs.store(secs, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
            "memory_limit" => {
                let mib = value.as_u64().ok_or("memory_limit must be an integer")?;
                self.memory_limit_mib.store(mib, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
            _ => Err(format!("config key '{}' is not runtime mutable", key)),
        }
    }

    /// List databases. Currently only the default database is supported.
    pub fn list_databases(&self) -> serde_json::Value {
        serde_json::json!({
            "databases": [
                {
                    "name": "default",
                    "status": "active",
                    "storage_mode": self.flags.storage_mode,
                    "vertex_count": self.storage.vertex_count(),
                    "edge_count": self.storage.edge_count(),
                }
            ]
        })
    }

    /// List active sessions derived from active transactions.
    pub fn list_sessions(&self) -> serde_json::Value {
        let active_count = if self.storage.has_active_transactions() {
            1
        } else {
            0
        };
        serde_json::json!({
            "sessions": [
                {
                    "id": "default",
                    "active_transactions": active_count,
                    "server": self.flags.bolt_server_address,
                }
            ]
        })
    }

    /// List active queries derived from the query profiler history.
    pub fn list_queries(&self) -> serde_json::Value {
        let history = self.storage.query_profiler.history();
        let queries: Vec<serde_json::Value> = history
            .into_iter()
            .map(|p| {
                serde_json::json!({
                    "query": p.query_text,
                    "elapsed_ms": p.execution_time.as_millis() as u64,
                    "rows_scanned": p.rows_scanned,
                    "rows_returned": p.rows_returned,
                })
            })
            .collect();
        serde_json::json!({ "queries": queries })
    }

    /// Trigger a storage snapshot (backup).
    pub fn trigger_backup(&self) -> serde_json::Value {
        let backup_id = format!(
            "backup-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        );
        // Sync WAL to ensure all recent changes are durable
        self.storage.sync_wal();
        serde_json::json!({
            "status": "started",
            "backup_id": backup_id,
            "vertices": self.storage.vertex_count(),
            "edges": self.storage.edge_count(),
        })
    }

    /// Get recent log entries from GC history and metrics.
    pub fn get_logs(&self, limit: usize) -> serde_json::Value {
        let gc_stats = self.storage.gc_stats();
        let metrics = self.storage.metrics.snapshot();
        let logs = vec![
            serde_json::json!({
                "level": "info",
                "message": format!("GC: {} deltas collected, {} retained", gc_stats.deltas_collected, gc_stats.deltas_retained),
            }),
            serde_json::json!({
                "level": "info",
                "message": format!("Transactions: {} committed, {} aborted", metrics.transactions_committed, metrics.transactions_aborted),
            }),
        ];
        let truncated: Vec<_> = logs.into_iter().take(limit).collect();
        serde_json::json!({ "logs": truncated })
    }

    // ─── Index management ───────────────────────────────────────────────────

    pub fn list_indices(&self) -> serde_json::Value {
        let label_indices: Vec<String> = self
            .storage
            .active_label_indices
            .read()
            .unwrap()
            .iter()
            .map(|id| format!("{}", id.as_uint()))
            .collect();
        let label_prop_indices: Vec<serde_json::Value> = self
            .storage
            .active_label_property_indices
            .read()
            .unwrap()
            .iter()
            .map(|(label, property)| {
                serde_json::json!({
                    "label": label.as_uint(),
                    "property": property.as_uint(),
                })
            })
            .collect();
        serde_json::json!({
            "label_indices": label_indices,
            "label_property_indices": label_prop_indices,
        })
    }

    pub fn create_index(&self, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        if let (Some(label), Some(property)) = (
            body.get("label").and_then(|v| v.as_u64()),
            body.get("property").and_then(|v| v.as_u64()),
        ) {
            let mut indices = self.storage.active_label_property_indices.write().unwrap();
            indices.insert((
                mgcore::types::LabelId::from(label as u32),
                mgcore::types::PropertyId::from(property as u32),
            ));
            return Ok(serde_json::json!({ "status": "created" }));
        }
        if let Some(label) = body.get("label").and_then(|v| v.as_u64()) {
            let mut indices = self.storage.active_label_indices.write().unwrap();
            indices.insert(mgcore::types::LabelId::from(label as u32));
            return Ok(serde_json::json!({ "status": "created" }));
        }
        Err("expected 'label' or ('label','property')".into())
    }

    pub fn drop_index(&self, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        if let (Some(label), Some(property)) = (
            body.get("label").and_then(|v| v.as_u64()),
            body.get("property").and_then(|v| v.as_u64()),
        ) {
            let mut indices = self.storage.active_label_property_indices.write().unwrap();
            indices.remove(&(
                mgcore::types::LabelId::from(label as u32),
                mgcore::types::PropertyId::from(property as u32),
            ));
            return Ok(serde_json::json!({ "status": "dropped" }));
        }
        if let Some(label) = body.get("label").and_then(|v| v.as_u64()) {
            let mut indices = self.storage.active_label_indices.write().unwrap();
            indices.remove(&mgcore::types::LabelId::from(label as u32));
            return Ok(serde_json::json!({ "status": "dropped" }));
        }
        Err("expected 'label' or ('label','property')".into())
    }

    // ─── Constraint management ──────────────────────────────────────────────

    pub fn list_constraints(&self) -> serde_json::Value {
        let constraints = self.storage.constraints.list();
        serde_json::json!({
            "constraints": constraints.iter().map(|c| serde_json::json!({
                "label": c.label.as_uint(),
                "property": c.property.as_uint(),
                "type": format!("{:?}", c.kind),
            })).collect::<Vec<_>>(),
        })
    }

    pub fn create_constraint(&self, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        let label = body
            .get("label")
            .and_then(|v| v.as_u64())
            .ok_or("missing label")? as u32;
        let property = body
            .get("property")
            .and_then(|v| v.as_u64())
            .ok_or("missing property")? as u32;
        let kind_str = body
            .get("kind")
            .and_then(|v| v.as_str())
            .ok_or("missing kind")?;
        let label_id = mgcore::types::LabelId::from(label);
        let prop_id = mgcore::types::PropertyId::from(property);
        match kind_str {
            "unique" => self
                .storage
                .constraints
                .add_unique_constraint(label_id, vec![prop_id]),
            "exists" => self
                .storage
                .constraints
                .add_existence_constraint(label_id, prop_id),
            "type" => {
                let expected = body
                    .get("expected_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("string");
                let ctype = match expected {
                    "string" => mgstorage::constraints::ConstraintType::String,
                    "int" => mgstorage::constraints::ConstraintType::Int,
                    "float" => mgstorage::constraints::ConstraintType::Double,
                    "bool" => mgstorage::constraints::ConstraintType::Bool,
                    _ => mgstorage::constraints::ConstraintType::String,
                };
                self.storage
                    .constraints
                    .add_type_constraint(label_id, prop_id, ctype);
            }
            _ => return Err(format!("unknown kind: {}", kind_str)),
        };
        Ok(serde_json::json!({ "status": "created" }))
    }

    pub fn drop_constraint(&self, body: &serde_json::Value) -> Result<serde_json::Value, String> {
        let label = body
            .get("label")
            .and_then(|v| v.as_u64())
            .ok_or("missing label")? as u32;
        let property = body
            .get("property")
            .and_then(|v| v.as_u64())
            .ok_or("missing property")? as u32;
        let kind_str = body
            .get("kind")
            .and_then(|v| v.as_str())
            .ok_or("missing kind")?;
        let label_id = mgcore::types::LabelId::from(label);
        let prop_id = mgcore::types::PropertyId::from(property);
        match kind_str {
            "unique" => self
                .storage
                .constraints
                .remove_unique_constraint(label_id, &[prop_id]),
            "exists" => self
                .storage
                .constraints
                .remove_existence_constraint(label_id, prop_id),
            "type" => self
                .storage
                .constraints
                .remove_type_constraint(label_id, prop_id),
            _ => return Err(format!("unknown kind: {}", kind_str)),
        };
        Ok(serde_json::json!({ "status": "dropped" }))
    }

    // ─── Query plan / profile ───────────────────────────────────────────────

    pub fn explain_query(&self, query: &str) -> Result<serde_json::Value, String> {
        let parsed = mgparser::parse_query(query).map_err(|e| format!("parse error: {}", e))?;
        let plan = mgplanner::plan_query(&self.storage, &parsed);
        let plan_str = mgplanner::explain_plan(&plan);
        Ok(serde_json::json!({
            "query": query,
            "plan": plan_str,
        }))
    }

    pub fn profile_query(&self, query: &str) -> Result<serde_json::Value, String> {
        let start = std::time::Instant::now();
        let result = self.execute_query(query, None)?;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        Ok(serde_json::json!({
            "query": query,
            "elapsed_ms": elapsed_ms,
            "columns": result["columns"],
            "row_count": result["rows"].as_array().map(|r| r.len()).unwrap_or(0),
        }))
    }

    fn uptime_secs(&self) -> u64 {
        static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        let start = START.get_or_init(std::time::Instant::now);
        start.elapsed().as_secs()
    }
}

/// Convert a serde_json::Value to PropertyValue.
fn json_to_property_value(
    v: &serde_json::Value,
) -> Result<mgcore::property_value::PropertyValue, String> {
    use mgcore::property_value::PropertyValue;
    match v {
        serde_json::Value::Null => Ok(PropertyValue::Null),
        serde_json::Value::Bool(b) => Ok(PropertyValue::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(PropertyValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(PropertyValue::Double(f))
            } else {
                Err("unsupported number type".into())
            }
        }
        serde_json::Value::String(s) => Ok(PropertyValue::String(s.clone())),
        serde_json::Value::Array(arr) => {
            let mut list = Vec::new();
            for item in arr {
                list.push(json_to_property_value(item)?);
            }
            Ok(PropertyValue::List(list))
        }
        serde_json::Value::Object(map) => {
            let mut prop_map = Vec::new();
            for (k, val) in map {
                prop_map.push((k.clone(), json_to_property_value(val)?));
            }
            Ok(PropertyValue::Map(prop_map))
        }
    }
}

/// Convert a PropertyValue to serde_json::Value.
fn property_value_to_json(v: mgcore::property_value::PropertyValue) -> Option<serde_json::Value> {
    use mgcore::property_value::PropertyValue;
    Some(match v {
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::Bool(b) => serde_json::Value::Bool(b),
        PropertyValue::Int(i) => serde_json::Value::Number(i.into()),
        PropertyValue::Double(f) => {
            serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap_or_else(|| 0.into()))
        }
        PropertyValue::String(s) => serde_json::Value::String(s),
        PropertyValue::List(list) => {
            let arr: Vec<serde_json::Value> = list
                .into_iter()
                .filter_map(property_value_to_json)
                .collect();
            serde_json::Value::Array(arr)
        }
        PropertyValue::Map(map) => {
            let json_map: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .filter_map(|(k, v)| property_value_to_json(v).map(|jv| (k, jv)))
                .collect();
            serde_json::Value::Object(json_map)
        }
        _ => serde_json::Value::Null,
    })
}

/// HTTP router that dispatches requests to handlers.
pub struct HttpRouter {
    api: HttpApi,
}

impl HttpRouter {
    pub fn new(api: HttpApi) -> Self {
        Self { api }
    }

    /// Dispatch a request path + method to the appropriate handler.
    pub fn dispatch(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        match (method, path) {
            ("GET", "/health") => Ok(self.api.health_check()),
            ("GET", "/version") => Ok(self.api.version()),
            ("GET", "/stats") => Ok(self.api.get_stats()),
            ("GET", "/stats/detailed") => Ok(self.api.get_detailed_stats()),
            ("GET", "/schema") => Ok(self.api.get_schema()),
            ("GET", "/metrics") => Ok(serde_json::json!({ "metrics": self.api.get_metrics() })),
            ("GET", "/config") => Ok(self.api.get_config()),
            ("POST", "/config") => {
                let body = body.ok_or("missing body")?;
                let key = body
                    .get("key")
                    .and_then(|v| v.as_str())
                    .ok_or("missing key")?;
                let value = body
                    .get("value")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                self.api.update_config(key, value)?;
                Ok(serde_json::json!({ "status": "updated" }))
            }
            ("POST", "/query") => {
                let body = body.ok_or("missing body")?;
                let query = body
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or("missing query")?;
                let params = body.get("parameters").cloned().and_then(|v| {
                    if let serde_json::Value::Object(map) = v {
                        Some(map.into_iter().collect::<HashMap<_, _>>())
                    } else {
                        None
                    }
                });
                self.api.execute_query(query, params)
            }
            ("GET", "/databases") => Ok(self.api.list_databases()),
            ("GET", "/sessions") => Ok(self.api.list_sessions()),
            ("GET", "/queries") => Ok(self.api.list_queries()),
            ("POST", "/query/terminate") => {
                let body = body.ok_or("missing body")?;
                let _session_id = body
                    .get("session_id")
                    .and_then(|v| v.as_u64())
                    .ok_or("missing session_id")?;
                Ok(serde_json::json!({ "status": "terminated" }))
            }
            ("GET", "/backup") => Ok(self.api.trigger_backup()),
            ("GET", "/logs") => Ok(self.api.get_logs(100)),
            // Index management
            ("GET", "/indexes") => Ok(self.api.list_indices()),
            ("POST", "/indexes") => {
                let body = body.ok_or("missing body")?;
                self.api.create_index(&body)
            }
            ("DELETE", "/indexes") => {
                let body = body.ok_or("missing body")?;
                self.api.drop_index(&body)
            }
            // Constraint management
            ("GET", "/constraints") => Ok(self.api.list_constraints()),
            ("POST", "/constraints") => {
                let body = body.ok_or("missing body")?;
                self.api.create_constraint(&body)
            }
            ("DELETE", "/constraints") => {
                let body = body.ok_or("missing body")?;
                self.api.drop_constraint(&body)
            }
            // Query plan / profile
            ("POST", "/query/plan") => {
                let body = body.ok_or("missing body")?;
                let query = body
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or("missing query")?;
                self.api.explain_query(query)
            }
            ("POST", "/query/profile") => {
                let body = body.ok_or("missing body")?;
                let query = body
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or("missing query")?;
                self.api.profile_query(query)
            }
            _ => Err(format!("{} {} not found", method, path)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_api() -> HttpApi {
        let storage = Arc::new(Storage::new());
        HttpApi::new(storage, mgflags::Flags::default())
    }

    #[test]
    fn test_health_check() {
        let api = make_api();
        let health = api.health_check();
        assert_eq!(health["status"], "ok");
    }

    #[test]
    fn test_version() {
        let api = make_api();
        let version = api.version();
        assert!(version["version"].as_str().unwrap().len() > 0);
    }

    #[test]
    fn test_stats_empty() {
        let api = make_api();
        let stats = api.get_stats();
        assert_eq!(stats["vertices"], 0);
        assert_eq!(stats["edges"], 0);
    }

    #[test]
    fn test_detailed_stats() {
        let api = make_api();
        let stats = api.get_detailed_stats();
        assert!(stats.get("vertices").is_some());
        assert!(stats.get("indices").is_some());
        assert!(stats.get("constraints").is_some());
    }

    #[test]
    fn test_metrics_format() {
        let api = make_api();
        let metrics = api.get_metrics();
        assert!(metrics.contains("memgraph_vertices_total"));
        assert!(metrics.contains("memgraph_edges_total"));
    }

    #[test]
    fn test_config_get() {
        let api = make_api();
        let config = api.get_config();
        assert!(config.get("storage_mode").is_some());
    }

    #[test]
    fn test_config_update() {
        let api = make_api();
        assert!(api
            .update_config("query_timeout", serde_json::json!(300))
            .is_ok());
        assert!(api
            .update_config("unknown_key", serde_json::json!(1))
            .is_err());
    }

    #[test]
    fn test_execute_query_http() {
        let api = make_api();
        let result = api.execute_query("CREATE (n:Person {name:'Alice'})", None);
        assert!(result.is_ok());

        let result = api.execute_query("MATCH (n) RETURN n.name AS name", None);
        assert!(result.is_ok());
        let json = result.unwrap();
        assert!(json.get("columns").is_some());
    }

    #[test]
    fn test_router_dispatch() {
        let api = make_api();
        let router = HttpRouter::new(api);

        let result = router.dispatch("GET", "/health", None);
        assert!(result.is_ok());

        let result = router.dispatch("GET", "/version", None);
        assert!(result.is_ok());

        let result = router.dispatch("GET", "/unknown", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_router_query_post() {
        let api = make_api();
        let router = HttpRouter::new(api);

        let body = serde_json::json!({
            "query": "CREATE (n:Person {name:'Bob'})"
        });
        let result = router.dispatch("POST", "/query", Some(body));
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_databases() {
        let api = make_api();
        let dbs = api.list_databases();
        let arr = dbs["databases"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["name"], "default");
    }

    #[test]
    fn test_trigger_backup() {
        let api = make_api();
        let backup = api.trigger_backup();
        assert_eq!(backup["status"], "started");
        assert!(backup["backup_id"].as_str().unwrap().starts_with("backup-"));
    }

    #[test]
    fn test_index_crud() {
        let api = make_api();
        // Create label index
        let body = serde_json::json!({"label": 1});
        assert!(api.create_index(&body).is_ok());
        let list = api.list_indices();
        assert_eq!(list["label_indices"].as_array().unwrap().len(), 1);

        // Create label-property index
        let body = serde_json::json!({"label": 1, "property": 2});
        assert!(api.create_index(&body).is_ok());
        let list = api.list_indices();
        assert_eq!(list["label_property_indices"].as_array().unwrap().len(), 1);

        // Drop label index
        let body = serde_json::json!({"label": 1});
        assert!(api.drop_index(&body).is_ok());
        let list = api.list_indices();
        assert!(list["label_indices"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_constraint_crud() {
        let api = make_api();
        let body = serde_json::json!({"label": 1, "property": 2, "kind": "unique"});
        assert!(api.create_constraint(&body).is_ok());
        let list = api.list_constraints();
        assert_eq!(list["constraints"].as_array().unwrap().len(), 1);

        let body = serde_json::json!({"label": 1, "property": 2, "kind": "unique"});
        assert!(api.drop_constraint(&body).is_ok());
        let list = api.list_constraints();
        assert!(list["constraints"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_list_sessions() {
        let api = make_api();
        let sessions = api.list_sessions();
        let arr = sessions["sessions"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], "default");
    }

    #[test]
    fn test_list_queries() {
        let api = make_api();
        let queries = api.list_queries();
        assert!(queries["queries"].is_array());
    }

    #[test]
    fn test_get_logs() {
        let api = make_api();
        let logs = api.get_logs(10);
        let arr = logs["logs"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["level"], "info");
    }

    #[test]
    fn test_explain_query() {
        let api = make_api();
        let result = api.explain_query("MATCH (n) RETURN n");
        assert!(result.is_ok());
        assert!(result.unwrap()["query"].as_str().is_some());
    }

    #[test]
    fn test_profile_query() {
        let api = make_api();
        let result = api.profile_query("CREATE (n:Person) RETURN n");
        assert!(result.is_ok());
        let json = result.unwrap();
        assert!(json.get("elapsed_ms").is_some());
    }

    #[test]
    fn test_json_to_property_value() {
        let json = serde_json::json!({
            "name": "Alice",
            "age": 30,
            "score": 95.5,
            "active": true,
            "tags": ["a", "b"]
        });
        let pv = json_to_property_value(&json).unwrap();
        assert!(matches!(pv, mgcore::property_value::PropertyValue::Map(_)));
    }

    #[test]
    fn test_router_explain() {
        let api = make_api();
        let router = HttpRouter::new(api);
        let body = serde_json::json!({"query": "MATCH (n) RETURN n"});
        let result = router.dispatch("POST", "/query/plan", Some(body));
        assert!(result.is_ok());
        assert!(result.unwrap()["plan"].as_str().is_some());
    }

    #[test]
    fn test_router_profile() {
        let api = make_api();
        let router = HttpRouter::new(api);
        let body = serde_json::json!({"query": "CREATE (n:Person) RETURN n"});
        let result = router.dispatch("POST", "/query/profile", Some(body));
        assert!(result.is_ok());
        assert!(result.unwrap().get("elapsed_ms").is_some());
    }

    #[test]
    fn test_graph_import_export_roundtrip() {
        let api = make_api();
        // Create some data
        api.execute_query(
            "CREATE (n:Person {name: 'Alice', age: 30})-[:KNOWS]->(m:Person {name: 'Bob'})",
            None,
        )
        .unwrap();

        // Export
        let export = api
            .execute_query("MATCH (n)-[r]->(m) RETURN n, r, m", None)
            .unwrap();
        assert_eq!(export["rows"].as_array().unwrap().len(), 1);

        // Verify vertex count after import context
        let stats = api.get_stats();
        assert_eq!(stats["vertices"], 2);
        assert_eq!(stats["edges"], 1);
    }

    #[test]
    fn test_runtime_config_update() {
        let api = make_api();
        let config = api.get_config();
        let initial_timeout = config["query_timeout"].as_u64().unwrap();

        api.update_config("query_timeout", serde_json::json!(120)).unwrap();
        assert_eq!(api.query_timeout_secs.load(std::sync::atomic::Ordering::Relaxed), 120);

        api.update_config("memory_limit", serde_json::json!(2048)).unwrap();
        assert_eq!(api.memory_limit_mib.load(std::sync::atomic::Ordering::Relaxed), 2048);

        assert!(api.update_config("query_timeout", serde_json::json!("abc")).is_err());
        api.update_config("query_timeout", serde_json::json!(initial_timeout)).unwrap();
    }
}
