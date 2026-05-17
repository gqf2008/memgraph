//! Bolt protocol TCP server (async with tokio).
//!
//! Listens on port 7687, accepts connections, performs Bolt handshake,
//! and dispatches messages via a proper Bolt v4.x/5.x state machine.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;

/// Either a plain TCP stream or a TLS-wrapped stream.
enum BoltStream {
    Plain(TcpStream),
    Tls(tokio_rustls::server::TlsStream<TcpStream>),
}

impl tokio::io::AsyncRead for BoltStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            BoltStream::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            BoltStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for BoltStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            BoltStream::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            BoltStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            BoltStream::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            BoltStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            BoltStream::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            BoltStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

use mgbolt::framing::MAX_CHUNK_SIZE;
use mgbolt::handshake::{Handshake, VarInt};
use mgbolt::message::Message;
use mgbolt::value::{Value, SIG_NODE, SIG_RELATIONSHIP};
use mgcatalog::Catalog;
use mgcore::property_value::PropertyValue;
use mgdbms::DbmsHandler;
use mginterp::{execute_with_catalog_auth_dbms_and_params_timeout, set_active_transaction};
use mgstorage::storage::Storage;

use crate::admin::{AdminState, ConnectionId};
use crate::auth::AuthConfig;

pub const DEFAULT_PORT: u16 = 7687;
pub const DEFAULT_MAX_CONNECTIONS: usize = 100;

/// Convert a Bolt `Value` into `PropertyValue` for parameter binding.
fn bolt_value_to_property(v: &Value) -> PropertyValue {
    match v {
        Value::Null => PropertyValue::Null,
        Value::Bool(b) => PropertyValue::Bool(*b),
        Value::Int(n) => PropertyValue::Int(*n),
        Value::Float(f) => PropertyValue::Double(*f),
        Value::String(s) => PropertyValue::String(s.clone()),
        Value::List(items) => {
            PropertyValue::List(items.iter().map(bolt_value_to_property).collect())
        }
        Value::Map(entries) => PropertyValue::Map(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), bolt_value_to_property(v)))
                .collect(),
        ),
        Value::Bytes(b) => {
            PropertyValue::List(b.iter().map(|&byte| PropertyValue::Int(byte as i64)).collect())
        }
        Value::Struct(sig, fields) => {
            match *sig {
                SIG_NODE if fields.len() == 3 => {
                    let gid = match &fields[0] { Value::Int(id) => mgcore::types::Gid::from(*id as u64), _ => mgcore::types::Gid::from(0u64) };
                    let labels = match &fields[1] {
                        Value::List(ls) => ls.iter().filter_map(|v| match v { Value::String(_) => Some(mgcore::types::LabelId::from(0u32)), _ => None }).collect(),
                        _ => vec![],
                    };
                    let props = bolt_map_to_property_store(&fields[2]);
                    PropertyValue::Vertex(mgcore::property_value::VertexRef::new(gid, labels, props))
                }
                SIG_RELATIONSHIP if fields.len() == 5 => {
                    let gid = match &fields[0] { Value::Int(id) => mgcore::types::Gid::from(*id as u64), _ => mgcore::types::Gid::from(0u64) };
                    let from = match &fields[1] { Value::Int(id) => mgcore::types::Gid::from(*id as u64), _ => mgcore::types::Gid::from(0u64) };
                    let to = match &fields[2] { Value::Int(id) => mgcore::types::Gid::from(*id as u64), _ => mgcore::types::Gid::from(0u64) };
                    let props = bolt_map_to_property_store(&fields[4]);
                    PropertyValue::Edge(mgcore::property_value::EdgeRefValue::new(gid, mgcore::types::EdgeTypeId::from(0u32), from, to, props))
                }
                _ => PropertyValue::Null,
            }
        }
    }
}

fn bolt_map_to_property_store(v: &Value) -> mgcore::property_store::PropertyStore {
    let mut ps = mgcore::property_store::PropertyStore::new();
    if let Value::Map(entries) = v {
        for (i, (_, val)) in entries.iter().enumerate() {
            ps.set(mgcore::types::PropertyId::from(i as u32), bolt_value_to_property(val));
        }
    }
    ps
}

/// Convert a mgcore `PropertyValue` into a Bolt `Value`, resolving label/type
/// names and property keys through the catalog.
fn property_value_to_bolt(pv: &PropertyValue, catalog: &Catalog) -> Value {
    match pv {
        PropertyValue::Vertex(v) => {
            let labels: Vec<String> = v.labels.iter().map(|l| catalog.label_name(*l)).collect();
            let properties: HashMap<String, Value> = v
                .properties
                .iter()
                .map(|(pid, pval)| {
                    (
                        catalog.property_name(pid),
                        property_value_to_bolt(pval, catalog),
                    )
                })
                .collect();
            Value::node(v.gid.as_int(), labels, properties)
        }
        PropertyValue::Edge(e) => {
            let type_name = catalog.edge_type_name(e.edge_type);
            let properties: HashMap<String, Value> = e
                .properties
                .iter()
                .map(|(pid, pval)| {
                    (
                        catalog.property_name(pid),
                        property_value_to_bolt(pval, catalog),
                    )
                })
                .collect();
            Value::relationship(
                e.gid.as_int(),
                e.from_vertex.as_int(),
                e.to_vertex.as_int(),
                &type_name,
                properties,
            )
        }
        PropertyValue::Path(p) => {
            let mut nodes = Vec::new();
            let mut rels = Vec::new();
            for v in &p.vertices {
                let labels: Vec<String> = v.labels.iter().map(|l| catalog.label_name(*l)).collect();
                let properties: HashMap<String, Value> = v
                    .properties
                    .iter()
                    .map(|(pid, pval)| {
                        (
                            catalog.property_name(pid),
                            property_value_to_bolt(pval, catalog),
                        )
                    })
                    .collect();
                nodes.push(Value::node(v.gid.as_int(), labels, properties));
            }
            for e in &p.edges {
                let type_name = catalog.edge_type_name(e.edge_type);
                let properties: HashMap<String, Value> = e
                    .properties
                    .iter()
                    .map(|(pid, pval)| {
                        (
                            catalog.property_name(pid),
                            property_value_to_bolt(pval, catalog),
                        )
                    })
                    .collect();
                rels.push(Value::relationship(
                    e.gid.as_int(),
                    e.from_vertex.as_int(),
                    e.to_vertex.as_int(),
                    &type_name,
                    properties,
                ));
            }
            Value::path(nodes, rels)
        }
        PropertyValue::List(items) => Value::List(
            items
                .iter()
                .map(|v| property_value_to_bolt(v, catalog))
                .collect(),
        ),
        PropertyValue::Map(entries) => {
            let mut map = HashMap::new();
            for (k, v) in entries {
                map.insert(k.clone(), property_value_to_bolt(v, catalog));
            }
            Value::Map(map)
        }
        PropertyValue::Null => Value::Null,
        PropertyValue::Bool(b) => Value::Bool(*b),
        PropertyValue::Int(n) => Value::Int(*n),
        PropertyValue::Double(f) => Value::Float(*f),
        PropertyValue::String(s) => Value::String(s.clone()),
        PropertyValue::Date(d) => Value::date(*d),
        PropertyValue::LocalTime(t) => Value::local_time(*t),
        PropertyValue::LocalDateTime(dt) => Value::local_date_time(*dt),
        PropertyValue::ZonedDateTime(dt) => Value::zoned_date_time(dt, 5),
        PropertyValue::Duration(d) => Value::duration(*d),
        PropertyValue::Point2D(p) => Value::point_2d(*p),
        PropertyValue::Point3D(p) => Value::point_3d(*p),
        _ => Value::Null,
    }
}

/// Bolt connection state machine states.
#[derive(Clone, Copy, Debug, PartialEq)]
enum BoltState {
    Connected,
    Authentication, // Bolt 5.1+: waiting for LOGON after HELLO
    Authenticated,
    Ready,
    Streaming,
    TxReady,
    TxStreaming,
    Failed,
}

/// Per-connection server state.
struct ConnectionState {
    state: BoltState,
    pending_result: Option<mginterp::QueryResult>,
    result_iter: Option<usize>,
    in_transaction: bool,
    active_query_id: Option<u64>,
    explicit_tx: Option<Arc<mgstorage::transaction::Transaction>>,
    username: Option<String>,
    /// Last committed bookmark (Bolt 5.x causal consistency).
    last_bookmark: Option<String>,
    /// Current database context (for multi-database support).
    current_db: String,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            state: BoltState::Connected,
            pending_result: None,
            result_iter: None,
            in_transaction: false,
            active_query_id: None,
            explicit_tx: None,
            username: None,
            last_bookmark: None,
            current_db: "default".to_string(),
        }
    }

    fn reset(&mut self) {
        self.state = BoltState::Ready;
        self.pending_result = None;
        self.result_iter = None;
        self.in_transaction = false;
        self.active_query_id = None;
        self.explicit_tx = None;
        self.last_bookmark = None;
    }

    fn clear_streaming(&mut self) {
        self.pending_result = None;
        self.result_iter = None;
        self.active_query_id = None;
        self.state = if self.in_transaction {
            BoltState::TxReady
        } else {
            BoltState::Ready
        };
    }
}

/// Run the Bolt server, accepting connections on the given port.
pub async fn run(
    storage: Arc<Storage>,
    catalog: Arc<Catalog>,
    auth: Arc<AuthConfig>,
    admin: Arc<AdminState>,
    query_cache: Arc<crate::query_cache::QueryCache>,
    dbms: Arc<mgdbms::DbmsHandler>,
    tls_acceptor: Option<Arc<tokio_rustls::TlsAcceptor>>,
    cluster_state: Arc<mgcoord::ClusterState>,
    port: u16,
    max_connections: usize,
) {
    let addr = format!("0.0.0.0:{}", port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => {
            tracing::info!(
                "Bolt server listening on {} (max {} connections)",
                addr,
                max_connections
            );
            l
        }
        Err(e) => {
            tracing::error!("Failed to bind to {}: {}", addr, e);
            return;
        }
    };

    let sem = Arc::new(Semaphore::new(max_connections));

    loop {
        let permit = match sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break,
        };

        match listener.accept().await {
            Ok((stream, peer)) => {
                let storage = storage.clone();
                let catalog = catalog.clone();
                let auth = auth.clone();
                let admin = admin.clone();
                let query_cache = query_cache.clone();
                let dbms = dbms.clone();
                let tls = tls_acceptor.clone();
                let cluster = cluster_state.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Some(ref acceptor) = tls {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                tracing::info!("[bolt] TLS handshake completed for {}", peer);
                                handle_connection(storage, catalog, auth, admin, query_cache, dbms, cluster, BoltStream::Tls(tls_stream), peer).await;
                            }
                            Err(e) => {
                                tracing::warn!("[bolt] TLS handshake failed for {}: {}", peer, e);
                            }
                        }
                    } else {
                        handle_connection(storage, catalog, auth, admin, query_cache, dbms, cluster, BoltStream::Plain(stream), peer).await;
                    }
                });
            }
            Err(e) => {
                tracing::error!("Connection error: {}", e);
            }
        }
    }
}

async fn handle_connection(
    storage: Arc<Storage>,
    catalog: Arc<Catalog>,
    auth: Arc<AuthConfig>,
    admin: Arc<AdminState>,
    query_cache: Arc<crate::query_cache::QueryCache>,
    dbms: Arc<DbmsHandler>,
    cluster_state: Arc<mgcoord::ClusterState>,
    mut stream: BoltStream,
    peer: std::net::SocketAddr,
) {
    let peer_str = peer.to_string();
    tracing::info!("[bolt] connection from {}", peer_str);
    let mut conn_id: Option<ConnectionId> = None;

    // ── Handshake ──────────────────────────────────────────────────────
    let mut preamble = [0u8; 4];
    if stream.read_exact(&mut preamble).await.is_err() {
        return;
    }
    if preamble != Handshake::PREAMBLE {
        tracing::warn!("[bolt] bad preamble from {}", peer_str);
        return;
    }

    let mut version_bytes = [0u8; 16];
    if stream.read_exact(&mut version_bytes).await.is_err() {
        return;
    }
    let client_versions = Handshake::parse_versions(&version_bytes);
    tracing::info!("[bolt] client versions: {:?}", client_versions);

    // ── Manifest v1 negotiation (Bolt 5.7+) ────────────────────────────
    let version = if Handshake::is_manifest_v1_request(&client_versions) {
        // Respond with manifest v1 marker
        if stream
            .write_all(&Handshake::MANIFEST_V1.to_be_bytes())
            .await
            .is_err()
        {
            return;
        }
        // Send supported version ranges + capabilities
        let mut manifest = Vec::new();
        let ranges = Handshake::SUPPORTED_RANGES;
        manifest.extend_from_slice(&VarInt::encode(ranges.len() as u64));
        for &r in &ranges {
            manifest.extend_from_slice(&r.to_be_bytes());
        }
        manifest.extend_from_slice(&VarInt::encode(0u64)); // capabilities = 0
        if stream.write_all(&manifest).await.is_err() {
            return;
        }
        // Read client's chosen version (4 bytes) + capabilities (VarInt)
        let mut chosen = [0u8; 4];
        if stream.read_exact(&mut chosen).await.is_err() {
            return;
        }
        let chosen_version = u32::from_be_bytes(chosen);
        // Read capabilities VarInt (at least 1 byte)
        let mut cap_buf = [0u8; 16];
        if stream.read_exact(&mut cap_buf[..1]).await.is_err() {
            return;
        }
        let mut cap_len = 1usize;
        while cap_buf[cap_len - 1] & 0x80 != 0 {
            if cap_len >= cap_buf.len() {
                return;
            }
            if stream
                .read_exact(&mut cap_buf[cap_len..cap_len + 1])
                .await
                .is_err()
            {
                return;
            }
            cap_len += 1;
        }
        let normalized = Handshake::normalize_manifest_version(chosen_version);
        tracing::info!(
            "[bolt] manifest chose version 0x{:08X} -> normalized 0x{:04X}",
            chosen_version,
            normalized
        );
        if Handshake::SUPPORTED.contains(&normalized) {
            normalized
        } else {
            tracing::warn!(
                "[bolt] manifest chose unsupported version 0x{:08X} (normalized 0x{:04X})",
                chosen_version,
                normalized
            );
            let _ = stream.write_all(&[0x00, 0x00, 0x00, 0x00]).await;
            return;
        }
    } else {
        // ── Legacy negotiation ─────────────────────────────────────────
        let expanded = Handshake::expand_client_versions(&client_versions);
        tracing::info!("[bolt] expanded versions: {:?}", expanded);
        match Handshake::negotiate(&expanded) {
            Some(v) => v,
            None => {
                let _ = stream.write_all(&[0x00, 0x00, 0x00, 0x00]).await;
                tracing::warn!("[bolt] no compatible version for {}", peer_str);
                return;
            }
        }
    };

    // In legacy mode, the server confirms by echoing the chosen version.
    // In manifest v1 mode, no server response is expected after the
    // client's final choice — the client pipelines its first message.
    if !Handshake::is_manifest_v1_request(&client_versions)
        && stream.write_all(&version.to_be_bytes()).await.is_err()
    {
        return;
    }
    tracing::info!(
        "[bolt] negotiated v{}.{} with {}",
        version >> 8,
        version & 0xFF,
        peer_str
    );

    // ── Message loop ───────────────────────────────────────────────────
    let mut conn = ConnectionState::new();

    loop {
        let payload = match read_message(&mut stream).await {
            Ok(p) => p,
            Err(_) => break,
        };

        let msg = match parse_message(&payload) {
            Some(m) => m,
            None => {
                send_failure(&mut stream, "Protocol.Error", "failed to decode message").await;
                conn.state = BoltState::Failed;
                continue;
            }
        };

        if !is_valid_in_state(&msg, conn.state) {
            send_failure(
                &mut stream,
                "Protocol.State",
                &format!("{} not valid in state {:?}", msg_name(&msg), conn.state),
            )
            .await;
            conn.state = BoltState::Failed;
            continue;
        }

        match msg {
            Message::Hello {
                ref user_agent,
                ref extra,
            } => {
                tracing::info!("[bolt] HELLO from {} (agent: {})", peer_str, user_agent);

                // Determine if auth credentials are embedded in HELLO (Bolt <=5.0)
                // or if we should expect a separate LOGON (Bolt 5.1+).
                let has_embedded_auth = extra
                    .get("scheme")
                    .and_then(|v| {
                        if let Value::String(s) = v {
                            Some(s.as_str())
                        } else {
                            None
                        }
                    })
                    .map(|s| s != "none")
                    .unwrap_or(false);

                if has_embedded_auth {
                    // Legacy Bolt: authenticate immediately within HELLO
                    let principal = extra
                        .get("principal")
                        .and_then(|v| {
                            if let Value::String(s) = v {
                                Some(s.as_str())
                            } else {
                                None
                            }
                        })
                        .unwrap_or("");
                    let credentials = extra
                        .get("credentials")
                        .and_then(|v| {
                            if let Value::String(s) = v {
                                Some(s.as_str())
                            } else {
                                None
                            }
                        })
                        .unwrap_or("");
                    if auth.is_required() && auth.authenticate(principal, credentials).is_none() {
                        send_failure(
                            &mut stream,
                            "Neo.ClientError.Security.Unauthorized",
                            "Authentication failed",
                        )
                        .await;
                        conn.state = BoltState::Failed;
                        continue;
                    }
                    let user_str = extra.get("principal").and_then(|v| {
                        if let Value::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    });
                    conn.username = user_str.clone();
                    let cid = admin.register_connection(
                        peer_str.clone(),
                        user_str,
                        user_agent.clone(),
                        ((version >> 8) as u8, (version & 0xFF) as u8),
                    );
                    conn_id = Some(cid);
                    conn.state = BoltState::Authenticated;
                } else if auth.is_required() {
                    // Bolt 5.1+: no auth in HELLO, enter Authentication state and wait for LOGON
                    conn.state = BoltState::Authentication;
                } else {
                    // No auth required at all
                    let cid = admin.register_connection(
                        peer_str.clone(),
                        None,
                        user_agent.clone(),
                        ((version >> 8) as u8, (version & 0xFF) as u8),
                    );
                    conn_id = Some(cid);
                    conn.state = BoltState::Authenticated;
                }

                let mut meta = HashMap::new();
                // Neo4j drivers validate that the server name starts with "Neo4j/".
                meta.insert("server".into(), Value::String("Neo4j/5.28.0".into()));
                meta.insert(
                    "connection_id".into(),
                    Value::String(format!("bolt-{}", peer_str)),
                );
                if send_message(&mut stream, &Message::Success { metadata: meta })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Message::Logon { ref extra } => {
                tracing::info!("[bolt] LOGON from {}", peer_str);
                let scheme = extra
                    .get("scheme")
                    .and_then(|v| {
                        if let Value::String(s) = v {
                            Some(s.as_str())
                        } else {
                            None
                        }
                    })
                    .unwrap_or("none");
                let auth_ok = if scheme == "none" || !auth.is_required() {
                    !auth.is_required()
                } else {
                    let principal = extra
                        .get("principal")
                        .and_then(|v| {
                            if let Value::String(s) = v {
                                Some(s.as_str())
                            } else {
                                None
                            }
                        })
                        .unwrap_or("");
                    let credentials = extra
                        .get("credentials")
                        .and_then(|v| {
                            if let Value::String(s) = v {
                                Some(s.as_str())
                            } else {
                                None
                            }
                        })
                        .unwrap_or("");
                    auth.authenticate(principal, credentials).is_some()
                };
                if !auth_ok {
                    send_failure(
                        &mut stream,
                        "Neo.ClientError.Security.Unauthorized",
                        "Authentication failed",
                    )
                    .await;
                    conn.state = BoltState::Failed;
                    continue;
                }
                let principal = extra.get("principal").and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                });
                let cid = admin.register_connection(
                    peer_str.clone(),
                    principal,
                    "unknown".into(),
                    ((version >> 8) as u8, (version & 0xFF) as u8),
                );
                conn_id = Some(cid);
                conn.username = extra.get("principal").and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                });
                conn.state = BoltState::Authenticated;
                if send_message(
                    &mut stream,
                    &Message::Success {
                        metadata: HashMap::new(),
                    },
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            Message::Logoff => {
                tracing::info!("[bolt] LOGOFF from {}", peer_str);
                conn.username = None;
                conn.state = BoltState::Authentication;
                if send_message(
                    &mut stream,
                    &Message::Success {
                        metadata: HashMap::new(),
                    },
                )
                .await
                .is_err()
                {
                    break;
                }
            }

            Message::Run {
                ref query,
                ref parameters,
                ..
            } => {
                tracing::debug!("[bolt] RUN: {}", query);

                // Handle :USE database switching command (cheap byte check first — hot path)
                if query.as_bytes().first() == Some(&b':') {
                    if let Some(db_name) = query.strip_prefix(":USE ").or_else(|| query.strip_prefix(":use ")) {
                        let db_name = db_name.trim().trim_matches('`').trim_matches('"');
                        if dbms.exists(&db_name.to_string()) {
                            conn.current_db = db_name.to_string();
                            let mut meta = HashMap::new();
                            meta.insert("db".into(), Value::String(conn.current_db.clone()));
                            if send_message(&mut stream, &Message::Success { metadata: meta }).await.is_err() {
                                break;
                            }
                        } else {
                            send_failure(&mut stream, "Database.Error", &format!("database '{}' not found", db_name)).await;
                            conn.state = BoltState::Failed;
                        }
                        continue;
                    }
                }

                // Reject new queries when memory pressure is critical
                if admin.is_memory_critical() {
                    send_failure(
                        &mut stream,
                        "Memgraph.ClientError.MemoryLimitExceeded",
                        "Memory limit exceeded — server is under critical memory pressure.",
                    )
                    .await;
                    if let Some(cid) = conn_id {
                        admin.fail_query(cid);
                    }
                    conn.state = BoltState::Failed;
                    continue;
                }

                let qid = conn_id.map(|cid| admin.start_query(cid, query.clone()));
                let params: HashMap<String, PropertyValue> = parameters
                    .iter()
                    .map(|(k, v)| (k.clone(), bolt_value_to_property(v)))
                    .collect();
                let timeout = if admin.config().query_timeout_ms > 0 {
                    Some(std::time::Duration::from_millis(
                        admin.config().query_timeout_ms,
                    ))
                } else {
                    None
                };
                let _tx_guard = if let Some(ref tx) = conn.explicit_tx {
                    set_active_transaction(Some(tx.clone()))
                } else {
                    set_active_transaction(None)
                };

                // Try query cache first — reuse parsed query if available
                let cached = query_cache.get(query);
                let result = if let Some(cached_query) = cached {
                    mginterp::execute_query_with_binding(&storage, &cached_query.parsed, &params)
                } else {
                    execute_with_catalog_auth_dbms_and_params_timeout(
                        &storage,
                        query,
                        Some(&catalog),
                        &params,
                        Some(auth.auth_store()),
                        Some(&dbms),
                        timeout,
                    )
                };
                match result {
                    Ok(result) => {
                        let mut meta = HashMap::new();
                        meta.insert(
                            "fields".into(),
                            Value::List(
                                result
                                    .columns
                                    .iter()
                                    .map(|c| Value::String(c.clone()))
                                    .collect(),
                            ),
                        );
                        meta.insert("t_first".into(), Value::Int(0));
                        // Generate bookmark for causal consistency
                        let bookmark = next_bookmark();
                        conn.last_bookmark = Some(bookmark.clone());
                        meta.insert("bookmark".into(), Value::String(bookmark));
                        conn.pending_result = Some(result);
                        conn.result_iter = Some(0);
                        conn.active_query_id = qid;
                        conn.state = if conn.in_transaction {
                            BoltState::TxStreaming
                        } else {
                            BoltState::Streaming
                        };
                        if send_message(&mut stream, &Message::Success { metadata: meta })
                            .await
                            .is_err()
                        {
                            if let Some(qid) = qid {
                                admin.fail_query(qid);
                            }
                            break;
                        }
                    }
                    Err(e) => {
                        if let Some(qid) = qid {
                            admin.fail_query(qid);
                        }
                        send_failure(&mut stream, "Database.Error", &format!("{}", e)).await;
                        conn.state = BoltState::Failed;
                    }
                }
            }

            Message::Pull { n, .. } => {
                let n = if n <= 0 { i64::MAX } else { n };
                if let Some(ref result) = conn.pending_result {
                    if let Some(ref mut idx) = conn.result_iter {
                        let mut sent = 0i64;
                        while *idx < result.rows.len() && sent < n {
                            let row = &result.rows[*idx];
                            let fields: Vec<Value> = result
                                .columns
                                .iter()
                                .map(|col| {
                                    row.get(col)
                                        .map(|v| property_value_to_bolt(v, &catalog))
                                        .unwrap_or(Value::Null)
                                })
                                .collect();
                            if send_message(&mut stream, &Message::Record { fields })
                                .await
                                .is_err()
                            {
                                break;
                            }
                            *idx += 1;
                            sent += 1;
                        }
                        let has_more = *idx < result.rows.len();
                        let mut meta = HashMap::new();
                        meta.insert("has_more".into(), Value::Bool(has_more));
                        if !has_more {
                            meta.insert("type".into(), Value::String("r".into()));
                            meta.insert("t_last".into(), Value::Int(0));
                            conn.clear_streaming();
                        }
                        if send_message(&mut stream, &Message::Success { metadata: meta })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    } else {
                        let mut meta = HashMap::new();
                        meta.insert("has_more".into(), Value::Bool(false));
                        meta.insert("type".into(), Value::String("r".into()));
                        if send_message(&mut stream, &Message::Success { metadata: meta })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                } else {
                    let mut meta = HashMap::new();
                    meta.insert("has_more".into(), Value::Bool(false));
                    meta.insert("type".into(), Value::String("r".into()));
                    if send_message(&mut stream, &Message::Success { metadata: meta })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }

            Message::Discard { n, .. } => {
                let n = if n <= 0 { i64::MAX } else { n };
                let mut discarded = 0i64;
                if let Some(ref mut idx) = conn.result_iter {
                    let result_len = conn
                        .pending_result
                        .as_ref()
                        .map(|r| r.rows.len())
                        .unwrap_or(0);
                    while *idx < result_len && discarded < n {
                        *idx += 1;
                        discarded += 1;
                    }
                    let has_more = *idx < result_len;
                    let mut meta = HashMap::new();
                    meta.insert("has_more".into(), Value::Bool(has_more));
                    if !has_more {
                        conn.clear_streaming();
                    }
                    if send_message(&mut stream, &Message::Success { metadata: meta })
                        .await
                        .is_err()
                    {
                        break;
                    }
                } else {
                    let mut meta = HashMap::new();
                    meta.insert("has_more".into(), Value::Bool(false));
                    if send_message(&mut stream, &Message::Success { metadata: meta })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }

            Message::Begin { .. } => {
                if conn.in_transaction {
                    send_failure(
                        &mut stream,
                        "Database.Error",
                        "nested transactions not supported",
                    )
                    .await;
                    conn.state = BoltState::Failed;
                } else {
                    conn.explicit_tx = Some(
                        storage.begin_transaction(mgcore::delta::IsolationLevel::SnapshotIsolation),
                    );
                    conn.in_transaction = true;
                    conn.state = BoltState::TxReady;
                    let meta = HashMap::new();
                    if send_message(&mut stream, &Message::Success { metadata: meta })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }

            Message::Commit => {
                if !conn.in_transaction {
                    send_failure(&mut stream, "Database.Error", "no active transaction").await;
                    conn.state = BoltState::Failed;
                } else {
                    if let Some(ref tx) = conn.explicit_tx.take() {
                        storage.commit_transaction(tx);
                    }
                    conn.in_transaction = false;
                    conn.state = BoltState::Ready;
                    let mut meta = HashMap::new();
                    meta.insert("type".into(), Value::String("w".into()));
                    if let Some(ref bm) = conn.last_bookmark {
                        meta.insert("bookmark".into(), Value::String(bm.clone()));
                    }
                    if send_message(&mut stream, &Message::Success { metadata: meta })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }

            Message::Rollback => {
                if let Some(ref tx) = conn.explicit_tx.take() {
                    storage.abort_transaction(tx);
                }
                conn.in_transaction = false;
                conn.state = BoltState::Ready;
                let meta = HashMap::new();
                if send_message(&mut stream, &Message::Success { metadata: meta })
                    .await
                    .is_err()
                {
                    break;
                }
            }

            Message::Reset => {
                if let Some(ref tx) = conn.explicit_tx.take() {
                    storage.abort_transaction(tx);
                }
                conn.reset();
                let meta = HashMap::new();
                if send_message(&mut stream, &Message::Success { metadata: meta })
                    .await
                    .is_err()
                {
                    break;
                }
            }

            Message::Goodbye => {
                tracing::info!("[bolt] GOODBYE from {}", peer_str);
                break;
            }

            Message::Route { .. } => {
                // Build routing table from cluster state (Bolt v5.2)
                let mut rt = HashMap::new();
                for (db, addr) in cluster_state.routes() {
                    let mut servers = HashMap::new();
                    // Single-main topology: the main instance handles all roles
                    servers.insert("address".into(), Value::String(format!("{}:{}", addr.ip(), addr.port())));
                    let mut db_entry = HashMap::new();
                    db_entry.insert("servers".into(), Value::List(vec![Value::Map(servers)]));
                    rt.insert(db, Value::Map(db_entry));
                }
                let mut meta = HashMap::new();
                meta.insert("rt".into(), Value::Map(rt));
                if send_message(&mut stream, &Message::Success { metadata: meta })
                    .await
                    .is_err()
                {
                    break;
                }
            }

            Message::Telemetry { .. } => {
                if send_message(
                    &mut stream,
                    &Message::Success {
                        metadata: HashMap::new(),
                    },
                )
                .await
                .is_err()
                {
                    break;
                }
            }

            Message::Success { .. }
            | Message::Failure { .. }
            | Message::Ignored
            | Message::Record { .. } => {
                tracing::warn!("[bolt] received server message from client, ignoring");
            }
        }
    }

    tracing::info!("[bolt] disconnected {}", peer_str);
    if let Some(cid) = conn_id {
        admin.unregister_connection(cid);
    }
}

// ─── Async framing helpers ───────────────────────────────────────────────

async fn read_message(stream: &mut BoltStream) -> std::io::Result<Vec<u8>> {
    let mut payload = Vec::new();
    loop {
        let mut size_buf = [0u8; 2];
        stream.read_exact(&mut size_buf).await?;
        let chunk_size = u16::from_be_bytes(size_buf) as usize;
        if chunk_size == 0 {
            break;
        }
        let mut chunk = vec![0u8; chunk_size];
        stream.read_exact(&mut chunk).await?;
        payload.extend_from_slice(&chunk);
    }
    Ok(payload)
}

async fn write_message(stream: &mut BoltStream, payload: &[u8]) -> std::io::Result<()> {
    let mut offset = 0;
    while offset < payload.len() {
        let remaining = payload.len() - offset;
        let chunk_size = remaining.min(MAX_CHUNK_SIZE);
        let size_header = (chunk_size as u16).to_be_bytes();
        stream.write_all(&size_header).await?;
        stream
            .write_all(&payload[offset..offset + chunk_size])
            .await?;
        offset += chunk_size;
    }
    stream.write_all(&[0x00, 0x00]).await?;
    stream.flush().await
}

/// Generate the next unique bookmark for causal consistency.
fn next_bookmark() -> String {
    use std::sync::atomic::AtomicU64;
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    format!("bm:{}", seq)
}

async fn send_message(stream: &mut BoltStream, msg: &Message) -> std::io::Result<()> {
    let mut payload = Vec::new();
    msg.to_value().encode(&mut payload)?;
    write_message(stream, &payload).await
}

async fn send_failure(stream: &mut BoltStream, code: &str, message: &str) {
    let _ = send_message(
        stream,
        &Message::Failure {
            code: code.into(),
            message: message.into(),
        },
    )
    .await;
}

// ─── State validation ────────────────────────────────────────────────────

fn is_valid_in_state(msg: &Message, state: BoltState) -> bool {
    match state {
        BoltState::Connected => matches!(msg, Message::Hello { .. }),
        BoltState::Authentication => matches!(
            msg,
            Message::Logon { .. } | Message::Reset | Message::Goodbye
        ),
        BoltState::Authenticated => matches!(
            msg,
            Message::Run { .. } | Message::Begin { .. } | Message::Reset | Message::Goodbye
        ),
        BoltState::Ready => matches!(
            msg,
            Message::Run { .. }
                | Message::Begin { .. }
                | Message::Reset
                | Message::Goodbye
                | Message::Route { .. }
                | Message::Telemetry { .. }
                | Message::Logoff
        ),
        BoltState::Streaming => matches!(
            msg,
            Message::Pull { .. }
                | Message::Discard { .. }
                | Message::Reset
                | Message::Goodbye
                | Message::Route { .. }
                | Message::Logoff
        ),
        BoltState::TxReady => matches!(
            msg,
            Message::Run { .. }
                | Message::Commit
                | Message::Rollback
                | Message::Reset
                | Message::Goodbye
                | Message::Route { .. }
                | Message::Telemetry { .. }
                | Message::Logoff
        ),
        BoltState::TxStreaming => matches!(
            msg,
            Message::Pull { .. }
                | Message::Discard { .. }
                | Message::Reset
                | Message::Goodbye
                | Message::Route { .. }
                | Message::Logoff
        ),
        BoltState::Failed => matches!(msg, Message::Reset | Message::Goodbye | Message::Logoff),
    }
}

fn msg_name(msg: &Message) -> &'static str {
    match msg {
        Message::Hello { .. } => "HELLO",
        Message::Run { .. } => "RUN",
        Message::Pull { .. } => "PULL",
        Message::Discard { .. } => "DISCARD",
        Message::Begin { .. } => "BEGIN",
        Message::Commit => "COMMIT",
        Message::Rollback => "ROLLBACK",
        Message::Reset => "RESET",
        Message::Goodbye => "GOODBYE",
        Message::Success { .. } => "SUCCESS",
        Message::Failure { .. } => "FAILURE",
        Message::Ignored => "IGNORED",
        Message::Record { .. } => "RECORD",
        Message::Route { .. } => "ROUTE",
        Message::Telemetry { .. } => "TELEMETRY",
        Message::Logon { .. } => "LOGON",
        Message::Logoff => "LOGOFF",
    }
}

/// Parse a Bolt message from raw PackStream bytes.
fn parse_message(payload: &[u8]) -> Option<Message> {
    let val = match mgbolt::decode_value(payload) {
        Ok(v) => v.0,
        Err(e) => {
            tracing::debug!("[bolt] decode error: {:?}", e);
            return None;
        }
    };
    match val {
        Value::Struct(0x01, ref fields) => {
            let mut meta = fields
                .first()
                .and_then(|v| {
                    if let Value::Map(m) = v {
                        Some(m.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();
            let user_agent = meta
                .remove("user_agent")
                .and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| "unknown".into());
            Some(Message::Hello {
                user_agent,
                extra: meta,
            })
        }
        Value::Struct(0x10, ref fields) => {
            let query = fields
                .first()
                .and_then(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();
            let parameters = fields
                .get(1)
                .and_then(|v| {
                    if let Value::Map(m) = v {
                        Some(m.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();
            let extra = fields
                .get(2)
                .and_then(|v| {
                    if let Value::Map(m) = v {
                        Some(m.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();
            Some(Message::Run {
                query,
                parameters,
                extra,
            })
        }
        Value::Struct(0x3F, ref fields) => {
            let (n, qid) = parse_pull_discard_args(fields);
            Some(Message::Pull { n, qid })
        }
        Value::Struct(0x2F, ref fields) => {
            let (n, qid) = parse_pull_discard_args(fields);
            Some(Message::Discard { n, qid })
        }
        Value::Struct(0x11, ref fields) => {
            let extra = fields
                .first()
                .and_then(|v| {
                    if let Value::Map(m) = v {
                        Some(m.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();
            Some(Message::Begin { extra })
        }
        Value::Struct(0x12, _) => Some(Message::Commit),
        Value::Struct(0x13, _) => Some(Message::Rollback),
        Value::Struct(0x0F, _) => Some(Message::Reset),
        Value::Struct(0x02, _) => Some(Message::Goodbye),
        Value::Struct(0x66, ref fields) => {
            let mut routing = HashMap::new();
            let mut bookmarks = Vec::new();
            let extra = fields
                .first()
                .and_then(|v| {
                    if let Value::Map(m) = v {
                        Some(m.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();
            if let Some(Value::Map(r)) = extra.get("routing") {
                routing = r.clone();
            }
            if let Some(Value::List(bms)) = extra.get("bookmarks") {
                bookmarks = bms
                    .iter()
                    .filter_map(|v| {
                        if let Value::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
            }
            Some(Message::Route {
                routing,
                bookmarks,
                extra,
            })
        }
        Value::Struct(0x54, ref fields) => {
            let extra = fields
                .first()
                .and_then(|v| {
                    if let Value::Map(m) = v {
                        Some(m.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();
            let api = extra
                .get("api")
                .and_then(|v| {
                    if let Value::Int(i) = v {
                        Some(*i)
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            Some(Message::Telemetry { api })
        }
        Value::Struct(0x6A, ref fields) => {
            let extra = fields
                .first()
                .and_then(|v| {
                    if let Value::Map(m) = v {
                        Some(m.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_default();
            Some(Message::Logon { extra })
        }
        Value::Struct(0x6B, _) => Some(Message::Logoff),
        _ => None,
    }
}

fn parse_pull_discard_args(fields: &[Value]) -> (i64, i64) {
    let mut n = -1i64;
    let mut qid = -1i64;
    for (i, field) in fields.iter().enumerate() {
        if let Value::Map(m) = field {
            if i == 0 {
                if let Some(Value::Int(v)) = m.get("n") {
                    n = *v;
                }
            }
            if let Some(Value::Int(v)) = m.get("qid") {
                qid = *v;
            }
        }
    }
    (n, qid)
}
