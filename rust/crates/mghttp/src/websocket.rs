//! WebSocket log streaming server for Memgraph.
//!
//! Equivalent to C++ `src/communication/websocket/`.
//! Provides a WebSocket endpoint that broadcasts server log messages
//! to all authenticated connected clients.
//!
//! Protocol:
//! 1. Client connects to ws://host:port/
//! 2. Server expects first message: JSON auth `{ "username": "...", "password": "..." }`
//! 3. If auth succeeds, client receives `{ "type": "auth_ok" }`
//! 4. Server then streams log messages as JSON: `{ "type": "log", "level": "INFO", "message": "...", "timestamp": 1234567890 }`
//! 5. Clients can send `{ "type": "ping" }` and receive `{ "type": "pong" }`

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::tungstenite::protocol::Message;

/// A log entry broadcast to all connected WebSocket clients.
#[derive(Clone, Debug, serde::Serialize)]
pub struct LogEntry {
    pub level: String,
    pub message: String,
    pub timestamp_secs: u64,
    pub source: Option<String>,
}

/// Central broadcaster that distributes log messages to all WebSocket clients.
pub struct LogBroadcaster {
    tx: broadcast::Sender<LogEntry>,
}

impl LogBroadcaster {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Broadcast a log entry to all connected clients.
    pub fn broadcast(&self, entry: LogEntry) {
        let _ = self.tx.send(entry);
    }

    /// Subscribe to log entries.
    pub fn subscribe(&self) -> broadcast::Receiver<LogEntry> {
        self.tx.subscribe()
    }
}

impl Default for LogBroadcaster {
    fn default() -> Self {
        Self::new(1024)
    }
}

/// Client authentication state.
#[derive(Clone, Debug)]
enum AuthState {
    Pending,
    Authenticated { _username: String },
    Rejected,
}


/// WebSocket server configuration.
pub struct WebSocketConfig {
    pub addr: SocketAddr,
    pub auth_enabled: bool,
    pub bolt_user: Option<String>,
    pub bolt_pass: Option<String>,
}

/// WebSocket server that streams log messages to authenticated clients.
pub struct WebSocketServer {
    config: WebSocketConfig,
    broadcaster: Arc<LogBroadcaster>,
    clients: Arc<RwLock<HashSet<SocketAddr>>>,
}

impl WebSocketServer {
    pub fn new(config: WebSocketConfig, broadcaster: Arc<LogBroadcaster>) -> Self {
        Self {
            config,
            broadcaster,
            clients: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Start the WebSocket server. Runs until the process exits.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(self.config.addr).await?;
        tracing::info!("WebSocket server listening on ws://{}", self.config.addr);

        loop {
            let (stream, addr) = listener.accept().await?;
            let broadcaster = self.broadcaster.clone();
            let clients = self.clients.clone();
            let config = WebSocketConfig {
                addr: self.config.addr,
                auth_enabled: self.config.auth_enabled,
                bolt_user: self.config.bolt_user.clone(),
                bolt_pass: self.config.bolt_pass.clone(),
            };

            tokio::spawn(async move {
                clients.write().await.insert(addr);
                if let Err(e) = handle_connection(stream, addr, config, broadcaster).await {
                    tracing::debug!("WebSocket connection {} error: {}", addr, e);
                }
                clients.write().await.remove(&addr);
            });
        }
    }

    /// Get the number of connected clients.
    pub async fn client_count(&self) -> usize {
        self.clients.read().await.len()
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    _addr: SocketAddr,
    config: WebSocketConfig,
    broadcaster: Arc<LogBroadcaster>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    let mut auth_state = AuthState::Pending;
    let mut log_rx = broadcaster.subscribe();

    loop {
        tokio::select! {
            msg = ws_rx.next() => {
                let Some(msg) = msg else { break };
                let msg = msg?;

                match msg {
                    Message::Text(text) => {
                        match handle_client_message(&text, &config, &mut auth_state).await {
                            Some(response) => {
                                ws_tx.send(Message::Text(response)).await?;
                            }
                            None => {}
                        }
                    }
                    Message::Close(_) => break,
                    Message::Ping(data) => {
                        ws_tx.send(Message::Pong(data)).await?;
                    }
                    _ => {}
                }
            }

            Ok(entry) = log_rx.recv() => {
                match &auth_state {
                    AuthState::Authenticated { .. } => {
                        let json = serde_json::to_string(&entry)?;
                        if ws_tx.send(Message::Text(json)).await.is_err() {
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    let _ = ws_tx.close().await;
    Ok(())
}

async fn handle_client_message(
    text: &str,
    config: &WebSocketConfig,
    auth_state: &mut AuthState,
) -> Option<String> {
    let json: serde_json::Value = serde_json::from_str(text).ok()?;

    match json.get("type").and_then(|v| v.as_str()) {
        Some("auth") | Some("login") | None => {
            // Auth message: { "username": "...", "password": "..." }
            // or legacy: { "type": "auth", "username": "...", "password": "..." }
            let username = json.get("username").and_then(|v| v.as_str())?;
            let password = json.get("password").and_then(|v| v.as_str())?;

            if authenticate(config, username, password) {
                *auth_state = AuthState::Authenticated {
                    _username: username.to_string(),
                };
                Some(r#"{"type":"auth_ok","message":"authenticated"}"#.to_string())
            } else {
                *auth_state = AuthState::Rejected;
                Some(r#"{"type":"auth_error","message":"invalid credentials"}"#.to_string())
            }
        }
        Some("ping") => {
            Some(r#"{"type":"pong"}"#.to_string())
        }
        Some("subscribe") => {
            // Acknowledge subscription request
            Some(r#"{"type":"subscribed","channel":"logs"}"#.to_string())
        }
        Some(other) => {
            Some(format!(r#"{{"type":"error","message":"unknown type: {}"}}"#, other))
        }
    }
}

fn authenticate(config: &WebSocketConfig, username: &str, password: &str) -> bool {
    if !config.auth_enabled {
        return true;
    }
    match (&config.bolt_user, &config.bolt_pass) {
        (Some(u), Some(p)) => u == username && p == password,
        _ => false,
    }
}

/// Convenience function to create a default WebSocket server from flags.
pub fn server_from_flags(
    flags: &mgflags::Flags,
    broadcaster: Arc<LogBroadcaster>,
) -> Option<WebSocketServer> {
    if !flags.websocket_enabled || flags.websocket_port == 0 {
        return None;
    }
    let addr: SocketAddr = format!("{}:{}", flags.websocket_address, flags.websocket_port)
        .parse()
        .ok()?;
    let config = WebSocketConfig {
        addr,
        auth_enabled: flags.auth_enabled,
        bolt_user: flags.bolt_user.clone(),
        bolt_pass: flags.bolt_pass.clone(),
    };
    Some(WebSocketServer::new(config, broadcaster))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_broadcaster_broadcast() {
        let broadcaster = LogBroadcaster::new(10);
        let mut rx = broadcaster.subscribe();

        let entry = LogEntry {
            level: "INFO".into(),
            message: "test".into(),
            timestamp_secs: 1234,
            source: None,
        };
        broadcaster.broadcast(entry.clone());

        let received = rx.try_recv().unwrap();
        assert_eq!(received.message, "test");
        assert_eq!(received.level, "INFO");
    }

    #[test]
    fn test_log_broadcaster_multiple_subscribers() {
        let broadcaster = LogBroadcaster::new(10);
        let mut rx1 = broadcaster.subscribe();
        let mut rx2 = broadcaster.subscribe();

        broadcaster.broadcast(LogEntry {
            level: "WARN".into(),
            message: "multi".into(),
            timestamp_secs: 1,
            source: None,
        });

        assert_eq!(rx1.try_recv().unwrap().message, "multi");
        assert_eq!(rx2.try_recv().unwrap().message, "multi");
    }

    #[test]
    fn test_authenticate_no_auth() {
        let config = WebSocketConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            auth_enabled: false,
            bolt_user: None,
            bolt_pass: None,
        };
        assert!(authenticate(&config, "anyone", "anything"));
    }

    #[test]
    fn test_authenticate_with_credentials() {
        let config = WebSocketConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            auth_enabled: true,
            bolt_user: Some("admin".into()),
            bolt_pass: Some("secret".into()),
        };
        assert!(authenticate(&config, "admin", "secret"));
        assert!(!authenticate(&config, "admin", "wrong"));
        assert!(!authenticate(&config, "other", "secret"));
    }

    #[tokio::test]
    async fn test_handle_client_message_auth() {
        let config = WebSocketConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            auth_enabled: true,
            bolt_user: Some("user".into()),
            bolt_pass: Some("pass".into()),
        };
        let mut auth_state = AuthState::Pending;

        let resp = handle_client_message(
            r#"{"username":"user","password":"pass"}"#,
            &config,
            &mut auth_state,
        )
        .await;
        assert!(resp.unwrap().contains("auth_ok"));
        assert!(matches!(auth_state, AuthState::Authenticated { .. }));
    }

    #[tokio::test]
    async fn test_handle_client_message_bad_auth() {
        let config = WebSocketConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            auth_enabled: true,
            bolt_user: Some("user".into()),
            bolt_pass: Some("pass".into()),
        };
        let mut auth_state = AuthState::Pending;

        let resp = handle_client_message(
            r#"{"username":"user","password":"wrong"}"#,
            &config,
            &mut auth_state,
        )
        .await;
        assert!(resp.unwrap().contains("auth_error"));
        assert!(matches!(auth_state, AuthState::Rejected));
    }

    #[tokio::test]
    async fn test_handle_ping() {
        let config = WebSocketConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            auth_enabled: false,
            bolt_user: None,
            bolt_pass: None,
        };
        let mut auth_state = AuthState::Pending;

        let resp = handle_client_message(r#"{"type":"ping"}"#, &config, &mut auth_state).await;
        assert_eq!(resp.unwrap(), r#"{"type":"pong"}"#);
    }

    #[tokio::test]
    async fn test_websocket_server_client_count() {
        let config = WebSocketConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            auth_enabled: false,
            bolt_user: None,
            bolt_pass: None,
        };
        let broadcaster = Arc::new(LogBroadcaster::new(10));
        let server = WebSocketServer::new(config, broadcaster);
        assert_eq!(server.client_count().await, 0);
    }
}
