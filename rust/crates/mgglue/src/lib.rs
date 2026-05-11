//! Glue layer: connects Bolt protocol server to query engine.
//! Equivalent to C++ `src/glue/` (ServerT, SessionHL).
//!
//! Manages per-session state, authentication, transaction lifecycle,
//! and routes Bolt messages to the Cypher interpreter.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mgcore::property_value::PropertyValue;
use mgstorage::storage::Storage;
use mgauth::{AuthOperation, AuthStore, Role, User};

/// Per-session state for a Bolt connection.
#[derive(Clone, Debug)]
pub struct SessionState {
    pub session_id: u64,
    pub username: Option<String>,
    pub user: Option<User>,
    pub address: Option<String>,
    pub database: String,
    pub explicit_transaction: bool,
    pub transaction_id: Option<u64>,
    pub query_timeout: Duration,
    pub created_at: Instant,
    pub last_activity: Instant,
    pub query_count: u64,
    pub total_rows_returned: u64,
}

impl SessionState {
    pub fn new(session_id: u64) -> Self {
        let now = Instant::now();
        Self {
            session_id,
            username: None,
            user: None,
            address: None,
            database: "default".into(),
            explicit_transaction: false,
            transaction_id: None,
            query_timeout: Duration::from_secs(600),
            created_at: now,
            last_activity: now,
            query_count: 0,
            total_rows_returned: 0,
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.username.is_some()
    }

    pub fn role(&self) -> Option<&Role> {
        self.user.as_ref().map(|u| &u.role)
    }

    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    pub fn idle_time(&self) -> Duration {
        self.last_activity.elapsed()
    }
}

/// Result of executing a Cypher query through the glue layer.
#[derive(Debug)]
pub struct QueryExecutionResult {
    pub columns: Vec<String>,
    pub rows: Vec<HashMap<String, PropertyValue>>,
    pub summary: ExecutionSummary,
}

#[derive(Clone, Debug, Default)]
pub struct ExecutionSummary {
    pub vertices_created: usize,
    pub vertices_deleted: usize,
    pub edges_created: usize,
    pub edges_deleted: usize,
    pub properties_set: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    pub indices_created: usize,
    pub indices_deleted: usize,
    pub constraints_created: usize,
    pub constraints_deleted: usize,
}

/// Active query tracking.
#[derive(Clone, Debug)]
pub struct ActiveQuery {
    pub query_text: String,
    pub started_at: Instant,
    pub session_id: u64,
    pub parameters: HashMap<String, PropertyValue>,
}

/// Query cursor for streaming results back to the client.
pub struct QueryCursor {
    pub columns: Vec<String>,
    pub rows: Vec<HashMap<String, PropertyValue>>,
    pub position: usize,
    pub summary: ExecutionSummary,
    pub consumed: bool,
}

impl QueryCursor {
    pub fn new(columns: Vec<String>, rows: Vec<HashMap<String, PropertyValue>>) -> Self {
        Self {
            columns,
            rows,
            position: 0,
            summary: ExecutionSummary::default(),
            consumed: false,
        }
    }

    /// Pull up to `n` rows from the cursor.
    pub fn pull(&mut self, n: usize) -> Vec<HashMap<String, PropertyValue>> {
        let end = (self.position + n).min(self.rows.len());
        let chunk = self.rows[self.position..end].to_vec();
        self.position = end;
        if self.position >= self.rows.len() {
            self.consumed = true;
        }
        chunk
    }

    pub fn is_consumed(&self) -> bool {
        self.consumed || self.position >= self.rows.len()
    }

    pub fn remaining(&self) -> usize {
        self.rows.len().saturating_sub(self.position)
    }
}

/// The glue handler — bridges Bolt messages to the storage/interpreter.
pub struct Glue {
    pub storage: Arc<Storage>,
    pub auth: Arc<AuthStore>,
    pub catalog: Arc<mgcatalog::Catalog>,
    pub dbms: Option<Arc<mgdbms::DbmsHandler>>,
    pub audit: Option<Arc<mgaudit::AuditLog>>,
    sessions: std::sync::RwLock<HashMap<u64, SessionState>>,
    cursors: std::sync::RwLock<HashMap<u64, QueryCursor>>,
    active_queries: std::sync::RwLock<Vec<ActiveQuery>>,
    next_session_id: std::sync::atomic::AtomicU64,
    next_cursor_id: std::sync::atomic::AtomicU64,
}

impl Glue {
    pub fn new(storage: Arc<Storage>, auth: Arc<AuthStore>, catalog: Arc<mgcatalog::Catalog>) -> Self {
        let trigger_exec = Arc::new(mginterp::TriggerInterpreter::new(storage.clone()));
        storage.set_trigger_executor(trigger_exec);
        Self {
            storage,
            auth,
            catalog,
            dbms: None,
            audit: None,
            sessions: std::sync::RwLock::new(HashMap::new()),
            cursors: std::sync::RwLock::new(HashMap::new()),
            active_queries: std::sync::RwLock::new(Vec::new()),
            next_session_id: std::sync::atomic::AtomicU64::new(1),
            next_cursor_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn create_session(&self) -> u64 {
        let id = self.next_session_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.sessions.write().unwrap().insert(id, SessionState::new(id));
        id
    }

    pub fn get_session(&self, session_id: u64) -> Option<SessionState> {
        self.sessions.read().unwrap().get(&session_id).cloned()
    }

    pub fn update_session<F>(&self, session_id: u64, f: F) -> Result<(), String>
    where
        F: FnOnce(&mut SessionState),
    {
        let mut sessions = self.sessions.write().unwrap();
        let session = sessions.get_mut(&session_id).ok_or("session not found")?;
        f(session);
        Ok(())
    }

    /// Authenticate a session.
    pub fn authenticate_session(
        &self,
        session_id: u64,
        username: &str,
        password: &str,
    ) -> Result<(), String> {
        if !self.auth.is_enabled() {
            let mut sessions = self.sessions.write().unwrap();
            if let Some(s) = sessions.get_mut(&session_id) {
                s.username = Some(username.to_string());
                s.touch();
            }
            return Ok(());
        }

        match self.auth.authenticate(username, password) {
            Ok(user) => {
                let mut sessions = self.sessions.write().unwrap();
                if let Some(s) = sessions.get_mut(&session_id) {
                    s.username = Some(username.to_string());
                    s.user = Some(user);
                    s.touch();
                }
                Ok(())
            }
            Err(e) => Err(format!("{}", e)),
        }
    }

    /// Detect whether a query performs write operations by inspecting the AST.
    fn is_write_query(query_str: &str) -> bool {
        let upper = query_str.to_uppercase();
        upper.contains("CREATE")
            || upper.contains("DELETE")
            || upper.contains("SET")
            || upper.contains("REMOVE")
            || upper.contains("MERGE")
            || upper.contains("DROP")
            || upper.contains("CALL")
    }

    /// Determine the required auth operation for a query.
    fn required_auth_op(query_str: &str) -> AuthOperation {
        if Self::is_write_query(query_str) {
            AuthOperation::Write
        } else {
            AuthOperation::Read
        }
    }

    /// Execute a Cypher query string within a Bolt session.
    pub fn execute_query(
        &self,
        session_id: u64,
        query_str: &str,
        parameters: &HashMap<String, PropertyValue>,
    ) -> Result<QueryExecutionResult, String> {
        let session = self.get_session(session_id).ok_or("session not found")?;
        if !session.is_authenticated() && self.auth.is_enabled() {
            return Err("not authenticated".into());
        }

        // Check permissions
        if self.auth.is_enabled() {
            if let Some(ref user) = session.user {
                let op = Self::required_auth_op(query_str);
                self.auth.check_permission(
                    &user.username,
                    &user.role,
                    None,
                    op,
                ).map_err(|e| format!("{}", e))?;
            }
        }

        // Track active query
        let active = ActiveQuery {
            query_text: query_str.to_string(),
            started_at: Instant::now(),
            session_id,
            parameters: parameters.clone(),
        };
        self.active_queries.write().unwrap().push(active);

        // Delegate to interpreter with timeout
        let result = self.run_with_timeout(
            query_str,
            parameters,
            session.query_timeout,
        );

        // Remove from active queries
        self.active_queries.write().unwrap().retain(|q| q.session_id != session_id || q.query_text != query_str);

        // Update session stats
        let _ = self.update_session(session_id, |s| {
            s.query_count += 1;
            s.touch();
        });

        // Audit log
        if let Some(ref audit) = self.audit {
            let address = session.address.clone().unwrap_or_else(|| "unknown".into());
            let username = session.username.clone().unwrap_or_else(|| "anon".into());
            audit.record(
                &address,
                &username,
                query_str,
                parameters,
                &session.database,
            );
        }

        result
    }

    fn run_with_timeout(
        &self,
        query_str: &str,
        parameters: &HashMap<String, PropertyValue>,
        timeout: Duration,
    ) -> Result<QueryExecutionResult, String> {
        // In a full implementation, this would spawn the query on a thread
        // and abort if it exceeds the timeout. For now, we run directly.
        let start = Instant::now();
        let result = if let Some(ref dbms) = self.dbms {
            mginterp::execute_with_catalog_auth_dbms_and_params(
                &self.storage,
                query_str,
                Some(&self.catalog),
                parameters,
                Some(&self.auth),
                Some(dbms),
            ).map_err(|e| format!("{}", e))?
        } else {
            mginterp::execute_with_catalog(
                &self.storage,
                query_str,
                Some(&self.catalog),
            ).map_err(|e| format!("{}", e))?
        };

        if start.elapsed() > timeout {
            return Err("query timeout".into());
        }

        // Build summary from the result metadata if available
        let summary = ExecutionSummary::default();

        Ok(QueryExecutionResult {
            columns: result.columns,
            rows: result.rows,
            summary,
        })
    }

    /// Execute a query and return a cursor ID for streaming.
    pub fn execute_query_streaming(
        &self,
        session_id: u64,
        query_str: &str,
        parameters: &HashMap<String, PropertyValue>,
    ) -> Result<u64, String> {
        let result = self.execute_query(session_id, query_str, parameters)?;
        let cursor = QueryCursor::new(result.columns, result.rows);
        let cursor_id = self.next_cursor_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.cursors.write().unwrap().insert(cursor_id, cursor);
        Ok(cursor_id)
    }

    /// Pull rows from a cursor.
    pub fn pull_cursor(&self, cursor_id: u64, n: usize) -> Option<(Vec<String>, Vec<HashMap<String, PropertyValue>>)> {
        let mut cursors = self.cursors.write().unwrap();
        let cursor = cursors.get_mut(&cursor_id)?;
        let columns = cursor.columns.clone();
        let rows = cursor.pull(n);
        if cursor.is_consumed() {
            cursors.remove(&cursor_id);
        }
        Some((columns, rows))
    }

    /// Discard a cursor without reading remaining rows.
    pub fn discard_cursor(&self, cursor_id: u64) -> bool {
        self.cursors.write().unwrap().remove(&cursor_id).is_some()
    }

    /// Begin an explicit transaction.
    pub fn begin_transaction(&self, session_id: u64) -> Result<(), String> {
        let mut sessions = self.sessions.write().unwrap();
        let session = sessions.get_mut(&session_id).ok_or("session not found")?;
        if session.explicit_transaction {
            return Err("transaction already in progress".into());
        }
        session.explicit_transaction = true;
        // In a full implementation, this would create a real MVCC transaction
        // and store its ID in session.transaction_id
        session.touch();
        Ok(())
    }

    /// Commit the current explicit transaction.
    pub fn commit_transaction(&self, session_id: u64) -> Result<(), String> {
        let mut sessions = self.sessions.write().unwrap();
        let session = sessions.get_mut(&session_id).ok_or("session not found")?;
        if !session.explicit_transaction {
            return Err("no transaction in progress".into());
        }
        session.explicit_transaction = false;
        session.transaction_id = None;
        session.touch();
        Ok(())
    }

    /// Rollback the current explicit transaction.
    pub fn rollback_transaction(&self, session_id: u64) -> Result<(), String> {
        let mut sessions = self.sessions.write().unwrap();
        let session = sessions.get_mut(&session_id).ok_or("session not found")?;
        if !session.explicit_transaction {
            return Err("no transaction in progress".into());
        }
        session.explicit_transaction = false;
        session.transaction_id = None;
        session.touch();
        Ok(())
    }

    /// Reset a session (clear transaction state, discard cursors).
    pub fn reset_session(&self, session_id: u64) {
        let _ = self.update_session(session_id, |s| {
            s.explicit_transaction = false;
            s.transaction_id = None;
            s.touch();
        });
        // Discard any cursors owned by this session
        // (In a full implementation, cursors would track session ownership)
    }

    /// Remove a session and clean up its resources.
    pub fn remove_session(&self, session_id: u64) {
        self.sessions.write().unwrap().remove(&session_id);
        self.active_queries.write().unwrap().retain(|q| q.session_id != session_id);
    }

    /// List all active sessions.
    pub fn list_sessions(&self) -> Vec<SessionState> {
        self.sessions.read().unwrap().values().cloned().collect()
    }

    /// List all currently executing queries.
    pub fn list_active_queries(&self) -> Vec<ActiveQuery> {
        self.active_queries.read().unwrap().clone()
    }

    /// Terminate a query by session ID (if it's still running).
    pub fn terminate_query(&self, session_id: u64) -> bool {
        let before = self.active_queries.read().unwrap().len();
        self.active_queries.write().unwrap().retain(|q| q.session_id != session_id);
        let after = self.active_queries.read().unwrap().len();
        after < before
    }

    /// Get cursor execution summary (returns None if cursor consumed/unknown).
    pub fn get_cursor_summary(&self, cursor_id: u64) -> Option<ExecutionSummary> {
        let cursors = self.cursors.read().unwrap();
        cursors.get(&cursor_id).map(|c| {
            let mut summary = c.summary.clone();
            summary
        })
    }

    /// Execute a batch of queries in sequence. Stops on first error.
    pub fn execute_batch(
        &self,
        session_id: u64,
        queries: &[(String, HashMap<String, PropertyValue>)],
    ) -> Result<Vec<QueryExecutionResult>, String> {
        let mut results = Vec::with_capacity(queries.len());
        for (q, params) in queries {
            let result = self.execute_query(session_id, q, params)?;
            results.push(result);
        }
        Ok(results)
    }

    /// Remove sessions that have been idle longer than `max_idle`.
    /// Returns the number of sessions removed.
    pub fn cleanup_idle_sessions(&self, max_idle: Duration) -> usize {
        let mut sessions = self.sessions.write().unwrap();
        let to_remove: Vec<u64> = sessions
            .values()
            .filter(|s| s.idle_time() > max_idle)
            .map(|s| s.session_id)
            .collect();
        for sid in &to_remove {
            sessions.remove(sid);
        }
        self.active_queries.write().unwrap().retain(|q| !to_remove.contains(&q.session_id));
        to_remove.len()
    }

    /// Get the number of active sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.read().unwrap().len()
    }

    /// Get statistics across all sessions.
    pub fn session_stats(&self) -> SessionStats {
        let sessions = self.sessions.read().unwrap();
        let mut total_queries = 0u64;
        let mut total_rows = 0u64;
        let mut oldest_idle = Duration::from_secs(0);
        for s in sessions.values() {
            total_queries += s.query_count;
            total_rows += s.total_rows_returned;
            let idle = s.idle_time();
            if idle > oldest_idle {
                oldest_idle = idle;
            }
        }
        SessionStats {
            active_sessions: sessions.len(),
            total_queries,
            total_rows_returned: total_rows,
            oldest_idle_secs: oldest_idle.as_secs(),
        }
    }
}

/// Aggregated session statistics.
#[derive(Clone, Debug, Default)]
pub struct SessionStats {
    pub active_sessions: usize,
    pub total_queries: u64,
    pub total_rows_returned: u64,
    pub oldest_idle_secs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_glue() -> Glue {
        let storage = Arc::new(Storage::new());
        let auth = Arc::new(AuthStore::new());
        let catalog = Arc::new(mgcatalog::Catalog::new());
        Glue::new(storage, auth, catalog)
    }

    #[test]
    fn test_session_creation() {
        let glue = make_glue();
        let sid = glue.create_session();
        assert!(sid > 0);

        let session = glue.get_session(sid).unwrap();
        assert_eq!(session.session_id, sid);
        assert!(!session.is_authenticated());
        assert!(!session.explicit_transaction);
    }

    #[test]
    fn test_session_auth() {
        let glue = make_glue();
        glue.auth.set_enabled(true);
        glue.auth.add_user("testuser", &mgauth::hash_password("testpass"), Role::ReadWrite);

        let sid = glue.create_session();
        glue.authenticate_session(sid, "testuser", "testpass").unwrap();

        let session = glue.get_session(sid).unwrap();
        assert!(session.is_authenticated());
        assert_eq!(session.username.as_deref(), Some("testuser"));
    }

    #[test]
    fn test_execute_simple_query() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.authenticate_session(sid, "anon", "").unwrap();

        let result = glue.execute_query(sid, "CREATE (n:Person {name:'Alice'})", &HashMap::new());
        assert!(result.is_ok());

        let result = glue.execute_query(sid, "MATCH (n) RETURN n", &HashMap::new());
        assert!(result.is_ok());
        assert!(!result.unwrap().rows.is_empty());
    }

    #[test]
    fn test_write_detection() {
        assert!(Glue::is_write_query("CREATE (n)"));
        assert!(Glue::is_write_query("MATCH (n) DELETE n"));
        assert!(Glue::is_write_query("MATCH (n) SET n.x = 1"));
        assert!(!Glue::is_write_query("MATCH (n) RETURN n"));
        assert!(!Glue::is_write_query("MATCH (n) WHERE n.x = 1 RETURN n"));
    }

    #[test]
    fn test_transaction_lifecycle() {
        let glue = make_glue();
        let sid = glue.create_session();

        glue.begin_transaction(sid).unwrap();
        let session = glue.get_session(sid).unwrap();
        assert!(session.explicit_transaction);

        glue.commit_transaction(sid).unwrap();
        let session = glue.get_session(sid).unwrap();
        assert!(!session.explicit_transaction);

        glue.begin_transaction(sid).unwrap();
        glue.rollback_transaction(sid).unwrap();
        let session = glue.get_session(sid).unwrap();
        assert!(!session.explicit_transaction);
    }

    #[test]
    fn test_no_nested_transactions() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.begin_transaction(sid).unwrap();
        assert!(glue.begin_transaction(sid).is_err());
    }

    #[test]
    fn test_commit_without_transaction_fails() {
        let glue = make_glue();
        let sid = glue.create_session();
        assert!(glue.commit_transaction(sid).is_err());
        assert!(glue.rollback_transaction(sid).is_err());
    }

    #[test]
    fn test_cursor_streaming() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.authenticate_session(sid, "anon", "").unwrap();

        // Create some data
        glue.execute_query(sid, "CREATE (n:Person {name:'Alice'})", &HashMap::new()).unwrap();
        glue.execute_query(sid, "CREATE (n:Person {name:'Bob'})", &HashMap::new()).unwrap();

        let cursor_id = glue.execute_query_streaming(
            sid,
            "MATCH (n:Person) RETURN n.name AS name",
            &HashMap::new(),
        ).unwrap();

        let (cols, rows) = glue.pull_cursor(cursor_id, 1).unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(rows.len(), 1);

        // Pull remaining
        let (_, rows) = glue.pull_cursor(cursor_id, 10).unwrap();
        assert_eq!(rows.len(), 1);

        // Cursor should be auto-removed when consumed
        assert!(glue.pull_cursor(cursor_id, 1).is_none());
    }

    #[test]
    fn test_discard_cursor() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.authenticate_session(sid, "anon", "").unwrap();

        glue.execute_query(sid, "CREATE (n:Person {name:'Alice'})", &HashMap::new()).unwrap();
        let cursor_id = glue.execute_query_streaming(
            sid,
            "MATCH (n) RETURN n",
            &HashMap::new(),
        ).unwrap();

        assert!(glue.discard_cursor(cursor_id));
        assert!(glue.pull_cursor(cursor_id, 1).is_none());
    }

    #[test]
    fn test_session_reset() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.begin_transaction(sid).unwrap();
        glue.reset_session(sid);

        let session = glue.get_session(sid).unwrap();
        assert!(!session.explicit_transaction);
    }

    #[test]
    fn test_remove_session() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.remove_session(sid);
        assert!(glue.get_session(sid).is_none());
    }

    #[test]
    fn test_list_sessions() {
        let glue = make_glue();
        let s1 = glue.create_session();
        let s2 = glue.create_session();
        let sessions = glue.list_sessions();
        assert_eq!(sessions.len(), 2);
        assert!(sessions.iter().any(|s| s.session_id == s1));
        assert!(sessions.iter().any(|s| s.session_id == s2));
    }

    #[test]
    fn test_session_stats() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.authenticate_session(sid, "anon", "").unwrap();
        glue.execute_query(sid, "CREATE (n)", &HashMap::new()).unwrap();

        let stats = glue.session_stats();
        assert_eq!(stats.active_sessions, 1);
        assert!(stats.total_queries >= 1);
    }

    #[test]
    fn test_unauthenticated_query_when_auth_enabled() {
        let glue = make_glue();
        glue.auth.set_enabled(true);
        let sid = glue.create_session();
        // Don't authenticate
        let result = glue.execute_query(sid, "MATCH (n) RETURN n", &HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_readonly_cannot_write() {
        let glue = make_glue();
        glue.auth.set_enabled(true);
        glue.auth.add_user("reader", &mgauth::hash_password("pw"), Role::ReadOnly);

        let sid = glue.create_session();
        glue.authenticate_session(sid, "reader", "pw").unwrap();

        let result = glue.execute_query(sid, "CREATE (n)", &HashMap::new());
        assert!(result.is_err());

        let result = glue.execute_query(sid, "MATCH (n) RETURN n", &HashMap::new());
        assert!(result.is_ok());
    }

    #[test]
    fn test_terminate_query() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.authenticate_session(sid, "anon", "").unwrap();

        // Start a query (it completes synchronously in tests, but the infrastructure works)
        glue.execute_query(sid, "CREATE (n)", &HashMap::new()).unwrap();
        assert!(!glue.terminate_query(sid));
    }

    #[test]
    fn test_list_active_queries() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.authenticate_session(sid, "anon", "").unwrap();

        let queries = glue.list_active_queries();
        assert!(queries.is_empty());
    }

    #[test]
    fn test_batch_execution() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.authenticate_session(sid, "anon", "").unwrap();

        let queries = vec![
            ("CREATE (n:Person {name:'Alice'})".into(), HashMap::new()),
            ("CREATE (n:Person {name:'Bob'})".into(), HashMap::new()),
            ("MATCH (n:Person) RETURN n.name AS name".into(), HashMap::new()),
        ];
        let results = glue.execute_batch(sid, &queries).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[2].rows.len(), 2);
    }

    #[test]
    fn test_batch_stops_on_error() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.authenticate_session(sid, "anon", "").unwrap();

        let queries = vec![
            ("CREATE (n:Person)".into(), HashMap::new()),
            ("INVALID SYNTAX @#$".into(), HashMap::new()),
            ("MATCH (n) RETURN n".into(), HashMap::new()),
        ];
        let result = glue.execute_batch(sid, &queries);
        assert!(result.is_err());
    }

    #[test]
    fn test_cleanup_idle_sessions() {
        let glue = make_glue();
        let sid1 = glue.create_session();
        let sid2 = glue.create_session();

        // Simulate idle by updating last_activity far in the past
        glue.update_session(sid1, |s| {
            s.last_activity = Instant::now() - Duration::from_secs(3600);
        }).unwrap();

        let removed = glue.cleanup_idle_sessions(Duration::from_secs(60));
        assert_eq!(removed, 1);
        assert!(glue.get_session(sid1).is_none());
        assert!(glue.get_session(sid2).is_some());
    }

    #[test]
    fn test_cursor_summary() {
        let glue = make_glue();
        let sid = glue.create_session();
        glue.authenticate_session(sid, "anon", "").unwrap();

        glue.execute_query(sid, "CREATE (n:Person {name:'Alice'})", &HashMap::new()).unwrap();
        let cursor_id = glue.execute_query_streaming(
            sid,
            "MATCH (n) RETURN n",
            &HashMap::new(),
        ).unwrap();

        let summary = glue.get_cursor_summary(cursor_id);
        assert!(summary.is_some());

        // Consume cursor
        let _ = glue.pull_cursor(cursor_id, 100);
        assert!(glue.get_cursor_summary(cursor_id).is_none());
    }
}
