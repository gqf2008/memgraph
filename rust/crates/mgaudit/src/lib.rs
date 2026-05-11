//! Query audit logging for Memgraph.
//!
//! Equivalent to C++ `src/audit/log.hpp` and `log.cpp`.
//! Records every executed query with timestamp, user, address, database,
//! and parameters to a JSON-formatted log file with periodic background flush.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mgcore::property_value::PropertyValue;

/// A single audit log entry.
#[derive(Clone, Debug)]
pub struct AuditEntry {
    /// Unix timestamp in microseconds.
    pub timestamp_us: i64,
    /// Client network address.
    pub address: String,
    /// Authenticated username.
    pub username: String,
    /// Executed query text.
    pub query: String,
    /// Query parameters (if any).
    pub params: HashMap<String, PropertyValue>,
    /// Target database name.
    pub db: String,
}

/// Configuration for the audit log.
#[derive(Clone, Debug)]
pub struct AuditConfig {
    /// Directory where `audit.log` is written.
    pub storage_directory: PathBuf,
    /// Maximum number of entries to buffer before forced flush.
    pub buffer_size: usize,
    /// Interval between automatic flushes (milliseconds).
    pub buffer_flush_interval_ms: u32,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            storage_directory: PathBuf::from("./audit"),
            buffer_size: 1000,
            buffer_flush_interval_ms: 5000,
        }
    }
}

/// Thread-safe audit log with buffered writes and periodic flush.
pub struct AuditLog {
    config: AuditConfig,
    started: std::sync::atomic::AtomicBool,
    buffer: Mutex<Vec<AuditEntry>>,
    file: Mutex<Option<BufWriter<fs::File>>>,
    flush_handle: Mutex<Option<thread::JoinHandle<()>>>,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl AuditLog {
    /// Create a new audit log (not started yet).
    pub fn new(config: AuditConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            started: std::sync::atomic::AtomicBool::new(false),
            buffer: Mutex::new(Vec::new()),
            file: Mutex::new(None),
            flush_handle: Mutex::new(None),
            shutdown: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Start the audit log: create directory, open file, spawn flush thread.
    pub fn start(self: &Arc<Self>) -> Result<(), String> {
        if self.started.load(std::sync::atomic::Ordering::Acquire) {
            return Err("audit log already started".into());
        }

        fs::create_dir_all(&self.config.storage_directory)
            .map_err(|e| format!("failed to create audit directory: {}", e))?;

        let log_path = self.config.storage_directory.join("audit.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| format!("failed to open audit.log: {}", e))?;

        *self.file.lock().unwrap() = Some(BufWriter::new(file));
        self.started.store(true, std::sync::atomic::Ordering::Release);

        // Spawn background flush thread
        let interval = Duration::from_millis(self.config.buffer_flush_interval_ms as u64);
        let self_weak = Arc::downgrade(self);
        let shutdown = self.shutdown.clone();
        let handle = thread::spawn(move || {
            while !shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                thread::sleep(interval);
                if let Some(log) = self_weak.upgrade() {
                    if let Err(e) = log.flush() {
                        tracing::warn!("audit flush failed: {}", e);
                    }
                } else {
                    break;
                }
            }
        });
        *self.flush_handle.lock().unwrap() = Some(handle);

        tracing::info!("audit log started at {:?}", log_path);
        Ok(())
    }

    /// Record a query execution. Thread-safe, non-blocking (just appends to buffer).
    pub fn record(
        &self,
        address: &str,
        username: &str,
        query: &str,
        params: &HashMap<String, PropertyValue>,
        db: &str,
    ) {
        if !self.started.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }

        let timestamp_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;

        let entry = AuditEntry {
            timestamp_us,
            address: address.to_string(),
            username: username.to_string(),
            query: query.to_string(),
            params: params.clone(),
            db: db.to_string(),
        };

        let mut buffer = self.buffer.lock().unwrap();
        buffer.push(entry);

        // Force flush if buffer is full
        if buffer.len() >= self.config.buffer_size {
            drop(buffer); // release lock before flush
            if let Err(e) = self.flush() {
                tracing::warn!("audit forced flush failed: {}", e);
            }
        }
    }

    /// Flush buffered entries to disk. Called periodically or on shutdown.
    pub fn flush(&self) -> Result<(), String> {
        let entries: Vec<AuditEntry> = {
            let mut buffer = self.buffer.lock().unwrap();
            if buffer.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *buffer)
        };

        let mut file_guard = self.file.lock().unwrap();
        let writer = file_guard
            .as_mut()
            .ok_or("audit log file not open")?;

        for entry in entries {
            let line = format_audit_line(&entry);
            writeln!(writer, "{}", line)
                .map_err(|e| format!("failed to write audit entry: {}", e))?;
        }

        writer.flush().map_err(|e| format!("failed to flush audit file: {}", e))?;
        Ok(())
    }

    /// Reopen the log file (for log rotation).
    pub fn reopen_log(&self) -> Result<(), String> {
        if !self.started.load(std::sync::atomic::Ordering::Relaxed) {
            return Err("audit log not started".into());
        }

        // Flush any pending entries with the old file handle
        self.flush()?;

        let mut file_guard = self.file.lock().unwrap();
        *file_guard = None;

        let log_path = self.config.storage_directory.join("audit.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| format!("failed to reopen audit.log: {}", e))?;

        *file_guard = Some(BufWriter::new(file));
        tracing::info!("audit log reopened at {:?}", log_path);
        Ok(())
    }

    /// Stop the audit log: signal shutdown, flush remaining entries, close file.
    pub fn stop(&self) {
        if !self.started.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }

        self.started.store(false, std::sync::atomic::Ordering::Release);
        self.shutdown.store(true, std::sync::atomic::Ordering::Relaxed);

        // Wait for flush thread to finish
        if let Some(handle) = self.flush_handle.lock().unwrap().take() {
            let _ = handle.join();
        }

        // Final flush
        let _ = self.flush();

        // Close file
        let mut file_guard = self.file.lock().unwrap();
        *file_guard = None;

        tracing::info!("audit log stopped");
    }
}

impl Drop for AuditLog {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Format an audit entry as a JSON line.
fn format_audit_line(entry: &AuditEntry) -> String {
    let sec = entry.timestamp_us / 1_000_000;
    let us = (entry.timestamp_us % 1_000_000).unsigned_abs();

    let params_json: serde_json::Map<String, serde_json::Value> = entry
        .params
        .iter()
        .map(|(k, v)| (k.clone(), property_value_to_json(v)))
        .collect();

    serde_json::json!({
        "timestamp": format!("{}.{:06}", sec, us),
        "address": entry.address,
        "username": entry.username,
        "db": entry.db,
        "query": entry.query,
        "params": params_json,
    })
    .to_string()
}

fn property_value_to_json(value: &PropertyValue) -> serde_json::Value {
    match value {
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::Bool(b) => serde_json::Value::Bool(*b),
        PropertyValue::Int(i) => serde_json::Value::Number((*i).into()),
        PropertyValue::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
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
        _ => serde_json::Value::String(format!("{:?}", value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_audit_record_and_flush() {
        let tmp = tempfile::tempdir().unwrap();
        let config = AuditConfig {
            storage_directory: tmp.path().to_path_buf(),
            buffer_size: 10,
            buffer_flush_interval_ms: 100,
        };

        let log = AuditLog::new(config);
        log.start().unwrap();

        let mut params = HashMap::new();
        params.insert("name".into(), PropertyValue::String("Alice".into()));
        log.record("127.0.0.1:7687", "alice", "CREATE (n:Person {name: $name})", &params, "default");

        // Force flush
        log.flush().unwrap();

        // Read log file
        let content = fs::read_to_string(tmp.path().join("audit.log")).unwrap();
        assert!(content.contains("CREATE (n:Person"));
        assert!(content.contains("Alice"));
        assert!(content.contains("127.0.0.1"));

        log.stop();
    }

    #[test]
    fn test_audit_reopen_log() {
        let tmp = tempfile::tempdir().unwrap();
        let config = AuditConfig {
            storage_directory: tmp.path().to_path_buf(),
            buffer_size: 10,
            buffer_flush_interval_ms: 100,
        };

        let log = AuditLog::new(config);
        log.start().unwrap();

        log.record("127.0.0.1", "bob", "MATCH (n) RETURN n", &HashMap::new(), "default");
        log.flush().unwrap();

        let before = fs::read_to_string(tmp.path().join("audit.log")).unwrap();
        assert!(before.contains("MATCH (n)"));

        log.reopen_log().unwrap();

        log.record("127.0.0.1", "bob", "CREATE (n)", &HashMap::new(), "default");
        log.flush().unwrap();

        let after = fs::read_to_string(tmp.path().join("audit.log")).unwrap();
        assert!(after.contains("CREATE (n)"));

        log.stop();
    }

    #[test]
    fn test_audit_not_started_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let config = AuditConfig {
            storage_directory: tmp.path().to_path_buf(),
            buffer_size: 10,
            buffer_flush_interval_ms: 100,
        };

        let log = AuditLog::new(config);
        // Don't start
        log.record("127.0.0.1", "anon", "RETURN 1", &HashMap::new(), "default");
        log.flush().unwrap(); // should be no-op

        // File should not exist
        assert!(!tmp.path().join("audit.log").exists());
    }
}
