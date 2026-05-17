//! Durability manager: background thread for automatic snapshots and WAL rotation.
//!
//! Without WAL rotation, a single WAL file grows unbounded and recovery replay
//! time increases indefinitely. The `DurabilityManager` runs a background thread
//! that periodically takes snapshots and rotates WAL files based on configurable
//! thresholds.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mgcatalog::Catalog;
use mgstorage::storage::Storage;

use crate::checkpoint::CheckpointManager;
use crate::recovery::dump_snapshot;
use crate::snapshot::SnapshotWriter;

/// Configuration for the durability manager.
#[derive(Clone, Debug)]
pub struct DurabilityConfig {
    /// Directory for snapshot and WAL files.
    pub data_directory: PathBuf,
    /// How often to check if a snapshot is needed (seconds).
    pub snapshot_interval_secs: u64,
    /// Maximum WAL file size in bytes before forcing a snapshot+rotation.
    pub wal_max_size_bytes: u64,
    /// Maximum number of WAL records before forcing a snapshot+rotation.
    pub wal_max_records: u64,
    /// Number of full snapshots to retain (older ones are deleted).
    pub snapshot_retention: usize,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            data_directory: PathBuf::from("/tmp/memgraph"),
            snapshot_interval_secs: 300, // 5 minutes
            wal_max_size_bytes: 50 * 1024 * 1024, // 50 MB
            wal_max_records: 100_000,
            snapshot_retention: 3,
        }
    }
}

/// Manages automatic snapshots and WAL rotation.
///
/// Runs a background thread that:
/// 1. Periodically checks if a snapshot should be taken (interval or WAL thresholds)
/// 2. Takes a snapshot, writes it to disk
/// 3. Rotates the WAL (removes the old one, creates a new one)
/// 4. Enforces snapshot retention (removes old snapshots)
pub struct DurabilityManager {
    config: DurabilityConfig,
    storage: Arc<Storage>,
    catalog: Arc<Catalog>,
    checkpoint_mgr: Mutex<CheckpointManager>,
    shutting_down: AtomicBool,
    snapshot_count: AtomicU64,
    thread_handle: Mutex<Option<JoinHandle<()>>>,
    // WAL record counter: incremented by the WAL appender wrapper.
    wal_records_since_snapshot: AtomicU64,
}

impl DurabilityManager {
    /// Create a new durability manager and start the background thread.
    pub fn start(
        config: DurabilityConfig,
        storage: Arc<Storage>,
        catalog: Arc<Catalog>,
    ) -> Result<Arc<Self>, std::io::Error> {
        fs::create_dir_all(&config.data_directory)?;
        let snapshots_dir = config.data_directory.join("snapshots");
        fs::create_dir_all(&snapshots_dir)?;

        let checkpoint_mgr =
            CheckpointManager::new(&snapshots_dir, config.snapshot_retention)?;

        let manager = Arc::new(Self {
            config,
            storage,
            catalog,
            checkpoint_mgr: Mutex::new(checkpoint_mgr),
            shutting_down: AtomicBool::new(false),
            snapshot_count: AtomicU64::new(0),
            thread_handle: Mutex::new(None),
            wal_records_since_snapshot: AtomicU64::new(0),
        });

        let mgr_clone = manager.clone();
        let handle = thread::Builder::new()
            .name("durability-manager".into())
            .spawn(move || {
                mgr_clone.run_background_loop();
            })
            .expect("failed to spawn durability-manager thread");

        *manager.thread_handle.lock().unwrap() = Some(handle);

        Ok(manager)
    }

    /// Notify the manager that a WAL record was appended.
    /// Called by the WAL appender wrapper after each record.
    pub fn notify_wal_record(&self) {
        self.wal_records_since_snapshot.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the number of snapshots taken since startup.
    pub fn snapshot_count(&self) -> u64 {
        self.snapshot_count.load(Ordering::Relaxed)
    }

    /// Stop the background thread and take a final snapshot.
    pub fn stop(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        // Take a final snapshot before stopping
        self.take_snapshot_and_rotate_wal();
        if let Some(handle) = self.thread_handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }

    /// Force an immediate snapshot (used for `:snapshot` CLI command).
    pub fn force_snapshot(&self) -> Result<PathBuf, String> {
        self.take_snapshot_and_rotate_wal();
        let seq = self.snapshot_count.load(Ordering::Relaxed);
        let snapshots_dir = self.config.data_directory.join("snapshots");
        Ok(snapshots_dir.join(format!("{}.snap", seq)))
    }

    // ─── background loop ─────────────────────────────────────────────────────

    fn run_background_loop(&self) {
        let check_secs = (self.config.snapshot_interval_secs.max(1)).min(5);
        let mut elapsed = 0u64;

        loop {
            if self.shutting_down.load(Ordering::SeqCst) {
                tracing::info!("[durability] background thread exiting");
                return;
            }

            // Sleep in short chunks so shutdown is responsive
            thread::sleep(Duration::from_secs(check_secs));
            elapsed += check_secs;

            if self.shutting_down.load(Ordering::SeqCst) {
                return;
            }

            if elapsed < self.config.snapshot_interval_secs {
                continue;
            }
            elapsed = 0;

            let should_snapshot = self.should_take_snapshot();
            if should_snapshot {
                self.take_snapshot_and_rotate_wal();
            }
        }
    }

    /// Check if a snapshot should be taken based on WAL thresholds or interval.
    fn should_take_snapshot(&self) -> bool {
        let record_count = self.wal_records_since_snapshot.load(Ordering::Relaxed);
        if record_count >= self.config.wal_max_records {
            tracing::info!(
                "[durability] WAL record threshold reached ({}/{}) — triggering snapshot",
                record_count,
                self.config.wal_max_records
            );
            return true;
        }

        // Check WAL file size
        let wal_path = self.current_wal_path();
        if let Ok(metadata) = fs::metadata(&wal_path) {
            let size = metadata.len();
            if size >= self.config.wal_max_size_bytes {
                tracing::info!(
                    "[durability] WAL size threshold reached ({:.1}/{:.1} MB) — triggering snapshot",
                    size as f64 / (1024.0 * 1024.0),
                    self.config.wal_max_size_bytes as f64 / (1024.0 * 1024.0)
                );
                return true;
            }
        }

        false
    }

    // ─── snapshot + WAL rotation ─────────────────────────────────────────────

    fn take_snapshot_and_rotate_wal(&self) {
        let seq = self.snapshot_count.fetch_add(1, Ordering::SeqCst) + 1;
        let snapshots_dir = self.config.data_directory.join("snapshots");
        let snap_path = snapshots_dir.join(format!("{}.snap", seq));

        tracing::info!("[durability] taking snapshot #{} at {}", seq, snap_path.display());

        let snap_data = dump_snapshot(&self.storage, Some(&self.catalog));

        if let Err(e) = SnapshotWriter::write(&snap_path, &snap_data) {
            tracing::error!("[durability] failed to write snapshot: {}", e);
            return;
        }

        self.storage.sync_wal();

        {
            let mut mgr = self.checkpoint_mgr.lock().unwrap();
            mgr.register_snapshot(seq, &snap_path);
        }

        // Truncate the WAL so recovery only replays deltas since this snapshot
        self.storage.reset_wal();

        self.wal_records_since_snapshot.store(0, Ordering::SeqCst);

        tracing::info!("[durability] snapshot #{} complete, WAL rotated", seq);
    }

    /// Get the path of the current WAL file.
    fn current_wal_path(&self) -> PathBuf {
        self.config.data_directory.join("wal.mgwal")
    }
}

/// List WAL files for recovery, sorted chronologically.
/// WAL files follow the naming pattern: `wal_N.mgwal` (N is sequence number)
/// plus the current `wal.mgwal`.
///
/// Returns paths in order: oldest WAL first, current WAL last.
pub fn list_wal_files(data_directory: &Path) -> Vec<PathBuf> {
    let wal_dir = data_directory;
    let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();

    if let Ok(entries) = fs::read_dir(wal_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            // Pattern: wal_N.mgwal (rotated WALs)
            if name.starts_with("wal_") && name.ends_with(".mgwal") {
                let seq_str = &name[4..name.len() - 6]; // strip "wal_" prefix and ".mgwal" suffix
                if let Ok(seq) = seq_str.parse::<u64>() {
                    wal_files.push((seq, path));
                }
            }
        }
    }

    // Sort by sequence number (oldest first)
    wal_files.sort_by_key(|(seq, _)| *seq);
    let mut result: Vec<PathBuf> = wal_files.into_iter().map(|(_, p)| p).collect();

    // Add current WAL (always replayed last)
    let current_wal = data_directory.join("wal.mgwal");
    if current_wal.exists() {
        result.push(current_wal);
    }

    result
}

/// Clean up WAL files that are older than the latest full snapshot.
/// After a snapshot is taken, all WAL files with sequence numbers
/// less than the snapshot's sequence are no longer needed for recovery.
pub fn cleanup_old_wals(data_directory: &Path, snapshot_sequence: u64) {
    if let Ok(entries) = fs::read_dir(data_directory) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.starts_with("wal_") && name.ends_with(".mgwal") {
                let seq_str = &name[4..name.len() - 6];
                if let Ok(seq) = seq_str.parse::<u64>() {
                    if seq < snapshot_sequence {
                        if let Err(e) = fs::remove_file(&path) {
                            tracing::warn!(
                                "[durability] failed to remove old WAL {}: {}",
                                path.display(),
                                e
                            );
                        } else {
                            tracing::debug!(
                                "[durability] removed old WAL {}",
                                path.display()
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::delta::IsolationLevel;
    use mgcore::property_value::PropertyValue;
    use mgcore::types::{Gid, LabelId, PropertyId};
    use mgstorage::storage::Storage;
    use std::fs;

    fn tmp_dir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).ok();
        p
    }

    #[test]
    fn test_durability_config_defaults() {
        let config = DurabilityConfig::default();
        assert_eq!(config.snapshot_interval_secs, 300);
        assert_eq!(config.wal_max_size_bytes, 50 * 1024 * 1024);
        assert_eq!(config.wal_max_records, 100_000);
        assert_eq!(config.snapshot_retention, 3);
    }

    #[test]
    fn test_list_wal_files_empty() {
        let dir = tmp_dir("mg_wal_list_empty");
        let files = list_wal_files(&dir);
        assert!(files.is_empty());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_list_wal_files_with_sequence() {
        let dir = tmp_dir("mg_wal_list_seq");
        fs::write(dir.join("wal_1.mgwal"), b"w1").unwrap();
        fs::write(dir.join("wal_3.mgwal"), b"w3").unwrap();
        fs::write(dir.join("wal_2.mgwal"), b"w2").unwrap();
        fs::write(dir.join("wal.mgwal"), b"current").unwrap();
        // Non-WAL file should be ignored
        fs::write(dir.join("other.dat"), b"x").unwrap();

        let files = list_wal_files(&dir);
        assert_eq!(files.len(), 4);
        // Sorted: wal_1, wal_2, wal_3, then current wal
        assert!(files[0].ends_with("wal_1.mgwal"));
        assert!(files[1].ends_with("wal_2.mgwal"));
        assert!(files[2].ends_with("wal_3.mgwal"));
        assert!(files[3].ends_with("wal.mgwal"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_cleanup_old_wals() {
        let dir = tmp_dir("mg_wal_cleanup");
        fs::write(dir.join("wal_1.mgwal"), b"w1").unwrap();
        fs::write(dir.join("wal_2.mgwal"), b"w2").unwrap();
        fs::write(dir.join("wal_3.mgwal"), b"w3").unwrap();
        fs::write(dir.join("wal.mgwal"), b"current").unwrap();

        // Snapshot at sequence 3 — should remove WALs 1 and 2
        cleanup_old_wals(&dir, 3);

        assert!(!dir.join("wal_1.mgwal").exists());
        assert!(!dir.join("wal_2.mgwal").exists());
        assert!(dir.join("wal_3.mgwal").exists());
        assert!(dir.join("wal.mgwal").exists());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_durability_manager_force_snapshot() {
        let dir = tmp_dir("mg_dur_force_snap");
        let storage = Arc::new(Storage::new());
        let catalog = Arc::new(Catalog::new());

        // Create some data
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage
            .vertex_set_property(
                &tx,
                Gid::from(1u64),
                PropertyId::from(0u32),
                PropertyValue::String("test".into()),
            )
            .unwrap();
        storage.commit_transaction(&tx);

        let config = DurabilityConfig {
            data_directory: dir.clone(),
            snapshot_interval_secs: 3600, // long interval so it doesn't auto-trigger
            wal_max_size_bytes: u64::MAX,
            wal_max_records: u64::MAX,
            snapshot_retention: 5,
        };

        let manager = DurabilityManager::start(config, storage, catalog).unwrap();

        // Force a snapshot
        let snap_path = manager.force_snapshot().unwrap();
        assert!(snap_path.exists());
        assert_eq!(manager.snapshot_count(), 1); // force_snapshot increments once

        manager.stop();
        assert_eq!(manager.snapshot_count(), 2); // stop takes final snapshot
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_durability_manager_wal_record_counter() {
        let dir = tmp_dir("mg_dur_wal_count");
        let storage = Arc::new(Storage::new());
        let catalog = Arc::new(Catalog::new());

        let config = DurabilityConfig {
            data_directory: dir.clone(),
            snapshot_interval_secs: 3600,
            wal_max_size_bytes: u64::MAX,
            wal_max_records: 3, // trigger snapshot after 3 WAL records
            snapshot_retention: 5,
        };

        let manager = DurabilityManager::start(config, storage, catalog).unwrap();

        // Simulate WAL record notifications
        manager.notify_wal_record();
        manager.notify_wal_record();
        assert_eq!(
            manager.wal_records_since_snapshot.load(Ordering::Relaxed),
            2
        );

        // Third record should trigger snapshot threshold on next check
        manager.notify_wal_record();

        // But since the background loop is slow (3600s interval), we force it
        manager.force_snapshot().unwrap();

        // Counter should be reset after snapshot
        assert_eq!(
            manager.wal_records_since_snapshot.load(Ordering::Relaxed),
            0
        );

        manager.stop();
        fs::remove_dir_all(&dir).ok();
    }
}