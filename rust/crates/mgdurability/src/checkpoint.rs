//! Checkpoint management for durability files.
//!
//! Tracks multiple snapshots with monotonic sequence numbers, enforces
//! retention policies, and validates snapshot integrity via checksum stubs.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::snapshot::SnapshotError;

/// Metadata for a single checkpoint (snapshot or incremental snapshot).
#[derive(Clone, Debug, PartialEq)]
pub struct CheckpointMeta {
    pub sequence: u64,
    pub path: PathBuf,
    pub checksum: u64, // CRC32 checksum of file contents
    pub is_incremental: bool,
    pub base_sequence: Option<u64>,
}

/// Manages a directory of snapshot checkpoints.
pub struct CheckpointManager {
    dir: PathBuf,
    retention: usize,
    checkpoints: BTreeMap<u64, CheckpointMeta>,
}

impl CheckpointManager {
    /// Create a new manager for the given directory.
    ///
    /// `retention` is the maximum number of full snapshots to keep.
    /// Older snapshots beyond this count are removed.
    pub fn new(dir: impl AsRef<Path>, retention: usize) -> Result<Self, std::io::Error> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let mut mgr = Self {
            dir,
            retention,
            checkpoints: BTreeMap::new(),
        };
        mgr.scan()?;
        Ok(mgr)
    }

    /// Scan the directory and load existing checkpoint metadata.
    fn scan(&mut self) -> Result<(), std::io::Error> {
        self.checkpoints.clear();
        let entries = match fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(_) => return Ok(()),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "snap" || ext == "incr" {
                    if let Some(seq) = Self::sequence_from_filename(&path) {
                        let is_incr = ext == "incr";
                        let base_seq = if is_incr {
                            Self::base_sequence_from_file(&path).ok()
                        } else {
                            None
                        };
                        let checksum = Self::compute_checksum(&path).unwrap_or(0);
                        self.checkpoints.insert(
                            seq,
                            CheckpointMeta {
                                sequence: seq,
                                path,
                                checksum,
                                is_incremental: is_incr,
                                base_sequence: base_seq,
                            },
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Register a newly written snapshot.
    pub fn register_snapshot(&mut self, sequence: u64, path: impl AsRef<Path>) {
        let path = path.as_ref().to_path_buf();
        let checksum = Self::compute_checksum(&path).unwrap_or(0);
        self.checkpoints.insert(
            sequence,
            CheckpointMeta {
                sequence,
                path,
                checksum,
                is_incremental: false,
                base_sequence: None,
            },
        );
        self.enforce_retention();
    }

    /// Register a newly written incremental snapshot.
    pub fn register_incremental(
        &mut self,
        sequence: u64,
        base_sequence: u64,
        path: impl AsRef<Path>,
    ) {
        let path = path.as_ref().to_path_buf();
        let checksum = Self::compute_checksum(&path).unwrap_or(0);
        self.checkpoints.insert(
            sequence,
            CheckpointMeta {
                sequence,
                path,
                checksum,
                is_incremental: true,
                base_sequence: Some(base_sequence),
            },
        );
    }

    /// Get the latest full snapshot sequence, if any.
    pub fn latest_full_snapshot(&self) -> Option<u64> {
        self.checkpoints
            .values()
            .rev()
            .find(|c| !c.is_incremental)
            .map(|c| c.sequence)
    }

    /// Get the latest checkpoint sequence (full or incremental).
    pub fn latest_sequence(&self) -> Option<u64> {
        self.checkpoints.keys().next_back().copied()
    }

    /// Get metadata for a specific sequence.
    pub fn get(&self, sequence: u64) -> Option<&CheckpointMeta> {
        self.checkpoints.get(&sequence)
    }

    /// List all checkpoint sequences in ascending order.
    pub fn sequences(&self) -> Vec<u64> {
        self.checkpoints.keys().copied().collect()
    }

    /// Remove old full snapshots beyond the retention count.
    fn enforce_retention(&mut self) {
        let fulls: Vec<u64> = self
            .checkpoints
            .values()
            .filter(|c| !c.is_incremental)
            .map(|c| c.sequence)
            .collect();

        if fulls.len() > self.retention {
            let to_remove = fulls.len() - self.retention;
            for seq in fulls.into_iter().take(to_remove) {
                if let Some(meta) = self.checkpoints.remove(&seq) {
                    let _ = fs::remove_file(&meta.path);
                    // Also remove any incrementals that depend on this base
                    let dependent_seqs: Vec<u64> = self
                        .checkpoints
                        .iter()
                        .filter(|(_, c)| c.base_sequence == Some(seq))
                        .map(|(k, _)| *k)
                        .collect();
                    for dep_seq in dependent_seqs {
                        if let Some(dep) = self.checkpoints.remove(&dep_seq) {
                            let _ = fs::remove_file(&dep.path);
                        }
                    }
                }
            }
        }
    }

    /// Validate the integrity of a checkpoint file by recomputing its CRC32.
    pub fn validate(&self, sequence: u64) -> Result<bool, SnapshotError> {
        let meta = self
            .get(sequence)
            .ok_or_else(|| SnapshotError::Corrupt("checkpoint not found".into()))?;
        let current = Self::compute_checksum(&meta.path)?;
        Ok(current == meta.checksum)
    }

    /// Remove a checkpoint and any dependent incrementals.
    pub fn remove(&mut self, sequence: u64) -> Result<(), std::io::Error> {
        if let Some(meta) = self.checkpoints.remove(&sequence) {
            fs::remove_file(&meta.path)?;
            if !meta.is_incremental {
                let dependents: Vec<u64> = self
                    .checkpoints
                    .iter()
                    .filter(|(_, c)| c.base_sequence == Some(sequence))
                    .map(|(k, _)| *k)
                    .collect();
                for dep in dependents {
                    if let Some(dep_meta) = self.checkpoints.remove(&dep) {
                        fs::remove_file(&dep_meta.path)?;
                    }
                }
            }
        }
        Ok(())
    }

    // ─── helpers ─────────────────────────────────────────────────────────────

    fn sequence_from_filename(path: &Path) -> Option<u64> {
        let stem = path.file_stem()?.to_str()?;
        // Handle names like "5.snap" or "42_10.incr"
        stem.split('_').next()?.parse().ok()
    }

    fn base_sequence_from_file(path: &Path) -> Result<u64, SnapshotError> {
        // For incremental files, read the base_sequence from the header.
        // Stub: derive from filename like "42_10.incr" where 10 is base.
        path.file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.split('_').nth(1))
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| SnapshotError::Corrupt("cannot infer base sequence".into()))
    }

    /// Compute a CRC32 checksum of the file contents.
    fn compute_checksum(path: &Path) -> Result<u64, std::io::Error> {
        let data = fs::read(path)?;
        let crc = crc32(&data);
        Ok(crc as u64)
    }
}

/// CRC32 lookup table (IEEE 802.3 polynomial).
static CRC32_TABLE: std::sync::LazyLock<[u32; 256]> = std::sync::LazyLock::new(|| {
    let mut table = [0u32; 256];
    for i in 0..256 {
        let mut crc = i as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = 0xEDB88320 ^ (crc >> 1);
            } else {
                crc >>= 1;
            }
        }
        table[i] = crc;
    }
    table
});

/// Compute CRC32-IEEE checksum of data.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for byte in data {
        crc = CRC32_TABLE[((crc ^ (*byte as u32)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFFFFFF
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_dir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(name);
        let _ = fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn test_checkpoint_manager_scan_and_register() {
        let dir = tmp_dir("mg_ckpt_scan");
        let mut mgr = CheckpointManager::new(&dir, 5).unwrap();

        // Create dummy files
        fs::write(dir.join("1.snap"), b"snapshot1").unwrap();
        fs::write(dir.join("2.snap"), b"snapshot2").unwrap();
        fs::write(dir.join("3_2.incr"), b"incr").unwrap();

        mgr.scan().unwrap();
        assert_eq!(mgr.sequences(), vec![1, 2, 3]);
        assert_eq!(mgr.latest_full_snapshot(), Some(2));
        assert_eq!(mgr.latest_sequence(), Some(3));

        let meta = mgr.get(3).unwrap();
        assert!(meta.is_incremental);
        assert_eq!(meta.base_sequence, Some(2));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_checkpoint_retention_removes_old() {
        let dir = tmp_dir("mg_ckpt_retention");
        let mut mgr = CheckpointManager::new(&dir, 2).unwrap();

        fs::write(dir.join("1.snap"), b"snap1").unwrap();
        fs::write(dir.join("2.snap"), b"snap2").unwrap();
        fs::write(dir.join("3.snap"), b"snap3").unwrap();
        fs::write(dir.join("4_3.incr"), b"incr").unwrap();

        mgr.scan().unwrap();
        // After scan, retention is not enforced automatically on existing files.
        // Register a new one to trigger enforcement.
        fs::write(dir.join("5.snap"), b"snap5").unwrap();
        mgr.register_snapshot(5, dir.join("5.snap"));

        // Should have removed the two oldest full snapshots (1, 2) and keep 3, 5
        assert!(!dir.join("1.snap").exists());
        assert!(!dir.join("2.snap").exists());
        assert!(dir.join("3.snap").exists());
        assert!(dir.join("5.snap").exists());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_checkpoint_validate() {
        let dir = tmp_dir("mg_ckpt_validate");
        let mut mgr = CheckpointManager::new(&dir, 5).unwrap();

        let path = dir.join("10.snap");
        fs::write(&path, b"validate_me").unwrap();
        mgr.register_snapshot(10, &path);

        assert!(mgr.validate(10).unwrap());

        // Corrupt file
        fs::write(&path, b"corrupted!").unwrap();
        assert!(!mgr.validate(10).unwrap());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_checkpoint_remove_with_dependents() {
        let dir = tmp_dir("mg_ckpt_remove");
        let mut mgr = CheckpointManager::new(&dir, 5).unwrap();

        fs::write(dir.join("1.snap"), b"snap1").unwrap();
        fs::write(dir.join("2_1.incr"), b"incr1").unwrap();
        fs::write(dir.join("3_1.incr"), b"incr2").unwrap();
        mgr.scan().unwrap();

        mgr.remove(1).unwrap();
        assert!(!dir.join("1.snap").exists());
        assert!(!dir.join("2_1.incr").exists());
        assert!(!dir.join("3_1.incr").exists());
        assert!(mgr.get(1).is_none());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_checkpoint_manager_empty_dir() {
        let dir = tmp_dir("mg_ckpt_empty");
        let mgr = CheckpointManager::new(&dir, 3).unwrap();
        assert!(mgr.sequences().is_empty());
        assert!(mgr.latest_full_snapshot().is_none());
        fs::remove_dir_all(&dir).ok();
    }
}
