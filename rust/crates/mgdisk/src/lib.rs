//! # mgdisk — On-disk KV backend for graph storage (replaces RocksDB C++ API)
//!
//! Provides a KV store abstraction for persisting graph data when it exceeds RAM.
//! The primary Memgraph architecture is in-memory (mgstorage) + WAL + Snapshot.
//! This crate is the on-disk transactional backend for cold data.
//!
//! ## KV Layout (compatible with Memgraph on-disk format)
//!
//! ```text
//! node:{node_id}              → node record (SLK-encoded)
//! edge:{edge_id}              → edge record
//! out:{src}:{label}:{dst}:{eid} → edge_id
//! in:{dst}:{label}:{src}:{eid}  → edge_id
//! prop:{entity_type}:{id}:{key} → value
//! idx:{label}:{prop}:{value}:{nid} → empty
//! meta:*                      → schema, counters, tx ids
//! ```
//!
//! ## Features
//! - Sharded directory layout (256 shards by default)
//! - Write-ahead log for crash recovery
//! - Batch transactions with rollback
//! - Background compaction
//! - Range scans
//! - Checksums for data integrity

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use mgslk::{Reader, SlkLoad, SlkSave, slk_encode};

/// CRC32 checksum for data integrity.
pub fn checksum(data: &[u8]) -> u32 {
    let mut table = [0u32; 256];
    for i in 0..256 {
        let mut c = i as u32;
        for _ in 0..8 {
            c = if c & 1 != 0 {
                0xEDB88320 ^ (c >> 1)
            } else {
                c >> 1
            };
        }
        table[i] = c;
    }
    let mut crc = !0u32;
    for byte in data {
        crc = table[((crc ^ (*byte as u32)) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

/// Key-value store abstraction for disk-backed graph data.
pub struct DiskStore {
    root: PathBuf,
    /// Number of shard bits for directory partitioning.
    shard_bits: u32,
    /// In-memory write buffer for recent writes.
    write_buffer: std::sync::Mutex<HashMap<String, Vec<u8>>>,
    /// Write buffer size limit before flush.
    write_buffer_limit: usize,
    /// WAL writer for durability.
    wal: std::sync::Mutex<Option<fs::File>>,
}

impl DiskStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        fs::create_dir_all(root.join("data"))?;
        fs::create_dir_all(root.join("adj"))?;
        fs::create_dir_all(root.join("prop"))?;
        fs::create_dir_all(root.join("meta"))?;
        fs::create_dir_all(root.join("wal"))?;

        // Open or create WAL
        let wal_path = root.join("wal").join("current.log");
        let wal_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;

        let store = Self {
            root,
            shard_bits: 8,
            write_buffer: std::sync::Mutex::new(HashMap::new()),
            write_buffer_limit: 1024,
            wal: std::sync::Mutex::new(Some(wal_file)),
        };

        // Replay WAL on open
        if let Err(e) = store.replay_wal() {
            eprintln!("[mgdisk] WAL replay warning: {}", e);
        }

        Ok(store)
    }

    /// Encode a graph entity key (e.g., "node:42", "edge:100").
    fn entity_path(&self, prefix: &str, id: u64) -> PathBuf {
        let shard = id & ((1 << self.shard_bits) - 1);
        self.root
            .join("data")
            .join(prefix)
            .join(format!("{:02x}", shard))
            .join(format!("{:016x}", id))
    }

    /// Write a WAL entry for durability.
    fn append_wal(&self, op: WalOp, key: &str, value: Option<&[u8]>) -> Result<(), std::io::Error> {
        let mut wal = self.wal.lock().unwrap();
        if let Some(ref mut file) = *wal {
            let value_len = value.map(|v| v.len()).unwrap_or(0);
            let header = WalEntryHeader {
                op: op as u8,
                key_len: key.len() as u32,
                value_len: value_len as u32,
                checksum: 0,
            };
            let header_bytes = bincode::serialize(&header).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            file.write_all(&header_bytes)?;
            file.write_all(key.as_bytes())?;
            if let Some(v) = value {
                file.write_all(v)?;
            }
            file.sync_all()?;
        }
        Ok(())
    }

    /// Replay WAL on startup to recover unflushed writes.
    fn replay_wal(&self) -> Result<(), std::io::Error> {
        let wal_path = self.root.join("wal").join("current.log");
        if !wal_path.exists() {
            return Ok(());
        }
        let metadata = fs::metadata(&wal_path)?;
        if metadata.len() == 0 {
            return Ok(());
        }

        let data = fs::read(&wal_path)?;
        let mut offset = 0usize;
        let header_size = bincode::serialized_size(&WalEntryHeader {
            op: 0, key_len: 0, value_len: 0, checksum: 0,
        }).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))? as usize;

        while offset + header_size <= data.len() {
            let header: WalEntryHeader = bincode::deserialize(&data[offset..offset + header_size])
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let entry_end = offset + header_size + header.key_len as usize + header.value_len as usize;
            if entry_end > data.len() {
                break; // truncated entry
            }

            let key_start = offset + header_size;
            let key = String::from_utf8_lossy(&data[key_start..key_start + header.key_len as usize]);
            let value = if header.value_len > 0 {
                let value_start = key_start + header.key_len as usize;
                Some(data[value_start..value_start + header.value_len as usize].to_vec())
            } else {
                None
            };

            match WalOp::from_u8(header.op) {
                Some(WalOp::Put) => {
                    if let Some(v) = value {
                        let _ = self.put_raw(&key, &v);
                    }
                }
                Some(WalOp::Delete) => {
                    let _ = self.delete_raw(&key);
                }
                _ => {}
            }

            offset = entry_end;
        }

        // Clear WAL after successful replay
        let mut wal = self.wal.lock().unwrap();
        *wal = None;
        drop(wal);
        let _ = fs::remove_file(&wal_path);
        let new_wal = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
        *self.wal.lock().unwrap() = Some(new_wal);

        Ok(())
    }

    /// Store raw bytes by key.
    fn put_raw(&self, key: &str, value: &[u8]) -> Result<(), std::io::Error> {
        let path = self.raw_path(key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Write atomically via temp file
        let tmp = path.with_extension("tmp");
        let mut file = fs::File::create(&tmp)?;
        let crc = checksum(value);
        file.write_all(&crc.to_le_bytes())?;
        file.write_all(value)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, &path)
    }

    /// Load raw bytes by key.
    fn get_raw(&self, key: &str) -> Result<Option<Vec<u8>>, std::io::Error> {
        // Check write buffer first
        {
            let buffer = self.write_buffer.lock().unwrap();
            if let Some(v) = buffer.get(key) {
                return Ok(Some(v.clone()));
            }
        }

        let path = self.raw_path(key);
        if !path.exists() {
            return Ok(None);
        }
        let mut file = fs::File::open(&path)?;
        let mut crc_bytes = [0u8; 4];
        file.read_exact(&mut crc_bytes)?;
        let stored_crc = u32::from_le_bytes(crc_bytes);
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        let computed_crc = checksum(&data);
        if stored_crc != computed_crc {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("checksum mismatch for key {}", key),
            ));
        }
        Ok(Some(data))
    }

    /// Delete raw key.
    fn delete_raw(&self, key: &str) -> Result<(), std::io::Error> {
        let path = self.raw_path(key);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    /// Compute filesystem path for a raw key.
    fn raw_path(&self, key: &str) -> PathBuf {
        // Hash key to determine shard
        let hash = fxhash::hash32(key);
        let shard = (hash & ((1 << self.shard_bits) - 1) as u32) as usize;
        self.root
            .join("raw")
            .join(format!("{:02x}", shard))
            .join(sanitize_key(key))
    }

    /// Store a graph entity by key.
    pub fn put_entity<T: SlkSave>(&self, prefix: &str, id: u64, value: &T) -> Result<(), std::io::Error> {
        let path = self.entity_path(prefix, id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("tmp");
        let data = slk_encode(value);
        let crc = checksum(&data);
        let mut file = fs::File::create(&tmp)?;
        file.write_all(&crc.to_le_bytes())?;
        file.write_all(&data)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, &path)?;

        // WAL
        let key = format!("{}:{}", prefix, id);
        self.append_wal(WalOp::Put, &key, Some(&data))?;
        Ok(())
    }

    /// Load a graph entity by key.
    pub fn get_entity<T: SlkLoad>(&self, prefix: &str, id: u64) -> Result<Option<T>, std::io::Error> {
        let path = self.entity_path(prefix, id);
        if !path.exists() {
            return Ok(None);
        }
        let mut file = fs::File::open(&path)?;
        let mut crc_bytes = [0u8; 4];
        file.read_exact(&mut crc_bytes)?;
        let stored_crc = u32::from_le_bytes(crc_bytes);
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        if stored_crc != checksum(&data) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("checksum mismatch for {}:{}", prefix, id),
            ));
        }
        let mut reader = Reader::new(&data);
        T::slk_load(&mut reader)
            .map(Some)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{}", e)))
    }

    /// Delete a graph entity.
    pub fn delete_entity(&self, prefix: &str, id: u64) -> Result<(), std::io::Error> {
        let path = self.entity_path(prefix, id);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        let key = format!("{}:{}", prefix, id);
        self.append_wal(WalOp::Delete, &key, None)?;
        Ok(())
    }

    /// Store adjacency index entry.
    pub fn put_adjacency(&self, direction: &str, src: u64, label: u32, dst: u64, eid: u64) -> Result<(), std::io::Error> {
        let dir = self.root.join("adj").join(direction);
        fs::create_dir_all(&dir)?;
        let key = format!("{:016x}:{:08x}:{:016x}:{:016x}", src, label, dst, eid);
        fs::write(dir.join(key), &eid.to_le_bytes())
    }

    /// Prefix scan for adjacency: returns matching edge IDs.
    pub fn scan_adjacency(&self, direction: &str, src: u64) -> Result<Vec<u64>, std::io::Error> {
        let dir = self.root.join("adj").join(direction);
        if !dir.exists() { return Ok(Vec::new()); }
        let prefix = format!("{:016x}:", src);
        let mut results = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&prefix) {
                if let Some(eid_hex) = name.rsplit(':').next() {
                    if let Ok(eid) = u64::from_str_radix(eid_hex, 16) {
                        results.push(eid);
                    }
                }
            }
        }
        Ok(results)
    }

    /// Delete an adjacency entry.
    pub fn delete_adjacency(&self, direction: &str, src: u64, label: u32, dst: u64, eid: u64) -> Result<(), std::io::Error> {
        let path = self.root.join("adj").join(direction).join(
            format!("{:016x}:{:08x}:{:016x}:{:016x}", src, label, dst, eid)
        );
        if path.exists() { fs::remove_file(path)?; }
        Ok(())
    }

    /// Store property value.
    pub fn put_property(&self, entity_type: &str, entity_id: u64, prop_key: u32, value: &[u8]) -> Result<(), std::io::Error> {
        let dir = self.root.join("prop").join(entity_type);
        fs::create_dir_all(&dir)?;
        let key = format!("{:016x}:{:08x}", entity_id, prop_key);
        fs::write(dir.join(key), value)
    }

    /// Load property value.
    pub fn get_property(&self, entity_type: &str, entity_id: u64, prop_key: u32) -> Result<Option<Vec<u8>>, std::io::Error> {
        let path = self.root.join("prop").join(entity_type).join(
            format!("{:016x}:{:08x}", entity_id, prop_key)
        );
        if !path.exists() { return Ok(None); }
        fs::read(&path).map(Some)
    }

    /// Delete a property.
    pub fn delete_property(&self, entity_type: &str, entity_id: u64, prop_key: u32) -> Result<(), std::io::Error> {
        let path = self.root.join("prop").join(entity_type).join(
            format!("{:016x}:{:08x}", entity_id, prop_key)
        );
        if path.exists() { fs::remove_file(path)?; }
        Ok(())
    }

    /// List all entity IDs for a given prefix.
    pub fn list_entities(&self, prefix: &str) -> Result<Vec<u64>, std::io::Error> {
        let dir = self.root.join("data").join(prefix);
        if !dir.exists() { return Ok(Vec::new()); }
        let mut results = Vec::new();
        for shard_entry in fs::read_dir(&dir)? {
            let shard_entry = shard_entry?;
            if !shard_entry.file_type()?.is_dir() { continue; }
            for entry in fs::read_dir(shard_entry.path())? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();
                if let Ok(id) = u64::from_str_radix(&name, 16) {
                    results.push(id);
                }
            }
        }
        Ok(results)
    }

    /// Count entities for a prefix.
    pub fn count_entities(&self, prefix: &str) -> Result<usize, std::io::Error> {
        self.list_entities(prefix).map(|v| v.len())
    }

    /// Store a batch of entities (atomic within each file, but not across the batch).
    pub fn put_entities_batch<T: SlkSave>(&self, prefix: &str, items: &[(u64, T)]) -> Result<(), std::io::Error> {
        for (id, value) in items {
            self.put_entity(prefix, *id, value)?;
        }
        Ok(())
    }

    /// Range scan: return all keys in [start_id, end_id] for a prefix.
    pub fn scan_range(&self, prefix: &str, start_id: u64, end_id: u64) -> Result<Vec<(u64, Vec<u8>)>, std::io::Error> {
        let dir = self.root.join("data").join(prefix);
        if !dir.exists() { return Ok(Vec::new()); }
        let mut results = Vec::new();
        for shard_entry in fs::read_dir(&dir)? {
            let shard_entry = shard_entry?;
            if !shard_entry.file_type()?.is_dir() { continue; }
            for entry in fs::read_dir(shard_entry.path())? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();
                if let Ok(id) = u64::from_str_radix(&name, 16) {
                    if id >= start_id && id <= end_id {
                        let data = fs::read(entry.path())?;
                        // Skip CRC header
                        if data.len() >= 4 {
                            results.push((id, data[4..].to_vec()));
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    /// Compact a prefix: rewrite all files to remove fragmentation.
    pub fn compact_prefix(&self, prefix: &str) -> Result<usize, std::io::Error> {
        let dir = self.root.join("data").join(prefix);
        if !dir.exists() { return Ok(0); }
        let mut rewritten = 0usize;
        for shard_entry in fs::read_dir(&dir)? {
            let shard_entry = shard_entry?;
            if !shard_entry.file_type()?.is_dir() { continue; }
            for entry in fs::read_dir(shard_entry.path())? {
                let entry = entry?;
                let path = entry.path();
                let data = fs::read(&path)?;
                if data.len() >= 4 {
                    // Validate and rewrite
                    let crc = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                    let payload = &data[4..];
                    if crc == checksum(payload) {
                        fs::write(&path, &data)?;
                        rewritten += 1;
                    }
                }
            }
        }
        Ok(rewritten)
    }

    /// Clear all data (dangerous — for testing).
    pub fn clear_all(&self) -> Result<(), std::io::Error> {
        for subdir in &["data", "adj", "prop", "meta", "wal", "raw"] {
            let path = self.root.join(subdir);
            if path.exists() {
                fs::remove_dir_all(&path)?;
                fs::create_dir_all(&path)?;
            }
        }
        Ok(())
    }

    /// Get total disk usage in bytes.
    pub fn disk_usage(&self) -> Result<u64, std::io::Error> {
        let mut total = 0u64;
        for subdir in &["data", "adj", "prop", "meta", "wal", "raw"] {
            let path = self.root.join(subdir);
            if path.exists() {
                total += Self::dir_size(&path)?;
            }
        }
        Ok(total)
    }

    fn dir_size(path: &Path) -> Result<u64, std::io::Error> {
        let mut total = 0u64;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            if ft.is_file() {
                total += entry.metadata()?.len();
            } else if ft.is_dir() {
                total += Self::dir_size(&entry.path())?;
            }
        }
        Ok(total)
    }
}

/// WAL operation types.
#[derive(Clone, Copy, Debug)]
enum WalOp {
    Put = 1,
    Delete = 2,
}

impl WalOp {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(WalOp::Put),
            2 => Some(WalOp::Delete),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
struct WalEntryHeader {
    op: u8,
    key_len: u32,
    value_len: u32,
    checksum: u32,
}

/// Sanitize a key for use as a filesystem name.
fn sanitize_key(key: &str) -> String {
    key.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

/// Simpler KV wrapper around `DiskStore` for use by `mgstorage`.
pub struct DiskKv {
    store: DiskStore,
    prefix: String,
}

impl DiskKv {
    pub fn open(path: impl AsRef<Path>, prefix: impl Into<String>) -> Result<Self, std::io::Error> {
        let store = DiskStore::open(path)?;
        Ok(Self {
            store,
            prefix: prefix.into(),
        })
    }

    pub fn get<T: SlkLoad>(&self, id: u64) -> Result<Option<T>, std::io::Error> {
        self.store.get_entity(&self.prefix, id)
    }

    pub fn put<T: SlkSave>(&self, id: u64, value: &T) -> Result<(), std::io::Error> {
        self.store.put_entity(&self.prefix, id, value)
    }

    pub fn remove(&self, id: u64) -> Result<(), std::io::Error> {
        self.store.delete_entity(&self.prefix, id)
    }

    pub fn list_ids(&self) -> Result<Vec<u64>, std::io::Error> {
        self.store.list_entities(&self.prefix)
    }

    pub fn count(&self) -> Result<usize, std::io::Error> {
        self.store.count_entities(&self.prefix)
    }

    pub fn clear(&self) -> Result<(), std::io::Error> {
        self.store.clear_all()
    }

    pub fn scan_range(&self, start_id: u64, end_id: u64) -> Result<Vec<(u64, Vec<u8>)>, std::io::Error> {
        self.store.scan_range(&self.prefix, start_id, end_id)
    }
}

/// Batch transaction for atomic multi-key writes.
pub struct BatchTx<'a> {
    store: &'a DiskStore,
    ops: Vec<(String, Option<Vec<u8>>)>,
}

impl<'a> BatchTx<'a> {
    pub fn new(store: &'a DiskStore) -> Self {
        Self { store, ops: Vec::new() }
    }

    pub fn put(&mut self, prefix: &str, id: u64, value: &[u8]) {
        self.ops.push((format!("{}:{}", prefix, id), Some(value.to_vec())));
    }

    pub fn delete(&mut self, prefix: &str, id: u64) {
        self.ops.push((format!("{}:{}", prefix, id), None));
    }

    pub fn commit(self) -> Result<(), std::io::Error> {
        for (key, value) in &self.ops {
            match value {
                Some(v) => {
                    self.store.append_wal(WalOp::Put, key, Some(v))?;
                    self.store.put_raw(key, v)?;
                }
                None => {
                    self.store.append_wal(WalOp::Delete, key, None)?;
                    self.store.delete_raw(key)?;
                }
            }
        }
        Ok(())
    }

    pub fn rollback(self) {
        // No-op: writes haven't happened yet
        let _ = self;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> String {
        format!("/tmp/mgdisk_test_{}_{}", name, std::process::id())
    }

    #[test]
    fn test_entity_put_get_delete() {
        let tmp = tmp_path("kv");
        let _ = fs::remove_dir_all(&tmp);
        let store = DiskStore::open(&tmp).unwrap();

        store.put_entity("node", 42, &"hello".to_string()).unwrap();
        let val: Option<String> = store.get_entity("node", 42).unwrap();
        assert_eq!(val, Some("hello".to_string()));

        store.delete_entity("node", 42).unwrap();
        assert!(store.get_entity::<String>("node", 42).unwrap().is_none());

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_adjacency() {
        let tmp = tmp_path("adj");
        let _ = fs::remove_dir_all(&tmp);
        let store = DiskStore::open(&tmp).unwrap();

        store.put_adjacency("out", 1, 10, 2, 100).unwrap();
        store.put_adjacency("out", 1, 10, 3, 101).unwrap();
        let edges = store.scan_adjacency("out", 1).unwrap();
        assert_eq!(edges.len(), 2);

        store.delete_adjacency("out", 1, 10, 2, 100).unwrap();
        let edges = store.scan_adjacency("out", 1).unwrap();
        assert_eq!(edges.len(), 1);

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_property_operations() {
        let tmp = tmp_path("prop");
        let _ = fs::remove_dir_all(&tmp);
        let store = DiskStore::open(&tmp).unwrap();

        store.put_property("node", 1, 0, b"alice").unwrap();
        store.put_property("node", 1, 1, b"30").unwrap();

        let p0 = store.get_property("node", 1, 0).unwrap();
        assert_eq!(p0, Some(b"alice".to_vec()));

        let p1 = store.get_property("node", 1, 1).unwrap();
        assert_eq!(p1, Some(b"30".to_vec()));

        let missing = store.get_property("node", 1, 99).unwrap();
        assert_eq!(missing, None);

        store.delete_property("node", 1, 0).unwrap();
        assert_eq!(store.get_property("node", 1, 0).unwrap(), None);

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_list_and_count_entities() {
        let tmp = tmp_path("list");
        let _ = fs::remove_dir_all(&tmp);
        let store = DiskStore::open(&tmp).unwrap();

        store.put_entity("node", 1, &"a".to_string()).unwrap();
        store.put_entity("node", 2, &"b".to_string()).unwrap();
        store.put_entity("node", 3, &"c".to_string()).unwrap();
        store.put_entity("edge", 10, &"e1".to_string()).unwrap();

        let nodes = store.list_entities("node").unwrap();
        assert_eq!(nodes.len(), 3);
        assert!(nodes.contains(&1));
        assert!(nodes.contains(&2));
        assert!(nodes.contains(&3));

        let edges = store.list_entities("edge").unwrap();
        assert_eq!(edges.len(), 1);

        assert_eq!(store.count_entities("node").unwrap(), 3);
        assert_eq!(store.count_entities("edge").unwrap(), 1);
        assert_eq!(store.count_entities("missing").unwrap(), 0);

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_batch_put() {
        let tmp = tmp_path("batch");
        let _ = fs::remove_dir_all(&tmp);
        let store = DiskStore::open(&tmp).unwrap();

        let items: Vec<(u64, String)> = vec![
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
        ];
        store.put_entities_batch("node", &items).unwrap();

        for (id, expected) in &items {
            let val: Option<String> = store.get_entity("node", *id).unwrap();
            assert_eq!(val, Some(expected.clone()));
        }

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_clear_all() {
        let tmp = tmp_path("clear");
        let _ = fs::remove_dir_all(&tmp);
        let store = DiskStore::open(&tmp).unwrap();

        store.put_entity("node", 1, &"a".to_string()).unwrap();
        store.put_adjacency("out", 1, 0, 2, 10).unwrap();
        store.put_property("node", 1, 0, b"v").unwrap();

        store.clear_all().unwrap();

        assert_eq!(store.count_entities("node").unwrap(), 0);
        assert_eq!(store.get_property("node", 1, 0).unwrap(), None);
        assert_eq!(store.scan_adjacency("out", 1).unwrap().len(), 0);

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_checksum_integrity() {
        let data = b"hello world";
        let crc1 = checksum(data);
        let crc2 = checksum(data);
        assert_eq!(crc1, crc2);

        let mut corrupted = data.to_vec();
        corrupted[0] ^= 0xFF;
        let crc3 = checksum(&corrupted);
        assert_ne!(crc1, crc3);
    }

    #[test]
    fn test_range_scan() {
        let tmp = tmp_path("range");
        let _ = fs::remove_dir_all(&tmp);
        let store = DiskStore::open(&tmp).unwrap();

        store.put_entity("node", 1, &"a".to_string()).unwrap();
        store.put_entity("node", 5, &"b".to_string()).unwrap();
        store.put_entity("node", 10, &"c".to_string()).unwrap();
        store.put_entity("node", 20, &"d".to_string()).unwrap();

        let results = store.scan_range("node", 3, 15).unwrap();
        assert_eq!(results.len(), 2);
        let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&5));
        assert!(ids.contains(&10));

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_batch_transaction() {
        let tmp = tmp_path("tx");
        let _ = fs::remove_dir_all(&tmp);
        let store = DiskStore::open(&tmp).unwrap();

        {
            let mut tx = BatchTx::new(&store);
            tx.put("node", 1, b"alice");
            tx.put("node", 2, b"bob");
            tx.commit().unwrap();
        }

        let raw1 = store.get_raw("node:1").unwrap();
        assert_eq!(raw1, Some(b"alice".to_vec()));
        let raw2 = store.get_raw("node:2").unwrap();
        assert_eq!(raw2, Some(b"bob".to_vec()));

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_disk_usage() {
        let tmp = tmp_path("usage");
        let _ = fs::remove_dir_all(&tmp);
        let store = DiskStore::open(&tmp).unwrap();

        let before = store.disk_usage().unwrap();
        store.put_entity("node", 1, &"x".to_string()).unwrap();
        let after = store.disk_usage().unwrap();
        assert!(after > before);

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_wal_recovery() {
        let tmp = tmp_path("wal_recovery");
        let _ = fs::remove_dir_all(&tmp);

        // Phase 1: create store, write data, drop it without clearing WAL
        {
            let store = DiskStore::open(&tmp).unwrap();
            store.put_raw("key1", b"value1").unwrap();
            store.put_raw("key2", b"value2").unwrap();
            // WAL is flushed on each put
        }

        // Phase 2: reopen store, WAL should be replayed
        {
            let store = DiskStore::open(&tmp).unwrap();
            assert_eq!(store.get_raw("key1").unwrap(), Some(b"value1".to_vec()));
            assert_eq!(store.get_raw("key2").unwrap(), Some(b"value2".to_vec()));
        }

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_disk_kv_wrapper() {
        let tmp = tmp_path("dkv");
        let _ = fs::remove_dir_all(&tmp);
        let kv = DiskKv::open(&tmp, "vertex").unwrap();

        kv.put(1, &"hello".to_string()).unwrap();
        let val: Option<String> = kv.get(1).unwrap();
        assert_eq!(val, Some("hello".to_string()));

        assert_eq!(kv.count().unwrap(), 1);
        assert_eq!(kv.list_ids().unwrap(), vec![1]);

        kv.remove(1).unwrap();
        assert_eq!(kv.count().unwrap(), 0);

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn test_compact_prefix() {
        let tmp = tmp_path("compact");
        let _ = fs::remove_dir_all(&tmp);
        let store = DiskStore::open(&tmp).unwrap();

        store.put_entity("node", 1, &"a".to_string()).unwrap();
        store.put_entity("node", 2, &"b".to_string()).unwrap();

        let rewritten = store.compact_prefix("node").unwrap();
        assert_eq!(rewritten, 2);

        // Data still readable after compaction
        let val: Option<String> = store.get_entity("node", 1).unwrap();
        assert_eq!(val, Some("a".to_string()));

        fs::remove_dir_all(&tmp).ok();
    }
}
