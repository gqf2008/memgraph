//! Recovery: replay WAL records onto a Storage engine, and dump Storage to snapshot.

use std::path::Path;

use mgcore::delta::IsolationLevel;
use mgstorage::storage::{Storage, StorageError};
use crate::snapshot::NameMapperSnapshot;

use crate::delta_record::DeltaRecord;
use crate::snapshot::{EdgeSnapshotEntry, SnapshotData, SnapshotReader, VertexSnapshotEntry};
use crate::wal::WalReader;

/// Recovery orchestrator that can load snapshots and WALs from either
/// current Rust format or legacy C++ format (v14-v34).
pub struct Recovery;

impl Recovery {
    /// Load a snapshot and WAL files from the given paths, automatically
    /// detecting format and dispatching to the correct reader.
    ///
    /// # Errors
    /// Returns a descriptive string on I/O failure, corrupt data, or
    /// unsupported format version.
    pub fn load_from_path(
        storage: &Storage,
        catalog: Option<&mgcatalog::Catalog>,
        snapshot_path: impl AsRef<Path>,
        wal_paths: &[impl AsRef<Path>],
    ) -> Result<(), String> {
        let snap_data = std::fs::read(snapshot_path.as_ref())
            .map_err(|e| format!("snapshot read error: {}", e))?;

        let snap = match crate::version::detect_format(&snap_data) {
            Ok(crate::version::FormatKind::LegacyCpp(v)) if (14..=34).contains(&v) => {
                crate::legacy::LegacySnapshotReader::read(&snap_data, v)
                    .map_err(|e| format!("legacy snapshot parse error: {}", e))?
            }
            Ok(crate::version::FormatKind::Current) => {
                SnapshotReader::read(snapshot_path.as_ref())
                    .map_err(|e| format!("snapshot read error: {}", e))?
            }
            Ok(crate::version::FormatKind::LegacyCpp(v)) => {
                return Err(format!("unsupported legacy snapshot version: {}", v));
            }
            Ok(crate::version::FormatKind::Unknown(v)) => {
                return Err(format!("unknown snapshot version: {}", v));
            }
            Err(e) => return Err(format!("format detection error: {}", e)),
        };

        Self::restore_snapshot(storage, catalog, &snap)?;

        for wal_path in wal_paths {
            let wal_data = match std::fs::read(wal_path.as_ref()) {
                Ok(d) => d,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(format!("WAL read error: {}", e)),
            };

            let records = match crate::version::detect_format(&wal_data) {
                Ok(crate::version::FormatKind::LegacyCpp(v)) if (14..=34).contains(&v) => {
                    crate::legacy::LegacyWalReader::read(&wal_data, v)
                        .map_err(|e| format!("legacy WAL parse error: {}", e))?
                }
                Ok(crate::version::FormatKind::Current) => {
                    let reader = WalReader::open(wal_path.as_ref())
                        .map_err(|e| format!("WAL open error: {}", e))?;
                    reader.records().to_vec()
                }
                Ok(crate::version::FormatKind::LegacyCpp(v)) => {
                    return Err(format!("unsupported legacy WAL version: {}", v));
                }
                Ok(crate::version::FormatKind::Unknown(v)) => {
                    return Err(format!("unknown WAL version: {}", v));
                }
                Err(e) => return Err(format!("WAL format detection error: {}", e)),
            };

            for record in &records {
                if let Err(e) = replay_record(storage, record) {
                    let msg = format!("{}", e);
                    if msg.contains("already exists") || msg.contains("not found") {
                        continue;
                    }
                    return Err(format!("replay error: {}", msg));
                }
            }
        }

        Ok(())
    }

    /// Recover from a legacy C++ snapshot file (v14-v34).
    pub fn recover_from_legacy_snapshot(
        storage: &Storage,
        catalog: Option<&mgcatalog::Catalog>,
        path: impl AsRef<Path>,
        version: u64,
    ) -> Result<(), String> {
        let snap = crate::legacy::read_legacy_snapshot(path, version)
            .map_err(|e| format!("legacy snapshot read error: {}", e))?;
        Self::restore_snapshot(storage, catalog, &snap)
    }

    /// Recover from a legacy C++ WAL file (v14-v34).
    pub fn recover_from_legacy_wal(
        storage: &Storage,
        path: impl AsRef<Path>,
        version: u64,
    ) -> Result<(), String> {
        let records = crate::legacy::read_legacy_wal(path, version)
            .map_err(|e| format!("legacy WAL read error: {}", e))?;
        for record in &records {
            if let Err(e) = replay_record(storage, record) {
                let msg = format!("{}", e);
                if msg.contains("already exists") || msg.contains("not found") {
                    continue;
                }
                return Err(format!("replay error: {}", msg));
            }
        }
        Ok(())
    }

    fn restore_snapshot(
        storage: &Storage,
        catalog: Option<&mgcatalog::Catalog>,
        snap: &SnapshotData,
    ) -> Result<(), String> {
        if let Some(cat) = catalog {
            cat.load_mappings(&snap.name_mapper.labels, &snap.name_mapper.properties, &snap.name_mapper.edge_types);
        }

        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        for v in &snap.vertices {
            storage
                .create_vertex(&tx, v.gid)
                .map_err(|e| format!("recovery vertex create: {}", e))?;
            for label in &v.labels {
                storage
                    .vertex_add_label(&tx, v.gid, *label)
                    .map_err(|e| format!("recovery label add: {}", e))?;
            }
            for (key, value) in &v.properties {
                storage
                    .vertex_set_property(&tx, v.gid, *key, value.clone())
                    .map_err(|e| format!("recovery property set: {}", e))?;
            }
        }
        for e in &snap.edges {
            storage
                .create_edge(&tx, e.gid, e.from_vertex, e.to_vertex, e.edge_type)
                .map_err(|e| format!("recovery edge create: {}", e))?;
            for (key, value) in &e.properties {
                storage
                    .edge_set_property(&tx, e.gid, *key, value.clone())
                    .map_err(|e| format!("recovery edge property set: {}", e))?;
            }
        }
        storage.commit_transaction(&tx);
        Ok(())
    }
}

/// Dump the current committed state of a Storage into a SnapshotData.
///
/// Uses a fresh read-only transaction so that uncommitted changes from
/// active transactions are not included in the snapshot.
pub fn dump_snapshot(storage: &Storage, catalog: Option<&mgcatalog::Catalog>) -> SnapshotData {
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

    let vertices: Vec<VertexSnapshotEntry> = storage
        .all_vertices_in_tx(&tx)
        .into_iter()
        .map(|snap| VertexSnapshotEntry {
            gid: snap.gid,
            labels: snap.labels,
            properties: snap.properties.iter().map(|(k, v)| (k, v.clone())).collect(),
        })
        .collect();

    let edges: Vec<EdgeSnapshotEntry> = storage
        .all_edges_in_tx(&tx)
        .into_iter()
        .map(|snap| EdgeSnapshotEntry {
            gid: snap.gid,
            from_vertex: snap.from_vertex,
            to_vertex: snap.to_vertex,
            edge_type: snap.edge_type,
            properties: snap.properties.iter().map(|(k, v)| (k, v.clone())).collect(),
        })
        .collect();

    let name_mapper = if let Some(cat) = catalog {
        let (labels, properties, edge_types) = cat.dump_mappings();
        NameMapperSnapshot { labels, properties, edge_types }
    } else {
        NameMapperSnapshot { labels: vec![], properties: vec![], edge_types: vec![] }
    };

    SnapshotData { name_mapper, vertices, edges }
}

/// Replay a single delta record onto the storage engine.
/// Uses short-lived transactions; each successful record is committed.
pub fn replay_record(storage: &Storage, record: &DeltaRecord) -> Result<(), StorageError> {
    let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
    let result = match record {
        DeltaRecord::VertexCreate { gid, .. } => storage.create_vertex(&tx, *gid).map(|_| ()),
        DeltaRecord::VertexAddLabel { gid, label } => {
            storage.vertex_add_label(&tx, *gid, *label)
        }
        DeltaRecord::VertexRemoveLabel { gid, label } => {
            storage.vertex_remove_label(&tx, *gid, *label)
        }
        DeltaRecord::VertexSetProperty { gid, key, value } => {
            storage.vertex_set_property(&tx, *gid, *key, value.clone())
        }
        DeltaRecord::EdgeSetProperty { gid, key, value } => {
            storage.edge_set_property(&tx, *gid, *key, value.clone())
        }
        DeltaRecord::EdgeCreate {
            gid,
            from_vertex,
            to_vertex,
            edge_type,
            ..
        } => storage
            .create_edge(&tx, *gid, *from_vertex, *to_vertex, *edge_type)
            .map(|_| ()),
        DeltaRecord::EdgeDelete { gid } => storage.delete_edge(&tx, *gid),
        DeltaRecord::VertexDelete { gid } => storage.delete_vertex(&tx, *gid),
        DeltaRecord::TransactionStart { .. } | DeltaRecord::TransactionEnd { .. } => Ok(()),
        // Index, constraint, and metadata deltas — no storage mutation needed during WAL replay
        _ => Ok(()),
    };
    if result.is_ok() {
        storage.commit_transaction(&tx);
    } else {
        storage.abort_transaction(&tx);
    }
    result
}

/// Recover storage state from a snapshot file and WAL files.
/// Loads the snapshot, restores catalog mappings, then replays WAL records.
///
/// This is the free-function convenience API. For format-auto-detecting
/// recovery that also handles legacy C++ files, use [`Recovery::load_from_path`].
pub fn recover(storage: &Storage, catalog: Option<&mgcatalog::Catalog>, snapshot_path: &str, wal_paths: &[&str]) -> Result<(), String> {
    Recovery::load_from_path(storage, catalog, snapshot_path, wal_paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::SnapshotWriter;
    use mgcore::property_value::PropertyValue;
    use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
    use std::fs;

    #[test]
    fn test_full_persistence_roundtrip() {
        let tmp_snap = "/tmp/mg_roundtrip.snap";
        let tmp_wal = "/tmp/mg_roundtrip.wal";
        let _ = fs::remove_file(tmp_snap);
        let _ = fs::remove_file(tmp_wal);

        // ── Create data ──────────────────────────────────────────
        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);

        storage.create_vertex(&tx, Gid::from(1u64)).unwrap();
        storage
            .vertex_add_label(&tx, Gid::from(1u64), LabelId::from(10u32))
            .unwrap();
        storage
            .vertex_set_property(
                &tx,
                Gid::from(1u64),
                PropertyId::from(0u32),
                PropertyValue::String("alice".into()),
            )
            .unwrap();

        storage.create_vertex(&tx, Gid::from(2u64)).unwrap();
        storage
            .vertex_set_property(
                &tx,
                Gid::from(2u64),
                PropertyId::from(0u32),
                PropertyValue::Int(42),
            )
            .unwrap();

        storage
            .create_edge(
                &tx,
                Gid::from(100u64),
                Gid::from(1u64),
                Gid::from(2u64),
                EdgeTypeId::from(5u32),
            )
            .unwrap();

        storage.commit_transaction(&tx);

        // ── Persist to Snapshot ───────────────────────────────────
        let snap = dump_snapshot(&storage, None::<&mgcatalog::Catalog>);
        SnapshotWriter::write(tmp_snap, &snap).unwrap();

        // Also write WAL (empty in this test since we only snapshot)
        {
            let mut wal_writer = crate::wal::WalWriter::create(tmp_wal).unwrap();
            wal_writer.sync().unwrap();
        }

        // ── Recover into new Storage ─────────────────────────────
        let recovered = Storage::new();
        recover(&recovered, None, tmp_snap, &[tmp_wal]).unwrap();

        // ── Verify ───────────────────────────────────────────────
        let verify_tx = recovered.begin_transaction(IsolationLevel::SnapshotIsolation);

        let v1 = recovered.get_vertex(Gid::from(1u64), &verify_tx).unwrap();
        assert_eq!(v1.gid, Gid::from(1u64));
        assert_eq!(
            *v1.properties.get(PropertyId::from(0u32)),
            PropertyValue::String("alice".into())
        );

        let v2 = recovered.get_vertex(Gid::from(2u64), &verify_tx).unwrap();
        assert_eq!(
            *v2.properties.get(PropertyId::from(0u32)),
            PropertyValue::Int(42)
        );

        let edge = recovered
            .get_edge(Gid::from(100u64), &verify_tx)
            .unwrap();
        assert_eq!(edge.from_vertex, Gid::from(1u64));
        assert_eq!(edge.to_vertex, Gid::from(2u64));

        // Cleanup
        fs::remove_file(tmp_snap).ok();
        fs::remove_file(tmp_wal).ok();
    }

    #[test]
    fn test_recovery_load_from_path_current_format() {
        let tmp_snap = "/tmp/mg_recovery_load_current.snap";
        let tmp_wal = "/tmp/mg_recovery_load_current.wal";
        let _ = fs::remove_file(tmp_snap);
        let _ = fs::remove_file(tmp_wal);

        let storage = Storage::new();
        let tx = storage.begin_transaction(IsolationLevel::SnapshotIsolation);
        storage.create_vertex(&tx, Gid::from(7u64)).unwrap();
        storage.vertex_add_label(&tx, Gid::from(7u64), LabelId::from(3u32)).unwrap();
        storage.commit_transaction(&tx);

        let snap = dump_snapshot(&storage, None::<&mgcatalog::Catalog>);
        SnapshotWriter::write(tmp_snap, &snap).unwrap();

        {
            let mut wal_writer = crate::wal::WalWriter::create(tmp_wal).unwrap();
            wal_writer.sync().unwrap();
        }

        let recovered = Storage::new();
        Recovery::load_from_path(&recovered, None, tmp_snap, &[tmp_wal]).unwrap();

        let verify_tx = recovered.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v = recovered.get_vertex(Gid::from(7u64), &verify_tx).unwrap();
        assert_eq!(v.gid, Gid::from(7u64));

        fs::remove_file(tmp_snap).ok();
        fs::remove_file(tmp_wal).ok();
    }

    #[test]
    fn test_recovery_from_legacy_snapshot() {
        let tmp_snap = "/tmp/mg_recovery_legacy_snap.snap";
        let _ = fs::remove_file(tmp_snap);

        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&20u64.to_le_bytes());
        // SECTION_VERTEX
        buf.push(crate::delta_record::SECTION_VERTEX);
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&99u64.to_le_bytes()); // gid
        buf.extend_from_slice(&0u64.to_le_bytes()); // label_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // prop_count
        // SECTION_EDGE
        buf.push(crate::delta_record::SECTION_EDGE);
        buf.extend_from_slice(&0u64.to_le_bytes());
        fs::write(tmp_snap, &buf).unwrap();

        let recovered = Storage::new();
        Recovery::recover_from_legacy_snapshot(&recovered, None, tmp_snap, 20).unwrap();

        let verify_tx = recovered.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v = recovered.get_vertex(Gid::from(99u64), &verify_tx).unwrap();
        assert_eq!(v.gid, Gid::from(99u64));

        fs::remove_file(tmp_snap).ok();
    }

    #[test]
    fn test_recovery_from_legacy_wal() {
        let tmp_wal = "/tmp/mg_recovery_legacy_wal.wal";
        let _ = fs::remove_file(tmp_wal);

        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGwl");
        buf.extend_from_slice(&20u64.to_le_bytes());
        buf.push(crate::delta_record::DELTA_VERTEX_CREATE);
        buf.extend_from_slice(&77u64.to_le_bytes()); // gid
        buf.extend_from_slice(&100u64.to_le_bytes()); // timestamp
        fs::write(tmp_wal, &buf).unwrap();

        let recovered = Storage::new();
        Recovery::recover_from_legacy_wal(&recovered, tmp_wal, 20).unwrap();

        let verify_tx = recovered.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v = recovered.get_vertex(Gid::from(77u64), &verify_tx).unwrap();
        assert_eq!(v.gid, Gid::from(77u64));

        fs::remove_file(tmp_wal).ok();
    }

    #[test]
    fn test_recovery_load_from_path_mixed_legacy_and_current() {
        let tmp_legacy_snap = "/tmp/mg_recovery_mixed_legacy.snap";
        let tmp_current_wal = "/tmp/mg_recovery_mixed_current.wal";
        let _ = fs::remove_file(tmp_legacy_snap);
        let _ = fs::remove_file(tmp_current_wal);

        // Build legacy snapshot with one vertex
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&14u64.to_le_bytes());
        buf.push(crate::delta_record::SECTION_VERTEX);
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&55u64.to_le_bytes()); // gid
        buf.extend_from_slice(&0u64.to_le_bytes()); // label_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // prop_count
        buf.push(crate::delta_record::SECTION_EDGE);
        buf.extend_from_slice(&0u64.to_le_bytes());
        fs::write(tmp_legacy_snap, &buf).unwrap();

        // Build current-format WAL (empty)
        {
            let mut wal_writer = crate::wal::WalWriter::create(tmp_current_wal).unwrap();
            wal_writer.sync().unwrap();
        }

        let recovered = Storage::new();
        Recovery::load_from_path(&recovered, None, tmp_legacy_snap, &[tmp_current_wal]).unwrap();

        let verify_tx = recovered.begin_transaction(IsolationLevel::SnapshotIsolation);
        let v = recovered.get_vertex(Gid::from(55u64), &verify_tx).unwrap();
        assert_eq!(v.gid, Gid::from(55u64));

        fs::remove_file(tmp_legacy_snap).ok();
        fs::remove_file(tmp_current_wal).ok();
    }

    #[test]
    fn test_recovery_invalid_version_error() {
        let tmp_snap = "/tmp/mg_recovery_bad_ver.snap";
        let _ = fs::remove_file(tmp_snap);

        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&999u64.to_le_bytes());
        fs::write(tmp_snap, &buf).unwrap();

        let recovered = Storage::new();
        let err = Recovery::load_from_path(&recovered, None, tmp_snap, &[] as &[&str]).unwrap_err();
        assert!(err.contains("unknown snapshot version") || err.contains("unsupported"));

        fs::remove_file(tmp_snap).ok();
    }
}
