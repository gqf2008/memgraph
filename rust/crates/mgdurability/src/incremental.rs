//! Incremental snapshot support.
//!
//! An incremental snapshot only stores vertices and edges that have changed
//! since a base snapshot (identified by a sequence number).  This reduces I/O
//! for large databases with small working-set churn.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use mgslk::{Builder, Reader, SlkLoad, SlkSave, DURABILITY_VERSION, SNAPSHOT_MAGIC};

use crate::snapshot::{EdgeSnapshotEntry, NameMapperSnapshot, SnapshotError, VertexSnapshotEntry};

/// Header for an incremental snapshot file.
///
/// Layout: `[MGsn magic: 4B][version: u64 LE][base_seq: u64 LE][sections...]`
#[derive(Clone, Debug, PartialEq)]
pub struct IncrementalSnapshotHeader {
    pub base_sequence: u64,
    pub name_mapper: Option<NameMapperSnapshot>,
}

impl SlkSave for IncrementalSnapshotHeader {
    fn slk_save(&self, builder: &mut Builder) {
        self.base_sequence.slk_save(builder);
        self.name_mapper.is_some().slk_save(builder);
        if let Some(ref nm) = self.name_mapper {
            nm.slk_save(builder);
        }
    }
}

impl SlkLoad for IncrementalSnapshotHeader {
    fn slk_load(reader: &mut Reader) -> Result<Self, mgslk::SlkDecodeError> {
        let base_sequence = u64::slk_load(reader)?;
        let has_mapper = bool::slk_load(reader)?;
        let name_mapper = if has_mapper {
            Some(NameMapperSnapshot::slk_load(reader)?)
        } else {
            None
        };
        Ok(Self {
            base_sequence,
            name_mapper,
        })
    }
}

/// Data contained in an incremental snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct IncrementalSnapshotData {
    pub header: IncrementalSnapshotHeader,
    pub new_vertices: Vec<VertexSnapshotEntry>,
    pub modified_vertices: Vec<VertexSnapshotEntry>,
    pub deleted_vertices: Vec<u64>, // Gid as raw u64 for simplicity
    pub new_edges: Vec<EdgeSnapshotEntry>,
    pub modified_edges: Vec<EdgeSnapshotEntry>,
    pub deleted_edges: Vec<u64>, // Gid as raw u64
}

/// Writer for incremental snapshots.
pub struct IncrementalSnapshotWriter;

impl IncrementalSnapshotWriter {
    /// Write an incremental snapshot to the given path.
    pub fn write(
        path: impl AsRef<Path>,
        data: &IncrementalSnapshotData,
    ) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;

        // Magic + version
        file.write_all(SNAPSHOT_MAGIC)?;
        file.write_all(&DURABILITY_VERSION.to_le_bytes())?;

        // Header section
        Self::write_section(&mut file, std::slice::from_ref(&data.header), b"HEAD")?;

        // New vertices
        Self::write_section(&mut file, &data.new_vertices, b"NVER")?;
        // Modified vertices
        Self::write_section(&mut file, &data.modified_vertices, b"MVER")?;
        // Deleted vertices (stored as Vec<u64>)
        Self::write_raw_section(&mut file, &data.deleted_vertices, b"DVER")?;
        // New edges
        Self::write_section(&mut file, &data.new_edges, b"NEDG")?;
        // Modified edges
        Self::write_section(&mut file, &data.modified_edges, b"MEDG")?;
        // Deleted edges
        Self::write_raw_section(&mut file, &data.deleted_edges, b"DEDG")?;

        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    fn write_section<T: SlkSave + Clone>(
        file: &mut File,
        items: &[T],
        _tag: &[u8; 4],
    ) -> Result<(), std::io::Error> {
        let (mut builder, collector) = Builder::new_collecting();
        items.to_vec().slk_save(&mut builder);
        builder.finalize();
        let framed = collector.into_vec();
        file.write_all(&framed)?;
        Ok(())
    }

    fn write_raw_section(
        file: &mut File,
        items: &[u64],
        _tag: &[u8; 4],
    ) -> Result<(), std::io::Error> {
        let (mut builder, collector) = Builder::new_collecting();
        items.to_vec().slk_save(&mut builder);
        builder.finalize();
        let framed = collector.into_vec();
        file.write_all(&framed)?;
        Ok(())
    }
}

/// Reader for incremental snapshots.
pub struct IncrementalSnapshotReader;

impl IncrementalSnapshotReader {
    /// Read an incremental snapshot from the given path.
    pub fn read(path: impl AsRef<Path>) -> Result<IncrementalSnapshotData, SnapshotError> {
        let mut file = File::open(path.as_ref()).map_err(SnapshotError::Io)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(SnapshotError::Io)?;

        if data.len() < 12 {
            return Err(SnapshotError::Corrupt(
                "incremental snapshot too short".into(),
            ));
        }

        if &data[0..4] != SNAPSHOT_MAGIC {
            return Err(SnapshotError::Corrupt("invalid snapshot magic".into()));
        }

        let version = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);

        if version > DURABILITY_VERSION {
            return Err(SnapshotError::UnsupportedVersion(version));
        }

        let sections = Self::parse_sections(&data[12..])?;
        if sections.len() < 7 {
            return Err(SnapshotError::Corrupt(
                "incremental snapshot missing sections".into(),
            ));
        }

        let header: Vec<IncrementalSnapshotHeader> = Self::decode_section(&sections[0])?;
        let header = header
            .into_iter()
            .next()
            .ok_or_else(|| SnapshotError::Corrupt("missing incremental snapshot header".into()))?;

        let new_vertices: Vec<VertexSnapshotEntry> = Self::decode_section(&sections[1])?;
        let modified_vertices: Vec<VertexSnapshotEntry> = Self::decode_section(&sections[2])?;
        let deleted_vertices: Vec<u64> = Self::decode_section(&sections[3])?;
        let new_edges: Vec<EdgeSnapshotEntry> = Self::decode_section(&sections[4])?;
        let modified_edges: Vec<EdgeSnapshotEntry> = Self::decode_section(&sections[5])?;
        let deleted_edges: Vec<u64> = Self::decode_section(&sections[6])?;

        Ok(IncrementalSnapshotData {
            header,
            new_vertices,
            modified_vertices,
            deleted_vertices,
            new_edges,
            modified_edges,
            deleted_edges,
        })
    }

    fn parse_sections(mut data: &[u8]) -> Result<Vec<Vec<u8>>, SnapshotError> {
        let mut sections = Vec::new();
        while !data.is_empty() {
            if data.len() < 4 {
                break;
            }
            let seg_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            if seg_size == 0 {
                data = &data[4..];
                continue;
            }
            let section_end = 4 + seg_size + 4;
            if data.len() < section_end {
                break;
            }
            sections.push(data[..section_end].to_vec());
            data = &data[section_end..];
        }
        Ok(sections)
    }

    fn decode_section<T: SlkLoad>(section_data: &[u8]) -> Result<Vec<T>, SnapshotError> {
        let mut reader = Reader::new(section_data);
        Vec::<T>::slk_load(&mut reader)
            .map_err(|e| SnapshotError::Corrupt(format!("failed to decode section: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::property_value::PropertyValue;
    use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
    use std::fs;

    #[test]
    fn test_incremental_snapshot_roundtrip() {
        let tmp = "/tmp/mg_incr_snap_test.snap";
        let _ = fs::remove_file(tmp);

        let data = IncrementalSnapshotData {
            header: IncrementalSnapshotHeader {
                base_sequence: 42,
                name_mapper: Some(NameMapperSnapshot {
                    labels: vec![("L".into(), LabelId::from(1u32))],
                    properties: vec![],
                    edge_types: vec![],
                }),
            },
            new_vertices: vec![VertexSnapshotEntry {
                gid: Gid::from(10u64),
                labels: vec![LabelId::from(1u32)],
                properties: vec![(PropertyId::from(0u32), PropertyValue::Int(99))],
            }],
            modified_vertices: vec![VertexSnapshotEntry {
                gid: Gid::from(11u64),
                labels: vec![],
                properties: vec![(PropertyId::from(1u32), PropertyValue::String("upd".into()))],
            }],
            deleted_vertices: vec![1, 2, 3],
            new_edges: vec![EdgeSnapshotEntry {
                gid: Gid::from(100u64),
                from_vertex: Gid::from(10u64),
                to_vertex: Gid::from(11u64),
                edge_type: EdgeTypeId::from(5u32),
                properties: vec![],
            }],
            modified_edges: vec![],
            deleted_edges: vec![50],
        };

        IncrementalSnapshotWriter::write(tmp, &data).unwrap();
        let recovered = IncrementalSnapshotReader::read(tmp).unwrap();

        assert_eq!(recovered.header.base_sequence, 42);
        assert!(recovered.header.name_mapper.is_some());
        assert_eq!(recovered.new_vertices.len(), 1);
        assert_eq!(recovered.new_vertices[0].gid, Gid::from(10u64));
        assert_eq!(recovered.modified_vertices.len(), 1);
        assert_eq!(recovered.deleted_vertices, vec![1, 2, 3]);
        assert_eq!(recovered.new_edges.len(), 1);
        assert_eq!(recovered.new_edges[0].gid, Gid::from(100u64));
        assert!(recovered.modified_edges.is_empty());
        assert_eq!(recovered.deleted_edges, vec![50]);

        fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_incremental_snapshot_empty() {
        let tmp = "/tmp/mg_incr_snap_empty.snap";
        let _ = fs::remove_file(tmp);

        let data = IncrementalSnapshotData {
            header: IncrementalSnapshotHeader {
                base_sequence: 0,
                name_mapper: None,
            },
            new_vertices: vec![],
            modified_vertices: vec![],
            deleted_vertices: vec![],
            new_edges: vec![],
            modified_edges: vec![],
            deleted_edges: vec![],
        };

        IncrementalSnapshotWriter::write(tmp, &data).unwrap();
        let recovered = IncrementalSnapshotReader::read(tmp).unwrap();
        assert_eq!(recovered.header.base_sequence, 0);
        assert!(recovered.header.name_mapper.is_none());
        assert!(recovered.new_vertices.is_empty());

        fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_incremental_snapshot_bad_magic() {
        let tmp = "/tmp/mg_incr_snap_bad.snap";
        let _ = fs::remove_file(tmp);
        fs::write(tmp, b"XXXX").unwrap();
        let err = IncrementalSnapshotReader::read(tmp).unwrap_err();
        assert!(matches!(err, SnapshotError::Corrupt(_)));
        fs::remove_file(tmp).ok();
    }
}
