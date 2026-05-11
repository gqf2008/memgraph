//! Database snapshot for full-state persistence.
//!
//! Format (matching C++): `[MGsn magic: 4B][version: u64 LE][sections...]`
//! Each section is SLK-framed.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use mgcore::property_value::PropertyValue;
use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
use mgslk::{Builder, Reader, SlkDecodeError, SlkLoad, SlkSave};
use mgslk::{DURABILITY_VERSION, SNAPSHOT_MAGIC};

/// Catalog name→ID mapping persisted in snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct NameMapperSnapshot {
    pub labels: Vec<(String, LabelId)>,
    pub properties: Vec<(String, PropertyId)>,
    pub edge_types: Vec<(String, EdgeTypeId)>,
}

impl SlkSave for NameMapperSnapshot {
    fn slk_save(&self, builder: &mut Builder) {
        self.labels.slk_save(builder);
        self.properties.slk_save(builder);
        self.edge_types.slk_save(builder);
    }
}

impl SlkLoad for NameMapperSnapshot {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            labels: Vec::<(String, LabelId)>::slk_load(reader)?,
            properties: Vec::<(String, PropertyId)>::slk_load(reader)?,
            edge_types: Vec::<(String, EdgeTypeId)>::slk_load(reader)?,
        })
    }
}

/// Snapshot serialized data.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotData {
    pub name_mapper: NameMapperSnapshot,
    pub vertices: Vec<VertexSnapshotEntry>,
    pub edges: Vec<EdgeSnapshotEntry>,
}

/// Serialized vertex in a snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct VertexSnapshotEntry {
    pub gid: Gid,
    pub labels: Vec<LabelId>,
    pub properties: Vec<(PropertyId, PropertyValue)>,
}

impl SlkSave for VertexSnapshotEntry {
    fn slk_save(&self, builder: &mut Builder) {
        self.gid.slk_save(builder);
        self.labels.slk_save(builder);
        self.properties.slk_save(builder);
    }
}

impl SlkLoad for VertexSnapshotEntry {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            gid: Gid::slk_load(reader)?,
            labels: Vec::slk_load(reader)?,
            properties: Vec::<(PropertyId, PropertyValue)>::slk_load(reader)?,
        })
    }
}

/// Serialized edge in a snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeSnapshotEntry {
    pub gid: Gid,
    pub from_vertex: Gid,
    pub to_vertex: Gid,
    pub edge_type: EdgeTypeId,
    pub properties: Vec<(PropertyId, PropertyValue)>,
}

impl SlkSave for EdgeSnapshotEntry {
    fn slk_save(&self, builder: &mut Builder) {
        self.gid.slk_save(builder);
        self.from_vertex.slk_save(builder);
        self.to_vertex.slk_save(builder);
        self.edge_type.slk_save(builder);
        self.properties.slk_save(builder);
    }
}

impl SlkLoad for EdgeSnapshotEntry {
    fn slk_load(reader: &mut Reader) -> Result<Self, SlkDecodeError> {
        Ok(Self {
            gid: Gid::slk_load(reader)?,
            from_vertex: Gid::slk_load(reader)?,
            to_vertex: Gid::slk_load(reader)?,
            edge_type: EdgeTypeId::slk_load(reader)?,
            properties: Vec::<(PropertyId, PropertyValue)>::slk_load(reader)?,
        })
    }
}

/// Snapshot writer. Writes full database state.
pub struct SnapshotWriter;

impl SnapshotWriter {
    /// Write a snapshot to the given path. Each section is SLK-framed.
    pub fn write(path: impl AsRef<Path>, data: &SnapshotData) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;

        // Write magic: "MGsn"
        file.write_all(SNAPSHOT_MAGIC)?;
        // Write version: u64 LE
        file.write_all(&DURABILITY_VERSION.to_le_bytes())?;

        // Write name mapper section (labels, properties, edge types) — single element
        Self::write_section(&mut file, std::slice::from_ref(&data.name_mapper), b"NAME")?;
        // Write vertex section
        Self::write_section(&mut file, &data.vertices, b"VERT")?;
        // Write edge section
        Self::write_section(&mut file, &data.edges, b"EDGE")?;

        file.flush()?;
        file.sync_all()?;

        Ok(())
    }

    fn write_section<T: SlkSave + Clone>(
        file: &mut File,
        items: &[T],
        _section_tag: &[u8; 4],
    ) -> Result<(), std::io::Error> {
        let (mut builder, collector) = Builder::new_collecting();
        items.to_vec().slk_save(&mut builder);
        builder.finalize();
        let framed = collector.into_vec();
        file.write_all(&framed)?;
        Ok(())
    }
}

/// Snapshot reader. Reads full database state from a snapshot file.
pub struct SnapshotReader;

impl SnapshotReader {
    /// Read a snapshot from the given path.
    pub fn read(path: impl AsRef<Path>) -> Result<SnapshotData, SnapshotError> {
        let mut file = File::open(path.as_ref()).map_err(SnapshotError::Io)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(SnapshotError::Io)?;

        if data.len() < 12 {
            return Err(SnapshotError::Corrupt("snapshot file too short".into()));
        }

        // Verify magic
        if &data[0..4] != SNAPSHOT_MAGIC {
            return Err(SnapshotError::Corrupt("invalid snapshot magic".into()));
        }

        // Read version
        let version = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);

        // Detect C++-format snapshots (versions 14-34) and convert
        if mgslk::is_cpp_version(version) {
            return crate::legacy::LegacySnapshotReader::read(&data, version)
                .map_err(|e| SnapshotError::Corrupt(format!("C++ snapshot parse error: {}", e)));
        }

        if version > DURABILITY_VERSION {
            return Err(SnapshotError::UnsupportedVersion(version));
        }

        // Parse all sections from remaining data
        let section_data = &data[12..];
        let sections = Self::parse_sections(section_data)?;

        // v100+: 3 sections (NAME, VERT, EDGE). Older: 2 sections (VERT, EDGE).
        let (name_mapper, vertices, edges) = if sections.len() >= 3 {
            let nm: Vec<NameMapperSnapshot> = Self::decode_section(&sections[0])?;
            let nm = nm.into_iter().next().unwrap_or(NameMapperSnapshot {
                labels: vec![],
                properties: vec![],
                edge_types: vec![],
            });
            let v: Vec<VertexSnapshotEntry> = Self::decode_section(&sections[1])?;
            let e: Vec<EdgeSnapshotEntry> = Self::decode_section(&sections[2])?;
            (nm, v, e)
        } else if sections.len() == 2 {
            let v: Vec<VertexSnapshotEntry> = Self::decode_section(&sections[0])?;
            let e: Vec<EdgeSnapshotEntry> = Self::decode_section(&sections[1])?;
            (
                NameMapperSnapshot {
                    labels: vec![],
                    properties: vec![],
                    edge_types: vec![],
                },
                v,
                e,
            )
        } else {
            return Err(SnapshotError::Corrupt("missing sections".into()));
        };

        Ok(SnapshotData {
            name_mapper,
            vertices,
            edges,
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
                // Footer of current section. Section data is everything up to here.
                // We already captured it in the current section buffer.
                data = &data[4..];
                continue;
            }
            let section_end = 4 + seg_size + 4; // header + payload + footer
            if data.len() < section_end {
                break;
            }
            // The section is the entire SLK stream (including header and footer)
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

/// Snapshot errors.
#[derive(Debug)]
pub enum SnapshotError {
    Io(std::io::Error),
    Corrupt(String),
    UnsupportedVersion(u64),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::Io(e) => write!(f, "snapshot I/O error: {}", e),
            SnapshotError::Corrupt(msg) => write!(f, "corrupt snapshot: {}", msg),
            SnapshotError::UnsupportedVersion(v) => {
                write!(f, "unsupported snapshot version: {}", v)
            }
        }
    }
}

impl From<std::io::Error> for SnapshotError {
    fn from(e: std::io::Error) -> Self {
        SnapshotError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::property_value::PropertyValue;
    use mgcore::types::{EdgeTypeId, Gid, LabelId, PropertyId};
    use std::fs;

    #[test]
    fn test_snapshot_write_and_read() {
        let tmp = "/tmp/mg_snapshot_test.snap";
        let _ = fs::remove_file(tmp);

        let data = SnapshotData {
            name_mapper: NameMapperSnapshot {
                labels: vec![],
                properties: vec![],
                edge_types: vec![],
            },
            vertices: vec![
                VertexSnapshotEntry {
                    gid: Gid::from(1u64),
                    labels: vec![LabelId::from(10u32)],
                    properties: vec![(PropertyId::from(0u32), PropertyValue::Int(42))],
                },
                VertexSnapshotEntry {
                    gid: Gid::from(2u64),
                    labels: vec![LabelId::from(20u32)],
                    properties: vec![],
                },
            ],
            edges: vec![EdgeSnapshotEntry {
                gid: Gid::from(100u64),
                from_vertex: Gid::from(1u64),
                to_vertex: Gid::from(2u64),
                edge_type: EdgeTypeId::from(5u32),
                properties: vec![(PropertyId::from(0u32), PropertyValue::String("e".into()))],
            }],
        };

        // Write
        SnapshotWriter::write(tmp, &data).unwrap();

        // Read
        let recovered = SnapshotReader::read(tmp).unwrap();
        assert_eq!(recovered.vertices.len(), 2);
        assert_eq!(recovered.vertices[0].gid, Gid::from(1u64));
        assert_eq!(recovered.vertices[0].labels, vec![LabelId::from(10u32)]);
        assert_eq!(recovered.edges.len(), 1);
        assert_eq!(recovered.edges[0].gid, Gid::from(100u64));

        fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_snapshot_empty() {
        let tmp = "/tmp/mg_snapshot_empty.snap";
        let _ = fs::remove_file(tmp);

        let data = SnapshotData {
            name_mapper: NameMapperSnapshot {
                labels: vec![],
                properties: vec![],
                edge_types: vec![],
            },
            vertices: vec![],
            edges: vec![],
        };

        SnapshotWriter::write(tmp, &data).unwrap();
        let recovered = SnapshotReader::read(tmp).unwrap();
        assert!(recovered.vertices.is_empty());
        assert!(recovered.edges.is_empty());

        fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_snapshot_bad_magic() {
        let tmp = "/tmp/mg_snapshot_bad.snap";
        let _ = fs::remove_file(tmp);

        fs::write(tmp, b"XXXX").unwrap();
        let err = SnapshotReader::read(tmp).unwrap_err();
        assert!(matches!(err, SnapshotError::Corrupt(_)));

        fs::remove_file(tmp).ok();
    }
}
