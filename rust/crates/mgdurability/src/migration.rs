//! Migration utilities for converting legacy durability files to the current format.
//!
//! Performs version detection and delegates to the appropriate legacy reader,
//! then re-serializes with the current Rust writer.

use std::path::Path;

use crate::legacy::{CppFormatError, LegacySnapshotReader, LegacyWalReader};
use crate::snapshot::{SnapshotError, SnapshotWriter};
use crate::version::detect_format;
use crate::wal::WalError;

/// Convert a C++ v14 snapshot/WAL to the current Rust format.
///
/// The input file is read in its legacy format and written back as a current
/// format file at `output_path`.
pub fn convert_v14_to_current(
    input_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
) -> Result<(), ConversionError> {
    let data = std::fs::read(input_path.as_ref())?;
    let format = detect_format(&data).map_err(|e| ConversionError::Detect(e.to_string()))?;

    match format {
        crate::version::FormatKind::LegacyCpp(14) => {
            let snap = LegacySnapshotReader::read(&data, 14)?;
            SnapshotWriter::write(output_path, &snap)?;
            Ok(())
        }
        crate::version::FormatKind::LegacyCpp(v) if v < 14 => Err(ConversionError::Unsupported(
            format!("version {} is too old", v),
        )),
        crate::version::FormatKind::LegacyCpp(v) if v > 14 => {
            // v15-v19: use v14 reader as a best-effort fallback
            let snap = LegacySnapshotReader::read(&data, 14)?;
            SnapshotWriter::write(output_path, &snap)?;
            Ok(())
        }
        crate::version::FormatKind::Current => {
            // Already current — just copy
            std::fs::copy(input_path, output_path)?;
            Ok(())
        }
        _ => Err(ConversionError::Unsupported(format!(
            "cannot convert format {:?}",
            format
        ))),
    }
}

/// Convert a C++ v20 snapshot/WAL to the current Rust format.
///
/// v20 is closer to the current format (marker constants aligned), so
/// conversion is more straightforward than v14.
pub fn convert_v20_to_current(
    input_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
) -> Result<(), ConversionError> {
    let data = std::fs::read(input_path.as_ref())?;
    let format = detect_format(&data).map_err(|e| ConversionError::Detect(e.to_string()))?;

    match format {
        crate::version::FormatKind::LegacyCpp(v) if (20..=34).contains(&v) => {
            let snap = LegacySnapshotReader::read(&data, v)?;
            SnapshotWriter::write(output_path, &snap)?;
            Ok(())
        }
        crate::version::FormatKind::Current => {
            std::fs::copy(input_path, output_path)?;
            Ok(())
        }
        crate::version::FormatKind::LegacyCpp(v) if v < 20 => Err(ConversionError::Unsupported(
            format!("v{} snapshot too old for v20 converter", v),
        )),
        _ => Err(ConversionError::Unsupported(format!(
            "cannot convert format {:?}",
            format
        ))),
    }
}

/// Convert a legacy WAL file to the current Rust WAL format.
pub fn convert_legacy_wal_to_current(
    input_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
) -> Result<(), ConversionError> {
    let data = std::fs::read(input_path.as_ref())?;
    let format = detect_format(&data).map_err(|e| ConversionError::Detect(e.to_string()))?;

    match format {
        crate::version::FormatKind::LegacyCpp(v) if (14..=34).contains(&v) => {
            let records = LegacyWalReader::read(&data, v)?;
            let mut writer = crate::wal::WalWriter::create(output_path)?;
            for rec in &records {
                writer.append_record(rec)?;
            }
            writer.sync()?;
            Ok(())
        }
        crate::version::FormatKind::Current => {
            std::fs::copy(input_path, output_path)?;
            Ok(())
        }
        _ => Err(ConversionError::Unsupported(format!(
            "cannot convert WAL format {:?}",
            format
        ))),
    }
}

/// Errors that can occur during format conversion.
#[derive(Debug)]
pub enum ConversionError {
    Io(std::io::Error),
    Detect(String),
    Unsupported(String),
    Cpp(CppFormatError),
    Snapshot(SnapshotError),
    Wal(WalError),
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConversionError::Io(e) => write!(f, "I/O error: {}", e),
            ConversionError::Detect(s) => write!(f, "format detection error: {}", s),
            ConversionError::Unsupported(s) => write!(f, "unsupported conversion: {}", s),
            ConversionError::Cpp(e) => write!(f, "C++ parse error: {}", e),
            ConversionError::Snapshot(e) => write!(f, "snapshot error: {}", e),
            ConversionError::Wal(e) => write!(f, "WAL error: {}", e),
        }
    }
}

impl From<std::io::Error> for ConversionError {
    fn from(e: std::io::Error) -> Self {
        ConversionError::Io(e)
    }
}

impl From<CppFormatError> for ConversionError {
    fn from(e: CppFormatError) -> Self {
        ConversionError::Cpp(e)
    }
}

impl From<SnapshotError> for ConversionError {
    fn from(e: SnapshotError) -> Self {
        ConversionError::Snapshot(e)
    }
}

impl From<WalError> for ConversionError {
    fn from(e: WalError) -> Self {
        ConversionError::Wal(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mgcore::property_value::PropertyValue;
    use mgcore::types::{Gid, PropertyId};
    use std::fs;

    fn build_cpp_snapshot_v20() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&20u64.to_le_bytes());
        // SECTION_VERTEX
        buf.push(0x20);
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&1u64.to_le_bytes()); // gid
        buf.extend_from_slice(&1u64.to_le_bytes()); // label_count
        buf.extend_from_slice(&10u64.to_le_bytes()); // label
        buf.extend_from_slice(&1u64.to_le_bytes()); // prop_count
        buf.extend_from_slice(&0u64.to_le_bytes()); // prop id
        buf.push(0x12); // TYPE_INT
        buf.extend_from_slice(&42i64.to_le_bytes());
        // SECTION_EDGE
        buf.push(0x21);
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&100u64.to_le_bytes());
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.extend_from_slice(&5u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf
    }

    #[test]
    fn test_convert_v20_snapshot_to_current() {
        let input = "/tmp/mg_convert_v20_in.snap";
        let output = "/tmp/mg_convert_v20_out.snap";
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);

        fs::write(input, build_cpp_snapshot_v20()).unwrap();
        convert_v20_to_current(input, output).unwrap();

        let recovered = crate::snapshot::SnapshotReader::read(output).unwrap();
        assert_eq!(recovered.vertices.len(), 1);
        assert_eq!(recovered.vertices[0].gid, Gid::from(1u64));
        assert_eq!(recovered.edges.len(), 1);
        assert_eq!(recovered.edges[0].gid, Gid::from(100u64));

        fs::remove_file(input).ok();
        fs::remove_file(output).ok();
    }

    #[test]
    fn test_convert_v14_snapshot_to_current() {
        let mut data = build_cpp_snapshot_v20();
        data[4..12].copy_from_slice(&14u64.to_le_bytes());

        let input = "/tmp/mg_convert_v14_in.snap";
        let output = "/tmp/mg_convert_v14_out.snap";
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);

        fs::write(input, &data).unwrap();
        convert_v14_to_current(input, output).unwrap();

        let recovered = crate::snapshot::SnapshotReader::read(output).unwrap();
        assert_eq!(recovered.vertices.len(), 1);
        assert_eq!(recovered.vertices[0].gid, Gid::from(1u64));

        fs::remove_file(input).ok();
        fs::remove_file(output).ok();
    }

    #[test]
    fn test_convert_current_snapshot_noop() {
        let input = "/tmp/mg_convert_cur_in.snap";
        let output = "/tmp/mg_convert_cur_out.snap";
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);

        let data = crate::snapshot::SnapshotData {
            name_mapper: crate::snapshot::NameMapperSnapshot {
                labels: vec![],
                properties: vec![],
                edge_types: vec![],
            },
            vertices: vec![crate::snapshot::VertexSnapshotEntry {
                gid: Gid::from(7u64),
                labels: vec![],
                properties: vec![(PropertyId::from(0u32), PropertyValue::Int(1))],
            }],
            edges: vec![],
        };
        crate::snapshot::SnapshotWriter::write(input, &data).unwrap();
        convert_v20_to_current(input, output).unwrap();

        let recovered = crate::snapshot::SnapshotReader::read(output).unwrap();
        assert_eq!(recovered.vertices[0].gid, Gid::from(7u64));

        fs::remove_file(input).ok();
        fs::remove_file(output).ok();
    }

    #[test]
    fn test_convert_legacy_wal_to_current() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGwl");
        buf.extend_from_slice(&20u64.to_le_bytes());
        buf.push(0x50); // V20_VERTEX_CREATE
        buf.extend_from_slice(&10u64.to_le_bytes());
        buf.extend_from_slice(&100u64.to_le_bytes());
        buf.push(0x58); // V20_TRANSACTION_END
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&101u64.to_le_bytes());

        let input = "/tmp/mg_convert_wal_in.wal";
        let output = "/tmp/mg_convert_wal_out.wal";
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);

        fs::write(input, &buf).unwrap();
        convert_legacy_wal_to_current(input, output).unwrap();

        let reader = crate::wal::WalReader::open(output).unwrap();
        assert_eq!(reader.len(), 2);
        assert!(matches!(
            reader.records()[0],
            crate::delta_record::DeltaRecord::VertexCreate { .. }
        ));

        fs::remove_file(input).ok();
        fs::remove_file(output).ok();
    }

    #[test]
    fn test_convert_unsupported_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"MGsn");
        buf.extend_from_slice(&999u64.to_le_bytes());
        let input = "/tmp/mg_convert_bad.snap";
        let output = "/tmp/mg_convert_bad_out.snap";
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);
        fs::write(input, &buf).unwrap();
        let err = convert_v20_to_current(input, output).unwrap_err();
        assert!(matches!(err, ConversionError::Unsupported(_)));
        fs::remove_file(input).ok();
        fs::remove_file(output).ok();
    }
}
