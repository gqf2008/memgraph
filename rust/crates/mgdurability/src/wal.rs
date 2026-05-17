//! Write-Ahead Log (WAL) for database durability.
//!
//! Format (matching C++): `[MGwl magic: 4B][version: u64 LE][records...]`
//! Each record is SLK-framed: `[u32 LE size][SLK payload][u32 LE CRC32 checksum][0x00000000 footer]`.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::Path;

use mgslk::{Builder, Reader, SlkLoad, SlkSave, DURABILITY_VERSION, WAL_MAGIC};

/// Compute CRC32 checksum using the standard IEEE/ISO polynomial (0xEDB88320).
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFFFFFFu32;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFFFFFF
}

use crate::delta_record::DeltaRecord;

/// WAL writer. Appends delta records to a WAL file.
pub struct WalWriter {
    file: File,
    path: String,
    records_written: u64,
}

impl WalWriter {
    /// Create a new WAL file with the MGwl header.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;

        // Write magic: "MGwl"
        file.write_all(WAL_MAGIC)?;
        // Write version: u64 LE
        file.write_all(&DURABILITY_VERSION.to_le_bytes())?;

        Ok(Self {
            file,
            path: path.as_ref().to_string_lossy().to_string(),
            records_written: 0,
        })
    }

    /// Append a delta record to the WAL. Each record is SLK-framed with CRC32 checksum.
    pub fn append_record(&mut self, record: &DeltaRecord) -> Result<(), std::io::Error> {
        let (mut builder, collector) = Builder::new_collecting();
        record.slk_save(&mut builder);
        builder.finalize();
        let framed_data = collector.into_vec();

        // Compute CRC32 over the entire SLK frame (size + payload + footer)
        let checksum = crc32(&framed_data);

        self.file.write_all(&framed_data)?;
        self.file.write_all(&checksum.to_le_bytes())?;
        self.records_written += 1;
        Ok(())
    }

    /// Sync the WAL file to disk.
    pub fn sync(&mut self) -> Result<(), std::io::Error> {
        self.file.flush()?;
        self.file.sync_all()
    }

    /// Reset the WAL: truncate the current file and rewrite the header.
    /// Called after a snapshot to prevent unbounded WAL growth.
    pub fn reset(&mut self) -> Result<(), std::io::Error> {
        self.file.flush()?;
        self.file.sync_all()?;
        self.file.set_len(0)?;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_all(WAL_MAGIC)?;
        self.file.write_all(&DURABILITY_VERSION.to_le_bytes())?;
        self.file.flush()?;
        self.records_written = 0;
        Ok(())
    }

    pub fn records_written(&self) -> u64 {
        self.records_written
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

/// WAL reader. Reads delta records from a WAL file during recovery.
#[derive(Debug)]
pub struct WalReader {
    records: Vec<DeltaRecord>,
}

impl WalReader {
    /// Open and read a WAL file. Returns all delta records in order.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let mut file = File::open(path.as_ref()).map_err(WalError::Io)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(WalError::Io)?;
        Self::from_bytes(&data)
    }

    /// Parse WAL records directly from an in-memory byte slice (useful when
    /// the WAL was transferred over the network and is not yet on disk).
    pub fn from_bytes(data: &[u8]) -> Result<Self, WalError> {
        if data.len() < 12 {
            return Err(WalError::Corrupt("WAL file too short".into()));
        }

        // Verify magic
        if &data[0..4] != WAL_MAGIC {
            return Err(WalError::Corrupt("invalid WAL magic".into()));
        }

        // Read version
        let version = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);

        if mgslk::is_cpp_version(version) {
            let records = crate::legacy::LegacyWalReader::read(data, version)
                .map_err(|e| WalError::Corrupt(format!("C++ WAL parse error: {}", e)))?;
            return Ok(Self { records });
        }

        if version > DURABILITY_VERSION {
            return Err(WalError::UnsupportedVersion(version));
        }

        // Read SLK-framed records from the remaining data
        let record_data = &data[12..];
        let records = Self::parse_records(record_data)?;

        Ok(Self { records })
    }

    /// Parse SLK-framed delta records from a byte slice.
    /// Each record format: [4-byte size][payload][4-byte CRC32][4-byte footer]
    fn parse_records(mut data: &[u8]) -> Result<Vec<DeltaRecord>, WalError> {
        let mut records = Vec::new();

        while !data.is_empty() {
            // Each record is a self-contained SLK stream with its own framing
            if data.len() < 4 {
                break;
            }
            let seg_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

            if seg_size == 0 {
                // Footer — end of this record's stream
                data = &data[4..];
                continue;
            }

            // Record = [seg_size(4)][payload(seg_size)][CRC32(4)][footer(4)]
            let record_end = 4 + seg_size + 4 + 4;
            if data.len() < record_end {
                break;
            }

            // The SLK frame includes size + payload + footer
            let slk_frame = &data[..4 + seg_size + 4];

            // Verify CRC32 checksum
            let stored_crc = u32::from_le_bytes([
                data[4 + seg_size + 4],
                data[4 + seg_size + 5],
                data[4 + seg_size + 6],
                data[4 + seg_size + 7],
            ]);
            let computed_crc = crc32(slk_frame);
            if stored_crc != computed_crc {
                // CRC mismatch — corrupted record, skip it
                tracing::warn!(
                    "WAL CRC mismatch at offset {}: stored={:#010x}, computed={:#010x}",
                    records.len(),
                    stored_crc,
                    computed_crc
                );
                data = &data[record_end..];
                continue;
            }

            let record_bytes = &data[..4 + seg_size + 4];
            let mut reader = Reader::new(record_bytes);
            let record = DeltaRecord::slk_load(&mut reader)
                .map_err(|e| WalError::Corrupt(format!("failed to decode record: {}", e)))?;

            records.push(record);
            data = &data[record_end..];
        }

        Ok(records)
    }

    /// Get all records from this WAL file.
    pub fn records(&self) -> &[DeltaRecord] {
        &self.records
    }

    /// Number of records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// WAL errors.
#[derive(Debug)]
pub enum WalError {
    Io(std::io::Error),
    Corrupt(String),
    UnsupportedVersion(u64),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "WAL I/O error: {}", e),
            WalError::Corrupt(msg) => write!(f, "corrupt WAL: {}", msg),
            WalError::UnsupportedVersion(v) => write!(f, "unsupported WAL version: {}", v),
        }
    }
}

impl From<std::io::Error> for WalError {
    fn from(e: std::io::Error) -> Self {
        WalError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta_record::DeltaRecord;
    use mgcore::property_value::PropertyValue;
    use mgcore::types::{Gid, LabelId, PropertyId};
    use std::fs;

    #[test]
    fn test_wal_write_and_read() {
        let tmp = "/tmp/mg_wal_test.wal";
        let _ = fs::remove_file(tmp);

        // Write
        {
            let mut writer = WalWriter::create(tmp).unwrap();
            writer
                .append_record(&DeltaRecord::TransactionStart { timestamp: 1 })
                .unwrap();
            writer
                .append_record(&DeltaRecord::VertexCreate {
                    gid: Gid::from(10u64),
                    timestamp: 100,
                })
                .unwrap();
            writer
                .append_record(&DeltaRecord::VertexAddLabel {
                    gid: Gid::from(10u64),
                    label: LabelId::from(5u32),
                })
                .unwrap();
            writer
                .append_record(&DeltaRecord::VertexSetProperty {
                    gid: Gid::from(10u64),
                    key: PropertyId::from(0u32),
                    value: PropertyValue::String("hello".into()),
                })
                .unwrap();
            writer
                .append_record(&DeltaRecord::TransactionEnd {
                    timestamp: 1,
                    commit_timestamp: 101,
                })
                .unwrap();
            writer.sync().unwrap();
            assert_eq!(writer.records_written(), 5);
        }

        // Read
        {
            let reader = WalReader::open(tmp).unwrap();
            assert_eq!(reader.len(), 5);
            assert!(matches!(
                reader.records()[0],
                DeltaRecord::TransactionStart { .. }
            ));
            assert!(matches!(
                reader.records()[1],
                DeltaRecord::VertexCreate { .. }
            ));
            assert!(matches!(
                reader.records()[4],
                DeltaRecord::TransactionEnd { .. }
            ));
        }

        fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_wal_empty() {
        let tmp = "/tmp/mg_wal_empty.wal";
        let _ = fs::remove_file(tmp);

        {
            let _writer = WalWriter::create(tmp).unwrap();
        }
        {
            let reader = WalReader::open(tmp).unwrap();
            assert!(reader.records().is_empty());
        }

        fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_wal_bad_magic() {
        // Create a file with wrong magic
        let tmp = "/tmp/mg_wal_bad.wal";
        let _ = fs::remove_file(tmp);

        fs::write(tmp, b"XXXX").unwrap();
        let err = WalReader::open(tmp).unwrap_err();
        assert!(matches!(err, WalError::Corrupt(_)));

        fs::remove_file(tmp).ok();
    }

    #[test]
    fn test_wal_from_bytes_matches_open() {
        let tmp = "/tmp/mg_wal_from_bytes.wal";
        let _ = fs::remove_file(tmp);

        // Write a WAL with two records.
        {
            let mut writer = WalWriter::create(tmp).unwrap();
            writer
                .append_record(&DeltaRecord::VertexCreate {
                    gid: Gid::from(10u64),
                    timestamp: 100,
                })
                .unwrap();
            writer
                .append_record(&DeltaRecord::VertexAddLabel {
                    gid: Gid::from(10u64),
                    label: LabelId::from(5u32),
                })
                .unwrap();
            writer.sync().unwrap();
        }

        // Read via `open`.
        let via_open = WalReader::open(tmp).unwrap();

        // Read the same file into memory and parse via `from_bytes`.
        let bytes = fs::read(tmp).unwrap();
        let via_bytes = WalReader::from_bytes(&bytes).unwrap();

        assert_eq!(via_open.len(), via_bytes.len());
        assert_eq!(via_open.records(), via_bytes.records());

        fs::remove_file(tmp).ok();
    }
}
