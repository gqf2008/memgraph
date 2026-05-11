//! Format version detection and dispatch.
//!
//! Determines whether a durability file is in the current Rust SLK format
//! or an older C++ format (v14-v34) and selects the appropriate reader.

use mgslk::{DURABILITY_VERSION, SNAPSHOT_MAGIC, WAL_MAGIC};

/// Classification of a durability file format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormatKind {
    /// Current Rust SLK format (version >= 100).
    Current,
    /// Legacy C++ format (versions 14-34).
    LegacyCpp(u64),
    /// Unrecognized / unsupported.
    Unknown(u64),
}

/// Detect the format of a snapshot or WAL file from its header bytes.
///
/// Expects at least 12 bytes: `[magic: 4][version: u64 LE]`.
pub fn detect_format(data: &[u8]) -> Result<FormatKind, &'static str> {
    if data.len() < 12 {
        return Err("file too short for header");
    }
    let magic = &data[0..4];
    let version = u64::from_le_bytes([
        data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
    ]);

    if magic != SNAPSHOT_MAGIC && magic != WAL_MAGIC {
        return Err("unrecognized magic");
    }

    if mgslk::is_cpp_version(version) {
        Ok(FormatKind::LegacyCpp(version))
    } else if version <= DURABILITY_VERSION {
        Ok(FormatKind::Current)
    } else {
        Ok(FormatKind::Unknown(version))
    }
}

/// Convenience: true if the header indicates a snapshot file.
pub fn is_snapshot(data: &[u8]) -> bool {
    data.len() >= 4 && &data[0..4] == SNAPSHOT_MAGIC
}

/// Convenience: true if the header indicates a WAL file.
pub fn is_wal(data: &[u8]) -> bool {
    data.len() >= 4 && &data[0..4] == WAL_MAGIC
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_current_snapshot() {
        let mut buf = Vec::new();
        buf.extend_from_slice(SNAPSHOT_MAGIC);
        buf.extend_from_slice(&DURABILITY_VERSION.to_le_bytes());
        assert_eq!(detect_format(&buf).unwrap(), FormatKind::Current);
        assert!(is_snapshot(&buf));
    }

    #[test]
    fn test_detect_current_wal() {
        let mut buf = Vec::new();
        buf.extend_from_slice(WAL_MAGIC);
        buf.extend_from_slice(&DURABILITY_VERSION.to_le_bytes());
        assert_eq!(detect_format(&buf).unwrap(), FormatKind::Current);
        assert!(is_wal(&buf));
    }

    #[test]
    fn test_detect_legacy_cpp_v14() {
        let mut buf = Vec::new();
        buf.extend_from_slice(SNAPSHOT_MAGIC);
        buf.extend_from_slice(&14u64.to_le_bytes());
        assert_eq!(detect_format(&buf).unwrap(), FormatKind::LegacyCpp(14));
    }

    #[test]
    fn test_detect_legacy_cpp_v34() {
        let mut buf = Vec::new();
        buf.extend_from_slice(WAL_MAGIC);
        buf.extend_from_slice(&34u64.to_le_bytes());
        assert_eq!(detect_format(&buf).unwrap(), FormatKind::LegacyCpp(34));
    }

    #[test]
    fn test_detect_unknown_future_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(SNAPSHOT_MAGIC);
        buf.extend_from_slice(&999u64.to_le_bytes());
        assert_eq!(detect_format(&buf).unwrap(), FormatKind::Unknown(999));
    }

    #[test]
    fn detect_too_short() {
        assert_eq!(detect_format(b"MG"), Err("file too short for header"));
    }

    #[test]
    fn detect_bad_magic() {
        let mut buf = b"XXXX".to_vec();
        buf.extend_from_slice(&100u64.to_le_bytes());
        assert_eq!(detect_format(&buf), Err("unrecognized magic"));
    }
}
