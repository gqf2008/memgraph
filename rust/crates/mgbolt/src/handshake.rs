//! Bolt handshake — magic preamble + version negotiation.
//!
//! Supports both the legacy 4-slot format (Bolt ≤5.0) and the
//! manifest-v1 format introduced in Bolt 5.7+.

/// Bolt protocol handshake utilities.
pub struct Handshake;

impl Handshake {
    /// Magic preamble bytes: `0x60 0x60 0xB0 0x17`
    pub const PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

    /// Manifest v1 request marker (occupies one version slot).
    pub const MANIFEST_V1: u32 = 0x0000_01FF;

    /// Server-supported versions, newest first.
    pub const SUPPORTED: [u32; 10] = [
        0x0508, // Bolt v5.8
        0x0507, // Bolt v5.7
        0x0506, // Bolt v5.6
        0x0505, // Bolt v5.5
        0x0504, // Bolt v5.4
        0x0503, // Bolt v5.3
        0x0502, // Bolt v5.2
        0x0403, // Bolt v4.3
        0x0401, // Bolt v4.1
        0x0400, // Bolt v4.0
    ];

    /// Version ranges advertised during manifest negotiation (4.3+ style).
    /// Each entry is `[reserved, range, highest_minor, major]`.
    /// Range = how many additional minors below highest_minor.
    pub const SUPPORTED_RANGES: [u32; 2] = [
        0x0006_0805, // 5.8 down to 5.2  (range=6)
        0x0003_0304, // 4.3 down to 4.0  (range=3)
    ];

    /// Max versions the client can propose in legacy format (4 × u32 = 16 bytes).
    pub const MAX_VERSIONS: usize = 4;

    /// Returns true if the client has requested manifest-v1 negotiation.
    pub fn is_manifest_v1_request(client_versions: &[u32]) -> bool {
        client_versions.contains(&Self::MANIFEST_V1)
    }

    /// Select the highest mutually supported version from a flat list.
    /// Returns None if no version matches.
    pub fn negotiate(client_versions: &[u32]) -> Option<u32> {
        for &server_v in &Self::SUPPORTED {
            if client_versions.contains(&server_v) {
                return Some(server_v);
            }
        }
        None
    }

    /// Read client versions from the handshake bytes (legacy big-endian u32s).
    pub fn parse_versions(data: &[u8]) -> Vec<u32> {
        data.chunks_exact(4)
            .take(Self::MAX_VERSIONS)
            .map(|chunk| u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect()
    }

    /// Expand client version proposals into a flat list of concrete versions.
    /// Handles both legacy single-version entries and 4.3+ range-encoded
    /// entries (`[reserved, range, highest_minor, major]`).
    pub fn expand_client_versions(client_versions: &[u32]) -> Vec<u32> {
        let mut out = Vec::new();
        for &v in client_versions {
            if v == Self::MANIFEST_V1 || v == 0 {
                continue;
            }
            let bytes = v.to_be_bytes();
            // Legacy format: reserved bytes are zero, range byte is zero
            if bytes[0] == 0 && bytes[1] == 0 {
                out.push(v);
                continue;
            }
            // 4.3+ range format: [reserved, range, highest_minor, major]
            let range = bytes[1] as u16;
            let highest_minor = bytes[2] as u16;
            let major = bytes[3] as u16;
            for i in 0..=range {
                let minor = highest_minor.saturating_sub(i);
                out.push(((major as u32) << 8) | (minor as u32));
            }
        }
        out
    }

    /// Pack a concrete version into a manifest response value.
    pub fn pack_version(major: u8, minor: u8) -> u32 {
        ((major as u32) << 8) | (minor as u32)
    }

    /// Normalize a version received during manifest negotiation.
    /// Manifest format sends versions as `[0, 0, minor, major]` (big-endian u32).
    /// This converts to our internal `(major << 8) | minor` format.
    pub fn normalize_manifest_version(v: u32) -> u32 {
        // If it's already in our supported list, return as-is (legacy format).
        if Self::SUPPORTED.contains(&v) {
            return v;
        }
        let bytes = v.to_be_bytes();
        if bytes[0] == 0 && bytes[1] == 0 {
            let major = bytes[3];
            let minor = bytes[2];
            let normalized = ((major as u32) << 8) | (minor as u32);
            if Self::SUPPORTED.contains(&normalized) {
                return normalized;
            }
        }
        v
    }
}

/// VarInt helpers (same encoding as Protocol Buffers).
pub struct VarInt;

impl VarInt {
    /// Encode an unsigned 64-bit integer as VarInt bytes.
    pub fn encode(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
        out
    }

    /// Decode a VarInt from a byte slice.
    /// Returns (decoded_value, bytes_consumed) or None if incomplete.
    pub fn decode(data: &[u8]) -> Option<(u64, usize)> {
        let mut result: u64 = 0;
        let mut shift = 0;
        for (i, &b) in data.iter().enumerate() {
            let val = (b & 0x7F) as u64;
            result |= val << shift;
            if b & 0x80 == 0 {
                return Some((result, i + 1));
            }
            shift += 7;
            if shift >= 64 {
                return None; // overflow
            }
        }
        None // incomplete
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_negotiate() {
        let client_versions = vec![0x0502, 0x0403, 0x0300, 0x0100];
        assert_eq!(Handshake::negotiate(&client_versions), Some(0x0502));
    }

    #[test]
    fn test_handshake_no_match() {
        let client_versions = vec![0x0300, 0x0200, 0x0100, 0x0000];
        assert_eq!(Handshake::negotiate(&client_versions), None);
    }

    #[test]
    fn test_handshake_preamble() {
        assert_eq!(Handshake::PREAMBLE, [0x60, 0x60, 0xB0, 0x17]);
    }

    #[test]
    fn test_manifest_detection() {
        let v = vec![0x0000_01FF, 0x0508, 0x0403, 0x0000];
        assert!(Handshake::is_manifest_v1_request(&v));

        let v2 = vec![0x0508, 0x0403, 0x0000, 0x0000];
        assert!(!Handshake::is_manifest_v1_request(&v2));
    }

    #[test]
    fn test_expand_range_versions() {
        // 5.8 down to 5.6: [0x00, 0x02, 0x08, 0x05]
        let client = vec![0x0002_0805];
        let expanded = Handshake::expand_client_versions(&client);
        assert!(expanded.contains(&0x0508));
        assert!(expanded.contains(&0x0507));
        assert!(expanded.contains(&0x0506));
        assert!(!expanded.contains(&0x0505));
    }

    #[test]
    fn test_expand_mixed() {
        // Python driver style: manifest marker + ranges + legacy
        let client = vec![0x0000_01FF, 0x0008_0805, 0x0002_0404, 0x0000_0003];
        let expanded = Handshake::expand_client_versions(&client);
        assert!(expanded.contains(&0x0508));
        assert!(expanded.contains(&0x0502));
        assert!(expanded.contains(&0x0404));
        assert!(expanded.contains(&0x0402));
        assert!(expanded.contains(&0x0003));
    }

    #[test]
    fn test_varint_roundtrip() {
        let cases = [0u64, 1, 127, 128, 255, 256, 16383, 16384, 1_000_000];
        for &v in &cases {
            let encoded = VarInt::encode(v);
            let (decoded, consumed) = VarInt::decode(&encoded).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn test_normalize_manifest_version() {
        // Manifest format: [0, 0, minor, major] -> [0, 0, major, minor]
        assert_eq!(Handshake::normalize_manifest_version(0x0000_0805), 0x0508);
        assert_eq!(Handshake::normalize_manifest_version(0x0000_0705), 0x0507);
        assert_eq!(Handshake::normalize_manifest_version(0x0000_0304), 0x0403);
        // Legacy format should be returned as-is
        assert_eq!(Handshake::normalize_manifest_version(0x0508), 0x0508);
        assert_eq!(Handshake::normalize_manifest_version(0x0403), 0x0403);
    }
}
