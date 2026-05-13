//! Bolt chunked framing.
//!
//! Messages are framed as: `[u16 chunk_size][chunk_data...][0x0000 end marker]`
//! Max chunk data size is 65535 bytes.

use std::io::{self, Read, Write};

/// Max size of a single chunk's data (2 bytes for size header).
pub const MAX_CHUNK_SIZE: usize = 65535;

/// Write a complete Bolt message with chunked framing.
pub fn write_message(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < payload.len() {
        let remaining = payload.len() - offset;
        let chunk_size = remaining.min(MAX_CHUNK_SIZE);
        let size_header = (chunk_size as u16).to_be_bytes();
        w.write_all(&size_header)?;
        w.write_all(&payload[offset..offset + chunk_size])?;
        offset += chunk_size;
    }
    // End marker
    w.write_all(&[0x00, 0x00])?;
    w.flush()
}

/// Read a complete Bolt message into a buffer.
/// Returns the message payload (all chunks concatenated, without headers).
pub fn read_message(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut payload = Vec::new();
    loop {
        let mut size_buf = [0u8; 2];
        r.read_exact(&mut size_buf)?;
        let chunk_size = u16::from_be_bytes(size_buf) as usize;

        if chunk_size == 0 {
            break; // end marker
        }

        let mut chunk = vec![0u8; chunk_size];
        r.read_exact(&mut chunk)?;
        payload.extend_from_slice(&chunk);
    }
    Ok(payload)
}

/// Encode a message value and wrap it in chunked framing.
pub fn encode_message(w: &mut impl Write, msg: &crate::message::Message) -> io::Result<()> {
    let mut payload = Vec::new();
    msg.to_value().encode(&mut payload)?;
    write_message(w, &payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;
    use crate::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_write_and_read_message() {
        // Encode a SUCCESS message
        let msg = Message::Success {
            metadata: {
                let mut m = HashMap::new();
                m.insert("server".into(), Value::String("test".into()));
                m
            },
        };

        let mut buf = Vec::new();
        encode_message(&mut buf, &msg).unwrap();

        // Parse back
        let mut cursor = std::io::Cursor::new(&buf);
        let payload = read_message(&mut cursor).unwrap();
        assert!(!payload.is_empty());
    }

    #[test]
    fn test_empty_message() {
        let mut buf = Vec::new();
        write_message(&mut buf, &[]).unwrap();
        // Should just be the end marker
        assert_eq!(buf, &[0x00, 0x00]);
    }

    #[test]
    fn test_large_message_chunking() {
        let payload = vec![0x42u8; MAX_CHUNK_SIZE + 100];
        let mut buf = Vec::new();
        write_message(&mut buf, &payload).unwrap();

        // Should have 2 chunks + end marker
        // First chunk: 2 bytes size + 65535 bytes
        // Second chunk: 2 bytes size + 100 bytes
        // End marker: 2 bytes
        assert_eq!(buf.len(), 2 + 65535 + 2 + 100 + 2);
    }
}
