//! # mgbolt — Bolt protocol implementation
//!
//! Bolt v4.x/5.x wire protocol for Memgraph.
//! Supports handshake, chunked framing, and PackStream encoding.

pub mod decoder;
pub mod framing;
pub mod handshake;
pub mod message;
pub mod ssl;
pub mod value;

pub use decoder::decode_value;
pub use handshake::Handshake;
pub use message::Message;
pub use ssl::{client_config, client_config_with_ca, server_config, server_config_mtls, TlsError};
pub use value::Value;
