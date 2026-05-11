//! # mgbolt — Bolt protocol implementation
//!
//! Bolt v4.x/5.x wire protocol for Memgraph.
//! Supports handshake, chunked framing, and PackStream encoding.

pub mod handshake;
pub mod value;
pub mod message;
pub mod framing;
pub mod decoder;
pub mod ssl;

pub use handshake::Handshake;
pub use value::Value;
pub use message::Message;
pub use decoder::decode_value;
pub use ssl::{client_config, client_config_with_ca, server_config, server_config_mtls, TlsError};
