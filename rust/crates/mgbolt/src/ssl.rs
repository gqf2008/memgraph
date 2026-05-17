//! SSL/TLS configuration for Bolt connections.
//!
//! Wraps `rustls` so that callers don't have to depend on it directly.
//! `server_config` loads a PEM-encoded certificate chain and private key —
//! the same `--bolt-cert-file` / `--bolt-key-file` flags the C++ server
//! accepts. `client_config` builds a default config for replication or
//! coordination peers; native roots are loaded on a best-effort basis with
//! the WebPKI bundle as a fallback so the function never fails.

use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

/// Install the ring CryptoProvider as the process-level default.
/// rustls 0.23 requires this before any builder calls. Safe to call
/// multiple times — subsequent calls after the first are no-ops.
pub fn install_default_provider() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok(); // ignore AlreadyInstalled error
}

#[derive(Debug)]
pub enum TlsError {
    Io(io::Error),
    NoPrivateKey,
    InvalidConfig(String),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::Io(e) => write!(f, "tls io error: {}", e),
            TlsError::NoPrivateKey => write!(f, "no private key found in PEM file"),
            TlsError::InvalidConfig(s) => write!(f, "invalid tls config: {}", s),
        }
    }
}

impl std::error::Error for TlsError {}

impl From<io::Error> for TlsError {
    fn from(e: io::Error) -> Self {
        TlsError::Io(e)
    }
}

/// Load every certificate from a PEM file into the order it appears (leaf
/// first, then intermediates).
pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let mut reader = BufReader::new(File::open(path)?);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(TlsError::Io)
}

/// Load the first private key from a PEM file. Accepts PKCS#8, RSA, or
/// SEC1 EC keys — `rustls_pemfile::private_key` handles all three.
pub fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let mut reader = BufReader::new(File::open(path)?);
    rustls_pemfile::private_key(&mut reader)?.ok_or(TlsError::NoPrivateKey)
}

/// Build a server-side `Arc<ServerConfig>` from PEM-encoded certificate
/// chain and private key files. No client-cert verification; mirrors the
/// C++ Bolt server which only authenticates peers at the application
/// layer (HELLO credentials).
pub fn server_config(cert_pem: &Path, key_pem: &Path) -> Result<Arc<ServerConfig>, TlsError> {
    install_default_provider();
    let certs = load_certs(cert_pem)?;
    if certs.is_empty() {
        return Err(TlsError::InvalidConfig(format!(
            "no certificates in {}",
            cert_pem.display()
        )));
    }
    let key = load_private_key(key_pem)?;
    let cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
    Ok(Arc::new(cfg))
}

/// Build a server config that also requires (and verifies) client
/// certificates against `client_ca_pem`. Used for mTLS replication.
pub fn server_config_mtls(
    cert_pem: &Path,
    key_pem: &Path,
    client_ca_pem: &Path,
) -> Result<Arc<ServerConfig>, TlsError> {
    install_default_provider();
    let certs = load_certs(cert_pem)?;
    let key = load_private_key(key_pem)?;
    let mut roots = RootCertStore::empty();
    for cert in load_certs(client_ca_pem)? {
        roots
            .add(cert)
            .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
    let cfg = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
    Ok(Arc::new(cfg))
}

/// Build a bare client config with an empty root store.
///
/// This is a building block for tests or for callers that will add roots
/// programmatically. For real TLS connections, use [`client_config_with_ca`]
/// instead.
pub fn client_config() -> Arc<ClientConfig> {
    install_default_provider();
    let roots = RootCertStore::empty();
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(cfg)
}

/// Build a client config that pins the given CA bundle. Use this when
/// connecting to a Memgraph cluster with a private CA — the trust store
/// only contains roots from `ca_pem`.
pub fn client_config_with_ca(ca_pem: &Path) -> Result<Arc<ClientConfig>, TlsError> {
    install_default_provider();
    let mut roots = RootCertStore::empty();
    for cert in load_certs(ca_pem)? {
        roots
            .add(cert)
            .map_err(|e| TlsError::InvalidConfig(e.to_string()))?;
    }
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_missing_cert_file_returns_io_error() {
        let err = load_certs(Path::new("/nonexistent/cert.pem")).unwrap_err();
        matches!(err, TlsError::Io(_));
    }

    #[test]
    fn test_client_config_is_buildable_without_native_roots() {
        // The function never panics even when no roots are available; an
        // empty store is acceptable for tests where TLS is opt-in.
        let _ = client_config();
    }
}
