//! TLS for rtmps targets.
//!
//! Pattern: Strategy (trust anchors) — `webpki-roots` plus, for tests only, extra roots passed in
//! through `RelayConfig`. The host name is always checked against the certificate; there is no switch
//! to turn that off, here or in the public API.

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, crypto::aws_lc_rs};

use crate::error::WireError;

/// A TLS client configuration that always verifies the server.
#[derive(Clone)]
pub struct Tls {
    connector: TlsConnector,
}

impl Tls {
    /// Built-in roots plus `extra_roots` (DER; empty outside tests).
    pub fn new(extra_roots: &[Vec<u8>]) -> Self {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        for der in extra_roots {
            let _ = roots.add(CertificateDer::from(der.clone()));
        }
        let config = ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("the default protocol versions are valid")
            .with_root_certificates(roots)
            .with_no_client_auth();
        Self {
            connector: TlsConnector::from(Arc::new(config)),
        }
    }

    /// Run TLS over `tcp`, checking that the certificate is for `host`.
    pub async fn connect(
        &self,
        host: &str,
        tcp: TcpStream,
    ) -> Result<tokio_rustls::client::TlsStream<TcpStream>, WireError> {
        let name = ServerName::try_from(host.to_string()).map_err(|_| WireError::Tls)?;
        self.connector
            .connect(name, tcp)
            .await
            .map_err(|_| WireError::Tls)
    }
}
