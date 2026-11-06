//! Open a connection to a target address: TCP, then TLS for rtmps.
//!
//! Pattern: Factory — gives a `Box<dyn Duplex>` so the rest does not care whether it is secure.

use fs_streaming_core::TargetUrl;
use tokio::net::TcpStream;

use super::{Duplex, Tls};
use crate::config::Limits;

/// Why dialling failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DialFailure {
    /// No TCP connection in time.
    Unreachable,
    /// TCP worked, TLS did not.
    Tls,
}

/// Opens connections with one TLS configuration and one set of limits.
#[derive(Clone)]
pub struct Dialer {
    tls: Tls,
    limits: Limits,
}

impl Dialer {
    /// A dialer.
    pub fn new(tls: Tls, limits: Limits) -> Self {
        Self { tls, limits }
    }

    /// The limits in use.
    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Connect. The error says which step failed; `Ok` also says nothing about RTMP yet.
    pub async fn dial(&self, url: &TargetUrl) -> Result<Box<dyn Duplex>, DialFailure> {
        let addr = (url.host(), url.port());
        let tcp = tokio::time::timeout(self.limits.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| DialFailure::Unreachable)?
            .map_err(|_| DialFailure::Unreachable)?;
        let _ = tcp.set_nodelay(true);
        if !url.is_secure() {
            return Ok(Box::new(tcp));
        }
        let tls = tokio::time::timeout(
            self.limits.connect_timeout,
            self.tls.connect(url.host(), tcp),
        )
        .await
        .map_err(|_| DialFailure::Tls)?
        .map_err(|_| DialFailure::Tls)?;
        Ok(Box::new(tls))
    }
}
