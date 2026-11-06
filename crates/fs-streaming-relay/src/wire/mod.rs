//! RTMP on the wire, over `rml_rtmp`'s chunk and handshake parts.
//!
//! Pattern: Adapter — `rml_rtmp` is sans-I/O; these files add the sockets, the timeouts and the
//! limits, and keep every payload as it came. Our own small client replaces `rml_rtmp`'s client
//! session because that session re-writes metadata and hides the stream id; see `DESIGN.md`.

pub mod amf;
mod client;
mod dial;
mod handshake;
mod link;
mod reader;
mod reply;
mod tls;

pub use client::{Client, PublishOutcome};
pub use dial::{DialFailure, Dialer};
pub use handshake::handshake;
pub use reader::MessageReader;
pub use tls::Tls;

use tokio::io::{AsyncRead, AsyncWrite};

/// Anything we can speak RTMP over: a TCP stream or a TLS stream.
pub trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}
