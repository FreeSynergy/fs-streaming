//! Errors of the wire and the relay.
//!
//! Pattern: none (plain error enum via `thiserror`) — fixed wording of our own. Text from the far end
//! is never kept, so a key echoed by a hostile server cannot end up in an error or a log line.

use fs_streaming_core::Note;

/// What can go wrong on a connection.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// Reading or writing failed.
    #[error("connection failed")]
    Io,
    /// The far end closed the connection.
    #[error("connection closed")]
    Closed,
    /// No answer in time.
    #[error("timed out")]
    Timeout,
    /// The bytes were not RTMP, or not RTMP we can follow.
    #[error("protocol error")]
    Protocol,
    /// A limit was exceeded (chunk or message size).
    #[error("limit exceeded")]
    Limit,
    /// TLS failed.
    #[error("tls failed")]
    Tls,
}

impl WireError {
    /// The note a person sees for it.
    pub fn note(&self) -> Note {
        match self {
            Self::Timeout => Note::NoAnswer,
            Self::Protocol | Self::Limit => Note::ProtocolError,
            Self::Tls => Note::TlsFailed,
            Self::Io | Self::Closed => Note::ConnectionClosed,
        }
    }
}

impl From<std::io::Error> for WireError {
    fn from(_: std::io::Error) -> Self {
        Self::Io
    }
}
